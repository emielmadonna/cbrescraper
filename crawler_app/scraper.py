from playwright.sync_api import sync_playwright
import pandas as pd
import time
from urllib.parse import urljoin
try:
    from crawler_app.vector_db import VectorDB
except ImportError:
    # Fallback for when running as script vs module
    from vector_db import VectorDB

class GenericCrawler:
    def __init__(self, headless=False, disable_vectors=False):
        self.headless = headless
        self.disable_vectors = disable_vectors
        self.playwright = None
        self.browser = None
        self.context = None
        self.page = None
        
        # Initialize Vector DB
        if not self.disable_vectors:
            self.vector_db = VectorDB()
        else:
            self.vector_db = None
            print("Vector DB disabled (Test Mode/Dry Run).")

    def exists(self, url, namespace=None):
        """Checks if a URL already exists in the vector DB by querying metadata."""
        if not self.vector_db:
            return False
        return self.vector_db.exists(url, namespace)

    def format_phone(self, phone_str):
        """Standardizes phone numbers to E.164 and strips emojis."""
        if not phone_str or phone_str == "Not Found":
            return None
        import re
        # Strip ALL emojis and non-ASCII characters
        cleaned = re.sub(r'[^\x00-\x7F]+', '', phone_str)
        # Strip all but digits and +
        digits = re.sub(r'[^\d+]', '', cleaned)
        
        if not digits:
            return None
            
        # Handle US missing prefix (assuming 10 digits = US)
        # Handle cases like "tel:12061234567" or "+1206..."
        if digits.startswith('+'):
            return digits
        
        # Heuristic Fix for CBRE Typo (426 -> 425) behavior seen in Bothell listings
        # 426 is an unassigned area code, 425 is correct for the region.
        if digits.startswith("426"):
            digits = "425" + digits[3:]
        elif digits.startswith("1426"):
             digits = "1425" + digits[4:]
             
        if len(digits) == 10:
            return "+1" + digits
        elif len(digits) == 11 and digits.startswith('1'):
            return "+" + digits
        elif len(digits) > 10:
            return "+" + digits
            
        return "+" + digits # Fallback base format

    def start_browser(self):
        """Starts the browser instance."""
        if not self.playwright:
            print(f"Starting browser (Headless={self.headless})...")
            self.playwright = sync_playwright().start()
            self.browser = self.playwright.chromium.launch(headless=self.headless)
            # Set a standard desktop viewport to avoid mobile layouts/detection
            self.context = self.browser.new_context(
                viewport={'width': 1280, 'height': 800},
                user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            )
            self.page = self.context.new_page()

    def close_browser(self):
        """Closes the browser instance."""
        if self.page:
            self.page.close()
        if self.context:
            self.context.close()
        if self.browser:
            self.browser.close()
        if self.playwright:
            self.playwright.stop()
        
        self.playwright = None
        self.browser = None
        self.context = None
        self.page = None

    def get_links(self, directory_url, card_selector='.CoveoResult', link_selector='a.cbre-c-listCards__title-link', name_selector=None, limit=None):
        """
        Extracts profile information using the persistent browser with pagination logic.
        Returns a list of dictionaries with name and profile URL.
        """
        results = []
        if not self.page:
            self.start_browser()
            
        # Clean URL to prevent pre-filtering limits
        try:
            import re
            clean_url = re.sub(r'([?&])numberOfResults=\d+', '', directory_url)
            clean_url = re.sub(r'([?&])first=\d+', '', clean_url)
            clean_url = clean_url.replace("&&", "&").replace("?&", "?")
            if clean_url.endswith("&") or clean_url.endswith("?"):
                clean_url = clean_url[:-1]
             
            print(f"Original URL: {directory_url}")
            print(f"Cleaned URL: {clean_url}")
            directory_url = clean_url
        except Exception as e:
            print(f"Error cleaning URL: {e}")

        try:
            print(f"Navigating to {directory_url}")
            self.page.goto(directory_url, timeout=60000)
            
            # Wait for content
            try:
                self.page.wait_for_load_state('networkidle', timeout=10000)
                # Wait for result cards instead of just links
                self.page.wait_for_selector(card_selector, timeout=10000)
            except Exception as e:
                print(f"Warning: Timeout waiting for results. Page might still have loaded content.")
            
            # Pagination Loop
            page_num = 1
            while True:
                print(f"  > Processing Page {page_num}...")
                
                # Scroll down to trigger lazy loading
                self.page.mouse.wheel(0, 500)
                time.sleep(0.3)
                self.page.mouse.wheel(0, 500)
                time.sleep(0.3)
                
                # Harvest from current page
                cards = self.page.query_selector_all(card_selector)
                for card in cards:
                     # Check limit inside the loop
                    if limit and len(results) >= limit:
                        print(f"    Reached limit ({limit}). Stopping pagination.")
                        return results

                    item = {'Name': '', 'URL': ''}
                    
                    # Try to get Link and Name
                    # If link_selector is explicitly None or empty, assume card is the link
                    if not link_selector:
                        link_el = card
                    else:
                        link_el = card.query_selector(link_selector)

                    if link_el and link_el.get_attribute('href'):
                        href = link_el.get_attribute('href')
                        item['URL'] = urljoin(directory_url, href)
                        
                        # Name extraction
                        if name_selector:
                             name_el = card.query_selector(name_selector)
                             if name_el:
                                 item['Name'] = name_el.inner_text().strip()
                        elif link_el != card:
                             # Default: use link text if link is a child
                             item['Name'] = link_el.inner_text().strip()
                    else:
                        # Fallback for people without profile pages (e.g. Madison Lichter)
                        # ONLY if we are in the default 'CoveoResult' mode
                        if card_selector == '.CoveoResult':
                            name_el = card.query_selector('p.cbre-c-listCards__title')
                            if name_el:
                                item['Name'] = name_el.inner_text().strip()
                        
                    # Fix Name if still empty
                    if not item['Name']:
                         item['Name'] = "Unknown"

                    # Only add if we have a URL (unless it's a person without a link, logic preserved above?)
                    # Actually, for properties we MUST have a URL.
                    # For people, we might capture 'Unknown' url? The original code didn't add if no URL was found (except specifically falling back).
                    # Let's simple check:
                    if item['URL']:
                        # Avoid duplicates
                        if not any(r['Name'] == item['Name'] and r['URL'] == item['URL'] for r in results):
                            results.append(item)
                
                print(f"    Found {len(cards)} items on this page. Total unique: {len(results)}")
                
                if limit and len(results) >= limit:
                     print(f"    Reached limit ({limit}). Stopping pagination.")
                     break

                # Find and Click Next Button
                next_btn_selector = 'span[title="Next"]'
                next_btn = self.page.query_selector(next_btn_selector)
                
                if next_btn and next_btn.is_visible():
                    try:
                        print("    Clicking 'Next' page...")
                        # Click the parent or use javascript to ensure it triggers
                        self.page.evaluate('el => el.click()', next_btn)
                        
                        # Wait for results to update (check first result changes)
                        first_name_before = cards[0].inner_text().strip() if cards else ""
                        
                        time.sleep(2)
                        self.page.wait_for_load_state('networkidle', timeout=5000)
                        
                        # Verify we actually moved
                        new_cards = self.page.query_selector_all('.CoveoResult')
                        if new_cards and new_cards[0].inner_text().strip() == first_name_before:
                            print("    Wait... Page did not seem to change. Retrying click...")
                            self.page.evaluate('el => el.click()', next_btn)
                            time.sleep(2)
                        
                        page_num += 1
                        if page_num > 50: # Safety break
                            break
                    except Exception as e:
                        print(f"    Could not click Next button: {e}")
                        break
                else:
                    print("    No 'Next' button found (visible). Reached end.")
                    break
                        
        except Exception as e:
            print(f"Error fetching directory: {e}")
        
        return results

    def scrape_details(self, profile_url, phone_selector, experience_selector):
        """
        Scrapes details from a single profile page using the persistent browser.
        """
        data = {
            'URL': profile_url, 
            'First Name': '', 
            'Last Name': '',
            'Title': '',
            'Email': '',
            'Phone': '', 
            'Address Line': '',
            'City': '',
            'State': '',
            'Zip': '',
            'Full Address': '',
            'Experience': '',
            'vCardURL': '',
            'LinkedProperties': [],
            'ListingsURL': ''
        }
        # URL Normalization
        if 'test-www1.cbre.com' in profile_url:
            profile_url = profile_url.replace('test-www1.cbre.com', 'www.cbre.com')
            data['URL'] = profile_url

        print(f"  > Scraper visiting: {profile_url}")
        
        if not self.page:
            self.start_browser()
            
        try:
            # CBRE can have many background trackers, so 'load' or 'domcontentloaded' is safer than 'networkidle'
            self.page.goto(profile_url, timeout=30000, wait_until='load')
            
            # --- Cloudflare Detection ---
            if "Verify you are human" in self.page.content() or "cf-challenge" in self.page.content():
                print("  !! Cloudflare Challenge Detected! Attempting to wait/solve...")
                # Wait for the challenge to be solved manually if headless=False
                # Or try a simple click if it's the standard checkbox
                try:
                    # Give it a few seconds to auto-solve or user to click
                    time.sleep(5)
                    # Try to find the checkbox iframe and click?
                    # Highly variable, but let's at least wait.
                except:
                    pass

            # Extra wait for the name since it's the hero element
            try:
                self.page.wait_for_selector("h1.cbre-c-personHero__name", timeout=15000)
            except:
                if "Verify you are human" in self.page.content():
                    print("  !! STILL BLOCKED by Cloudflare. Suggest running with 'Show Browser = True' to solve manually.")
                pass
        except Exception as e:
            print(f"  !! SKIPPING: Could not reach {profile_url}. Error: {e}")
            for key in data:
                if key != 'URL':
                    data[key] = "SKIPPED (Unreachable)"
            return data

        try:
            # --- 1. Name & Title ---
            try:
                name_el = self.page.query_selector("h1.cbre-c-personHero__name")
                if name_el:
                    name_val = name_el.inner_text().strip()
                    parts = name_val.split(" ", 1)
                    data['First Name'] = parts[0]
                    data['Last Name'] = parts[1] if len(parts) > 1 else ""
                
                # Title often in personHero sub-heading
                title_el = self.page.query_selector(".cbre-c-personHero__designation") or \
                           self.page.query_selector(".cbre-c-personHero__title")
                if title_el:
                    data['Title'] = title_el.inner_text().strip()
            except Exception as e:
                 print(f"Error parsing name/title: {e}")

            # --- 2. Phone, Email & vCard ---
            try:
                js_contact = """
                    () => {
                        const res = {phone_data: [], vcard: null, email: null};
                        const clean = (s) => s ? s.replace('tel:', '').replace('mailto:', '').trim() : "";

                        // 1. Hero Section
                        const hero = document.querySelector('.cbre-c-personHero');
                        if (hero) {
                            hero.querySelectorAll('a[href^="tel:"]').forEach(a => {
                                const label = a.getAttribute('aria-label') || 'Phone';
                                res.phone_data.push({label: label, number: clean(a.getAttribute('href'))});
                            });
                            hero.querySelectorAll('a[href^="mailto:"]').forEach(a => {
                                if(!res.email) res.email = clean(a.getAttribute('href'));
                            });
                            const vc = hero.querySelector('a[aria-label*="Contact Card"]');
                            if (vc) res.vcard = vc.href;
                        }
                        
                        // 2. Office Cards
                        const office = document.querySelector('.cbre-c-inlineCards--office');
                        if (office) {
                            office.querySelectorAll('a[href^="tel:"]').forEach(a => {
                                res.phone_data.push({label: 'Office', number: clean(a.getAttribute('href'))});
                            });
                            office.querySelectorAll('a[href^="mailto:"]').forEach(a => {
                                if(!res.email) res.email = clean(a.getAttribute('href'));
                            });
                        }
                        
                        // 3. Fallback Greedy Email
                        if (!res.email) {
                            const m = document.body.innerText.match(/[\\w\\.-]+@[\\w\\.-]+\\.\\w+/);
                            if (m) res.email = m[0];
                        }

                        return res;
                    }
                """
                contact_val = self.page.evaluate(js_contact)
                
                # Map Categorized Phones
                data['phone_number'] = None
                data['mobile_phoneNumber'] = None
                data['phone_numbers'] = [] # Keep for internal list
                
                for p_item in contact_val['phone_data']:
                    label = p_item['label'].lower()
                    clean_num = self.format_phone(p_item['number'])
                    if not clean_num: continue
                    
                    if clean_num not in data['phone_numbers']:
                        data['phone_numbers'].append(clean_num)
                    
                    if any(k in label for k in ['cell', 'mobile', 'handset']):
                        if not data['mobile_phoneNumber']: data['mobile_phoneNumber'] = clean_num
                    else:
                        if not data['phone_number']: data['phone_number'] = clean_num
                
                # Legacy fields for backward compat
                data['Phone'] = " | ".join(data['phone_numbers']) if data['phone_numbers'] else "Not Found"
                data['Email'] = contact_val['email'] or "Not Found"
                if contact_val['vcard']:
                    v = contact_val['vcard']
                    data['vCardURL'] = f"https://www.cbre.com{v}" if v.startswith('/') else v
                else:
                    data['vCardURL'] = "Not Found"
            except Exception as e:
                print(f"Error parsing contact: {e}")

            # --- 3. Extract & Parse Address ---
            try:
                raw_address = ""
                
                if 'cbre.com' in profile_url:
                    # Logic to find the "Associated Office" address card
                    js_addr = """
                        () => {
                           // Target the specific office card designation
                           const officeCard = document.querySelector('.cbre-c-inlineCards--office');
                           if (officeCard) {
                               const designation = officeCard.querySelector('.cbre-c-inlineCards__personDesignation');
                               if (designation) {
                                   return designation.innerText.trim();
                               }
                           }
                           
                           // Fallback to searching headers if class changed
                           const headers = Array.from(document.querySelectorAll('h3.cbre-c-inlineCards__title, h2, div'));
                           const officeHeader = headers.find(h => h.innerText.includes('Associated Office'));
                           if (officeHeader) {
                               const container = officeHeader.closest('.cbre-c-inlineCards__contactCardWrapper') || 
                                                 officeHeader.closest('.cbre-c-inlineCards--office');
                               if (container) {
                                   const designation = container.querySelector('.cbre-c-inlineCards__personDesignation');
                                   if (designation) return designation.innerText.trim();
                                   return container.innerText.replace('Associated Office', '').replace('Location', '').trim();
                               }
                           }
                           return null;
                        }
                    """
                    raw_address = self.page.evaluate(js_addr) or ""
                
                # Cleanup the raw blob
                junk_terms = [
                    "Associated Office", "Location", "Get Directions", "Contact", 
                    "Find Your Perfect Space", "Search Properties", "Search Now", 
                    "Find My Listings"
                ]
                
                clean_lines = []
                if raw_address:
                    for line in raw_address.split('\n'):
                        s = line.strip()
                        # Filter out numbers that looks like office phone if they leaked in
                        if s and s not in junk_terms and not s.startswith("View my") and not s.startswith("+1"):
                            clean_lines.append(s)
                
                data['Full Address'] = "\n".join(clean_lines) if clean_lines else ""
                
                # Parsing City/State/Zip from LAST line of cleaned address
                if clean_lines:
                    # Heuristic: Last line is "Seattle, WA 98101"
                    last_line = clean_lines[-1]
                    
                    # Everything before last line is street
                    # If multiple lines remain, join them
                    street_candidates = clean_lines[:-1]
                    data['Address Line'] = ", ".join(street_candidates)
                    
                    if "," in last_line:
                        # "Seattle, WA 98101"
                        parts = last_line.rsplit(",", 1)
                        data['City'] = parts[0].strip()
                        
                        state_zip = parts[1].strip()
                        # Use regex to handle multiple tabs/spaces from CBRE site
                        import re
                        sz_parts = re.split(r'\s+', state_zip)
                        if len(sz_parts) >= 2:
                            data['State'] = sz_parts[0]
                            data['Zip'] = sz_parts[1]
                        else:
                            data['State'] = state_zip
                    else:
                        # Fallback
                        data['City'] = last_line
                        
            except Exception as e:
                data['Full Address'] = f"Error: {e}"

            # --- 4. Extract Experience ---
            try:
                exp_val = "Not Found"
                if 'cbre.com' in profile_url:
                    js_script = """
                        () => {
                            // Strategy 1: Look for "Professional Experience" header in various forms
                            const headers = Array.from(document.querySelectorAll('div.cbre-c-inlineBodyCard__title, h2, h3'));
                            const targetHeader = headers.find(h => h.innerText.trim().includes('Professional Experience'));
                            
                            if (targetHeader) {
                                // Try description sibling
                                let desc = targetHeader.parentElement.querySelector('.cbre-c-inlineBodyCard__description');
                                if (desc) return desc.innerText.trim();
                                
                                // Try next sibling
                                if (targetHeader.nextElementSibling) return targetHeader.nextElementSibling.innerText.trim();
                                
                                // Try parent's content excluding header
                                return targetHeader.parentElement.innerText.replace(targetHeader.innerText, '').trim();
                            }
                            return null;
                        }
                    """
                    exp_val = self.page.evaluate(js_script) or "Not Found"
                    
                data['Experience'] = exp_val
                
                # --- 4.5 Extract Specialties & Pilot Keywords ---
                try:
                    js_spec = """
                        () => {
                            const bioEl = document.querySelector('.cbre-c-inlineBodyCard__description.cbre-c-wysiwyg');
                            const bioText = bioEl ? bioEl.innerText : "";
                            
                            const specs = [];
                            document.querySelectorAll('.cbre-c-inlineCards__specialtyTag, .cbre-c-cl-tag').forEach(el => {
                                specs.push(el.innerText.trim());
                            });
                            
                            // Heuristic keywords for March Pilot (Case-Insensitive check)
                            const keywords = ["Industrial", "Logistics", "Kent Valley", "South Seattle", "Tenant Representation", "Landlord Representation", "Office", "Retail", "Investment Sales"];
                            const foundKeywords = keywords.filter(k => {
                                const rel = new RegExp(k, 'i');
                                return rel.test(bioText);
                            });
                            
                            // Combine structured tags and bio keywords
                            const allSpecs = [...new Set([...specs, ...foundKeywords])];
                            
                            // Get first paragraph / sentence for bio summary
                            const firstParagraph = bioText.split('\\n\\n')[0] || bioText.split(/[.!]/)[0];

                            return {
                                specialties: allSpecs.join(', '),
                                specialty_tags: allSpecs,
                                bio_summary: firstParagraph.trim()
                            };
                        }
                    """
                    spec_res = self.page.evaluate(js_spec)
                    data['Specialties'] = spec_res['specialties'] or "N/A"
                    data['specialty_tags'] = spec_res['specialty_tags']
                    data['bio_summary'] = spec_res['bio_summary'] or data.get('Experience', '')[:500]
                except:
                    data['Specialties'] = "N/A"
                    data['specialty_tags'] = []
                    data['bio_summary'] = data.get('Experience', '')[:500]

            except Exception as e:
                data['Experience'] = f"Error: {e}"

            # --- 5. Extract Linked Properties & Listings ---
            try:
                js_props = """
                    () => {
                        const res = {listingsUrl: null, transactions: [], debug: {}};
                        
                        // 1. Get Listings URL if any
                        const links = Array.from(document.querySelectorAll('a'));
                        const listingLink = links.find(a => a.innerText.includes('Search Properties') || a.innerText.includes('View My Listings'));
                        if (listingLink) res.listingsUrl = listingLink.href;
                        
                        // 2. Get Significant Transactions
                        const headers = Array.from(document.querySelectorAll('h3, h4, div.cbre-c-inlineBodyCard__title'));
                        const transHeader = headers.find(h => h.innerText.trim() === 'Significant Transactions');
                        
                        if (transHeader) {
                            const container = transHeader.closest('.cbre-c-inlineBodyCard');
                            if (container) {
                                // Clone to safe manipulation
                                const clone = container.cloneNode(true);
                                
                                // Remove known junk items / promo cards that share the space
                                const junkSelectors = ['.cbre-c-inlineBodyCard__card', '.cbre-c-inlineBodyCard__title'];
                                junkSelectors.forEach(sel => {
                                    clone.querySelectorAll(sel).forEach(el => el.remove());
                                });
                                
                                // Extract clean text
                                const text = clone.innerText.trim();
                                if (text) {
                                    // Split logic could be added here if structure is consistent
                                    res.transactions.push(text);
                                }
                            }
                        }
                        
                        return res;
                    }
                """
                props_val = self.page.evaluate(js_props)
                
                # Parse Significant Transactions from the text blob if needed
                raw_tx = props_val['transactions']
                clean_tx = []
                
                if raw_tx and isinstance(raw_tx, list):
                    blob = "\n".join(raw_tx)
                    
                    # Section markers
                    start_marker = "Significant Transactions"
                    end_marker = "Clients Represented" # Heuristic based on seen profile
                    
                    if start_marker in blob:
                        try:
                            start_idx = blob.find(start_marker) + len(start_marker)
                            end_idx = blob.find(end_marker, start_idx)
                            
                            if end_idx == -1:
                                # Try end of string
                                chunk = blob[start_idx:]
                            else:
                                chunk = blob[start_idx:end_idx]
                                
                            # Clean up the chunk (it is newline separated)
                            lines = [l.strip() for l in chunk.split('\n') if l.strip()]
                            
                            # Heuristic: The transactions appear in blocks of 4 lines (Name, Location, Type, Size)
                            if len(lines) > 0 and len(lines) % 4 == 0:
                                groups = []
                                for i in range(0, len(lines), 4):
                                    groups.append({
                                        'Name': lines[i],
                                        'Location': lines[i+1],
                                        'Type': lines[i+2],
                                        'Size': lines[i+3]
                                    })
                                clean_tx = groups
                            else:
                                clean_tx = lines
                        except Exception as e:
                            print(f"Error parsing transaction blob: {e}")
                            clean_tx = raw_tx # Fallback
                    else:
                         # Maybe the list is the whole thing?
                         clean_tx = raw_tx
                
                data['LinkedProperties'] = clean_tx
                data['ListingsURL'] = props_val['listingsUrl']
            except Exception as e:
                data['LinkedProperties'] = []
                data['ListingsURL'] = ""
                print(f"Error extracting linked properties: {e}")

        except Exception as e:
            print(f"Error scraping {profile_url}: {e}")
        
        # Upsert to Pinecone
        if self.vector_db:
             self.vector_db.upsert_person(data)
             
        return data

    def scrape_property(self, property_url):
        """
        Scrapes details from a property page, specifically handling the 'Contact for Details' modal.
        """
        data = {
            'URL': property_url,
            'Property Name': '',
            'Address': '',
            'Description': '',
            'Brokers': [],
            'Brochure URL': 'Not Found'
        }
        
        print(f"  > Property Scraper visiting: {property_url}")
        
        if not self.page:
            self.start_browser()
            
        try:
            self.page.goto(property_url, timeout=45000, wait_until='domcontentloaded')
            
            # Dismiss Cookie Banner (OneTrust/Generic) if present
            try:
                cookie_btn = self.page.query_selector('#onetrust-accept-btn-handler, #onetrust-consent-sdk button, .cookie-banner button')
                if cookie_btn and cookie_btn.is_visible():
                    print("    Dismissing cookie banner...")
                    cookie_btn.click()
                    time.sleep(1)
            except: pass

            # Wait for meaningful content
            try:
                self.page.wait_for_selector('h1', timeout=10000)
                # Give SPA a moment to fill text
                for _ in range(5):
                    if len(self.page.inner_text('h1').strip()) > 5: break
                    time.sleep(1)
            except: pass
            
            # --- 1. Basic Info (Name & Initial Address) ---
            title_el = self.page.query_selector('h1')
            if title_el:
                title_text = title_el.inner_text().strip()
                # Filter out generic placeholder titles
                if title_text.lower() in ['www.cbre.com', 'cbre']:
                    title_text = ""
                
                if title_text:
                    t_parts = [p.strip() for p in title_text.split('\n') if p.strip()]
                    data['Property Name'] = t_parts[0]
                    if len(t_parts) > 1:
                        data['Address'] = ", ".join(t_parts[1:])

            # --- 2. Initial Data Scan (Static Contacts & Brochure) ---
            # Some pages have contacts visible without a modal
            static_brokers = self.page.query_selector_all('div[class*="contact"], div[class*="agent"], section[class*="contact"]')
            for s_el in static_brokers:
                txt = s_el.inner_text().lower()
                if any(k in txt for k in ["contact", "agent", "broker"]):
                    # If we find a block with a phone or email pattern, extract it
                    import re
                    # Improved: Check for tel: links first
                    tel_links = s_el.query_selector_all('a[href^="tel:"]')
                    clean_phones = []
                    mobile_phone = None
                    office_phone = None
                    
                    for tel in tel_links:
                        raw = tel.get_attribute('href').replace('tel:', '')
                        label = tel.inner_text().lower() or tel.get_attribute('aria-label') or ""
                        cleaned = self.format_phone(raw)
                        if cleaned:
                            if any(k in label for k in ['cell', 'mobile', 'handset']) and not mobile_phone:
                                mobile_phone = cleaned
                            elif not office_phone:
                                office_phone = cleaned
                            clean_phones.append(cleaned)
                            
                    # Fallback to regex if no tel links
                    if not clean_phones:
                        raw_regex = list(set(re.findall(r'(\d{3}[-\.\s]??\d{3}[-\.\s]??\d{4})', s_el.inner_text())))
                        for r in raw_regex:
                            c = self.format_phone(r)
                            if c: clean_phones.append(c)
                            if not office_phone: office_phone = c

                    emails = list(set(re.findall(r'[\w\.-]+@[\w\.-]+\.\w+', s_el.inner_text())))
                    if clean_phones or emails:
                        name_el = s_el.query_selector('strong, h3, h4, [class*="name"]')
                        name = name_el.inner_text().strip() if name_el else "Contact"
                        data['Brokers'].append({
                            'Name': name, 
                            'phone_number': office_phone,
                            'mobile_phoneNumber': mobile_phone,
                            'phone_numbers': clean_phones,
                            'Emails': emails
                        })
                        print(f"    - Static Agent Found: {name}")

            # Greedy Brochure Detection (Hero, Modal, Spaces)
            def find_brochure():
                search_js = """
                () => {
                    const results = [];
                    // 1. Check all elements with data attributes commonly used for links
                    document.querySelectorAll('[data-pill-link-info], [data-url], [data-href]').forEach(el => {
                        const url = el.getAttribute('data-pill-link-info') || el.getAttribute('data-url') || el.getAttribute('data-href');
                        const label = (el.innerText + (el.getAttribute('data-pill-asset-type') || "")).toLowerCase();
                        if (url && label.includes('brochure')) results.push(url);
                    });
                    
                    // 2. Check all anchors and buttons with "Brochure" text
                    document.querySelectorAll('a, button, div.cbre-c-pd-hero__button, div[class*="pill"], div.cbre-c-pd-collateralBar__pillContainer').forEach(el => {
                        if (el.innerText.toLowerCase().includes('brochure')) {
                            const href = el.href || el.getAttribute('href');
                            if (href) results.push(href);
                            else {
                                // Check children/parents for href
                                const pLink = el.closest('a');
                                if (pLink) results.push(pLink.href);
                                const cLink = el.querySelector('a');
                                if (cLink) results.push(cLink.href);
                            }
                        }
                    });
                    return results;
                }
                """
                candidates = self.page.evaluate(search_js)
                if candidates:
                    current_path = property_url.split('?')[0].rstrip('/')
                    for b_url in candidates:
                        if not b_url or b_url.startswith('#') or 'javascript:' in b_url: continue
                        if b_url.startswith('/'): b_url = f"https://www.cbre.com{b_url}"
                        
                        check_url = b_url.split('?')[0].rstrip('/')
                        is_likely_file = any(ext in b_url.lower() for ext in ['.pdf', '.doc', '.zip', 'fileassets', 'resources', 'brochure'])
                        
                        if check_url != current_path and is_likely_file:
                            return b_url
                return None

            b_link = find_brochure()
            if b_link: 
                data['Brochure URL'] = b_link
                print(f"    Found Brochure (Main): {data['Brochure URL']}")

            # --- 3. Contact For Details Modal (Associated Contacts) ---
            try:
                # Force click modal even if we have some brokers, to see if more/brochure exists
                btn_selector = '.cbre-c-pd-brokerCard__button, button:has-text("Contact For Details"), button:has-text("Contact Agent"), .cbre-c-pd-brokerCard__contact-button'
                btns = self.page.query_selector_all(btn_selector)
                for i, btn in enumerate(btns):
                    try:
                        if btn.is_visible():
                            print(f"    Opening Modal (Button {i+1})...")
                            self.page.evaluate('el => el.click()', btn)
                            break
                    except: continue
                
                # Check for Modal
                try:
                    self.page.wait_for_selector('.cbre-c-pl-contact-form, .cbre-c-pl-contact-form__content', timeout=5000)
                    print("    Modal appeared.")
                    time.sleep(1) # Wait for brokers to render
                    # Try finding brochure again in modal
                    b_link_modal = find_brochure()
                    if b_link_modal and data['Brochure URL'] == 'Not Found':
                        data['Brochure URL'] = b_link_modal
                        print(f"    Found Brochure (Modal): {data['Brochure URL']}")
                except:
                    print("    Contact modal didn't appear in time.")
                    
                # Try primary selector first, fallback to any cards
                brokers_els = self.page.query_selector_all('.cbre-c-pl-contact-form__broker-content') or \
                              self.page.query_selector_all('.cbre-c-pl-contact-form__broker') or \
                              self.page.query_selector_all('[class*="broker"]')

                for broker_el in brokers_els:
                    broker_info = {}
                    name_el = broker_el.query_selector('[class*="name"]') or broker_el.query_selector('strong, span, h4')
                    if name_el:
                        broker_info['Name'] = name_el.inner_text().strip()
                        
                    # Phones & Emails
                    phones_els = broker_el.query_selector_all('a[href^="tel:"]')
                    raw_phone_data = []
                    for p_el in phones_els:
                        number = p_el.get_attribute('href').replace('tel:', '').strip()
                        # Check link text, aria-label, and parent text for labels
                        label = p_el.inner_text().lower() or p_el.get_attribute('aria-label') or ""
                        parent = p_el.query_selector('xpath=..')
                        parent_text = parent.inner_text().lower() if parent else ""
                        full_context = f"{label} {parent_text}"
                        raw_phone_data.append({'label': full_context, 'number': number})
                    
                    clean_phones = []
                    mobile_phone = None
                    office_phone = None
                    for pi in raw_phone_data:
                        c = self.format_phone(pi['number'])
                        if c:
                            if any(k in pi['label'] for k in ['cell', 'mobile', 'handset']) and not mobile_phone:
                                mobile_phone = c
                            elif not office_phone:
                                office_phone = c
                            clean_phones.append(c)
                            
                    broker_info['phone_number'] = office_phone
                    broker_info['mobile_phoneNumber'] = mobile_phone
                    broker_info['phone_numbers'] = clean_phones
                    
                    email_els = broker_el.query_selector_all('a[href^="mailto:"]')
                    broker_info['Emails'] = [e.inner_text().replace('mailto:', '').strip() for e in email_els]
                    
                    if broker_info.get('Name') or broker_info.get('phone_numbers'):
                        data['Brokers'].append(broker_info)
                        print(f"    - Agent Found: {broker_info.get('Name')}")
                        
                if not data['Brokers']:
                    print("    No brokers found in modal. Trying greedy text search...")
                    try:
                        modal_sel = '.cbre-c-pl-contact-form, .cbre-c-pl-contact-form__content'
                        txt = self.page.inner_text(modal_sel)
                        import re
                        emails = list(set(re.findall(r'[\w\.-]+@[\w\.-]+\.\w+', txt)))
                        raw_phones = list(set(re.findall(r'(\d{3}[-\.\s]??\d{3}[-\.\s]??\d{4})', txt)))
                        clean_phones = [self.format_phone(p) for p in raw_phones if self.format_phone(p)]
                        if emails or clean_phones:
                            data['Brokers'].append({'Name': 'Alternative Contact', 'phone_numbers': clean_phones, 'Emails': emails})
                            print(f"    - Greedy Contacts Found: {len(clean_phones)} phones, {len(emails)} emails")
                    except: pass
            except Exception as e:
                print(f"    Modal handling skipped/failed: {e}")

            # --- 4. Precision Extraction (Address & Highlights) ---
            try:
                js_script = """
                () => {
                    const sections = {};
                    const getCleanText = (el) => el ? el.innerText.replace(/\\s+/g, ' ').trim() : "";
                    const searchTags = ['h1', 'h2', 'h3', 'h4', 'h5', 'div.cbre-c-pd-overview__title', 'strong'];
                    document.querySelectorAll(searchTags.join(',')).forEach(el => {
                        const txt = el.innerText.trim().toLowerCase();
                        let key = null;
                        if (txt === "highlights" || txt.includes("highlights")) key = "Highlights";
                        else if (txt === "overview" || txt.includes("overview")) key = "Overview";
                        
                        if (key && !sections[key]) {
                            let content = [];
                            let runner = el.nextElementSibling;
                            if (!runner && el.parentElement) runner = el.parentElement.nextElementSibling;
                            let b = 0;
                            while (runner && b < 10) { // Check more blocks
                                if (['H1','H2','H3','H4'].includes(runner.tagName)) break;
                                const t = getCleanText(runner);
                                if (t.length > 5) { content.push(t); b++; }
                                runner = runner.nextElementSibling;
                            }
                            sections[key] = content.join('\\n');
                        }
                    });
                    
                    let addr = "";
                    const sels = ['.cbre-c-pd-hero__address', '.cbre-c-pd-hero__sub-title', 'address', '.cbre-c-pd-description__address'];
                    for (const s of sels) {
                        const el = document.querySelector(s);
                        if (el && el.innerText.trim().length > 5 && el.innerText.trim().length < 200) { 
                            addr = el.innerText.trim(); 
                            break; 
                        }
                    }
                    
                    if (!addr) {
                        const candidates = Array.from(document.querySelectorAll('p, div, span'))
                            .filter(el => el.innerText.trim().length > 10 && el.innerText.trim().length < 100 && el.innerText.match(/[A-Z]{2}\\s+\\d{5}/));
                        if (candidates.length > 0) addr = candidates[0].innerText.trim();
                    }
                    
                    let fb = "";
                    const m = document.querySelector('.cbre-c-pd-overview__description, .cbre-c-pd-description, .cbre-c-pd-text-media__description, #overview');
                    if (m) fb = m.innerText.trim().slice(0, 1500);
                    
                    let sqft = "";
                    const sqft_match = document.body.innerText.match(/(\\d{1,3}(?:,\\d{3})*\\s*-\\s*)?\\d{1,3}(?:,\\d{3})*\\s*SF/i);
                    if (sqft_match) sqft = sqft_match[0];

                    return { highlights: sections['Highlights']||"", overview: sections['Overview']||"", fallback: fb, address: addr, sqft: sqft };
                }
                """
                res = self.page.evaluate(js_script)
                data['SqFt'] = res.get('sqft', 'N/A')
                
                # Format Description
                parts = []
                if res.get('highlights'): parts.append(f"Highlights:\\n{res['highlights']}")
                if res.get('overview'): parts.append(f"Overview:\\n{res['overview']}")
                if not parts and res.get('fallback'): parts.append(res['fallback'])
                data['Description'] = "\\n\\n".join(parts)
                
                # Set Address
                raw_addr = res.get('address', '')
                if raw_addr:
                    # If we have Address from H1 already, check if raw_addr is just city/state
                    if data['Address']:
                        # If H1 had street, and raw_addr is city/state, join them properly
                        # But prevent "Street, Street, City"
                        if raw_addr.lower() not in data['Address'].lower():
                            data['Address'] = f"{data['Address']}, {raw_addr}"
                    else:
                        data['Address'] = raw_addr

                # Final de-duplicate Name from Address
                if data['Address'] and data['Property Name']:
                    # Only remove the EXACT name if it's a prefix
                    name = data['Property Name'].lower()
                    addr_low = data['Address'].lower()
                    if addr_low.startswith(name):
                        data['Address'] = data['Address'][len(name):].strip().lstrip(',').strip()
                
                print(f"    > Extracted Address: {data['Address']}")
                print(f"    > Extracted Description: {len(data['Description'])} chars")
            except Exception as e:
                print(f"    Error in JS extraction: {e}")

        except Exception as e:
            print(f"Error scraping property: {e}")
            
        # 5. Save to Vector DB
        if self.vector_db:
            self.vector_db.upsert_property(data)
            
        return data
