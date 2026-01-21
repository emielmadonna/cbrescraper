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
    def __init__(self, headless=True, disable_vectors=False):
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

    def start_browser(self):
        """Starts the browser instance."""
        if not self.playwright:
            print(f"Starting browser (Headless={self.headless})...")
            self.playwright = sync_playwright().start()
            self.browser = self.playwright.chromium.launch(headless=self.headless)
            self.context = self.browser.new_context()
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
            # --- 1. Extract Name (First/Last) ---
            try:
                # CBRE Name Selector
                name_val = ""
                name_el = self.page.query_selector("h1.cbre-c-personHero__name")
                if name_el:
                    name_val = name_el.inner_text().strip()
                
                if name_val:
                    parts = name_val.split(" ", 1)
                    data['First Name'] = parts[0]
                    data['Last Name'] = parts[1] if len(parts) > 1 else ""
                else:
                    data['First Name'] = "Not Found"
            except Exception as e:
                 print(f"Error parsing name: {e}")

            # --- 2. Extract Phone (Direct, Mobile, Office) & vCard ---
            try:
                js_contact = """
                    () => {
                        const data = {phones: [], vcard: null};
                        
                        // Helper to clean phone
                        const cleanPhone = (str) => str.replace('tel:', '').trim();

                        // Check Hero Section (Direct/Mobile)
                        const hero = document.querySelector('.cbre-c-personHero');
                        if (hero) {
                            const heroPhones = Array.from(hero.querySelectorAll('a[href^="tel:"]'));
                            heroPhones.forEach(a => {
                                const label = a.getAttribute('aria-label') || 'Phone';
                                const num = cleanPhone(a.getAttribute('href'));
                                if (num) {
                                    data.phones.push(`${label}: ${num}`);
                                }
                            });
                            
                            // vCard
                            const vcardLink = hero.querySelector('a[aria-label="Download Contact Card"]');
                            if (vcardLink) data.vcard = vcardLink.href;
                        }
                        
                        // Check Office Card
                        const officeCard = document.querySelector('.cbre-c-inlineCards--office');
                        if (officeCard) {
                            const officePhones = Array.from(officeCard.querySelectorAll('a[href^="tel:"]'));
                            officePhones.forEach(a => {
                                const label = a.getAttribute('aria-label') || 'Office';
                                const num = cleanPhone(a.getAttribute('href'));
                                if (num) {
                                    data.phones.push(`Office ${label}: ${num}`);
                                }
                            });
                        }

                        return data;
                    }
                """
                contact_val = self.page.evaluate(js_contact)
                data['Phone'] = " | ".join(contact_val['phones']) if contact_val['phones'] else "Not Found"
                if contact_val['vcard']:
                    v = contact_val['vcard']
                    if v.startswith('/'):
                        v = f"https://www.cbre.com{v}"
                    data['vCardURL'] = v
                else:
                    data['vCardURL'] = "Not Found"
            except Exception as e:
                data['Phone'] = f"Error: {e}"
                data['vCardURL'] = "Error"

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
            'Brokers': []
        }
        
        print(f"  > Property Scraper visiting: {property_url}")
        
        if not self.page:
            self.start_browser()
            
        try:
            self.page.goto(property_url, timeout=30000, wait_until='domcontentloaded')
            
            # --- 1. Basic Property Info (Hero Section) ---
            try:
                # Wait for at least the H1 or the Contact button to indicate load
                try:
                    self.page.wait_for_selector('h1', timeout=10000)
                except:
                    print("    Timeout waiting for H1.")

                # Example selectors - adjust based on actual page structure
                title_el = self.page.query_selector('h1')
                if title_el:
                    data['Property Name'] = title_el.inner_text().strip()
                    
                addr_el = self.page.query_selector('.cbre-c-pd-hero__address') or self.page.query_selector('address')
                if addr_el:
                    data['Address'] = addr_el.inner_text().strip()
            except Exception as e:
                print(f"    Error scraping basic info: {e}")

            # --- 2. Contact For Details Modal ---
            try:
                # Look for the Contact Button with a wait
                btn_selector = '.cbre-c-pd-brokerCard__button'
                try:
                    self.page.wait_for_selector(btn_selector, timeout=1000)
                except:
                    print("    Timeout waiting for contact button (it might not exist).")

                contact_btn = self.page.query_selector(btn_selector)
                
                if contact_btn:
                    print("    Clicking 'Contact For Details' button (JS force)...")
                    try:
                        # DIRECT JS Click - Fastest, ignores visibility/scroll headers
                        self.page.evaluate('el => el.click()', contact_btn)
                    except Exception as e:
                        print(f"    JS click failed: {e}")
                    
                    # Wait for modal to appear
                    modal_selector = '.cbre-c-pl-contact-form'
                    try:
                        self.page.wait_for_selector(modal_selector, timeout=5000)
                        print("    Modal appeared.")
                        
                        # Extract Broker Info from Modal
                        brokers_els = self.page.query_selector_all('.cbre-c-pl-contact-form__broker-content')
                        
                        for broker_el in brokers_els:
                            broker_info = {}
                            
                            # Name
                            name_el = broker_el.query_selector('.cbre-c-pl-contact-form__broker-name')
                            if name_el:
                                broker_info['Name'] = name_el.inner_text().strip()
                                
                            # Phones
                            phones_els = broker_el.query_selector_all('a[href^="tel:"]')
                            phones = [p.inner_text().strip() for p in phones_els]
                            broker_info['Phones'] = phones
                            
                            # Emails
                            email_els = broker_el.query_selector_all('a[href^="mailto:"]')
                            emails = [e.inner_text().strip() for e in email_els]
                            broker_info['Emails'] = emails
                            
                            if broker_info:
                                data['Brokers'].append(broker_info)
                                
                        if not data['Brokers']:
                            print("    No brokers found in list.")
                            
                    except Exception as e:
                        print(f"    Error waiting for or parsing modal: {e}")
                else:
                    print("    'Contact For Details' button not found.")
                    
            except Exception as e:
                print(f"    Error handling contact modal: {e}")

            # --- 3. Extra Data: Brochure & Highlights ---
            try:
                # Brochure
                # Look for any link containing text "Brochure"
                brochure_el = self.page.query_selector('a:has-text("Brochure")')
                if brochure_el:
                    b_url = brochure_el.get_attribute('href')
                    if b_url and b_url.startswith('/'):
                        b_url = f"https://www.cbre.com{b_url}"
                    data['Brochure URL'] = b_url
                    print(f"    Found Brochure: {data['Brochure URL']}")
                else:
                    data['Brochure URL'] = "Not Found"

            except Exception as e:
                print(f"    Error extracting brochure: {e}")

            # --- Highlights / Overview / Description ---
            try:
                description_parts = []
                
                # Helper to find section by text and collect siblings
                def get_section_text(headers_to_match):
                    found_text = []
                    # Find all headers (h1-h6, div, p)
                    elements = self.page.query_selector_all('h1, h2, h3, h4, div, p')
                    for i, el in enumerate(elements):
                        try:
                            txt = el.inner_text().strip().lower()
                            if any(m in txt for m in headers_to_match):
                                # Found a header. Get next few siblings of the PARENT to capture the content
                                parent = el.evaluate_handle('el => el.parentElement')
                                if parent:
                                    # Get all text from the parent, but skip the header text itself to avoid duplication
                                    full_txt = parent.inner_text()
                                    header_txt = el.inner_text()
                                    content = full_txt.replace(header_txt, "").strip()
                                    if content:
                                        found_text.append(f"{txt.capitalize()}:\n{content}")
                        except:
                            continue
                    return found_text

                # 1. Try to find "Highlights" and "Overview"
                description_parts.extend(get_section_text(['highlights', 'overview']))

                # 2. Try standard class names for Overview/Description as fallback
                desc_el = self.page.query_selector('.cbre-c-pd-overview__description') or \
                          self.page.query_selector('.cbre-c-text-media__description') or \
                          self.page.query_selector('div[class*="description"]') or \
                          self.page.query_selector('.cbre-c-pd-description')
                
                if desc_el:
                     description_parts.append(desc_el.inner_text().strip())

                # Final Join
                data['Description'] = "\n\n".join(list(dict.fromkeys(description_parts))) # Dedup preserving order
                print(f"    Extracted Description Length: {len(data['Description'])}")
                
            except Exception as e:
                print(f"    Error extracting description: {e}")

            # --- Address Fix ---
            if not data['Address']:
                try:
                    # Try to find address in another location
                    addr_alt = self.page.query_selector('.cbre-c-pd-hero__address') or \
                               self.page.query_selector('.address') or \
                               self.page.query_selector('[itemprop="address"]')
                    if addr_alt:
                        data['Address'] = addr_alt.inner_text().strip()
                except:
                    pass

        except Exception as e:
            print(f"Error scraping property {property_url}: {e}")
            
        # Upsert to Pinecone property index
        if self.vector_db:
            self.vector_db.upsert_property(data)
            
        return data
