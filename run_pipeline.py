import argparse
import sys
import os
import uvicorn
import json
import time
from crawler_app.scraper import GenericCrawler
from crawler_app.vector_db import VectorDB

def print_person_summary(p_data):
    """Prints a clean summary of extracted person data."""
    print(f"\n👤 [PERSON FOUND]")
    print(f"NAME: {p_data.get('First Name', '')} {p_data.get('Last Name', '')}")
    print(f"TITLE: {p_data.get('Title', 'N/A')}")
    print(f"EMAIL: {p_data.get('Email', 'N/A')}")
    
    p_num = p_data.get('phone_number', 'N/A')
    m_num = p_data.get('mobile_phoneNumber', 'N/A')
    print(f"PHONE: {p_num}")
    print(f"MOBILE: {m_num}")
    
    loc = p_data.get('Full Address', 'N/A').replace('\n', ', ')
    print(f"LOCATION: {loc}")
    print(f"SPECIALTIES: {p_data.get('Specialties', 'N/A')}")
    if p_data.get('Experience'):
        print(f"EXPERIENCE: {p_data['Experience'][:200].strip()}...")
    print(f"----------------------------------------\n")

def print_property_summary(p_data):
    """Prints a clean summary of extracted property data."""
    print(f"\n✅ [PROPERTY FOUND]")
    print(f"PROPERTY NAME: {p_data.get('Property Name', 'N/A')}")
    print(f"ADDRESS: {p_data.get('Address', 'N/A')}")
    print(f"SQ FT: {p_data.get('SqFt', 'N/A')}")
    print(f"BROCHURE URL: {p_data.get('Brochure URL', 'Not Found')}")
    brokers = p_data.get('Brokers', [])
    if brokers:
        print(f"CONNECTED AGENTS:")
        for b in brokers:
            off = b.get('phone_number')
            mob = b.get('mobile_phoneNumber')
            e_list = b.get('Emails', [])
            contact_str = f"     - {b.get('Name')}"
            if off: contact_str += f" | 📞 Office: {off}"
            if mob: contact_str += f" | 📱 Mobile: {mob}"
            if e_list: contact_str += f" | ✉️ {', '.join(e_list)}"
            print(contact_str)
    else:
        print(f"CONNECTED AGENTS: None found")
    
    if p_data.get('Description'):
        print(f"DESCRIPTION: {p_data['Description'][:300].strip()}...")
    print(f"----------------------------------------\n")

def main():
    parser = argparse.ArgumentParser(description="Run CBRE Scraper Pipeline")
    parser.add_argument("--url", required=True, help="Target URL to scrape (directory or profile)")
    parser.add_argument("--hide-browser", action="store_true", help="Run in headless mode (not recommended for CBRE)")
    parser.add_argument("--mode", choices=['auto', 'person', 'property'], default='auto', help="Force specific scraper mode")
    parser.add_argument("--dry-run", action="store_true", help="Test mode: Do not save to Vector DB")
    parser.add_argument("--limit", type=int, default=None, help="Max items to process (for testing)")
    
    args = parser.parse_args()
    
    print(f"Pipeline started for URL: {args.url}")
    print(f"Mode: {args.mode}")
    print(f"Headless: {args.hide_browser}")
    print(f"Dry Run: {args.dry_run}")
    
    # Initialize Crawler (Default to headed)
    crawler = GenericCrawler(headless=args.hide_browser, disable_vectors=args.dry_run)
    
    # Initialize Vector DB if keys exist and NOT dry run
    vdb = None
    if not args.dry_run and os.getenv("PINECONE_API_KEY") and os.getenv("OPENAI_API_KEY"):
        print("Vector DB keys detected. Initializing...")
        vdb = VectorDB()
    else:
        if args.dry_run:
            print("Dry Run enabled. Vector DB skipped.")
        else:
            print("Vectors DB keys missing. Skipping vectorization.")
    
    try:
        # Determine mode logic
        is_person = False
        is_property = False
        
        if args.mode == 'person':
            is_person = True
        elif args.mode == 'property':
            is_property = True
        else:
            # Auto-detection
            if "/people/" in args.url and not args.url.endswith("/people"):
                is_person = True
            elif "/details/" in args.url: # Explicit details page
                is_property = True
            elif "/properties/" in args.url or "/listings/" in args.url:
                is_property = True
            else:
                pass

        if is_person:
            # Check if it's a directory/search URL despite being in 'person' mode
            # If it's a details page, it's never a directory
            is_directory_url = (
                "/details/" not in args.url and (
                    "#" in args.url or 
                    "?" in args.url or 
                    args.url.rstrip('/').endswith("/people")
                )
            )
            
            if is_directory_url:
                print("detected Person Directory/Search URL.")
                print(f"Gathering profiles from: {args.url}")
                
                # Get list of profiles (Default selectors are for People)
                results = crawler.get_links(args.url, limit=args.limit)
                print(f"--- FOUND {len(results)} PROFILES ---")
                
                # Apply Limit logic used to be here, but now inside get_links too.
                # Double check to be safe or if get_links limit didn't trigger.
                if args.limit and len(results) > args.limit:
                     results = results[:args.limit]

                # Iterate and Scrape
                all_data = []
                for i, res in enumerate(results):
                    profile_url = res.get('URL')
                    if not profile_url: continue
                    
                    print(f"[{i+1}/{len(results)}] Checking Profile: {res.get('Name')} ({profile_url})")
                    
                    # PRE-SCRAPE DUPLICATE CHECK
                    if vdb and vdb.exists(profile_url):
                        print(f"    - Skipping (Already in Vector DB): {profile_url}")
                        continue

                    try:
                        p_data = crawler.scrape_details(profile_url, None, None)
                        all_data.append(p_data)
                        
                        # Detailed Key-Value Summary for UI
                        print_person_summary(p_data)
                        
                        time.sleep(2)
                    except Exception as e:
                        print(f"   > Error scraping profile: {e}")
                
                print("--- BATCH COMPLETE ---")
                # print(json.dumps(all_data, indent=2))
                
            else:
                # Single Profile Mode
                print("Running Single Person Scraper...")
                data = crawler.scrape_details(args.url, None, None)
                print("--- DATA EXTRACTED ---")
                print_person_summary(data)
            
        elif is_property:
             # Check for Property Directory
             # If it's a details page, it's NEVER a directory
             is_prop_directory = (
                 "/details/" not in args.url and (
                     "properties-for-lease" in args.url or 
                     "properties-for-sale" in args.url or
                     "?" in args.url
                 )
             )
             
             if is_prop_directory:
                 print("detected Property Directory/Search URL.")
                 print(f"Gathering properties from: {args.url}")
                 
                 # Property Selectors
                 # Card: .cbre-c-pl-property-card-link
                 # Link: Same as card
                 # Name: .cbre-c-pl-property-card-heading
                 results = crawler.get_links(
                     args.url, 
                     card_selector='.cbre-c-pl-property-card-link',
                     link_selector=None, # Card implies link
                     name_selector='.cbre-c-pl-property-card-heading',
                     limit=args.limit
                 )
                 print(f"--- FOUND {len(results)} PROPERTIES ---")
                 
                 if args.limit and len(results) > args.limit:
                     results = results[:args.limit]

                 all_data = []
                 for i, res in enumerate(results):
                     prop_url = res.get('URL')
                     if not prop_url: continue
                     
                     print(f"[{i+1}/{len(results)}] Checking Property: {res.get('Name')} ({prop_url})")

                     # PRE-SCRAPE DUPLICATE CHECK
                     if vdb and vdb.exists(prop_url):
                         print(f"    - Skipping (Already in Vector DB): {prop_url}")
                         continue

                     try:
                         p_data = crawler.scrape_property(prop_url)
                         all_data.append(p_data)
                         
                         # Detailed Key-Value Summary for UI
                         print_property_summary(p_data)
                         
                         time.sleep(2)
                     except Exception as e:
                         print(f"   > Error scraping property: {e}")

                 print("--- BATCH COMPLETE ---")
                 # print(json.dumps(all_data, indent=2)) # Reduced verbosity

             else:
                print(f"Running Single Property Scraper: {args.url}")
                data = crawler.scrape_property(args.url)
                
                # Detailed Key-Value Summary for UI
                print_property_summary(data)
                
        else:
            # Likely a directory
            print("Running Directory Scraper...")
            # We assume get_links returns a list of URLs or objects
            results = crawler.get_links(args.url, None)
            
            # Apply limit if test mode
            if args.limit and len(results) > args.limit:
                print(f"Applying limit: {args.limit} (found {len(results)})")
                results = results[:args.limit]
                
            print(f"--- FOUND {len(results)} PROFILES (Processing subset as requested) ---")
            
            # If we are in directory mode, we might want to actually scrape the children 
            # if we are testing the full flow.
            # But the 'get_links' might just return links.
            # Let's just print functionality for now unless the user wants recursion.
            # User said "test like x number of properties".
            # If this is a directory, let's scrape the children up to the limit.
            
            for i, res in enumerate(results):
                link = res.get('url') if isinstance(res, dict) else res
                if not link: continue
                
                print(f"[{i+1}/{len(results)}] Scraping child: {link}")
                if "people" in link:
                    p_data = crawler.scrape_details(link, None, None)
                else:
                    p_data = crawler.scrape_property(link)
                    
                print(f"Extracted: {p_data.get('Name') or p_data.get('Property Name')}")
                # Upsert is automatic inside scraper if not dry_run
            
    except Exception as e:
        print(f"Pipeline Error: {e}")
    finally:
        crawler.close_browser()
        print("Pipeline finished.")

if __name__ == "__main__":
    main()
