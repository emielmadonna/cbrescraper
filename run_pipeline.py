import argparse
import sys
import os
import uvicorn
import json
import time
from crawler_app.scraper import GenericCrawler
from crawler_app.vector_db import VectorDB

def main():
    parser = argparse.ArgumentParser(description="Run CBRE Scraper Pipeline")
    parser.add_argument("--url", required=True, help="Target URL to scrape (directory or profile)")
    parser.add_argument("--show-browser", action="store_true", help="Run with visible browser")
    parser.add_argument("--mode", choices=['auto', 'person', 'property'], default='auto', help="Force specific scraper mode")
    parser.add_argument("--dry-run", action="store_true", help="Test mode: Do not save to Vector DB")
    parser.add_argument("--limit", type=int, default=None, help="Max items to process (for testing)")
    
    args = parser.parse_args()
    
    print(f"Pipeline started for URL: {args.url}")
    print(f"Mode: {args.mode}")
    print(f"Headless: {not args.show_browser}")
    print(f"Dry Run: {args.dry_run}")
    
    # Initialize Crawler
    crawler = GenericCrawler(headless=not args.show_browser, disable_vectors=args.dry_run)
    
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
            elif "/properties/" in args.url or "/listings/" in args.url:
                is_property = True
            else:
                pass

        if is_person:
            # Check if it's a directory/search URL despite being in 'person' mode
            is_directory_url = (
                "#" in args.url or 
                "?" in args.url or 
                args.url.rstrip('/').endswith("/people")
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
                    
                    print(f"[{i+1}/{len(results)}] Scraping Profile: {res.get('Name')} ({profile_url})")
                    try:
                        p_data = crawler.scrape_details(profile_url, None, None)
                        all_data.append(p_data)
                        
                        # Detailed Key-Value Summary for UI
                        print(f"\n👤 [PERSON FOUND]")
                        print(f"NAME: {p_data.get('First Name')} {p_data.get('Last Name')}")
                        print(f"TITLE: {p_data.get('Title', 'N/A')}")
                        print(f"EMAIL: {p_data.get('Email', 'N/A')}")
                        print(f"PHONE: {p_data.get('Phone', 'N/A')}")
                        loc = p_data.get('Full Address', 'N/A').replace('\n', ', ')
                        print(f"LOCATION: {loc}")
                        if p_data.get('Experience'):
                            print(f"EXPERIENCE: {p_data['Experience'][:200].strip()}...")
                        print(f"----------------------------------------\n")
                        
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
                print(json.dumps(data, indent=2))
            
        elif is_property:
             # Check for Property Directory
             is_prop_directory = (
                 "properties-for-lease" in args.url or 
                 "properties-for-sale" in args.url or
                 "?" in args.url
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
                     
                     print(f"[{i+1}/{len(results)}] Scraping Property: {res.get('Name')} ({prop_url})")
                     try:
                         p_data = crawler.scrape_property(prop_url)
                         all_data.append(p_data)
                         
                         # Detailed Key-Value Summary for UI
                         print(f"\n✅ [PROPERTY FOUND]")
                         print(f"PROPERTY NAME: {p_data.get('Property Name', 'N/A')}")
                         print(f"ADDRESS: {p_data.get('Address', 'N/A')}")
                         print(f"BROCHURE URL: {p_data.get('Brochure URL', 'Not Found')}")
                         brokers = p_data.get('Brokers', [])
                         if brokers:
                             print(f"CONNECTED AGENTS:")
                             for b in brokers:
                                 contact_str = f"     - {b.get('Name')}"
                                 if b.get('Phones'): contact_str += f" | 📞 {'/'.join(b.get('Phones'))}"
                                 if b.get('Emails'): contact_str += f" | ✉️ {'/'.join(b.get('Emails'))}"
                                 print(contact_str)
                         else:
                             print(f"CONNECTED AGENTS: None found")
                         
                         if p_data.get('Description'):
                             print(f"DESCRIPTION: {p_data['Description'][:300].strip()}...")
                         print(f"----------------------------------------\n")
                         
                         time.sleep(2)
                     except Exception as e:
                         print(f"   > Error scraping property: {e}")

                 print("--- BATCH COMPLETE ---")
                 # print(json.dumps(all_data, indent=2)) # Reduced verbosity

             else:
                print("\n✅ [PROPERTY FOUND]")
                print(f"PROPERTY NAME: {data.get('Property Name', 'N/A')}")
                print(f"ADDRESS: {data.get('Address', 'N/A')}")
                print(f"BROCHURE URL: {data.get('Brochure URL', 'Not Found')}")
                
                brokers = data.get('Brokers', [])
                broker_names = [b.get('Name') for b in brokers if b.get('Name')]
                print(f"CONNECTED AGENTS: {', '.join(broker_names) if broker_names else 'None found'}")
                
                if data.get('Description'):
                    print(f"DESCRIPTION: {data['Description'][:200]}...")
                print(f"----------------------------------------\n")
                
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
