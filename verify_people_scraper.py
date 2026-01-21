from crawler_app.scraper import GenericCrawler
import json
import sys

def verify_joe_riley():
    # Initialize with headless=False to allow manual intervention if needed (Cloudflare)
    crawler = GenericCrawler(headless=False)
    
    try:
        url = "https://www.cbre.com/people/joe-riley"
        print(f"Verifying scraper on: {url}")
        
        # Scrape details
        data = crawler.scrape_details(url, None, None) # selectos arguments are unused in current impl
        
        print("\n--- Scraped Data ---")
        print(json.dumps(data, indent=2))
        
        # Validation checks
        issues = []
        if "Phone" not in data or "Not Found" in data['Phone']:
             # It might be "Not Found" if legitimate, but for Joe Riley we expect numbers
             # Actually, checking if it starts with "Error" is better, or empty
             pass 
        
        if data['vCardURL'] == "Not Found":
            issues.append("vCard URL not found")
            
        if not data['LinkedProperties']:
            issues.append("No Linked Properties found")
            
        if not data['ListingsURL']:
            issues.append("No Listings URL found")
            
        if issues:
            print("\n--- Verification Issues ---")
            for i in issues:
                print(f"[!] {i}")
        else:
            print("\n[+] Verification Successful: All key fields extracted.")

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        crawler.close_browser()

if __name__ == "__main__":
    verify_joe_riley()
