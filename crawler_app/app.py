import streamlit as st
import pandas as pd
from scraper import GenericCrawler
import time

st.set_page_config(page_title="Web Data Crawler", layout="wide")

st.title("🕷️ Web Data Crawler")
st.markdown("""
This tool crawls a directory page to extract details from individual profiles.
""")

# Sidebar for configuration
st.sidebar.header("Configuration")
target_url = st.sidebar.text_input("Directory URL", placeholder="https://example.com/directory")

# Auto-configure for known sites
default_link_sel = ""
default_phone_sel = ""
default_exp_sel = ""

if "cbre.com" in target_url:
    st.sidebar.success("✅ CBRE Website Detected! Auto-filling selectors.")
    default_link_sel = ".cbre-c-listCards__title-link.CoveoResultLink"
    default_phone_sel = "a.cbre-c-contactInfo__link[aria-label='Phone']"
    default_exp_sel = "div.cbre-c-inlineBodyCard__card"

st.sidebar.subheader("CSS Selectors")
link_selector = st.sidebar.text_input("Profile Link Selector", value=default_link_sel, placeholder="e.g., .profile-card a")
phone_selector = st.sidebar.text_input("Phone Number Selector", value=default_phone_sel, placeholder="e.g., .contact-info .phone")
experience_selector = st.sidebar.text_input("Experience Selector", value=default_exp_sel, placeholder="e.g., .experience-section")

# Initialize crawler
show_browser = st.sidebar.checkbox("👀 Show Browser (Watch it work)", value=True, help="Uncheck this to run faster in the background.")

# Stop button logic
if "stop_crawl" not in st.session_state:
    st.session_state.stop_crawl = False

def stop_callback():
    st.session_state.stop_crawl = True

# Restart logic
if st.sidebar.button("🔄 Reset / Restart App"):
    st.session_state.stop_crawl = False
    # Clear any other session state if needed, though mostly we just want to re-run
    st.rerun()

def stop_callback():
    st.session_state.stop_crawl = True

if st.button("Start Crawling"):
    st.session_state.stop_crawl = False # Reset stop flag
    
    # Re-init crawler with user preference
    crawler = GenericCrawler(headless=not show_browser)
    
    if not target_url:
        st.error("Please enter a Directory URL.")
    elif not link_selector:
        st.error("Please enter a Link Selector so we know which profiles to visit.")
    else:
        st.write("### Step 1: Finding Profiles...")
        
        status_container = st.empty()
        status_container.info(f"Scanning {target_url}...")
        
        # Start browser once
        crawler.start_browser()
        
        try:
            links_data = crawler.get_links(target_url, link_selector)
            
            if links_data:
                st.success(f"✅ Found {len(links_data)} profiles.")
                
                st.write("### Step 2: Extracting Data Live")
                # Show Stop Button 
                st.button("⛔ Stop Crawling", on_click=stop_callback, key="stop_btn")
                
                progress_bar = st.progress(0)
                
                # Create a placeholder for the dataframe so it updates live
                table_placeholder = st.empty()
                results = []
                
                for i, item in enumerate(links_data):
                    # Check for stop
                    if st.session_state.stop_crawl:
                        st.warning("🛑 Crawl stopped by user.")
                        break

                    name = item['Name']
                    link = item['URL']

                    # Update status
                    status_container.text(f"👉 Processing ({i+1}/{len(links_data)}): {name}")
                    
                    if link:
                        # Scrape
                        data = crawler.scrape_details(link, phone_selector, experience_selector)
                    else:
                        # Fallback for profiles without detail links
                        data = {
                            'URL': 'No Profile Page', 
                            'First Name': name.split(" ", 1)[0] if name else "Unknown", 
                            'Last Name': name.split(" ", 1)[1] if (name and " " in name) else "",
                            'Phone': 'Visit Search Card', 
                            'Address Line': '',
                            'City': '',
                            'State': '',
                            'Zip': '',
                            'Full Address': 'No Detail Page Available',
                            'Experience': 'N/A'
                        }
                    
                    results.append(data)
                    
                    # Update table immediately
                    current_df = pd.DataFrame(results)
                    table_placeholder.dataframe(current_df, width="stretch")
                    
                    progress_bar.progress((i + 1) / len(links_data))
                else:
                    status_container.success("🎉 Crawl Complete!")
                
                # Final CSV Download
                csv = pd.DataFrame(results).to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="Download data as CSV",
                    data=csv,
                    file_name='crawl_results.csv',
                    mime='text/csv',
                )
            else:
                st.warning("No profiles found. Please check your **Profile Link Selector**.")
        
        finally:
            # Always close the browser
            crawler.close_browser()


st.markdown("---")
with st.expander("Help: How to get CSS Selectors"):
    st.markdown("""
    1. Open the website in Chrome or Firefox.
    2. Right-click the element you want (e.g., the link to a profile).
    3. Select **Inspect**.
    4. In the Elements panel, look at the class name or tag.
    5. Example: If the link is `<a class="user-link" href="...">`, the selector is `.user-link`.
    """)
