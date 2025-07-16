import requests
from bs4 import BeautifulSoup
import re

def find_pagination_mechanism():
    """Find the correct pagination mechanism"""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    
    url = "https://www.bigspeak.com/keynote-speakers/"
    print(f"Analyzing pagination at: {url}\n")
    
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Look for pagination elements
    print("1. Looking for pagination elements...")
    
    # Common pagination class names
    pagination_classes = ['pagination', 'paging', 'page-numbers', 'wp-pagenavi', 'nav-links']
    
    for cls in pagination_classes:
        pag_elem = soup.find(['div', 'nav', 'ul'], class_=cls)
        if pag_elem:
            print(f"\nFound pagination element with class '{cls}':")
            print(f"HTML: {str(pag_elem)[:200]}...")
            
            # Find all links in pagination
            links = pag_elem.find_all('a', href=True)
            for link in links[:5]:
                print(f"  Link: {link.text.strip()} -> {link['href']}")
    
    # Look for any links with page numbers
    print("\n2. Looking for page number links...")
    all_links = soup.find_all('a', href=True)
    page_links = []
    
    for link in all_links:
        href = link['href']
        # Check various pagination patterns
        if any(pattern in href for pattern in ['/page/', 'paged=', 'p=', 'pg=']):
            if link.text.strip() and link.text.strip().isdigit():
                page_links.append((link.text.strip(), href))
    
    if page_links:
        print("Found page number links:")
        for text, href in page_links[:10]:
            print(f"  Page {text}: {href}")
    
    # Check for AJAX/JavaScript pagination
    print("\n3. Checking for JavaScript/AJAX pagination...")
    scripts = soup.find_all('script')
    for script in scripts:
        if script.string and 'page' in script.string.lower():
            if 'ajax' in script.string.lower() or 'load' in script.string.lower():
                print("Found potential AJAX pagination script")
                break
    
    # Look for data attributes that might indicate pagination
    print("\n4. Looking for data attributes...")
    elements_with_data = soup.find_all(attrs={"data-page": True})
    if elements_with_data:
        print(f"Found {len(elements_with_data)} elements with data-page attribute")
    
    # Check the actual URL structure of "View More" or similar buttons
    print("\n5. Checking for 'Load More' or 'View More' buttons...")
    load_more = soup.find_all(['button', 'a'], string=re.compile(r'(Load More|View More|Show More|Next)', re.I))
    for btn in load_more:
        print(f"Found: {btn.text.strip()}")
        if btn.get('href'):
            print(f"  URL: {btn['href']}")
        if btn.get('onclick'):
            print(f"  OnClick: {btn['onclick']}")

if __name__ == "__main__":
    find_pagination_mechanism()