import requests
from bs4 import BeautifulSoup

def check_pagination():
    """Check if pagination is actually working"""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    
    print("Checking if pagination returns different speakers...\n")
    
    speakers_by_page = {}
    
    for page in [1, 2, 3]:
        url = f"https://www.bigspeak.com/keynote-speakers/?page={page}"
        print(f"Fetching page {page}: {url}")
        
        try:
            response = requests.get(url, headers=headers, timeout=30)
            print(f"Status code: {response.status_code}")
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Find speakers
                speakers_list = soup.find('div', class_='speakers-list')
                if speakers_list:
                    speakers = speakers_list.find_all('div', class_='speaker')
                    
                    page_speakers = []
                    for speaker in speakers[:5]:  # First 5 speakers
                        name_elem = speaker.find('h3')
                        if name_elem:
                            name = name_elem.text.strip()
                            page_speakers.append(name)
                    
                    speakers_by_page[page] = page_speakers
                    print(f"Found {len(speakers)} speakers on page {page}")
                    print(f"First 5: {', '.join(page_speakers)}")
                else:
                    print("No speakers-list found")
            else:
                print(f"Failed with status: {response.status_code}")
                
        except Exception as e:
            print(f"Error: {e}")
        
        print("-" * 50)
    
    # Check if pages have different speakers
    print("\nChecking for differences between pages:")
    if len(speakers_by_page) >= 2:
        page1_speakers = set(speakers_by_page.get(1, []))
        page2_speakers = set(speakers_by_page.get(2, []))
        
        if page1_speakers == page2_speakers:
            print("⚠️  WARNING: Page 1 and Page 2 have the same speakers!")
            print("The pagination parameter might not be working correctly.")
        else:
            print("✓ Pages have different speakers - pagination is working")
            print(f"Unique to page 1: {page1_speakers - page2_speakers}")
            print(f"Unique to page 2: {page2_speakers - page1_speakers}")

if __name__ == "__main__":
    check_pagination()