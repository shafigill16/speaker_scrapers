import requests
from bs4 import BeautifulSoup
import json

def diagnose_page_structure():
    """Diagnose the actual HTML structure of the speakers page"""
    url = "https://www.bigspeak.com/keynote-speakers/"
    
    print(f"Fetching {url}...")
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    
    response = requests.get(url, headers=headers)
    print(f"Status code: {response.status_code}")
    
    if response.status_code != 200:
        print("Failed to fetch page")
        return
    
    soup = BeautifulSoup(response.text, 'html.parser')
    
    print("\n1. Looking for common container elements...")
    # Check for various possible containers
    containers = {
        'divs with class': soup.find_all('div', class_=True)[:5],
        'articles': soup.find_all('article')[:5],
        'sections': soup.find_all('section')[:5],
        'main': soup.find_all('main')[:5]
    }
    
    for name, elements in containers.items():
        if elements:
            print(f"\nFound {len(elements)} {name}:")
            for elem in elements[:2]:
                classes = elem.get('class', [])
                print(f"  - Classes: {classes}")
    
    print("\n2. Looking for speaker names (h1-h6 tags)...")
    for i in range(1, 7):
        headers = soup.find_all(f'h{i}')[:5]
        if headers:
            print(f"\nFound {len(headers)} h{i} tags:")
            for h in headers[:3]:
                text = h.text.strip()[:50]
                print(f"  - {text}...")
    
    print("\n3. Looking for links that might be speaker profiles...")
    links = soup.find_all('a', href=True)
    speaker_links = [a for a in links if '/speakers/' in a.get('href', '')]
    
    if speaker_links:
        print(f"\nFound {len(speaker_links)} potential speaker links:")
        for link in speaker_links[:5]:
            print(f"  - Text: {link.text.strip()}")
            print(f"    Href: {link['href']}")
    
    print("\n4. Looking for speaker-related class names...")
    all_classes = []
    for elem in soup.find_all(class_=True):
        all_classes.extend(elem.get('class', []))
    
    speaker_classes = [c for c in set(all_classes) if any(word in c.lower() for word in ['speaker', 'person', 'profile', 'card'])]
    
    if speaker_classes:
        print(f"\nFound classes with speaker-related names:")
        for cls in sorted(speaker_classes)[:10]:
            print(f"  - {cls}")
    
    # Save a sample of the HTML for manual inspection
    with open('page_sample.html', 'w', encoding='utf-8') as f:
        f.write(soup.prettify()[:10000])  # First 10k chars
    print("\n5. Saved first 10k chars of HTML to page_sample.html for inspection")
    
    # Look for specific patterns
    print("\n6. Looking for fee ranges...")
    fee_patterns = soup.find_all(text=lambda t: t and ('$' in t and ',' in t))
    if fee_patterns:
        print(f"Found {len(fee_patterns)} potential fee ranges:")
        for fee in fee_patterns[:3]:
            print(f"  - {fee.strip()}")

if __name__ == "__main__":
    diagnose_page_structure()