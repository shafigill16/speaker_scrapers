import requests
from bs4 import BeautifulSoup

def check_topics_structure():
    """Check how topics are structured in the HTML"""
    url = "https://www.bigspeak.com/keynote-speakers/"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Find the speakers list
    speakers_list = soup.find('div', class_='speakers-list')
    if not speakers_list:
        print("Could not find speakers list")
        return
    
    # Get first speaker card
    first_speaker = speakers_list.find('div', class_='speaker')
    if not first_speaker:
        print("Could not find any speaker")
        return
    
    print("First speaker card HTML structure:")
    print("=" * 50)
    
    # Look for topics in various ways
    print("\n1. Looking for 'Speaking Topics' text...")
    topics_text = first_speaker.find(string=lambda t: t and 'Speaking Topics' in t)
    if topics_text:
        print(f"Found: {topics_text}")
        parent = topics_text.find_parent()
        print(f"Parent tag: {parent.name}")
        print(f"Parent HTML: {str(parent)[:200]}...")
        
        # Look for siblings
        next_sib = parent.find_next_sibling()
        if next_sib:
            print(f"\nNext sibling: {next_sib.name}")
            print(f"Next sibling content: {str(next_sib)[:200]}...")
    
    print("\n2. Looking for all links in speaker card...")
    all_links = first_speaker.find_all('a')
    for i, link in enumerate(all_links):
        href = link.get('href', '')
        text = link.text.strip()
        if text and 'speakers' not in href and 'availability' not in href:
            print(f"Link {i}: {text} -> {href}")
    
    print("\n3. Looking for spans with topics...")
    spans = first_speaker.find_all('span')
    for span in spans:
        text = span.text.strip()
        if len(text) > 5 and len(text) < 50:  # Reasonable topic length
            print(f"Span: {text}")
    
    # Save the first speaker card for inspection
    with open('first_speaker.html', 'w', encoding='utf-8') as f:
        f.write(first_speaker.prettify())
    print("\nSaved first speaker card to first_speaker.html")

if __name__ == "__main__":
    check_topics_structure()