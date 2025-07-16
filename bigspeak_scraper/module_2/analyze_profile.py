import requests
from bs4 import BeautifulSoup
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from module_1.config import get_collection

def analyze_profile_page():
    """Analyze a speaker profile page to understand available data"""
    
    # Get a sample speaker URL from database
    collection = get_collection()
    sample_speaker = collection.find_one({})
    
    if not sample_speaker:
        print("No speakers found in database")
        return
    
    profile_url = sample_speaker['profile_url']
    print(f"Analyzing profile page for: {sample_speaker['name']}")
    print(f"URL: {profile_url}\n")
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    
    response = requests.get(profile_url, headers=headers)
    if response.status_code != 200:
        print(f"Failed to fetch page: {response.status_code}")
        return
    
    soup = BeautifulSoup(response.text, 'html.parser')
    
    print("=== PROFILE PAGE ANALYSIS ===\n")
    
    # 1. Check for bio/about section
    print("1. Looking for biography/about section...")
    bio_sections = soup.find_all(['div', 'section'], class_=lambda x: x and any(word in str(x).lower() for word in ['bio', 'about', 'description']))
    if bio_sections:
        print(f"Found {len(bio_sections)} potential bio sections")
        for section in bio_sections[:2]:
            text = section.text.strip()[:200]
            print(f"  - {text}...")
    
    # 2. Check for full description
    print("\n2. Looking for main content area...")
    main_content = soup.find('main') or soup.find('div', class_='content') or soup.find('article')
    if main_content:
        paragraphs = main_content.find_all('p')
        print(f"Found {len(paragraphs)} paragraphs in main content")
        if paragraphs:
            print(f"First paragraph: {paragraphs[0].text.strip()[:150]}...")
    
    # 3. Check for videos
    print("\n3. Looking for videos...")
    videos = soup.find_all(['iframe', 'video'])
    youtube_embeds = [v for v in videos if v.get('src') and 'youtube' in v.get('src', '')]
    if youtube_embeds:
        print(f"Found {len(youtube_embeds)} YouTube videos")
        for video in youtube_embeds[:2]:
            print(f"  - {video.get('src')}")
    
    # 4. Check for social media links
    print("\n4. Looking for social media links...")
    social_links = []
    for link in soup.find_all('a', href=True):
        href = link['href']
        if any(social in href for social in ['twitter.com', 'linkedin.com', 'facebook.com', 'instagram.com']):
            social_links.append(href)
    
    if social_links:
        print(f"Found {len(social_links)} social media links:")
        for link in set(social_links):
            print(f"  - {link}")
    
    # 5. Check for expertise/topics in detail
    print("\n5. Looking for detailed expertise/topics...")
    expertise_keywords = ['expertise', 'topics', 'speaks about', 'specializes']
    for keyword in expertise_keywords:
        elements = soup.find_all(string=lambda t: t and keyword in t.lower())
        if elements:
            print(f"Found '{keyword}' mentions: {len(elements)}")
    
    # 6. Check for testimonials
    print("\n6. Looking for testimonials...")
    testimonial_sections = soup.find_all(['div', 'section'], class_=lambda x: x and 'testimonial' in str(x).lower())
    quotes = soup.find_all('blockquote')
    print(f"Found {len(testimonial_sections)} testimonial sections and {len(quotes)} blockquotes")
    
    # 7. Check for books/publications
    print("\n7. Looking for books/publications...")
    book_keywords = ['book', 'author', 'publication', 'bestseller']
    book_mentions = []
    for keyword in book_keywords:
        elements = soup.find_all(string=lambda t: t and keyword in t.lower())
        book_mentions.extend(elements)
    print(f"Found {len(book_mentions)} book-related mentions")
    
    # 8. Check for awards/achievements
    print("\n8. Looking for awards/achievements...")
    award_keywords = ['award', 'recognition', 'achievement', 'honor', 'acclaimed']
    award_mentions = []
    for keyword in award_keywords:
        elements = soup.find_all(string=lambda t: t and keyword in t.lower())
        award_mentions.extend(elements[:2])
    print(f"Found {len(award_mentions)} award-related mentions")
    
    # 9. Check for high-res images
    print("\n9. Looking for high-resolution images...")
    images = soup.find_all('img')
    speaker_images = [img for img in images if img.get('src') and any(name_part.lower() in img.get('src', '').lower() for name_part in sample_speaker['name'].split())]
    print(f"Found {len(speaker_images)} potential speaker images")
    for img in speaker_images[:3]:
        print(f"  - {img.get('src')}")
    
    # Save sample HTML for inspection
    with open('sample_profile.html', 'w', encoding='utf-8') as f:
        f.write(soup.prettify()[:20000])
    print("\n10. Saved first 20k chars of profile HTML to sample_profile.html")

if __name__ == "__main__":
    analyze_profile_page()