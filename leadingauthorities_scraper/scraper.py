import requests
from bs4 import BeautifulSoup
import pymongo
import sys
import json
from urllib.parse import urljoin
import re
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# --- Configuration ---
MONGO_URI = os.getenv("MONGO_URI")
PROXY = {
    "http": os.getenv("PROXY_HTTP"),
    "https": os.getenv("PROXY_HTTPS")
}
BASE_URL = os.getenv("BASE_URL", "https://www.leadingauthorities.com")
DB_NAME = os.getenv("DB_NAME", "leading_authorities")
COLLECTION_NAME = os.getenv("COLLECTION_NAME", "speakers_final_details")
TOTAL_PAGES = int(os.getenv("TOTAL_PAGES", "103"))

def get_db_collection():
    """Establishes a connection to MongoDB and returns the collection object."""
    try:
        client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.server_info()
        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]
        print("Successfully connected to MongoDB.")
        return collection
    except pymongo.errors.ConnectionFailure as e:
        print(f"Error: Could not connect to MongoDB. {e}")
        sys.exit(1)

def scrape_speaker_page(speaker_url, session):
    """Scrapes all specified details from an individual speaker's page."""
    try:
        response = session.get(speaker_url, proxies=PROXY, timeout=30)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch speaker page {speaker_url}. Error: {e}")
        return None

    soup = BeautifulSoup(response.content, 'html.parser')
    speaker_data = {'speaker_page_url': speaker_url}

    # --- Primary Details from JSON-LD (Most Reliable Method) ---
    json_ld_script = soup.find('script', type='application/ld+json')
    if json_ld_script:
        try:
            data = json.loads(json_ld_script.string)
            if '@graph' in data and data['@graph']:
                person_data = next((item for item in data['@graph'] if item.get('@type') == 'Person'), None)
                if person_data:
                    speaker_data['name'] = person_data.get('name', '').strip()
                    speaker_data['job_title'] = person_data.get('jobTitle', '').strip()
                    speaker_data['description'] = person_data.get('description', '').strip()
                    speaker_data['speaker_image_url'] = person_data.get('image')
                    speaker_data['speaker_website'] = person_data.get('url')
                    same_as_links = person_data.get('sameAs', [])
                    if isinstance(same_as_links, list):
                        socials = {
                            'twitter': next((url for url in same_as_links if 'twitter.com' in url), None),
                            'linkedin': next((url for url in same_as_links if 'linkedin.com' in url), None),
                            'facebook': next((url for url in same_as_links if 'facebook.com' in url), None),
                            'youtube': next((url for url in same_as_links if 'youtube.com' in url), None),
                            'podcasts': next((url for url in same_as_links if 'podcast' in url), None),
                        }
                        speaker_data['social_media'] = {k: v for k, v in socials.items() if v}
        except (json.JSONDecodeError, KeyError, StopIteration):
            print(f"    - Could not fully parse JSON-LD for {speaker_url}. Using HTML fallbacks.")

    # --- Fallback and Additional HTML Parsing (Based on paste.txt structure) ---
    if not speaker_data.get('name'):
        name_tag = soup.select_one('.speaker-title h1, div.speaker-hero--title h1')
        speaker_data['name'] = name_tag.text.strip() if name_tag else 'N/A'
    if not speaker_data.get('job_title'):
        job_title_tag = soup.select_one('.speaker_brand_dec, div.speaker-hero--tagline')
        speaker_data['job_title'] = job_title_tag.text.strip() if job_title_tag else 'N/A'
    if not speaker_data.get('description'):
        desc_tag = soup.select_one('.profile-description')
        speaker_data['description'] = desc_tag.get_text(separator='\n', strip=True) if desc_tag else 'N/A'
    if not speaker_data.get('speaker_image_url'):
        img_tag = soup.select_one('.speaker-profile-image img, div.speaker-hero--photo img')
        if img_tag and img_tag.get('src'):
            speaker_data['speaker_image_url'] = urljoin(BASE_URL, img_tag['src'])

    # --- Profile Menu Links (Speaker Specific) ---
    profile_menu = soup.select_one('.profile-section-menu-wrapper')
    if profile_menu:
        download_profile_link = profile_menu.select_one('a[href*="/print/view/pdf/speaker/bio"]')
        speaker_data['download_profile_link'] = urljoin(BASE_URL, download_profile_link['href']) if download_profile_link else 'Not Available'
        if not speaker_data.get('speaker_website'):
            website_link = profile_menu.select_one('a:-soup-contains("Website")')
            if website_link: speaker_data['speaker_website'] = website_link.get('href')
        if not speaker_data.get('social_media'):
            socials = {}
            twitter_link = profile_menu.select_one('a[href*="twitter.com"]')
            if twitter_link: socials['twitter'] = twitter_link['href']
            speaker_data['social_media'] = socials

    # --- Speaking Topics (Advanced Parsing for Paragraphs) ---
    topics_list = []
    topics_container = soup.select_one('.speaker-topics-description .topics-panel-wrapper')
    if topics_container:
        current_topic = {}
        for p_tag in topics_container.find_all('p'):
            strong_tag = p_tag.find('strong')
            if strong_tag:
                if current_topic: # Save the previous topic
                    topics_list.append(current_topic)
                title = strong_tag.get_text(strip=True)
                strong_tag.extract() # Remove title to get rest of description
                current_topic = {"title": title, "description": p_tag.get_text(strip=True)}
            elif current_topic:
                current_topic['description'] += '\n' + p_tag.get_text(strip=True)
        if current_topic: # Append the last topic
            topics_list.append(current_topic)
    speaker_data['topics'] = topics_list
    download_topic_tag = soup.select_one('.speaker-topics-link a[href*="/print/view/pdf/speaker/topic"]')
    if download_topic_tag:
        speaker_data['download_topics_link'] = urljoin(BASE_URL, download_topic_tag['href'])

    # --- Videos ---
    videos = []
    video_elements = soup.select('div.sp-video__thumbs-item')
    for el in video_elements:
        vid = el.get('data-vid')
        title = el.get('data-videotitle')
        video_page_url = el.get('data-videourl')
        style = el.select_one('div.thumb').get('style', '')
        thumb_url_match = re.search(r"url\('?([^'\"\)]+)'?\)", style)
        thumbnail_url = urljoin(BASE_URL, thumb_url_match.group(1)) if thumb_url_match else 'N/A'
        if title and vid:
            videos.append({
                "title": title.strip(), "video_id": vid,
                "video_page_url": urljoin(BASE_URL, video_page_url) if video_page_url else 'N/A',
                "thumbnail_url": thumbnail_url
            })
    speaker_data['videos'] = videos

    # --- Speaker Fees ---
    fees = {}
    fee_elements = soup.select('ul.fee-structure li')
    for item in fee_elements:
        location_tag = item.select_one('p:nth-of-type(1)')
        fee_tag = item.select_one('p:nth-of-type(2)')
        if location_tag and fee_tag:
            location = location_tag.text.strip().replace(':', '')
            fees[location] = fee_tag.text.strip()
    speaker_data['speaker_fees'] = fees

    # --- Books / Related Publications ---
    publications = []
    # Selector for books
    book_elements = soup.select('.latest-book-list')
    for book in book_elements:
        title_tag = book.select_one('.latest-book-list-title h2')
        img_tag = book.select_one('.latest-book-list-img img')
        publications.append({
            'title': title_tag.text.strip() if title_tag else 'N/A',
            'url': book.get('href', 'N/A'),
            'image_url': urljoin(BASE_URL, img_tag['src']) if img_tag and img_tag.has_attr('src') else 'N/A'
        })
    # Selector for other related links/articles (from paste.txt)
    small_image_links = soup.select('.speaker-small-images ul li a')
    for link in small_image_links:
        img_tag = link.find('img')
        publications.append({
            'title': img_tag.get('alt', 'N/A') if img_tag else 'N/A',
            'url': urljoin(BASE_URL, link.get('href', 'N/A')),
            'image_url': urljoin(BASE_URL, img_tag['src']) if img_tag and img_tag.has_attr('src') else 'N/A'
        })
    speaker_data['books_and_publications'] = publications

    # --- Topics & Types Categories ---
    topics_and_types = [
        {'name': item.text.strip(), 'url': urljoin(BASE_URL, item['href'])}
        for item in soup.select('.topics-types-section .links--item a')
    ]
    speaker_data['topics_and_types'] = topics_and_types
    
    # --- Recent News ---
    recent_news = [
        {"title": post.select_one('h2').text.strip(), "url": urljoin(BASE_URL, post.select_one('a')['href'])}
        for post in soup.select('.recent-news-block .news-box') if post.select_one('a') and post.select_one('h2')
    ]
    speaker_data['recent_news'] = recent_news
    
    # --- Client Testimonials ---
    client_testimonials = []
    testimonial_elements = soup.select('.testimonial-block .testimonials--item, .testimonial-block .swiper-slide')
    for item in testimonial_elements:
        quote_tag = item.select_one('blockquote, div > div')
        author_tag = item.select_one('.testimonial-bottom-text')
        if quote_tag and author_tag:
            client_testimonials.append({
                'quote': quote_tag.get_text(strip=True),
                'author': author_tag.get_text(strip=True).replace('|', ', ').strip()
            })
    speaker_data['client_testimonials'] = client_testimonials
    
    return speaker_data

def main():
    """Main function to orchestrate the scraping process."""
    collection = get_db_collection()
    session = requests.Session()

    print(f"Starting to scrape {TOTAL_PAGES} pages from {BASE_URL}/speaker-search")

    for page_num in range(TOTAL_PAGES):
        search_url = f"{BASE_URL}/speaker-search?page={page_num}"
        print(f"\n--- Scraping Page {page_num + 1}/{TOTAL_PAGES} ---")

        try:
            response = session.get(search_url, proxies=PROXY, timeout=30)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"Failed to fetch search page {page_num + 1}. Error: {e}. Skipping page.")
            continue

        soup = BeautifulSoup(response.content, 'html.parser')
        speaker_items = soup.select('div.speaker-grid--item h2.speaker-grid--title a, a.view-profile--btn')
        
        if not speaker_items:
            print(f"No speaker links found on page {page_num + 1}. It might be the end.")
            break

        processed_urls = set()
        for item in speaker_items:
            relative_url = item.get('href')
            if not relative_url:
                continue
            
            speaker_url = urljoin(BASE_URL, relative_url)
            if speaker_url in processed_urls:
                continue
            processed_urls.add(speaker_url)
            
            if collection.count_documents({'speaker_page_url': speaker_url}) > 0:
                print(f"  Skipping already scraped speaker: {speaker_url}")
                continue

            print(f"  Fetching details for: {speaker_url}")
            speaker_details = scrape_speaker_page(speaker_url, session)

            if speaker_details:
                try:
                    collection.update_one(
                        {'speaker_page_url': speaker_url},
                        {'$set': speaker_details},
                        upsert=True
                    )
                    print(f"    -> Saved '{speaker_details.get('name', 'N/A')}' to MongoDB.")
                except Exception as e:
                    print(f"    -> ERROR: Could not save data to MongoDB. {e}")

    print("\n--- Scraping process completed. ---")

if __name__ == "__main__":
    main()
