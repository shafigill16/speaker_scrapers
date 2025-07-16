import requests
from bs4 import BeautifulSoup
import pymongo
import sys
import json
import re
import time
from urllib.parse import urljoin
from datetime import datetime
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# --- Configuration ---
# MongoDB Configuration
MONGO_USERNAME = os.getenv('MONGO_USERNAME')
MONGO_PASSWORD = os.getenv('MONGO_PASSWORD')
MONGO_HOST = os.getenv('MONGO_HOST')
MONGO_PORT = os.getenv('MONGO_PORT')
MONGO_AUTH_SOURCE = os.getenv('MONGO_AUTH_SOURCE')
MONGO_URI = f"mongodb://{MONGO_USERNAME}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/?authSource={MONGO_AUTH_SOURCE}"

# Proxy Configuration
PROXY_USERNAME = os.getenv('PROXY_USERNAME')
PROXY_PASSWORD = os.getenv('PROXY_PASSWORD')
PROXY_HOST = os.getenv('PROXY_HOST')
PROXY_PORT = os.getenv('PROXY_PORT')
PROXY = {
    "http": f"http://{PROXY_USERNAME}:{PROXY_PASSWORD}@{PROXY_HOST}:{PROXY_PORT}",
    "https": f"http://{PROXY_USERNAME}:{PROXY_PASSWORD}@{PROXY_HOST}:{PROXY_PORT}"
}

# Scraper Configuration
BASE_URL = os.getenv('BASE_URL', 'https://www.a-speakers.com')
DB_NAME = os.getenv('DB_NAME', 'a_speakers')
COLLECTION_NAME = os.getenv('COLLECTION_NAME', 'speakers')

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
    """Scrapes detailed information from an individual speaker's page on A-Speakers."""
    try:
        response = session.get(speaker_url, proxies=PROXY, timeout=30)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch speaker page {speaker_url}. Error: {e}")
        return None

    soup = BeautifulSoup(response.content, 'html.parser')
    speaker_data = {'url': speaker_url}

    # Extract JSON-LD structured data
    json_ld_scripts = soup.find_all('script', type='application/ld+json')
    for script in json_ld_scripts:
        try:
            data = json.loads(script.string)
            if data.get('@type') == 'Person':
                speaker_data['name'] = data.get('name', 'N/A')
                speaker_data['job_title'] = data.get('jobTitle', 'N/A')
                speaker_data['description'] = data.get('description', 'N/A')
                speaker_data['image_url'] = data.get('image', 'N/A')
                break
        except:
            pass

    # Extract main heading if not in JSON-LD
    if 'name' not in speaker_data or speaker_data['name'] == 'N/A':
        h1_tag = soup.select_one('h1')
        speaker_data['name'] = h1_tag.text.strip() if h1_tag else "N/A"
    
    # Extract meta tag information
    meta_name = soup.find('meta', {'itemprop': 'name'})
    if meta_name and 'name' not in speaker_data:
        speaker_data['name'] = meta_name.get('content', 'N/A')

    # Extract job title from hero section
    job_title_elem = soup.select_one('.profile-hero .rte-content')
    if job_title_elem:
        speaker_data['job_title'] = job_title_elem.get_text(strip=True)
    
    # Extract location (visible in hero section)
    location_elem = soup.find('svg', class_=lambda c: c and ('h-16' in str(c) or 'w-13' in str(c)))
    if location_elem and location_elem.parent:
        location_text = location_elem.parent.get_text(strip=True)
        if location_text and location_text != '':
            speaker_data['location'] = location_text

    # Extract "Why you should book" section with bullet points
    why_book_section = soup.find('article', id='profile-usp')
    if why_book_section:
        # The ul might have different structure on different pages
        # First try the standard selector
        ul = why_book_section.find('ul', {'role': 'list'})
        if not ul:
            # Try alternative selectors
            ul = why_book_section.find('ul')
        
        why_book_points = []
        
        if ul:
            # Get all li elements
            bullet_points = ul.find_all('li')
            
            for li in bullet_points:
                # Get all text from the li, then clean it up
                li_text = li.get_text(separator=' ', strip=True)
                
                # Remove the "h-12" text that comes from the SVG title
                li_text = li_text.replace('h-12', '').strip()
                
                # Only add if it's substantial text
                if li_text and len(li_text) > 30:
                    why_book_points.append(li_text)
        
        if why_book_points:
            speaker_data['why_book_points'] = why_book_points
    
    # Extract biography section
    bio_section = soup.find('article', id='profile-biography')
    if bio_section:
        # Extract main bio content, excluding any h- class text
        bio_contents = bio_section.select('.rte-content')
        if bio_contents:
            full_bio_parts = []
            for content in bio_contents:
                # Remove any elements that just contain CSS classes like "h-12"
                for elem in content.find_all(text=lambda t: t and re.match(r'^h-\d+$', t.strip())):
                    elem.extract()
                text = content.get_text(separator='\n', strip=True)
                if text and not text.startswith('h-'):
                    full_bio_parts.append(text)
            speaker_data['full_bio'] = '\n\n'.join(full_bio_parts)

    # Extract topics
    topics_section = soup.find('article', class_='profile-topics')
    if topics_section:
        # Look for topic links
        topic_links = topics_section.select('ul li a')
        if topic_links:
            topics = []
            for link in topic_links:
                topic_text = link.get_text(strip=True)
                if topic_text:
                    topics.append(topic_text)
            speaker_data['topics'] = topics
        else:
            speaker_data['topics'] = []
    else:
        speaker_data['topics'] = []

    # Extract keynotes
    keynotes = []
    keynotes_section = soup.find('article', class_='profile-keynotes')
    if keynotes_section:
        # Find individual keynote articles
        keynote_articles = keynotes_section.select('article[id*="keynote"], article.grid')
        for article in keynote_articles:
            keynote = {}
            
            # Extract title
            title_elem = article.select_one('h3 span.text-base-xl-600')
            if title_elem:
                keynote['title'] = title_elem.get_text(strip=True)
            
            # Extract full description from toggle content
            desc_elem = article.select_one('.toggle-content__content')
            if desc_elem:
                keynote['description'] = desc_elem.get_text(separator='\n', strip=True)
            
            # Extract keynote ID from article
            if article.get('id'):
                keynote['id'] = article.get('id')
            
            if keynote.get('title'):
                keynotes.append(keynote)
    
    speaker_data['keynotes'] = keynotes

    # Extract videos
    videos = []
    videos_section = soup.find('article', class_='profile-videos')
    if not videos_section:
        # Alternative selector for video section
        videos_section = soup.find('article', class_='video-slider')
    
    if videos_section:
        # Extract video information
        video_items = videos_section.select('.video-ribbon, .js-media--youtube')
        for item in video_items:
            video_info = {}
            
            # Get video title
            title_elem = item.select_one('h3')
            if title_elem:
                video_info['title'] = title_elem.get_text(strip=True)
            
            # Get video description
            desc_elem = item.select_one('p.text-base-sm')
            if desc_elem:
                video_info['description'] = desc_elem.get_text(strip=True)
            
            # Get video URL from iframe
            iframe = item.select_one('iframe')
            if iframe:
                video_info['url'] = iframe.get('data-src') or iframe.get('src', '')
                video_info['video_id'] = iframe.get('id', '')
            
            # Get thumbnail if available
            img = item.select_one('img[src*="youtube"]')
            if img:
                video_info['thumbnail'] = img.get('src', '')
            
            if video_info.get('url') or video_info.get('title'):
                videos.append(video_info)
    
    speaker_data['videos'] = videos

    # Extract customer reviews
    reviews = []
    reviews_section = soup.find('article', id='profile-reviews')
    if reviews_section:
        # Find all review articles
        review_articles = reviews_section.select('article[itemprop="review"]')
        
        for review in review_articles:
            review_data = {}
            
            # Extract rating
            rating_elem = review.select_one('span[itemprop="ratingValue"]')
            if rating_elem:
                review_data['rating'] = int(rating_elem.get_text(strip=True))
            else:
                # Count the star SVGs as fallback
                stars = review.select('ul[role="list"] svg')
                review_data['rating'] = len(stars) if stars else 5
            
            # Extract review text
            review_text_elem = review.select_one('p[itemprop="reviewBody"]')
            if review_text_elem:
                review_data['review_text'] = review_text_elem.get_text(strip=True)
            
            # Extract author information
            author_elem = review.select_one('p[itemprop="author"]')
            if author_elem:
                # Get author name/title
                author_name_elem = author_elem.select_one('span[itemprop="name"]')
                if author_name_elem:
                    review_data['author_title'] = author_name_elem.get_text(strip=True)
                
                # Get organization
                org_elem = author_elem.select_one('span.text-primary-cta-color-text-disabled')
                if org_elem:
                    review_data['author_organization'] = org_elem.get_text(strip=True)
            
            if review_data.get('review_text'):
                reviews.append(review_data)
        
        # Also get the total review count and average rating if available
        aggregate_rating = reviews_section.select_one('div[itemprop="aggregateRating"]')
        if aggregate_rating:
            avg_rating_elem = aggregate_rating.select_one('span[itemprop="ratingValue"]')
            review_count_elem = aggregate_rating.select_one('span[itemprop="reviewCount"]')
            
            if avg_rating_elem:
                speaker_data['average_rating'] = float(avg_rating_elem.get_text(strip=True))
            if review_count_elem:
                speaker_data['total_reviews'] = int(review_count_elem.get_text(strip=True))
    
    speaker_data['reviews'] = reviews

    # Extract social media links (only speaker's personal links)
    social_links = {}
    social_platforms = ['twitter', 'linkedin', 'facebook', 'instagram', 'youtube']
    
    # Look for social links in the speaker profile area only
    profile_section = soup.find('article', class_='profile') or soup.find('section', class_='speaker-profile')
    if profile_section:
        for platform in social_platforms:
            link = profile_section.find('a', href=lambda h: h and platform in h.lower() and '/company/' not in h.lower() and '/pages/' not in h.lower())
            if link:
                social_links[platform] = link.get('href')
    
    # Only add social_media if we found speaker-specific links
    if social_links:
        speaker_data['social_media'] = social_links

    # Extract fee/price information if available
    fee_info = soup.find(string=lambda t: t and any(word in t.lower() for word in ['fee', 'price', 'cost']))
    if fee_info:
        fee_parent = fee_info.parent
        if fee_parent:
            speaker_data['fee_range'] = fee_parent.text.strip()

    # Extract languages if available (from speaker profile, not general site)
    # Look for a section specifically about languages the speaker speaks
    languages_section = soup.find('div', class_=re.compile('language|speaks', re.I))
    if languages_section and 'speaks' in languages_section.text.lower():
        # Extract actual language names, not JSON data
        lang_text = languages_section.get_text(strip=True)
        # Remove the label and extract languages
        if ':' in lang_text:
            speaker_data['languages'] = lang_text.split(':')[1].strip()
        else:
            speaker_data['languages'] = lang_text

    return speaker_data

def main():
    """Main function to orchestrate the scraping process for A-Speakers."""
    collection = get_db_collection()
    session = requests.Session()

    print(f"Starting to scrape speakers from {BASE_URL}/speakers/")
    
    page_num = 1
    # Use a set to track scraped URLs to detect when the listing ends
    scraped_urls = set()

    while True:
        # The website uses a page parameter for its infinite scroll
        search_url = f"{BASE_URL}/speakers/?page={page_num}"
        print(f"\n--- Scraping Page {page_num} ---")

        max_retries = 3
        for retry in range(max_retries):
            try:
                response = session.get(search_url, proxies=PROXY, timeout=60)
                response.raise_for_status()
                break
            except requests.exceptions.RequestException as e:
                if retry < max_retries - 1:
                    print(f"Failed to fetch search page {page_num}. Error: {e}. Retrying... ({retry + 1}/{max_retries})")
                    time.sleep(5)  # Wait 5 seconds before retry
                else:
                    print(f"Failed to fetch search page {page_num} after {max_retries} attempts. Error: {e}. Ending scrape.")
                    return

        soup = BeautifulSoup(response.content, 'html.parser')
        # Selector for each speaker block on the main list page
        speaker_items = soup.select('li.speaker-item')

        if not speaker_items:
            print("No more speaker items found. Scraping process complete.")
            break
        
        new_speakers_found = False
        for item in speaker_items:
            link_tag = item.select_one('a')
            if not link_tag or not link_tag.has_attr('href'):
                continue
            
            relative_url = link_tag['href']
            speaker_url = urljoin(BASE_URL, relative_url)

            # If we have seen this URL before, we assume we've hit the end of the unique content
            if speaker_url in scraped_urls:
                continue

            new_speakers_found = True
            scraped_urls.add(speaker_url)

            # Check if speaker already exists in the database
            if collection.count_documents({'url': speaker_url}) > 0:
                print(f"  Skipping already scraped speaker: {speaker_url}")
                continue

            print(f"  Fetching details for: {speaker_url}")
            speaker_details = scrape_speaker_page(speaker_url, session)

            if speaker_details:
                # Extract additional info from the list page
                # Don't override location if already extracted from profile
                if 'location' not in speaker_details or speaker_details['location'] == "N/A":
                    location_tag = item.select_one('div.field-name-field-speaker-location')
                    if not location_tag:
                        location_tag = item.select_one('span.location')
                    if location_tag:
                        speaker_details['location'] = location_tag.text.strip()
                
                # Price/Fee range from list
                price_tag = item.select_one('div.price') or item.select_one('span.fee')
                if price_tag and 'fee_range' not in speaker_details:
                    speaker_details['fee_range'] = price_tag.text.strip()
                
                # Languages from list
                lang_tag = item.select_one('div.languages') or item.select_one('span.language')
                if lang_tag and 'languages' not in speaker_details:
                    speaker_details['languages'] = lang_tag.text.strip()
                
                # Add scraping timestamp
                speaker_details['scraped_at'] = datetime.utcnow()
                
                try:
                    # Insert or update speaker data
                    collection.update_one(
                        {'url': speaker_url},
                        {'$set': speaker_details},
                        upsert=True
                    )
                    print(f"    -> Saved '{speaker_details.get('name', 'N/A')}' to MongoDB.")
                    print(f"       Topics: {len(speaker_details.get('topics', []))}")
                    print(f"       Keynotes: {len(speaker_details.get('keynotes', []))}")
                    print(f"       Videos: {len(speaker_details.get('videos', []))}")
                    if speaker_details.get('why_book_points'):
                        print(f"       Why Book Points: {len(speaker_details.get('why_book_points', []))}")
                    if speaker_details.get('reviews'):
                        print(f"       Reviews: {len(speaker_details.get('reviews', []))}")
                        if speaker_details.get('average_rating'):
                            print(f"       Average Rating: {speaker_details.get('average_rating')}/5")
                except Exception as e:
                    print(f"    -> ERROR: Could not save data to MongoDB. {e}")
        
        # If a page yields no new speakers, stop the process
        if not new_speakers_found:
            print("No new speakers found on this page. Ending scrape.")
            break

        page_num += 1

    print("\n--- Scraping process completed. ---")

if __name__ == "__main__":
    main()
