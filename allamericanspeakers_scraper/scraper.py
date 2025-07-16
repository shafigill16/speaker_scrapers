import requests
from bs4 import BeautifulSoup
import pymongo
import sys
import json
import re
import time
from urllib.parse import urljoin, quote
from datetime import datetime
import logging
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Configuration ---
MONGO_URI = os.getenv('MONGO_URI', 'mongodb://localhost:27017/')
DB_NAME = os.getenv('DB_NAME', 'allamericanspeakers')
COLLECTION_NAME = os.getenv('COLLECTION_NAME', 'speakers')
BASE_URL = os.getenv('BASE_URL', 'https://www.allamericanspeakers.com')

# Headers to mimic a real browser
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1'
}

def get_db_collection():
    """Establishes a connection to MongoDB and returns the collection object."""
    try:
        client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.server_info()
        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]
        logging.info("Successfully connected to MongoDB.")
        return collection
    except pymongo.errors.ConnectionFailure as e:
        logging.error(f"Error: Could not connect to MongoDB. {e}")
        sys.exit(1)

def clean_location(location_text):
    """Clean up location text by removing FAQ content."""
    if not location_text:
        return None
    
    # Remove everything after "but can be booked"
    clean_loc = re.split(r',?\s*but can be booked', location_text)[0]
    
    # Also remove "and can be booked" variations
    clean_loc = re.split(r',?\s*and can be booked', clean_loc)[0]
    
    return clean_loc.strip()

def extract_fee_range(fee_text):
    """Extract and normalize fee range from text."""
    if not fee_text:
        return None
    
    # Clean up the text
    fee_text = fee_text.strip()
    
    # Extract live event fee
    live_match = re.search(r'Live Event:\s*\$?([\d,]+)\s*-\s*\$?([\d,]+)', fee_text)
    virtual_match = re.search(r'Virtual Event:\s*\$?([\d,]+)\s*-\s*\$?([\d,]+)', fee_text)
    
    fee_info = {}
    if live_match:
        fee_info['live_event'] = f"${live_match.group(1)} - ${live_match.group(2)}"
    if virtual_match:
        fee_info['virtual_event'] = f"${virtual_match.group(1)} - ${virtual_match.group(2)}"
    
    return fee_info if fee_info else None

def extract_reviews(speaker_id, session):
    """Extract reviews from the reviews popup URL."""
    reviews = []
    review_url = f"{BASE_URL}/float_box/reviews.php?spid={speaker_id}"
    
    try:
        response = session.get(review_url, headers=HEADERS, timeout=10)
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find all review list items
            review_items = soup.select('div.ReviewsList ul li')
            
            for item in review_items:
                review_data = {}
                
                # Extract rating (count full stars)
                star_imgs = item.select('div.rating-star img[src*="full-star"]')
                if star_imgs:
                    review_data['rating'] = len(star_imgs)
                
                # Extract review text
                review_text = item.find('p')
                if review_text:
                    review_data['text'] = review_text.get_text(strip=True)
                
                # Extract reviewer name/organization
                author_elem = item.find('div', class_='review-author')
                if author_elem:
                    review_data['author'] = author_elem.get_text(strip=True)
                
                if review_data.get('text'):
                    reviews.append(review_data)
                    
    except Exception as e:
        logging.debug(f"Could not fetch reviews for speaker {speaker_id}: {e}")
    
    return reviews

def scrape_speaker_page(speaker_url, session):
    """Scrapes detailed information from an individual speaker's page."""
    try:
        response = session.get(speaker_url, headers=HEADERS, timeout=30)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch speaker page {speaker_url}. Error: {e}")
        return None

    # Use response.content instead of response.text to handle encoding properly
    soup = BeautifulSoup(response.content, 'html.parser')
    speaker_data = {'url': speaker_url}

    # Extract speaker ID from URL
    match = re.search(r'/speakers/(\d+)/', speaker_url)
    if match:
        speaker_data['speaker_id'] = match.group(1)

    # Extract name
    name_elem = soup.find('h1', class_='speaker-name')
    if name_elem:
        # Remove any nested links and get text
        name_text = name_elem.get_text(strip=True)
        speaker_data['name'] = name_text
    
    # Extract byline/job title
    byline_elem = soup.find('h2', class_='bayline')
    if byline_elem:
        speaker_data['job_title'] = byline_elem.get_text(strip=True)
    
    # Extract biography
    bio_section = soup.find('div', class_='Biography')
    if bio_section:
        # Remove the contact form link paragraph if present
        for p in bio_section.find_all('p', class_='content-link'):
            p.decompose()
        speaker_data['biography'] = bio_section.get_text(separator='\n', strip=True)
    
    # Extract location from structured data or page content - CLEANED VERSION
    location_pattern = re.compile(r'generally travels from ([^,]+(?:,\s*[^,]+)*)')
    location_text = soup.get_text()
    location_match = location_pattern.search(location_text)
    if location_match:
        raw_location = location_match.group(1)
        speaker_data['location'] = clean_location(raw_location)
    
    # Extract speaking fee
    fee_section = soup.find('div', class_='SpeakerFee')
    if fee_section:
        fee_content = fee_section.find('div', class_='SecContent')
        if fee_content:
            fee_text = fee_content.get_text(strip=True)
            speaker_data['fee_range'] = extract_fee_range(fee_text)
    
    # Extract categories/topics
    category_section = soup.find('div', class_='SpeakerCategory')
    if category_section:
        categories = []
        category_links = category_section.find_all('a')
        for link in category_links:
            cat_text = link.get_text(strip=True)
            if cat_text and cat_text != 'View All':
                categories.append(cat_text)
        if categories:
            speaker_data['categories'] = categories
    
    # Extract speaking topics - STRUCTURED VERSION
    topics_section = soup.find('div', class_='SpeakingTopicsIntro')
    if topics_section:
        topics = []
        topic_items = topics_section.find_all('li')
        for item in topic_items:
            topic_title = item.find('span', class_='topic-title')
            topic_desc = item.find('div', class_='topic-info')
            
            if topic_title:
                topic_data = {
                    'title': topic_title.get_text(strip=True)
                }
                if topic_desc:
                    topic_data['description'] = topic_desc.get_text(strip=True)
                topics.append(topic_data)
            else:
                # Fallback - try to split title and description by first sentence
                topic_text = item.get_text(strip=True)
                if topic_text and not topic_text.startswith('View All'):
                    # Try to split by common patterns
                    if '?' in topic_text and topic_text.index('?') < len(topic_text) - 1:
                        # Split after first question mark
                        split_idx = topic_text.index('?') + 1
                        title = topic_text[:split_idx].strip()
                        desc = topic_text[split_idx:].strip()
                        topics.append({'title': title, 'description': desc})
                    elif '. ' in topic_text and len(topic_text) > 50:
                        # Split after first sentence if text is long
                        split_idx = topic_text.index('. ') + 1
                        title = topic_text[:split_idx].strip()
                        desc = topic_text[split_idx:].strip()
                        topics.append({'title': title, 'description': desc})
                    else:
                        # Use first 100 chars as title if no clear split
                        if len(topic_text) > 100:
                            topics.append({'title': topic_text[:100] + '...', 'description': topic_text})
                        else:
                            topics.append({'title': topic_text})
        
        if topics:
            speaker_data['speaking_topics'] = topics
    
    # Extract images
    images = []
    
    # Main profile image
    profile_img = soup.find('img', {'id': 'MainProfilePic'})
    if profile_img and profile_img.get('src'):
        images.append({
            'type': 'profile',
            'url': urljoin(BASE_URL, profile_img['src']),
            'alt': profile_img.get('alt', '')
        })
    
    # Gallery images
    gallery_section = soup.find('div', class_='speaker-gallery')
    if gallery_section:
        gallery_images = gallery_section.find_all('img')
        for img in gallery_images:
            if img.get('src'):
                images.append({
                    'type': 'gallery',
                    'url': urljoin(BASE_URL, img['src']),
                    'alt': img.get('alt', '')
                })
    
    if images:
        speaker_data['images'] = images
    
    # Extract videos - UPDATED VERSION
    videos = []
    
    # Look for video section with YouTube links
    video_section = soup.find('div', class_='SpeakerVideoIntro')
    if video_section:
        # Find all video list items
        video_items = video_section.find_all('li')
        for item in video_items:
            # Find the video link
            video_link = item.find('a', href=re.compile(r'youtube\.com|youtu\.be'))
            if video_link:
                video_url = video_link.get('href')
                video_data = {
                    'url': video_url,
                    'type': 'youtube'
                }
                
                # Get title from the same li
                title_elem = item.find('div', class_='video-title')
                if title_elem:
                    video_data['title'] = title_elem.get_text(strip=True)
                
                # Get description from the same li
                desc_elem = item.find('div', class_='video-text')
                if desc_elem:
                    video_data['description'] = desc_elem.get_text(strip=True)
                
                videos.append(video_data)
    
    # Also look for iframe embeds (fallback)
    if not videos:
        iframe_videos = soup.find_all('iframe', src=re.compile(r'youtube|vimeo'))
        for iframe in iframe_videos:
            video_src = iframe.get('src')
            if video_src:
                videos.append({
                    'url': video_src,
                    'type': 'youtube' if 'youtube' in video_src else 'vimeo' if 'vimeo' in video_src else 'other'
                })
    
    if videos:
        speaker_data['videos'] = videos
    
    # Extract social media links
    social_links = {}
    social_section = soup.find('div', class_='profile-social-media')
    if social_section:
        for platform in ['facebook', 'twitter', 'linkedin', 'instagram', 'youtube']:
            link = social_section.find('a', href=re.compile(platform, re.I))
            if link:
                social_links[platform] = link.get('href')
    
    if social_links:
        speaker_data['social_media'] = social_links
    
    # Extract aggregate rating info
    rating_info = {}
    rating_div = soup.find('div', class_='Rating')
    if rating_div:
        # Count full stars
        full_stars = rating_div.find_all('img', src=re.compile('rating-star-full'))
        if full_stars:
            rating_info['average_rating'] = len(full_stars)
    
    # Extract review count
    review_link = soup.find('a', onclick=re.compile(r'reviews\.php\?spid='))
    if review_link:
        review_text = review_link.get_text(strip=True)
        review_count_match = re.search(r'(\d+)\s*review', review_text)
        if review_count_match:
            rating_info['review_count'] = int(review_count_match.group(1))
    
    if rating_info:
        speaker_data['rating'] = rating_info
    
    # Extract individual reviews
    if 'speaker_id' in speaker_data:
        reviews = extract_reviews(speaker_data['speaker_id'], session)
        if reviews:
            speaker_data['reviews'] = reviews

    return speaker_data

def get_speakers_from_sitemap(session=None):
    """Fetch speaker URLs from sitemap."""
    if session is None:
        session = requests.Session()
    
    speakers = []
    sitemap_url = f"{BASE_URL}/sitemap.xml"
    
    try:
        response = session.get(sitemap_url, headers=HEADERS, timeout=30)
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'xml')
            urls = soup.find_all('loc')
            
            for url in urls:
                url_text = url.get_text()
                if '/speakers/' in url_text:
                    # Clean up CDATA if present
                    url_text = url_text.replace('<![CDATA[', '').replace(']]>', '').strip()
                    speakers.append(url_text)
            
            logging.info(f"Found {len(speakers)} speakers in sitemap")
    except Exception as e:
        logging.error(f"Could not fetch sitemap: {e}")
    
    return speakers

def main():
    """Main function to orchestrate the scraping process."""
    collection = get_db_collection()
    session = requests.Session()
    
    # Create index on speaker_id for faster lookups and ensure uniqueness
    collection.create_index([("speaker_id", 1)], unique=True, sparse=True)
    collection.create_index([("url", 1)], unique=True)
    
    logging.info(f"Starting to scrape speakers from {BASE_URL}")
    
    # Get list of speaker URLs from sitemap
    speaker_urls = get_speakers_from_sitemap(session)
    
    if not speaker_urls:
        logging.error("No speaker URLs found. The website structure might have changed.")
        return
    
    logging.info(f"Found {len(speaker_urls)} speaker URLs to process")
    
    # Stats tracking
    stats = {
        'processed': 0,
        'new': 0,
        'updated': 0,
        'errors': 0,
        'skipped': 0
    }
    
    # Process all speakers
    logging.info(f"Processing all {len(speaker_urls)} speakers...")
    
    for idx, speaker_url in enumerate(speaker_urls, 1):
        try:
            # Extract speaker ID from URL for checking
            speaker_id_match = re.search(r'/speakers/(\d+)/', speaker_url)
            speaker_id = speaker_id_match.group(1) if speaker_id_match else None
            
            # Check if already scraped
            existing = None
            if speaker_id:
                existing = collection.find_one({'speaker_id': speaker_id})
            else:
                existing = collection.find_one({'url': speaker_url})
            
            if existing:
                # Check if we need to update (missing critical fields)
                needs_update = False
                critical_fields = ['videos', 'reviews', 'name', 'location']
                
                for field in critical_fields:
                    if field not in existing or not existing[field]:
                        needs_update = True
                        break
                
                # Also check if location needs cleaning
                if 'location' in existing and existing['location'] and 'but can be booked' in existing['location']:
                    needs_update = True
                
                if not needs_update:
                    logging.info(f"[{idx}/{len(speaker_urls)}] Skipping complete: {speaker_url}")
                    stats['skipped'] += 1
                    continue
                else:
                    logging.info(f"[{idx}/{len(speaker_urls)}] Updating incomplete: {speaker_url}")
            else:
                logging.info(f"[{idx}/{len(speaker_urls)}] Fetching new: {speaker_url}")
            
            speaker_data = scrape_speaker_page(speaker_url, session)
            
            if speaker_data:
                # Add timestamp
                speaker_data['scraped_at'] = datetime.utcnow()
                
                # Determine if this is new or update
                is_new = existing is None
                
                # Use upsert with speaker_id as primary key if available
                if 'speaker_id' in speaker_data:
                    result = collection.update_one(
                        {'speaker_id': speaker_data['speaker_id']},
                        {'$set': speaker_data},
                        upsert=True
                    )
                else:
                    result = collection.update_one(
                        {'url': speaker_url},
                        {'$set': speaker_data},
                        upsert=True
                    )
                
                if result.upserted_id:
                    stats['new'] += 1
                else:
                    stats['updated'] += 1
                
                logging.info(f"  -> {'Added' if is_new else 'Updated'} '{speaker_data.get('name', 'N/A')}'")
                
                # Log some statistics
                if 'categories' in speaker_data:
                    logging.info(f"     Categories: {len(speaker_data['categories'])}")
                if 'speaking_topics' in speaker_data:
                    logging.info(f"     Topics: {len(speaker_data['speaking_topics'])}")
                if 'videos' in speaker_data:
                    logging.info(f"     Videos: {len(speaker_data['videos'])}")
                if 'reviews' in speaker_data:
                    logging.info(f"     Reviews: {len(speaker_data['reviews'])}")
                if 'rating' in speaker_data:
                    rating = speaker_data['rating']
                    logging.info(f"     Rating: {rating.get('average_rating', 'N/A')}/5 ({rating.get('review_count', 0)} reviews)")
                
                stats['processed'] += 1
                
        except pymongo.errors.DuplicateKeyError:
            logging.warning(f"[{idx}/{len(speaker_urls)}] Duplicate key, skipping: {speaker_url}")
            stats['skipped'] += 1
        except Exception as e:
            logging.error(f"[{idx}/{len(speaker_urls)}] ERROR processing {speaker_url}: {e}")
            stats['errors'] += 1
        
        # Be polite to the server
        time.sleep(2)
        
        # Take a longer break every 50 speakers
        if idx % 50 == 0:
            logging.info("Taking a 30-second break...")
            logging.info(f"Progress: Processed={stats['processed']}, New={stats['new']}, Updated={stats['updated']}, Skipped={stats['skipped']}, Errors={stats['errors']}")
            time.sleep(30)
    
    logging.info("Scraping process completed.")
    logging.info(f"Final stats: Processed={stats['processed']}, New={stats['new']}, Updated={stats['updated']}, Skipped={stats['skipped']}, Errors={stats['errors']}")

if __name__ == "__main__":
    main()