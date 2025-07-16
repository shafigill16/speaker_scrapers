#!/usr/bin/env python3
"""
EventRaptor Speaker Scraper

This script scrapes speaker profiles from EventRaptor's speaker directory.
It extracts comprehensive information about each speaker including:
- Basic info (name, tagline, credentials)
- Biography
- Business areas/categories
- Social media links
- Profile images
- Events they've participated in
- Presentations

The data is stored in MongoDB for easy querying and analysis.
"""

import requests
from bs4 import BeautifulSoup
import pymongo
import sys
import json
import re
import time
from urllib.parse import urljoin, urlparse, parse_qs
from datetime import datetime
import logging
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Configuration from environment variables
MONGO_URI = os.getenv('MONGO_URI', 'mongodb://localhost:27017/')
DB_NAME = os.getenv('DB_NAME', 'eventraptor')
COLLECTION_NAME = os.getenv('COLLECTION_NAME', 'speakers')
BASE_URL = os.getenv('BASE_URL', 'https://app.eventraptor.com')
SPEAKERS_URL = os.getenv('SPEAKERS_URL', 'https://app.eventraptor.com/speakers')

# Request headers
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1'
}

def get_db_collection():
    """Establish a connection to MongoDB and return the collection object.
    
    Returns:
        pymongo.collection.Collection: MongoDB collection for speakers
        
    Raises:
        SystemExit: If connection to MongoDB fails
    """
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

def get_speaker_urls_from_page(page_num, session):
    """Extract speaker profile URLs from a specific page."""
    try:
        url = f"{SPEAKERS_URL}?page={page_num}"
        response = session.get(url, headers=HEADERS, timeout=30)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        speaker_urls = []
        
        # Find speaker profile links
        # Look for links that go to /speaker-profiles/
        profile_links = soup.find_all('a', href=re.compile(r'/speaker-profiles/'))
        
        for link in profile_links:
            href = link.get('href')
            if href and '/speaker-profiles/' in href:
                full_url = urljoin(BASE_URL, href)
                if full_url not in speaker_urls:
                    speaker_urls.append(full_url)
        
        return speaker_urls
        
    except Exception as e:
        logging.error(f"Error fetching page {page_num}: {e}")
        return []

def get_total_pages(session):
    """Get the total number of pages from the speakers listing."""
    try:
        response = session.get(SPEAKERS_URL, headers=HEADERS, timeout=30)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Method 1: Look for "Last »" in button tags (for Livewire apps)
        last_button = soup.find('button', string=re.compile(r'Last\s*»'))
        if last_button and last_button.get('wire:click'):
            wire_click = last_button.get('wire:click', '')
            match = re.search(r'setPage\((\d+)', wire_click)
            if match:
                return int(match.group(1))
        
        # Method 2: Look for "Page X of Y" pattern
        page_info = soup.find(string=re.compile(r'Page\s+\d+\s+of\s+\d+'))
        if page_info:
            match = re.search(r'Page\s+\d+\s+of\s+(\d+)', page_info)
            if match:
                return int(match.group(1))
        
        # Method 3: Look for page info in div elements
        page_divs = soup.find_all('div', class_='text-xs')
        for div in page_divs:
            text = div.get_text(strip=True)
            match = re.search(r'Page\s+\d+\s+of\s+(\d+)', text)
            if match:
                return int(match.group(1))
        
        # Method 4: Look for "Last »" link in anchor tags (original method)
        last_link = soup.find('a', string='Last »')
        if last_link and last_link.get('href'):
            href = last_link.get('href')
            # Extract page number from URL
            parsed = urlparse(href)
            params = parse_qs(parsed.query)
            if 'page' in params:
                return int(params['page'][0])
        
        # Method 5: Look for pagination buttons with setPage
        buttons = soup.find_all('button', attrs={'wire:click': re.compile(r'setPage\(\d+')})
        page_numbers = []
        for button in buttons:
            wire_click = button.get('wire:click', '')
            match = re.search(r'setPage\((\d+)', wire_click)
            if match:
                page_numbers.append(int(match.group(1)))
        if page_numbers:
            return max(page_numbers)
        
        # Method 6: Alternative - look for page numbers in navigation
        pagination = soup.find('nav', {'aria-label': 'Pagination Navigation'})
        if pagination:
            page_links = pagination.find_all('a', href=re.compile(r'page=\d+'))
            page_numbers = []
            for link in page_links:
                match = re.search(r'page=(\d+)', link.get('href', ''))
                if match:
                    page_numbers.append(int(match.group(1)))
            if page_numbers:
                return max(page_numbers)
        
        logging.warning("Could not determine total pages, defaulting to 1")
        return 1  # Default to 1 page if we can't find pagination
        
    except Exception as e:
        logging.error(f"Error getting total pages: {e}")
        return 1

def scrape_speaker_profile(speaker_url, session):
    """Scrape detailed information from a speaker profile page.
    
    Args:
        speaker_url (str): URL of the speaker profile
        session (requests.Session): HTTP session for requests
        
    Returns:
        dict: Speaker data or None if scraping fails
    """
    try:
        response = session.get(speaker_url, headers=HEADERS, timeout=30)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        speaker_data = {'url': speaker_url}
        
        # Extract speaker ID from URL
        match = re.search(r'/speaker-profiles/([^/]+)/?$', speaker_url)
        if match:
            speaker_data['speaker_id'] = match.group(1)
        
        # Extract name
        name_elem = soup.find('dd', class_=re.compile(r'text-xl.*font-bold'))
        if name_elem:
            speaker_data['name'] = name_elem.get_text(strip=True)
        
        # Extract tagline and credentials
        if name_elem:
            tagline_elem = name_elem.find_next_sibling('dd')
            if tagline_elem and 'italic' not in tagline_elem.get('class', []):
                speaker_data['tagline'] = tagline_elem.get_text(strip=True)
                
                cred_elem = tagline_elem.find_next_sibling('dd')
                if cred_elem and 'italic' in cred_elem.get('class', []):
                    speaker_data['credentials'] = cred_elem.get_text(strip=True)
        
        # Extract business areas
        business_areas = []
        area_elements = soup.find_all('span', class_='badge')
        for elem in area_elements:
            text = elem.get_text(strip=True)
            if text and '+' not in text:  # Filter out "+N" badges
                business_areas.append(text)
        if business_areas:
            speaker_data['business_areas'] = business_areas
        
        # Extract biography/about
        bio_section = soup.find('dd', class_='ck-content')
        if bio_section:
            bio_text = bio_section.get_text(separator='\n', strip=True)
            if bio_text:
                speaker_data['biography'] = bio_text
        
        # Extract presentations
        presentations = []
        pres_section = soup.find('h2', string='Presentations')
        if pres_section:
            pres_parent = pres_section.find_parent()
            while pres_parent and pres_parent.name not in ['section', 'div']:
                pres_parent = pres_parent.find_parent()
            
            if pres_parent:
                h3_titles = pres_parent.find_all('h3')
                for title in h3_titles:
                    pres_text = title.get_text(strip=True)
                    if pres_text:
                        presentations.append(pres_text)
        
        if presentations:
            speaker_data['presentations'] = presentations
        
        # Extract profile image
        profile_img = soup.find('img', class_=re.compile(r'object-cover.*rounded-full'))
        if not profile_img and 'name' in speaker_data:
            profile_img = soup.find('img', {'alt': speaker_data['name']})
        if not profile_img:
            profile_img = soup.find('img', src=re.compile(r'/storage/.*avatar'))
        if profile_img and profile_img.get('src'):
            speaker_data['profile_image'] = urljoin(BASE_URL, profile_img['src'])
        
        # Extract social media links
        social_links = {}
        social_patterns = {
            'linkedin': r'linkedin\.com',
            'twitter': r'twitter\.com|x\.com',
            'facebook': r'facebook\.com',
            'instagram': r'instagram\.com',
            'youtube': r'youtube\.com'
        }
        
        for platform, pattern in social_patterns.items():
            link = soup.find('a', href=re.compile(pattern, re.I))
            if link:
                social_links[platform] = link.get('href')
        
        if social_links:
            speaker_data['social_media'] = social_links
        
        # Extract contact info if available
        email_elem = soup.find('a', href=re.compile(r'^mailto:'))
        if email_elem:
            speaker_data['email'] = email_elem.get('href').replace('mailto:', '')
        
        # Extract events
        events = []
        events_heading = None
        for h2 in soup.find_all('h2'):
            if 'Events' in h2.get_text(strip=True):
                events_heading = h2
                break
        
        if events_heading:
            events_section = events_heading.find_parent()
            while events_section and (events_section.name not in ['div', 'section'] or not events_section.get('class')):
                events_section = events_section.find_parent()
            
            if not events_section:
                events_section = events_heading.find_parent()
            
            if events_section:
                event_links = events_section.find_all('a', href=re.compile(r'/events/\d+'))
                
                for link in event_links:
                    event_info = {
                        'url': urljoin(BASE_URL, link.get('href')),
                        'event_id': re.search(r'/events/(\d+)', link.get('href')).group(1) if re.search(r'/events/(\d+)', link.get('href')) else None
                    }
                    
                    event_name = link.get_text(strip=True)
                    if event_name:
                        event_info['name'] = event_name
                    
                    if event_info not in events:
                        events.append(event_info)
        
        if events:
            speaker_data['events'] = events
        
        return speaker_data
        
    except Exception as e:
        logging.error(f"Error scraping profile {speaker_url}: {e}")
        return None

def main():
    """Main function to orchestrate the scraping process."""
    collection = get_db_collection()
    session = requests.Session()
    
    # Create indexes for faster lookups and uniqueness
    collection.create_index([("speaker_id", 1)], unique=True, sparse=True)
    collection.create_index([("url", 1)], unique=True)
    
    logging.info(f"Starting to scrape speakers from {BASE_URL}")
    
    # Get total number of pages
    total_pages = get_total_pages(session)
    logging.info(f"Found {total_pages} pages to process")
    
    # Stats tracking
    stats = {
        'processed': 0,
        'new': 0,
        'updated': 0,
        'errors': 0,
        'skipped': 0
    }
    
    all_speaker_urls = []
    
    # Collect all speaker URLs
    logging.info("Collecting speaker URLs from all pages...")
    for page in range(1, total_pages + 1):
        logging.info(f"Fetching page {page}/{total_pages}")
        page_urls = get_speaker_urls_from_page(page, session)
        all_speaker_urls.extend(page_urls)
        time.sleep(1)  # Rate limiting
        
        # Extended break every 20 pages
        if page % 20 == 0:
            logging.info("Taking a 10-second break...")
            time.sleep(10)
    
    logging.info(f"Collected {len(all_speaker_urls)} speaker URLs")
    
    # Scrape each speaker profile
    for idx, speaker_url in enumerate(all_speaker_urls, 1):
        try:
            # Check if speaker already exists
            speaker_id_match = re.search(r'/speaker-profiles/([^/]+)/?$', speaker_url)
            speaker_id = speaker_id_match.group(1) if speaker_id_match else None
            
            existing = None
            if speaker_id:
                existing = collection.find_one({'speaker_id': speaker_id})
            else:
                existing = collection.find_one({'url': speaker_url})
            
            if existing:
                logging.info(f"[{idx}/{len(all_speaker_urls)}] Skipping already scraped: {speaker_url}")
                stats['skipped'] += 1
                continue
            
            logging.info(f"[{idx}/{len(all_speaker_urls)}] Fetching: {speaker_url}")
            
            speaker_data = scrape_speaker_profile(speaker_url, session)
            
            if speaker_data:
                # Add timestamp
                speaker_data['scraped_at'] = datetime.utcnow()
                
                # Insert or update in database
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
                
                logging.info(f"  -> Saved '{speaker_data.get('name', 'N/A')}'")
                
                # Log some details
                if 'business_areas' in speaker_data:
                    logging.info(f"     Business Areas: {len(speaker_data['business_areas'])}")
                if 'presentations' in speaker_data:
                    logging.info(f"     Presentations: {len(speaker_data['presentations'])}")
                if 'events' in speaker_data:
                    logging.info(f"     Events: {len(speaker_data['events'])}")
                
                stats['processed'] += 1
                
        except pymongo.errors.DuplicateKeyError:
            logging.warning(f"[{idx}/{len(all_speaker_urls)}] Duplicate key, skipping: {speaker_url}")
            stats['skipped'] += 1
        except Exception as e:
            logging.error(f"[{idx}/{len(all_speaker_urls)}] ERROR processing {speaker_url}: {e}")
            stats['errors'] += 1
        
        # Rate limiting
        time.sleep(2)
        
        # Extended break every 50 speakers
        if idx % 50 == 0:
            logging.info("Taking a 30-second break...")
            logging.info(f"Progress: Processed={stats['processed']}, New={stats['new']}, Updated={stats['updated']}, Skipped={stats['skipped']}, Errors={stats['errors']}")
            time.sleep(30)
    
    logging.info("Scraping process completed.")
    logging.info(f"Final stats: Processed={stats['processed']}, New={stats['new']}, Updated={stats['updated']}, Skipped={stats['skipped']}, Errors={stats['errors']}")

if __name__ == "__main__":
    main()