#!/usr/bin/env python3
"""
Enhanced Free Speaker Bureau scraper with MongoDB integration and comprehensive data extraction
Based on actual HTML structure analysis
"""
import requests
from bs4 import BeautifulSoup
import pymongo
from pymongo import MongoClient
import json
import time
from tqdm import tqdm
import logging
from urllib.parse import urljoin, urlparse, quote
import re
import random
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from config import MONGODB_CONFIG, PROXY_CONFIG, SCRAPER_CONFIG, HEADERS, PROXY_LIST

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('enhanced_mongodb_scraper.log'),
        logging.StreamHandler()
    ]
)

class EnhancedSpeakerScraper:
    def __init__(self):
        self.base_url = SCRAPER_CONFIG['base_url']
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        
        # Proxy list for random selection
        self.proxy_list = PROXY_LIST if PROXY_LIST else []
        
        # Set initial proxy (either from PROXY_CONFIG or random from list)
        if PROXY_CONFIG:
            self.session.proxies.update(PROXY_CONFIG)
            logging.info(f"Using proxy configuration from PROXY_CONFIG")
        elif self.proxy_list:
            self.set_random_proxy()
        else:
            logging.warning("No proxy configured - running without proxy")
        
        # MongoDB setup
        self.setup_mongodb()
        
        # Track statistics
        self.stats = {
            'total_scraped': 0,
            'successful': 0,
            'failed': 0,
            'duplicates': 0,
            'errors': []
        }
    
    def set_random_proxy(self):
        """Set a random proxy from the list"""
        if not self.proxy_list:
            logging.warning("No proxy list available for rotation")
            return
            
        proxy = random.choice(self.proxy_list)
        self.session.proxies = {
            'http': f'http://{proxy}',
            'https': f'http://{proxy}'
        }
        logging.info(f"Using proxy: {proxy}")
    
    def setup_mongodb(self):
        """Setup MongoDB connection and collection"""
        try:
            self.client = MongoClient(MONGODB_CONFIG['uri'], serverSelectionTimeoutMS=5000)
            self.client.server_info()  # Test connection
            logging.info("Successfully connected to MongoDB")
            
            self.db = self.client[MONGODB_CONFIG['database']]
            self.collection = self.db[MONGODB_CONFIG['collection']]
            
            # Create indexes
            self.collection.create_index([("profile_url", 1)], unique=True)
            self.collection.create_index([("name", 1)])
            self.collection.create_index([("location", 1)])
            self.collection.create_index([("speaking_topics", 1)])
            self.collection.create_index([("specialties", 1)])
            self.collection.create_index([("speaker_since", 1)])
            
            logging.info(f"Using database: {MONGODB_CONFIG['database']}, collection: {MONGODB_CONFIG['collection']}")
            
        except Exception as e:
            logging.error(f"Failed to connect to MongoDB: {e}")
            raise
    
    def get_soup(self, url, retries=3):
        """Fetch a page and return BeautifulSoup object with retry logic and proxy rotation"""
        for attempt in range(retries):
            try:
                # Rotate proxy on each attempt
                if attempt > 0:
                    self.set_random_proxy()
                    time.sleep(random.uniform(2, 4))  # Random delay between attempts
                    
                # Disable SSL verification when using proxy
                response = self.session.get(url, timeout=SCRAPER_CONFIG['request_timeout'], verify=False)
                response.raise_for_status()
                return BeautifulSoup(response.text, 'html.parser')
            except requests.RequestException as e:
                logging.warning(f"Attempt {attempt + 1} failed for {url}: {e}")
                if attempt == retries - 1:
                    logging.error(f"Failed to fetch {url} after {retries} attempts")
                    self.stats['errors'].append({'url': url, 'error': str(e)})
                    return None
                time.sleep(2 ** attempt)  # Exponential backoff
        return None
    
    def extract_speaker_urls_from_search(self, soup):
        """Extract speaker URLs from search results page"""
        speaker_urls = []
        
        # Look for all links that match speaker profile pattern
        profile_links = soup.find_all('a', href=re.compile(r'/speaker-presenter/|/speaker/'))
        
        for link in profile_links:
            href = link.get('href', '')
            # Filter out non-profile links
            if href and '/connect' not in href and '/writeareview' not in href:
                full_url = urljoin(self.base_url, href)
                if full_url not in speaker_urls and 'freespeakerbureau.com' in full_url:
                    speaker_urls.append(full_url)
        
        return speaker_urls
    
    def extract_comprehensive_profile(self, soup, url):
        """Extract all available information from speaker profile page"""
        profile = {
            'profile_url': url,
            'scraped_at': datetime.utcnow(),
            'last_updated': datetime.utcnow()
        }
        
        try:
            # Name - from h1 tag
            name_elem = soup.find('h1', class_='bold')
            if not name_elem:
                name_elem = soup.find('h1')
            if name_elem:
                profile['name'] = name_elem.text.strip()
            
            # Extract from meta tags
            meta_desc = soup.find('meta', {'name': 'description'})
            if meta_desc:
                profile['meta_description'] = meta_desc.get('content', '')
            
            # Profile image
            img_elem = soup.find('img', class_='img-rounded')
            if not img_elem:
                img_elem = soup.find('img', src=re.compile(r'/pictures/profile/'))
            if img_elem and img_elem.get('src'):
                profile['image_url'] = urljoin(self.base_url, img_elem['src'])
            
            # Role/Category
            role_elem = soup.find('span', class_='profile-header-top-category')
            if role_elem:
                profile['role'] = role_elem.text.strip()
            
            # Company
            company_elem = soup.find('span', class_='textbox-company')
            if not company_elem:
                company_elem = soup.find('span', class_='profile-header-company')
            if company_elem:
                profile['company'] = company_elem.text.strip()
            
            # Location
            breadcrumb = soup.find('ol', class_='breadcrumb')
            if breadcrumb:
                breadcrumb_items = breadcrumb.find_all('li')
                if len(breadcrumb_items) >= 4:
                    # Extract city and state from breadcrumb
                    state = breadcrumb_items[2].text.strip()
                    city = breadcrumb_items[3].text.strip()
                    profile['location'] = f"{city}, {state}"
                    profile['city'] = city
                    profile['state'] = state
                    profile['country'] = breadcrumb_items[1].text.strip()
            
            # Biography/About
            about_section = soup.find('div', class_='field-about_me')
            if about_section:
                # Remove script tags and get clean text
                for script in about_section.find_all('script'):
                    script.decompose()
                profile['biography'] = ' '.join(about_section.stripped_strings)
            
            # Speaker Since
            speaker_since_elem = soup.find('span', class_='years-experience')
            if speaker_since_elem:
                try:
                    profile['speaker_since'] = int(speaker_since_elem.text.strip())
                except:
                    profile['speaker_since'] = speaker_since_elem.text.strip()
            
            # Areas of Expertise
            expertise_elem = soup.find('span', class_='textarea-rep_matters')
            if expertise_elem:
                expertise_text = expertise_elem.get_text(separator='\n')
                profile['areas_of_expertise'] = [e.strip() for e in expertise_text.split('\n') if e.strip()]
            
            # Previous Speaking Engagements
            engagements_elem = soup.find('span', class_='textarea-affiliation')
            if engagements_elem:
                profile['previous_engagements'] = engagements_elem.get_text(separator='\n').strip()
            
            # Credentials
            credentials_elem = soup.find('span', class_='textarea-credentials')
            if credentials_elem:
                creds_text = credentials_elem.get_text(separator='\n')
                profile['credentials'] = [c.strip() for c in creds_text.split('\n') if c.strip()]
            
            # Awards
            awards_elem = soup.find('span', class_='textarea-awards')
            if awards_elem:
                profile['awards'] = awards_elem.text.strip()
            
            # Initialize contact info dictionary
            contact = {}
            
            # Website
            website_link = soup.find('a', {'title': 'Website', 'class': 'weblink'})
            if website_link and website_link.get('href'):
                profile['website'] = website_link['href']
            
            # Speaker OneSheet PDF
            onesheet_link = soup.find('a', class_='view-member-cv-link')
            if onesheet_link and onesheet_link.get('href'):
                profile['speaker_onesheet_url'] = onesheet_link['href']
            
            # Social Media Links
            social_links = {}
            social_section = soup.find('div', class_='member_social_icons')
            if social_section:
                # LinkedIn
                linkedin = social_section.find('a', class_=re.compile(r'linkedin'))
                if linkedin and linkedin.get('href'):
                    social_links['linkedin'] = linkedin['href']
                
                # YouTube/TED Talks
                youtube = social_section.find('a', class_=re.compile(r'youtube'))
                if youtube and youtube.get('href'):
                    social_links['youtube'] = youtube['href']
                
                # Instagram
                instagram = social_section.find('a', class_=re.compile(r'instagram'))
                if instagram and instagram.get('href'):
                    social_links['instagram'] = instagram['href']
                
                # Facebook
                facebook = social_section.find('a', class_=re.compile(r'facebook'))
                if facebook and facebook.get('href'):
                    social_links['facebook'] = facebook['href']
                
                # Twitter/X
                twitter = social_section.find('a', class_=re.compile(r'twitter'))
                if twitter and twitter.get('href'):
                    social_links['twitter'] = twitter['href']
                
                # WhatsApp (may be in social section too)
                whatsapp = social_section.find('a', class_=re.compile(r'whatsapp'))
                if whatsapp and whatsapp.get('href'):
                    social_links['whatsapp'] = whatsapp['href']
                    if not contact.get('whatsapp'):
                        contact['whatsapp'] = whatsapp['href']
                
                # TikTok
                tiktok = social_section.find('a', href=re.compile(r'tiktok\.com'))
                if tiktok and tiktok.get('href'):
                    social_links['tiktok'] = tiktok['href']
                
                # Pinterest
                pinterest = social_section.find('a', href=re.compile(r'pinterest\.com'))
                if pinterest and pinterest.get('href'):
                    social_links['pinterest'] = pinterest['href']
            
            if social_links:
                profile['social_media'] = social_links
            
            # Specialties/Speaking Topics
            specialties = []
            specialties_section = soup.find('div', class_='specialties-table')
            if specialties_section:
                specialty_parent = specialties_section.parent
                if specialty_parent:
                    all_specialties = specialty_parent.find_all('div', class_='specialties-table')
                    for spec_div in all_specialties:
                        spec_link = spec_div.find('a', class_='btn')
                        if spec_link and spec_link.text:
                            specialty_text = spec_link.text.strip()
                            if specialty_text and specialty_text != "Request Information »":
                                specialties.append(specialty_text)
            
            if specialties:
                profile['specialties'] = specialties
                profile['speaking_topics'] = specialties  # Duplicate for compatibility
            
            # Extract phone number (may be hidden behind JavaScript)
            phone_patterns = [
                # Look for tel: links
                (r'href="tel:(\d+)"', 'href'),
                # Look for WhatsApp links which often contain phone
                (r'wa\.me/(\d+)', 'whatsapp'),
                # Look for phone in text
                (r'Call:\s*(?:<u>)?(\d{10,15})(?:</u>)?', 'text'),
                # General phone pattern
                (r'(\+?1?[-.\s]?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4})', 'general')
            ]
            
            for pattern, source in phone_patterns:
                phone_match = re.search(pattern, str(soup))
                if phone_match:
                    phone = phone_match.group(1)
                    # Clean up phone number
                    if source == 'whatsapp' and phone.startswith('1'):
                        phone = phone[1:]  # Remove country code
                    if not contact.get('phone'):
                        contact['phone'] = phone
                        profile['phone_source'] = source
                    break
            
            # Check if phone exists but is hidden
            phone_section = soup.find('div', string=re.compile(r'Phone Number'))
            if phone_section:
                profile['has_phone_section'] = True
                
            # Extract email (may not be directly visible)
            email_patterns = [
                # mailto links
                (r'href="mailto:([^"]+)"', 'mailto'),
                # Email in text
                (r'([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})', 'text'),
                # Contact form email field
                (r'name="email"[^>]*value="([^"]+)"', 'form')
            ]
            
            for pattern, source in email_patterns:
                email_match = re.search(pattern, str(soup))
                if email_match:
                    email = email_match.group(1)
                    if '@' in email and not email.endswith('@yoursite.com'):
                        contact['email'] = email
                        profile['email_source'] = source
                        break
            
            # Extract booking/calendar link
            booking_link = soup.find('a', {'title': 'Booking Link'})
            if booking_link and booking_link.get('href'):
                contact['booking_url'] = booking_link['href']
            
            # Check for Calendly or other scheduling links
            calendly_patterns = [
                r'(https?://[^"\s]*calendly\.com[^"\s]*)',
                r'(https?://[^"\s]*link\.goexpandnow\.com[^"\s]*)',
                r'(https?://[^"\s]*acuityscheduling\.com[^"\s]*)'
            ]
            
            for pattern in calendly_patterns:
                cal_match = re.search(pattern, str(soup))
                if cal_match:
                    contact['scheduling_url'] = cal_match.group(1)
                    break
            
            # WhatsApp contact
            whatsapp_link = soup.find('a', href=re.compile(r'wa\.me/'))
            if whatsapp_link and whatsapp_link.get('href'):
                contact['whatsapp'] = whatsapp_link['href']
            
            # Add contact info to profile if any was found
            if contact:
                profile['contact_info'] = contact
            
            # Member level/status
            member_level_patterns = ['Premium Member', 'Gold Member', 'Silver Member', 'Featured Speaker']
            page_text = str(soup)
            for pattern in member_level_patterns:
                if pattern in page_text:
                    profile['member_level'] = pattern.replace(' Member', '').replace(' Speaker', '').lower()
                    break
            
            return profile
            
        except Exception as e:
            logging.error(f"Error extracting profile from {url}: {e}")
            profile['extraction_error'] = str(e)
            return profile
    
    def save_to_mongodb(self, speaker_data):
        """Save or update speaker data in MongoDB"""
        try:
            # Clean data before saving
            speaker_data = {k: v for k, v in speaker_data.items() if v}  # Remove None values
            
            result = self.collection.update_one(
                {'profile_url': speaker_data['profile_url']},
                {
                    '$set': speaker_data,
                    '$setOnInsert': {'created_at': datetime.utcnow()}
                },
                upsert=True
            )
            
            if result.upserted_id:
                self.stats['successful'] += 1
                logging.info(f"Inserted new speaker: {speaker_data.get('name', 'Unknown')}")
            else:
                self.stats['duplicates'] += 1
                logging.info(f"Updated existing speaker: {speaker_data.get('name', 'Unknown')}")
            
            return True
            
        except Exception as e:
            self.stats['failed'] += 1
            logging.error(f"Failed to save to MongoDB: {e}")
            return False
    
    def scrape_speaker_profile(self, url):
        """Scrape a single speaker profile and save to MongoDB"""
        soup = self.get_soup(url)
        if not soup:
            return None
        
        profile = self.extract_comprehensive_profile(soup, url)
        
        # Save to MongoDB if we have at least a name
        if profile and profile.get('name'):
            self.save_to_mongodb(profile)
            return profile
        
        return None
    
    def get_all_speaker_urls(self, max_pages=20):
        """Get all speaker URLs from search results"""
        all_urls = set()
        
        for offset in range(0, max_pages * 50, 50):
            url = f"{self.base_url}/search_results?offset={offset}"
            logging.info(f"Fetching speaker URLs from: {url}")
            
            soup = self.get_soup(url)
            if not soup:
                break
            
            speaker_urls = self.extract_speaker_urls_from_search(soup)
            if not speaker_urls:
                logging.info("No more speaker URLs found")
                break
            
            all_urls.update(speaker_urls)
            logging.info(f"Found {len(speaker_urls)} speaker URLs on page (total: {len(all_urls)})")
            
            time.sleep(SCRAPER_CONFIG['delay_between_requests'])
        
        return list(all_urls)
    
    def scrape_speakers_batch(self, urls, max_workers=None):
        """Scrape multiple speaker profiles in parallel"""
        if max_workers is None:
            max_workers = SCRAPER_CONFIG['max_workers']
        
        profiles = []
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_url = {executor.submit(self.scrape_speaker_profile, url): url for url in urls}
            
            for future in tqdm(as_completed(future_to_url), total=len(urls), desc="Scraping profiles"):
                url = future_to_url[future]
                try:
                    profile = future.result()
                    if profile:
                        profiles.append(profile)
                    self.stats['total_scraped'] += 1
                except Exception as e:
                    logging.error(f"Error scraping {url}: {e}")
                    self.stats['failed'] += 1
        
        return profiles
    
    def scrape_all(self, limit=None, batch_size=None):
        """Main method to scrape all speakers"""
        if batch_size is None:
            batch_size = SCRAPER_CONFIG['batch_size']
        
        logging.info("Starting enhanced speaker scraping with MongoDB integration...")
        
        # Get all speaker URLs
        logging.info("Collecting speaker URLs...")
        all_urls = self.get_all_speaker_urls()
        logging.info(f"Found {len(all_urls)} unique speaker URLs")
        
        if limit:
            all_urls = all_urls[:limit]
            logging.info(f"Limiting to {limit} speakers")
        
        # Scrape profiles in batches
        total_profiles = []
        
        for i in range(0, len(all_urls), batch_size):
            batch_urls = all_urls[i:i + batch_size]
            logging.info(f"Processing batch {i//batch_size + 1}/{(len(all_urls) + batch_size - 1)//batch_size}")
            
            batch_profiles = self.scrape_speakers_batch(batch_urls)
            total_profiles.extend(batch_profiles)
            
            # Small delay between batches
            time.sleep(SCRAPER_CONFIG['delay_between_requests'])
        
        # Print final statistics
        self.print_statistics()
        
        return total_profiles
    
    def print_statistics(self):
        """Print detailed scraping statistics"""
        logging.info("\n" + "="*60)
        logging.info("SCRAPING STATISTICS")
        logging.info("="*60)
        logging.info(f"Total URLs processed: {self.stats['total_scraped']}")
        logging.info(f"Successfully saved: {self.stats['successful']}")
        logging.info(f"Updated existing: {self.stats['duplicates']}")
        logging.info(f"Failed: {self.stats['failed']}")
        
        if self.stats['errors']:
            logging.info(f"\nErrors encountered: {len(self.stats['errors'])}")
            for error in self.stats['errors'][:5]:  # Show first 5 errors
                logging.info(f"  - {error['url']}: {error['error']}")
        
        # Get collection statistics
        try:
            total_in_db = self.collection.count_documents({})
            logging.info(f"\nTotal speakers in database: {total_in_db}")
            
            # Aggregation statistics
            pipeline = [
                {"$group": {
                    "_id": "$location",
                    "count": {"$sum": 1}
                }},
                {"$sort": {"count": -1}},
                {"$limit": 10}
            ]
            
            top_locations = list(self.collection.aggregate(pipeline))
            if top_locations:
                logging.info("\nTop 10 Locations:")
                for loc in top_locations:
                    logging.info(f"  {loc['_id']}: {loc['count']} speakers")
            
            # Topics statistics
            topic_pipeline = [
                {"$unwind": "$specialties"},
                {"$group": {
                    "_id": "$specialties",
                    "count": {"$sum": 1}
                }},
                {"$sort": {"count": -1}},
                {"$limit": 10}
            ]
            
            top_topics = list(self.collection.aggregate(topic_pipeline))
            if top_topics:
                logging.info("\nTop 10 Speaking Topics:")
                for topic in top_topics:
                    logging.info(f"  {topic['_id']}: {topic['count']} speakers")
        
        except Exception as e:
            logging.error(f"Error getting statistics: {e}")
    
    def export_sample(self, limit=5):
        """Export sample data for verification"""
        samples = list(self.collection.find().limit(limit))
        
        for sample in samples:
            sample['_id'] = str(sample['_id'])
            if 'scraped_at' in sample:
                sample['scraped_at'] = sample['scraped_at'].isoformat()
            if 'last_updated' in sample:
                sample['last_updated'] = sample['last_updated'].isoformat()
            if 'created_at' in sample:
                sample['created_at'] = sample['created_at'].isoformat()
        
        filename = f'sample_export_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
        with open(filename, 'w') as f:
            json.dump(samples, f, indent=2)
        
        logging.info(f"Exported {len(samples)} sample speakers to {filename}")
        return filename
    
    def close(self):
        """Close MongoDB connection"""
        try:
            self.client.close()
            logging.info("MongoDB connection closed")
        except Exception as e:
            logging.error(f"Error closing MongoDB connection: {e}")


def main():
    scraper = None
    try:
        # Initialize scraper
        scraper = EnhancedSpeakerScraper()
        
        # Scrape speakers (remove limit for full scrape)
        speakers = scraper.scrape_all(limit=None)
        
        logging.info(f"\nScraping completed! Total profiles scraped: {len(speakers)}")
        
        # Export sample for verification
        scraper.export_sample()
        
    except Exception as e:
        logging.error(f"Fatal error: {e}")
    finally:
        if scraper:
            scraper.close()


if __name__ == "__main__":
    main()