import requests
from bs4 import BeautifulSoup
import json
import time
import os
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import re
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, DuplicateKeyError
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class MongoDBSpeakerProfileScraper:
    """
    Module 3: Extracts detailed speaker profiles and stores them in MongoDB
    Features: MongoDB integration, high resumability, incremental updates, error recovery
    """
    
    def __init__(self, config_file: str = "config.json"):
        try:
            self.base_url = os.getenv("BASE_URL", "https://sessionize.com")
            
            # MongoDB connection
            self.mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
            self.db_name = os.getenv("MONGO_DB_NAME", "sessionize_scraper")
            
            # Load configuration first
            self.config = self.load_config(config_file)
            
            # Setup logging early with error handling
            self.setup_logging()
            
            # Log the start of initialization
            logging.info("Starting MongoDB Speaker Profile Scraper initialization")
            
            # Setup MongoDB connection
            self.setup_mongodb()
            
            # Setup session with proper headers
            self.session = self.setup_session()
            
            # Generate run ID for this execution
            self.run_id = self.generate_run_id()
            
            logging.info("MongoDB Speaker Profile Scraper initialized successfully")
            
        except Exception as e:
            print(f"Error during initialization: {e}")
            # Ensure basic logging is available
            logging.basicConfig(level=logging.INFO)
            logging.error(f"Initialization failed: {e}")
            raise
    
    def load_config(self, config_file: str) -> Dict:
        """Load configuration from file or use defaults"""
        default_config = {
            "rate_limiting": {
                "delay_between_requests": 1,
                "max_concurrent_requests": 3
            },
            "error_handling": {
                "max_retries": 3,
                "backoff_factor": 2,
                "timeout": 30
            },
            "output": {
                "log_directory": "./logs"
            },
            "resumability": {
                "checkpoint_frequency": 10,
                "max_failed_attempts": 5,
                "profile_update_days": 7
            },
            "batch_processing": {
                "batch_size": 20,           # Number of profiles to save in each batch
                "max_batch_wait_time": 300  # Maximum time to wait before forcing a batch save (seconds)
            }
        }        
        if os.path.exists(config_file):
            try:
                with open(config_file, 'r') as f:
                    user_config = json.load(f)
                    default_config.update(user_config)
            except Exception as e:
                logging.warning(f"Could not load config file {config_file}: {e}")
        
        return default_config
    
    def setup_logging(self):
        """Setup logging configuration with better error handling"""
        try:
            log_dir = self.config["output"]["log_directory"]
            os.makedirs(log_dir, exist_ok=True)
            
            log_file = os.path.join(log_dir, f"module3_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
            
            # Clear any existing handlers
            for handler in logging.root.handlers[:]:
                logging.root.removeHandler(handler)
            
            # Configure logging with force=True to override existing config
            logging.basicConfig(
                level=logging.INFO,
                format='%(asctime)s - %(levelname)s - %(message)s',
                handlers=[
                    logging.FileHandler(log_file, encoding='utf-8'),
                    logging.StreamHandler()
                ],
                force=True  # This ensures the configuration is applied
            )
            
            # Test logging
            logging.info(f"Logging initialized successfully. Log file: {log_file}")
            print(f"Log file created at: {log_file}")
            
        except Exception as e:
            print(f"Failed to setup logging: {e}")
            # Fallback to basic console logging
            logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    def generate_run_id(self) -> str:
        """Generate unique run ID"""
        return datetime.now().strftime('%Y%m%d_%H%M%S')
    
    def setup_mongodb(self):
        """Setup MongoDB connection and collections"""
        try:
            self.client = MongoClient(self.mongo_uri)
            self.db = self.client[self.db_name]
            
            # Test connection
            self.client.admin.command('ping')
            logging.info("Connected to MongoDB successfully")
            
            # Setup collections
            self.speakers_collection = self.db.speakers
            self.speaker_profiles_collection = self.db.speaker_profiles
            self.scraper_state_collection = self.db.scraper_state
            
            # Create indexes for better performance
            self.speaker_profiles_collection.create_index("username", unique=True)
            self.speaker_profiles_collection.create_index("metadata.scraped_at")
            self.speaker_profiles_collection.create_index("metadata.run_id")
            self.scraper_state_collection.create_index("module")
            
        except ConnectionFailure as e:
            logging.error(f"Failed to connect to MongoDB: {e}")
            raise
    
    def setup_session(self) -> requests.Session:
        """Setup requests session with proper headers"""
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        })
        return session
    
    def load_state_from_mongodb(self) -> Dict:
        """Load previous state from MongoDB for resumability"""
        try:
            state_doc = self.scraper_state_collection.find_one({"module": "speaker_profile_scraper"})
            if state_doc:
                # Remove MongoDB ObjectId
                if '_id' in state_doc:
                    del state_doc['_id']
                return state_doc
        except Exception as e:
            logging.warning(f"Could not load state from MongoDB: {e}")
        
        return {
            "module": "speaker_profile_scraper",
            "processed_speakers": [],
            "failed_speakers": {},
            "current_speaker": None,
            "total_speakers_processed": 0,
            "total_speakers_found": 0,
            "last_run": None,
            "run_id": self.run_id
        }
    
    def save_state_to_mongodb(self, processed_speakers: List[str], current_speaker: str = None, 
                             total_processed: int = 0, total_found: int = 0, failed_speakers: Dict = None):
        """Save current state to MongoDB for resumability"""
        state_data = {
            "module": "speaker_profile_scraper",
            "processed_speakers": processed_speakers,
            "failed_speakers": failed_speakers or {},
            "current_speaker": current_speaker,
            "total_speakers_processed": total_processed,
            "total_speakers_found": total_found,
            "last_run": datetime.now(),
            "run_id": self.run_id
        }
        
        try:
            self.scraper_state_collection.replace_one(
                {"module": "speaker_profile_scraper"},
                state_data,
                upsert=True
            )
        except Exception as e:
            logging.error(f"Could not save state to MongoDB: {e}")
    
    def load_speakers_from_mongodb(self) -> List[Dict]:
        """Load speakers data from MongoDB"""
        try:
            speakers = list(self.speakers_collection.find({}))
            
            # Remove MongoDB ObjectId for processing
            for speaker in speakers:
                if '_id' in speaker:
                    del speaker['_id']
            
            logging.info(f"Loaded {len(speakers)} speakers from MongoDB")
            return speakers
            
        except Exception as e:
            raise Exception(f"Could not load speakers from MongoDB: {e}")
    
    def should_update_speaker(self, username: str) -> bool:
        """Determine if a speaker profile should be updated"""
        try:
            existing_profile = self.speaker_profiles_collection.find_one({"username": username})
            
            if not existing_profile:
                return True  # New speaker, needs scraping
            
            # Check if profile is older than configured days
            if 'metadata' in existing_profile and 'scraped_at' in existing_profile['metadata']:
                scraped_at = existing_profile['metadata']['scraped_at']
                if isinstance(scraped_at, str):
                    scraped_date = datetime.fromisoformat(scraped_at.replace('Z', '+00:00'))
                else:
                    scraped_date = scraped_at
                
                days_old = (datetime.now() - scraped_date.replace(tzinfo=None)).days
                update_threshold = self.config["resumability"]["profile_update_days"]
                
                if days_old > update_threshold:
                    return True
            
            return False
            
        except Exception as e:
            logging.warning(f"Error checking update status for {username}: {e}")
            return True  # Default to update on error
    
    def retry_with_backoff(self, func, *args, **kwargs):
        """Retry function with exponential backoff"""
        max_retries = self.config["error_handling"]["max_retries"]
        backoff_factor = self.config["error_handling"]["backoff_factor"]
        
        for attempt in range(max_retries):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if attempt == max_retries - 1:
                    logging.error(f"Final attempt failed for {func.__name__}: {e}")
                    raise
                
                wait_time = backoff_factor ** attempt
                logging.warning(f"Attempt {attempt + 1} failed for {func.__name__}: {e}. Retrying in {wait_time}s...")
                time.sleep(wait_time)
    
    def fetch_speaker_profile(self, speaker_url: str) -> BeautifulSoup:
        """Fetch and parse a speaker profile page"""
        def _fetch():
            response = self.session.get(
                speaker_url,
                timeout=self.config["error_handling"]["timeout"]
            )
            response.raise_for_status()
            return BeautifulSoup(response.content, 'html.parser')
        
        return self.retry_with_backoff(_fetch)
    
    def extract_speaker_name(self, soup: BeautifulSoup) -> str:
        """Extract speaker name from profile page"""
        name_element = soup.select_one('h1.c-s-speaker-info__name')
        return name_element.get_text(strip=True) if name_element else None
    
    def extract_speaker_tagline(self, soup: BeautifulSoup) -> str:
        """Extract speaker tagline"""
        tagline_element = soup.select_one('p.c-s-speaker-info__tagline')
        return tagline_element.get_text(strip=True) if tagline_element else None
    
    def extract_speaker_bio(self, soup: BeautifulSoup) -> str:
        """Extract speaker biography"""
        bio_element = soup.select_one('.c-s-speaker-info__bio p')
        return bio_element.get_text(strip=True) if bio_element else None
    
    def extract_speaker_location(self, soup: BeautifulSoup) -> str:
        """Extract speaker location"""
        location_element = soup.select_one('p.c-s-speaker-info__location')
        if location_element:
            location_text = location_element.get_text(strip=True)
            return location_text
        return None
    
    def extract_profile_picture(self, soup: BeautifulSoup) -> str:
        """Extract profile picture URL"""
        img_element = soup.select_one('.c-s-speaker-info__avatar img')
        return img_element.get('src') if img_element else None
    
    def extract_social_links(self, soup: BeautifulSoup) -> Dict:
        """Extract social media and other links with comprehensive platform detection"""
        links = {}
        processed_urls = set()  # Track processed URLs to prevent duplicates
        
        link_elements = soup.select('.c-s-links__link')
        
        for link in link_elements:
            href = link.get('href', '')
            label_element = link.select_one('.o-label')
            
            if not label_element or not href:
                continue
            
            # Skip if URL already processed (prevents duplicates)
            if href in processed_urls:
                continue
            processed_urls.add(href)
            
            label = label_element.get_text(strip=True)
            
            # Comprehensive platform detection
            platform_detected = False
            
            # Social Media Platforms
            if any(domain in href.lower() for domain in ['twitter.com', 'x.com']):
                links['twitter'] = {'url': href, 'handle': label}
                platform_detected = True
            elif 'linkedin.com' in href.lower():
                links['linkedin'] = {'url': href, 'label': label}
                platform_detected = True
            elif 'github.com' in href.lower():
                links['github'] = {'url': href, 'handle': label}
                platform_detected = True
            elif 'instagram.com' in href.lower():
                links['instagram'] = {'url': href, 'handle': label}
                platform_detected = True
            elif any(domain in href.lower() for domain in ['facebook.com', 'fb.com']):
                links['facebook'] = {'url': href, 'label': label}
                platform_detected = True
            elif 'youtube.com' in href.lower() or 'youtu.be' in href.lower():
                links['youtube'] = {'url': href, 'channel': label}
                platform_detected = True
            elif 'tiktok.com' in href.lower():
                links['tiktok'] = {'url': href, 'handle': label}
                platform_detected = True
            elif 'mastodon' in href.lower():
                links['mastodon'] = {'url': href, 'handle': label}
                platform_detected = True
            elif 'discord' in href.lower():
                links['discord'] = {'url': href, 'server': label}
                platform_detected = True
            elif 'telegram' in href.lower():
                links['telegram'] = {'url': href, 'channel': label}
                platform_detected = True
            elif 'whatsapp' in href.lower():
                links['whatsapp'] = {'url': href, 'contact': label}
                platform_detected = True
            elif 'snapchat.com' in href.lower():
                links['snapchat'] = {'url': href, 'handle': label}
                platform_detected = True
            elif 'pinterest.com' in href.lower():
                links['pinterest'] = {'url': href, 'profile': label}
                platform_detected = True
            elif 'reddit.com' in href.lower():
                links['reddit'] = {'url': href, 'profile': label}
                platform_detected = True
            elif 'stackoverflow.com' in href.lower():
                links['stackoverflow'] = {'url': href, 'profile': label}
                platform_detected = True
            elif 'medium.com' in href.lower() or href.lower().endswith('.medium.com'):
                links['medium'] = {'url': href, 'profile': label}
                platform_detected = True
            elif 'dev.to' in href.lower():
                links['dev_to'] = {'url': href, 'profile': label}
                platform_detected = True
            elif 'hashnode' in href.lower():
                links['hashnode'] = {'url': href, 'blog': label}
                platform_detected = True
            elif 'substack.com' in href.lower():
                links['substack'] = {'url': href, 'newsletter': label}
                platform_detected = True
            elif 'twitch.tv' in href.lower():
                links['twitch'] = {'url': href, 'channel': label}
                platform_detected = True
            elif 'vimeo.com' in href.lower():
                links['vimeo'] = {'url': href, 'channel': label}
                platform_detected = True
            elif 'behance.net' in href.lower():
                links['behance'] = {'url': href, 'portfolio': label}
                platform_detected = True
            elif 'dribbble.com' in href.lower():
                links['dribbble'] = {'url': href, 'portfolio': label}
                platform_detected = True
            elif 'codepen.io' in href.lower():
                links['codepen'] = {'url': href, 'profile': label}
                platform_detected = True
            elif 'gitlab.com' in href.lower():
                links['gitlab'] = {'url': href, 'profile': label}
                platform_detected = True
            elif 'bitbucket.org' in href.lower():
                links['bitbucket'] = {'url': href, 'profile': label}
                platform_detected = True
            elif 'slack.com' in href.lower():
                links['slack'] = {'url': href, 'workspace': label}
                platform_detected = True
            elif 'skype.com' in href.lower() or href.startswith('skype:'):
                links['skype'] = {'url': href, 'contact': label}
                platform_detected = True
            elif 'zoom.us' in href.lower():
                links['zoom'] = {'url': href, 'meeting': label}
                platform_detected = True
            elif 'clubhouse.com' in href.lower():
                links['clubhouse'] = {'url': href, 'profile': label}
                platform_detected = True
            elif 'patreon.com' in href.lower():
                links['patreon'] = {'url': href, 'page': label}
                platform_detected = True
            elif 'ko-fi.com' in href.lower():
                links['ko_fi'] = {'url': href, 'page': label}
                platform_detected = True
            elif 'buymeacoffee.com' in href.lower():
                links['buymeacoffee'] = {'url': href, 'page': label}
                platform_detected = True
            elif 'paypal.me' in href.lower():
                links['paypal'] = {'url': href, 'donation': label}
                platform_detected = True
            elif 'calendly.com' in href.lower():
                links['calendly'] = {'url': href, 'booking': label}
                platform_detected = True
            elif 'cal.com' in href.lower():
                links['cal_com'] = {'url': href, 'booking': label}
                platform_detected = True
            elif 'linktree' in href.lower():
                links['linktree'] = {'url': href, 'profile': label}
                platform_detected = True
            elif 'linktr.ee' in href.lower():
                links['linktree'] = {'url': href, 'profile': label}
                platform_detected = True
            elif 'bio.link' in href.lower():
                links['bio_link'] = {'url': href, 'profile': label}
                platform_detected = True
            elif 'carrd.co' in href.lower():
                links['carrd'] = {'url': href, 'page': label}
                platform_detected = True
            
            # Professional Platforms
            elif 'crunchbase.com' in href.lower():
                links['crunchbase'] = {'url': href, 'profile': label}
                platform_detected = True
            elif 'angel.co' in href.lower() or 'wellfound.com' in href.lower():
                links['angellist'] = {'url': href, 'profile': label}
                platform_detected = True
            elif 'producthunt.com' in href.lower():
                links['product_hunt'] = {'url': href, 'profile': label}
                platform_detected = True
            elif 'ycombinator.com' in href.lower():
                links['ycombinator'] = {'url': href, 'profile': label}
                platform_detected = True
            elif 'f6s.com' in href.lower():
                links['f6s'] = {'url': href, 'profile': label}
                platform_detected = True
            elif 'xing.com' in href.lower():
                links['xing'] = {'url': href, 'profile': label}
                platform_detected = True
            elif 'researchgate.net' in href.lower():
                links['researchgate'] = {'url': href, 'profile': label}
                platform_detected = True
            elif 'academia.edu' in href.lower():
                links['academia'] = {'url': href, 'profile': label}
                platform_detected = True
            elif 'orcid.org' in href.lower():
                links['orcid'] = {'url': href, 'profile': label}
                platform_detected = True
            elif 'scholar.google' in href.lower():
                links['google_scholar'] = {'url': href, 'profile': label}
                platform_detected = True
            elif 'mvp.microsoft.com' in href.lower():
                links['microsoft_mvp'] = {'url': href, 'profile': label}
                platform_detected = True
            elif 'rd.microsoft.com' in href.lower():
                links['microsoft_rd'] = {'url': href, 'profile': label}
                platform_detected = True
            
            # Content & Learning Platforms
            elif 'udemy.com' in href.lower():
                links['udemy'] = {'url': href, 'instructor': label}
                platform_detected = True
            elif 'coursera.org' in href.lower():
                links['coursera'] = {'url': href, 'instructor': label}
                platform_detected = True
            elif 'pluralsight.com' in href.lower():
                links['pluralsight'] = {'url': href, 'author': label}
                platform_detected = True
            elif 'egghead.io' in href.lower():
                links['egghead'] = {'url': href, 'instructor': label}
                platform_detected = True
            elif 'teachable.com' in href.lower():
                links['teachable'] = {'url': href, 'school': label}
                platform_detected = True
            elif 'thinkific.com' in href.lower():
                links['thinkific'] = {'url': href, 'school': label}
                platform_detected = True
            elif 'gumroad.com' in href.lower():
                links['gumroad'] = {'url': href, 'store': label}
                platform_detected = True
            elif 'leanpub.com' in href.lower():
                links['leanpub'] = {'url': href, 'author': label}
                platform_detected = True
            elif 'amazon.com' in href.lower() and ('author' in href.lower() or 'dp/' in href.lower()):
                links['amazon_author'] = {'url': href, 'books': label}
                platform_detected = True
            elif 'goodreads.com' in href.lower():
                links['goodreads'] = {'url': href, 'author': label}
                platform_detected = True
            
            # Podcast Platforms
            elif 'spotify.com' in href.lower() and ('podcast' in href.lower() or 'show/' in href.lower()):
                links['spotify_podcast'] = {'url': href, 'podcast': label}
                platform_detected = True
            elif 'anchor.fm' in href.lower():
                links['anchor'] = {'url': href, 'podcast': label}
                platform_detected = True
            elif 'podcasts.apple.com' in href.lower():
                links['apple_podcasts'] = {'url': href, 'podcast': label}
                platform_detected = True
            elif 'podcasts.google.com' in href.lower():
                links['google_podcasts'] = {'url': href, 'podcast': label}
                platform_detected = True
            elif 'overcast.fm' in href.lower():
                links['overcast'] = {'url': href, 'podcast': label}
                platform_detected = True
            elif 'pocketcasts.com' in href.lower():
                links['pocket_casts'] = {'url': href, 'podcast': label}
                platform_detected = True
            elif 'castbox.fm' in href.lower():
                links['castbox'] = {'url': href, 'podcast': label}
                platform_detected = True
            
            # Check for specific icon types if platform not detected by URL
            elif not platform_detected:
                if link.select_one('.o-icon-pen'):  # Blog icon
                    links['blog'] = {'url': href, 'label': label}
                    platform_detected = True
                elif link.select_one('.o-icon-building'):  # Company icon
                    links['company'] = {'url': href, 'label': label}
                    platform_detected = True
                elif link.select_one('.o-icon-globe') or link.select_one('.o-icon-link'):  # Website icon
                    links['website'] = {'url': href, 'label': label}
                    platform_detected = True
                elif link.select_one('.o-icon-envelope') or 'mailto:' in href:  # Email icon
                    links['email'] = {'url': href, 'contact': label}
                    platform_detected = True
                elif link.select_one('.o-icon-phone') or href.startswith('tel:'):  # Phone icon
                    links['phone'] = {'url': href, 'contact': label}
                    platform_detected = True
            
            # If still not detected, add to other category (but avoid duplicates)
            if not platform_detected:
                if 'other' not in links:
                    links['other'] = []
                
                # Check if this URL is already in other category
                url_already_exists = any(item['url'] == href for item in links['other'])
                if not url_already_exists:
                    links['other'].append({'url': href, 'label': label})
        
        return links
    
    def extract_expertise_areas(self, soup: BeautifulSoup) -> List[str]:
        """Extract areas of expertise"""
        expertise_areas = []
        
        expertise_section = soup.select('.c-s-speaker-info__group--industry .c-s-tags__item')
        
        for item in expertise_section:
            text = item.get_text(strip=True)
            if text:
                expertise_areas.append(text)
        
        return expertise_areas
    
    def extract_topics(self, soup: BeautifulSoup) -> List[str]:
        """Extract speaking topics"""
        topics = []
        
        topics_section = soup.select('.c-s-speaker-info__group:not(.c-s-speaker-info__group--industry) .c-s-tags__item')
        
        for item in topics_section:
            text = item.get_text(strip=True)
            if text:
                topics.append(text)
        
        return topics
    
    def extract_sessions(self, soup: BeautifulSoup) -> List[Dict]:
        """Extract session information"""
        sessions = []
        
        session_elements = soup.select('.c-s-session')
        
        for session in session_elements:
            title_element = session.select_one('.c-s-session__title a')
            summary_element = session.select_one('.c-s-session__summary')
            
            session_data = {
                'title': title_element.get_text(strip=True) if title_element else None,
                'url': title_element.get('href') if title_element else None,
                'summary': summary_element.get_text(strip=True) if summary_element else None
            }
            
            if session_data['title']:
                sessions.append(session_data)
        
        return sessions
    
    def extract_events(self, soup: BeautifulSoup) -> List[Dict]:
        """Extract speaking events"""
        events = []
        
        event_elements = soup.select('.c-s-event')
        
        for event in event_elements:
            name_element = event.select_one('.c-s-event__name')
            date_element = event.select_one('.c-s-event__meta--date')
            location_element = event.select_one('.c-s-event__meta--location')
            session_element = event.select_one('.c-s-event__session')
            
            event_data = {
                'name': name_element.get_text(strip=True) if name_element else None,
                'url': name_element.get('href') if name_element else None,
                'date': date_element.get_text(strip=True) if date_element else None,
                'location': location_element.get_text(strip=True) if location_element else None,
                'sessions': session_element.get_text(strip=True) if session_element else None,
                'is_sessionize_event': bool(event.select_one('.c-s-event__type[title="Sessionize Event"]'))
            }
            
            if event_data['name']:
                events.append(event_data)
        
        return events
    
    def scrape_speaker_profile(self, speaker_data: Dict) -> Dict:
        """Scrape detailed information from a speaker profile"""
        speaker_url = speaker_data['url']
        speaker_username = speaker_data['username']
        
        try:
            soup = self.fetch_speaker_profile(speaker_url)
            
            profile_data = {
                'username': speaker_username,
                'basic_info': {
                    'name': self.extract_speaker_name(soup),
                    'username': speaker_username,
                    'url': speaker_url,
                    'tagline': self.extract_speaker_tagline(soup),
                    'bio': self.extract_speaker_bio(soup),
                    'location': self.extract_speaker_location(soup),
                    'profile_picture': self.extract_profile_picture(soup)
                },
                'professional_info': {
                    'expertise_areas': self.extract_expertise_areas(soup),
                    'topics': self.extract_topics(soup),
                    'social_links': self.extract_social_links(soup)
                },
                'speaking_history': {
                    'sessions': self.extract_sessions(soup),
                    'events': self.extract_events(soup)
                },
                'metadata': {
                    'scraped_at': datetime.now().isoformat(),
                    'source_categories': speaker_data.get('categories', []),
                    'run_id': self.run_id
                }
            }
            
            return profile_data
            
        except Exception as e:
            logging.error(f"Error scraping speaker profile {speaker_url}: {e}")
            return None
    
    def save_speaker_profiles_batch_to_mongodb(self, profiles_batch: List[Dict]) -> tuple:
        """Save multiple speaker profiles to MongoDB in a single batch operation"""
        if not profiles_batch:
            return 0, 0
        
        saved_count = 0
        updated_count = 0
        
        try:
            # Prepare bulk operations
            bulk_operations = []
            
            for profile_data in profiles_batch:
                # Create upsert operation for each profile
                bulk_operations.append(
                    {
                        "replaceOne": {
                            "filter": {"username": profile_data["username"]},
                            "replacement": profile_data,
                            "upsert": True
                        }
                    }
                )
            
            # Execute bulk operation
            if bulk_operations:
                result = self.speaker_profiles_collection.bulk_write(bulk_operations, ordered=False)
                
                saved_count = result.upserted_count
                updated_count = result.modified_count
                
                logging.info(f"Batch saved: {saved_count} new profiles, {updated_count} updated profiles")
                
                # Log individual speaker names for tracking
                for profile in profiles_batch:
                    username = profile.get('username', 'unknown')
                    name = profile.get('basic_info', {}).get('name', 'Unknown')
                    logging.debug(f"Batch processed: {name} ({username})")
            
            return saved_count, updated_count
            
        except Exception as e:
            logging.error(f"Error in batch save operation: {e}")
            # Fallback to individual saves if batch fails
            logging.info("Falling back to individual saves...")
            return self.fallback_individual_saves(profiles_batch)

    def fallback_individual_saves(self, profiles_batch: List[Dict]) -> tuple:
        """Fallback method to save profiles individually if batch operation fails"""
        saved_count = 0
        updated_count = 0
        
        for profile_data in profiles_batch:
            try:
                result = self.speaker_profiles_collection.replace_one(
                    {"username": profile_data["username"]},
                    profile_data,
                    upsert=True
                )
                
                if result.upserted_id:
                    saved_count += 1
                elif result.modified_count > 0:
                    updated_count += 1
                    
            except Exception as e:
                logging.error(f"Error saving individual profile {profile_data.get('username', 'unknown')}: {e}")
                continue
        
        return saved_count, updated_count
    
    def run(self) -> Dict:
        """Main execution method with batch processing"""
        logging.info("Starting MongoDB Speaker Profile Scraper with Batch Processing")
        
        try:
            # Load speakers data from MongoDB
            all_speakers = self.load_speakers_from_mongodb()
            
            if not all_speakers:
                logging.info("No speakers found in MongoDB. Please run Module 2 first.")
                return {"total_speakers": 0}
            
            # Filter speakers that need processing
            speakers_to_process = []
            for speaker in all_speakers:
                if self.should_update_speaker(speaker['username']):
                    speakers_to_process.append(speaker)
            
            if not speakers_to_process:
                logging.info("No speakers need processing. All profiles are up to date.")
                total_profiles = self.speaker_profiles_collection.count_documents({})
                return {"total_speakers": total_profiles, "new_speakers_scraped": 0}
            
            logging.info(f"Found {len(speakers_to_process)} speakers to process")
            
            # Load state from MongoDB
            state = self.load_state_from_mongodb()
            processed_speakers = set(state.get("processed_speakers", []))
            failed_speakers = state.get("failed_speakers", {})
            
            # Filter out already processed speakers for this run
            speakers_to_process = [
                s for s in speakers_to_process 
                if s['username'] not in processed_speakers or 
                failed_speakers.get(s['username'], 0) < self.config["resumability"]["max_failed_attempts"]
            ]
            
            logging.info(f"After filtering processed speakers: {len(speakers_to_process)} speakers remaining")
            
            # Initialize counters and batch processing
            total_speakers = len(speakers_to_process)
            scraped_count = 0
            updated_count = 0
            failed_count = 0
            
            # Batch processing variables
            batch_size = self.config["batch_processing"]["batch_size"]
            profiles_batch = []
            batch_start_time = datetime.now()
            max_batch_wait_time = self.config["batch_processing"]["max_batch_wait_time"]
            
            # Process speakers
            for i, speaker in enumerate(speakers_to_process, 1):
                speaker_username = speaker['username']
                
                try:
                    logging.info(f"Processing speaker {i}/{total_speakers}: {speaker['name']} ({speaker_username})")
                    
                    # Scrape speaker profile
                    profile_data = self.scrape_speaker_profile(speaker)
                    
                    if profile_data:
                        # Add to batch instead of saving immediately
                        profiles_batch.append(profile_data)
                        
                        # Mark as processed
                        processed_speakers.add(speaker_username)
                        
                        # Remove from failed speakers if it was there
                        if speaker_username in failed_speakers:
                            del failed_speakers[speaker_username]
                        
                        logging.info(f"Added to batch: {speaker['name']} (batch size: {len(profiles_batch)})")
                        
                    else:
                        # Track failed attempt
                        failed_speakers[speaker_username] = failed_speakers.get(speaker_username, 0) + 1
                        failed_count += 1
                        logging.warning(f"Failed to scrape speaker: {speaker['name']} (attempt {failed_speakers[speaker_username]})")
                    
                    # Check if batch is ready to save
                    current_time = datetime.now()
                    time_elapsed = (current_time - batch_start_time).total_seconds()
                    
                    should_save_batch = (
                        len(profiles_batch) >= batch_size or  # Batch size reached
                        i == total_speakers or               # Last speaker
                        time_elapsed >= max_batch_wait_time  # Max wait time exceeded
                    )
                    
                    if should_save_batch and profiles_batch:
                        logging.info(f"Saving batch of {len(profiles_batch)} profiles to MongoDB...")
                        
                        # Check which profiles are new vs updates
                        new_profiles = []
                        updated_profiles = []
                        
                        for profile in profiles_batch:
                            existing_profile = self.speaker_profiles_collection.find_one(
                                {"username": profile["username"]}
                            )
                            if existing_profile:
                                updated_profiles.append(profile)
                            else:
                                new_profiles.append(profile)
                        
                        # Save batch to MongoDB
                        batch_saved, batch_updated = self.save_speaker_profiles_batch_to_mongodb(profiles_batch)
                        
                        # Update counters
                        scraped_count += len(new_profiles)
                        updated_count += len(updated_profiles)
                        
                        logging.info(f"Batch completed: {batch_saved} new, {batch_updated} updated")
                        
                        # Clear batch and reset timer
                        profiles_batch = []
                        batch_start_time = datetime.now()
                    
                    # Save checkpoint periodically (but not on every speaker)
                    if i % self.config["resumability"]["checkpoint_frequency"] == 0:
                        self.save_state_to_mongodb(
                            processed_speakers=list(processed_speakers),
                            current_speaker=None,
                            total_processed=scraped_count + updated_count,
                            total_found=total_speakers,
                            failed_speakers=failed_speakers
                        )
                        logging.info(f"Checkpoint saved after processing {i} speakers")
                    
                    # Rate limiting
                    time.sleep(self.config["rate_limiting"]["delay_between_requests"])
                    
                except Exception as e:
                    logging.error(f"Error processing speaker {speaker_username}: {e}")
                    failed_speakers[speaker_username] = failed_speakers.get(speaker_username, 0) + 1
                    failed_count += 1
                    continue
            
            # Save any remaining profiles in the final batch
            if profiles_batch:
                logging.info(f"Saving final batch of {len(profiles_batch)} profiles...")
                final_saved, final_updated = self.save_speaker_profiles_batch_to_mongodb(profiles_batch)
                
                # Update final counters (these should already be counted, but just in case)
                logging.info(f"Final batch completed: {final_saved} new, {final_updated} updated")
            
            # Save final state
            self.save_state_to_mongodb(
                processed_speakers=list(processed_speakers),
                current_speaker=None,
                total_processed=scraped_count + updated_count,
                total_found=total_speakers,
                failed_speakers=failed_speakers
            )
            
            # Get final statistics from MongoDB
            total_profiles_in_db = self.speaker_profiles_collection.count_documents({})
            
            # Print summary
            print(f"\n=== MongoDB Speaker Profile Scraping with Batch Processing Completed ===")
            print(f"Total speaker profiles in database: {total_profiles_in_db}")
            print(f"New speakers scraped this run: {scraped_count}")
            print(f"Existing speakers updated: {updated_count}")
            print(f"Failed speakers: {failed_count}")
            print(f"Batch size used: {batch_size}")
            
            if failed_count > 0:
                print(f"\nFailed speakers (will retry in next run):")
                for username, attempts in failed_speakers.items():
                    print(f"  - {username}: {attempts} failed attempts")
            
            return {
                "total_speakers_in_database": total_profiles_in_db,
                "new_speakers_scraped": scraped_count,
                "speakers_updated": updated_count,
                "failed_speakers": failed_count,
                "batch_size_used": batch_size,
                "run_id": self.run_id
            }
            
        except Exception as e:
            logging.error(f"MongoDB Speaker Profile Scraper failed: {e}")
            raise
        
        finally:
            time.sleep(self.config["rate_limiting"]["delay_between_requests"])
            # Close MongoDB connection
            if hasattr(self, 'client'):
                self.client.close()

def main():
    """Main function to run the MongoDB speaker profile scraper"""
    try:
        scraper = MongoDBSpeakerProfileScraper()
        result = scraper.run()
        return result
    except Exception as e:
        print(f"Error running MongoDB Speaker Profile Scraper: {e}")
        return None

if __name__ == "__main__":
    main()
