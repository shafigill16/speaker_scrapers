#!/usr/bin/env python3
"""
SpeakerHub Scraper with Infinite Scroll Support
Extracts comprehensive speaker information and stores in MongoDB
"""

import logging
import time
import random
from datetime import datetime
from typing import List, Dict, Optional, Any
from dataclasses import dataclass, asdict
from urllib.parse import urljoin

from camoufox.sync_api import Camoufox
from bs4 import BeautifulSoup
from pymongo import MongoClient, UpdateOne
from pymongo.errors import ConnectionFailure, OperationFailure
import json


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('speakerhub_scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


@dataclass
class Speaker:
    """Speaker data model"""
    uid: str
    profile_url: str
    name: str
    first_name: str
    last_name: str
    speaker_type: str
    job_title: Optional[str] = None
    company: Optional[str] = None
    profile_picture: Optional[str] = None
    bio_summary: Optional[str] = None
    country: Optional[str] = None
    state: Optional[str] = None
    city: Optional[str] = None
    available_regions: List[str] = None
    languages: List[str] = None
    event_types: List[str] = None
    topics: List[str] = None
    scraped_at: datetime = None
    
    def __post_init__(self):
        if self.scraped_at is None:
            self.scraped_at = datetime.utcnow()
        if self.available_regions is None:
            self.available_regions = []
        if self.languages is None:
            self.languages = []
        if self.event_types is None:
            self.event_types = []
        if self.topics is None:
            self.topics = []


class MongoDBHandler:
    """MongoDB connection and operations handler"""
    
    def __init__(self, connection_string: str, database_name: str, collection_name: str):
        self.connection_string = connection_string
        self.database_name = database_name
        self.collection_name = collection_name
        self.client = None
        self.db = None
        self.collection = None
        
    def connect(self) -> bool:
        """Establish MongoDB connection"""
        try:
            self.client = MongoClient(self.connection_string, serverSelectionTimeoutMS=5000)
            # Test connection
            self.client.admin.command('ping')
            self.db = self.client[self.database_name]
            self.collection = self.db[self.collection_name]
            logger.info(f"Successfully connected to MongoDB: {self.database_name}.{self.collection_name}")
            return True
        except (ConnectionFailure, OperationFailure) as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            return False
    
    def upsert_speaker(self, speaker: Speaker) -> bool:
        """Insert or update speaker data"""
        try:
            speaker_dict = asdict(speaker)
            result = self.collection.update_one(
                {"uid": speaker.uid},
                {"$set": speaker_dict},
                upsert=True
            )
            if result.upserted_id:
                logger.debug(f"Inserted new speaker: {speaker.name} (UID: {speaker.uid})")
            else:
                logger.debug(f"Updated speaker: {speaker.name} (UID: {speaker.uid})")
            return True
        except Exception as e:
            logger.error(f"Failed to upsert speaker {speaker.name}: {e}")
            return False
    
    def bulk_upsert_speakers(self, speakers: List[Speaker]) -> int:
        """Bulk insert or update speakers"""
        if not speakers:
            return 0
            
        try:
            operations = []
            for speaker in speakers:
                # Convert to dict and handle datetime serialization
                speaker_dict = asdict(speaker)
                # Convert datetime to ISO format string for MongoDB
                if 'scraped_at' in speaker_dict and isinstance(speaker_dict['scraped_at'], datetime):
                    speaker_dict['scraped_at'] = speaker_dict['scraped_at'].isoformat()
                
                operations.append({
                    "filter": {"uid": speaker.uid},
                    "update": {"$set": speaker_dict},
                    "upsert": True
                })
            
            # Use update_many syntax
            from pymongo import UpdateOne
            bulk_operations = [UpdateOne(**op) for op in operations]
            
            result = self.collection.bulk_write(bulk_operations)
            logger.info(f"Bulk operation complete: {result.upserted_count} inserted, {result.modified_count} updated")
            return result.upserted_count + result.modified_count
        except Exception as e:
            logger.error(f"Bulk upsert failed: {e}")
            return 0
    
    def get_speaker_count(self) -> int:
        """Get total number of speakers in collection"""
        try:
            return self.collection.count_documents({})
        except Exception as e:
            logger.error(f"Failed to count speakers: {e}")
            return -1
    
    def close(self):
        """Close MongoDB connection"""
        if self.client:
            self.client.close()
            logger.info("MongoDB connection closed")


class SpeakerExtractor:
    """Extract speaker information from HTML elements"""
    
    def __init__(self, base_url: str = "https://speakerhub.com"):
        self.base_url = base_url
        self.event_type_mapping = {
            "event-type-conference": "Conference (Full-day Event)",
            "event-type-workshop": "Workshop (3+ hour event)",
            "event-type-session": "Session (1-2 hour event)",
            "event-type-moderator": "Moderator",
            "event-type-webinar": "Webinar (Virtual event)",
            "event-type-volunteer": "School (incl. charity)",
            "event-type-meetup": "Meetup",
            "event-type-panel": "Panel",
            "event-type-cert": "Certificate Program",
            "event-type-emcee": "Emcee"
        }
    
    def extract_speaker_from_card(self, card_element) -> Optional[Speaker]:
        """Extract speaker data from a speaker card element"""
        try:
            # Extract UID
            uid = card_element.get('data-uid')
            if not uid:
                logger.warning("No UID found for speaker card")
                return None
            
            # Extract basic info from hidden link
            link = card_element.find('a', class_='user-link')
            if not link:
                logger.warning(f"No user link found for UID: {uid}")
                return None
            
            profile_url = urljoin(self.base_url, link.get('href', ''))
            full_text = link.text.strip()
            
            # Parse name and type
            parts = full_text.split(' - ')
            name = parts[0] if parts else "Unknown"
            speaker_type = parts[1] if len(parts) > 1 else "Speaker"
            
            # Extract first and last name
            first_name_elem = card_element.find('div', {'itemprop': 'givenName'})
            last_name_elem = card_element.find('div', {'itemprop': 'familyName'})
            first_name = first_name_elem.text.strip() if first_name_elem else name.split()[0] if name.split() else ""
            last_name = last_name_elem.text.strip() if last_name_elem else name.split()[-1] if len(name.split()) > 1 else ""
            
            # Extract job and company
            job_elem = card_element.find('div', {'itemprop': 'jobTitle'})
            job_title = job_elem.text.strip() if job_elem else None
            
            company_elem = card_element.find('span', class_='company')
            company = company_elem.text.strip() if company_elem else None
            
            # Extract profile picture
            img_elem = card_element.find('img', {'itemprop': 'image'})
            profile_picture = img_elem.get('src') if img_elem else None
            if profile_picture and not profile_picture.startswith('http'):
                profile_picture = urljoin(self.base_url, profile_picture)
            
            # Extract bio summary
            bio_elem = card_element.find('div', class_='field-name-field-bio-summary')
            bio_summary = bio_elem.find('p').text.strip() if bio_elem and bio_elem.find('p') else None
            
            # Extract location
            country_elem = card_element.find('div', class_='field-name-field-country')
            country_text = country_elem.find('div', class_='field-item').text.strip() if country_elem else None
            
            country = None
            state = None
            if country_text:
                if '(' in country_text and ')' in country_text:
                    country = country_text.split('(')[0].strip()
                    state = country_text.split('(')[1].rstrip(')')
                else:
                    country = country_text
            
            city_elem = card_element.find('div', class_='field-name-field-user-city')
            city = city_elem.find('p').text.strip() if city_elem and city_elem.find('p') else None
            
            # Extract available regions
            available_elem = card_element.find('div', class_='field-name-field-user-available')
            available_regions = []
            if available_elem:
                for item in available_elem.find_all('div', class_='field-item'):
                    available_regions.append(item.text.strip())
            
            # Extract languages
            languages_elem = card_element.find('div', class_='field-name-field-languages')
            languages = []
            if languages_elem:
                for item in languages_elem.find_all('div', class_='field-item'):
                    languages.append(item.text.strip())
            
            # Extract event types
            event_types_elem = card_element.find('div', class_='field-name-field-event-type')
            event_types = []
            if event_types_elem:
                for icon in event_types_elem.find_all('i'):
                    classes = icon.get('class', [])
                    for cls in classes:
                        if cls in self.event_type_mapping:
                            event_types.append(self.event_type_mapping[cls])
            
            # Extract topics/tags
            tags_elem = card_element.find('div', class_='field-name-field-tags')
            topics = []
            if tags_elem:
                for tag_link in tags_elem.find_all('a', class_='value'):
                    topics.append(tag_link.text.strip())
            
            # Create Speaker object
            speaker = Speaker(
                uid=uid,
                profile_url=profile_url,
                name=name,
                first_name=first_name,
                last_name=last_name,
                speaker_type=speaker_type,
                job_title=job_title,
                company=company,
                profile_picture=profile_picture,
                bio_summary=bio_summary,
                country=country,
                state=state,
                city=city,
                available_regions=available_regions,
                languages=languages,
                event_types=event_types,
                topics=topics
            )
            
            return speaker
            
        except Exception as e:
            logger.error(f"Failed to extract speaker from card: {e}")
            return None


class SpeakerHubScraper:
    """Main scraper class with infinite scroll support"""
    
    def __init__(self, mongo_handler: MongoDBHandler, max_scroll_attempts: int = 50, no_content_threshold: int = 5):
        self.mongo_handler = mongo_handler
        self.extractor = SpeakerExtractor()
        self.max_scroll_attempts = max_scroll_attempts
        self.no_content_threshold = no_content_threshold
        self.scraped_uids = set()
        
    def setup_browser(self) -> Camoufox:
        """Setup and return configured browser instance"""
        logger.info("Setting up Camoufox browser...")
        browser = Camoufox(headless=True)
        return browser
    
    def human_like_delay(self, min_sec: float = 1, max_sec: float = 3):
        """Random delay to simulate human behavior"""
        delay = random.uniform(min_sec, max_sec)
        time.sleep(delay)
    
    def scroll_and_wait(self, page, scroll_pause_time: float = 2):
        """Scroll to bottom and wait for new content"""
        # Get current scroll height
        last_height = page.evaluate("document.body.scrollHeight")
        
        # Scroll to bottom with smooth behavior
        page.evaluate("""
            window.scrollTo({
                top: document.body.scrollHeight,
                behavior: 'smooth'
            });
        """)
        
        # Wait for new content to load
        time.sleep(scroll_pause_time)
        
        # Additional wait for lazy loading
        page.wait_for_timeout(5000)
        
        # Get new scroll height
        new_height = page.evaluate("document.body.scrollHeight")
        
        # Also check if loading indicator is visible
        is_loading = page.evaluate("""
            (() => {
                const loader = document.querySelector('.loader, .loading, .spinner, [class*="load"]');
                return loader && loader.offsetParent !== null;
            })();
        """)
        
        # Return whether new content was loaded or still loading
        return new_height > last_height or is_loading
    
    def extract_speakers_from_page(self, page) -> List[Speaker]:
        """Extract all speakers currently visible on page"""
        content = page.content()
        soup = BeautifulSoup(content, 'html.parser')
        
        # Find all speaker cards
        speaker_cards = soup.find_all('div', class_='user-speaker-card')
        
        speakers = []
        for card in speaker_cards:
            # Check if already scraped
            uid = card.get('data-uid')
            if uid and uid not in self.scraped_uids:
                speaker = self.extractor.extract_speaker_from_card(card)
                if speaker:
                    speakers.append(speaker)
                    self.scraped_uids.add(uid)
        
        return speakers
    
    def scrape_all_speakers(self) -> int:
        """Main scraping method with infinite scroll"""
        total_scraped = 0
        
        try:
            with self.setup_browser() as browser:
                page = browser.new_page()
                
                # Set headers
                page.set_extra_http_headers({
                    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "Accept-Language": "en-US,en;q=0.9",
                })
                
                logger.info("Navigating to SpeakerHub speakers page...")
                page.goto("https://speakerhub.com/speakers", wait_until="domcontentloaded")
                self.human_like_delay(3, 5)
                
                # Check if we're on the right page
                if "wwv" in page.url:
                    logger.error("Bot detection triggered! Cannot proceed.")
                    return 0
                
                logger.info("Successfully loaded speakers page. Starting infinite scroll...")
                
                scroll_count = 0
                no_new_content_count = 0
                batch_size = 50  # Save to DB every 50 speakers
                speakers_batch = []
                
                while scroll_count < self.max_scroll_attempts:
                    # Extract speakers from current view
                    new_speakers = self.extract_speakers_from_page(page)
                    
                    if new_speakers:
                        speakers_batch.extend(new_speakers)
                        logger.info(f"Extracted {len(new_speakers)} new speakers. Total unique: {len(self.scraped_uids)}")
                        no_new_content_count = 0
                        
                        # Save batch to MongoDB
                        if len(speakers_batch) >= batch_size:
                            saved_count = self.mongo_handler.bulk_upsert_speakers(speakers_batch)
                            total_scraped += saved_count
                            speakers_batch = []
                            logger.info(f"Saved batch to MongoDB. Total saved: {total_scraped}")
                    else:
                        no_new_content_count += 1
                        logger.debug(f"No new speakers found. Count: {no_new_content_count}")
                    
                    # Stop if no new content for several scrolls
                    if no_new_content_count >= self.no_content_threshold:
                        logger.info(f"No new content after {self.no_content_threshold} scrolls. Assuming end of list.")
                        break
                    
                    # Scroll and check for new content
                    has_new_content = self.scroll_and_wait(page, random.uniform(2, 4))
                    
                    if not has_new_content:
                        no_new_content_count += 1
                    
                    scroll_count += 1
                    
                    # Random longer pause occasionally
                    if scroll_count % 10 == 0:
                        logger.info(f"Scroll {scroll_count}: Taking a longer break...")
                        self.human_like_delay(5, 8)
                
                # Save any remaining speakers
                if speakers_batch:
                    saved_count = self.mongo_handler.bulk_upsert_speakers(speakers_batch)
                    total_scraped += saved_count
                    logger.info(f"Saved final batch to MongoDB. Total saved: {total_scraped}")
                
                logger.info(f"Scraping complete! Total speakers scraped: {len(self.scraped_uids)}")
                
        except Exception as e:
            logger.error(f"Scraping failed: {e}")
            import traceback
            traceback.print_exc()
        
        return total_scraped


def main():
    """Main execution function"""
    print("="*60)
    print("SpeakerHub Scraper with MongoDB Integration")
    print("="*60)
    
    # MongoDB configuration
    MONGO_CONNECTION = "mongodb://admin:dev2018@5.161.225.172:27017/?authSource=admin"
    DATABASE_NAME = "speakerhub_scraper"
    COLLECTION_NAME = "speakers"
    
    # Initialize MongoDB handler
    mongo_handler = MongoDBHandler(MONGO_CONNECTION, DATABASE_NAME, COLLECTION_NAME)
    
    # Connect to MongoDB
    if not mongo_handler.connect():
        print("Failed to connect to MongoDB. Exiting.")
        return
    
    # Get initial count
    initial_count = mongo_handler.get_speaker_count()
    print(f"\nInitial speaker count in database: {initial_count}")
    
    # Initialize and run scraper
    scraper = SpeakerHubScraper(mongo_handler)
    
    print("\nStarting scraper...")
    print("This will scroll through all speakers on the page.")
    print("Press Ctrl+C to stop at any time.\n")
    
    try:
        total_scraped = scraper.scrape_all_speakers()
        
        # Get final count
        final_count = mongo_handler.get_speaker_count()
        
        print("\n" + "="*60)
        print("Scraping Summary")
        print("="*60)
        print(f"Total speakers processed: {total_scraped}")
        print(f"Initial database count: {initial_count}")
        print(f"Final database count: {final_count}")
        print(f"New speakers added: {final_count - initial_count if initial_count >= 0 else 'Unknown'}")
        
    except KeyboardInterrupt:
        print("\n\nScraping interrupted by user.")
    finally:
        mongo_handler.close()
        print("\nScraper finished.")


if __name__ == "__main__":
    main()