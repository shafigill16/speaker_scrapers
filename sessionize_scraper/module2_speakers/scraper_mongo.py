import requests
from bs4 import BeautifulSoup
import json
import time
import os
import logging
from datetime import datetime
from typing import Dict, List, Optional
import re
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, DuplicateKeyError
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class MongoDBSpeakerURLCollector:
    """
    Module 2: Collects speaker profile URLs from each category page and stores them in MongoDB
    Features: MongoDB integration, pagination handling, error recovery, incremental speaker appending
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
            logging.info("Starting MongoDB Speaker URL Collector initialization")
            
            # Setup MongoDB connection
            self.setup_mongodb()
            
            # Setup session with proper headers
            self.session = self.setup_session()
            
            logging.info("MongoDB Speaker URL Collector initialized successfully")
            
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
            "pagination": {
                "max_pages_per_category": 50,
                "speakers_per_page": 30
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
    
    def setup_mongodb(self):
        """Setup MongoDB connection and collections"""
        try:
            self.client = MongoClient(self.mongo_uri)
            self.db = self.client[self.db_name]
            
            # Test connection
            self.client.admin.command('ping')
            logging.info("Connected to MongoDB successfully")
            
            # Setup collections
            self.categories_collection = self.db.categories
            self.speakers_collection = self.db.speakers
            
            # Create indexes for better performance
            self.speakers_collection.create_index("username", unique=True)
            self.speakers_collection.create_index("category_slug")
            self.speakers_collection.create_index("discovered_at")
            self.speakers_collection.create_index([("category_slug", 1), ("username", 1)])
            
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
    
    def setup_logging(self):
        """Setup logging configuration with better error handling"""
        try:
            log_dir = self.config["output"]["log_directory"]
            os.makedirs(log_dir, exist_ok=True)
            
            log_file = os.path.join(log_dir, f"module2_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
            
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
    
    def load_categories_from_mongodb(self) -> List[Dict]:
        """Load categories from MongoDB"""
        try:
            categories = list(self.categories_collection.find({}))
            
            # Remove MongoDB ObjectId for processing
            for category in categories:
                if '_id' in category:
                    del category['_id']
            
            logging.info(f"Loaded {len(categories)} categories from MongoDB")
            return categories
            
        except Exception as e:
            raise Exception(f"Could not load categories from MongoDB: {e}")
    
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
    
    def fetch_category_page(self, category_url: str, page: int = 1) -> BeautifulSoup:
        """Fetch and parse a category page"""
        def _fetch():
            # Construct URL with pagination
            if page > 1:
                url = f"{category_url}?page={page-1}"  # Sessionize uses 0-based pagination
            else:
                url = category_url
            
            response = self.session.get(
                url,
                timeout=self.config["error_handling"]["timeout"]
            )
            response.raise_for_status()
            return BeautifulSoup(response.content, 'html.parser')
        
        return self.retry_with_backoff(_fetch)
    
    def extract_speakers_from_page(self, soup: BeautifulSoup, category_slug: str) -> List[Dict]:
        """Extract speaker information from a category page"""
        speakers = []
        
        # Find speaker entries based on the HTML structure
        speaker_entries = soup.select('.c-entry.c-entry--speaker')
        
        for entry in speaker_entries:
            try:
                # Extract speaker name and URL
                title_element = entry.select_one('h3.c-entry__title a')
                if not title_element:
                    continue
                
                speaker_name = title_element.get_text(strip=True)
                speaker_href = title_element.get('href', '')
                
                if not speaker_name or not speaker_href:
                    continue
                
                # Build full URL
                speaker_url = f"{self.base_url}{speaker_href}" if speaker_href.startswith('/') else speaker_href
                
                # Extract username from href
                speaker_username = speaker_href.split('/')[-1] if '/' in speaker_href else speaker_href
                
                # Extract tagline
                tagline_element = entry.select_one('p.c-entry__tagline')
                tagline = tagline_element.get_text(strip=True) if tagline_element else None
                
                # Extract location - look for the first meta item with location icon
                location = None
                location_items = entry.select('.c-entry__meta-item')
                for meta_item in location_items:
                    if meta_item.select_one('.o-icon-map-marker-alt'):
                        location_element = meta_item.select_one('.c-entry__meta-value')
                        if location_element:
                            location = location_element.get_text(strip=True)
                            break
                
                # Extract events and sessions count
                events_count = None
                sessions_count = None
                
                for meta_item in location_items:
                    meta_value = meta_item.select_one('.c-entry__meta-value')
                    if meta_value:
                        text = meta_value.get_text(strip=True)
                        if 'event' in text.lower():
                            events_count = text
                        elif 'session' in text.lower():
                            sessions_count = text
                
                speaker_data = {
                    "name": speaker_name,
                    "username": speaker_username,
                    "url": speaker_url,
                    "href": speaker_href,
                    "tagline": tagline,
                    "location": location,
                    "events_count": events_count,
                    "sessions_count": sessions_count,
                    "category_slug": category_slug,
                    "discovered_at": datetime.now(),
                    "last_updated": datetime.now()
                }
                
                speakers.append(speaker_data)
                logging.debug(f"Extracted speaker: {speaker_name} ({speaker_username})")
                
            except Exception as e:
                logging.error(f"Error extracting speaker from entry: {e}")
                continue
        
        return speakers
    
    def get_pagination_info(self, soup: BeautifulSoup) -> Dict:
        """Extract pagination information from the page"""
        pagination_info = {
            "current_page": 1,
            "total_pages": 1,
            "total_speakers": 0,
            "has_next": False
        }
        
        try:
            # Look for pagination navigation
            nav_page = soup.select_one('.c-nav-page')
            if not nav_page:
                return pagination_info
            
            # Extract total speakers count
            page_info = nav_page.select_one('.c-nav-page__info')
            if page_info:
                info_text = page_info.get_text(strip=True)
                # Extract numbers from text like "1–30 of 583"
                numbers = re.findall(r'\d+', info_text)
                if len(numbers) >= 3:
                    pagination_info["total_speakers"] = int(numbers[2])
            
            # Check for next page link
            next_link = nav_page.select_one('.c-nav-page__item--next a')
            pagination_info["has_next"] = next_link is not None
            
            # Extract page numbers
            page_items = nav_page.select('.c-nav-page__item:not(.c-nav-page__item--next)')
            page_numbers = []
            for item in page_items:
                text = item.get_text(strip=True)
                if text.isdigit():
                    page_numbers.append(int(text))
            
            if page_numbers:
                pagination_info["total_pages"] = max(page_numbers)
                # Find current page (the one without a link)
                current_item = nav_page.select_one('.c-nav-page__item.is-active')
                if current_item and current_item.get_text(strip=True).isdigit():
                    pagination_info["current_page"] = int(current_item.get_text(strip=True))
            
        except Exception as e:
            logging.warning(f"Error extracting pagination info: {e}")
        
        return pagination_info
    
    def save_speakers_to_mongodb(self, speakers: List[Dict]) -> tuple:
        """Save speakers to MongoDB with upsert"""
        saved_count = 0
        updated_count = 0
        
        for speaker in speakers:
            try:
                # Check if speaker already exists
                existing_speaker = self.speakers_collection.find_one({"username": speaker["username"]})
                
                if existing_speaker:
                    # Update existing speaker with new category if not already present
                    existing_categories = existing_speaker.get("categories", [])
                    if speaker["category_slug"] not in existing_categories:
                        # Add new category to existing speaker
                        result = self.speakers_collection.update_one(
                            {"username": speaker["username"]},
                            {
                                "$addToSet": {"categories": speaker["category_slug"]},
                                "$set": {"last_updated": datetime.now()}
                            }
                        )
                        if result.modified_count > 0:
                            updated_count += 1
                            logging.info(f"Added category {speaker['category_slug']} to existing speaker: {speaker['name']} ({speaker['username']})")
                else:
                    # Create new speaker record
                    speaker["categories"] = [speaker["category_slug"]]
                    result = self.speakers_collection.insert_one(speaker)
                    if result.inserted_id:
                        saved_count += 1
                        logging.info(f"Saved new speaker: {speaker['name']} ({speaker['username']})")
                
            except DuplicateKeyError:
                # Handle race condition where speaker was inserted between check and insert
                logging.debug(f"Speaker {speaker['username']} already exists (race condition)")
                continue
            except Exception as e:
                logging.error(f"Error saving speaker {speaker.get('username', 'unknown')}: {e}")
                continue
        
        return saved_count, updated_count
    
    def collect_speakers_from_category(self, category: Dict) -> Dict:
        """Collect all speakers from a single category"""
        category_slug = category["slug"]
        category_url = category["url"]
        category_name = category["name"]
        
        logging.info(f"Processing category: {category_name} ({category_slug})")
        
        all_speakers = []
        page = 1
        max_pages = self.config["pagination"]["max_pages_per_category"]
        
        while page <= max_pages:
            try:
                logging.info(f"Fetching page {page} for category {category_slug}")
                
                # Fetch the page
                soup = self.fetch_category_page(category_url, page)
                
                # Extract speakers from this page
                speakers_on_page = self.extract_speakers_from_page(soup, category_slug)
                
                if not speakers_on_page:
                    logging.info(f"No speakers found on page {page}, ending pagination")
                    break
                
                all_speakers.extend(speakers_on_page)
                logging.info(f"Found {len(speakers_on_page)} speakers on page {page}")
                
                # Get pagination info
                pagination_info = self.get_pagination_info(soup)
                
                # Check if there are more pages
                if not pagination_info["has_next"] or page >= pagination_info.get("total_pages", 1):
                    logging.info(f"Reached last page ({page}) for category {category_slug}")
                    break
                
                page += 1
                
                # Rate limiting
                time.sleep(self.config["rate_limiting"]["delay_between_requests"])
                
            except Exception as e:
                logging.error(f"Error processing page {page} for category {category_slug}: {e}")
                # Continue to next page or break based on error type
                if "404" in str(e) or "not found" in str(e).lower():
                    break
                page += 1
        
        # Save speakers to MongoDB
        saved_count, updated_count = self.save_speakers_to_mongodb(all_speakers)
        
        # Get total speakers for this category from MongoDB
        total_speakers = self.speakers_collection.count_documents({"categories": category_slug})
        
        category_result = {
            "category_name": category_name,
            "category_slug": category_slug,
            "category_url": category_url,
            "category_type": category.get("type", "unknown"),
            "total_speakers": total_speakers,
            "new_speakers_found": saved_count,
            "updated_speakers": updated_count,
            "pages_processed": page - 1,
            "processed_at": datetime.now()
        }
        
        logging.info(f"Completed category {category_slug}: {total_speakers} total speakers ({saved_count} new, {updated_count} updated)")
        return category_result
    
    def run(self) -> Dict:
        """Main execution method"""
        logging.info("Starting MongoDB Speaker URL Collector")
        
        try:
            # Load categories from MongoDB
            all_categories = self.load_categories_from_mongodb()
            
            if not all_categories:
                raise ValueError("No categories found in MongoDB. Please run Module 1 first.")
            
            logging.info(f"Found {len(all_categories)} categories to process")
            
            # Process each category
            total_speakers_collected = 0
            total_new_speakers = 0
            total_updated_speakers = 0
            category_results = []
            
            for i, category in enumerate(all_categories, 1):
                category_slug = category["slug"]
                
                try:
                    logging.info(f"Processing category {i}/{len(all_categories)}: {category_slug}")
                    
                    # Collect speakers from this category
                    category_result = self.collect_speakers_from_category(category)
                    category_results.append(category_result)
                    
                    # Update counters
                    total_speakers_collected += category_result["total_speakers"]
                    total_new_speakers += category_result["new_speakers_found"]
                    total_updated_speakers += category_result["updated_speakers"]
                    
                    logging.info(f"Completed category {category_slug}: {category_result['total_speakers']} total speakers ({category_result['new_speakers_found']} new, {category_result['updated_speakers']} updated)")
                    
                except Exception as e:
                    logging.error(f"Failed to process category {category_slug}: {e}")
                    continue
            
            # Get final statistics from MongoDB
            total_speakers_in_db = self.speakers_collection.count_documents({})
            total_categories_processed = len([r for r in category_results if r["total_speakers"] > 0])
            
            # Print summary
            print(f"\n=== MongoDB Speaker Collection Completed ===")
            print(f"Categories processed: {total_categories_processed}/{len(all_categories)}")
            print(f"Total speakers in database: {total_speakers_in_db}")
            print(f"New speakers found this run: {total_new_speakers}")
            print(f"Updated speakers this run: {total_updated_speakers}")
            
            # Category breakdown
            print(f"\nCategory breakdown:")
            for result in category_results:
                if result["total_speakers"] > 0:
                    print(f"  - {result['category_name']}: {result['total_speakers']} speakers ({result['new_speakers_found']} new, {result['updated_speakers']} updated)")
            
            return {
                "total_categories_processed": total_categories_processed,
                "total_speakers_in_database": total_speakers_in_db,
                "total_new_speakers_found": total_new_speakers,
                "total_updated_speakers": total_updated_speakers,
                "category_results": category_results,
                "completed_at": datetime.now().isoformat()
            }
            
        except Exception as e:
            logging.error(f"MongoDB Speaker URL Collector failed: {e}")
            raise
        
        finally:
            time.sleep(self.config["rate_limiting"]["delay_between_requests"])
            # Close MongoDB connection
            if hasattr(self, 'client'):
                self.client.close()

def main():
    """Main function to run the MongoDB speaker URL collector"""
    try:
        collector = MongoDBSpeakerURLCollector()
        result = collector.run()
        return result
    except Exception as e:
        print(f"Error running MongoDB Speaker URL Collector: {e}")
        return None

if __name__ == "__main__":
    main()
