import requests
from bs4 import BeautifulSoup
import json
import time
import os
import logging
from datetime import datetime
from typing import Dict, List, Optional
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, DuplicateKeyError
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class MongoDBCategoryCollector:
    """
    Enhanced Module 1: Collects categories and stores them in MongoDB
    Features: MongoDB integration, incremental updates, robust error handling
    """
    
    def __init__(self, config_file: str = "config.json"):
        self.base_url = os.getenv("BASE_URL", "https://sessionize.com")
        self.speakers_directory = os.getenv("SPEAKERS_DIRECTORY", "https://sessionize.com/speakers-directory")
        
        # MongoDB connection
        self.mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
        self.db_name = os.getenv("MONGO_DB_NAME", "sessionize_scraper")
        
        # Load configuration
        self.config = self.load_config(config_file)
        
        # Setup MongoDB connection
        self.setup_mongodb()
        
        # Setup session with proper headers
        self.session = self.setup_session()
        
        # Setup logging
        self.setup_logging()
    
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
            
            # Create indexes for better performance
            self.categories_collection.create_index("slug", unique=True)
            self.categories_collection.create_index("type")
            
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
        """Setup logging configuration"""
        log_dir = self.config["output"]["log_directory"]
        os.makedirs(log_dir, exist_ok=True)
        
        log_file = os.path.join(log_dir, f"module1_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
    
    def load_existing_categories(self) -> Dict:
        """Load existing categories from MongoDB"""
        try:
            regular_categories = list(self.categories_collection.find({"type": "regular_category"}))
            custom_lists = list(self.categories_collection.find({"type": "custom_list"}))
            
            # Remove MongoDB ObjectId for processing
            for category in regular_categories + custom_lists:
                if '_id' in category:
                    del category['_id']
            
            logging.info(f"Loaded {len(regular_categories)} regular categories and {len(custom_lists)} custom lists from MongoDB")
            
            return {
                "regular_categories": regular_categories,
                "custom_lists": custom_lists
            }
        except Exception as e:
            logging.warning(f"Could not load existing categories from MongoDB: {e}")
            return {"regular_categories": [], "custom_lists": []}
    
    def save_categories_to_mongodb(self, categories: List[Dict], category_type: str):
        """Save categories to MongoDB with upsert"""
        saved_count = 0
        updated_count = 0
        
        for category in categories:
            try:
                # Add type and timestamp
                category['type'] = category_type
                category['last_updated'] = datetime.now()
                
                # Upsert based on slug
                result = self.categories_collection.replace_one(
                    {"slug": category["slug"]},
                    category,
                    upsert=True
                )
                
                if result.upserted_id:
                    saved_count += 1
                    logging.info(f"Saved new {category_type}: {category['name']} ({category['slug']})")
                elif result.modified_count > 0:
                    updated_count += 1
                    logging.info(f"Updated {category_type}: {category['name']} ({category['slug']})")
                
            except Exception as e:
                logging.error(f"Error saving category {category.get('slug', 'unknown')}: {e}")
                continue
        
        return saved_count, updated_count
    
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
    
    def fetch_categories_page(self) -> BeautifulSoup:
        """Fetch and parse the main categories page"""
        def _fetch():
            response = self.session.get(
                self.speakers_directory,
                timeout=self.config["error_handling"]["timeout"]
            )
            response.raise_for_status()
            return BeautifulSoup(response.content, 'html.parser')
        
        return self.retry_with_backoff(_fetch)
    
    def extract_regular_categories(self, soup: BeautifulSoup) -> List[Dict]:
        """Extract regular category information from the main directory"""
        categories = []
        
        # Find the main directory section
        main_directory_section = soup.select_one('.c-directory-list.c-directory-list--four')
        
        if not main_directory_section:
            logging.warning("Main directory section not found")
            return categories
        
        category_items = main_directory_section.select('.c-directory-list__item')
        
        for item in category_items:
            try:
                link_element = item.select_one('h3.c-directory-list__title a')
                if not link_element:
                    continue
                
                category_name = link_element.get_text(strip=True)
                category_href = link_element.get('href', '')
                
                if not category_name or not category_href:
                    continue
                
                # Skip if this doesn't look like a regular category
                if not category_href.startswith('/speakers-directory/'):
                    continue
                
                category_url = f"{self.base_url}{category_href}"
                category_slug = category_href.split('/')[-1]
                
                icon_element = item.select_one('.c-directory-list__icon i')
                icon_class = icon_element.get('class', []) if icon_element else []
                
                category_data = {
                    "name": category_name,
                    "slug": category_slug,
                    "url": category_url,
                    "href": category_href,
                    "icon_classes": icon_class,
                    "type": "regular_category",
                    "discovered_at": datetime.now()
                }
                
                categories.append(category_data)
                logging.info(f"Extracted regular category: {category_name} ({category_slug})")
                
            except Exception as e:
                logging.error(f"Error extracting regular category from item: {e}")
                continue
        
        return categories
    
    def extract_custom_lists(self, soup: BeautifulSoup) -> List[Dict]:
        """Extract custom lists from the Custom Lists section"""
        custom_lists = []
        
        # Look for the Custom Lists section
        custom_lists_section = soup.find('h2', string='Custom Lists')
        if not custom_lists_section:
            logging.warning("Custom Lists section not found")
            return custom_lists
        
        # Find the parent container
        section_container = custom_lists_section.find_parent('div', class_='c-block__content')
        if not section_container:
            logging.warning("Custom Lists container not found")
            return custom_lists
        
        # Extract custom list items
        custom_list_items = section_container.select('.c-directory-list__item')
        
        for item in custom_list_items:
            try:
                link_element = item.select_one('h3.c-directory-list__title a')
                if not link_element:
                    continue
                
                list_name = link_element.get_text(strip=True)
                list_href = link_element.get('href', '')
                
                if not list_name or not list_href:
                    continue
                
                list_url = f"{self.base_url}{list_href}"
                list_slug = list_href.split('/')[-1]
                
                summary_element = item.select_one('.c-directory-list__summary')
                summary = summary_element.get_text(strip=True) if summary_element else None
                
                meta_element = item.select_one('.c-directory-list__meta')
                speaker_count = meta_element.get_text(strip=True) if meta_element else None
                
                custom_list_data = {
                    "name": list_name,
                    "slug": list_slug,
                    "url": list_url,
                    "href": list_href,
                    "summary": summary,
                    "speaker_count": speaker_count,
                    "type": "custom_list",
                    "discovered_at": datetime.now()
                }
                
                custom_lists.append(custom_list_data)
                logging.info(f"Extracted custom list: {list_name} ({list_slug})")
                
            except Exception as e:
                logging.error(f"Error extracting custom list from item: {e}")
                continue
        
        return custom_lists
    
    def run(self) -> Dict:
        """Main execution method"""
        logging.info("Starting MongoDB Category Collector")
        
        try:
            # Load existing categories from MongoDB
            existing_data = self.load_existing_categories()
            
            # Fetch the categories page
            logging.info(f"Fetching categories from: {self.speakers_directory}")
            soup = self.fetch_categories_page()
            
            # Extract current categories and custom lists
            logging.info("Extracting regular categories from HTML...")
            current_regular_categories = self.extract_regular_categories(soup)
            
            logging.info("Extracting custom lists from HTML...")
            current_custom_lists = self.extract_custom_lists(soup)
            
            # Save to MongoDB
            logging.info("Saving categories to MongoDB...")
            reg_saved, reg_updated = self.save_categories_to_mongodb(current_regular_categories, "regular_category")
            custom_saved, custom_updated = self.save_categories_to_mongodb(current_custom_lists, "custom_list")
            
            # Get final counts
            total_regular = self.categories_collection.count_documents({"type": "regular_category"})
            total_custom = self.categories_collection.count_documents({"type": "custom_list"})
            
            # Print summary
            print(f"\n=== MongoDB Category Collection Completed ===")
            print(f"Regular categories in database: {total_regular} ({reg_saved} new, {reg_updated} updated)")
            print(f"Custom lists in database: {total_custom} ({custom_saved} new, {custom_updated} updated)")
            print(f"Total categories: {total_regular + total_custom}")
            
            if reg_saved > 0:
                print(f"\nNew regular categories:")
                new_cats = self.categories_collection.find({"type": "regular_category"}).sort("discovered_at", -1).limit(reg_saved)
                for cat in new_cats:
                    print(f"  + {cat['name']} ({cat['slug']})")
            
            if custom_saved > 0:
                print(f"\nNew custom lists:")
                new_lists = self.categories_collection.find({"type": "custom_list"}).sort("discovered_at", -1).limit(custom_saved)
                for cl in new_lists:
                    print(f"  + {cl['name']} ({cl['slug']}) - {cl.get('speaker_count', 'N/A')}")
            
            return {
                "total_regular_categories": total_regular,
                "total_custom_lists": total_custom,
                "new_regular_categories": reg_saved,
                "new_custom_lists": custom_saved,
                "updated_regular_categories": reg_updated,
                "updated_custom_lists": custom_updated
            }
            
        except Exception as e:
            logging.error(f"MongoDB Category Collector failed: {e}")
            raise
        
        finally:
            time.sleep(self.config["rate_limiting"]["delay_between_requests"])
            # Close MongoDB connection
            if hasattr(self, 'client'):
                self.client.close()

def main():
    """Main function to run the MongoDB category collector"""
    try:
        collector = MongoDBCategoryCollector()
        result = collector.run()
        return result
    except Exception as e:
        print(f"Error running MongoDB Category Collector: {e}")
        return None

if __name__ == "__main__":
    main()
