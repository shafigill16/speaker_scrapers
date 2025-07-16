import requests
from bs4 import BeautifulSoup
import json
import time
import os
import logging
from datetime import datetime
from typing import Dict, List, Optional

class IncrementalCategoryCollector:
    """
    Enhanced Module 1: Handles dynamic categories with incremental updates
    Features: Appends new categories, preserves existing data, robust error handling
    """
    
    def __init__(self, config_file: str = "config.json"):
        self.base_url = "https://sessionize.com"
        self.speakers_directory = "https://sessionize.com/speakers-directory"
        self.output_file = "categories.json"
        
        # Load configuration
        self.config = self.load_config(config_file)
        
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
                "data_directory": "./data",
                "backup_directory": "./backups",
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
    
    def load_existing_data(self) -> Dict:
        """Load existing categories data if available"""
        data_dir = self.config["output"]["data_directory"]
        full_path = os.path.join(data_dir, self.output_file)
        
        if os.path.exists(full_path):
            try:
                with open(full_path, 'r', encoding='utf-8') as f:
                    existing_data = json.load(f)
                    logging.info(f"Loaded existing data with {len(existing_data.get('regular_categories', []))} regular categories and {len(existing_data.get('custom_lists', []))} custom lists")
                    return existing_data
            except Exception as e:
                logging.warning(f"Could not load existing data: {e}")
        
        return {
            "regular_categories": [],
            "custom_lists": [],
            "all_categories": [],
            "metadata": {}
        }
    
    def save_data(self, data: Dict, filename: str):
        """Save data to JSON file with backup"""
        backup_dir = self.config["output"]["backup_directory"]
        os.makedirs(backup_dir, exist_ok=True)
        
        # Create backup if file exists
        if os.path.exists(filename):
            backup_file = os.path.join(backup_dir, f"{os.path.basename(filename)}.backup")
            try:
                os.rename(filename, backup_file)
                logging.info(f"Created backup: {backup_file}")
            except Exception as e:
                logging.warning(f"Could not create backup: {e}")
        
        # Save new data
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            logging.info(f"Data saved successfully to {filename}")
        except Exception as e:
            logging.error(f"Could not save data to {filename}: {e}")
            raise
    
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
                    "discovered_at": datetime.now().isoformat()
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
                    "discovered_at": datetime.now().isoformat()
                }
                
                custom_lists.append(custom_list_data)
                logging.info(f"Extracted custom list: {list_name} ({list_slug})")
                
            except Exception as e:
                logging.error(f"Error extracting custom list from item: {e}")
                continue
        
        return custom_lists
    
    def merge_categories(self, existing_items: List[Dict], new_items: List[Dict], item_type: str) -> List[Dict]:
        """Merge new items with existing ones, avoiding duplicates"""
        existing_slugs = {item['slug'] for item in existing_items}
        merged_items = existing_items.copy()
        newly_added = []
        
        for new_item in new_items:
            if new_item['slug'] not in existing_slugs:
                merged_items.append(new_item)
                newly_added.append(new_item)
                logging.info(f"Added new {item_type}: {new_item['name']} ({new_item['slug']})")
        
        if newly_added:
            logging.info(f"Found {len(newly_added)} new {item_type}")
        else:
            logging.info(f"No new {item_type} found")
        
        return merged_items
    
    def validate_categories_and_lists(self, categories: List[Dict], custom_lists: List[Dict]) -> tuple:
        """Validate extracted categories and custom lists"""
        valid_categories = []
        valid_custom_lists = []
        
        # Validate regular categories
        for category in categories:
            if not all(key in category for key in ['name', 'slug', 'url']):
                logging.warning(f"Category missing required fields: {category}")
                continue
            
            if '/speakers-directory/' not in category['url']:
                logging.warning(f"Invalid category URL format: {category['url']}")
                continue
            
            valid_categories.append(category)
        
        # Validate custom lists
        for custom_list in custom_lists:
            if not all(key in custom_list for key in ['name', 'slug', 'url']):
                logging.warning(f"Custom list missing required fields: {custom_list}")
                continue
            
            if not custom_list['url'].startswith('https://sessionize.com/'):
                logging.warning(f"Invalid custom list URL format: {custom_list['url']}")
                continue
            
            valid_custom_lists.append(custom_list)
        
        return valid_categories, valid_custom_lists
    
    def run(self) -> Dict:
        """Main execution method with incremental updates"""
        logging.info("Starting Incremental Category Collector")
        
        try:
            # Create output directory
            data_dir = self.config["output"]["data_directory"]
            os.makedirs(data_dir, exist_ok=True)
            
            # Update output file path
            full_output_path = os.path.join(data_dir, self.output_file)
            
            # Load existing data
            existing_data = self.load_existing_data()
            
            # Fetch the categories page
            logging.info(f"Fetching categories from: {self.speakers_directory}")
            soup = self.fetch_categories_page()
            
            # Extract current categories and custom lists
            logging.info("Extracting regular categories from HTML...")
            current_regular_categories = self.extract_regular_categories(soup)
            
            logging.info("Extracting custom lists from HTML...")
            current_custom_lists = self.extract_custom_lists(soup)
            
            # Validate extracted data
            logging.info("Validating extracted categories and custom lists...")
            valid_categories, valid_custom_lists = self.validate_categories_and_lists(
                current_regular_categories, current_custom_lists
            )
            
            # Merge with existing data
            logging.info("Merging with existing data...")
            existing_regular_categories = existing_data.get("regular_categories", [])
            existing_custom_lists = existing_data.get("custom_lists", [])
            
            merged_regular_categories = self.merge_categories(
                existing_regular_categories, valid_categories, "regular categories"
            )
            
            merged_custom_lists = self.merge_categories(
                existing_custom_lists, valid_custom_lists, "custom lists"
            )
            
            # Prepare output data
            output_data = {
                "regular_categories": merged_regular_categories,
                "custom_lists": merged_custom_lists,
                "all_categories": merged_regular_categories + merged_custom_lists,
                "metadata": {
                    "total_regular_categories": len(merged_regular_categories),
                    "total_custom_lists": len(merged_custom_lists),
                    "total_all_categories": len(merged_regular_categories) + len(merged_custom_lists),
                    "new_regular_categories": len(merged_regular_categories) - len(existing_regular_categories),
                    "new_custom_lists": len(merged_custom_lists) - len(existing_custom_lists),
                    "last_updated": datetime.now().isoformat(),
                    "source_url": self.speakers_directory,
                    "module": "incremental_category_collector",
                    "version": "2.1"
                }
            }
            
            # Save data
            self.save_data(output_data, full_output_path)
            
            # Print summary
            new_regular = len(merged_regular_categories) - len(existing_regular_categories)
            new_custom = len(merged_custom_lists) - len(existing_custom_lists)
            
            print(f"\n=== Incremental Category Collection Completed ===")
            print(f"Regular categories: {len(merged_regular_categories)} (+ {new_regular} new)")
            print(f"Custom lists: {len(merged_custom_lists)} (+ {new_custom} new)")
            print(f"Total categories: {len(merged_regular_categories) + len(merged_custom_lists)}")
            print(f"Output saved to: {full_output_path}")
            
            if new_regular > 0:
                print(f"\nNew regular categories:")
                new_cats = merged_regular_categories[-new_regular:]
                for cat in new_cats:
                    print(f"  + {cat['name']} ({cat['slug']})")
            
            if new_custom > 0:
                print(f"\nNew custom lists:")
                new_lists = merged_custom_lists[-new_custom:]
                for cl in new_lists:
                    print(f"  + {cl['name']} ({cl['slug']}) - {cl.get('speaker_count', 'N/A')}")
            
            return output_data
            
        except Exception as e:
            logging.error(f"Incremental Category Collector failed: {e}")
            raise
        
        finally:
            time.sleep(self.config["rate_limiting"]["delay_between_requests"])

def main():
    """Main function to run the incremental category collector"""
    try:
        collector = IncrementalCategoryCollector()
        result = collector.run()
        return result
    except Exception as e:
        print(f"Error running Incremental Category Collector: {e}")
        return None

if __name__ == "__main__":
    main()
