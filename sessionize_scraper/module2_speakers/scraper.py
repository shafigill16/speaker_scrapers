import requests
from bs4 import BeautifulSoup
import json
import time
import os
import logging
from datetime import datetime
from typing import Dict, List, Optional
import re
from urllib.parse import urljoin, urlparse

class SpeakerURLCollector:
    """
    Module 2: Collects speaker profile URLs from each category page
    Features: Pagination handling, error recovery, incremental speaker appending
    """
    
    def __init__(self, config_file: str = "config.json"):
        self.base_url = "https://sessionize.com"
        self.categories_file = "categories.json"
        self.output_file = "speakers_by_category.json"
        
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
        
        log_file = os.path.join(log_dir, f"module2_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
    
    def load_categories(self) -> Dict:
        """Load categories from Module 1 output"""
        data_dir = self.config["output"]["data_directory"]
        categories_path = os.path.join(data_dir, self.categories_file)
        
        if not os.path.exists(categories_path):
            raise FileNotFoundError(f"Categories file not found: {categories_path}. Please run Module 1 first.")
        
        try:
            with open(categories_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            raise Exception(f"Could not load categories file: {e}")
    
    def load_existing_speakers(self) -> Dict:
        """Load existing speakers data if available"""
        data_dir = self.config["output"]["data_directory"]
        full_path = os.path.join(data_dir, self.output_file)
        
        if os.path.exists(full_path):
            try:
                with open(full_path, 'r', encoding='utf-8') as f:
                    existing_data = json.load(f)
                    logging.info(f"Loaded existing speakers data with {len(existing_data.get('categories', {}))} categories")
                    return existing_data
            except Exception as e:
                logging.warning(f"Could not load existing speakers data: {e}")
        
        return {
            "categories": {},
            "metadata": {}
        }
    
    def save_data(self, data: Dict, filename: str):
        """Save data to JSON file with backup"""
        # Ensure the directory exists
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        
        backup_dir = self.config["output"]["backup_directory"]
        os.makedirs(backup_dir, exist_ok=True)
        
        # Create backup if file exists
        if os.path.exists(filename):
            backup_filename = f"{os.path.splitext(os.path.basename(filename))[0]}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.backup"
            backup_file = os.path.join(backup_dir, backup_filename)
            try:
                import shutil
                shutil.copy2(filename, backup_file)
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
                    "discovered_at": datetime.now().isoformat()
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
    
    def merge_speakers(self, existing_speakers: List[Dict], new_speakers: List[Dict]) -> List[Dict]:
        """Merge new speakers with existing ones, avoiding duplicates"""
        existing_usernames = {speaker['username'] for speaker in existing_speakers}
        merged_speakers = existing_speakers.copy()
        newly_added = []
        
        for new_speaker in new_speakers:
            if new_speaker['username'] not in existing_usernames:
                merged_speakers.append(new_speaker)
                newly_added.append(new_speaker)
                logging.info(f"Added new speaker: {new_speaker['name']} ({new_speaker['username']})")
        
        if newly_added:
            logging.info(f"Found {len(newly_added)} new speakers in this category")
        else:
            logging.info("No new speakers found in this category")
        
        return merged_speakers
    
    def collect_speakers_from_category(self, category: Dict, existing_speakers: List[Dict] = None) -> Dict:
        """Collect all speakers from a single category"""
        category_slug = category["slug"]
        category_url = category["url"]
        category_name = category["name"]
        
        if existing_speakers is None:
            existing_speakers = []
        
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
        
        # Merge with existing speakers
        merged_speakers = self.merge_speakers(existing_speakers, all_speakers)
        
        category_result = {
            "category_name": category_name,
            "category_slug": category_slug,
            "category_url": category_url,
            "category_type": category.get("type", "unknown"),
            "speakers": merged_speakers,
            "pagination": {
                "total_speakers": len(merged_speakers),
                "new_speakers_found": len(merged_speakers) - len(existing_speakers),
                "pages_processed": page - 1,
                "last_page": page - 1
            },
            "processed_at": datetime.now().isoformat()
        }
        
        logging.info(f"Completed category {category_slug}: {len(merged_speakers)} total speakers ({len(merged_speakers) - len(existing_speakers)} new)")
        return category_result
    
    def run(self) -> Dict:
        """Main execution method"""
        logging.info("Starting Module 2: Speaker URL Collector")
        
        try:
            # Create output directory first
            data_dir = self.config["output"]["data_directory"]
            os.makedirs(data_dir, exist_ok=True)
            
            # Construct full output file path
            full_output_path = os.path.join(data_dir, self.output_file)
            
            # Load categories from Module 1
            categories_data = self.load_categories()
            all_categories = categories_data.get("all_categories", [])
            
            if not all_categories:
                raise ValueError("No categories found in the categories file")
            
            logging.info(f"Found {len(all_categories)} categories to process")
            
            # Load existing speakers data
            existing_data = self.load_existing_speakers()
            
            # Initialize results with existing data
            results = existing_data.get("categories", {})
            total_speakers_collected = 0
            total_new_speakers = 0
            
            # Process each category
            for i, category in enumerate(all_categories, 1):
                category_slug = category["slug"]
                
                try:
                    logging.info(f"Processing category {i}/{len(all_categories)}: {category_slug}")
                    
                    # Get existing speakers for this category
                    existing_speakers = results.get(category_slug, {}).get("speakers", [])
                    
                    # Collect speakers from this category
                    category_result = self.collect_speakers_from_category(category, existing_speakers)
                    results[category_slug] = category_result
                    
                    # Update counters
                    speakers_count = len(category_result["speakers"])
                    new_speakers_count = category_result["pagination"]["new_speakers_found"]
                    total_speakers_collected += speakers_count
                    total_new_speakers += new_speakers_count
                    
                    logging.info(f"Completed category {category_slug}: {speakers_count} total speakers ({new_speakers_count} new)")
                    
                    # Save intermediate results every 5 categories
                    if i % 5 == 0:
                        intermediate_data = {
                            "categories": results,
                            "metadata": {
                                "total_categories_processed": i,
                                "total_categories": len(all_categories),
                                "total_speakers_collected": total_speakers_collected,
                                "total_new_speakers_found": total_new_speakers,
                                "last_updated": datetime.now().isoformat(),
                                "module": "speaker_url_collector",
                                "version": "2.1",
                                "status": "in_progress"
                            }
                        }
                        self.save_data(intermediate_data, full_output_path)
                        logging.info(f"Saved intermediate results after {i} categories")
                    
                except Exception as e:
                    logging.error(f"Failed to process category {category_slug}: {e}")
                    continue
            
            # Final output data
            final_output_data = {
                "categories": results,
                "metadata": {
                    "total_categories_processed": len(all_categories),
                    "total_speakers_collected": total_speakers_collected,
                    "total_new_speakers_found": total_new_speakers,
                    "completed_at": datetime.now().isoformat(),
                    "module": "speaker_url_collector",
                    "version": "2.1",
                    "status": "completed"
                }
            }
            
            # Save final results
            self.save_data(final_output_data, full_output_path)
            
            # Print summary
            print(f"\n=== Module 2 Completed Successfully ===")
            print(f"Categories processed: {len(all_categories)}")
            print(f"Total speakers in database: {total_speakers_collected}")
            print(f"New speakers found this run: {total_new_speakers}")
            print(f"Output saved to: {full_output_path}")
            
            # Category breakdown
            print(f"\nCategory breakdown:")
            for category_slug, category_data in results.items():
                speakers_count = len(category_data["speakers"])
                new_count = category_data["pagination"]["new_speakers_found"]
                print(f"  - {category_data['category_name']}: {speakers_count} speakers ({new_count} new)")
            
            return final_output_data
            
        except Exception as e:
            logging.error(f"Module 2 failed: {e}")
            raise
        
        finally:
            time.sleep(self.config["rate_limiting"]["delay_between_requests"])

def main():
    """Main function to run the speaker URL collector"""
    try:
        collector = SpeakerURLCollector()
        result = collector.run()
        return result
    except Exception as e:
        print(f"Error running Module 2: {e}")
        return None

if __name__ == "__main__":
    main()
