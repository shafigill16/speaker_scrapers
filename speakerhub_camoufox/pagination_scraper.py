#!/usr/bin/env python3
"""
SpeakerHub Scraper with Pagination Support
Properly handles the pagination system instead of infinite scroll
"""

import logging
import time
import random
from datetime import datetime
from typing import List, Optional
from urllib.parse import urljoin

from camoufox.sync_api import Camoufox
from bs4 import BeautifulSoup
from pymongo import MongoClient, UpdateOne
from pymongo.errors import ConnectionFailure, OperationFailure

from config import MONGO_CONFIG, SCRAPER_CONFIG
from speakerhub_scraper import Speaker, MongoDBHandler, SpeakerExtractor


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pagination_scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class PaginationScraper:
    """Scraper that handles pagination instead of infinite scroll"""
    
    def __init__(self, mongo_handler: MongoDBHandler):
        self.mongo_handler = mongo_handler
        self.extractor = SpeakerExtractor()
        self.base_url = "https://speakerhub.com"
        self.scraped_uids = set()
        
    def human_delay(self, min_sec: float = 1, max_sec: float = 3):
        """Random human-like delay"""
        delay = random.uniform(min_sec, max_sec)
        time.sleep(delay)
    
    def extract_speakers_from_page(self, page_content: str) -> List[Speaker]:
        """Extract speakers from page content"""
        soup = BeautifulSoup(page_content, 'html.parser')
        speaker_cards = soup.find_all('div', class_='user-speaker-card')
        
        speakers = []
        for card in speaker_cards:
            uid = card.get('data-uid')
            if uid and uid not in self.scraped_uids:
                speaker = self.extractor.extract_speaker_from_card(card)
                if speaker:
                    speakers.append(speaker)
                    self.scraped_uids.add(uid)
        
        return speakers
    
    def get_next_page_url(self, page_content: str) -> Optional[str]:
        """Extract next page URL from current page"""
        soup = BeautifulSoup(page_content, 'html.parser')
        
        # Look for pagination links
        # First try the "Show More" button
        show_more = soup.find('a', text=lambda t: t and 'Show More' in t)
        if show_more and show_more.get('href'):
            return urljoin(self.base_url, show_more['href'])
        
        # Try standard pagination
        pager = soup.find('ul', class_='pager')
        if pager:
            # Look for next page link
            next_link = pager.find('a', {'href': lambda x: x and 'page=' in x})
            if next_link:
                return urljoin(self.base_url, next_link['href'])
        
        # Try numbered pages
        current_page = 1
        if '?page=' in page_content:
            try:
                current_page = int(page_content.split('?page=')[1].split('"')[0])
            except:
                pass
        
        # Check if there's a link to the next page number
        next_page_link = soup.find('a', {'href': f'?page={current_page + 1}'})
        if next_page_link:
            return urljoin(self.base_url, f"/speakers?page={current_page + 1}")
        
        return None
    
    def scrape_all_pages(self):
        """Scrape all pages using pagination"""
        total_scraped = 0
        page_num = 1
        batch_size = SCRAPER_CONFIG.get('batch_size', 50)
        speakers_batch = []
        
        try:
            with Camoufox(headless=True) as browser:
                page = browser.new_page()
                
                # Set headers
                page.set_extra_http_headers({
                    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "Accept-Language": "en-US,en;q=0.9",
                })
                
                # Start with first page
                current_url = f"{self.base_url}/speakers"
                
                while current_url:
                    logger.info(f"\n{'='*50}")
                    logger.info(f"Scraping page {page_num}: {current_url}")
                    
                    # Navigate to page with increased timeout
                    try:
                        page.goto(current_url, wait_until="networkidle", timeout=60000)  # 60 second timeout
                    except Exception as timeout_error:
                        logger.warning(f"Timeout on {current_url}, retrying with domcontentloaded...")
                        try:
                            page.goto(current_url, wait_until="domcontentloaded", timeout=45000)
                            page.wait_for_timeout(5000)  # Extra wait for content
                        except:
                            logger.error(f"Failed to load {current_url} after retry")
                            raise
                    
                    self.human_delay(2, 4)
                    
                    # Check for bot detection
                    if "wwv" in page.url:
                        logger.error("Bot detection triggered!")
                        break
                    
                    # Get page content
                    content = page.content()
                    
                    # Extract speakers
                    new_speakers = self.extract_speakers_from_page(content)
                    
                    if new_speakers:
                        speakers_batch.extend(new_speakers)
                        logger.info(f"Extracted {len(new_speakers)} speakers from page {page_num}")
                        logger.info(f"Total unique speakers so far: {len(self.scraped_uids)}")
                        
                        # Save batch if needed
                        if len(speakers_batch) >= batch_size:
                            saved = self.mongo_handler.bulk_upsert_speakers(speakers_batch)
                            total_scraped += saved
                            speakers_batch = []
                            logger.info(f"Saved batch to MongoDB. Total saved: {total_scraped}")
                    else:
                        logger.warning(f"No new speakers found on page {page_num}")
                    
                    # Get next page URL
                    next_url = self.get_next_page_url(content)
                    
                    if next_url:
                        logger.info(f"Found next page: {next_url}")
                        current_url = next_url
                        page_num += 1
                        
                        # Take a break between pages
                        if page_num % 10 == 0:
                            logger.info(f"Taking a longer break after {page_num} pages...")
                            self.human_delay(5, 8)
                        else:
                            self.human_delay(2, 4)
                    else:
                        logger.info("No more pages found. Scraping complete!")
                        break
                
                # Save any remaining speakers
                if speakers_batch:
                    saved = self.mongo_handler.bulk_upsert_speakers(speakers_batch)
                    total_scraped += saved
                    logger.info(f"Saved final batch. Total saved: {total_scraped}")
                
        except Exception as e:
            logger.error(f"Scraping failed: {e}")
            import traceback
            traceback.print_exc()
        
        return total_scraped, len(self.scraped_uids)


def main():
    """Main function"""
    print("="*60)
    print("SpeakerHub Pagination Scraper")
    print("="*60)
    
    # Initialize MongoDB
    mongo_handler = MongoDBHandler(
        MONGO_CONFIG['connection_string'],
        MONGO_CONFIG['database_name'],
        MONGO_CONFIG['collection_name']
    )
    
    if not mongo_handler.connect():
        print("Failed to connect to MongoDB")
        return
    
    # Get initial count
    initial_count = mongo_handler.get_speaker_count()
    print(f"Initial speakers in database: {initial_count}")
    
    # Run scraper
    scraper = PaginationScraper(mongo_handler)
    
    print("\nStarting pagination scraper...")
    print("This will go through all pages sequentially.")
    print("Press Ctrl+C to stop at any time.\n")
    
    start_time = datetime.now()
    
    try:
        total_saved, total_found = scraper.scrape_all_pages()
        
        # Get final count
        final_count = mongo_handler.get_speaker_count()
        
        # Calculate duration
        duration = datetime.now() - start_time
        
        print("\n" + "="*60)
        print("Scraping Complete!")
        print("="*60)
        print(f"Duration: {duration}")
        print(f"Total speakers found: {total_found}")
        print(f"Total saved to database: {total_saved}")
        print(f"Initial database count: {initial_count}")
        print(f"Final database count: {final_count}")
        print(f"New speakers added: {final_count - initial_count}")
        
    except KeyboardInterrupt:
        print("\n\nScraping interrupted by user.")
    finally:
        mongo_handler.close()
        print("\nScraper finished.")


if __name__ == "__main__":
    main()