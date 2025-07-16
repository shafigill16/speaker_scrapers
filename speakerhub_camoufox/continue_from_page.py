#!/usr/bin/env python3
"""
Continue scraping from a specific page number
"""

import logging
from datetime import datetime
from pagination_scraper import MongoDBHandler, SpeakerExtractor
from config import MONGO_CONFIG
from camoufox.sync_api import Camoufox
from bs4 import BeautifulSoup
import time
import random

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('continue_scraping.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def continue_from_page(start_page_num):
    """Continue scraping from a specific page number"""
    
    # Initialize MongoDB
    mongo_handler = MongoDBHandler(
        MONGO_CONFIG['connection_string'],
        MONGO_CONFIG['database_name'],
        MONGO_CONFIG['collection_name']
    )
    
    if not mongo_handler.connect():
        logger.error("Failed to connect to MongoDB")
        return
    
    extractor = SpeakerExtractor()
    scraped_uids = set()
    batch_size = 50
    speakers_batch = []
    total_saved = 0
    
    try:
        with Camoufox(headless=True) as browser:
            page = browser.new_page()
            
            # Set headers
            page.set_extra_http_headers({
                "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
            })
            
            # Start from the specified page
            current_url = f"https://speakerhub.com/speakers?page={start_page_num}"
            page_counter = start_page_num // 2  # Adjust for display
            
            logger.info(f"Starting from page {page_counter} (URL: {current_url})")
            
            while current_url:
                logger.info(f"\n{'='*50}")
                logger.info(f"Scraping page {page_counter}: {current_url}")
                
                try:
                    # Navigate with longer timeout
                    page.goto(current_url, wait_until="domcontentloaded", timeout=60000)
                    page.wait_for_timeout(5000)  # Extra wait
                    
                    # Get content
                    content = page.content()
                    soup = BeautifulSoup(content, 'html.parser')
                    
                    # Extract speakers
                    speaker_cards = soup.find_all('div', class_='user-speaker-card')
                    new_speakers = []
                    
                    for card in speaker_cards:
                        uid = card.get('data-uid')
                        if uid and uid not in scraped_uids:
                            speaker = extractor.extract_speaker_from_card(card)
                            if speaker:
                                new_speakers.append(speaker)
                                scraped_uids.add(uid)
                    
                    if new_speakers:
                        speakers_batch.extend(new_speakers)
                        logger.info(f"Extracted {len(new_speakers)} speakers from page {page_counter}")
                        logger.info(f"Total unique speakers so far: {len(scraped_uids)}")
                        
                        # Save batch
                        if len(speakers_batch) >= batch_size:
                            saved = mongo_handler.bulk_upsert_speakers(speakers_batch)
                            total_saved += saved
                            speakers_batch = []
                            logger.info(f"Saved batch to MongoDB. Total saved: {total_saved}")
                    
                    # Find next page
                    next_link = soup.find('a', string=lambda t: t and 'Show More' in t)
                    if not next_link:
                        next_link = soup.find('a', {'href': f'?page={start_page_num + 2}'})
                    
                    if next_link and next_link.get('href'):
                        current_url = f"https://speakerhub.com{next_link['href']}"
                        start_page_num += 2
                        page_counter += 1
                        
                        # Random delay
                        time.sleep(random.uniform(3, 5))
                    else:
                        logger.info("No more pages found")
                        break
                        
                except Exception as e:
                    logger.error(f"Error on page {page_counter}: {e}")
                    # Try to continue with next page
                    start_page_num += 2
                    page_counter += 1
                    current_url = f"https://speakerhub.com/speakers?page={start_page_num}"
                    time.sleep(10)  # Longer wait after error
                    
            # Save remaining speakers
            if speakers_batch:
                saved = mongo_handler.bulk_upsert_speakers(speakers_batch)
                total_saved += saved
                
            logger.info(f"\nScraping complete! Total saved: {total_saved}")
                    
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        mongo_handler.close()

if __name__ == "__main__":
    # Start from page 314
    continue_from_page(1)