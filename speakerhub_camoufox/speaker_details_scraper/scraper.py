#!/usr/bin/env python3
"""
Main scraper for extracting detailed speaker information with resume capability
"""

import logging
import time
import random
import uuid
from datetime import datetime
from typing import List, Dict, Optional
from camoufox.sync_api import Camoufox
from bs4 import BeautifulSoup

from config import SCRAPER_CONFIG, BROWSER_CONFIG, MONGO_CONFIG
from database import SpeakerDetailsDB
from parser_v5 import SpeakerDetailsParserV5 as SpeakerDetailsParser
from models import DetailedSpeaker

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('speaker_details_scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class SpeakerDetailsScraper:
    """Scraper for detailed speaker information with resume capability"""
    
    def __init__(self, session_id: Optional[str] = None):
        self.session_id = session_id or str(uuid.uuid4())
        self.db = SpeakerDetailsDB(
            MONGO_CONFIG['connection_string'],
            MONGO_CONFIG['database_name'],
            MONGO_CONFIG['source_collection'],
            MONGO_CONFIG['details_collection'],
            MONGO_CONFIG['resume_collection']
        )
        self.parser = SpeakerDetailsParser()
        self.browser = None
        self.page = None
        
        # Tracking variables
        self.processed_count = 0
        self.success_count = 0
        self.error_count = 0
        self.consecutive_errors = 0
        self.start_time = datetime.now()
        
    def run(self, resume: bool = True, limit: Optional[int] = None):
        """Main scraping method with resume capability"""
        logger.info(f"Starting speaker details scraper (Session: {self.session_id})")
        
        if not self.db.connect():
            logger.error("Failed to connect to MongoDB")
            return
            
        try:
            # Check for resume state
            if resume:
                resume_state = self.db.get_resume_state(self.session_id)
                if resume_state:
                    self._restore_from_state(resume_state)
                    logger.info(f"Resuming from speaker index {self.processed_count}")
                else:
                    logger.info("No resume state found, starting from beginning")
            else:
                # Clear any existing resume state
                self.db.clear_resume_state(self.session_id)
                logger.info("Starting fresh scraping session")
                
            # Get speakers to scrape
            speakers = self.db.get_speakers_to_scrape(limit=limit)
            
            if not speakers:
                logger.info("No speakers to scrape")
                return
                
            logger.info(f"Found {len(speakers)} speakers to scrape")
            
            # Process speakers with browser context
            with Camoufox(headless=BROWSER_CONFIG['headless']) as browser:
                self.browser = browser
                self.page = browser.new_page()
                
                # Set headers
                self.page.set_extra_http_headers(BROWSER_CONFIG['extra_headers'])
                
                logger.info("Browser initialized successfully")
                
                # Process speakers
                self._process_speakers(speakers)
            
            # Complete successfully - clear resume state
            if self.success_count > 0:
                self.db.clear_resume_state(self.session_id)
                logger.info("Scraping completed successfully")
                
        except KeyboardInterrupt:
            logger.info("Scraping interrupted by user")
            self._save_resume_state()
            
        except Exception as e:
            logger.error(f"Fatal error in scraper: {e}")
            self._save_resume_state()
            
        finally:
            self._print_summary()
            self._cleanup()
            
            
    def _process_speakers(self, speakers: List[Dict]):
        """Process list of speakers"""
        batch = []
        
        # Skip already processed speakers if resuming
        start_index = self.processed_count
        speakers_to_process = speakers[start_index:]
        
        for i, speaker_basic in enumerate(speakers_to_process, start=start_index):
            try:
                # Check if we should stop due to errors
                if self.consecutive_errors >= SCRAPER_CONFIG['max_errors_before_stop']:
                    logger.error(f"Too many consecutive errors ({self.consecutive_errors}), stopping")
                    break
                    
                logger.info(f"\n{'='*50}")
                logger.info(f"Processing speaker {i+1}/{len(speakers)}: {speaker_basic.get('name')}")
                
                # Mark as processing
                self.db.mark_speaker_as_processing(speaker_basic['uid'])
                
                # Scrape detailed info
                detailed_speaker = self._scrape_speaker_details(speaker_basic)
                
                if detailed_speaker:
                    batch.append(detailed_speaker)
                    self.success_count += 1
                    self.consecutive_errors = 0
                    
                    # Save batch if needed
                    if len(batch) >= SCRAPER_CONFIG['batch_size']:
                        self._save_batch(batch)
                        batch = []
                        
                else:
                    self.error_count += 1
                    self.consecutive_errors += 1
                    
                self.processed_count += 1
                
                # Save resume state periodically
                if self.processed_count % SCRAPER_CONFIG['save_state_every'] == 0:
                    self._save_resume_state()
                    
                # Take breaks
                if self.processed_count % SCRAPER_CONFIG['long_break_after'] == 0:
                    logger.info(f"Taking a long break for {SCRAPER_CONFIG['long_break_duration']} seconds...")
                    time.sleep(SCRAPER_CONFIG['long_break_duration'])
                    
            except Exception as e:
                logger.error(f"Error processing speaker {speaker_basic.get('name')}: {e}")
                self.error_count += 1
                self.consecutive_errors += 1
                
        # Save remaining batch
        if batch:
            self._save_batch(batch)
            
    def _scrape_speaker_details(self, speaker_basic: Dict) -> Optional[DetailedSpeaker]:
        """Scrape detailed information for a single speaker"""
        profile_url = speaker_basic.get('profile_url')
        if not profile_url:
            logger.error(f"No profile URL for speaker {speaker_basic.get('name')}")
            return None
            
        retries = 0
        while retries < SCRAPER_CONFIG['max_retries']:
            try:
                # Navigate to speaker page
                logger.info(f"Loading page: {profile_url}")
                self.page.goto(
                    profile_url, 
                    wait_until="domcontentloaded",
                    timeout=SCRAPER_CONFIG['page_timeout']
                )
                
                # Wait for content to load
                self.page.wait_for_timeout(SCRAPER_CONFIG['wait_after_load'])
                
                # Check for bot detection
                if self._is_bot_detected():
                    logger.warning("Bot detection detected, waiting longer...")
                    time.sleep(SCRAPER_CONFIG['error_delay'])
                    retries += 1
                    continue
                    
                # Get page content
                content = self.page.content()
                
                # Parse content
                detailed_speaker = self.parser.parse(content, speaker_basic)
                
                # Add delay between requests
                delay = random.uniform(SCRAPER_CONFIG['min_delay'], SCRAPER_CONFIG['max_delay'])
                time.sleep(delay)
                
                return detailed_speaker
                
            except Exception as e:
                logger.error(f"Error scraping {profile_url} (attempt {retries+1}): {e}")
                retries += 1
                
                if retries < SCRAPER_CONFIG['max_retries']:
                    time.sleep(SCRAPER_CONFIG['retry_delay'])
                else:
                    # Mark as failed
                    self.db.mark_speaker_as_failed(
                        speaker_basic['uid'],
                        f"Failed after {SCRAPER_CONFIG['max_retries']} attempts: {str(e)}"
                    )
                    
        return None
        
    def _is_bot_detected(self) -> bool:
        """Check if bot detection is triggered"""
        try:
            # Check for common bot detection patterns
            if "www.w3.org/1999/xhtml" in self.page.url:
                return True
                
            # Check page title
            title = self.page.title()
            if any(word in title.lower() for word in ['blocked', 'denied', 'captcha', 'verify']):
                return True
                
            # Check for specific elements
            soup = BeautifulSoup(self.page.content(), 'html.parser')
            if soup.find('div', {'id': 'challenge-form'}):
                return True
                
            return False
            
        except:
            return False
            
    def _save_batch(self, batch: List[DetailedSpeaker]):
        """Save a batch of speakers to database"""
        try:
            saved = self.db.bulk_save_speakers(batch)
            logger.info(f"Saved batch of {saved} speakers")
            
        except Exception as e:
            logger.error(f"Error saving batch: {e}")
            # Try to save individually
            for speaker in batch:
                try:
                    self.db.save_speaker_details(speaker)
                except Exception as e2:
                    logger.error(f"Failed to save speaker {speaker.name}: {e2}")
                    
    def _save_resume_state(self):
        """Save current state for resume capability"""
        state = {
            'session_id': self.session_id,
            'processed_count': self.processed_count,
            'success_count': self.success_count,
            'error_count': self.error_count,
            'start_time': self.start_time,
            'last_saved': datetime.now()
        }
        
        if self.db.save_resume_state(self.session_id, state):
            logger.info(f"Saved resume state at speaker {self.processed_count}")
            
    def _restore_from_state(self, state: Dict):
        """Restore scraper state from saved state"""
        self.processed_count = state.get('processed_count', 0)
        self.success_count = state.get('success_count', 0)
        self.error_count = state.get('error_count', 0)
        self.start_time = state.get('start_time', datetime.now())
        
    def _cleanup(self):
        """Clean up resources"""
        # Browser cleanup is handled by context manager
        self.db.close()
        
    def _print_summary(self):
        """Print scraping summary"""
        duration = datetime.now() - self.start_time
        
        logger.info(f"\n{'='*50}")
        logger.info("SCRAPING SUMMARY")
        logger.info(f"{'='*50}")
        logger.info(f"Session ID: {self.session_id}")
        logger.info(f"Duration: {duration}")
        logger.info(f"Total processed: {self.processed_count}")
        logger.info(f"Successful: {self.success_count}")
        logger.info(f"Failed: {self.error_count}")
        
        if self.processed_count > 0:
            success_rate = (self.success_count / self.processed_count) * 100
            logger.info(f"Success rate: {success_rate:.2f}%")
            
        # Get overall stats before closing DB
        try:
            stats = self.db.get_scraping_stats()
            if stats:
                logger.info(f"\nOVERALL DATABASE STATS:")
                logger.info(f"Total speakers: {stats.get('total_speakers', 0)}")
                logger.info(f"Completed: {stats.get('completed', 0)}")
                logger.info(f"Failed: {stats.get('failed', 0)}")
                logger.info(f"Pending: {stats.get('pending', 0)}")
                logger.info(f"Completion: {stats.get('completion_percentage', 0)}%")
        except:
            pass


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Scrape detailed speaker information')
    parser.add_argument('--no-resume', action='store_true', help='Start fresh without resuming')
    parser.add_argument('--limit', type=int, help='Limit number of speakers to scrape')
    parser.add_argument('--session-id', help='Custom session ID for tracking')
    parser.add_argument('--retry-failed', action='store_true', help='Retry failed speakers')
    
    args = parser.parse_args()
    
    if args.retry_failed:
        # Special mode to retry failed speakers
        scraper = SpeakerDetailsScraper(session_id=args.session_id)
        scraper.retry_failed_speakers(limit=args.limit)
    else:
        # Normal scraping
        scraper = SpeakerDetailsScraper(session_id=args.session_id)
        scraper.run(resume=not args.no_resume, limit=args.limit)


if __name__ == "__main__":
    main()