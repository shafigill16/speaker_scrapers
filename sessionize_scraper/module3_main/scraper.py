import requests
from bs4 import BeautifulSoup
import json
import time
import os
import logging
from datetime import datetime
from typing import Dict, List, Optional
import re
import hashlib

class SpeakerProfileScraper:
    """
    Module 3: Extracts detailed speaker information from individual speaker profiles
    Features: High resumability, incremental updates, error recovery, robust data extraction
    """
    
    def __init__(self, config_file: str = "config.json"):
        self.base_url = "https://sessionize.com"
        self.speakers_file = "speakers_by_category.json"
        self.output_file = "speaker_profiles.json"
        self.state_file = "module3_state.json"
        self.checkpoint_file = "module3_checkpoint.json"
        
        # Load configuration
        self.config = self.load_config(config_file)
        
        # Setup session with proper headers
        self.session = self.setup_session()
        
        # Setup logging
        self.setup_logging()
        
        # State management
        self.state = self.load_state()
    
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
            "resumability": {
                "checkpoint_frequency": 10,  # Save progress every N speakers
                "max_failed_attempts": 5     # Skip speaker after N failed attempts
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
        
        log_file = os.path.join(log_dir, f"module3_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
    
    def load_state(self) -> Dict:
        """Load previous state for resumability"""
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logging.warning(f"Could not load state file: {e}")
        
        return {
            "processed_speakers": [],
            "failed_speakers": {},
            "current_speaker": None,
            "total_speakers_processed": 0,
            "total_speakers_found": 0,
            "last_run": None,
            "run_id": self.generate_run_id()
        }
    
    def generate_run_id(self) -> str:
        """Generate unique run ID"""
        return datetime.now().strftime('%Y%m%d_%H%M%S')
    
    def save_state(self, processed_speakers: List[str], current_speaker: str = None, 
                   total_processed: int = 0, total_found: int = 0):
        """Save current state for resumability"""
        self.state.update({
            "processed_speakers": processed_speakers,
            "current_speaker": current_speaker,
            "total_speakers_processed": total_processed,
            "total_speakers_found": total_found,
            "last_run": datetime.now().isoformat()
        })
        
        try:
            with open(self.state_file, 'w') as f:
                json.dump(self.state, f, indent=2)
        except Exception as e:
            logging.error(f"Could not save state: {e}")
    
    def load_speakers_data(self) -> Dict:
        """Load speakers data from Module 2 output"""
        data_dir = self.config["output"]["data_directory"]
        speakers_path = os.path.join(data_dir, self.speakers_file)
        
        if not os.path.exists(speakers_path):
            raise FileNotFoundError(f"Speakers file not found: {speakers_path}. Please run Module 2 first.")
        
        try:
            with open(speakers_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            raise Exception(f"Could not load speakers file: {e}")
    
    def load_existing_profiles(self) -> Dict:
        """Load existing speaker profiles if available"""
        data_dir = self.config["output"]["data_directory"]
        os.makedirs(data_dir, exist_ok=True)
        
        full_path = os.path.join(data_dir, self.output_file)
        
        if os.path.exists(full_path):
            try:
                with open(full_path, 'r', encoding='utf-8') as f:
                    existing_data = json.load(f)
                    logging.info(f"Loaded existing profiles data with {len(existing_data.get('speakers', {}))} speakers")
                    return existing_data
            except Exception as e:
                logging.warning(f"Could not load existing profiles data: {e}")
        
        # Create new file structure if it doesn't exist
        initial_data = {
            "speakers": {},
            "metadata": {
                "total_speakers": 0,
                "last_updated": datetime.now().isoformat(),
                "module": "speaker_profile_scraper",
                "version": "3.0"
            }
        }
        
        return initial_data
    
    def save_data(self, data: Dict, filename: str):
        """Save data to JSON file with backup"""
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
            # Remove the SVG icon and get just the text
            location_text = location_element.get_text(strip=True)
            return location_text
        return None
    
    def extract_profile_picture(self, soup: BeautifulSoup) -> str:
        """Extract profile picture URL"""
        img_element = soup.select_one('.c-s-speaker-info__avatar img')
        return img_element.get('src') if img_element else None
    
    def extract_social_links(self, soup: BeautifulSoup) -> Dict:
        """Extract social media and other links"""
        links = {}
        
        # Find all links in the links section
        link_elements = soup.select('.c-s-links__link')
        
        for link in link_elements:
            href = link.get('href', '')
            label_element = link.select_one('.o-label')
            
            if not label_element:
                continue
                
            label = label_element.get_text(strip=True)
            
            # Determine link type based on URL pattern and icon
            if 'twitter.com' in href or 'x.com' in href:
                links['twitter'] = {'url': href, 'handle': label}
            elif 'linkedin.com' in href:
                links['linkedin'] = {'url': href, 'label': label}
            elif 'github.com' in href:
                links['github'] = {'url': href, 'handle': label}
            elif link.select_one('.o-icon-pen'):  # Blog icon
                links['blog'] = {'url': href, 'label': label}
            elif link.select_one('.o-icon-building'):  # Company icon
                links['company'] = {'url': href, 'label': label}
            else:
                links['other'] = links.get('other', [])
                links['other'].append({'url': href, 'label': label})
        
        return links
    
    def extract_expertise_areas(self, soup: BeautifulSoup) -> List[str]:
        """Extract areas of expertise"""
        expertise_areas = []
        
        # Find the expertise section
        expertise_section = soup.select('.c-s-speaker-info__group--industry .c-s-tags__item')
        
        for item in expertise_section:
            # Remove the SVG icon and get just the text
            text = item.get_text(strip=True)
            if text:
                expertise_areas.append(text)
        
        return expertise_areas
    
    def extract_topics(self, soup: BeautifulSoup) -> List[str]:
        """Extract speaking topics"""
        topics = []
        
        # Find the topics section - it's the section after expertise
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
            # Fetch the speaker profile page
            soup = self.fetch_speaker_profile(speaker_url)
            
            # Extract all speaker information
            profile_data = {
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
                    'source_category': speaker_data.get('category_slug', 'unknown'),
                    'run_id': self.state.get('run_id', 'unknown')
                }
            }
            
            return profile_data
            
        except Exception as e:
            logging.error(f"Error scraping speaker profile {speaker_url}: {e}")
            return None
    
    def should_update_speaker(self, existing_profile: Dict, speaker_data: Dict) -> bool:
        """Determine if a speaker profile should be updated"""
        # Always update if profile is older than 7 days
        if existing_profile and 'metadata' in existing_profile:
            scraped_at = existing_profile['metadata'].get('scraped_at')
            if scraped_at:
                try:
                    scraped_date = datetime.fromisoformat(scraped_at.replace('Z', '+00:00'))
                    days_old = (datetime.now() - scraped_date.replace(tzinfo=None)).days
                    if days_old > 7:
                        return True
                except:
                    return True
        
        # Update if no existing profile
        return existing_profile is None
    
    def get_all_speakers_to_process(self, speakers_data: Dict, existing_profiles: Dict) -> List[Dict]:
        """Get list of all speakers that need to be processed"""
        all_speakers = []
        
        for category_slug, category_data in speakers_data.get("categories", {}).items():
            for speaker in category_data.get("speakers", []):
                speaker['category_slug'] = category_slug
                
                # Check if speaker needs processing
                existing_profile = existing_profiles.get("speakers", {}).get(speaker['username'])
                
                if self.should_update_speaker(existing_profile, speaker):
                    all_speakers.append(speaker)
        
        return all_speakers
    
    def run(self) -> Dict:
        """Main execution method with high resumability"""
        logging.info("Starting Module 3: Speaker Profile Scraper with High Resumability")
        
        try:
            # Create output directory
            data_dir = self.config["output"]["data_directory"]
            os.makedirs(data_dir, exist_ok=True)
            
            # Update output file path
            full_output_path = os.path.join(data_dir, self.output_file)
            
            # Load speakers data from Module 2
            speakers_data = self.load_speakers_data()
            
            # Load existing profiles data
            existing_profiles = self.load_existing_profiles()
            
            # Get all speakers that need processing
            speakers_to_process = self.get_all_speakers_to_process(speakers_data, existing_profiles)
            
            if not speakers_to_process:
                logging.info("No speakers need processing. All profiles are up to date.")
                return existing_profiles
            
            logging.info(f"Found {len(speakers_to_process)} speakers to process")
            
            # Get processed speakers from state
            processed_speakers = set(self.state.get("processed_speakers", []))
            failed_speakers = self.state.get("failed_speakers", {})
            
            # Filter out already processed speakers for this run
            speakers_to_process = [
                s for s in speakers_to_process 
                if s['username'] not in processed_speakers or 
                failed_speakers.get(s['username'], 0) < self.config["resumability"]["max_failed_attempts"]
            ]
            
            logging.info(f"After filtering processed speakers: {len(speakers_to_process)} speakers remaining")
            
            # Initialize counters
            total_speakers = len(speakers_to_process)
            scraped_count = 0
            updated_count = 0
            failed_count = 0
            
            # Process speakers
            for i, speaker in enumerate(speakers_to_process, 1):
                speaker_username = speaker['username']
                
                try:
                    logging.info(f"Processing speaker {i}/{total_speakers}: {speaker['name']} ({speaker_username})")
                    
                    # Update state with current speaker
                    self.save_state(
                        processed_speakers=list(processed_speakers),
                        current_speaker=speaker_username,
                        total_processed=scraped_count,
                        total_found=total_speakers
                    )
                    
                    # Scrape speaker profile
                    profile_data = self.scrape_speaker_profile(speaker)
                    
                    if profile_data:
                        # Add/update speaker in existing profiles
                        if "speakers" not in existing_profiles:
                            existing_profiles["speakers"] = {}
                        
                        # Check if this is a new speaker or update
                        is_new_speaker = speaker_username not in existing_profiles["speakers"]
                        
                        existing_profiles["speakers"][speaker_username] = profile_data
                        
                        if is_new_speaker:
                            scraped_count += 1
                            logging.info(f"Successfully scraped new speaker: {speaker['name']}")
                        else:
                            updated_count += 1
                            logging.info(f"Successfully updated speaker: {speaker['name']}")
                        
                        # Mark as processed
                        processed_speakers.add(speaker_username)
                        
                        # Remove from failed speakers if it was there
                        if speaker_username in failed_speakers:
                            del failed_speakers[speaker_username]
                        
                    else:
                        # Track failed attempt
                        failed_speakers[speaker_username] = failed_speakers.get(speaker_username, 0) + 1
                        failed_count += 1
                        logging.warning(f"Failed to scrape speaker: {speaker['name']} (attempt {failed_speakers[speaker_username]})")
                    
                    # Save checkpoint periodically
                    if i % self.config["resumability"]["checkpoint_frequency"] == 0:
                        # Update metadata
                        existing_profiles["metadata"] = {
                            "total_speakers": len(existing_profiles.get("speakers", {})),
                            "new_speakers_scraped": scraped_count,
                            "speakers_updated": updated_count,
                            "failed_speakers": failed_count,
                            "last_updated": datetime.now().isoformat(),
                            "module": "speaker_profile_scraper",
                            "version": "3.0",
                            "run_id": self.state.get('run_id', 'unknown')
                        }
                        
                        # Save intermediate results
                        self.save_data(existing_profiles, full_output_path)
                        
                        # Update state
                        self.state["failed_speakers"] = failed_speakers
                        self.save_state(
                            processed_speakers=list(processed_speakers),
                            current_speaker=None,
                            total_processed=scraped_count + updated_count,
                            total_found=total_speakers
                        )
                        
                        logging.info(f"Checkpoint saved after processing {i} speakers")
                    
                    # Rate limiting
                    time.sleep(self.config["rate_limiting"]["delay_between_requests"])
                    
                except Exception as e:
                    logging.error(f"Error processing speaker {speaker_username}: {e}")
                    failed_speakers[speaker_username] = failed_speakers.get(speaker_username, 0) + 1
                    failed_count += 1
                    continue
            
            # Final data preparation
            final_output_data = {
                "speakers": existing_profiles.get("speakers", {}),
                "metadata": {
                    "total_speakers": len(existing_profiles.get("speakers", {})),
                    "new_speakers_scraped": scraped_count,
                    "speakers_updated": updated_count,
                    "failed_speakers": failed_count,
                    "completed_at": datetime.now().isoformat(),
                    "module": "speaker_profile_scraper",
                    "version": "3.0",
                    "run_id": self.state.get('run_id', 'unknown')
                }
            }
            
            # Save final results
            self.save_data(final_output_data, full_output_path)
            
            # Update final state
            self.state["failed_speakers"] = failed_speakers
            self.save_state(
                processed_speakers=list(processed_speakers),
                current_speaker=None,
                total_processed=scraped_count + updated_count,
                total_found=total_speakers
            )
            
            # Print summary
            print(f"\n=== Module 3 Completed Successfully ===")
            print(f"Total speakers in database: {len(existing_profiles.get('speakers', {}))}")
            print(f"New speakers scraped this run: {scraped_count}")
            print(f"Existing speakers updated: {updated_count}")
            print(f"Failed speakers: {failed_count}")
            print(f"Output saved to: {full_output_path}")
            
            if failed_count > 0:
                print(f"\nFailed speakers (will retry in next run):")
                for username, attempts in failed_speakers.items():
                    print(f"  - {username}: {attempts} failed attempts")
            
            return final_output_data
            
        except Exception as e:
            logging.error(f"Module 3 failed: {e}")
            raise
        
        finally:
            time.sleep(self.config["rate_limiting"]["delay_between_requests"])

def main():
    """Main function to run the speaker profile scraper"""
    try:
        scraper = SpeakerProfileScraper()
        result = scraper.run()
        return result
    except Exception as e:
        print(f"Error running Module 3: {e}")
        return None

if __name__ == "__main__":
    main()
