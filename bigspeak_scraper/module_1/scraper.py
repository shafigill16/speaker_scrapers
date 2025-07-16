import requests
from bs4 import BeautifulSoup
import time
import logging
from urllib.parse import urljoin, urlparse, parse_qs
from datetime import datetime
from config import (
    SPEAKERS_URL, BASE_URL, REQUEST_TIMEOUT, 
    RETRY_ATTEMPTS, DELAY_BETWEEN_REQUESTS, get_collection
)

logger = logging.getLogger(__name__)

class BigSpeakMainDirectoryScraper:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        })
        self.collection = get_collection()
        self.scraped_count = 0
        self.error_count = 0
    
    def get_page(self, url):
        """Fetch a page with retry logic"""
        for attempt in range(RETRY_ATTEMPTS):
            try:
                logger.info(f"Fetching: {url} (Attempt {attempt + 1})")
                response = self.session.get(url, timeout=REQUEST_TIMEOUT)
                response.raise_for_status()
                return response
            except requests.RequestException as e:
                logger.warning(f"Attempt {attempt + 1} failed for {url}: {e}")
                if attempt < RETRY_ATTEMPTS - 1:
                    time.sleep(2 ** attempt)
                else:
                    logger.error(f"Failed to fetch {url} after {RETRY_ATTEMPTS} attempts")
                    self.error_count += 1
                    return None
    
    def parse_speaker_card(self, card):
        """Extract speaker information from a card element"""
        try:
            speaker = {
                'scraped_at': datetime.utcnow(),
                'source': 'main_directory'
            }
            
            # Extract name and profile link
            name_link = card.find(['h2', 'h3']).find('a')
            if not name_link:
                logger.warning("No name link found in speaker card")
                return None
            
            speaker['name'] = name_link.text.strip()
            speaker['profile_url'] = urljoin(BASE_URL, name_link.get('href', ''))
            
            # Extract speaker ID from profile URL
            profile_path = urlparse(speaker['profile_url']).path
            speaker['speaker_id'] = profile_path.strip('/').split('/')[-1]
            
            # Extract professional description
            desc_elem = card.find('em') or card.find('p', class_='description')
            if desc_elem:
                speaker['description'] = desc_elem.text.strip()
            
            # Extract speaking topics
            topics = []
            topics_div = card.find('div', class_='topics')
            if topics_div:
                topics_list = topics_div.find('ul')
                if topics_list:
                    topic_links = topics_list.find_all('a')
                    for link in topic_links:
                        topic_text = link.text.strip()
                        topic_url = urljoin(BASE_URL, link.get('href', ''))
                        topics.append({
                            'name': topic_text,
                            'url': topic_url
                        })
            speaker['topics'] = topics
            
            # Extract fee range
            fee_div = card.find('div', class_='fee')
            if fee_div:
                fee_span = fee_div.find('span')
                if fee_span:
                    speaker['fee_range'] = fee_span.text.strip()
                else:
                    speaker['fee_range'] = "Please Inquire"
            else:
                speaker['fee_range'] = "Please Inquire"
            
            # Extract image URL if available (handle lazy loading)
            img_link = card.find('a', class_='image')
            if img_link:
                # Check for lazy-loaded background image
                data_bg = img_link.get('data-bg', '')
                if 'url(' in data_bg:
                    # Extract URL from url(...) format
                    img_url = data_bg.split('url(')[1].split(')')[0].strip()
                    speaker['image_url'] = img_url
            else:
                img_elem = card.find('img')
                if img_elem and img_elem.get('src'):
                    speaker['image_url'] = urljoin(BASE_URL, img_elem['src'])
            
            logger.debug(f"Parsed speaker: {speaker['name']}")
            return speaker
            
        except Exception as e:
            logger.error(f"Error parsing speaker card: {e}")
            self.error_count += 1
            return None
    
    def get_total_pages(self):
        """Get the total number of pages"""
        response = self.get_page(SPEAKERS_URL)
        if not response:
            return 1
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Look for pagination
        pagination = soup.find(['div', 'nav'], class_=['pagination', 'wp-pagination'])
        if pagination:
            # Try to find the last page number
            page_links = pagination.find_all('a')
            page_numbers = []
            
            for link in page_links:
                # Check href for page parameter
                href = link.get('href', '')
                if 'page=' in href:
                    try:
                        page_num = int(parse_qs(urlparse(href).query).get('page', [0])[0])
                        page_numbers.append(page_num)
                    except:
                        pass
                
                # Also check link text
                try:
                    page_num = int(link.text.strip())
                    page_numbers.append(page_num)
                except:
                    pass
            
            if page_numbers:
                total_pages = max(page_numbers)
                logger.info(f"Found {total_pages} total pages")
                return total_pages
        
        # Default based on previous analysis
        logger.info("Could not determine total pages, defaulting to 146")
        return 146
    
    def scrape_page(self, page_num=1):
        """Scrape a single page of speakers"""
        if page_num == 1:
            url = SPEAKERS_URL
        else:
            url = f"{SPEAKERS_URL}page/{page_num}/"
        
        response = self.get_page(url)
        if not response:
            return []
        
        soup = BeautifulSoup(response.text, 'html.parser')
        speakers_on_page = []
        
        # Based on diagnosis, look for the speakers list container
        speakers_list = soup.find('div', class_='speakers-list')
        if not speakers_list:
            logger.warning("Could not find speakers-list container")
            return []
        
        # Find individual speaker divs within the list
        cards = speakers_list.find_all('div', class_='speaker')
        
        if cards:
            logger.info(f"Found {len(cards)} speaker cards on page {page_num}")
        
        if not cards:
            logger.warning(f"No speaker cards found on page {page_num}")
            
        for card in cards:
            speaker = self.parse_speaker_card(card)
            if speaker and speaker.get('name'):
                speakers_on_page.append(speaker)
        
        return speakers_on_page
    
    def save_speakers_to_db(self, speakers):
        """Save speakers to MongoDB with upsert logic"""
        if not speakers:
            return
        
        try:
            for speaker in speakers:
                # Use speaker_id as unique identifier
                filter_query = {'speaker_id': speaker['speaker_id']}
                
                # Update with new data but preserve certain fields
                update_query = {
                    '$set': speaker,
                    '$setOnInsert': {
                        'first_scraped_at': datetime.utcnow()
                    }
                }
                
                self.collection.update_one(filter_query, update_query, upsert=True)
                self.scraped_count += 1
            
            logger.info(f"Saved/updated {len(speakers)} speakers to database")
            
        except Exception as e:
            logger.error(f"Error saving to database: {e}")
            self.error_count += 1
    
    def scrape_all_pages(self, start_page=1, max_pages=None):
        """Scrape all pages from the main directory"""
        total_pages = self.get_total_pages()
        
        if max_pages:
            total_pages = min(total_pages, max_pages)
        
        logger.info(f"Starting scrape from page {start_page} to {total_pages}")
        
        for page_num in range(start_page, total_pages + 1):
            logger.info(f"\n--- Scraping page {page_num}/{total_pages} ---")
            
            speakers = self.scrape_page(page_num)
            
            if speakers:
                logger.info(f"Found {len(speakers)} speakers on page {page_num}")
                self.save_speakers_to_db(speakers)
            else:
                logger.warning(f"No speakers found on page {page_num}")
            
            # Progress update
            if page_num % 10 == 0:
                logger.info(f"\nProgress: {page_num}/{total_pages} pages completed")
                logger.info(f"Total speakers scraped: {self.scraped_count}")
                logger.info(f"Total errors: {self.error_count}")
            
            # Be polite to the server
            time.sleep(DELAY_BETWEEN_REQUESTS)
        
        logger.info(f"\n=== Scraping completed ===")
        logger.info(f"Total speakers scraped: {self.scraped_count}")
        logger.info(f"Total errors: {self.error_count}")
        
        return {
            'total_scraped': self.scraped_count,
            'total_errors': self.error_count,
            'pages_scraped': total_pages - start_page + 1
        }

def main():
    """Main execution function"""
    scraper = BigSpeakMainDirectoryScraper()
    
    # Create index on speaker_id for better performance
    try:
        scraper.collection.create_index('speaker_id', unique=True)
        scraper.collection.create_index('name')
        logger.info("Database indexes created/verified")
    except Exception as e:
        logger.warning(f"Could not create indexes: {e}")
    
    # Start scraping (set max_pages for testing, remove for full scrape)
    results = scraper.scrape_all_pages(start_page=1, max_pages=None)  # Test with 5 pages first
    
    print(f"\n{'='*50}")
    print(f"Scraping Summary:")
    print(f"- Total speakers scraped: {results['total_scraped']}")
    print(f"- Total errors: {results['total_errors']}")
    print(f"- Pages processed: {results['pages_scraped']}")
    print(f"{'='*50}")

if __name__ == "__main__":
    main()