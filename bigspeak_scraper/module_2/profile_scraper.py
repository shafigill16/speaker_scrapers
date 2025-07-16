import requests
from bs4 import BeautifulSoup
import time
import logging
import re
from urllib.parse import urljoin
from datetime import datetime
from config import (
    BASE_URL, REQUEST_TIMEOUT, RETRY_ATTEMPTS, 
    DELAY_BETWEEN_REQUESTS, BATCH_SIZE,
    get_speakers_collection, get_profiles_collection
)

logger = logging.getLogger(__name__)

class BigSpeakProfileScraper:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
        })
        self.speakers_collection = get_speakers_collection()
        self.profiles_collection = get_profiles_collection()
        self.scraped_count = 0
        self.error_count = 0
    
    def get_page(self, url):
        """Fetch a page with retry logic"""
        for attempt in range(RETRY_ATTEMPTS):
            try:
                logger.debug(f"Fetching: {url} (Attempt {attempt + 1})")
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
    
    def extract_biography(self, soup):
        """Extract full biography text"""
        bio_texts = []
        
        # Look for main content area
        main_content = soup.find('main') or soup.find('article') or soup.find('div', class_='content')
        
        if main_content:
            # Find all paragraphs that look like bio content
            paragraphs = main_content.find_all('p')
            
            for p in paragraphs:
                text = p.text.strip()
                # Filter out short paragraphs and navigation text
                if len(text) > 50 and not any(skip in text.lower() for skip in ['contact us', 'call us', 'fee range']):
                    bio_texts.append(text)
        
        # Also check for specific bio sections
        bio_sections = soup.find_all(['div', 'section'], class_=re.compile(r'bio|about|description', re.I))
        for section in bio_sections:
            text = section.get_text(separator='\n', strip=True)
            if text and len(text) > 50:
                bio_texts.append(text)
        
        # Combine and deduplicate
        full_bio = '\n\n'.join(list(dict.fromkeys(bio_texts)))
        return full_bio
    
    def extract_speaking_topics(self, soup):
        """Extract detailed speaking topics"""
        topics = []
        
        # Look for speaking topics sections
        topics_sections = soup.find_all(['div', 'section'], string=re.compile(r'speaking topics|keynote topics', re.I))
        
        for section in topics_sections:
            parent = section.find_parent()
            if parent:
                # Look for topic lists
                topic_items = parent.find_all(['li', 'div', 'h3', 'h4'])
                for item in topic_items:
                    topic_text = item.text.strip()
                    if topic_text and len(topic_text) > 5:
                        topics.append(topic_text)
        
        # Also look for topics in specific patterns
        topic_headers = soup.find_all(['h3', 'h4'], string=re.compile(r'^[A-Z].*:.*'))
        for header in topic_headers:
            topic_title = header.text.strip()
            # Get description (next sibling paragraph)
            next_elem = header.find_next_sibling()
            if next_elem and next_elem.name == 'p':
                topic_desc = next_elem.text.strip()
                topics.append({
                    'title': topic_title,
                    'description': topic_desc
                })
            else:
                topics.append({'title': topic_title, 'description': ''})
        
        return topics
    
    def extract_books(self, soup):
        """Extract books and publications"""
        books = []
        
        # Look for book mentions
        book_patterns = [
            re.compile(r'author of[^.]+', re.I),
            re.compile(r'wrote[^.]+book[^.]+', re.I),
            re.compile(r'bestsell[^.]+', re.I),
            re.compile(r'"([^"]+)".*book', re.I)
        ]
        
        text_content = soup.get_text()
        for pattern in book_patterns:
            matches = pattern.findall(text_content)
            for match in matches:
                if isinstance(match, str) and len(match) > 5:
                    books.append(match.strip())
        
        # Look for book titles in quotes
        quoted_titles = re.findall(r'"([^"]+)"', text_content)
        for title in quoted_titles:
            # Check if it looks like a book title
            if len(title.split()) >= 2 and len(title) < 100:
                # Check context around the quote
                context = text_content[max(0, text_content.find(title) - 50):text_content.find(title) + len(title) + 50]
                if any(word in context.lower() for word in ['book', 'author', 'wrote', 'published']):
                    books.append(title)
        
        return list(dict.fromkeys(books))  # Remove duplicates
    
    def extract_videos(self, soup):
        """Extract video URLs and titles"""
        videos = []
        
        # Look for YouTube embeds
        iframes = soup.find_all('iframe', src=re.compile(r'youtube|vimeo'))
        for iframe in iframes:
            video_url = iframe.get('src', '')
            if video_url:
                # Extract video ID from YouTube URL
                youtube_match = re.search(r'youtube\.com/embed/([^?]+)', video_url)
                if youtube_match:
                    video_id = youtube_match.group(1)
                    videos.append({
                        'platform': 'youtube',
                        'video_id': video_id,
                        'embed_url': video_url,
                        'watch_url': f'https://www.youtube.com/watch?v={video_id}'
                    })
        
        # Look for video links
        video_links = soup.find_all('a', href=re.compile(r'youtube\.com/watch|vimeo\.com'))
        for link in video_links:
            videos.append({
                'platform': 'youtube' if 'youtube' in link['href'] else 'vimeo',
                'url': link['href'],
                'title': link.text.strip() if link.text else ''
            })
        
        return videos
    
    def extract_awards(self, soup):
        """Extract awards and recognitions"""
        awards = []
        
        # Look for award mentions
        award_keywords = ['award', 'recognition', 'honor', 'named', 'acclaimed', 'recipient']
        
        for keyword in award_keywords:
            elements = soup.find_all(string=re.compile(rf'\b{keyword}\b', re.I))
            for elem in elements:
                # Get the full sentence
                parent = elem.parent
                if parent:
                    text = parent.text.strip()
                    if len(text) > 20 and len(text) < 300:
                        awards.append(text)
        
        return list(dict.fromkeys(awards))  # Remove duplicates
    
    def extract_social_media(self, soup):
        """Extract speaker's social media links"""
        social_links = {
            'twitter': None,
            'linkedin': None,
            'facebook': None,
            'instagram': None,
            'website': None
        }
        
        # Look for social media links
        all_links = soup.find_all('a', href=True)
        for link in all_links:
            href = link['href'].lower()
            
            if 'twitter.com' in href and '/bigspeak' not in href:
                social_links['twitter'] = link['href']
            elif 'linkedin.com/in/' in href:
                social_links['linkedin'] = link['href']
            elif 'facebook.com' in href and '/bigspeak' not in href:
                social_links['facebook'] = link['href']
            elif 'instagram.com' in href and '/bigspeak' not in href:
                social_links['instagram'] = link['href']
            elif link.text and 'website' in link.text.lower():
                social_links['website'] = link['href']
        
        return {k: v for k, v in social_links.items() if v}  # Return only found links
    
    def extract_credentials(self, soup):
        """Extract additional credentials and expertise"""
        credentials = []
        
        # Look for education
        edu_patterns = [
            re.compile(r'(B\.?A\.?|B\.?S\.?|M\.?A\.?|M\.?S\.?|M\.?B\.?A\.?|Ph\.?D\.?)[^.]*University[^.]*', re.I),
            re.compile(r'graduated from[^.]+', re.I),
            re.compile(r'degree in[^.]+', re.I)
        ]
        
        text_content = soup.get_text()
        for pattern in edu_patterns:
            matches = pattern.findall(text_content)
            credentials.extend(matches)
        
        # Look for certifications and positions
        cert_keywords = ['certified', 'instructor', 'professor', 'fellow', 'member', 'board']
        for keyword in cert_keywords:
            elements = soup.find_all(string=re.compile(rf'\b{keyword}\b', re.I))
            for elem in elements[:5]:  # Limit to avoid too many
                parent = elem.parent
                if parent:
                    text = parent.text.strip()
                    if len(text) > 10 and len(text) < 200:
                        credentials.append(text)
        
        return list(dict.fromkeys(credentials))  # Remove duplicates
    
    def extract_images(self, soup, speaker_name):
        """Extract high-resolution speaker images"""
        images = []
        
        # Look for images with speaker name in URL or alt text
        img_tags = soup.find_all('img')
        name_parts = speaker_name.lower().split()
        
        for img in img_tags:
            src = img.get('src', '')
            alt = img.get('alt', '').lower()
            
            # Check if image is likely of the speaker
            if any(part in src.lower() or part in alt for part in name_parts):
                # Get full URL
                full_url = urljoin(BASE_URL, src)
                
                # Try to get high-res version
                high_res_url = full_url
                if '-300x' in full_url or '-150x' in full_url:
                    # Remove WordPress size suffix
                    high_res_url = re.sub(r'-\d+x\d+(\.\w+)$', r'\1', full_url)
                
                images.append({
                    'url': high_res_url,
                    'alt': img.get('alt', ''),
                    'width': img.get('width', ''),
                    'height': img.get('height', '')
                })
        
        # Also check Open Graph image
        og_image = soup.find('meta', property='og:image')
        if og_image and og_image.get('content'):
            images.append({
                'url': og_image['content'],
                'type': 'og:image'
            })
        
        return images
    
    def scrape_profile(self, speaker):
        """Scrape detailed information from a speaker's profile page"""
        profile_url = speaker['profile_url']
        speaker_id = speaker['speaker_id']
        
        logger.info(f"Scraping profile for {speaker['name']} - {profile_url}")
        
        response = self.get_page(profile_url)
        if not response:
            return None
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Extract all profile data
        profile_data = {
            'speaker_id': speaker_id,
            'name': speaker['name'],
            'profile_url': profile_url,
            'biography': self.extract_biography(soup),
            'speaking_topics_detailed': self.extract_speaking_topics(soup),
            'books': self.extract_books(soup),
            'videos': self.extract_videos(soup),
            'awards': self.extract_awards(soup),
            'social_media': self.extract_social_media(soup),
            'credentials': self.extract_credentials(soup),
            'images': self.extract_images(soup, speaker['name']),
            'scraped_at': datetime.utcnow(),
            'source': 'profile_page'
        }
        
        # Add existing data from module_1
        profile_data['basic_info'] = {
            'description': speaker.get('description', ''),
            'fee_range': speaker.get('fee_range', ''),
            'topics': speaker.get('topics', [])
        }
        
        return profile_data
    
    def save_profile(self, profile_data):
        """Save profile data to MongoDB"""
        if not profile_data:
            return
        
        try:
            # Use speaker_id as unique identifier
            filter_query = {'speaker_id': profile_data['speaker_id']}
            
            # Update with new data
            update_query = {
                '$set': profile_data,
                '$setOnInsert': {
                    'first_scraped_at': datetime.utcnow()
                }
            }
            
            self.profiles_collection.update_one(filter_query, update_query, upsert=True)
            self.scraped_count += 1
            logger.info(f"Saved profile for {profile_data['name']}")
            
        except Exception as e:
            logger.error(f"Error saving profile: {e}")
            self.error_count += 1
    
    def get_speakers_to_scrape(self, limit=None, skip_existing=True):
        """Get list of speakers to scrape profiles for"""
        query = {}
        
        if skip_existing:
            # Get speaker IDs that already have profiles
            existing_profiles = self.profiles_collection.distinct('speaker_id')
            query = {'speaker_id': {'$nin': existing_profiles}}
        
        cursor = self.speakers_collection.find(query)
        
        if limit:
            cursor = cursor.limit(limit)
        
        return list(cursor)
    
    def scrape_all_profiles(self, limit=None, skip_existing=True):
        """Scrape profiles for all speakers"""
        speakers = self.get_speakers_to_scrape(limit=limit, skip_existing=skip_existing)
        total_speakers = len(speakers)
        
        logger.info(f"Starting profile scraping for {total_speakers} speakers")
        
        for i, speaker in enumerate(speakers, 1):
            logger.info(f"\n--- Processing {i}/{total_speakers}: {speaker['name']} ---")
            
            # Scrape profile
            profile_data = self.scrape_profile(speaker)
            
            if profile_data:
                self.save_profile(profile_data)
            else:
                logger.warning(f"Failed to scrape profile for {speaker['name']}")
            
            # Progress update
            if i % 10 == 0:
                logger.info(f"\nProgress: {i}/{total_speakers} profiles completed")
                logger.info(f"Successfully scraped: {self.scraped_count}")
                logger.info(f"Errors: {self.error_count}")
            
            # Rate limiting
            time.sleep(DELAY_BETWEEN_REQUESTS)
        
        logger.info(f"\n=== Profile scraping completed ===")
        logger.info(f"Total profiles scraped: {self.scraped_count}")
        logger.info(f"Total errors: {self.error_count}")
        
        return {
            'total_scraped': self.scraped_count,
            'total_errors': self.error_count,
            'total_attempted': total_speakers
        }

def main():
    """Main execution function"""
    scraper = BigSpeakProfileScraper()
    
    # Create index on speaker_id for better performance
    try:
        scraper.profiles_collection.create_index('speaker_id', unique=True)
        scraper.profiles_collection.create_index('name')
        logger.info("Database indexes created/verified")
    except Exception as e:
        logger.warning(f"Could not create indexes: {e}")
    
    # Check how many profiles we need to scrape
    total_speakers = scraper.speakers_collection.count_documents({})
    existing_profiles = scraper.profiles_collection.count_documents({})
    
    print(f"\n{'='*50}")
    print(f"Profile Scraping Status:")
    print(f"- Total speakers in database: {total_speakers}")
    print(f"- Existing profiles: {existing_profiles}")
    print(f"- Profiles to scrape: {total_speakers - existing_profiles}")
    print(f"{'='*50}\n")
    
    # Start scraping (limit to 10 for testing)
    results = scraper.scrape_all_profiles(limit=10, skip_existing=True)
    
    print(f"\n{'='*50}")
    print(f"Scraping Summary:")
    print(f"- Profiles scraped: {results['total_scraped']}")
    print(f"- Errors: {results['total_errors']}")
    print(f"- Total attempted: {results['total_attempted']}")
    print(f"{'='*50}")

if __name__ == "__main__":
    main()