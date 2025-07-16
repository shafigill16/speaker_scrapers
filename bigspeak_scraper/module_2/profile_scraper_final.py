import requests
from bs4 import BeautifulSoup
import time
import logging
import re
import json
from urllib.parse import urljoin, urlparse
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
    
    def extract_structured_data(self, soup):
        """Extract structured data from JSON-LD"""
        structured_data = {}
        
        # Find all JSON-LD scripts
        json_ld_scripts = soup.find_all('script', type='application/ld+json')
        
        for script in json_ld_scripts:
            try:
                data = json.loads(script.string)
                
                # Handle both single objects and arrays
                if isinstance(data, list):
                    for item in data:
                        if item.get('@type') == 'Person':
                            structured_data.update({
                                'job_title': item.get('jobTitle', ''),
                                'description_structured': item.get('description', ''),
                                'email': item.get('email', ''),
                                'telephone': item.get('telephone', ''),
                                'address': item.get('address', {})
                            })
                elif isinstance(data, dict):
                    if data.get('@type') == 'Person' or (data.get('@graph') and any(g.get('@type') == 'Person' for g in data['@graph'])):
                        structured_data.update(data)
                        
            except json.JSONDecodeError:
                logger.warning("Failed to parse JSON-LD data")
                
        return structured_data
    
    def extract_location(self, soup, structured_data):
        """Extract speaker's location/travel from information"""
        location_info = {}
        
        # Look for "Travels From" in the bullets section
        # BigSpeak has a consistent structure with labeled list items
        bullet_items = soup.find_all('li', class_='secondary')
        
        for li in bullet_items:
            label_elem = li.find('p', class_='label')
            if label_elem and 'travels from' in label_elem.get_text(strip=True).lower():
                # Found the travels from section
                value_elem = li.find('p', class_='value')
                if value_elem:
                    location_text = value_elem.get_text(strip=True)
                    # Clean up the location text
                    location_text = location_text.replace('\n', ' ').strip()
                    location_text = ' '.join(location_text.split())  # Normalize whitespace
                    
                    if location_text:
                        location_info['travels_from'] = location_text
                        return location_info
        
        # Fallback: check structured data
        if not location_info.get('travels_from') and structured_data.get('address'):
            addr = structured_data['address']
            if isinstance(addr, dict):
                location_parts = []
                if addr.get('addressLocality'):
                    location_parts.append(addr['addressLocality'])
                if addr.get('addressRegion'):
                    location_parts.append(addr['addressRegion'])
                if addr.get('addressCountry'):
                    location_parts.append(addr['addressCountry'])
                
                if location_parts:
                    location_info['travels_from'] = ', '.join(location_parts)
        
        return location_info
    
    def extract_languages(self, soup):
        """Extract languages spoken"""
        languages = []
        
        # Look for the languages section - it's typically in a list item with label/value structure
        # Find all list items
        list_items = soup.find_all('li', class_='secondary')
        
        for li in list_items:
            label_elem = li.find('p', class_='label')
            if label_elem and 'languages spoken' in label_elem.get_text(strip=True).lower():
                # Found the languages section
                value_elem = li.find('p', class_='value')
                if value_elem:
                    lang_text = value_elem.get_text(strip=True)
                    # Split by common separators
                    langs = re.split(r'[,;]|\sand\s', lang_text)
                    languages = [l.strip() for l in langs if l.strip() and len(l) > 1]
                    break
        
        return languages
    
    def extract_why_section(self, soup):
        """Extract the 'Why [Speaker Name]?' section"""
        why_section = soup.find('h4', string=re.compile(r'Why.*\?', re.I))
        
        if why_section:
            # Find the parent container
            container = why_section.find_parent('div', class_='body-content')
            if container:
                # Find the content div after the heading
                content_div = container.find_next_sibling('div', class_='entry-content')
                if content_div:
                    # Get text from all elements
                    text_parts = []
                    for elem in content_div.find_all(['p', 'span']):
                        text = elem.get_text(strip=True)
                        if text and len(text) > 20:
                            text_parts.append(text)
                    return ' '.join(text_parts)
        
        return ""
    
    def extract_speaker_topics(self, soup):
        """Extract keynote speaker topics"""
        topics = []
        
        # Find the topics list
        topics_heading = soup.find('h4', string=re.compile(r'Keynote Speaker Topics', re.I))
        if topics_heading:
            topics_list = topics_heading.find_next('ul', class_='topics')
            if topics_list:
                for li in topics_list.find_all('li'):
                    topic = li.get_text(strip=True)
                    if topic:
                        topics.append(topic)
        
        return topics
    
    def extract_speaking_programs(self, soup):
        """Extract detailed speaking programs/presentations"""
        programs = []
        
        # Look for "Suggested Keynote Speaker Programs" section
        programs_heading = soup.find('h4', string=re.compile(r'Suggested Keynote Speaker Programs', re.I))
        
        if programs_heading:
            # Find the content container
            content_container = programs_heading.find_parent('div').find_next_sibling('div', class_='with-padding')
            
            if content_container:
                # Find all program entries
                program_entries = content_container.find_all('div', class_='entry-content')
                
                for entry in program_entries:
                    program = {}
                    
                    # Get title
                    title_elem = entry.find(['h4', 'h3'])
                    if title_elem:
                        program['title'] = title_elem.get_text(strip=True)
                    
                    # Get short description
                    short_desc = entry.find('p', class_='short-text')
                    if short_desc:
                        program['short_description'] = short_desc.get_text(strip=True)
                    
                    # Get full description and key takeaways
                    long_text = entry.find('div', class_='long-text')
                    if long_text:
                        # Extract paragraphs
                        desc_parts = []
                        for p in long_text.find_all('p'):
                            text = p.get_text(strip=True)
                            if text:
                                desc_parts.append(text)
                        program['full_description'] = '\n\n'.join(desc_parts)
                        
                        # Extract key takeaways
                        takeaways = []
                        ul = long_text.find('ul')
                        if ul:
                            for li in ul.find_all('li'):
                                takeaway = li.get_text(strip=True)
                                if takeaway:
                                    takeaways.append(takeaway)
                        program['key_takeaways'] = takeaways
                    
                    if program.get('title'):
                        programs.append(program)
        
        return programs
    
    def extract_suggested_programs(self, soup):
        """Extract the program details from Suggested Keynote Speaker Programs section"""
        suggested_programs = []
        
        # Find the suggested programs heading
        suggested_heading = soup.find('h4', string=re.compile(r'Suggested Keynote Speaker Programs', re.I))
        
        if suggested_heading:
            # Get the container with all programs
            container = suggested_heading.find_parent('div').find_next_sibling('div', class_='with-padding')
            
            if container:
                # Find all program entries
                program_entries = container.find_all('div', class_='entry-content')
                
                for entry in program_entries:
                    program = {}
                    
                    # Get program title
                    title_elem = entry.find('h4', class_='uppercase')
                    if title_elem:
                        program['title'] = title_elem.get_text(strip=True)
                    
                    # Get short description
                    short_desc = entry.find('p', class_='short-text')
                    if short_desc:
                        program['short_description'] = short_desc.get_text(strip=True)
                    
                    # Get full description from long-text div
                    long_text_div = entry.find('div', class_='long-text')
                    if long_text_div:
                        # Extract main description paragraphs
                        desc_div = long_text_div.find('div', class_='body_14')
                        if desc_div:
                            desc_paragraphs = []
                            for p in desc_div.find_all('p'):
                                text = p.get_text(strip=True)
                                if text:
                                    desc_paragraphs.append(text)
                            program['full_description'] = '\n\n'.join(desc_paragraphs)
                        
                        # Extract audience takeaways
                        takeaways_div = long_text_div.find('div', class_='list_body')
                        if takeaways_div:
                            takeaways = []
                            ul = takeaways_div.find('ul')
                            if ul:
                                for li in ul.find_all('li'):
                                    takeaway = li.get_text(strip=True)
                                    if takeaway:
                                        takeaways.append(takeaway)
                            program['audience_takeaways'] = takeaways
                    
                    if program.get('title'):
                        suggested_programs.append(program)
        
        return suggested_programs
    
    def extract_biography(self, soup):
        """Extract full biography text"""
        bio_texts = []
        
        # Look for the main content section after the programs
        # The biography is typically in an entry-content div without the speaker programs
        content_sections = soup.find_all('div', class_='entry-content')
        
        for section in content_sections:
            # Skip if it's inside a program section (has h4 with program title)
            if section.find_previous_sibling('h4'):
                continue
            
            # Skip if it's the "Why" section
            if section.find_previous('h4', string=re.compile(r'Why.*\?', re.I)):
                continue
                
            # Get all paragraphs
            paragraphs = section.find_all('p')
            for p in paragraphs:
                text = p.get_text(strip=True)
                # Filter out short paragraphs and contact info
                if len(text) > 50 and not any(skip in text.lower() for skip in ['questions?', 'contact us', 'info@bigspeak']):
                    bio_texts.append(text)
        
        # If no bio found, look for paragraphs that mention the speaker's career/background
        if not bio_texts:
            all_paragraphs = soup.find_all('p')
            for p in all_paragraphs:
                text = p.get_text(strip=True)
                # Look for biographical indicators
                if any(indicator in text.lower() for indicator in ['career', 'since', 'started', 'began', 'founded', 'author', 'expert']):
                    if len(text) > 100 and not any(skip in text.lower() for skip in ['questions?', 'contact us']):
                        bio_texts.append(text)
        
        # Combine and deduplicate
        full_bio = '\n\n'.join(list(dict.fromkeys(bio_texts)))
        return full_bio
    
    def extract_videos(self, soup):
        """Extract video information"""
        videos = []
        
        # Look for video section
        video_section = soup.find('div', class_=re.compile(r'row-videos|videos|video-gallery', re.I))
        
        if video_section:
            # Find all video items
            video_items = video_section.find_all(['div', 'article'], class_=re.compile(r'video|bs-videos-item', re.I))
            
            for item in video_items:
                video_data = {}
                
                # Find video link
                video_link = item.find('a', class_='lightbox-video') or item.find('a', href=re.compile(r'youtube|vimeo'))
                
                if video_link:
                    video_url = video_link.get('href', '')
                    video_title = video_link.get('title', '') or video_link.get_text(strip=True)
                    
                    # Extract video ID from YouTube URL
                    youtube_match = re.search(r'youtube\.com/embed/([^?]+)', video_url) or \
                                  re.search(r'youtube\.com/watch\?v=([^&]+)', video_url)
                    
                    if youtube_match:
                        video_id = youtube_match.group(1)
                        video_data = {
                            'platform': 'youtube',
                            'video_id': video_id,
                            'embed_url': f'https://www.youtube.com/embed/{video_id}',
                            'watch_url': f'https://www.youtube.com/watch?v={video_id}',
                            'title': video_title
                        }
                    elif 'vimeo' in video_url:
                        vimeo_match = re.search(r'vimeo\.com/(\d+)', video_url)
                        if vimeo_match:
                            video_id = vimeo_match.group(1)
                            video_data = {
                                'platform': 'vimeo',
                                'video_id': video_id,
                                'url': video_url,
                                'title': video_title
                            }
                    
                    # Get thumbnail if available
                    img_elem = item.find('span', class_='image') or item.find('img')
                    if img_elem:
                        # Extract from data-bg attribute
                        data_bg = img_elem.get('data-bg', '')
                        if 'url(' in data_bg:
                            thumbnail = data_bg.split('url(')[1].split(')')[0].strip()
                            video_data['thumbnail'] = thumbnail
                        elif img_elem.get('src'):
                            video_data['thumbnail'] = img_elem['src']
                    
                    if video_data:
                        videos.append(video_data)
        
        # Also check for embedded iframes
        all_iframes = soup.find_all('iframe', src=re.compile(r'youtube|vimeo'))
        for iframe in all_iframes:
            src = iframe.get('src', '')
            # Avoid duplicates
            if not any(src in str(v) for v in videos):
                youtube_match = re.search(r'youtube\.com/embed/([^?]+)', src)
                if youtube_match:
                    video_id = youtube_match.group(1)
                    videos.append({
                        'platform': 'youtube',
                        'video_id': video_id,
                        'embed_url': src,
                        'watch_url': f'https://www.youtube.com/watch?v={video_id}'
                    })
        
        return videos
    
    def extract_testimonials(self, soup):
        """Extract testimonials"""
        testimonials = []
        
        # Look for testimonials heading
        test_heading = soup.find('h4', string=re.compile(r'testimonial', re.I))
        if test_heading:
            # Find the slideshow container
            test_container = test_heading.find_next('div', class_='bs-slideshow-single')
            if test_container:
                # Find all testimonial boxes
                testimonial_boxes = test_container.find_all('div', class_='speaker-testimonial-box')
                
                for box in testimonial_boxes:
                    testimonial = {}
                    
                    # Extract quote text
                    text_div = box.find('div', class_='text')
                    if text_div:
                        quote_elem = text_div.find('p')
                        if quote_elem:
                            quote = quote_elem.get_text(strip=True)
                            # Clean up quote marks
                            quote = re.sub(r'^["\'""'']|["\'""'']$', '', quote)
                            testimonial['quote'] = quote
                    
                    # Extract attribution
                    meta_div = box.find('div', class_='meta')
                    if meta_div:
                        # Get company/author from title attribute or text
                        name_elem = meta_div.find('p', class_='name')
                        if name_elem:
                            # Try title attribute first
                            company = name_elem.get('title', '')
                            if not company:
                                strong_elem = name_elem.find('strong')
                                if strong_elem:
                                    company = strong_elem.get_text(strip=True)
                            if company:
                                testimonial['company'] = company
                    
                    if testimonial.get('quote'):
                        testimonials.append(testimonial)
        
        return testimonials
    
    def extract_books(self, soup):
        """Extract books and publications with purchase links"""
        books = []
        
        # Look in bio and main content
        text_content = soup.get_text()
        
        # Common book patterns
        book_patterns = [
            re.compile(r'author of[^.]+?"([^"]+)"', re.I),
            re.compile(r'wrote[^.]+?"([^"]+)"', re.I),
            re.compile(r'book[^.]+?"([^"]+)"', re.I),
            re.compile(r'"([^"]+)"[^.]*(?:bestseller|book)', re.I),
            re.compile(r'published[^.]+?"([^"]+)"', re.I)
        ]
        
        found_titles = set()
        
        for pattern in book_patterns:
            matches = pattern.findall(text_content)
            for match in matches:
                if isinstance(match, str) and len(match) > 3 and len(match) < 150:
                    # Basic validation - likely a book title
                    if not any(skip in match.lower() for skip in ['click here', 'contact us', 'learn more']):
                        found_titles.add(match.strip())
        
        # Look for Amazon links or book purchase links
        book_links = {}
        all_links = soup.find_all('a', href=True)
        for link in all_links:
            href = link['href']
            link_text = link.get_text(strip=True)
            
            # Check if it's a book purchase link
            if any(site in href for site in ['amazon.com', 'barnesandnoble.com', 'bookshop.org']):
                # Try to match with book titles
                matched = False
                for title in found_titles:
                    if title.lower() in link_text.lower() or title.lower() in href.lower():
                        book_links[title] = href
                        matched = True
                        break
                
                # Also check if the link text itself is a book title
                if not matched and link_text and len(link_text) > 5 and '"' not in link_text:
                    if any(indicator in href for indicator in ['/dp/', '/gp/product/', 'isbn']):
                        found_titles.add(link_text)
                        book_links[link_text] = href
        
        # Convert to list with additional info if available
        for title in found_titles:
            book_info = {'title': title}
            
            # Add purchase link if available
            if title in book_links:
                book_info['purchase_link'] = book_links[title]
            
            # Check if it's mentioned as bestseller
            context = text_content[max(0, text_content.find(title) - 100):text_content.find(title) + 100]
            if 'bestsell' in context.lower():
                book_info['bestseller'] = True
            
            books.append(book_info)
        
        return books
    
    def extract_awards(self, soup):
        """Extract awards and recognitions"""
        awards = []
        
        # Look for award mentions in content
        award_patterns = [
            re.compile(r'(named|recognized|awarded|recipient of)[^.]+', re.I),
            re.compile(r'(won|received|earned)[^.]+?(award|recognition|honor)', re.I),
            re.compile(r'[^.]+?(award|prize|honor|recognition)[^.]+', re.I)
        ]
        
        text_content = soup.get_text()
        found_awards = set()
        
        for pattern in award_patterns:
            matches = pattern.findall(text_content)
            for match in matches:
                if isinstance(match, tuple):
                    match = ' '.join(match)
                
                # Clean up and validate
                award_text = match.strip()
                if len(award_text) > 20 and len(award_text) < 300:
                    # Filter out generic text
                    if not any(skip in award_text.lower() for skip in ['contact us', 'click here', 'learn more']):
                        found_awards.add(award_text)
        
        return list(found_awards)
    
    def extract_social_media(self, soup):
        """Extract speaker's personal social media links"""
        social_links = {
            'twitter': None,
            'linkedin': None,
            'facebook': None,
            'instagram': None,
            'youtube': None,
            'website': None
        }
        
        # Look for social media links
        all_links = soup.find_all('a', href=True)
        
        # BigSpeak's social media URLs to exclude
        bigspeak_social = [
            'bigspeak.com',
            '/BigSpeak',
            'pages/BigSpeak',
            'company/1045467',  # BigSpeak's LinkedIn
            '@bigspeak'
        ]
        
        for link in all_links:
            href = link['href']
            
            # Skip BigSpeak's own social media
            if any(bs in href for bs in bigspeak_social):
                continue
            
            # Check for social platforms
            if 'twitter.com' in href or 'x.com' in href:
                social_links['twitter'] = href
            elif 'linkedin.com/in/' in href:
                social_links['linkedin'] = href
            elif 'facebook.com' in href and 'facebook.com/pages' not in href:
                social_links['facebook'] = href
            elif 'instagram.com' in href:
                social_links['instagram'] = href
            elif 'youtube.com/@' in href or 'youtube.com/c/' in href or 'youtube.com/user/' in href:
                social_links['youtube'] = href
            elif link.text and 'website' in link.text.lower() and 'bigspeak' not in href:
                social_links['website'] = href
        
        return {k: v for k, v in social_links.items() if v}  # Return only found links
    
    def extract_images(self, soup, speaker_name):
        """Extract all speaker images"""
        images = []
        
        # Get main image from meta tags
        og_image = soup.find('meta', property='og:image')
        if og_image and og_image.get('content'):
            images.append({
                'url': og_image['content'],
                'type': 'primary',
                'source': 'og:image'
            })
        
        # Get images from content
        img_tags = soup.find_all('img')
        name_parts = speaker_name.lower().split()
        
        for img in img_tags:
            src = img.get('src', '')
            alt = img.get('alt', '').lower()
            
            # Check if image is likely of the speaker
            if any(part in src.lower() or part in alt for part in name_parts):
                # Skip small images
                width = img.get('width', '')
                if width and int(width) < 100:
                    continue
                
                full_url = urljoin(BASE_URL, src)
                
                # Try to get high-res version
                high_res_url = full_url
                if re.search(r'-\d+x\d+\.(jpg|jpeg|png)', full_url):
                    # Remove WordPress size suffix
                    high_res_url = re.sub(r'-\d+x\d+(\.\w+)$', r'\1', full_url)
                
                images.append({
                    'url': high_res_url,
                    'alt': img.get('alt', ''),
                    'width': img.get('width', ''),
                    'height': img.get('height', ''),
                    'type': 'content'
                })
        
        # Get lazy-loaded images
        lazy_images = soup.find_all(['div', 'span', 'a'], attrs={'data-bg': True})
        for elem in lazy_images:
            data_bg = elem.get('data-bg', '')
            if 'url(' in data_bg:
                img_url = data_bg.split('url(')[1].split(')')[0].strip()
                if any(part in img_url.lower() for part in name_parts):
                    images.append({
                        'url': img_url,
                        'type': 'lazy-loaded'
                    })
        
        # Remove duplicates
        seen_urls = set()
        unique_images = []
        for img in images:
            if img['url'] not in seen_urls:
                seen_urls.add(img['url'])
                unique_images.append(img)
        
        return unique_images
    
    def extract_additional_info(self, soup):
        """Extract any additional information not covered by other methods"""
        additional_info = {}
        
        # Extract meta description
        meta_desc = soup.find('meta', attrs={'name': 'description'})
        if meta_desc:
            additional_info['meta_description'] = meta_desc.get('content', '')
        
        # Extract any custom fields or data attributes
        speaker_elem = soup.find('body', class_=re.compile(r'speaker-'))
        if speaker_elem:
            classes = speaker_elem.get('class', [])
            for cls in classes:
                if 'postid-' in cls:
                    additional_info['post_id'] = cls.replace('postid-', '')
        
        # Look for any download links (one-sheets, etc.)
        download_links = soup.find_all('a', href=re.compile(r'\.(pdf|doc|docx)', re.I))
        if download_links:
            additional_info['downloads'] = []
            for link in download_links:
                additional_info['downloads'].append({
                    'text': link.get_text(strip=True),
                    'url': urljoin(BASE_URL, link['href'])
                })
        
        # Check for virtual capabilities
        if any(term in soup.get_text().lower() for term in ['virtual keynote', 'virtual speaker', 'virtual presentation']):
            additional_info['virtual_capable'] = True
        
        return additional_info
    
    def scrape_profile(self, speaker):
        """Scrape detailed information from a speaker's profile page"""
        profile_url = speaker['profile_url']
        speaker_id = speaker['speaker_id']
        
        logger.info(f"Scraping profile for {speaker['name']} - {profile_url}")
        
        response = self.get_page(profile_url)
        if not response:
            return None
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Extract structured data first
        structured_data = self.extract_structured_data(soup)
        
        # Extract all profile data
        profile_data = {
            'speaker_id': speaker_id,
            'name': speaker['name'],
            'profile_url': profile_url,
            'structured_data': structured_data,
            'location': self.extract_location(soup, structured_data),
            'languages': self.extract_languages(soup),
            'why_choose': self.extract_why_section(soup),
            'keynote_topics': self.extract_speaker_topics(soup),
            'speaking_programs': self.extract_speaking_programs(soup),
            'suggested_programs': self.extract_suggested_programs(soup),
            'biography': self.extract_biography(soup),
            'videos': self.extract_videos(soup),
            'testimonials': self.extract_testimonials(soup),
            'books': self.extract_books(soup),
            'awards': self.extract_awards(soup),
            'social_media': self.extract_social_media(soup),
            'images': self.extract_images(soup, speaker['name']),
            'additional_info': self.extract_additional_info(soup),
            'scraped_at': datetime.utcnow(),
            'source': 'profile_page_final'
        }
        
        # Add existing data from module_1
        profile_data['basic_info'] = {
            'description': speaker.get('description', ''),
            'fee_range': speaker.get('fee_range', ''),
            'topics': speaker.get('topics', []),
            'image_url': speaker.get('image_url', '')
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
            # Get speaker IDs that already have profiles with final source
            existing_profiles = self.profiles_collection.distinct(
                'speaker_id', 
                {'source': 'profile_page_final'}
            )
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
        scraper.profiles_collection.create_index('source')
        logger.info("Database indexes created/verified")
    except Exception as e:
        logger.warning(f"Could not create indexes: {e}")
    
    # Check how many profiles we need to scrape
    total_speakers = scraper.speakers_collection.count_documents({})
    existing_profiles_final = scraper.profiles_collection.count_documents({'source': 'profile_page_final'})
    
    print(f"\n{'='*50}")
    print(f"Profile Scraping Status (Final):")
    print(f"- Total speakers in database: {total_speakers}")
    print(f"- Existing Final profiles: {existing_profiles_final}")
    print(f"- Profiles to scrape: {total_speakers - existing_profiles_final}")
    print(f"{'='*50}\n")
    
    # Start scraping (set limit=None to scrape all profiles)
    results = scraper.scrape_all_profiles(limit=None, skip_existing=True)  # Change to limit=None for full scrape
    
    print(f"\n{'='*50}")
    print(f"Scraping Summary:")
    print(f"- Profiles scraped: {results['total_scraped']}")
    print(f"- Errors: {results['total_errors']}")
    print(f"- Total attempted: {results['total_attempted']}")
    print(f"{'='*50}")

if __name__ == "__main__":
    main()