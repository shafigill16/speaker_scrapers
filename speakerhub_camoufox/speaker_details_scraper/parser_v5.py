#!/usr/bin/env python3
"""
Comprehensive HTML parser for extracting ALL detailed speaker information - Version 5
Complete rewrite with proper field extraction based on actual HTML structure
"""

import re
import logging
from bs4 import BeautifulSoup, NavigableString, Tag
from typing import List, Dict, Optional, Tuple, Any
from models import (
    DetailedSpeaker, SpeakerFee, PastTalk, Education, 
    Publication, Presentation, Workshop, Testimonial
)
import json

logger = logging.getLogger(__name__)


class SpeakerDetailsParserV5:
    """Parse HTML content to extract ALL detailed speaker information - Version 5"""
    
    def __init__(self):
        self.soup = None
        
    def parse(self, html_content: str, basic_info: Dict) -> DetailedSpeaker:
        """Parse HTML and return DetailedSpeaker object"""
        self.soup = BeautifulSoup(html_content, 'html.parser')
        
        # Start with basic info from part 1
        speaker = DetailedSpeaker(
            uid=basic_info.get('uid', ''),
            profile_url=basic_info.get('profile_url', ''),
            name=basic_info.get('name', ''),
            first_name=basic_info.get('first_name'),
            last_name=basic_info.get('last_name')
        )
        
        # Extract all information using multiple strategies
        self._extract_all_fields(speaker)
        self._extract_from_fieldsets(speaker)
        self._extract_from_javascript_settings(speaker)
        self._extract_from_structured_data(speaker)
        
        # Mark as completed
        speaker.scraping_status = 'completed'
        
        return speaker
        
    def _extract_all_fields(self, speaker: DetailedSpeaker):
        """Extract all fields using direct field selectors"""
        
        # Professional info
        self._extract_field_text(speaker, 'professional_title', 'field-name-field-user-professional-title')
        self._extract_field_text(speaker, 'pronouns', 'field-name-field-preferred-pronouns')
        self._extract_field_text(speaker, 'job_title', 'field-name-field-job')
        self._extract_field_text(speaker, 'company', 'field-name-field-company')
        
        # Location
        self._extract_location_comprehensive(speaker)
        self._extract_field_text(speaker, 'timezone', 'field-name-field-user-timezone')
        
        # Languages - special handling for links
        self._extract_languages_comprehensive(speaker)
        
        # Event types and fees
        self._extract_event_types_comprehensive(speaker)
        
        # Topics
        self._extract_topics_comprehensive(speaker)
        
        # Bio
        self._extract_bio_comprehensive(speaker)
        
        # Media and profile picture
        self._extract_media_comprehensive(speaker)
        
        # Fee range
        self._extract_fee_comprehensive(speaker)
        
    def _extract_field_text(self, speaker: DetailedSpeaker, attr_name: str, field_class: str):
        """Generic field text extraction"""
        try:
            field = self.soup.find('div', class_=field_class)
            if field:
                field_item = field.find('div', class_='field-item')
                if field_item:
                    text = field_item.get_text(strip=True)
                    if text:
                        setattr(speaker, attr_name, text)
        except Exception as e:
            logger.error(f"Error extracting {attr_name}: {e}")
            
    def _extract_location_comprehensive(self, speaker: DetailedSpeaker):
        """Extract location with special parsing"""
        try:
            # Country field often contains "Country (State)"
            country_field = self.soup.find('div', class_='field-name-field-country')
            if country_field:
                country_item = country_field.find('div', class_='field-item')
                if country_item:
                    location_text = country_item.get_text(strip=True)
                    # Parse "Canada (Ontario)" format
                    match = re.match(r'([^(]+)(?:\s*\(([^)]+)\))?', location_text)
                    if match:
                        speaker.country = match.group(1).strip()
                        if match.group(2):
                            speaker.state_province = match.group(2).strip()
                            
            # City
            self._extract_field_text(speaker, 'city', 'field-name-field-user-city')
            
        except Exception as e:
            logger.error(f"Error extracting location: {e}")
            
    def _extract_languages_comprehensive(self, speaker: DetailedSpeaker):
        """Extract languages with link handling"""
        try:
            lang_field = self.soup.find('div', class_='field-name-field-languages')
            if lang_field:
                # Look for all links or text within field-items
                field_items = lang_field.find_all('div', class_='field-item')
                for item in field_items:
                    # Try links first
                    links = item.find_all('a')
                    if links:
                        for link in links:
                            lang = link.get_text(strip=True)
                            if lang and lang not in speaker.languages:
                                speaker.languages.append(lang)
                    else:
                        # Direct text
                        text = item.get_text(strip=True)
                        if text and text not in speaker.languages:
                            speaker.languages.append(text)
                            
        except Exception as e:
            logger.error(f"Error extracting languages: {e}")
            
    def _extract_event_types_comprehensive(self, speaker: DetailedSpeaker):
        """Extract event types from icons"""
        try:
            event_field = self.soup.find('div', class_='field-name-field-event-type')
            if event_field:
                icons = event_field.find_all('i')
                
                event_type_map = {
                    'event-type-conference': 'Conference',
                    'event-type-workshop': 'Workshop',
                    'event-type-session': 'Session',
                    'event-type-moderator': 'Moderator',
                    'event-type-webinar': 'Webinar',
                    'event-type-volunteer': 'School (incl. charity)',
                    'event-type-meetup': 'Meetup',
                    'event-type-panel': 'Panel',
                    'event-type-cert': 'Certificate Program',
                    'event-type-emcee': 'Emcee'
                }
                
                for icon in icons:
                    classes = icon.get('class', [])
                    for cls in classes:
                        if cls in event_type_map:
                            event_type = event_type_map[cls]
                            # Check if active
                            if 'active' in classes or not any('active' in str(i.get('class', [])) for i in icons):
                                if event_type not in speaker.event_types:
                                    speaker.event_types.append(event_type)
                                    
                                # Extract tooltip for fee info
                                tooltip = icon.get('data-original-title', '') or icon.get('title', '')
                                fee = SpeakerFee(
                                    event_type=event_type,
                                    event_description=tooltip.split('<br')[0].strip() if tooltip else event_type,
                                    fee=None
                                )
                                
                                # Try to extract fee
                                if 'speaker fee:' in tooltip.lower():
                                    fee_match = re.search(r'speaker fee:\s*\$?([\d,]+)', tooltip, re.IGNORECASE)
                                    if fee_match:
                                        fee.fee = f"${fee_match.group(1)}"
                                        
                                speaker.speaker_fees.append(fee)
                            break
                            
        except Exception as e:
            logger.error(f"Error extracting event types: {e}")
            
    def _extract_topics_comprehensive(self, speaker: DetailedSpeaker):
        """Extract topics and tags"""
        try:
            # Topic categories
            topics_field = self.soup.find('div', class_='field-name-field-topics')
            if topics_field:
                links = topics_field.find_all('a')
                for link in links:
                    topic = link.get_text(strip=True)
                    if topic and topic not in speaker.topic_categories:
                        speaker.topic_categories.append(topic)
                        
            # Tags
            tags_field = self.soup.find('div', class_='field-name-field-tags')
            if tags_field:
                links = tags_field.find_all('a')
                for link in links:
                    tag = link.get_text(strip=True)
                    if tag and tag not in speaker.topics:
                        speaker.topics.append(tag)
                        
        except Exception as e:
            logger.error(f"Error extracting topics: {e}")
            
    def _extract_bio_comprehensive(self, speaker: DetailedSpeaker):
        """Extract bio from multiple sources"""
        try:
            # Bio summary field
            bio_field = self.soup.find('div', class_='field-name-field-bio-summary')
            if bio_field:
                bio_item = bio_field.find('div', class_='field-item')
                if bio_item:
                    speaker.bio_summary = bio_item.get_text(separator='\n', strip=True)
                    
            # Meta description as fallback
            if not speaker.bio_summary:
                meta_desc = self.soup.find('meta', {'name': 'description'})
                if meta_desc and meta_desc.get('content'):
                    speaker.bio_summary = meta_desc['content']
                    
            # Set why_choose_me and full_bio
            if speaker.bio_summary:
                speaker.why_choose_me = speaker.bio_summary
                speaker.full_bio = speaker.bio_summary
                
        except Exception as e:
            logger.error(f"Error extracting bio: {e}")
            
    def _extract_media_comprehensive(self, speaker: DetailedSpeaker):
        """Extract media resources"""
        try:
            # Profile picture
            pic_field = self.soup.find('div', class_='field-name-field-profile-picture')
            if pic_field:
                img = pic_field.find('img')
                if img and img.get('src'):
                    speaker.profile_picture_url = self._make_absolute_url(img['src'])
                    
            # Articles/links
            articles_field = self.soup.find('div', class_='field-name-field-u-articles')
            if articles_field:
                links = articles_field.find_all('a')
                for link in links:
                    if link.get_text(strip=True):
                        pub = Publication(
                            title=link.get_text(strip=True),
                            type='article',
                            url=link.get('href')
                        )
                        speaker.publications.append(pub)
                        
            # Media field for videos/press
            media_field = self.soup.find('div', class_='field-name-media')
            if media_field:
                # Look for press info
                press_items = media_field.find_all('div', class_='field-press-info')
                for item in press_items:
                    link = item.find('a')
                    if link and link.get('href'):
                        speaker.press_kit_url = self._make_absolute_url(link['href'])
                        
        except Exception as e:
            logger.error(f"Error extracting media: {e}")
            
    def _extract_fee_comprehensive(self, speaker: DetailedSpeaker):
        """Extract fee range"""
        try:
            fee_field = self.soup.find('div', class_='field-name-field-user-fee-category')
            if fee_field:
                fee_div = fee_field.find('div', class_='speaker-fee')
                if fee_div:
                    # Check tooltip
                    tooltip = fee_div.get('data-original-title', '') or fee_div.get('title', '')
                    if 'Fee range:' in tooltip:
                        speaker.fee_range = tooltip.replace('Fee range:', '').strip()
                    else:
                        # Count active indicators
                        active_items = fee_div.find_all('div', class_='price-item active')
                        if active_items:
                            fee_ranges = {
                                1: "< $1,000",
                                2: "$1,000 - $1,500",
                                3: "$1,500 - $5,000",
                                4: "$5,000 - $10,000",
                                5: "$10,000+"
                            }
                            speaker.fee_range = fee_ranges.get(len(active_items), "Unknown")
                            
        except Exception as e:
            logger.error(f"Error extracting fee range: {e}")
            
    def _extract_from_fieldsets(self, speaker: DetailedSpeaker):
        """Extract data from fieldset/panel structures"""
        try:
            fieldsets = self.soup.find_all('fieldset')
            
            for fieldset in fieldsets:
                legend = fieldset.find('legend')
                if not legend:
                    continue
                    
                legend_text = legend.get_text(strip=True).lower()
                
                if 'degrees' in legend_text:
                    self._extract_education_from_fieldset(speaker, fieldset)
                elif 'presentations' in legend_text or 'keynote' in legend_text:
                    self._extract_presentations_from_fieldset(speaker, fieldset)
                elif 'workshop' in legend_text or 'agenda' in legend_text:
                    self._extract_workshops_from_fieldset(speaker, fieldset)
                elif 'past talks' in legend_text or 'spoke at' in legend_text:
                    self._extract_past_talks_from_fieldset(speaker, fieldset)
                elif 'testimonial' in legend_text:
                    self._extract_testimonials_from_fieldset(speaker, fieldset)
                elif 'current position' in legend_text:
                    self._extract_current_position_from_fieldset(speaker, fieldset)
                    
        except Exception as e:
            logger.error(f"Error extracting from fieldsets: {e}")
            
    def _extract_education_from_fieldset(self, speaker: DetailedSpeaker, fieldset):
        """Extract education from fieldset"""
        try:
            # Find all education entries
            edu_groups = fieldset.find_all('div', class_='group-education')
            if not edu_groups:
                # Try alternative structure
                edu_groups = fieldset.find_all('div', class_='multiple-inline-element')
                
            for group in edu_groups:
                degree = None
                institution = None
                year = None
                
                # Faculty/Degree
                faculty_elem = group.find('div', class_=re.compile('field.*education.*faculty'))
                if faculty_elem:
                    degree = faculty_elem.get_text(strip=True)
                    
                # School/Institution
                school_elem = group.find('div', class_=re.compile('field.*education.*school'))
                if school_elem:
                    institution = school_elem.get_text(strip=True)
                    
                # Date
                date_elem = group.find('div', class_=re.compile('field.*education.*date'))
                if date_elem:
                    date_text = date_elem.get_text(strip=True)
                    # Extract years
                    year_match = re.search(r'(\d{4})\s*to\s*(\d{4})', date_text)
                    if year_match:
                        year = f"{year_match.group(1)}-{year_match.group(2)}"
                    else:
                        year_match = re.search(r'(\d{4})', date_text)
                        if year_match:
                            year = year_match.group(1)
                            
                if degree:
                    # Check if certification
                    cert_keywords = ['CERTIF', 'COACH', 'PCC', 'ACC', 'MCC', 'CEC', 'CMC', 'CERTIFIED', 'EQI', 'CHES']
                    if any(cert in degree.upper() for cert in cert_keywords):
                        cert_text = degree
                        if institution:
                            cert_text += f" from {institution}"
                        if year:
                            cert_text += f" ({year})"
                        if cert_text not in speaker.certifications:
                            speaker.certifications.append(cert_text)
                    else:
                        edu = Education(
                            degree=degree,
                            institution=institution,
                            year=year
                        )
                        speaker.education.append(edu)
                        
        except Exception as e:
            logger.error(f"Error extracting education: {e}")
            
    def _extract_presentations_from_fieldset(self, speaker: DetailedSpeaker, fieldset):
        """Extract presentations from fieldset"""
        try:
            # Find all presentation entries
            pres_groups = fieldset.find_all('div', class_='multiple-inline-element')
            
            for group in pres_groups:
                title = None
                description = None
                
                # Title
                title_elem = group.find('div', class_=re.compile('field.*keynote.*title'))
                if title_elem:
                    title = title_elem.get_text(strip=True)
                    
                # Description
                desc_elem = group.find('div', class_=re.compile('field.*keynote.*description'))
                if desc_elem:
                    description = desc_elem.get_text(separator='\n', strip=True)
                    
                if title:
                    pres = Presentation(
                        title=title,
                        description=description
                    )
                    speaker.presentations.append(pres)
                    
        except Exception as e:
            logger.error(f"Error extracting presentations: {e}")
            
    def _extract_workshops_from_fieldset(self, speaker: DetailedSpeaker, fieldset):
        """Extract workshops from fieldset"""
        try:
            # Find all workshop entries
            workshop_groups = fieldset.find_all('div', class_='multiple-inline-element')
            
            for group in workshop_groups:
                objective = None
                duration = None
                description = None
                
                # Objective/Title
                obj_elem = group.find('div', class_=re.compile('field.*agenda.*objective'))
                if obj_elem:
                    objective = obj_elem.get_text(strip=True)
                    
                # Duration
                dur_elem = group.find('div', class_=re.compile('field.*agenda.*duration'))
                if dur_elem:
                    duration = dur_elem.get_text(strip=True)
                    
                # Description
                desc_elem = group.find('div', class_=re.compile('field.*agenda.*description'))
                if desc_elem:
                    description = desc_elem.get_text(separator='\n', strip=True)
                    
                if objective:
                    workshop = Workshop(
                        title=objective,
                        description=description,
                        duration=duration
                    )
                    speaker.workshops.append(workshop)
                    
        except Exception as e:
            logger.error(f"Error extracting workshops: {e}")
            
    def _extract_past_talks_from_fieldset(self, speaker: DetailedSpeaker, fieldset):
        """Extract past talks from fieldset"""
        try:
            # Find spoke-at div
            spoke_at = fieldset.find('div', class_='spoke-at')
            if spoke_at:
                talk_groups = spoke_at.find_all('div', class_='sh-field-group-element')
                
                for group in talk_groups:
                    title = None
                    event_name = None
                    location = None
                    date = None
                    
                    # Title
                    title_elem = group.find('div', class_='field-item-field_event_presentation_title')
                    if title_elem:
                        title = title_elem.get_text(strip=True)
                        
                    # Event name
                    event_elem = group.find('div', class_='field-item-field_event_name')
                    if event_elem:
                        event_name = event_elem.get_text(strip=True)
                        
                    # Location
                    loc_elem = group.find('div', class_='field-item-field_event_location')
                    if loc_elem:
                        location = loc_elem.get_text(strip=True)
                        
                    # Date
                    date_elem = group.find('div', class_='field-item-field_event_date_timestamp')
                    if date_elem:
                        date = date_elem.get_text(strip=True)
                        
                    if title:
                        talk = PastTalk(
                            title=title,
                            event_name=event_name,
                            location=location,
                            date=date
                        )
                        speaker.past_talks.append(talk)
                        
            # Update total talks
            speaker.total_talks = len(speaker.past_talks)
            
        except Exception as e:
            logger.error(f"Error extracting past talks: {e}")
            
    def _extract_testimonials_from_fieldset(self, speaker: DetailedSpeaker, fieldset):
        """Extract testimonials from fieldset"""
        try:
            # Extract count from legend
            legend = fieldset.find('legend')
            if legend:
                count_match = re.search(r'\((\d+)\)', legend.get_text())
                if count_match:
                    speaker.recommendations_count = int(count_match.group(1))
                    
            # Look for actual testimonial content
            test_items = fieldset.find_all('div', class_='testimonial-item')
            for item in test_items:
                content = item.get_text(strip=True)
                if content and len(content) > 10:
                    testimonial = Testimonial(content=content)
                    speaker.testimonials.append(testimonial)
                    
        except Exception as e:
            logger.error(f"Error extracting testimonials: {e}")
            
    def _extract_current_position_from_fieldset(self, speaker: DetailedSpeaker, fieldset):
        """Extract current position from fieldset"""
        try:
            # Job title
            job_elem = fieldset.find('div', class_='field-name-field-job')
            if job_elem:
                job_item = job_elem.find('div', class_='field-item')
                if job_item:
                    speaker.job_title = job_item.get_text(strip=True)
                    
            # Company
            company_elem = fieldset.find('div', class_='field-name-field-company')
            if company_elem:
                company_item = company_elem.find('div', class_='field-item')
                if company_item:
                    speaker.company = company_item.get_text(strip=True)
                    
        except Exception as e:
            logger.error(f"Error extracting current position: {e}")
            
    def _extract_from_javascript_settings(self, speaker: DetailedSpeaker):
        """Extract data from JavaScript settings if available"""
        try:
            # Look for Drupal.settings in script tags
            scripts = self.soup.find_all('script', type='text/javascript')
            for script in scripts:
                if script.string and 'Drupal.settings' in script.string:
                    # Try to extract recommendations count from modal data
                    rec_match = re.search(r'recommendations.*?(\d+).*?anonymous', script.string, re.IGNORECASE)
                    if rec_match and not speaker.recommendations_count:
                        speaker.recommendations_count = int(rec_match.group(1))
                        
        except Exception as e:
            logger.error(f"Error extracting from JavaScript: {e}")
            
    def _extract_from_structured_data(self, speaker: DetailedSpeaker):
        """Extract from schema.org structured data"""
        try:
            # Look for Person schema
            person_elem = self.soup.find(attrs={'itemtype': 'http://schema.org/Person'})
            if person_elem:
                # Extract any additional data from microdata
                pass
                
            # Meta tags
            gender_meta = self.soup.find('meta', {'property': 'profile:gender'})
            if gender_meta and gender_meta.get('content'):
                # Could store gender info if needed
                pass
                
        except Exception as e:
            logger.error(f"Error extracting structured data: {e}")
            
    def _make_absolute_url(self, url: str) -> str:
        """Convert relative URLs to absolute URLs"""
        if not url:
            return url
        if url.startswith('http'):
            return url
        elif url.startswith('//'):
            return f'https:{url}'
        elif url.startswith('/'):
            return f'https://speakerhub.com{url}'
        else:
            return f'https://speakerhub.com/{url}'