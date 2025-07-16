#!/usr/bin/env python3
"""
Data models for detailed speaker information
"""

from dataclasses import dataclass, field
from typing import List, Optional, Dict
from datetime import datetime


@dataclass
class SpeakerFee:
    """Speaker fee information for different event types"""
    event_type: str
    event_description: str
    fee: Optional[str] = None
    
    
@dataclass
class PastTalk:
    """Past speaking engagement"""
    title: str
    event_name: Optional[str] = None
    location: Optional[str] = None
    date: Optional[str] = None
    description: Optional[str] = None
    
    
@dataclass
class Education:
    """Educational qualification"""
    degree: str
    institution: Optional[str] = None
    year: Optional[str] = None
    
    
@dataclass
class Publication:
    """Book or article publication"""
    title: str
    type: str  # 'book' or 'article'
    publication: Optional[str] = None
    date: Optional[str] = None
    url: Optional[str] = None
    
    
@dataclass
class Presentation:
    """Available presentation topic"""
    title: str
    description: Optional[str] = None
    
    
@dataclass
class Workshop:
    """Workshop offering"""
    title: str
    description: Optional[str] = None
    duration: Optional[str] = None
    

@dataclass
class Testimonial:
    """Speaker testimonial/recommendation"""
    content: str
    author_name: Optional[str] = None
    author_title: Optional[str] = None
    author_company: Optional[str] = None
    date: Optional[str] = None


@dataclass
class DetailedSpeaker:
    """Complete speaker profile with all detailed information"""
    # Basic information (from part 1)
    uid: str
    profile_url: str
    name: str
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    
    # Professional information
    professional_title: Optional[str] = None  # e.g., PCC, PhD
    job_title: Optional[str] = None
    company: Optional[str] = None
    pronouns: Optional[str] = None
    
    # Location
    country: Optional[str] = None
    state_province: Optional[str] = None
    city: Optional[str] = None
    timezone: Optional[str] = None
    
    # Contact and social
    website: Optional[str] = None
    linkedin_url: Optional[str] = None
    twitter_url: Optional[str] = None
    facebook_url: Optional[str] = None
    instagram_url: Optional[str] = None
    youtube_url: Optional[str] = None
    
    # Speaking capabilities
    languages: List[str] = field(default_factory=list)
    event_types: List[str] = field(default_factory=list)
    topics: List[str] = field(default_factory=list)
    topic_categories: List[str] = field(default_factory=list)
    
    # Bio and description
    bio_summary: Optional[str] = None
    full_bio: Optional[str] = None
    why_choose_me: Optional[str] = None
    
    # Speaking fees
    fee_range: Optional[str] = None  # e.g., "$5,000 - $10,000"
    speaker_fees: List[SpeakerFee] = field(default_factory=list)
    
    # Experience and credentials
    years_experience: Optional[int] = None
    total_talks: Optional[int] = None
    past_talks: List[PastTalk] = field(default_factory=list)
    education: List[Education] = field(default_factory=list)
    certifications: List[str] = field(default_factory=list)
    awards: List[str] = field(default_factory=list)
    
    # Content and materials
    presentations: List[Presentation] = field(default_factory=list)
    workshops: List[Workshop] = field(default_factory=list)
    publications: List[Publication] = field(default_factory=list)
    videos: List[str] = field(default_factory=list)  # Video URLs
    
    # Testimonials and ratings
    testimonials: List[Testimonial] = field(default_factory=list)
    recommendations_count: int = 0
    rating: Optional[float] = None
    
    # Professional affiliations
    affiliations: List[str] = field(default_factory=list)
    competencies: Dict[str, List[str]] = field(default_factory=dict)  # e.g., {'core': [...], 'secondary': [...]}
    
    # Media and resources
    profile_picture_url: Optional[str] = None
    banner_image_url: Optional[str] = None
    press_kit_url: Optional[str] = None
    
    # Metadata
    scraped_at: datetime = field(default_factory=datetime.now)
    last_updated: Optional[datetime] = None
    scraping_status: str = 'pending'  # pending, in_progress, completed, failed
    error_message: Optional[str] = None
    
    def to_dict(self) -> dict:
        """Convert to dictionary for MongoDB storage"""
        data = {
            'uid': self.uid,
            'profile_url': self.profile_url,
            'name': self.name,
            'first_name': self.first_name,
            'last_name': self.last_name,
            'professional_title': self.professional_title,
            'job_title': self.job_title,
            'company': self.company,
            'pronouns': self.pronouns,
            'country': self.country,
            'state_province': self.state_province,
            'city': self.city,
            'timezone': self.timezone,
            'website': self.website,
            'linkedin_url': self.linkedin_url,
            'twitter_url': self.twitter_url,
            'facebook_url': self.facebook_url,
            'instagram_url': self.instagram_url,
            'youtube_url': self.youtube_url,
            'languages': self.languages,
            'event_types': self.event_types,
            'topics': self.topics,
            'topic_categories': self.topic_categories,
            'bio_summary': self.bio_summary,
            'full_bio': self.full_bio,
            'why_choose_me': self.why_choose_me,
            'fee_range': self.fee_range,
            'speaker_fees': [
                {
                    'event_type': fee.event_type,
                    'event_description': fee.event_description,
                    'fee': fee.fee
                } for fee in self.speaker_fees
            ],
            'years_experience': self.years_experience,
            'total_talks': self.total_talks,
            'past_talks': [
                {
                    'title': talk.title,
                    'event_name': talk.event_name,
                    'location': talk.location,
                    'date': talk.date,
                    'description': talk.description
                } for talk in self.past_talks
            ],
            'education': [
                {
                    'degree': edu.degree,
                    'institution': edu.institution,
                    'year': edu.year
                } for edu in self.education
            ],
            'certifications': self.certifications,
            'awards': self.awards,
            'presentations': [
                {
                    'title': pres.title,
                    'description': pres.description
                } for pres in self.presentations
            ],
            'workshops': [
                {
                    'title': ws.title,
                    'description': ws.description,
                    'duration': ws.duration
                } for ws in self.workshops
            ],
            'publications': [
                {
                    'title': pub.title,
                    'type': pub.type,
                    'publication': pub.publication,
                    'date': pub.date,
                    'url': pub.url
                } for pub in self.publications
            ],
            'videos': self.videos,
            'testimonials': [
                {
                    'content': test.content,
                    'author_name': test.author_name,
                    'author_title': test.author_title,
                    'author_company': test.author_company,
                    'date': test.date
                } for test in self.testimonials
            ],
            'recommendations_count': self.recommendations_count,
            'rating': self.rating,
            'affiliations': self.affiliations,
            'competencies': self.competencies,
            'profile_picture_url': self.profile_picture_url,
            'banner_image_url': self.banner_image_url,
            'press_kit_url': self.press_kit_url,
            'scraped_at': self.scraped_at,
            'last_updated': self.last_updated,
            'scraping_status': self.scraping_status,
            'error_message': self.error_message
        }
        
        # Remove None values to save space
        return {k: v for k, v in data.items() if v is not None}