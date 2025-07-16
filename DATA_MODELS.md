# Speaker Scrapers - Data Models Documentation

This document provides a comprehensive overview of the data models used by each scraper in the speaker_scrapers collection. Each scraper extracts specific fields based on the information available on their respective websites.

## Table of Contents
1. [AllAmericanSpeakers Scraper](#1-allamericanspeakers-scraper)
2. [ASpeakers Scraper](#2-aspeakers-scraper)
3. [BigSpeak Scraper](#3-bigspeak-scraper)
4. [EventRaptor Scraper](#4-eventraptor-scraper)
5. [FreeSpeakerBureau Scraper](#5-freespeakerbureau-scraper)
6. [LeadingAuthorities Scraper](#6-leadingauthorities-scraper)
7. [Sessionize Scraper](#7-sessionize-scraper)
8. [SpeakerHub Camoufox Scraper](#8-speakerhub-camoufox-scraper)
9. [TheSpeakerHandbook Scraper](#9-thespeakerhandbook-scraper)

---

## 1. AllAmericanSpeakers Scraper

**Website**: allamericanspeakers.com  
**Data Collection**: Speaker profiles, biographies, fees, and media

### Data Fields:
```json
{
  "speaker_id": "string - unique identifier",
  "url": "string - profile URL",
  "name": "string - speaker's full name",
  "job_title": "string - professional title",
  "biography": "string - detailed bio text",
  "location": "string - where speaker travels from",
  "fee_range": {
    "live_event": "string - e.g., '$5,000 - $10,000'",
    "virtual_event": "string - virtual event fee"
  },
  "categories": ["array of category strings"],
  "speaking_topics": [
    {
      "title": "string - topic name",
      "description": "string - topic details"
    }
  ],
  "images": [
    {
      "type": "string - 'profile' or 'gallery'",
      "url": "string - image URL",
      "alt": "string - alt text"
    }
  ],
  "videos": [
    {
      "url": "string - video URL",
      "type": "string - 'youtube', 'vimeo', 'other'",
      "title": "string - optional video title",
      "description": "string - optional description"
    }
  ],
  "social_media": {
    "facebook": "string - URL",
    "twitter": "string - URL",
    "linkedin": "string - URL",
    "instagram": "string - URL",
    "youtube": "string - URL"
  },
  "rating": {
    "average_rating": "integer - 1-5",
    "review_count": "integer"
  },
  "reviews": [
    {
      "rating": "integer",
      "text": "string - review content",
      "author": "string - reviewer name"
    }
  ],
  "scraped_at": "datetime - when data was collected"
}
```

---

## 2. ASpeakers Scraper

**Website**: a-speakers.com  
**Data Collection**: Speaker profiles with keynotes, videos, and reviews

### Data Fields:
```json
{
  "url": "string - profile URL",
  "name": "string - speaker name",
  "job_title": "string - professional title",
  "description": "string - brief description",
  "image_url": "string - profile image",
  "location": "string - speaker location",
  "why_book_points": ["array of selling points"],
  "full_bio": "string - complete biography",
  "topics": ["array of speaking topics"],
  "keynotes": [
    {
      "id": "string - keynote ID",
      "title": "string - keynote title",
      "description": "string - keynote details"
    }
  ],
  "videos": [
    {
      "title": "string",
      "description": "string",
      "url": "string - embed URL",
      "video_id": "string",
      "thumbnail": "string - thumbnail URL"
    }
  ],
  "reviews": [
    {
      "rating": "integer",
      "review_text": "string",
      "author_title": "string",
      "author_organization": "string"
    }
  ],
  "average_rating": "float",
  "total_reviews": "integer",
  "social_media": {
    "twitter": "string",
    "linkedin": "string",
    "facebook": "string",
    "instagram": "string",
    "youtube": "string"
  },
  "fee_range": "string",
  "languages": "string",
  "scraped_at": "datetime"
}
```

---

## 3. BigSpeak Scraper

**Website**: bigspeak.com  
**Data Collection**: Two-module approach - directory listing and detailed profiles

### Module 1 - Directory Data:
```json
{
  "speaker_id": "string - unique identifier",
  "name": "string",
  "profile_url": "string",
  "description": "string - brief description",
  "topics": [
    {
      "name": "string - topic name",
      "url": "string - topic URL"
    }
  ],
  "fee_range": "string - or 'Please Inquire'",
  "image_url": "string",
  "source": "string - 'main_directory'",
  "scraped_at": "datetime",
  "first_scraped_at": "datetime"
}
```

### Module 2 - Detailed Profile Data:
```json
{
  "All Module 1 fields plus...",
  "structured_data": "object - JSON-LD data",
  "location": {
    "travels_from": "string"
  },
  "languages": ["array of languages"],
  "why_choose": "string - why book this speaker",
  "keynote_topics": ["array of topics"],
  "speaking_programs": [
    {
      "title": "string",
      "short_description": "string",
      "full_description": "string",
      "key_takeaways": ["array of takeaways"]
    }
  ],
  "suggested_programs": [
    {
      "title": "string",
      "short_description": "string",
      "full_description": "string",
      "audience_takeaways": ["array of takeaways"]
    }
  ],
  "biography": "string - full bio",
  "videos": [
    {
      "platform": "string",
      "video_id": "string",
      "embed_url": "string",
      "watch_url": "string",
      "title": "string",
      "thumbnail": "string"
    }
  ],
  "testimonials": [
    {
      "quote": "string",
      "company": "string"
    }
  ],
  "books": [
    {
      "title": "string",
      "purchase_link": "string",
      "bestseller": "boolean"
    }
  ],
  "awards": ["array of award strings"],
  "social_media": "object - personal social links",
  "images": [
    {
      "url": "string",
      "type": "string",
      "alt": "string",
      "width": "string",
      "height": "string"
    }
  ],
  "additional_info": {
    "meta_description": "string",
    "post_id": "string",
    "downloads": ["array of downloadable resources"],
    "virtual_capable": "boolean"
  }
}
```

---

## 4. EventRaptor Scraper

**Website**: eventraptor.com  
**Data Collection**: Speaker profiles with event associations

### Data Fields:
```json
{
  "speaker_id": "string - unique identifier",
  "url": "string - profile URL",
  "name": "string",
  "tagline": "string - speaker tagline",
  "credentials": "string - qualifications",
  "business_areas": ["array of business areas"],
  "biography": "string",
  "presentations": ["array of presentation titles"],
  "profile_image": "string - image URL",
  "social_media": {
    "linkedin": "string",
    "twitter": "string",
    "facebook": "string",
    "instagram": "string",
    "youtube": "string"
  },
  "email": "string",
  "events": [
    {
      "name": "string - event name",
      "url": "string - event URL",
      "event_id": "string"
    }
  ],
  "scraped_at": "datetime"
}
```

---

## 5. FreeSpeakerBureau Scraper

**Website**: freespeakerbureau.com  
**Data Collection**: Comprehensive speaker profiles with contact information

### Data Fields:
```json
{
  "profile_url": "string",
  "name": "string",
  "meta_description": "string",
  "image_url": "string",
  "role": "string",
  "company": "string",
  "location": "string - full location",
  "city": "string",
  "state": "string",
  "country": "string",
  "biography": "string",
  "speaker_since": "integer/string - year",
  "areas_of_expertise": ["array of expertise areas"],
  "previous_engagements": "string",
  "credentials": ["array of credentials"],
  "awards": "string",
  "website": "string",
  "speaker_onesheet_url": "string - PDF URL",
  "social_media": {
    "linkedin": "string",
    "youtube": "string",
    "instagram": "string",
    "facebook": "string",
    "twitter": "string",
    "whatsapp": "string",
    "tiktok": "string",
    "pinterest": "string"
  },
  "specialties": ["array - same as speaking_topics"],
  "speaking_topics": ["array of topics"],
  "contact_info": {
    "phone": "string",
    "email": "string",
    "booking_url": "string",
    "scheduling_url": "string - Calendly, etc.",
    "whatsapp": "string"
  },
  "member_level": "string - 'premium', 'gold', 'silver', etc.",
  "has_phone_section": "boolean",
  "phone_source": "string",
  "email_source": "string",
  "scraped_at": "datetime",
  "last_updated": "datetime",
  "created_at": "datetime"
}
```

---

## 6. LeadingAuthorities Scraper

**Website**: leadingauthorities.com  
**Data Collection**: High-profile speakers with multimedia content

### Data Fields:
```json
{
  "speaker_page_url": "string",
  "name": "string",
  "job_title": "string",
  "description": "string",
  "speaker_image_url": "string",
  "speaker_website": "string",
  "social_media": {
    "twitter": "string",
    "linkedin": "string",
    "facebook": "string",
    "youtube": "string",
    "podcasts": "string"
  },
  "download_profile_link": "string - PDF URL",
  "topics": [
    {
      "title": "string",
      "description": "string"
    }
  ],
  "download_topics_link": "string",
  "videos": [
    {
      "title": "string",
      "video_id": "string",
      "video_page_url": "string",
      "thumbnail_url": "string"
    }
  ],
  "speaker_fees": {
    "Location Type": "Fee"
  },
  "books_and_publications": [
    {
      "title": "string",
      "url": "string",
      "image_url": "string"
    }
  ],
  "topics_and_types": [
    {
      "name": "string",
      "url": "string"
    }
  ],
  "recent_news": [
    {
      "title": "string",
      "url": "string"
    }
  ],
  "client_testimonials": [
    {
      "quote": "string",
      "author": "string"
    }
  ]
}
```

---

## 7. Sessionize Scraper

**Website**: sessionize.com  
**Data Collection**: Tech speakers with session history (Module 3 final output)

### Data Fields:
```json
{
  "username": "string - unique identifier",
  "basic_info": {
    "name": "string",
    "username": "string",
    "url": "string",
    "tagline": "string",
    "bio": "string",
    "location": "string",
    "profile_picture": "string"
  },
  "professional_info": {
    "expertise_areas": ["array of expertise"],
    "topics": ["array of topics"],
    "social_links": {
      "twitter": {
        "url": "string",
        "handle": "string"
      },
      "linkedin": {
        "url": "string",
        "label": "string"
      },
      "github": {
        "url": "string",
        "handle": "string"
      },
      "other": [
        {
          "url": "string",
          "label": "string"
        }
      ]
    }
  },
  "speaking_history": {
    "sessions": [
      {
        "title": "string",
        "url": "string",
        "summary": "string"
      }
    ],
    "events": [
      {
        "name": "string",
        "url": "string",
        "date": "string",
        "location": "string",
        "sessions": "string",
        "is_sessionize_event": "boolean"
      }
    ]
  },
  "metadata": {
    "scraped_at": "string - ISO format",
    "source_categories": ["array of categories"],
    "run_id": "string"
  }
}
```

---

## 8. SpeakerHub Camoufox Scraper

**Website**: speakerhub.com  
**Data Collection**: Two-stage scraping - basic listing and detailed profiles

### Basic Speaker Data:
```json
{
  "uid": "string - unique identifier",
  "profile_url": "string",
  "name": "string",
  "first_name": "string",
  "last_name": "string",
  "speaker_type": "string",
  "job_title": "string",
  "company": "string",
  "profile_picture": "string",
  "bio_summary": "string",
  "country": "string",
  "state": "string",
  "city": "string",
  "available_regions": ["array of regions"],
  "languages": ["array of languages"],
  "event_types": ["array of event types"],
  "topics": ["array of topics"],
  "scraped_at": "datetime"
}
```

### Detailed Speaker Profile:
```json
{
  "All basic fields plus...",
  "professional_title": "string - PCC, PhD, etc.",
  "pronouns": "string",
  "state_province": "string",
  "timezone": "string",
  "website": "string",
  "linkedin_url": "string",
  "twitter_url": "string",
  "facebook_url": "string",
  "instagram_url": "string",
  "youtube_url": "string",
  "topic_categories": ["array of categories"],
  "full_bio": "string",
  "why_choose_me": "string",
  "fee_range": "string",
  "speaker_fees": [
    {
      "event_type": "string",
      "event_description": "string",
      "fee": "string"
    }
  ],
  "years_experience": "integer",
  "total_talks": "integer",
  "past_talks": [
    {
      "title": "string",
      "event_name": "string",
      "location": "string",
      "date": "string",
      "description": "string"
    }
  ],
  "education": [
    {
      "degree": "string",
      "institution": "string",
      "year": "string"
    }
  ],
  "certifications": ["array of certifications"],
  "awards": ["array of awards"],
  "presentations": [
    {
      "title": "string",
      "description": "string"
    }
  ],
  "workshops": [
    {
      "title": "string",
      "description": "string",
      "duration": "string"
    }
  ],
  "publications": [
    {
      "title": "string",
      "type": "string - 'book' or 'article'",
      "publication": "string",
      "date": "string",
      "url": "string"
    }
  ],
  "videos": ["array of video URLs"],
  "testimonials": [
    {
      "content": "string",
      "author_name": "string",
      "author_title": "string",
      "author_company": "string",
      "date": "string"
    }
  ],
  "recommendations_count": "integer",
  "rating": "float",
  "affiliations": ["array of affiliations"],
  "competencies": {
    "core": ["array of core competencies"],
    "secondary": ["array of secondary competencies"]
  },
  "profile_picture_url": "string",
  "banner_image_url": "string",
  "press_kit_url": "string",
  "last_updated": "datetime",
  "scraping_status": "string",
  "error_message": "string"
}
```

---

## 9. TheSpeakerHandbook Scraper

**Website**: thespeakerhandbook.com  
**Data Collection**: Limited to testimonials data

### Data Fields:
```json
{
  "testimonials": [
    {
      "content": "string - testimonial text",
      "person": "string - author name",
      "position": "string - author position",
      "organization": "string - author organization"
    }
  ]
}
```

---

## Common Fields Across Scrapers

Most scrapers share these common field categories:

### Core Information
- **name**: Speaker's full name
- **profile_url**: Direct link to speaker's profile
- **biography/bio**: Detailed speaker biography
- **job_title/role**: Professional title or role
- **location**: Geographic location or travel base

### Professional Details
- **topics/speaking_topics**: Areas of expertise
- **fee_range**: Speaking fee information
- **languages**: Languages spoken
- **experience/years_experience**: Professional experience

### Media & Content
- **videos**: Video content URLs and metadata
- **images/profile_image**: Photos and headshots
- **testimonials/reviews**: Client feedback
- **publications/books**: Written works

### Contact & Social
- **social_media**: Links to social platforms
- **email**: Contact email
- **website**: Personal/professional website

### Metadata
- **scraped_at**: Timestamp of data collection
- **speaker_id/uid**: Unique identifier
- **last_updated**: Last modification time

---

## Database Storage

All scrapers store data in MongoDB with appropriate indexes for:
- Unique identifiers (speaker_id, uid, username)
- Profile URLs
- Names (for search functionality)
- Scraped timestamps (for data freshness)

## Data Quality Notes

1. **Field Availability**: Not all fields are available for every speaker
2. **Data Formats**: Dates, fees, and ratings may vary in format across scrapers
3. **Social Media**: Platform availability varies by website
4. **Media Content**: Video and image URLs may require periodic validation
5. **Text Fields**: Biography and description lengths vary significantly

## Usage Recommendations

- Use unique identifiers for deduplication
- Implement data validation for critical fields
- Consider field mapping for cross-platform aggregation
- Regular updates recommended for time-sensitive data (fees, availability)
- Store raw HTML/JSON for future reprocessing if needed