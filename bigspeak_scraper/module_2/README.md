# Module 2: BigSpeak Profile Detail Scraper

This module scrapes detailed information from individual speaker profile pages on BigSpeak.com.

## Features

- Scrapes detailed profiles for speakers found by Module 1
- Extracts comprehensive information:
  - Full biography/about text
  - Detailed speaking topics with descriptions
  - Books and publications
  - Awards and recognitions
  - Video content (YouTube/Vimeo links)
  - Social media profiles
  - Educational credentials
  - High-resolution images
- Stores in separate MongoDB collection (`speaker_profiles`)
- Resume capability - skips already scraped profiles
- Robust error handling and rate limiting

## Prerequisites

- Module 1 must be run first to populate the speakers collection
- MongoDB connection configured (same as Module 1)

## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. MongoDB configuration (already set in config.py):
```
mongodb://admin:dev2018@5.161.225.172:27017/?authSource=admin
Database: bigspeak_scraper
Collections: 
  - speakers (from module 1)
  - speaker_profiles (created by module 2)
```

## Usage

### Run the profile scraper:
```bash
python profile_scraper.py
```

By default, it scrapes 10 profiles for testing. To scrape all profiles:
```python
results = scraper.scrape_all_profiles(limit=None, skip_existing=True)
```

### View scraped profiles:
```bash
python utils.py
```

### Resume scraping:
The scraper automatically skips profiles that have already been scraped. Just run it again to continue.

### Scrape specific speakers:
```python
# Rescrape existing profiles
results = scraper.scrape_all_profiles(skip_existing=False, limit=50)
```

## Data Structure

Each speaker profile document contains:
```json
{
  "speaker_id": "unique-speaker-id",
  "name": "Speaker Name",
  "profile_url": "https://www.bigspeak.com/speakers/speaker-name/",
  "biography": "Full biography text...",
  "speaking_topics_detailed": [
    {
      "title": "Topic Title",
      "description": "Topic description..."
    }
  ],
  "books": ["Book Title 1", "Book Title 2"],
  "videos": [
    {
      "platform": "youtube",
      "video_id": "abc123",
      "watch_url": "https://youtube.com/watch?v=abc123"
    }
  ],
  "awards": ["Award description..."],
  "social_media": {
    "twitter": "https://twitter.com/speaker",
    "linkedin": "https://linkedin.com/in/speaker"
  },
  "credentials": ["PhD from Harvard", "CEO of Company"],
  "images": [
    {
      "url": "https://bigspeak.com/image.jpg",
      "alt": "Speaker Name"
    }
  ],
  "basic_info": {
    "description": "Short description from module 1",
    "fee_range": "$20,000 - $30,000",
    "topics": ["Leadership", "Innovation"]
  },
  "scraped_at": "2024-01-10T12:00:00Z",
  "first_scraped_at": "2024-01-10T12:00:00Z"
}
```

## Performance

- Processes approximately 20-30 profiles per minute (with 2-second delays)
- Full scraping of 2,000+ profiles takes approximately 2-3 hours
- MongoDB upsert ensures no duplicates

## Utilities

The `utils.py` script provides:
- `view_sample_profiles()` - View sample profile data
- `get_profile_stats()` - Statistics on scraped profiles
- `export_profiles_to_json()` - Export profiles to JSON
- `check_profile_quality()` - Check completeness of a specific profile

## Next Steps

After running both modules, you'll have:
1. Basic speaker information from Module 1
2. Detailed profile information from Module 2

This complete dataset can be used for:
- Building a speaker search engine
- Analyzing speaking topics and expertise
- Creating speaker recommendations
- Market analysis of speaking fees