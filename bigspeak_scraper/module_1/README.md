# Module 1: BigSpeak Main Directory Scraper

This module scrapes the main speaker directory from BigSpeak.com and stores the data in MongoDB.

## Features

- Scrapes all speakers from the main directory (https://www.bigspeak.com/keynote-speakers/)
- Handles pagination (146+ pages)
- Extracts: name, description, topics, fee range, profile URL, image URL
- Stores data in MongoDB with upsert logic (no duplicates)
- Robust error handling and retry logic
- Polite scraping with delays

## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. MongoDB connection is pre-configured to:
```
mongodb://admin:dev2018@5.161.225.172:27017/?authSource=admin
Database: bigspeak_scraper
Collection: speakers
```

## Usage

### Run the scraper:
```bash
python scraper.py
```

By default, it scrapes 5 pages for testing. To scrape all pages, modify the `main()` function:
```python
results = scraper.scrape_all_pages(start_page=1, max_pages=None)  # Remove max_pages limit
```

### View scraped data:
```bash
python utils.py
```

### Resume from a specific page:
```python
results = scraper.scrape_all_pages(start_page=50)  # Start from page 50
```

## Data Structure

Each speaker document contains:
```json
{
  "speaker_id": "unique-speaker-id",
  "name": "Speaker Name",
  "description": "Professional description",
  "topics": [
    {
      "name": "Topic Name",
      "url": "topic-url"
    }
  ],
  "fee_range": "$10,000 - $20,000",
  "profile_url": "https://www.bigspeak.com/speakers/speaker-name/",
  "image_url": "speaker-image-url",
  "scraped_at": "2024-01-10T12:00:00Z",
  "first_scraped_at": "2024-01-10T12:00:00Z",
  "source": "main_directory"
}
```

## Logging

Logs are saved to `module_1/scraper.log` and also displayed in console.

## Next Steps

Module 2 will scrape individual speaker profile pages for detailed information.