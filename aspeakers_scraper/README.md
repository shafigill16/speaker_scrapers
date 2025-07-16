# A-Speakers Scraper

A Python web scraper for extracting speaker information from the A-Speakers website (https://www.a-speakers.com).

## Features

This scraper extracts comprehensive speaker information including:

- **Basic Information**
  - Name
  - Job title/description
  - Location
  - Profile image URL
  
- **Professional Details**
  - Full biography
  - Topics/expertise areas
  - Keynotes/presentations
  - Videos
  - Social media links (Twitter, LinkedIn, Facebook, Instagram, YouTube)
  
- **Booking Information**
  - "Why book this speaker" bullet points
  - Fee range (if available)
  - Languages spoken
  
- **Customer Reviews**
  - Individual review ratings (1-5 stars)
  - Review text content
  - Reviewer's title/position
  - Reviewer's organization
  - Average rating across all reviews
  - Total number of reviews

## Requirements

- Python 3.x
- Required packages:
  - requests
  - beautifulsoup4
  - pymongo
  - lxml (optional, for faster parsing)

## Installation

1. Clone the repository
2. Install dependencies:
   ```bash
   pip install requests beautifulsoup4 pymongo
   ```

## Configuration

The scraper uses MongoDB for data storage. Update the following configuration in `scraper.py`:

```python
MONGO_URI = "mongodb://username:password@host:port/?authSource=admin"
PROXY = {
    "http": "your_proxy_url",
    "https": "your_proxy_url"
}
```

## Usage

Run the scraper:

```bash
python scraper.py
```

The scraper will:
1. Connect to MongoDB
2. Iterate through all speaker pages on A-Speakers
3. Extract detailed information for each speaker
4. Store the data in MongoDB (with upsert to avoid duplicates)
5. Display progress and statistics for each speaker

## Data Structure

Each speaker document in MongoDB contains:

```json
{
  "url": "speaker_profile_url",
  "name": "Speaker Name",
  "job_title": "Professional title/description",
  "location": "City, Country",
  "image_url": "profile_image_url",
  "description": "Short description",
  "full_bio": "Complete biography",
  "topics": ["Topic 1", "Topic 2", ...],
  "keynotes": [
    {
      "id": "keynote_id",
      "title": "Keynote Title",
      "description": "Full keynote description"
    }
  ],
  "videos": [
    {
      "title": "Video Title",
      "description": "Video description",
      "url": "video_url",
      "thumbnail": "thumbnail_url"
    }
  ],
  "why_book_points": [
    "Reason 1 to book this speaker",
    "Reason 2 to book this speaker",
    "Reason 3 to book this speaker"
  ],
  "reviews": [
    {
      "rating": 5,
      "review_text": "Review content",
      "author_title": "Reviewer's Title",
      "author_organization": "Company Name"
    }
  ],
  "average_rating": 5.0,
  "total_reviews": 8,
  "social_media": {
    "twitter": "url",
    "linkedin": "url",
    "facebook": "url"
  },
  "fee_range": "Price range if available",
  "languages": "Languages spoken",
  "scraped_at": "2024-01-15T10:30:00Z"
}
```

## Error Handling

The scraper includes:
- Retry logic for failed requests (3 attempts with 5-second delays)
- Timeout handling (60 seconds for listing pages, 30 seconds for speaker pages)
- Graceful error handling for missing data fields
- Progress tracking with detailed output

## Notes

- The scraper respects the website's structure and includes delays between requests
- Duplicate speakers are automatically handled with MongoDB's upsert functionality
- The scraper extracts only publicly available information