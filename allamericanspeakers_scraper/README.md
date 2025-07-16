# All American Speakers Scraper

A Python web scraper for extracting speaker information from the All American Speakers website (https://www.allamericanspeakers.com).

## Features

This scraper extracts comprehensive speaker information including:

- **Basic Information**
  - Name
  - Speaker ID
  - Job title/byline
  - Location (cleaned to remove FAQ text)
  - Profile URL
  
- **Professional Details**
  - Full biography
  - Categories (e.g., Business, Motivational, Leadership)
  - Speaking topics (structured with title and description)
  - Speaking fees (both live and virtual events)
  
- **Media Content**
  - Profile and gallery images
  - Videos with titles, URLs, and descriptions
  - Social media links
  
- **Reviews & Ratings**
  - Aggregate rating (average and count)
  - Individual reviews with:
    - Rating (1-5 stars)
    - Review text
    - Author/organization name

## Requirements

- Python 3.x
- Required packages:
  - requests
  - beautifulsoup4
  - pymongo
  - lxml (optional, for faster XML parsing)

## Installation

1. Clone the repository
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
   
   Or manually:
   ```bash
   pip install requests beautifulsoup4 pymongo lxml
   ```

## Configuration

The scraper uses MongoDB for data storage. Update the following configuration in `scraper.py`:

```python
MONGO_URI = "mongodb://username:password@host:port/?authSource=admin"
DB_NAME = "allamericanspeakers"
COLLECTION_NAME = "speakers"
```

## Usage

Run the scraper:

```bash
python scraper.py
```

The scraper will:
1. Connect to MongoDB
2. Create unique indexes on `speaker_id` and `url` to prevent duplicates
3. Fetch speaker URLs from the sitemap (16,522 speakers found)
4. Extract detailed information for each speaker including reviews
5. Store the data in MongoDB with smart update logic
6. Display detailed progress and statistics for each speaker

## Data Structure

Each speaker document in MongoDB contains:

```json
{
  "url": "https://www.allamericanspeakers.com/speakers/389198/%22Science-Bob%22-Pflugfelder",
  "speaker_id": "389198",
  "name": "\"Science Bob\" Pflugfelder",
  "job_title": "Science Communicator, Teacher, Author & Television Personality",
  "location": "San Francisco, CA, USA",
  "biography": "Full biography text...",
  "categories": ["Education", "Science", "STEM"],
  "speaking_topics": [
    {
      "title": "STEM (STEAM) Education",
      "description": "How does the STEAM movement help young students..."
    }
  ],
  "fee_range": {
    "live_event": "$10,000 - $20,000",
    "virtual_event": "$5,000 - $10,000"
  },
  "rating": {
    "average_rating": 5,
    "review_count": 2
  },
  "reviews": [
    {
      "rating": 5,
      "text": "The presentation was perfect for our audience...",
      "author": "Foth"
    }
  ],
  "images": [
    {
      "type": "profile",
      "url": "https://www.allamericanspeakers.com/images/...",
      "alt": "Speaker Name"
    }
  ],
  "videos": [
    {
      "url": "https://www.youtube.com/watch?v=...",
      "type": "youtube",
      "title": "Amazing Experiments with Science Bob",
      "description": "Science Bob shares demonstrations..."
    }
  ],
  "social_media": {
    "twitter": "https://twitter.com/...",
    "linkedin": "https://linkedin.com/..."
  },
  "scraped_at": "2025-07-15T17:30:00Z"
}
```

## Important Notes

- The scraper includes delays between requests to be respectful to the server
- It takes a 30-second break every 50 speakers to avoid overwhelming the server
- **Duplicate handling**: The scraper uses MongoDB unique indexes on `speaker_id` and `url` to prevent duplicates
- **Smart updates**: When re-running, the scraper only updates speakers missing critical fields (videos, reviews, name, location)
- **Location cleaning**: Automatically removes FAQ text from location data
- **Reviews extraction**: Fetches reviews from a separate AJAX endpoint for each speaker
- If the website structure changes, the scraper might need updates
- The search functionality is limited as the website uses AJAX for dynamic content loading

## Error Handling

The scraper includes:
- Timeout handling (30 seconds per request)
- Graceful error handling for missing data fields
- Logging for debugging and progress tracking
- MongoDB connection error handling

## How It Works

The scraper uses the sitemap to discover speaker URLs:

1. **Sitemap Parsing**: Fetches and parses the main sitemap at `/sitemap.xml`
2. **URL Extraction**: Filters URLs containing `/speakers/` to identify speaker profile pages
3. **Profile Scraping**: Visits each speaker page and extracts comprehensive information
4. **Review Extraction**: Makes additional requests to `/float_box/reviews.php` for each speaker
5. **Data Storage**: Saves extracted data to MongoDB with duplicate detection
6. **Smart Updates**: When re-running, only updates speakers missing critical fields

The scraper found 16,522 speakers in the sitemap.

## Performance

- Processes approximately 30 speakers per minute with delays
- Takes a 30-second break every 50 speakers
- Full scrape of 16,522 speakers takes approximately 10-12 hours
- Provides detailed statistics: new, updated, skipped, and error counts

## Limitations

- Some content might be loaded dynamically via JavaScript and may not be captured
- The scraper respects the website's structure and includes appropriate delays
- Reviews are only available for speakers who have them (not all speakers have reviews)