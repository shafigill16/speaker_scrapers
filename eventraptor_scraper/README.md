# EventRaptor Speaker Scraper

A comprehensive Python web scraper for extracting speaker profiles from EventRaptor's speaker directory (https://app.eventraptor.com/speakers). This scraper collects detailed information about speakers including their events participation and stores it in MongoDB for easy querying and analysis.

## Features

- **Complete Speaker Profile Extraction**
  - Name, tagline, and credentials
  - Full biography text
  - Business areas and categories
  - Social media links (LinkedIn, Twitter, Facebook, Instagram, YouTube)
  - Profile images
  - Contact information (email if available)
  
- **Event Participation Tracking**
  - Events the speaker has participated in
  - Event names and IDs
  - Direct links to event pages

- **Presentation Information**
  - Speaker presentations/talks (when available)

- **Robust Scraping Features**
  - Pagination support
  - Duplicate detection and handling
  - Progress tracking with detailed statistics
  - Error handling and logging
  - Rate limiting to be respectful to the server
  - Resume capability (skips already scraped speakers)

## Data Structure

Each speaker document contains the following fields:

```json
{
  "speaker_id": "john-doe",
  "name": "John Doe",
  "tagline": "Motivational Speaker & Business Coach",
  "credentials": "MBA, PhD in Leadership",
  "biography": "Full biography text...",
  "business_areas": ["Business", "Leadership", "Marketing"],
  "profile_image": "https://app.eventraptor.com/storage/...",
  "social_media": {
    "linkedin": "https://linkedin.com/in/johndoe",
    "twitter": "https://twitter.com/johndoe",
    "facebook": "https://facebook.com/johndoe"
  },
  "email": "john@example.com",
  "events": [
    {
      "event_id": "123",
      "name": "Business Summit 2024",
      "url": "https://app.eventraptor.com/events/123"
    }
  ],
  "presentations": ["How to Build a Successful Business"],
  "url": "https://app.eventraptor.com/speaker-profiles/john-doe",
  "scraped_at": "2024-01-15T10:30:00Z"
}
```

## Requirements

- Python 3.6+
- MongoDB instance
- Required packages (see requirements.txt):
  - requests
  - beautifulsoup4
  - pymongo

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd eventraptor_scraper
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Configure MongoDB connection:
Edit the `MONGO_URI` in `scraper.py` if needed:
```python
MONGO_URI = "mongodb://admin:dev2018@5.161.225.172:27017/?authSource=admin"
```

## Usage

### Basic Usage

Run the scraper:
```bash
python scraper.py
```

Or make it executable:
```bash
chmod +x scraper.py
./scraper.py
```

### Using the Shell Script

For convenience, use the provided shell script:
```bash
./run_scraper.sh
```

The scraper will:
1. Connect to MongoDB
2. Create unique indexes on `speaker_id` and `url` to prevent duplicates
3. Fetch the total number of pages from the speaker directory
4. Collect all speaker profile URLs
5. Scrape each speaker profile with detailed information
6. Store the data in MongoDB with duplicate detection
7. Display progress and statistics throughout the process

## Configuration

The scraper can be configured by modifying these variables in `scraper.py`:

- `MONGO_URI`: MongoDB connection string
- `DB_NAME`: Database name (default: "eventraptor")
- `COLLECTION_NAME`: Collection name (default: "speakers")
- `BASE_URL`: Base URL for EventRaptor
- `SPEAKERS_URL`: Speakers listing URL

## Rate Limiting

The scraper implements polite rate limiting:
- 1 second delay between pagination requests
- 2 second delay between speaker profile requests
- 10 second break every 20 pages
- 30 second break every 50 speakers

## Monitoring Progress

The scraper provides detailed logging:
- Connection status
- Current page/speaker being processed
- Statistics (processed, new, updated, skipped, errors)
- Periodic progress updates

Example output:
```
2024-01-15 10:30:00,123 - INFO - Successfully connected to MongoDB.
2024-01-15 10:30:00,124 - INFO - Starting to scrape speakers from https://app.eventraptor.com
2024-01-15 10:30:00,500 - INFO - Found 5 pages to process
2024-01-15 10:30:00,501 - INFO - Collecting speaker URLs from all pages...
2024-01-15 10:30:01,234 - INFO - Fetching page 1/5
...
2024-01-15 10:30:15,789 - INFO - [1/50] Fetching: https://app.eventraptor.com/speaker-profiles/john-doe
2024-01-15 10:30:16,123 - INFO -   -> Saved 'John Doe'
2024-01-15 10:30:16,124 - INFO -      Business Areas: 3
2024-01-15 10:30:16,125 - INFO -      Events: 5
...
2024-01-15 10:45:00,000 - INFO - Final stats: Processed=45, New=30, Updated=15, Skipped=5, Errors=0
```

## Database Indexes

The scraper automatically creates indexes for optimal performance:
- Unique index on `speaker_id`
- Unique index on `url`

## Error Handling

- Connection failures are logged with details
- Failed speaker profiles are skipped without stopping the scraper
- Duplicate entries are handled gracefully
- Progress is preserved (can resume after interruption)
- Timeout handling (30 seconds per request)

## Performance

- Processes approximately 25-30 speakers per minute with delays
- Full scrape time depends on the number of speakers
- The scraper is designed to handle large datasets efficiently

## Troubleshooting

### MongoDB Connection Issues
- Verify MongoDB is running and accessible
- Check firewall settings
- Ensure credentials are correct

### Scraping Issues
- Check internet connection
- Verify EventRaptor website is accessible
- Review error logs for specific issues

### Rate Limiting
If you encounter rate limiting from the server:
- Increase delay times in the script
- Run the scraper during off-peak hours

## Contributing

Feel free to submit issues or pull requests for improvements.

## License

This project is for educational and research purposes only. Please respect EventRaptor's terms of service and robots.txt when using this scraper.

## Disclaimer

This scraper is provided as-is. The authors are not responsible for any misuse or violations of website terms of service. Always ensure you have permission to scrape a website and respect rate limits.