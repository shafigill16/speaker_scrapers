# Free Speaker Bureau Scraper

A comprehensive web scraper for extracting speaker profiles from freespeakerbureau.com with MongoDB integration and proxy support.

## Features

- **MongoDB Integration**: Automatic upload to MongoDB with duplicate handling
- **Proxy Support**: Configurable proxy with rotation support
- **Environment-based Configuration**: Secure credential management via .env file
- **Comprehensive Data Extraction**:
  - Name, role, company, location (city, state, country)
  - Full biography and professional background
  - Speaking topics and specialties
  - Areas of expertise
  - Previous speaking engagements
  - Credentials and certifications
  - Awards and honors
  - Speaker since year
  - Contact information (website, social media)
  - Speaker OneSheet PDF URL
  - Profile image URL
  - Member level status
- **Advanced Features**:
  - Parallel scraping with configurable workers
  - Robust error handling and retry logic
  - Real-time progress tracking
  - Detailed logging and statistics
  - Automatic index creation for fast queries

## Installation

### 1. Clone the repository

```bash
git clone <repository-url>
cd freespeakerbureau_scraper
```

### 2. Create virtual environment

```bash
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure environment

Create a `.env` file in the project root with your credentials:

```env
# MongoDB Configuration
MONGODB_URI=mongodb://username:password@host:port/?authSource=admin
MONGODB_DATABASE=freespeakerbureau_scraper
MONGODB_COLLECTION=speakers_profiles2

# Proxy Configuration (optional)
PROXY_ROTATING_URL=http://username:password@proxy-host:port
PROXY_LIST=proxy1:port1,proxy2:port2

# Scraper Settings
BASE_URL=https://www.freespeakerbureau.com
MAX_WORKERS=5
BATCH_SIZE=10
REQUEST_TIMEOUT=30
RETRY_ATTEMPTS=3
DELAY_BETWEEN_REQUESTS=2
```

## Usage

### Command Line Interface

The main entry point is `run_scraper.py` which provides a CLI interface:

```bash
# Activate virtual environment
source venv/bin/activate

# Scrape all speakers
python run_scraper.py scrape

# Scrape with limit
python run_scraper.py scrape --limit 100

# Scrape with custom batch size
python run_scraper.py scrape --batch-size 20 --workers 3

# Check database status
python run_scraper.py check

# Export data to JSON
python run_scraper.py export

# Export with filters
python run_scraper.py export --filter-location California --filter-topic Leadership
```

### Command Options

**scrape** - Run the scraper
- `--limit`: Limit number of speakers to scrape (default: no limit)
- `--batch-size`: Number of profiles per batch (default: 10)
- `--workers`: Number of parallel workers (default: 5)
- `--export-sample`: Export N sample records after scraping

**check** - Check database status and statistics
- Shows total speakers, contact info availability, top locations, member levels

**export** - Export data from MongoDB to JSON
- `--filter-location`: Filter by location (e.g., California)
- `--filter-topic`: Filter by speaking topic

### Direct Script Usage

```python
# Run enhanced scraper directly
python enhanced_mongodb_scraper.py

# Check progress
python check_progress.py

# MongoDB utilities
python mongodb_utils.py
```

## Project Structure

```
freespeakerbureau_scraper/
├── .env                    # Environment variables (not in git)
├── .gitignore             # Git ignore rules
├── README.md              # This file
├── requirements.txt       # Python dependencies
├── config.py              # Configuration loader
├── enhanced_mongodb_scraper.py  # Main scraper logic
├── mongodb_utils.py       # MongoDB utility functions
├── run_scraper.py         # CLI interface
└── check_progress.py      # Progress checking utility
```

## Data Structure

Each speaker profile contains:

```json
{
  "profile_url": "https://www.freespeakerbureau.com/path/to/speaker",
  "name": "Speaker Name",
  "role": "Speaker / Presenter",
  "company": "Company Name",
  "location": "City, State",
  "city": "City",
  "state": "State",
  "country": "United States",
  "biography": "Full biography text...",
  "speaker_since": 2017,
  "areas_of_expertise": ["Topic 1", "Topic 2"],
  "specialties": ["Specialty 1", "Specialty 2"],
  "previous_engagements": "List of previous events...",
  "credentials": ["Credential 1", "Credential 2"],
  "awards": "Awards and recognition...",
  "contact_info": {
    "phone": "1234567890",
    "website": "https://speaker-website.com",
    "email": "speaker@email.com"
  },
  "social_media": {
    "linkedin": "https://linkedin.com/in/speaker",
    "twitter": "https://twitter.com/speaker"
  },
  "speaker_onesheet_url": "https://path/to/onesheet.pdf",
  "image_url": "https://path/to/image.jpg",
  "member_level": "gold",
  "fee_range": "$5,000 - $10,000",
  "languages": ["English", "Spanish"],
  "travels_from": "New York, NY",
  "scraped_at": "2025-01-16T12:00:00.000Z",
  "last_updated": "2025-01-16T12:00:00.000Z"
}
```

## Troubleshooting

### Proxy Connection Issues

If you encounter proxy errors:
1. Verify proxy credentials in `.env` file
2. Check if proxy service is active
3. Try using alternative proxy IPs in `PROXY_LIST`
4. Run without proxy by removing proxy settings from `.env`

### MongoDB Connection

If MongoDB connection fails:
1. Verify MongoDB URI format and credentials
2. Check network connectivity to MongoDB server
3. Ensure MongoDB is accepting connections from your IP
4. Verify database and collection names

### Website Blocking

If requests are being blocked:
1. Increase `DELAY_BETWEEN_REQUESTS` in `.env`
2. Reduce number of `MAX_WORKERS`
3. Use residential proxies instead of datacenter proxies
4. Check if website has updated anti-bot measures

### Common Errors

- **Rate Limiting (429)**: Increase delays between requests
- **Connection Timeout**: Increase `REQUEST_TIMEOUT` value
- **SSL Errors**: The scraper automatically disables SSL verification for proxies

## Output Files

- `enhanced_mongodb_scraper.log` - Detailed scraping logs
- `sample_export_[timestamp].json` - Sample of scraped data

## Best Practices

1. **Respect Rate Limits**: Use appropriate delays between requests
2. **Monitor Progress**: Use `check` command to monitor scraping progress
3. **Regular Exports**: Periodically export data for backup
4. **Error Handling**: Check logs for failed URLs and retry if needed

## Requirements

- Python 3.7+
- MongoDB instance (local or remote)
- Valid proxy credentials (optional but recommended)
- Sufficient disk space for logs

## Legal Notice

This scraper is for educational and research purposes only. Users must:
- Respect the website's Terms of Service
- Follow robots.txt guidelines
- Not use the data for commercial purposes without permission
- Consider reaching out to website owners for large-scale scraping

## Support

For issues or questions:
1. Check the logs in `enhanced_mongodb_scraper.log`
2. Verify your `.env` configuration
3. Ensure all dependencies are installed
4. Check MongoDB connectivity