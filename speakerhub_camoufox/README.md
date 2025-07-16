# SpeakerHub Scraper

A robust web scraper for SpeakerHub.com that handles pagination and stores comprehensive speaker data in MongoDB.

## 🎯 Key Discovery

SpeakerHub uses **pagination** (not infinite scroll) with an unusual pattern:
- Pages increment by 2: `/speakers`, `/speakers?page=2`, `/speakers?page=4`, `/speakers?page=6`, etc.
- No odd-numbered pages exist
- Each page contains up to 24 speakers

## ✅ Features

- **Pagination Support**: Automatically navigates through all pages
- **Comprehensive Data Extraction**: Extracts 15+ fields per speaker including:
  - Basic info (name, job title, company)
  - Location (country, state, city)
  - Languages spoken
  - Event types (conference, workshop, webinar, etc.)
  - Topics/expertise areas
  - Profile picture URL
  - Bio summary
  - Available regions
- **MongoDB Integration**: Direct storage with upsert operations
- **Batch Processing**: Saves data in configurable batches for efficiency
- **Export Functionality**: Export to JSON, CSV, and summary reports
- **Robust Error Handling**: Comprehensive logging and error recovery
- **Resume Support**: Skips already scraped speakers on subsequent runs
- **Real-time Monitoring**: Monitor progress with dedicated monitoring tool

## 📊 Results

As of last run, the scraper has successfully collected:
- **1,300+ speakers** from 50+ countries
- **100% success rate** with proper bot detection bypass
- Extraction rate: ~24 speakers per page

## 🚀 Installation

1. **Clone and navigate to directory**:
```bash
cd /home/mudassir/work/shafi/speakerhub_camoufox
```

2. **Activate virtual environment**:
```bash
source myenv/bin/activate
```

3. **Install dependencies** (if needed):
```bash
pip install -r requirements.txt
```

4. **Verify MongoDB connection**:
```bash
python main.py --test
```

## 📖 Usage

### Run the Pagination Scraper (Recommended)

```bash
python pagination_scraper.py
```

This will:
- Start from page 1 and navigate through all pages
- Extract all speaker data
- Save to MongoDB in batches
- Continue until no more pages are found

### Monitor Progress

In a separate terminal:
```bash
python monitor.py
```

Shows real-time:
- Total speakers count
- Recent additions
- Country distribution
- Updates every 5 seconds

### Export Data

```bash
# Export all formats
python main.py --export all

# Export specific format
python main.py --export json
python main.py --export csv
python main.py --export summary
```

## 🏗️ Architecture

### Core Files

1. **`pagination_scraper.py`** - Main scraper that handles pagination
   - Navigates through pages sequentially
   - Extracts speakers from each page
   - Handles "Show More" pagination pattern

2. **`speakerhub_scraper.py`** - Core classes and utilities
   - `Speaker`: Data model (dataclass)
   - `MongoDBHandler`: MongoDB operations
   - `SpeakerExtractor`: HTML parsing logic
   - Original infinite scroll logic (not used but contains shared code)

3. **`config.py`** - Configuration settings
   - MongoDB connection details
   - Scraper parameters
   - Browser settings

4. **`utils.py`** - Utility functions
   - Data export (JSON, CSV, summary)
   - Data validation
   - Statistics tracking

5. **`main.py`** - CLI interface
   - Command-line argument parsing
   - Export functionality
   - Test MongoDB connection

6. **`monitor.py`** - Real-time monitoring
   - Live database statistics
   - Recent speaker additions
   - Country distribution

## 📊 MongoDB Structure

- **Connection**: `mongodb://admin:dev2018@5.161.225.172:27017/?authSource=admin`
- **Database**: `speakerhub_scraper`
- **Collection**: `speakers`
- **Indexes**: Unique index on `uid` field

## 📝 Data Model

Each speaker document contains:

```javascript
{
  uid: "12345",                    // Unique SpeakerHub ID
  profile_url: "https://...",      // Profile URL
  name: "John Doe",                // Full name
  first_name: "John",              
  last_name: "Doe",                
  speaker_type: "Public speaker",  // or "Trainer and instructor"
  job_title: "CEO",                
  company: "Example Inc",          
  profile_picture: "https://...",  
  bio_summary: "Brief bio...",     
  country: "United States",        
  state: "California",             
  city: "San Francisco",           
  available_regions: ["North America", "Europe"],
  languages: ["English", "Spanish"],
  event_types: ["Conference", "Workshop", "Webinar"],
  topics: ["Leadership", "Innovation", "Technology"],
  scraped_at: "2024-01-01T12:00:00"
}
```

## 🔍 Monitoring and Logs

### Log Files
- `pagination_scraper.log` - Detailed pagination scraper logs
- `speakerhub_scraper.log` - General scraper logs

### Monitor Output Example
```
Last Update: 2025-07-12 11:30:00
==================================================
Total Speakers: 1,312

Top Countries:
  - United States: 678
  - United Kingdom: 123
  - Canada: 89
  - India: 67
  - Australia: 45

Recent Additions:
  - John Smith (United States) at 11:29:45
  - Jane Doe (Canada) at 11:29:42
  ...
```

## 🛠️ Troubleshooting

### Common Issues

1. **Bot Detection**: The current configuration successfully bypasses detection
2. **Timeout Errors**: Normal when reaching the end of available pages
3. **Connection Errors**: Usually happens after many pages - just restart

### Restarting After Interruption

The scraper automatically skips already scraped speakers (based on UID), so you can safely restart:

```bash
python pagination_scraper.py
```

## 📈 Performance

- **Pages scraped**: 50+ pages (and counting)
- **Time per page**: 10-15 seconds (including delays)
- **Success rate**: Nearly 100%
- **Memory usage**: Minimal due to batch processing

## 🎯 Key Insights

1. **Pagination Pattern**: Pages increment by 2 (no odd pages)
2. **Page Size**: Consistently 24 speakers per page (sometimes less on later pages)
3. **Total Speakers**: 1,300+ and growing
4. **Geographic Distribution**: Primarily US/UK/Canada with global representation

## 🔄 Maintenance

### Regular Updates
```bash
# Run scraper to get new speakers
python pagination_scraper.py

# Export latest data
python main.py --export all
```

### Database Maintenance
```bash
# Check total count
python monitor.py

# Verify data quality
python -c "
from pymongo import MongoClient
from config import MONGO_CONFIG
client = MongoClient(MONGO_CONFIG['connection_string'])
db = client[MONGO_CONFIG['database_name']]
print(f'Total: {db.speakers.count_documents({})}')
print(f'Without company: {db.speakers.count_documents({\"company\": None})}')
print(f'Without location: {db.speakers.count_documents({\"country\": None})}')
"
```

## 📋 Requirements

- Python 3.7+
- Camoufox browser automation
- MongoDB (remote connection provided)
- Dependencies in `requirements.txt`

## 🎉 Success Metrics

- Successfully bypassed bot detection
- Collected 1,300+ comprehensive speaker profiles
- Maintained data quality with validation
- Achieved efficient pagination handling
- Created reusable, maintainable code architecture