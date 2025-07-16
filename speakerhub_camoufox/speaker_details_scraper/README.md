# SpeakerHub Details Scraper (Part 2)

This is the second part of the SpeakerHub scraping project. While Part 1 collected basic speaker information from the main listings, Part 2 visits each individual speaker's profile page to extract comprehensive detailed information.

## 🎯 Features

- **Comprehensive Data Extraction**: Extracts 40+ fields per speaker including:
  - Professional information (title, company, pronouns)
  - Complete bio and descriptions
  - All speaking topics and expertise areas
  - Event types and speaking fees
  - Past speaking engagements
  - Education and certifications
  - Publications and workshops
  - Testimonials and recommendations
  - Social media links
  - And much more...

- **Robust Resume Capability**: 
  - Automatically saves progress every 5 speakers
  - Can resume from exact stopping point after interruption
  - Tracks session state in MongoDB
  - Start fresh or continue previous sessions

- **Smart Error Handling**:
  - Retry logic for failed pages
  - Bot detection avoidance
  - Graceful error recovery
  - Failed speakers can be retried later

- **Monitoring & Export**:
  - Real-time progress monitoring
  - Export to JSON and CSV formats
  - Detailed statistics and summaries

## 📋 Prerequisites

- Python 3.7+
- MongoDB connection (uses same database as Part 1)
- Speakers data from Part 1 must be available

## 🚀 Quick Start

1. **Test MongoDB Connection**:
```bash
python main.py --test
```

2. **Start Scraping** (with auto-resume):
```bash
python main.py --scrape
```

3. **Monitor Progress** (in another terminal):
```bash
python main.py --monitor
```

4. **Export Data**:
```bash
python main.py --export
```

## 📖 Usage

### Basic Commands

```bash
# Test database connection
python main.py --test

# Start scraping with resume capability
python main.py --scrape

# Start fresh (ignore previous state)
python main.py --scrape --no-resume

# Scrape limited number of speakers
python main.py --scrape --limit 50

# Monitor progress in real-time
python main.py --monitor

# Show statistics
python main.py --stats

# Export all data
python main.py --export

# Export including failed speakers
python main.py --export --include-failed

# Retry failed speakers
python main.py --retry-failed
```

### Advanced Usage

```bash
# Use custom session ID
python main.py --scrape --session-id "my-session-2024"

# Retry only first 10 failed speakers
python main.py --retry-failed --limit 10
```

## 🗄️ Data Model

The scraper extracts the following information for each speaker:

### Basic Information
- Name, professional title, pronouns
- Job title and company
- Location (country, state/province, city, timezone)

### Professional Details
- Languages spoken
- Event types offered
- Speaking topics (40+ categories)
- Fee ranges and specific event fees
- Years of experience

### Content
- Full biography
- "Why choose me" statement
- Past talks with dates and locations
- Available presentations
- Workshops offered
- Publications (books and articles)

### Credentials
- Education history
- Professional certifications
- Awards and recognition
- Professional affiliations
- Competencies (core, secondary, enabling)

### Social & Media
- Website and social media links
- Profile and banner images
- Video content
- Press kit materials

### Engagement
- Testimonials and recommendations
- Recommendation count
- Rating (if available)

## 🔄 Resume Capability

The scraper implements sophisticated resume functionality:

1. **Automatic State Saving**: Progress is saved every 5 speakers
2. **Session Tracking**: Each run has a unique session ID
3. **Graceful Recovery**: Can resume after crashes or interruptions
4. **Fresh Starts**: Option to ignore previous state and start over

### How Resume Works

- When you start scraping, it checks for existing session state
- If found, it resumes from the last processed speaker
- If not found or using `--no-resume`, it starts fresh
- Progress is saved to the `scraping_resume_state` collection

## 🛠️ Configuration

Edit `config.py` to adjust:

- **Batch size**: Number of speakers before saving (default: 10)
- **Delays**: Min/max delays between requests
- **Timeouts**: Page load and wait timeouts
- **Retry settings**: Max retries and delays
- **Break settings**: Long breaks after X speakers

## 📊 Monitoring

Real-time monitoring shows:
- Total speakers and completion percentage
- Current progress (completed/failed/pending)
- Recent completions with timestamps
- Live updates every 5 seconds

## 📤 Export Formats

### JSON Export
- Complete data with all nested structures
- UTF-8 encoded
- Pretty-printed for readability

### CSV Export
- Flattened structure for spreadsheet compatibility
- Lists converted to comma-separated strings
- Counts for nested items (e.g., total_talks, total_topics)

### Summary Report
- Overall statistics
- Country distribution (top 20)
- Topic distribution (top 20)
- Fee range distribution

## 🚨 Error Handling

The scraper handles various error scenarios:

- **Network errors**: Automatic retry with exponential backoff
- **Bot detection**: Extended delays and retry logic
- **Parse errors**: Mark as failed and continue
- **MongoDB errors**: Graceful degradation

Failed speakers are marked with error messages and can be retried later using `--retry-failed`.

## 📈 Performance

- Processes ~10-20 speakers per minute
- Human-like delays between requests
- Long breaks every 50 speakers
- Respects server resources

## 🔍 Troubleshooting

### Common Issues

1. **"No speakers to scrape"**
   - Ensure Part 1 has completed successfully
   - Check MongoDB connection
   - Verify collection names in config

2. **High failure rate**
   - May indicate bot detection
   - Try increasing delays in config
   - Check for changes in HTML structure

3. **Resume not working**
   - Check `scraping_resume_state` collection
   - Ensure session ID is consistent
   - Try with `--no-resume` flag

### Debug Mode

For detailed debugging, edit the logging level in `scraper.py`:
```python
logging.basicConfig(level=logging.DEBUG)
```

## 📝 Logs

Logs are saved to `speaker_details_scraper.log` with:
- Timestamp for each action
- Success/failure status
- Error messages and stack traces
- Performance metrics

## 🎯 Best Practices

1. **Run in background**: Use `screen` or `tmux` for long runs
2. **Monitor regularly**: Check progress to catch issues early
3. **Export periodically**: Don't wait for 100% completion
4. **Retry failures**: Run `--retry-failed` after main scrape
5. **Verify data**: Spot-check exported data for quality

## 🔒 Data Privacy

- Respects robots.txt and rate limits
- No personal data beyond publicly available info
- Implements ethical scraping practices
- Data stored securely in MongoDB