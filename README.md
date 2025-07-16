# Speaker Scrapers Collection

A comprehensive collection of web scrapers for various speaker bureau and directory websites. This project includes both Python and Node.js implementations with MongoDB integration for data storage.

## Overview

This repository contains scrapers for the following speaker platforms:

- **AllAmericanSpeakers** - Python-based scraper for allamericanspeakers.com
- **ASpeakers** - Python scraper for a-speakers.com
- **BigSpeak** - Multi-module Python scraper for bigspeak.com
- **EventRaptor** - Python scraper for eventraptor.com
- **FreeSpeakerBureau** - Enhanced MongoDB-integrated scraper
- **LeadingAuthorities** - Python scraper for leadingauthorities.com
- **Sessionize** - Multi-module scraper with category and speaker profile extraction
- **SpeakerHub** - Advanced scraper using Camoufox browser automation
- **TheSpeakerHandbook** - Node.js-based scraper

## Features

- **Multi-platform Support**: Scrapers for 9+ different speaker bureau websites
- **Language Flexibility**: Both Python and Node.js implementations
- **Database Integration**: MongoDB support for efficient data storage
- **Modular Design**: Many scrapers are split into modules for better organization
- **Resume Capability**: Several scrapers support resuming interrupted sessions
- **Data Export**: Multiple export formats including JSON and CSV

## Prerequisites

### Python Scrapers
- Python 3.8+
- MongoDB (for database-enabled scrapers)
- Virtual environment (recommended)

### Node.js Scrapers
- Node.js 14+
- npm or yarn

## Installation

### General Setup

1. Clone the repository:
```bash
git clone https://github.com/shafigill16/speaker_scrapers.git
cd speaker_scrapers
```

2. Each scraper has its own directory with specific requirements and setup instructions.

### Python Scrapers Setup

For Python-based scrapers, navigate to the specific scraper directory and set up a virtual environment:

```bash
cd [scraper_name]_scraper
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### Node.js Scrapers Setup

For Node.js-based scrapers (e.g., thespeakerhandbook_scraper):

```bash
cd thespeakerhandbook_scraper
npm install
```

## Configuration

Most scrapers require configuration before running:

1. **Environment Variables**: Copy `.env.example` to `.env` and fill in required values
2. **MongoDB Connection**: Update MongoDB connection strings in config files
3. **API Keys**: Some scrapers may require API keys or authentication tokens

## Usage

### Python Scrapers

Most Python scrapers can be run with:

```bash
python scraper.py
```

Some scrapers have additional scripts:
- `check_progress.py` - Monitor scraping progress
- `run_scraper.py` - Enhanced runner with error handling
- `resume_scraper.py` - Resume interrupted sessions

### Node.js Scrapers

```bash
node module1/scraper.js  # For module-based scrapers
node scraper.js          # For single-file scrapers
```

## Project Structure

```
speaker_scrapers/
├── allamericanspeakers_scraper/
│   ├── scraper.py
│   ├── requirements.txt
│   └── README.md
├── aspeakers_scraper/
│   ├── scraper.py
│   ├── requirements.txt
│   └── README.md
├── bigspeak_scraper/
│   ├── module_1/
│   └── module_2/
├── eventraptor_scraper/
├── freespeakerbureau_scraper/
├── leadingauthorities_scraper/
├── sessionize_scraper/
│   ├── module1_categories/
│   ├── module2_speakers/
│   └── module3_main/
├── speakerhub_camoufox/
│   ├── speaker_details_scraper/
│   └── pagination_scraper.py
└── thespeakerhandbook_scraper/
    ├── module1/
    ├── module2/
    └── shared/
```

## Data Output

Scrapers typically output data in the following formats:

- **JSON**: Structured data files
- **CSV**: Spreadsheet-compatible format
- **MongoDB**: Direct database storage
- **Log Files**: Detailed execution logs

Output locations vary by scraper but are typically in:
- `data/` directories
- `exports/` directories
- MongoDB collections

## Error Handling

Most scrapers include robust error handling:
- Automatic retry mechanisms
- Progress tracking and resume capability
- Detailed logging for debugging
- Graceful handling of rate limits

## Contributing

When adding new scrapers or improving existing ones:

1. Follow the existing project structure
2. Include a README.md in your scraper directory
3. Add requirements.txt for Python scrapers or package.json for Node.js
4. Implement proper error handling and logging
5. Test thoroughly before committing

## Security Notes

- Never commit `.env` files or credentials
- Use environment variables for sensitive data
- Follow robots.txt and terms of service for target websites
- Implement appropriate rate limiting

## Troubleshooting

Common issues and solutions:

1. **MongoDB Connection Errors**: Ensure MongoDB is running and connection string is correct
2. **Rate Limiting**: Adjust delay settings in scraper configuration
3. **Missing Dependencies**: Run `pip install -r requirements.txt` or `npm install`
4. **Permission Errors**: Check file permissions and user access rights

## License

This project is for educational and research purposes. Ensure compliance with target websites' terms of service and robots.txt files.

## Support

For issues or questions:
1. Check the individual scraper's README file
2. Review log files for error details
3. Ensure all dependencies are properly installed
4. Verify configuration settings