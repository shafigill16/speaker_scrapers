# TheSpeakerHandbook Scraper

A modular web scraper for extracting speaker information from thespeakerhandbook.com using their Typesense API and web scraping.

## Overview

This project consists of two main modules:

1. **Module 1**: Scrapes the speaker directory using the Typesense API
2. **Module 2**: Scrapes individual speaker profile pages for detailed information

## Prerequisites

- Node.js (v14 or higher)
- MongoDB instance (connection details configured in the project)
- Internet connection

## Installation

```bash
npm install
```

## Project Structure

```
thespeakerhandbook_scraper/
├── module1/
│   └── scraper.js      # Speaker directory scraper
├── module2/
│   └── scraper.js      # Speaker profile scraper
├── shared/
│   ├── db.js          # MongoDB connection and utilities
│   ├── config.js      # Configuration settings
│   └── utils.js       # Utility functions
├── package.json
└── README.md
```

## Configuration

All configuration is centralized in `shared/config.js`:

- **MongoDB**: Connection string is set to `mongodb://admin:dev2018@5.161.225.172:27017/?authSource=admin`
- **Database**: `thespeakerhandbook_scraper`
- **Collections**:
  - `speakers`: Basic speaker information from directory
  - `speaker_profiles`: Detailed profile information

## Usage

### 1. Scrape Speaker Directory

```bash
npm run scrape:directory
```

This will:
- Connect to the Typesense API
- Fetch all speakers in batches
- Store basic information in the `speakers` collection
- Fields collected include: name, profile URL, image, topics, languages, etc.

### 2. Scrape Speaker Profiles

```bash
npm run scrape:profiles
```

This will:
- Read unscraped speakers from the `speakers` collection
- Visit each speaker's profile page
- Extract detailed information (biography, talks, social links, etc.)
- Store in the `speaker_profiles` collection
- Update the `profile_scraped` flag in the `speakers` collection

### 3. Run Both Modules

```bash

siwhix
```

## MongoDB Collections

### speakers Collection

```javascript
{
  speaker_id: "41192",
  display_name: "Jaco van Gass",
  first_name: "Jaco",
  last_name: "van Gass",
  profile_url: "https://thespeakerhandbook.com/speaker/jaco-van-gass",
  image_url: "https://...",
  strapline: "Adventurer, Motivational Speaker...",
  topics: ["mindfulness-resilience", "motivation-and-inspiration"],
  home_country: "gb",
  languages: ["en_GB"],
  gender: "male",
  membership: "1",
  notability: ["adventurer-explorer", "olympic-athlete"],
  engagement_types: ["speaking"],
  event_type: ["inperson", "virtual"],
  scraped_at: Date,
  profile_scraped: false
}
```

### speaker_profiles Collection

```javascript
{
  speaker_id: "41192",
  display_name: "Jaco van Gass",
  profile_url: "https://...",
  page_title: "Jaco van Gass - Official Speaker Bio",
  meta_description: "View the official keynote speaker bio...",
  biography: "Full biography text...",
  job_title: "Adventurer",
  knows_about: "Motivation and Inspiration",
  nationality: "United Kingdom",
  talks: [{
    title: "Overcoming Adversity",
    description: "Talk description..."
  }],
  social_links: ["https://linkedin.com/..."],
  video_urls: ["https://youtube.com/..."],
  image_urls: ["https://..."],
  scrape_status: "success",
  scraped_at: Date
}
```

## Features

- **Resilient**: Implements retry logic with exponential backoff
- **Respectful**: Adds delays between requests to avoid overwhelming the server
- **Efficient**: Uses bulk operations for database writes
- **Modular**: Clean separation between directory and profile scraping
- **Error Handling**: Comprehensive error handling and logging

## Error Handling

- Failed API requests are retried up to 3 times
- Failed profile scrapes are marked with `scrape_status: "error"`
- Errors are logged but don't stop the entire process
- MongoDB connection errors will terminate the process

## Notes

- The Typesense API key is already configured in the project
- Profile scraping uses Cheerio to parse HTML
- The scraper respects rate limits with configurable delays
- All text content is cleaned and normalized

## Troubleshooting

1. **MongoDB Connection Issues**: Check if the MongoDB instance is accessible
2. **API Errors**: The Typesense API key might have changed
3. **Profile Scraping Errors**: Website structure might have changed
4. **Timeout Issues**: Increase timeout values in `shared/config.js`