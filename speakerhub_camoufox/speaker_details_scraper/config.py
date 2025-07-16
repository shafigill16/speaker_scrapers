#!/usr/bin/env python3
"""
Configuration for speaker details scraper
"""

# MongoDB Configuration for Part 2
# Using the same connection as Part 1
MONGO_CONFIG = {
    'connection_string': 'mongodb://admin:dev2018@5.161.225.172:27017/?authSource=admin',
    'database_name': 'speakerhub_scraper',
    'source_collection': 'speakers',  # Part 1 collection with basic speaker data
    'details_collection': 'speaker_details',  # New collection for detailed data
    'resume_collection': 'scraping_resume_state'  # Collection to track resume state
}

# Scraper Configuration
SCRAPER_CONFIG = {
    # Batch processing
    'batch_size': 10,  # Number of speakers to process before saving
    'concurrent_limit': 1,  # Number of concurrent pages (1 for safety)
    
    # Delays (in seconds)
    'min_delay': 3,
    'max_delay': 7,
    'error_delay': 30,
    'long_break_after': 50,  # Take a long break after X speakers
    'long_break_duration': 60,
    
    # Timeouts (in milliseconds)
    'page_timeout': 60000,
    'wait_after_load': 3000,
    
    # Retry settings
    'max_retries': 3,
    'retry_delay': 10,
    
    # Resume settings
    'save_state_every': 5,  # Save resume state every X speakers
    'max_errors_before_stop': 10,  # Stop if too many consecutive errors
}

# Browser Configuration
BROWSER_CONFIG = {
    'headless': True,
    'user_agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'viewport': {'width': 1920, 'height': 1080},
    'extra_headers': {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1'
    }
}

# Logging Configuration
LOG_CONFIG = {
    'log_file': 'speaker_details_scraper.log',
    'log_level': 'INFO',
    'log_format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
}

# Export Configuration
EXPORT_CONFIG = {
    'export_dir': 'exports',
    'formats': ['json', 'csv'],
    'include_failed': False  # Whether to include failed scrapes in exports
}