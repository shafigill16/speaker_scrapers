"""
Configuration settings for SpeakerHub scraper
"""

# MongoDB Configuration
MONGO_CONFIG = {
    "connection_string": "mongodb://admin:dev2018@5.161.225.172:27017/?authSource=admin",
    "database_name": "speakerhub_scraper",
    "collection_name": "speakers"
}

# Scraper Configuration
SCRAPER_CONFIG = {
    "max_scroll_attempts": 50000,  # Maximum number of scrolls
    "batch_size": 50,  # Number of speakers to save in one batch
    "scroll_pause_time": (2, 3),  # Min and max seconds to wait after scroll
    "human_delay": (2, 5),  # Min and max seconds for human-like delays
    "long_break_interval": 40,  # Take a longer break every N scrolls
    "long_break_duration": (3, 5),  # Duration of longer breaks
    "no_content_threshold": 50000  # Stop after N scrolls with no new content
}

# Browser Configuration
BROWSER_CONFIG = {
    "headless": True,
    "user_agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "headers": {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "DNT": "1",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1"
    }
}

# Logging Configuration
LOGGING_CONFIG = {
    "level": "INFO",
    "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    "filename": "speakerhub_scraper.log"
}