"""
Configuration settings for the Free Speaker Bureau scraper
Loads sensitive data from environment variables
"""
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# MongoDB Configuration
MONGODB_CONFIG = {
    'uri': os.getenv('MONGODB_URI', 'mongodb://localhost:27017/'),
    'database': os.getenv('MONGODB_DATABASE', 'freespeakerbureau_scraper'),
    'collection': os.getenv('MONGODB_COLLECTION', 'speakers_profiles'),
}

# Proxy Configuration
# Use rotating proxy if available, otherwise use proxy list
proxy_rotating = os.getenv('PROXY_ROTATING_URL')
proxy_list = os.getenv('PROXY_LIST', '').split(',') if os.getenv('PROXY_LIST') else []

if proxy_rotating:
    PROXY_CONFIG = {
        'http': proxy_rotating,
        'https': proxy_rotating
    }
else:
    PROXY_CONFIG = {}

# Export proxy list for random selection
PROXY_LIST = [p.strip() for p in proxy_list if p.strip()]

# Scraper Settings
SCRAPER_CONFIG = {
    'base_url': os.getenv('BASE_URL', 'https://www.freespeakerbureau.com'),
    'max_workers': int(os.getenv('MAX_WORKERS', '5')),
    'batch_size': int(os.getenv('BATCH_SIZE', '10')),
    'request_timeout': int(os.getenv('REQUEST_TIMEOUT', '30')),
    'retry_attempts': int(os.getenv('RETRY_ATTEMPTS', '3')),
    'delay_between_requests': int(os.getenv('DELAY_BETWEEN_REQUESTS', '2'))
}

# Request Headers
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate, br',
    'DNT': '1',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1'
}