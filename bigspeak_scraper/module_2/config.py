import os
import sys
from pymongo import MongoClient
import logging
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add parent directory to path to import from module_1
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# MongoDB Configuration (same as module_1)
MONGO_USERNAME = os.getenv('MONGO_USERNAME')
MONGO_PASSWORD = os.getenv('MONGO_PASSWORD')
MONGO_HOST = os.getenv('MONGO_HOST')
MONGO_PORT = os.getenv('MONGO_PORT')
MONGO_AUTH_SOURCE = os.getenv('MONGO_AUTH_SOURCE')
MONGO_URI = f"mongodb://{MONGO_USERNAME}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/?authSource={MONGO_AUTH_SOURCE}"
DATABASE_NAME = os.getenv('MONGO_DATABASE_NAME', 'bigspeak_scraper')
COLLECTION_NAME = "speakers"
PROFILES_COLLECTION_NAME = "speaker_profiles"  # New collection for detailed profiles

# Scraping Configuration
BASE_URL = os.getenv('BASE_URL', 'https://www.bigspeak.com')
REQUEST_TIMEOUT = 30
RETRY_ATTEMPTS = 3
DELAY_BETWEEN_REQUESTS = 2  # seconds (longer delay for profile pages)
BATCH_SIZE = 50  # Number of profiles to scrape before saving progress

# Logging Configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('profile_scraper.log'),
        logging.StreamHandler()
    ]
)

def get_database():
    """Get MongoDB database connection"""
    client = MongoClient(MONGO_URI)
    return client[DATABASE_NAME]

def get_speakers_collection():
    """Get speakers collection from module_1"""
    db = get_database()
    return db[COLLECTION_NAME]

def get_profiles_collection():
    """Get speaker profiles collection for module_2"""
    db = get_database()
    return db[PROFILES_COLLECTION_NAME]