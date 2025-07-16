import os
from pymongo import MongoClient
import logging
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# MongoDB Configuration
MONGO_USERNAME = os.getenv('MONGO_USERNAME')
MONGO_PASSWORD = os.getenv('MONGO_PASSWORD')
MONGO_HOST = os.getenv('MONGO_HOST')
MONGO_PORT = os.getenv('MONGO_PORT')
MONGO_AUTH_SOURCE = os.getenv('MONGO_AUTH_SOURCE')
MONGO_URI = f"mongodb://{MONGO_USERNAME}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/?authSource={MONGO_AUTH_SOURCE}"
DATABASE_NAME = os.getenv('MONGO_DATABASE_NAME', 'bigspeak_scraper')
COLLECTION_NAME = "speakers"

# Scraping Configuration
BASE_URL = os.getenv('BASE_URL', 'https://www.bigspeak.com')
SPEAKERS_URL = os.getenv('SPEAKERS_URL', 'https://www.bigspeak.com/keynote-speakers/')
REQUEST_TIMEOUT = 30
RETRY_ATTEMPTS = 3
DELAY_BETWEEN_REQUESTS = 1  # seconds

# Logging Configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper.log'),
        logging.StreamHandler()
    ]
)

def get_database():
    """Get MongoDB database connection"""
    client = MongoClient(MONGO_URI)
    return client[DATABASE_NAME]

def get_collection():
    """Get MongoDB collection"""
    db = get_database()
    return db[COLLECTION_NAME]