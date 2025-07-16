"""
Utility functions for loading environment variables and common operations
"""

import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def get_env_variable(var_name, default=None, required=False):
    """Get environment variable with optional default and required flag"""
    value = os.getenv(var_name, default)
    if required and not value:
        raise ValueError(f"Required environment variable {var_name} is not set")
    return value

# MongoDB Configuration
MONGO_URI = get_env_variable("MONGO_URI", required=True)
TARGET_DATABASE = get_env_variable("TARGET_DATABASE", "speaker_database")
COLLECTION = get_env_variable("COLLECTION", "unified_speakers_v3")

# Source databases configuration
SOURCES = {
    "a_speakers": "speakers",
    "allamericanspeakers": "speakers", 
    "bigspeak_scraper": "speaker_profiles",
    "eventraptor": "speakers",
    "freespeakerbureau_scraper": "speakers_profiles",
    "leading_authorities": "speakers_final_details",
    "sessionize_scraper": "speaker_profiles",
    "speakerhub_scraper": "speaker_details",
    "thespeakerhandbook_scraper": "speaker_profiles"
}