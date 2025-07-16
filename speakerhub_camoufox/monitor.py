#!/usr/bin/env python3
"""
Monitor SpeakerHub scraper database
"""

import time
from datetime import datetime
from pymongo import MongoClient
from config import MONGO_CONFIG
import sys


def monitor_collection():
    """Monitor MongoDB collection statistics"""
    
    # Connect to MongoDB
    client = MongoClient(MONGO_CONFIG['connection_string'])
    db = client[MONGO_CONFIG['database_name']]
    collection = db[MONGO_CONFIG['collection_name']]
    
    print("SpeakerHub Scraper Monitor")
    print("="*50)
    print(f"Database: {MONGO_CONFIG['database_name']}")
    print(f"Collection: {MONGO_CONFIG['collection_name']}")
    print("="*50)
    
    try:
        while True:
            # Get statistics
            total_count = collection.count_documents({})
            
            # Get recent speakers
            recent_speakers = list(collection.find({}, {
                'name': 1, 
                'scraped_at': 1,
                'country': 1,
                '_id': 0
            }).sort('scraped_at', -1).limit(5))
            
            # Get country distribution
            pipeline = [
                {"$group": {"_id": "$country", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}},
                {"$limit": 5}
            ]
            country_stats = list(collection.aggregate(pipeline))
            
            # Clear screen (works on Unix-like systems)
            print("\033[H\033[J", end="")
            
            # Print updated stats
            print(f"Last Update: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print("="*50)
            print(f"Total Speakers: {total_count}")
            print("\nTop Countries:")
            for stat in country_stats:
                print(f"  - {stat['_id']}: {stat['count']}")
            
            print("\nRecent Additions:")
            for speaker in recent_speakers:
                scraped_at = speaker.get('scraped_at', 'Unknown')
                if isinstance(scraped_at, str):
                    # Parse ISO format
                    try:
                        scraped_at = datetime.fromisoformat(scraped_at.replace('Z', '+00:00'))
                        scraped_at = scraped_at.strftime('%H:%M:%S')
                    except:
                        scraped_at = 'Unknown'
                
                print(f"  - {speaker['name']} ({speaker.get('country', 'Unknown')}) at {scraped_at}")
            
            print("\n[Press Ctrl+C to exit]")
            
            # Update every 5 seconds
            time.sleep(5)
            
    except KeyboardInterrupt:
        print("\n\nMonitoring stopped.")
    finally:
        client.close()


if __name__ == "__main__":
    monitor_collection()