#!/usr/bin/env python3
"""
Check scraping progress and statistics
"""
from pymongo import MongoClient
from config import MONGODB_CONFIG
import json

def check_progress():
    client = MongoClient(MONGODB_CONFIG['uri'])
    db = client[MONGODB_CONFIG['database']]
    collection = db[MONGODB_CONFIG['collection']]
    
    # Get statistics
    total = collection.count_documents({})
    with_phone = collection.count_documents({'contact_info.phone': {'$exists': True}})
    with_email = collection.count_documents({'contact_info.email': {'$exists': True}})
    with_website = collection.count_documents({'website': {'$exists': True}})
    with_onesheet = collection.count_documents({'speaker_onesheet_url': {'$exists': True}})
    
    print(f"\nFree Speaker Bureau Scraping Progress")
    print("="*50)
    print(f"Total speakers scraped: {total}")
    print(f"Speakers with phone: {with_phone}")
    print(f"Speakers with email: {with_email}")
    print(f"Speakers with website: {with_website}")
    print(f"Speakers with OneSheet PDF: {with_onesheet}")
    
    # Get sample with all contact info
    sample = collection.find_one({
        '$or': [
            {'contact_info.phone': {'$exists': True}},
            {'contact_info.email': {'$exists': True}},
            {'website': {'$exists': True}}
        ]
    })
    
    if sample:
        print("\nSample speaker with contact info:")
        sample['_id'] = str(sample['_id'])
        for key in ['scraped_at', 'last_updated', 'created_at']:
            if key in sample and hasattr(sample[key], 'isoformat'):
                sample[key] = sample[key].isoformat()
        print(json.dumps(sample, indent=2))
    
    client.close()

if __name__ == "__main__":
    check_progress()