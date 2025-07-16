from config import get_collection
from datetime import datetime
import json

def debug_database():
    """Debug database to understand what happened"""
    collection = get_collection()
    
    # Get total count
    total = collection.count_documents({})
    print(f"Total documents in database: {total}")
    
    # Check for duplicates by speaker_id
    pipeline = [
        {"$group": {"_id": "$speaker_id", "count": {"$sum": 1}}},
        {"$match": {"count": {"$gt": 1}}},
        {"$sort": {"count": -1}}
    ]
    duplicates = list(collection.aggregate(pipeline))
    print(f"\nDuplicate speaker_ids: {len(duplicates)}")
    
    # Get scraped_at timestamps
    print("\nChecking scraped_at timestamps:")
    speakers = list(collection.find({}, {"name": 1, "scraped_at": 1, "speaker_id": 1}).sort("scraped_at", -1).limit(10))
    for speaker in speakers:
        print(f"  - {speaker['name']} (ID: {speaker['speaker_id']}) - Scraped: {speaker.get('scraped_at', 'N/A')}")
    
    # Check unique speaker_ids
    unique_ids = collection.distinct("speaker_id")
    print(f"\nUnique speaker IDs: {len(unique_ids)}")
    
    # Check if there's an issue with the index
    indexes = collection.list_indexes()
    print("\nDatabase indexes:")
    for index in indexes:
        print(f"  - {index}")
    
    # Get the first and last few speaker IDs
    print("\nFirst 5 speaker IDs:")
    first_speakers = list(collection.find({}, {"speaker_id": 1, "name": 1}).limit(5))
    for s in first_speakers:
        print(f"  - {s['speaker_id']}: {s['name']}")
    
    # Check if we're overwriting data
    print("\nChecking for page 1 speakers (should be first 15):")
    page1_speakers = [
        "elatia-abate", "jim-abbott", "yassmin-abdel-magied", 
        "kevin-abdulrahman", "mariangela-m-abeo"
    ]
    for speaker_id in page1_speakers[:3]:
        doc = collection.find_one({"speaker_id": speaker_id})
        if doc:
            print(f"  ✓ Found: {doc['name']}")
        else:
            print(f"  ✗ Missing: {speaker_id}")

if __name__ == "__main__":
    debug_database()