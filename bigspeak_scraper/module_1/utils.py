from config import get_collection
import json
from datetime import datetime

def json_encoder(obj):
    """JSON encoder that handles datetime objects"""
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

def view_sample_speakers(limit=5):
    """View sample speakers from the database"""
    collection = get_collection()
    speakers = list(collection.find().limit(limit))
    
    print(f"\n{'='*50}")
    print(f"Sample Speakers (showing {limit}):")
    print(f"{'='*50}\n")
    
    for i, speaker in enumerate(speakers, 1):
        print(f"{i}. {speaker.get('name', 'Unknown')}")
        print(f"   ID: {speaker.get('speaker_id', 'N/A')}")
        print(f"   Description: {speaker.get('description', 'N/A')[:100]}...")
        print(f"   Fee Range: {speaker.get('fee_range', 'N/A')}")
        print(f"   Topics: {', '.join([t['name'] for t in speaker.get('topics', [])])}")
        print(f"   Profile URL: {speaker.get('profile_url', 'N/A')}")
        print()

def get_collection_stats():
    """Get statistics about the scraped data"""
    collection = get_collection()
    
    total_speakers = collection.count_documents({})
    
    # Get unique fee ranges
    fee_ranges = collection.distinct('fee_range')
    
    # Get speakers with most topics
    pipeline = [
        {"$project": {"name": 1, "topic_count": {"$size": "$topics"}}},
        {"$sort": {"topic_count": -1}},
        {"$limit": 5}
    ]
    top_speakers = list(collection.aggregate(pipeline))
    
    print(f"\n{'='*50}")
    print(f"Collection Statistics:")
    print(f"{'='*50}")
    print(f"Total Speakers: {total_speakers}")
    print(f"Unique Fee Ranges: {len(fee_ranges)}")
    print(f"\nFee Ranges:")
    for fee in sorted(fee_ranges):
        count = collection.count_documents({'fee_range': fee})
        print(f"  - {fee}: {count} speakers")
    
    print(f"\nSpeakers with Most Topics:")
    for speaker in top_speakers:
        print(f"  - {speaker['name']}: {speaker['topic_count']} topics")

def export_to_json(filename='speakers_export.json'):
    """Export all speakers to JSON file"""
    collection = get_collection()
    speakers = list(collection.find({}, {'_id': 0}))
    
    with open(f"module_1/{filename}", 'w', encoding='utf-8') as f:
        json.dump(speakers, f, indent=2, ensure_ascii=False, default=json_encoder)
    
    print(f"Exported {len(speakers)} speakers to module_1/{filename}")

if __name__ == "__main__":
    # Test the database connection and view some data
    print("Testing database connection and viewing data...")
    
    view_sample_speakers()
    get_collection_stats()
    
    # Optionally export to JSON
    # export_to_json()