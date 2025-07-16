from config import get_profiles_collection, get_speakers_collection
import json
from datetime import datetime

def json_encoder(obj):
    """JSON encoder that handles datetime objects"""
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

def view_sample_profiles(limit=3):
    """View sample speaker profiles from the database"""
    collection = get_profiles_collection()
    profiles = list(collection.find().limit(limit))
    
    print(f"\n{'='*50}")
    print(f"Sample Speaker Profiles (showing {limit}):")
    print(f"{'='*50}\n")
    
    for i, profile in enumerate(profiles, 1):
        print(f"{i}. {profile.get('name', 'Unknown')}")
        print(f"   Speaker ID: {profile.get('speaker_id', 'N/A')}")
        print(f"   Biography: {profile.get('biography', 'N/A')[:200]}...")
        
        # Books
        books = profile.get('books', [])
        if books:
            print(f"   Books: {', '.join(books[:3])}")
        
        # Videos
        videos = profile.get('videos', [])
        if videos:
            print(f"   Videos: {len(videos)} found")
        
        # Social Media
        social = profile.get('social_media', {})
        if social:
            print(f"   Social Media: {', '.join(social.keys())}")
        
        # Awards
        awards = profile.get('awards', [])
        if awards:
            print(f"   Awards: {len(awards)} recognitions")
        
        print()

def get_profile_stats():
    """Get statistics about the scraped profile data"""
    profiles_collection = get_profiles_collection()
    speakers_collection = get_speakers_collection()
    
    total_speakers = speakers_collection.count_documents({})
    total_profiles = profiles_collection.count_documents({})
    
    # Aggregate statistics
    pipeline = [
        {
            "$group": {
                "_id": None,
                "with_biography": {"$sum": {"$cond": [{"$gt": [{"$strLenCP": "$biography"}, 100]}, 1, 0]}},
                "with_books": {"$sum": {"$cond": [{"$gt": [{"$size": "$books"}, 0]}, 1, 0]}},
                "with_videos": {"$sum": {"$cond": [{"$gt": [{"$size": "$videos"}, 0]}, 1, 0]}},
                "with_awards": {"$sum": {"$cond": [{"$gt": [{"$size": "$awards"}, 0]}, 1, 0]}},
                "with_social": {"$sum": {"$cond": [{"$gt": [{"$size": {"$objectToArray": "$social_media"}}, 0]}, 1, 0]}}
            }
        }
    ]
    
    stats = list(profiles_collection.aggregate(pipeline))
    
    print(f"\n{'='*50}")
    print(f"Profile Scraping Statistics:")
    print(f"{'='*50}")
    print(f"Total Speakers: {total_speakers}")
    print(f"Profiles Scraped: {total_profiles} ({total_profiles/total_speakers*100:.1f}%)")
    print(f"Remaining: {total_speakers - total_profiles}")
    
    if stats:
        s = stats[0]
        print(f"\nProfile Content Stats:")
        print(f"  - With Biography: {s.get('with_biography', 0)}")
        print(f"  - With Books: {s.get('with_books', 0)}")
        print(f"  - With Videos: {s.get('with_videos', 0)}")
        print(f"  - With Awards: {s.get('with_awards', 0)}")
        print(f"  - With Social Media: {s.get('with_social', 0)}")
    
    # Sample some speakers without profiles
    print(f"\nSample speakers without profiles:")
    existing_profiles = profiles_collection.distinct('speaker_id')
    unscraped = speakers_collection.find(
        {'speaker_id': {'$nin': existing_profiles}}
    ).limit(5)
    
    for speaker in unscraped:
        print(f"  - {speaker['name']} ({speaker['speaker_id']})")

def export_profiles_to_json(filename='speaker_profiles.json', limit=None):
    """Export speaker profiles to JSON file"""
    collection = get_profiles_collection()
    
    query = {}
    cursor = collection.find(query, {'_id': 0})
    
    if limit:
        cursor = cursor.limit(limit)
    
    profiles = list(cursor)
    
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(profiles, f, indent=2, ensure_ascii=False, default=json_encoder)
    
    print(f"Exported {len(profiles)} profiles to {filename}")

def check_profile_quality(speaker_id):
    """Check the quality and completeness of a specific profile"""
    collection = get_profiles_collection()
    profile = collection.find_one({'speaker_id': speaker_id})
    
    if not profile:
        print(f"No profile found for speaker_id: {speaker_id}")
        return
    
    print(f"\n{'='*50}")
    print(f"Profile Quality Check: {profile['name']}")
    print(f"{'='*50}")
    
    # Check each field
    checks = {
        'Biography': len(profile.get('biography', '')) > 100,
        'Speaking Topics': len(profile.get('speaking_topics_detailed', [])) > 0,
        'Books': len(profile.get('books', [])) > 0,
        'Videos': len(profile.get('videos', [])) > 0,
        'Awards': len(profile.get('awards', [])) > 0,
        'Social Media': len(profile.get('social_media', {})) > 0,
        'Credentials': len(profile.get('credentials', [])) > 0,
        'High-res Images': len(profile.get('images', [])) > 0
    }
    
    for field, has_data in checks.items():
        status = "✅" if has_data else "❌"
        print(f"{status} {field}")
    
    completeness = sum(checks.values()) / len(checks) * 100
    print(f"\nProfile Completeness: {completeness:.1f}%")

if __name__ == "__main__":
    print("BigSpeak Profile Data Utilities")
    print("=" * 50)
    
    # Show current stats
    get_profile_stats()
    
    # Show sample profiles
    view_sample_profiles()
    
    # Export option
    # export_profiles_to_json('sample_profiles.json', limit=10)