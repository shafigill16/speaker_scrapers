from config import get_profiles_collection, get_speakers_collection
import json
from datetime import datetime

def json_encoder(obj):
    """JSON encoder that handles datetime objects"""
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

def view_detailed_profile(speaker_id):
    """View detailed profile information for a specific speaker"""
    collection = get_profiles_collection()
    profile = collection.find_one({'speaker_id': speaker_id})
    
    if not profile:
        print(f"No profile found for speaker_id: {speaker_id}")
        return
    
    print(f"\n{'='*70}")
    print(f"DETAILED PROFILE: {profile['name']}")
    print(f"{'='*70}")
    
    # Basic Info
    print(f"\n1. BASIC INFORMATION:")
    print(f"   Speaker ID: {profile.get('speaker_id', 'N/A')}")
    print(f"   Profile URL: {profile.get('profile_url', 'N/A')}")
    if profile.get('structured_data'):
        print(f"   Job Title: {profile['structured_data'].get('job_title', 'N/A')}")
        print(f"   Contact: {profile['structured_data'].get('telephone', 'N/A')}")
    
    # Why Choose Section
    if profile.get('why_choose'):
        print(f"\n2. WHY CHOOSE {profile['name'].upper()}?")
        print(f"   {profile['why_choose'][:300]}...")
    
    # Biography
    if profile.get('biography'):
        print(f"\n3. BIOGRAPHY:")
        print(f"   {profile['biography'][:500]}...")
        print(f"   [Total length: {len(profile['biography'])} characters]")
    
    # Keynote Topics
    if profile.get('keynote_topics'):
        print(f"\n4. KEYNOTE SPEAKER TOPICS:")
        for topic in profile['keynote_topics'][:10]:
            print(f"   • {topic}")
        if len(profile['keynote_topics']) > 10:
            print(f"   ... and {len(profile['keynote_topics']) - 10} more topics")
    
    # Speaking Programs
    if profile.get('speaking_programs'):
        print(f"\n5. SPEAKING PROGRAMS ({len(profile['speaking_programs'])} programs):")
        for i, program in enumerate(profile['speaking_programs'], 1):
            print(f"\n   Program {i}: {program['title']}")
            print(f"   Description: {program['short_description'][:150]}...")
            if program.get('key_takeaways'):
                print(f"   Key Takeaways: {len(program['key_takeaways'])} points")
    
    # Videos
    if profile.get('videos'):
        print(f"\n6. VIDEOS ({len(profile['videos'])} found):")
        for video in profile['videos'][:5]:
            print(f"   • {video.get('title', 'Untitled')} ({video['platform']})")
            if video.get('watch_url'):
                print(f"     URL: {video['watch_url']}")
    
    # Testimonials
    if profile.get('testimonials'):
        print(f"\n7. TESTIMONIALS ({len(profile['testimonials'])} found):")
        for i, testimonial in enumerate(profile['testimonials'][:3], 1):
            print(f"\n   Testimonial {i}:")
            print(f"   \"{testimonial['quote'][:150]}...\"")
            if testimonial.get('author'):
                print(f"   - {testimonial['author']}")
                if testimonial.get('company'):
                    print(f"     {testimonial['company']}")
    
    # Books
    if profile.get('books'):
        print(f"\n8. BOOKS & PUBLICATIONS:")
        for book in profile['books']:
            if isinstance(book, dict):
                print(f"   • {book['title']}")
                if book.get('bestseller'):
                    print(f"     (Bestseller)")
            else:
                print(f"   • {book}")
    
    # Awards
    if profile.get('awards'):
        print(f"\n9. AWARDS & RECOGNITIONS:")
        for award in profile['awards'][:5]:
            print(f"   • {award}")
    
    # Social Media
    if profile.get('social_media'):
        print(f"\n10. SOCIAL MEDIA:")
        for platform, url in profile['social_media'].items():
            print(f"    {platform.capitalize()}: {url}")
    
    # Images
    if profile.get('images'):
        print(f"\n11. IMAGES ({len(profile['images'])} found):")
        for img in profile['images'][:3]:
            print(f"    • Type: {img.get('type', 'N/A')} - {img['url'][:80]}...")
    
    # Data Quality Score
    print(f"\n12. DATA QUALITY SCORE:")
    score = calculate_profile_completeness(profile)
    print(f"    Overall Completeness: {score}%")

def calculate_profile_completeness(profile):
    """Calculate how complete a profile is"""
    fields = {
        'biography': 15,
        'why_choose': 10,
        'keynote_topics': 10,
        'speaking_programs': 15,
        'videos': 10,
        'testimonials': 10,
        'books': 5,
        'awards': 5,
        'social_media': 5,
        'images': 5,
        'structured_data': 10
    }
    
    total_score = 0
    
    for field, weight in fields.items():
        if profile.get(field):
            if isinstance(profile[field], (list, dict)):
                if len(profile[field]) > 0:
                    total_score += weight
            elif isinstance(profile[field], str) and len(profile[field]) > 10:
                total_score += weight
    
    return total_score

def get_profile_stats_v2():
    """Get enhanced statistics about the scraped profile data"""
    profiles_collection = get_profiles_collection()
    speakers_collection = get_speakers_collection()
    
    total_speakers = speakers_collection.count_documents({})
    total_profiles = profiles_collection.count_documents({})
    v2_profiles = profiles_collection.count_documents({'source': 'profile_page_v2'})
    
    print(f"\n{'='*70}")
    print(f"ENHANCED PROFILE SCRAPING STATISTICS")
    print(f"{'='*70}")
    print(f"\nOverall Stats:")
    print(f"  Total Speakers: {total_speakers}")
    print(f"  Total Profiles: {total_profiles}")
    print(f"  V2 Profiles: {v2_profiles}")
    print(f"  Remaining: {total_speakers - v2_profiles}")
    
    # Aggregate detailed statistics for V2 profiles
    pipeline = [
        {"$match": {"source": "profile_page_v2"}},
        {
            "$group": {
                "_id": None,
                "total": {"$sum": 1},
                "with_biography": {"$sum": {"$cond": [{"$gt": [{"$strLenCP": {"$ifNull": ["$biography", ""]}}, 100]}, 1, 0]}},
                "with_why_choose": {"$sum": {"$cond": [{"$gt": [{"$strLenCP": {"$ifNull": ["$why_choose", ""]}}, 50]}, 1, 0]}},
                "with_programs": {"$sum": {"$cond": [{"$gt": [{"$size": {"$ifNull": ["$speaking_programs", []]}}, 0]}, 1, 0]}},
                "with_videos": {"$sum": {"$cond": [{"$gt": [{"$size": {"$ifNull": ["$videos", []]}}, 0]}, 1, 0]}},
                "with_testimonials": {"$sum": {"$cond": [{"$gt": [{"$size": {"$ifNull": ["$testimonials", []]}}, 0]}, 1, 0]}},
                "with_books": {"$sum": {"$cond": [{"$gt": [{"$size": {"$ifNull": ["$books", []]}}, 0]}, 1, 0]}},
                "with_awards": {"$sum": {"$cond": [{"$gt": [{"$size": {"$ifNull": ["$awards", []]}}, 0]}, 1, 0]}},
                "with_social": {"$sum": {"$cond": [{"$gt": [{"$size": {"$objectToArray": {"$ifNull": ["$social_media", {}]}}}, 0]}, 1, 0]}},
                "avg_programs": {"$avg": {"$size": {"$ifNull": ["$speaking_programs", []]}}},
                "avg_videos": {"$avg": {"$size": {"$ifNull": ["$videos", []]}}},
                "avg_testimonials": {"$avg": {"$size": {"$ifNull": ["$testimonials", []]}}}
            }
        }
    ]
    
    stats = list(profiles_collection.aggregate(pipeline))
    
    if stats:
        s = stats[0]
        print(f"\nContent Coverage (V2 Profiles):")
        print(f"  With Biography: {s.get('with_biography', 0)} ({s.get('with_biography', 0)/s['total']*100:.1f}%)")
        print(f"  With 'Why Choose': {s.get('with_why_choose', 0)} ({s.get('with_why_choose', 0)/s['total']*100:.1f}%)")
        print(f"  With Speaking Programs: {s.get('with_programs', 0)} ({s.get('with_programs', 0)/s['total']*100:.1f}%)")
        print(f"  With Videos: {s.get('with_videos', 0)} ({s.get('with_videos', 0)/s['total']*100:.1f}%)")
        print(f"  With Testimonials: {s.get('with_testimonials', 0)} ({s.get('with_testimonials', 0)/s['total']*100:.1f}%)")
        print(f"  With Books: {s.get('with_books', 0)} ({s.get('with_books', 0)/s['total']*100:.1f}%)")
        print(f"  With Awards: {s.get('with_awards', 0)} ({s.get('with_awards', 0)/s['total']*100:.1f}%)")
        print(f"  With Social Media: {s.get('with_social', 0)} ({s.get('with_social', 0)/s['total']*100:.1f}%)")
        
        print(f"\nAverage Content per Profile:")
        print(f"  Avg Speaking Programs: {s.get('avg_programs', 0):.1f}")
        print(f"  Avg Videos: {s.get('avg_videos', 0):.1f}")
        print(f"  Avg Testimonials: {s.get('avg_testimonials', 0):.1f}")
    
    # Get top speakers by completeness
    print(f"\nTop 5 Most Complete Profiles:")
    profiles = list(profiles_collection.find({'source': 'profile_page_v2'}).limit(100))
    
    profiles_with_scores = []
    for profile in profiles:
        score = calculate_profile_completeness(profile)
        profiles_with_scores.append({
            'name': profile['name'],
            'speaker_id': profile['speaker_id'],
            'score': score
        })
    
    profiles_with_scores.sort(key=lambda x: x['score'], reverse=True)
    
    for i, p in enumerate(profiles_with_scores[:5], 1):
        print(f"  {i}. {p['name']} - {p['score']}% complete (ID: {p['speaker_id']})")

def export_enhanced_profiles(filename='enhanced_profiles.json', limit=None):
    """Export enhanced profile data"""
    collection = get_profiles_collection()
    
    query = {'source': 'profile_page_v2'}
    cursor = collection.find(query, {'_id': 0})
    
    if limit:
        cursor = cursor.limit(limit)
    
    profiles = list(cursor)
    
    # Add completeness score to each profile
    for profile in profiles:
        profile['completeness_score'] = calculate_profile_completeness(profile)
    
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(profiles, f, indent=2, ensure_ascii=False, default=json_encoder)
    
    print(f"Exported {len(profiles)} enhanced profiles to {filename}")

def compare_v1_v2_profiles():
    """Compare V1 and V2 profile data"""
    collection = get_profiles_collection()
    
    # Get a speaker that has both V1 and V2 data
    v1_profile = collection.find_one({'source': 'profile_page'})
    if v1_profile:
        v2_profile = collection.find_one({
            'speaker_id': v1_profile['speaker_id'],
            'source': 'profile_page_v2'
        })
        
        if v2_profile:
            print(f"\n{'='*70}")
            print(f"PROFILE COMPARISON: {v1_profile['name']}")
            print(f"{'='*70}")
            
            print("\nV1 Profile Fields:")
            for key in v1_profile.keys():
                if key not in ['_id', 'scraped_at', 'source']:
                    print(f"  • {key}")
            
            print("\nV2 Profile Fields:")
            for key in v2_profile.keys():
                if key not in ['_id', 'scraped_at', 'source']:
                    print(f"  • {key}")
            
            print("\nNew fields in V2:")
            v2_only = set(v2_profile.keys()) - set(v1_profile.keys())
            for field in v2_only:
                print(f"  + {field}")

if __name__ == "__main__":
    print("BigSpeak Enhanced Profile Data Utilities (V2)")
    print("=" * 70)
    
    # Show current stats
    get_profile_stats_v2()
    
    # Show a sample detailed profile
    print("\n\nSample Detailed Profile:")
    print("To view a detailed profile, run: view_detailed_profile('speaker_id')")
    
    # Compare V1 and V2 if available
    # compare_v1_v2_profiles()