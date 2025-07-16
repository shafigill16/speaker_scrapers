#!/usr/bin/env python3
"""
MongoDB utilities for managing speaker data
"""
from pymongo import MongoClient
from datetime import datetime
import json
from config import MONGODB_CONFIG

class MongoDBManager:
    def __init__(self):
        self.client = MongoClient(MONGODB_CONFIG['uri'], serverSelectionTimeoutMS=5000)
        self.db = self.client[MONGODB_CONFIG['database']]
        self.collection = self.db[MONGODB_CONFIG['collection']]
    
    def test_connection(self):
        """Test MongoDB connection"""
        try:
            # Test connection
            info = self.client.server_info()
            print("✓ Successfully connected to MongoDB")
            print(f"  Server version: {info['version']}")
            print(f"  Database: {MONGODB_CONFIG['database']}")
            print(f"  Collection: {MONGODB_CONFIG['collection']}")
            return True
        except Exception as e:
            print(f"✗ Failed to connect to MongoDB: {e}")
            return False
    
    def get_statistics(self):
        """Get collection statistics"""
        try:
            stats = {
                'total_speakers': self.collection.count_documents({}),
                'speakers_with_email': self.collection.count_documents({'contact_info.email': {'$exists': True}}),
                'speakers_with_phone': self.collection.count_documents({'contact_info.phone': {'$exists': True}}),
                'speakers_with_website': self.collection.count_documents({'contact_info.website': {'$exists': True}}),
                'speakers_with_social': self.collection.count_documents({'social_media': {'$exists': True}})
            }
            
            # Get top locations
            pipeline = [
                {"$group": {"_id": "$location", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}},
                {"$limit": 10}
            ]
            top_locations = list(self.collection.aggregate(pipeline))
            stats['top_locations'] = {loc['_id']: loc['count'] for loc in top_locations if loc['_id']}
            
            # Get member levels distribution
            pipeline = [
                {"$group": {"_id": "$member_level", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}}
            ]
            member_levels = list(self.collection.aggregate(pipeline))
            stats['member_levels'] = {level['_id']: level['count'] for level in member_levels if level['_id']}
            
            return stats
        except Exception as e:
            print(f"Error getting statistics: {e}")
            return None
    
    def find_speakers(self, query={}, limit=10):
        """Find speakers with optional query"""
        try:
            speakers = list(self.collection.find(query).limit(limit))
            return speakers
        except Exception as e:
            print(f"Error finding speakers: {e}")
            return []
    
    def export_to_json(self, filename=None, query={}):
        """Export speakers to JSON file"""
        try:
            if not filename:
                filename = f"speakers_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            
            speakers = list(self.collection.find(query))
            
            # Convert ObjectId to string for JSON serialization
            for speaker in speakers:
                if '_id' in speaker:
                    speaker['_id'] = str(speaker['_id'])
                if 'scraped_at' in speaker:
                    speaker['scraped_at'] = speaker['scraped_at'].isoformat()
                if 'last_updated' in speaker:
                    speaker['last_updated'] = speaker['last_updated'].isoformat()
                if 'created_at' in speaker:
                    speaker['created_at'] = speaker['created_at'].isoformat()
            
            with open(filename, 'w') as f:
                json.dump(speakers, f, indent=2)
            
            print(f"✓ Exported {len(speakers)} speakers to {filename}")
            return filename
        except Exception as e:
            print(f"Error exporting to JSON: {e}")
            return None
    
    def delete_duplicates(self):
        """Remove duplicate speakers based on profile_url"""
        try:
            pipeline = [
                {"$group": {
                    "_id": "$profile_url",
                    "count": {"$sum": 1},
                    "ids": {"$push": "$_id"}
                }},
                {"$match": {"count": {"$gt": 1}}}
            ]
            
            duplicates = list(self.collection.aggregate(pipeline))
            total_deleted = 0
            
            for dup in duplicates:
                # Keep the first one, delete the rest
                ids_to_delete = dup['ids'][1:]
                result = self.collection.delete_many({'_id': {'$in': ids_to_delete}})
                total_deleted += result.deleted_count
            
            print(f"✓ Deleted {total_deleted} duplicate speakers")
            return total_deleted
        except Exception as e:
            print(f"Error deleting duplicates: {e}")
            return 0
    
    def close(self):
        """Close MongoDB connection"""
        self.client.close()


def main():
    """Main utility function"""
    manager = MongoDBManager()
    
    print("MongoDB Speaker Database Utilities")
    print("="*50)
    
    # Test connection
    if not manager.test_connection():
        return
    
    # Get statistics
    print("\nDatabase Statistics:")
    stats = manager.get_statistics()
    if stats:
        print(f"  Total speakers: {stats['total_speakers']}")
        print(f"  With email: {stats['speakers_with_email']}")
        print(f"  With phone: {stats['speakers_with_phone']}")
        print(f"  With website: {stats['speakers_with_website']}")
        print(f"  With social media: {stats['speakers_with_social']}")
        
        if stats['top_locations']:
            print("\n  Top Locations:")
            for location, count in list(stats['top_locations'].items())[:5]:
                print(f"    {location}: {count}")
        
        if stats['member_levels']:
            print("\n  Member Levels:")
            for level, count in stats['member_levels'].items():
                print(f"    {level}: {count}")
    
    # Find sample speakers
    print("\nSample Speakers:")
    speakers = manager.find_speakers(limit=3)
    for speaker in speakers:
        print(f"  - {speaker.get('name', 'Unknown')} ({speaker.get('location', 'Unknown')})")
    
    # Export option
    print("\nOptions:")
    print("1. Export all data to JSON")
    print("2. Delete duplicates")
    print("3. Exit")
    
    manager.close()


if __name__ == "__main__":
    main()