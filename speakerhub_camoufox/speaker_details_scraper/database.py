#!/usr/bin/env python3
"""
MongoDB handler for speaker details with resume capability
"""

import logging
from datetime import datetime
from typing import List, Dict, Optional, Tuple
from pymongo import MongoClient, UpdateOne, ASCENDING
from pymongo.errors import ConnectionFailure, BulkWriteError
from models import DetailedSpeaker

logger = logging.getLogger(__name__)


class SpeakerDetailsDB:
    """Handle MongoDB operations for detailed speaker data with resume capability"""
    
    def __init__(self, connection_string: str, database_name: str, 
                 source_collection: str, details_collection: str, 
                 resume_collection: str):
        self.connection_string = connection_string
        self.database_name = database_name
        self.source_collection_name = source_collection
        self.details_collection_name = details_collection
        self.resume_collection_name = resume_collection
        self.client = None
        self.db = None
        self.source_collection = None
        self.details_collection = None
        self.resume_collection = None
        
    def connect(self) -> bool:
        """Establish MongoDB connection"""
        try:
            self.client = MongoClient(self.connection_string, serverSelectionTimeoutMS=5000)
            self.client.server_info()  # Test connection
            
            self.db = self.client[self.database_name]
            self.source_collection = self.db[self.source_collection_name]
            self.details_collection = self.db[self.details_collection_name]
            self.resume_collection = self.db[self.resume_collection_name]
            
            # Create indexes
            self._create_indexes()
            
            logger.info(f"Connected to MongoDB: {self.database_name}")
            return True
            
        except ConnectionFailure as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            return False
            
    def _create_indexes(self):
        """Create necessary indexes for performance"""
        try:
            # Unique index on uid for details collection
            self.details_collection.create_index("uid", unique=True)
            
            # Index on scraping_status for finding pending/failed speakers
            self.details_collection.create_index("scraping_status")
            
            # Compound index for efficient queries
            self.details_collection.create_index([
                ("scraping_status", ASCENDING),
                ("scraped_at", ASCENDING)
            ])
            
            # Resume state index
            self.resume_collection.create_index("session_id", unique=True)
            
            logger.info("Created MongoDB indexes")
            
        except Exception as e:
            logger.warning(f"Error creating indexes: {e}")
            
    def get_speakers_to_scrape(self, limit: Optional[int] = None) -> List[Dict]:
        """Get speakers from source collection that need detailed scraping"""
        try:
            # Get all speakers from source
            query = {}
            cursor = self.source_collection.find(query)
            
            if limit:
                cursor = cursor.limit(limit)
                
            speakers = list(cursor)
            
            # Filter out already scraped speakers
            scraped_uids = set(self.get_scraped_speaker_uids())
            speakers_to_scrape = [s for s in speakers if s.get('uid') not in scraped_uids]
            
            logger.info(f"Found {len(speakers_to_scrape)} speakers to scrape (out of {len(speakers)} total)")
            return speakers_to_scrape
            
        except Exception as e:
            logger.error(f"Error getting speakers to scrape: {e}")
            return []
            
    def get_scraped_speaker_uids(self) -> List[str]:
        """Get list of UIDs that have been successfully scraped"""
        try:
            # Only consider successfully scraped speakers
            scraped = self.details_collection.find(
                {"scraping_status": "completed"},
                {"uid": 1}
            )
            return [s['uid'] for s in scraped]
            
        except Exception as e:
            logger.error(f"Error getting scraped UIDs: {e}")
            return []
            
    def get_failed_speakers(self, limit: Optional[int] = None) -> List[Dict]:
        """Get speakers that failed to scrape for retry"""
        try:
            query = {"scraping_status": "failed"}
            cursor = self.details_collection.find(query)
            
            if limit:
                cursor = cursor.limit(limit)
                
            return list(cursor)
            
        except Exception as e:
            logger.error(f"Error getting failed speakers: {e}")
            return []
            
    def save_speaker_details(self, speaker: DetailedSpeaker) -> bool:
        """Save or update detailed speaker information"""
        try:
            speaker_dict = speaker.to_dict()
            speaker_dict['last_updated'] = datetime.now()
            
            result = self.details_collection.update_one(
                {"uid": speaker.uid},
                {"$set": speaker_dict},
                upsert=True
            )
            
            if result.modified_count > 0 or result.upserted_id:
                logger.info(f"Saved details for speaker: {speaker.name} (UID: {speaker.uid})")
                return True
            else:
                logger.warning(f"No changes for speaker: {speaker.name} (UID: {speaker.uid})")
                return False
                
        except Exception as e:
            logger.error(f"Error saving speaker {speaker.uid}: {e}")
            return False
            
    def bulk_save_speakers(self, speakers: List[DetailedSpeaker]) -> int:
        """Bulk save multiple speakers"""
        if not speakers:
            return 0
            
        try:
            operations = []
            for speaker in speakers:
                speaker_dict = speaker.to_dict()
                speaker_dict['last_updated'] = datetime.now()
                
                operations.append(
                    UpdateOne(
                        {"uid": speaker.uid},
                        {"$set": speaker_dict},
                        upsert=True
                    )
                )
                
            result = self.details_collection.bulk_write(operations, ordered=False)
            
            saved_count = result.modified_count + len(result.upserted_ids)
            logger.info(f"Bulk saved {saved_count} speakers")
            return saved_count
            
        except BulkWriteError as e:
            logger.error(f"Bulk write error: {e.details}")
            return 0
            
    def mark_speaker_as_processing(self, uid: str) -> bool:
        """Mark a speaker as currently being processed"""
        try:
            self.details_collection.update_one(
                {"uid": uid},
                {
                    "$set": {
                        "scraping_status": "in_progress",
                        "last_updated": datetime.now()
                    }
                },
                upsert=True
            )
            return True
            
        except Exception as e:
            logger.error(f"Error marking speaker {uid} as processing: {e}")
            return False
            
    def mark_speaker_as_failed(self, uid: str, error_message: str) -> bool:
        """Mark a speaker as failed with error message"""
        try:
            self.details_collection.update_one(
                {"uid": uid},
                {
                    "$set": {
                        "scraping_status": "failed",
                        "error_message": error_message,
                        "last_updated": datetime.now()
                    }
                },
                upsert=True
            )
            return True
            
        except Exception as e:
            logger.error(f"Error marking speaker {uid} as failed: {e}")
            return False
            
    # Resume functionality
    def save_resume_state(self, session_id: str, state: Dict) -> bool:
        """Save the current scraping state for resume capability"""
        try:
            state['updated_at'] = datetime.now()
            
            self.resume_collection.update_one(
                {"session_id": session_id},
                {"$set": state},
                upsert=True
            )
            
            logger.info(f"Saved resume state for session {session_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error saving resume state: {e}")
            return False
            
    def get_resume_state(self, session_id: str) -> Optional[Dict]:
        """Get the resume state for a session"""
        try:
            state = self.resume_collection.find_one({"session_id": session_id})
            if state:
                logger.info(f"Found resume state for session {session_id}")
            return state
            
        except Exception as e:
            logger.error(f"Error getting resume state: {e}")
            return None
            
    def clear_resume_state(self, session_id: str) -> bool:
        """Clear the resume state after successful completion"""
        try:
            result = self.resume_collection.delete_one({"session_id": session_id})
            if result.deleted_count > 0:
                logger.info(f"Cleared resume state for session {session_id}")
                return True
            return False
            
        except Exception as e:
            logger.error(f"Error clearing resume state: {e}")
            return False
            
    def get_scraping_stats(self) -> Dict:
        """Get statistics about scraping progress"""
        try:
            total_source = self.source_collection.count_documents({})
            
            stats = {
                'total_speakers': total_source,
                'completed': self.details_collection.count_documents({"scraping_status": "completed"}),
                'in_progress': self.details_collection.count_documents({"scraping_status": "in_progress"}),
                'failed': self.details_collection.count_documents({"scraping_status": "failed"}),
                'pending': total_source - self.details_collection.count_documents({})
            }
            
            stats['completion_percentage'] = round(
                (stats['completed'] / total_source * 100) if total_source > 0 else 0, 
                2
            )
            
            return stats
            
        except Exception as e:
            logger.error(f"Error getting stats: {e}")
            return {}
            
    def close(self):
        """Close MongoDB connection"""
        if self.client:
            self.client.close()
            logger.info("Closed MongoDB connection")