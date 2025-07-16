#!/usr/bin/env python3
"""
Utility functions for speaker details scraper
"""

import json
import csv
import os
from datetime import datetime
from typing import List, Dict, Optional
import logging

from config import EXPORT_CONFIG, MONGO_CONFIG
from database import SpeakerDetailsDB

logger = logging.getLogger(__name__)


class DataExporter:
    """Export detailed speaker data to various formats"""
    
    def __init__(self):
        self.db = SpeakerDetailsDB(
            MONGO_CONFIG['connection_string'],
            MONGO_CONFIG['database_name'],
            MONGO_CONFIG['source_collection'],
            MONGO_CONFIG['details_collection'],
            MONGO_CONFIG['resume_collection']
        )
        
    def export_all(self, include_failed: bool = False):
        """Export data in all configured formats"""
        if not self.db.connect():
            logger.error("Failed to connect to database")
            return
            
        try:
            # Create export directory
            os.makedirs(EXPORT_CONFIG['export_dir'], exist_ok=True)
            
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            
            for format_type in EXPORT_CONFIG['formats']:
                if format_type == 'json':
                    self.export_json(timestamp, include_failed)
                elif format_type == 'csv':
                    self.export_csv(timestamp, include_failed)
                    
            # Always export summary
            self.export_summary(timestamp)
            
        finally:
            self.db.close()
            
    def export_json(self, timestamp: str, include_failed: bool = False):
        """Export to JSON format"""
        try:
            # Get speakers
            query = {} if include_failed else {"scraping_status": "completed"}
            speakers = list(self.db.details_collection.find(query))
            
            # Convert ObjectId to string
            for speaker in speakers:
                if '_id' in speaker:
                    speaker['_id'] = str(speaker['_id'])
                    
            # Save to file
            filename = f"{EXPORT_CONFIG['export_dir']}/speaker_details_{timestamp}.json"
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(speakers, f, ensure_ascii=False, indent=2, default=str)
                
            logger.info(f"Exported {len(speakers)} speakers to {filename}")
            
        except Exception as e:
            logger.error(f"Error exporting JSON: {e}")
            
    def export_csv(self, timestamp: str, include_failed: bool = False):
        """Export to CSV format"""
        try:
            # Get speakers
            query = {} if include_failed else {"scraping_status": "completed"}
            speakers = list(self.db.details_collection.find(query))
            
            if not speakers:
                logger.warning("No speakers to export")
                return
                
            # Flatten nested data for CSV
            flattened_speakers = []
            for speaker in speakers:
                flat = self._flatten_speaker(speaker)
                flattened_speakers.append(flat)
                
            # Get all unique fields
            all_fields = set()
            for speaker in flattened_speakers:
                all_fields.update(speaker.keys())
                
            # Sort fields for consistent ordering
            fieldnames = sorted(all_fields)
            
            # Save to file
            filename = f"{EXPORT_CONFIG['export_dir']}/speaker_details_{timestamp}.csv"
            with open(filename, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(flattened_speakers)
                
            logger.info(f"Exported {len(speakers)} speakers to {filename}")
            
        except Exception as e:
            logger.error(f"Error exporting CSV: {e}")
            
    def export_summary(self, timestamp: str):
        """Export summary statistics"""
        try:
            stats = self.db.get_scraping_stats()
            
            # Get additional statistics
            completed_speakers = list(self.db.details_collection.find(
                {"scraping_status": "completed"},
                {"name": 1, "country": 1, "topics": 1, "fee_range": 1}
            ))
            
            # Country distribution
            country_dist = {}
            for speaker in completed_speakers:
                country = speaker.get('country', 'Unknown')
                country_dist[country] = country_dist.get(country, 0) + 1
                
            # Sort countries by count
            sorted_countries = sorted(country_dist.items(), key=lambda x: x[1], reverse=True)
            
            # Topic distribution
            topic_dist = {}
            for speaker in completed_speakers:
                for topic in speaker.get('topics', []):
                    topic_dist[topic] = topic_dist.get(topic, 0) + 1
                    
            # Sort topics by count
            sorted_topics = sorted(topic_dist.items(), key=lambda x: x[1], reverse=True)[:20]
            
            # Fee range distribution
            fee_dist = {}
            for speaker in completed_speakers:
                fee_range = speaker.get('fee_range', 'Unknown')
                fee_dist[fee_range] = fee_dist.get(fee_range, 0) + 1
                
            # Create summary
            summary = f"""Speaker Details Scraping Summary
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
{'='*50}

OVERALL STATISTICS:
- Total speakers in source: {stats.get('total_speakers', 0)}
- Successfully scraped: {stats.get('completed', 0)}
- Failed: {stats.get('failed', 0)}
- Pending: {stats.get('pending', 0)}
- Completion rate: {stats.get('completion_percentage', 0)}%

COUNTRY DISTRIBUTION (Top 20):
"""
            for country, count in sorted_countries[:20]:
                summary += f"  - {country}: {count}\n"
                
            summary += f"\nTOPIC DISTRIBUTION (Top 20):\n"
            for topic, count in sorted_topics:
                summary += f"  - {topic}: {count}\n"
                
            summary += f"\nFEE RANGE DISTRIBUTION:\n"
            for fee_range, count in sorted(fee_dist.items()):
                summary += f"  - {fee_range}: {count}\n"
                
            # Save to file
            filename = f"{EXPORT_CONFIG['export_dir']}/speaker_details_summary_{timestamp}.txt"
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(summary)
                
            logger.info(f"Exported summary to {filename}")
            
        except Exception as e:
            logger.error(f"Error exporting summary: {e}")
            
    def _flatten_speaker(self, speaker: Dict) -> Dict:
        """Flatten nested speaker data for CSV export"""
        flat = {
            'uid': speaker.get('uid'),
            'name': speaker.get('name'),
            'profile_url': speaker.get('profile_url'),
            'professional_title': speaker.get('professional_title'),
            'job_title': speaker.get('job_title'),
            'company': speaker.get('company'),
            'pronouns': speaker.get('pronouns'),
            'country': speaker.get('country'),
            'state_province': speaker.get('state_province'),
            'city': speaker.get('city'),
            'timezone': speaker.get('timezone'),
            'website': speaker.get('website'),
            'linkedin_url': speaker.get('linkedin_url'),
            'twitter_url': speaker.get('twitter_url'),
            'fee_range': speaker.get('fee_range'),
            'bio_summary': speaker.get('bio_summary'),
            'why_choose_me': speaker.get('why_choose_me'),
            'recommendations_count': speaker.get('recommendations_count'),
            'rating': speaker.get('rating'),
            'profile_picture_url': speaker.get('profile_picture_url'),
            'scraping_status': speaker.get('scraping_status'),
            'scraped_at': speaker.get('scraped_at'),
            'last_updated': speaker.get('last_updated')
        }
        
        # Convert lists to comma-separated strings
        flat['languages'] = ', '.join(speaker.get('languages', []))
        flat['event_types'] = ', '.join(speaker.get('event_types', []))
        flat['topics'] = ', '.join(speaker.get('topics', []))
        flat['topic_categories'] = ', '.join(speaker.get('topic_categories', []))
        flat['affiliations'] = ', '.join(speaker.get('affiliations', []))
        flat['certifications'] = ', '.join(speaker.get('certifications', []))
        flat['awards'] = ', '.join(speaker.get('awards', []))
        
        # Count nested items
        flat['total_past_talks'] = len(speaker.get('past_talks', []))
        flat['total_education'] = len(speaker.get('education', []))
        flat['total_publications'] = len(speaker.get('publications', []))
        flat['total_presentations'] = len(speaker.get('presentations', []))
        flat['total_workshops'] = len(speaker.get('workshops', []))
        flat['total_testimonials'] = len(speaker.get('testimonials', []))
        flat['total_videos'] = len(speaker.get('videos', []))
        
        # Speaker fees summary
        fees = speaker.get('speaker_fees', [])
        if fees:
            fee_summary = []
            for fee in fees:
                if fee.get('fee'):
                    fee_summary.append(f"{fee['event_type']}: {fee['fee']}")
            flat['speaker_fees_summary'] = ' | '.join(fee_summary)
        else:
            flat['speaker_fees_summary'] = ''
            
        return flat


class ProgressMonitor:
    """Monitor scraping progress in real-time"""
    
    def __init__(self):
        self.db = SpeakerDetailsDB(
            MONGO_CONFIG['connection_string'],
            MONGO_CONFIG['database_name'],
            MONGO_CONFIG['source_collection'],
            MONGO_CONFIG['details_collection'],
            MONGO_CONFIG['resume_collection']
        )
        
    def show_progress(self):
        """Display current scraping progress"""
        if not self.db.connect():
            logger.error("Failed to connect to database")
            return
            
        try:
            while True:
                # Clear screen
                os.system('cls' if os.name == 'nt' else 'clear')
                
                # Get stats
                stats = self.db.get_scraping_stats()
                
                # Get recent speakers
                recent = list(self.db.details_collection.find(
                    {"scraping_status": "completed"},
                    {"name": 1, "country": 1, "scraped_at": 1}
                ).sort("scraped_at", -1).limit(10))
                
                # Display
                print(f"SPEAKER DETAILS SCRAPING MONITOR")
                print(f"Last Update: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                print("="*60)
                print(f"Total Speakers: {stats.get('total_speakers', 0)}")
                print(f"Completed: {stats.get('completed', 0)} ({stats.get('completion_percentage', 0)}%)")
                print(f"In Progress: {stats.get('in_progress', 0)}")
                print(f"Failed: {stats.get('failed', 0)}")
                print(f"Pending: {stats.get('pending', 0)}")
                print("\nRecent Completions:")
                print("-"*60)
                
                for speaker in recent:
                    scraped_time = speaker.get('scraped_at', '')
                    if isinstance(scraped_time, datetime):
                        scraped_time = scraped_time.strftime('%H:%M:%S')
                    print(f"  - {speaker.get('name', 'Unknown'):30} ({speaker.get('country', 'Unknown'):20}) at {scraped_time}")
                    
                print("\nPress Ctrl+C to exit")
                
                # Wait before refresh
                time.sleep(5)
                
        except KeyboardInterrupt:
            print("\nMonitoring stopped")
            
        finally:
            self.db.close()


def test_connection():
    """Test MongoDB connection and show basic stats"""
    db = SpeakerDetailsDB(
        MONGO_CONFIG['connection_string'],
        MONGO_CONFIG['database_name'],
        MONGO_CONFIG['source_collection'],
        MONGO_CONFIG['details_collection'],
        MONGO_CONFIG['resume_collection']
    )
    
    if db.connect():
        print("✓ Successfully connected to MongoDB")
        
        # Show stats
        stats = db.get_scraping_stats()
        print(f"\nDatabase Statistics:")
        print(f"  - Total speakers in source: {stats.get('total_speakers', 0)}")
        print(f"  - Completed: {stats.get('completed', 0)}")
        print(f"  - Failed: {stats.get('failed', 0)}")
        print(f"  - Pending: {stats.get('pending', 0)}")
        
        db.close()
        return True
    else:
        print("✗ Failed to connect to MongoDB")
        return False


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        command = sys.argv[1]
        
        if command == "test":
            test_connection()
        elif command == "export":
            exporter = DataExporter()
            exporter.export_all()
        elif command == "monitor":
            monitor = ProgressMonitor()
            monitor.show_progress()
        else:
            print(f"Unknown command: {command}")
            print("Available commands: test, export, monitor")
    else:
        print("Usage: python utils.py [test|export|monitor]")