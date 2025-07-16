"""
Utility functions for SpeakerHub scraper
"""

import json
import csv
from datetime import datetime
from typing import List, Dict, Any
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


class DataExporter:
    """Export scraped data to various formats"""
    
    def __init__(self, output_dir: str = "exports"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
    
    def export_to_json(self, data: List[Dict[str, Any]], filename: str = None) -> str:
        """Export data to JSON file"""
        if filename is None:
            filename = f"speakers_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        filepath = self.output_dir / filename
        
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False, default=str)
            logger.info(f"Exported {len(data)} records to {filepath}")
            return str(filepath)
        except Exception as e:
            logger.error(f"Failed to export to JSON: {e}")
            return None
    
    def export_to_csv(self, data: List[Dict[str, Any]], filename: str = None) -> str:
        """Export data to CSV file"""
        if not data:
            logger.warning("No data to export")
            return None
        
        if filename is None:
            filename = f"speakers_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        filepath = self.output_dir / filename
        
        try:
            # Get all unique keys
            all_keys = set()
            for item in data:
                all_keys.update(item.keys())
            
            # Convert lists to strings for CSV
            processed_data = []
            for item in data:
                processed_item = {}
                for key, value in item.items():
                    if isinstance(value, list):
                        processed_item[key] = ', '.join(str(v) for v in value)
                    else:
                        processed_item[key] = value
                processed_data.append(processed_item)
            
            # Write CSV
            with open(filepath, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=sorted(all_keys))
                writer.writeheader()
                writer.writerows(processed_data)
            
            logger.info(f"Exported {len(data)} records to {filepath}")
            return str(filepath)
        except Exception as e:
            logger.error(f"Failed to export to CSV: {e}")
            return None
    
    def export_summary(self, data: List[Dict[str, Any]], filename: str = None) -> str:
        """Export a summary report of the data"""
        if filename is None:
            filename = f"summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        
        filepath = self.output_dir / filename
        
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write("SpeakerHub Scraping Summary\n")
                f.write("=" * 60 + "\n\n")
                f.write(f"Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"Total Speakers: {len(data)}\n\n")
                
                # Statistics
                if data:
                    # Countries
                    countries = {}
                    for speaker in data:
                        country = speaker.get('country', 'Unknown')
                        countries[country] = countries.get(country, 0) + 1
                    
                    f.write("Top Countries:\n")
                    for country, count in sorted(countries.items(), key=lambda x: x[1], reverse=True)[:10]:
                        f.write(f"  - {country}: {count}\n")
                    f.write("\n")
                    
                    # Languages
                    languages = {}
                    for speaker in data:
                        for lang in speaker.get('languages', []):
                            languages[lang] = languages.get(lang, 0) + 1
                    
                    f.write("Languages:\n")
                    for lang, count in sorted(languages.items(), key=lambda x: x[1], reverse=True):
                        f.write(f"  - {lang}: {count}\n")
                    f.write("\n")
                    
                    # Event Types
                    event_types = {}
                    for speaker in data:
                        for event_type in speaker.get('event_types', []):
                            event_types[event_type] = event_types.get(event_type, 0) + 1
                    
                    f.write("Event Types:\n")
                    for event_type, count in sorted(event_types.items(), key=lambda x: x[1], reverse=True):
                        f.write(f"  - {event_type}: {count}\n")
                    f.write("\n")
                    
                    # Sample speakers
                    f.write("Sample Speakers:\n")
                    f.write("-" * 60 + "\n")
                    for i, speaker in enumerate(data[:20], 1):
                        f.write(f"\n{i}. {speaker.get('name', 'Unknown')}\n")
                        if speaker.get('job_title'):
                            f.write(f"   Title: {speaker['job_title']}\n")
                        if speaker.get('company'):
                            f.write(f"   Company: {speaker['company']}\n")
                        if speaker.get('city') or speaker.get('country'):
                            location = []
                            if speaker.get('city'):
                                location.append(speaker['city'])
                            if speaker.get('state'):
                                location.append(speaker['state'])
                            if speaker.get('country'):
                                location.append(speaker['country'])
                            f.write(f"   Location: {', '.join(location)}\n")
                        if speaker.get('topics'):
                            f.write(f"   Topics: {', '.join(speaker['topics'][:5])}\n")
            
            logger.info(f"Exported summary to {filepath}")
            return str(filepath)
        except Exception as e:
            logger.error(f"Failed to export summary: {e}")
            return None


class DataValidator:
    """Validate and clean speaker data"""
    
    @staticmethod
    def validate_speaker(speaker_data: Dict[str, Any]) -> bool:
        """Validate that speaker has minimum required fields"""
        required_fields = ['uid', 'name', 'profile_url']
        
        for field in required_fields:
            if field not in speaker_data or not speaker_data[field]:
                logger.warning(f"Speaker missing required field: {field}")
                return False
        
        return True
    
    @staticmethod
    def clean_speaker_data(speaker_data: Dict[str, Any]) -> Dict[str, Any]:
        """Clean and normalize speaker data"""
        # Remove None values
        cleaned = {k: v for k, v in speaker_data.items() if v is not None}
        
        # Ensure lists are not None
        list_fields = ['available_regions', 'languages', 'event_types', 'topics']
        for field in list_fields:
            if field not in cleaned or cleaned[field] is None:
                cleaned[field] = []
        
        # Strip whitespace from strings
        for key, value in cleaned.items():
            if isinstance(value, str):
                cleaned[key] = value.strip()
            elif isinstance(value, list):
                cleaned[key] = [v.strip() if isinstance(v, str) else v for v in value]
        
        return cleaned


class ScraperStats:
    """Track and report scraping statistics"""
    
    def __init__(self):
        self.start_time = None
        self.end_time = None
        self.total_scraped = 0
        self.total_saved = 0
        self.errors = []
    
    def start(self):
        """Start timing"""
        self.start_time = datetime.now()
    
    def end(self):
        """End timing"""
        self.end_time = datetime.now()
    
    def add_error(self, error: str):
        """Add an error to the list"""
        self.errors.append({
            'timestamp': datetime.now(),
            'error': error
        })
    
    def get_duration(self) -> str:
        """Get formatted duration"""
        if not self.start_time or not self.end_time:
            return "Unknown"
        
        duration = self.end_time - self.start_time
        hours, remainder = divmod(duration.seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        
        if hours:
            return f"{hours}h {minutes}m {seconds}s"
        elif minutes:
            return f"{minutes}m {seconds}s"
        else:
            return f"{seconds}s"
    
    def print_summary(self):
        """Print statistics summary"""
        print("\n" + "="*60)
        print("Scraping Statistics")
        print("="*60)
        print(f"Duration: {self.get_duration()}")
        print(f"Total Scraped: {self.total_scraped}")
        print(f"Total Saved: {self.total_saved}")
        print(f"Success Rate: {(self.total_saved/self.total_scraped*100):.1f}%" if self.total_scraped > 0 else "N/A")
        print(f"Errors: {len(self.errors)}")
        
        if self.errors:
            print("\nRecent Errors:")
            for error in self.errors[-5:]:
                print(f"  - {error['timestamp'].strftime('%H:%M:%S')}: {error['error'][:80]}...")


def test_mongodb_connection(connection_string: str) -> bool:
    """Test MongoDB connection"""
    from pymongo import MongoClient
    
    try:
        client = MongoClient(connection_string, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        client.close()
        print("✅ MongoDB connection successful")
        return True
    except Exception as e:
        print(f"❌ MongoDB connection failed: {e}")
        return False