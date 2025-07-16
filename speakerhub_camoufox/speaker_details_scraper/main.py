#!/usr/bin/env python3
"""
Main entry point for speaker details scraper
"""

import argparse
import sys
from datetime import datetime

from scraper import SpeakerDetailsScraper
from utils import DataExporter, ProgressMonitor, test_connection
from database import SpeakerDetailsDB
from config import MONGO_CONFIG


def main():
    """Main entry point with CLI interface"""
    parser = argparse.ArgumentParser(
        description='SpeakerHub Details Scraper - Extract comprehensive speaker information',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Test database connection
  python main.py --test
  
  # Start scraping (with auto-resume)
  python main.py --scrape
  
  # Start fresh scraping (ignore previous state)
  python main.py --scrape --no-resume
  
  # Scrape only 10 speakers
  python main.py --scrape --limit 10
  
  # Monitor progress in real-time
  python main.py --monitor
  
  # Export all data
  python main.py --export
  
  # Show statistics
  python main.py --stats
  
  # Retry failed speakers
  python main.py --retry-failed
        """
    )
    
    # Actions
    parser.add_argument('--test', action='store_true', 
                       help='Test MongoDB connection')
    parser.add_argument('--scrape', action='store_true', 
                       help='Start scraping speaker details')
    parser.add_argument('--monitor', action='store_true', 
                       help='Monitor scraping progress in real-time')
    parser.add_argument('--export', action='store_true', 
                       help='Export scraped data')
    parser.add_argument('--stats', action='store_true', 
                       help='Show scraping statistics')
    parser.add_argument('--retry-failed', action='store_true', 
                       help='Retry speakers that failed to scrape')
    
    # Options
    parser.add_argument('--no-resume', action='store_true', 
                       help='Start fresh without resuming previous session')
    parser.add_argument('--limit', type=int, 
                       help='Limit number of speakers to scrape')
    parser.add_argument('--session-id', 
                       help='Custom session ID for tracking')
    parser.add_argument('--include-failed', action='store_true', 
                       help='Include failed speakers in export')
    
    args = parser.parse_args()
    
    # Validate that at least one action is specified
    actions = [args.test, args.scrape, args.monitor, args.export, args.stats, args.retry_failed]
    if not any(actions):
        parser.print_help()
        sys.exit(1)
        
    # Execute requested action
    if args.test:
        print("\n🔍 Testing MongoDB connection...")
        success = test_connection()
        sys.exit(0 if success else 1)
        
    elif args.scrape:
        print("\n🚀 Starting speaker details scraper...")
        print(f"Session started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        scraper = SpeakerDetailsScraper(session_id=args.session_id)
        scraper.run(resume=not args.no_resume, limit=args.limit)
        
    elif args.monitor:
        print("\n📊 Starting progress monitor...")
        print("Press Ctrl+C to exit\n")
        
        monitor = ProgressMonitor()
        monitor.show_progress()
        
    elif args.export:
        print("\n📤 Exporting speaker data...")
        
        exporter = DataExporter()
        exporter.export_all(include_failed=args.include_failed)
        print("✓ Export completed")
        
    elif args.stats:
        print("\n📈 Scraping Statistics")
        print("="*50)
        
        db = SpeakerDetailsDB(
            MONGO_CONFIG['connection_string'],
            MONGO_CONFIG['database_name'],
            MONGO_CONFIG['source_collection'],
            MONGO_CONFIG['details_collection'],
            MONGO_CONFIG['resume_collection']
        )
        
        if db.connect():
            stats = db.get_scraping_stats()
            
            print(f"Total speakers in source: {stats.get('total_speakers', 0)}")
            print(f"Successfully scraped: {stats.get('completed', 0)}")
            print(f"Failed: {stats.get('failed', 0)}")
            print(f"In progress: {stats.get('in_progress', 0)}")
            print(f"Pending: {stats.get('pending', 0)}")
            print(f"Completion rate: {stats.get('completion_percentage', 0)}%")
            
            # Get some sample data
            print("\n📋 Sample of completed speakers:")
            samples = list(db.details_collection.find(
                {"scraping_status": "completed"},
                {"name": 1, "country": 1, "topics": 1}
            ).limit(5))
            
            for speaker in samples:
                topics_count = len(speaker.get('topics', []))
                print(f"  - {speaker.get('name', 'Unknown'):30} | {speaker.get('country', 'Unknown'):20} | {topics_count} topics")
                
            db.close()
            
    elif args.retry_failed:
        print("\n🔄 Retrying failed speakers...")
        
        db = SpeakerDetailsDB(
            MONGO_CONFIG['connection_string'],
            MONGO_CONFIG['database_name'],
            MONGO_CONFIG['source_collection'],
            MONGO_CONFIG['details_collection'],
            MONGO_CONFIG['resume_collection']
        )
        
        if db.connect():
            failed_speakers = db.get_failed_speakers(limit=args.limit)
            
            if failed_speakers:
                print(f"Found {len(failed_speakers)} failed speakers to retry")
                
                # Reset their status to pending
                for speaker in failed_speakers:
                    db.details_collection.update_one(
                        {"uid": speaker['uid']},
                        {"$set": {"scraping_status": "pending", "error_message": None}}
                    )
                    
                db.close()
                
                # Run scraper
                scraper = SpeakerDetailsScraper(session_id=args.session_id)
                scraper.run(resume=True, limit=args.limit)
            else:
                print("No failed speakers found")
                db.close()


if __name__ == "__main__":
    main()