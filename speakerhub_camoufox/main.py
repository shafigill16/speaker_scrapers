#!/usr/bin/env python3
"""
Main entry point for SpeakerHub Scraper
"""

import argparse
import sys
import logging
from datetime import datetime

from speakerhub_scraper import SpeakerHubScraper, MongoDBHandler
from utils import DataExporter, test_mongodb_connection, ScraperStats
from config import MONGO_CONFIG, SCRAPER_CONFIG, LOGGING_CONFIG


def setup_logging():
    """Setup logging configuration"""
    logging.basicConfig(
        level=getattr(logging, LOGGING_CONFIG['level']),
        format=LOGGING_CONFIG['format'],
        handlers=[
            logging.FileHandler(LOGGING_CONFIG['filename']),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)


def export_from_mongodb(mongo_handler: MongoDBHandler, export_format: str = 'all'):
    """Export data from MongoDB to files"""
    logger = logging.getLogger(__name__)
    
    try:
        # Get all speakers from MongoDB
        speakers = list(mongo_handler.collection.find({}, {'_id': 0}))
        
        if not speakers:
            logger.warning("No speakers found in database")
            return
        
        logger.info(f"Found {len(speakers)} speakers in database")
        
        # Initialize exporter
        exporter = DataExporter()
        
        # Export based on format
        if export_format in ['json', 'all']:
            json_file = exporter.export_to_json(speakers)
            if json_file:
                print(f"✅ Exported to JSON: {json_file}")
        
        if export_format in ['csv', 'all']:
            csv_file = exporter.export_to_csv(speakers)
            if csv_file:
                print(f"✅ Exported to CSV: {csv_file}")
        
        if export_format in ['summary', 'all']:
            summary_file = exporter.export_summary(speakers)
            if summary_file:
                print(f"✅ Exported summary: {summary_file}")
                
    except Exception as e:
        logger.error(f"Export failed: {e}")


def main():
    """Main function"""
    parser = argparse.ArgumentParser(description='SpeakerHub Scraper with MongoDB Integration')
    parser.add_argument('--test', action='store_true', help='Test MongoDB connection')
    parser.add_argument('--export', choices=['json', 'csv', 'summary', 'all'], 
                       help='Export data from MongoDB')
    parser.add_argument('--max-scrolls', type=int, default=SCRAPER_CONFIG['max_scroll_attempts'],
                       help='Maximum number of scrolls (default: 50)')
    parser.add_argument('--batch-size', type=int, default=SCRAPER_CONFIG['batch_size'],
                       help='Batch size for MongoDB inserts (default: 50)')
    
    args = parser.parse_args()
    
    # Setup logging
    logger = setup_logging()
    
    print("="*60)
    print("SpeakerHub Scraper")
    print("="*60)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # Test MongoDB connection
    if args.test:
        print("Testing MongoDB connection...")
        if test_mongodb_connection(MONGO_CONFIG['connection_string']):
            print("Connection test passed!")
        else:
            print("Connection test failed!")
        return
    
    # Initialize MongoDB handler
    mongo_handler = MongoDBHandler(
        MONGO_CONFIG['connection_string'],
        MONGO_CONFIG['database_name'],
        MONGO_CONFIG['collection_name']
    )
    
    # Connect to MongoDB
    if not mongo_handler.connect():
        print("❌ Failed to connect to MongoDB. Exiting.")
        sys.exit(1)
    
    # Export mode
    if args.export:
        print(f"\nExporting data in {args.export} format...")
        export_from_mongodb(mongo_handler, args.export)
        mongo_handler.close()
        return
    
    # Scraping mode
    try:
        # Get initial count
        initial_count = mongo_handler.get_speaker_count()
        print(f"Current speakers in database: {initial_count}")
        
        # Update config with command line arguments
        SCRAPER_CONFIG['max_scroll_attempts'] = args.max_scrolls
        SCRAPER_CONFIG['batch_size'] = args.batch_size
        
        # Initialize scraper
        scraper = SpeakerHubScraper(
            mongo_handler, 
            max_scroll_attempts=args.max_scrolls,
            no_content_threshold=SCRAPER_CONFIG['no_content_threshold']
        )
        
        # Initialize statistics
        stats = ScraperStats()
        stats.start()
        
        print(f"\nStarting scraper with max {args.max_scrolls} scrolls...")
        print("Press Ctrl+C to stop at any time.\n")
        
        # Run scraper
        total_scraped = scraper.scrape_all_speakers()
        
        # Update statistics
        stats.end()
        stats.total_scraped = len(scraper.scraped_uids)
        stats.total_saved = total_scraped
        
        # Get final count
        final_count = mongo_handler.get_speaker_count()
        
        # Print summary
        print("\n" + "="*60)
        print("Scraping Complete!")
        print("="*60)
        print(f"Initial database count: {initial_count}")
        print(f"Final database count: {final_count}")
        print(f"New speakers added: {final_count - initial_count}")
        
        stats.print_summary()
        
        # Ask if user wants to export (skip in non-interactive mode)
        if final_count > 0 and sys.stdin.isatty():
            try:
                export_choice = input("\nExport data? (y/n): ").lower()
                if export_choice == 'y':
                    export_from_mongodb(mongo_handler, 'all')
            except EOFError:
                print("\nSkipping export prompt (non-interactive mode)")
        
    except KeyboardInterrupt:
        print("\n\n⚠️ Scraping interrupted by user.")
        logger.info("Scraping interrupted by user")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        logger.error(f"Scraping failed: {e}", exc_info=True)
    finally:
        mongo_handler.close()
        print("\n✅ Cleanup complete.")


if __name__ == "__main__":
    main()