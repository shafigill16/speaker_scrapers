#!/usr/bin/env python3
"""
Main script to run the Free Speaker Bureau scraper
"""
import sys
import argparse
from enhanced_mongodb_scraper import EnhancedSpeakerScraper, main as scraper_main
from mongodb_utils import MongoDBManager
import logging

def run_scraper(args):
    """Run the scraper with specified options"""
    try:
        scraper = EnhancedSpeakerScraper()
        
        print(f"\nStarting Free Speaker Bureau Scraper")
        print(f"{'='*60}")
        print(f"MongoDB: Connected to {scraper.db.name}")
        print(f"Collection: {scraper.collection.name}")
        print(f"Proxy: Configured")
        print(f"Limit: {args.limit if args.limit else 'No limit (scrape all)'}")
        print(f"Batch Size: {args.batch_size}")
        print(f"Max Workers: {args.workers}")
        print(f"{'='*60}\n")
        
        # Run scraper
        speakers = scraper.scrape_all(
            limit=args.limit,
            batch_size=args.batch_size
        )
        
        print(f"\n✓ Scraping completed successfully!")
        
        # Export sample if requested
        if args.export_sample:
            filename = scraper.export_sample(limit=args.export_sample)
            print(f"✓ Exported {args.export_sample} sample records to {filename}")
        
        scraper.close()
        
    except Exception as e:
        logging.error(f"Error running scraper: {e}")
        sys.exit(1)

def check_database(args):
    """Check database status and statistics"""
    try:
        manager = MongoDBManager()
        
        print("\nMongoDB Database Status")
        print("="*60)
        
        if not manager.test_connection():
            sys.exit(1)
        
        stats = manager.get_statistics()
        if stats:
            print(f"\nDatabase Statistics:")
            print(f"  Total speakers: {stats['total_speakers']}")
            print(f"  With email: {stats['speakers_with_email']}")
            print(f"  With phone: {stats['speakers_with_phone']}")
            print(f"  With website: {stats['speakers_with_website']}")
            print(f"  With social media: {stats['speakers_with_social']}")
            
            if stats.get('top_locations'):
                print("\n  Top Locations:")
                for location, count in list(stats['top_locations'].items())[:5]:
                    print(f"    {location}: {count}")
            
            if stats.get('member_levels'):
                print("\n  Member Levels:")
                for level, count in stats['member_levels'].items():
                    print(f"    {level}: {count}")
        
        manager.close()
        
    except Exception as e:
        logging.error(f"Error checking database: {e}")
        sys.exit(1)

def export_data(args):
    """Export data from MongoDB"""
    try:
        manager = MongoDBManager()
        
        print("\nExporting data from MongoDB...")
        
        query = {}
        if args.filter_location:
            query['location'] = {'$regex': args.filter_location, '$options': 'i'}
        if args.filter_topic:
            query['specialties'] = {'$in': [args.filter_topic]}
        
        filename = manager.export_to_json(query=query)
        if filename:
            print(f"✓ Data exported successfully to {filename}")
        
        manager.close()
        
    except Exception as e:
        logging.error(f"Error exporting data: {e}")
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser(
        description='Free Speaker Bureau Scraper with MongoDB Integration',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Scrape first 100 speakers
  python run_scraper.py scrape --limit 100
  
  # Scrape all speakers with custom batch size
  python run_scraper.py scrape --batch-size 20
  
  # Check database status
  python run_scraper.py check
  
  # Export all data
  python run_scraper.py export
  
  # Export speakers from California
  python run_scraper.py export --filter-location California
        """
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Scrape command
    scrape_parser = subparsers.add_parser('scrape', help='Scrape speaker profiles')
    scrape_parser.add_argument('--limit', type=int, help='Limit number of speakers to scrape')
    scrape_parser.add_argument('--batch-size', type=int, default=10, help='Number of profiles per batch (default: 10)')
    scrape_parser.add_argument('--workers', type=int, default=5, help='Number of parallel workers (default: 5)')
    scrape_parser.add_argument('--export-sample', type=int, help='Export N sample records after scraping')
    
    # Check command
    check_parser = subparsers.add_parser('check', help='Check database status and statistics')
    
    # Export command
    export_parser = subparsers.add_parser('export', help='Export data from MongoDB to JSON')
    export_parser.add_argument('--filter-location', help='Filter by location (e.g., California)')
    export_parser.add_argument('--filter-topic', help='Filter by speaking topic')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        sys.exit(1)
    
    if args.command == 'scrape':
        run_scraper(args)
    elif args.command == 'check':
        check_database(args)
    elif args.command == 'export':
        export_data(args)

if __name__ == '__main__':
    main()