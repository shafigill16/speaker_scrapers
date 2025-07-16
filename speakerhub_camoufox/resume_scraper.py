#!/usr/bin/env python3
"""
Resume scraping from a specific page number
"""

import sys
from pagination_scraper import PaginationScraper, MongoDBHandler
from config import MONGO_CONFIG
from datetime import datetime

def main():
    if len(sys.argv) > 1:
        start_page = int(sys.argv[1])
    else:
        # Calculate the page number based on the pattern (increment by 2)
        # Last successful was page 312, so next would be 314
        start_page = 314
    
    print(f"Resuming scraping from page {start_page}")
    print("="*60)
    
    # Initialize MongoDB
    mongo_handler = MongoDBHandler(
        MONGO_CONFIG['connection_string'],
        MONGO_CONFIG['database_name'],
        MONGO_CONFIG['collection_name']
    )
    
    if not mongo_handler.connect():
        print("Failed to connect to MongoDB")
        return
    
    # Create scraper instance
    scraper = PaginationScraper(mongo_handler)
    
    # Set the starting URL
    scraper.current_url = f"https://speakerhub.com/speakers?page={start_page}"
    scraper.page_num = start_page // 2  # Adjust page counter
    
    print(f"Starting from: {scraper.current_url}")
    print("Press Ctrl+C to stop at any time.\n")
    
    start_time = datetime.now()
    
    try:
        # Run the scraping loop
        total_saved, total_found = scraper.scrape_all_pages()
        
        duration = datetime.now() - start_time
        print(f"\nDuration: {duration}")
        print(f"New speakers found: {total_found}")
        
    except KeyboardInterrupt:
        print("\n\nScraping interrupted by user.")
    finally:
        mongo_handler.close()
        print("\nScraper finished.")

if __name__ == "__main__":
    main()