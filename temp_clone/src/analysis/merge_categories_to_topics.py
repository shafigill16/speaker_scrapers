#!/usr/bin/env python3
"""
Merge Categories into Topics

This script:
1. Merges the 'categories' field into 'topics' field
2. Ensures unique topics per speaker
3. Removes the categories field after merging
4. Updates the database with the new comprehensive topic mapping
"""

import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

import json
from pymongo import MongoClient, UpdateOne
from collections import Counter

MONGO_URI = os.getenv("MONGO_URI")
if not MONGO_URI:
    raise ValueError("MONGO_URI environment variable is required. Please set it in .env file")
TARGET_DB = os.getenv("TARGET_DATABASE", "speaker_database")
COLLECTION = os.getenv("COLLECTION", "unified_speakers_v3")

def load_comprehensive_mapping():
    """Load the comprehensive topic mapping"""
    try:
        with open("topic_mapping_comprehensive.json", "r", encoding="utf-8") as f:
            return json.load(f)
    except:
        print("Error: topic_mapping_comprehensive.json not found!")
        return None

def merge_categories_and_topics(collection, topic_mapping):
    """Merge categories into topics and apply new mapping"""
    
    print("Starting merge process...")
    
    # Track statistics
    stats = {
        "total_processed": 0,
        "categories_merged": 0,
        "topics_mapped": 0,
        "topics_before": Counter(),
        "topics_after": Counter()
    }
    
    # Process in batches
    batch_size = 1000
    bulk_ops = []
    
    # Create reverse mapping for quick lookup
    reverse_mapping = {}
    for canonical, variations in topic_mapping.items():
        for variation in variations:
            reverse_mapping[variation.lower()] = canonical
    
    cursor = collection.find({})
    
    for doc in cursor:
        stats["total_processed"] += 1
        
        # Get existing topics and categories
        topics = doc.get("topics", [])
        categories = doc.get("categories", [])
        unmapped = doc.get("topics_unmapped", [])
        
        # Track before state
        for t in topics:
            stats["topics_before"][t] += 1
        
        # Merge categories into topics
        all_topics = set(topics)
        if categories:
            all_topics.update(categories)
            stats["categories_merged"] += 1
        
        # Apply comprehensive mapping
        mapped_topics = set()
        new_unmapped = []
        
        for topic in all_topics:
            if topic:  # Skip empty topics
                topic_lower = topic.lower().strip()
                
                # Check if already canonical
                if topic in topic_mapping:
                    mapped_topics.add(topic)
                    stats["topics_mapped"] += 1
                # Check reverse mapping
                elif topic_lower in reverse_mapping:
                    mapped_topics.add(reverse_mapping[topic_lower])
                    stats["topics_mapped"] += 1
                else:
                    # Still unmapped
                    new_unmapped.append(topic)
        
        # Also process existing unmapped topics
        for topic in unmapped:
            if topic:
                topic_lower = topic.lower().strip()
                if topic_lower in reverse_mapping:
                    mapped_topics.add(reverse_mapping[topic_lower])
                    stats["topics_mapped"] += 1
                else:
                    new_unmapped.append(topic)
        
        # Track after state
        for t in mapped_topics:
            stats["topics_after"][t] += 1
        
        # Create update operation
        update = {
            "$set": {
                "topics": sorted(list(mapped_topics)),
                "topics_unmapped": sorted(list(set(new_unmapped)))
            }
        }
        
        # Remove categories field if it exists
        if "categories" in doc:
            update["$unset"] = {"categories": ""}
        
        bulk_ops.append(UpdateOne({"_id": doc["_id"]}, update))
        
        # Execute batch
        if len(bulk_ops) >= batch_size:
            collection.bulk_write(bulk_ops, ordered=False)
            print(f"  Processed {stats['total_processed']:,} documents...")
            bulk_ops = []
    
    # Execute remaining operations
    if bulk_ops:
        collection.bulk_write(bulk_ops, ordered=False)
    
    return stats

def generate_merge_report(stats, topic_mapping):
    """Generate a report about the merge operation"""
    
    report = []
    report.append("# Categories to Topics Merge Report")
    report.append(f"\n## Summary")
    report.append(f"- Total documents processed: {stats['total_processed']:,}")
    report.append(f"- Documents with categories merged: {stats['categories_merged']:,}")
    report.append(f"- Total topic mappings applied: {stats['topics_mapped']:,}")
    report.append(f"- Canonical topics in mapping: {len(topic_mapping)}")
    
    # Top topics before merge
    report.append(f"\n## Top 20 Topics Before Merge")
    report.append("| Topic | Count |")
    report.append("|-------|-------|")
    
    for topic, count in stats["topics_before"].most_common(20):
        report.append(f"| {topic} | {count:,} |")
    
    # Top topics after merge
    report.append(f"\n## Top 20 Topics After Merge")
    report.append("| Topic | Count |")
    report.append("|-------|-------|")
    
    for topic, count in stats["topics_after"].most_common(20):
        report.append(f"| {topic} | {count:,} |")
    
    # Topics with biggest increase
    report.append(f"\n## Topics with Biggest Increase")
    report.append("| Topic | Before | After | Increase |")
    report.append("|-------|--------|-------|----------|")
    
    increases = []
    for topic, after_count in stats["topics_after"].items():
        before_count = stats["topics_before"].get(topic, 0)
        increase = after_count - before_count
        if increase > 0:
            increases.append((topic, before_count, after_count, increase))
    
    for topic, before, after, increase in sorted(increases, key=lambda x: x[3], reverse=True)[:20]:
        report.append(f"| {topic} | {before:,} | {after:,} | +{increase:,} |")
    
    # Canonical topics distribution
    report.append(f"\n## Canonical Topics Distribution")
    report.append("| Canonical Topic | Speaker Count |")
    report.append("|----------------|---------------|")
    
    # Get counts for all canonical topics
    canonical_counts = []
    for canonical in topic_mapping.keys():
        count = stats["topics_after"].get(canonical, 0)
        if count > 0:
            canonical_counts.append((canonical, count))
    
    for topic, count in sorted(canonical_counts, key=lambda x: x[1], reverse=True):
        report.append(f"| {topic} | {count:,} |")
    
    return "\n".join(report)

def update_v3_standardization_to_use_new_mapping(collection, topic_mapping):
    """Update the V3 standardization script to use the new comprehensive mapping"""
    
    # Read the current V3 script
    with open("main_v3.py", "r") as f:
        v3_content = f.read()
    
    # Replace the topic mapping file reference
    v3_content = v3_content.replace(
        'with open("topic_mapping.json", "r", encoding="utf-8") as f:',
        'with open("topic_mapping_comprehensive.json", "r", encoding="utf-8") as f:'
    )
    
    # Save updated V3 script
    with open("main_v3_updated.py", "w") as f:
        f.write(v3_content)
    
    print("✅ Created main_v3_updated.py with comprehensive topic mapping")

def main():
    """Main function"""
    
    print("🔄 Merging Categories into Topics...")
    
    # Load comprehensive mapping
    topic_mapping = load_comprehensive_mapping()
    if not topic_mapping:
        return
    
    print(f"Loaded comprehensive mapping with {len(topic_mapping)} canonical topics")
    
    # Connect to MongoDB
    client = MongoClient(MONGO_URI)
    db = client[TARGET_DB]
    collection = db[COLLECTION]
    
    # Check if categories field exists
    sample = collection.find_one({"categories": {"$exists": True}})
    if not sample:
        print("No documents with 'categories' field found. Checking if merge already done...")
        
        # Just apply the comprehensive mapping
        print("Applying comprehensive topic mapping...")
        stats = merge_categories_and_topics(collection, topic_mapping)
    else:
        print(f"Found documents with 'categories' field. Starting merge...")
        stats = merge_categories_and_topics(collection, topic_mapping)
    
    # Generate report
    report = generate_merge_report(stats, topic_mapping)
    report_file = "CATEGORIES_MERGE_REPORT.md"
    with open(report_file, "w") as f:
        f.write(report)
    
    print(f"\n📊 Report saved to {report_file}")
    
    # Update V3 standardization script
    update_v3_standardization_to_use_new_mapping(collection, topic_mapping)
    
    # Summary
    print(f"\n✅ Merge Complete!")
    print(f"   - Processed: {stats['total_processed']:,} documents")
    print(f"   - Categories merged: {stats['categories_merged']:,}")
    print(f"   - Topics mapped: {stats['topics_mapped']:,}")
    
    # Verify categories field is gone
    remaining = collection.count_documents({"categories": {"$exists": True}})
    if remaining == 0:
        print("   - ✅ Categories field successfully removed")
    else:
        print(f"   - ⚠️  {remaining} documents still have categories field")

if __name__ == "__main__":
    main()