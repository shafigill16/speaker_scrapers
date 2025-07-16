#!/usr/bin/env python3
"""
Comprehensive Speaker Data Analysis

This script analyzes:
1. Speaker counts in each source database
2. Maximum fields per speaker in each database
3. Total speakers in unified collection
4. Field coverage percentages in unified collection
5. Unmapped topics
6. Duplicate speakers that were merged

Generates a visually appealing markdown report with charts and statistics.
"""

import os
from pymongo import MongoClient
from collections import defaultdict, Counter
from datetime import datetime
import json
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
if not MONGO_URI:
    raise ValueError("MONGO_URI environment variable is required. Please set it in .env file")
TARGET_DB = os.getenv("TARGET_DATABASE", "speaker_database")
COLLECTION = os.getenv("COLLECTION", "unified_speakers_v3")

# Source databases and their collections
SOURCES = {
    "a_speakers": "speakers",
    "allamericanspeakers": "speakers", 
    "bigspeak_scraper": "speaker_profiles",
    "eventraptor": "speakers",
    "freespeakerbureau_scraper": "speakers_profiles",
    "leading_authorities": "speakers_final_details",
    "sessionize_scraper": "speaker_profiles",
    "speakerhub_scraper": "speaker_details",
    "thespeakerhandbook_scraper": "speaker_profiles"
}

def count_fields(doc, prefix=""):
    """Count all fields in a document recursively"""
    count = 0
    if isinstance(doc, dict):
        for key, value in doc.items():
            if value is not None and value != "" and value != [] and value != {}:
                count += 1
                if isinstance(value, dict):
                    count += count_fields(value, f"{prefix}.{key}" if prefix else key)
                elif isinstance(value, list) and value:
                    # Count non-empty lists as 1 field
                    if isinstance(value[0], dict):
                        # For lists of objects, count fields in first object
                        count += count_fields(value[0], f"{prefix}.{key}[0]" if prefix else f"{key}[0]")
    return count

def analyze_source_databases(client):
    """Analyze each source database"""
    source_stats = {}
    
    for db_name, collection_name in SOURCES.items():
        print(f"Analyzing {db_name}...")
        
        if db_name not in client.list_database_names():
            print(f"  - Database not found")
            continue
            
        db = client[db_name]
        
        # Find the correct collection
        if collection_name not in db.list_collection_names():
            speaker_collections = [c for c in db.list_collection_names() if 'speaker' in c.lower()]
            if speaker_collections:
                collection_name = speaker_collections[0]
            else:
                print(f"  - No speaker collection found")
                continue
        
        collection = db[collection_name]
        
        # Get document count
        doc_count = collection.count_documents({})
        
        # Find document with max fields
        max_fields = 0
        max_fields_doc_id = None
        sample_size = min(1000, doc_count)
        
        for doc in collection.find().limit(sample_size):
            field_count = count_fields(doc)
            if field_count > max_fields:
                max_fields = field_count
                max_fields_doc_id = str(doc.get('_id', ''))
        
        # Get average document size
        avg_size_result = collection.aggregate([
            {"$sample": {"size": min(100, doc_count)}},
            {"$project": {"docSize": {"$bsonSize": "$$ROOT"}}},
            {"$group": {"_id": None, "avgSize": {"$avg": "$docSize"}}}
        ])
        
        avg_size = 0
        for result in avg_size_result:
            avg_size = result["avgSize"]
            break
        
        source_stats[db_name] = {
            "collection": collection_name,
            "document_count": doc_count,
            "max_fields": max_fields,
            "max_fields_doc_id": max_fields_doc_id,
            "avg_doc_size": int(avg_size)
        }
        
        print(f"  - Documents: {doc_count:,}")
        print(f"  - Max fields in a document: {max_fields}")
    
    return source_stats

def analyze_unified_collection(client):
    """Analyze the unified speakers collection"""
    db = client[TARGET_DB]
    
    if COLLECTION not in db.list_collection_names():
        return None, "Collection not found"
    
    collection = db[COLLECTION]
    total_docs = collection.count_documents({})
    
    print(f"\nAnalyzing unified collection ({total_docs:,} documents)...")
    
    # Field coverage analysis
    field_coverage = {}
    fields_to_analyze = [
        # Basic fields
        "name",
        "display_name",
        "job_title",
        "biography",
        "description",
        "tagline",
        
        # Location
        "location",
        "location.city",
        "location.state",
        "location.country",
        "location.timezone",
        
        # Contact & Social
        "contact",
        "contact.email",
        "contact.phone",
        "contact.website",
        "social_media",
        
        # Professional
        "professional_info",
        "professional_info.company",
        "professional_info.education",
        "professional_info.awards",
        "professional_info.certifications",
        
        # Content
        "topics",
        "content",
        "content.presentations",
        "content.keynotes",
        "testimonials",
        "reviews",
        
        # Media
        "media",
        "media.profile_image",
        "media.videos",
        "media.profile_pdf",
        
        # Speaking
        "speaking_info",
        "speaking_info.fee_ranges",
        "speaking_info.languages",
        "speaking_history",
        
        # Metadata
        "source_info",
        "platform_fields",
        "metadata"
    ]
    
    for field in fields_to_analyze:
        # Build query to check for non-empty field
        query = {field: {"$exists": True, "$ne": None}}
        
        # For arrays and objects, also check they're not empty
        if any(field.endswith(x) for x in ["info", "media", "contact", "location", "content", "history", "fields", "metadata"]):
            query = {
                "$and": [
                    {field: {"$exists": True}},
                    {field: {"$ne": None}},
                    {field: {"$ne": {}}},
                    {field: {"$ne": []}}
                ]
            }
        
        count = collection.count_documents(query)
        coverage = (count / total_docs * 100) if total_docs > 0 else 0
        field_coverage[field] = {
            "count": count,
            "percentage": coverage
        }
    
    # Analyze unmapped topics
    print("Analyzing unmapped topics...")
    unmapped_topics = Counter()
    
    pipeline = [
        {"$match": {"topics_unmapped": {"$exists": True, "$ne": []}}},
        {"$unwind": "$topics_unmapped"},
        {"$group": {"_id": "$topics_unmapped", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}}
    ]
    
    for result in collection.aggregate(pipeline):
        unmapped_topics[result["_id"]] = result["count"]
    
    # Analyze duplicates/merges
    print("Analyzing speaker duplicates...")
    
    # Count speakers by original source
    source_counts = Counter()
    pipeline = [
        {"$group": {"_id": "$source_info.original_source", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}}
    ]
    
    for result in collection.aggregate(pipeline):
        if result["_id"]:
            source_counts[result["_id"]] = result["count"]
    
    # Find potential duplicates by analyzing similar names
    duplicates = []
    
    # Sample approach: Find speakers with exact same name
    pipeline = [
        {"$group": {
            "_id": "$name",
            "count": {"$sum": 1},
            "speakers": {"$push": {
                "id": "$_id",
                "source": "$source_info.original_source",
                "location": "$location.full_location"
            }}
        }},
        {"$match": {"count": {"$gt": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 20}
    ]
    
    for result in collection.aggregate(pipeline):
        if result["count"] > 1:
            duplicates.append({
                "name": result["_id"],
                "count": result["count"],
                "sources": [s["source"] for s in result["speakers"][:5]]  # Show first 5 sources
            })
    
    return {
        "total_documents": total_docs,
        "field_coverage": field_coverage,
        "unmapped_topics": dict(unmapped_topics),
        "source_distribution": dict(source_counts),
        "potential_duplicates": duplicates
    }, None

def generate_visual_report(source_stats, unified_stats):
    """Generate a visually appealing markdown report"""
    
    report = []
    
    # Header
    report.append("# 📊 Comprehensive Speaker Data Analysis Report")
    report.append(f"\n*Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}*")
    
    # Executive Summary
    report.append("\n## 🎯 Executive Summary")
    
    total_source_docs = sum(s["document_count"] for s in source_stats.values())
    total_unified_docs = unified_stats["total_documents"]
    dedup_rate = ((total_source_docs - total_unified_docs) / total_source_docs * 100) if total_source_docs > 0 else 0
    
    report.append("\n```")
    report.append(f"Total Source Documents:     {total_source_docs:,}")
    report.append(f"Total Unified Speakers:     {total_unified_docs:,}")
    report.append(f"Deduplication Rate:         {dedup_rate:.1f}%")
    report.append(f"Speakers Merged:            {total_source_docs - total_unified_docs:,}")
    report.append("```")
    
    # Source Database Analysis
    report.append("\n## 📁 Source Database Analysis")
    
    report.append("\n### Document Count by Source")
    report.append("\n```")
    
    # Sort by document count
    sorted_sources = sorted(source_stats.items(), key=lambda x: x[1]["document_count"], reverse=True)
    
    max_count = sorted_sources[0][1]["document_count"] if sorted_sources else 1
    
    for db_name, stats in sorted_sources:
        count = stats["document_count"]
        bar_length = int(count / max_count * 30)
        bar = "█" * bar_length
        percentage = (count / total_source_docs * 100) if total_source_docs > 0 else 0
        report.append(f"{db_name:<25} {bar:<30} {count:>6,} ({percentage:>5.1f}%)")
    
    report.append("```")
    
    # Max fields analysis
    report.append("\n### Maximum Fields per Document")
    report.append("\n| Source | Max Fields | Document ID | Avg Doc Size |")
    report.append("|--------|------------|-------------|--------------|")
    
    for db_name, stats in sorted(source_stats.items(), key=lambda x: x[1]["max_fields"], reverse=True):
        report.append(f"| {db_name} | {stats['max_fields']} | {stats['max_fields_doc_id'][:12]}... | {stats['avg_doc_size']:,} bytes |")
    
    # Field Coverage in Unified Collection
    report.append("\n## 📈 Field Coverage Analysis (Unified Collection)")
    
    # Group fields by category
    basic_fields = ["name", "display_name", "job_title", "biography", "description", "tagline"]
    location_fields = ["location", "location.city", "location.state", "location.country", "location.timezone"]
    contact_fields = ["contact", "contact.email", "contact.phone", "contact.website", "social_media"]
    professional_fields = ["professional_info", "professional_info.company", "professional_info.education", 
                          "professional_info.awards", "professional_info.certifications"]
    content_fields = ["topics", "content", "content.presentations", "content.keynotes", "testimonials", "reviews"]
    media_fields = ["media", "media.profile_image", "media.videos", "media.profile_pdf"]
    
    categories = [
        ("🔤 Basic Information", basic_fields),
        ("📍 Location", location_fields),
        ("📞 Contact & Social", contact_fields),
        ("💼 Professional", professional_fields),
        ("📝 Content", content_fields),
        ("🖼️ Media", media_fields)
    ]
    
    for category_name, fields in categories:
        report.append(f"\n### {category_name}")
        report.append("\n```")
        
        for field in fields:
            if field in unified_stats["field_coverage"]:
                cov = unified_stats["field_coverage"][field]
                percentage = cov["percentage"]
                bar_length = int(percentage / 100 * 25)
                bar = "█" * bar_length + "░" * (25 - bar_length)
                
                # Indent nested fields
                display_field = field
                if "." in field:
                    indent = "  " * field.count(".")
                    display_field = indent + field.split(".")[-1]
                
                report.append(f"{display_field:<35} {bar} {percentage:>5.1f}% ({cov['count']:,} speakers)")
        
        report.append("```")
    
    # Unmapped Topics
    report.append("\n## 🏷️ Unmapped Topics Analysis")
    
    unmapped = unified_stats["unmapped_topics"]
    if unmapped:
        report.append(f"\n**Total Unique Unmapped Topics**: {len(unmapped)}")
        report.append("\n### Top 20 Unmapped Topics")
        report.append("\n| Topic | Occurrences |")
        report.append("|-------|-------------|")
        
        for topic, count in sorted(unmapped.items(), key=lambda x: x[1], reverse=True)[:20]:
            report.append(f"| {topic[:50]} | {count:,} |")
    else:
        report.append("\n✅ No unmapped topics found!")
    
    # Duplicate Analysis
    report.append("\n## 🔄 Duplicate Speaker Analysis")
    
    # Source distribution in unified collection
    report.append("\n### Speakers by Original Source")
    report.append("\n```")
    
    source_dist = unified_stats["source_distribution"]
    total_in_unified = sum(source_dist.values())
    
    for source, count in sorted(source_dist.items(), key=lambda x: x[1], reverse=True):
        percentage = (count / total_in_unified * 100) if total_in_unified > 0 else 0
        expected = source_stats.get(source, {}).get("document_count", 0)
        merged = expected - count if expected > 0 else 0
        
        report.append(f"{source:<25} {count:>6,} speakers (merged {merged:,} duplicates)")
    
    report.append("```")
    
    # Potential duplicates
    duplicates = unified_stats["potential_duplicates"]
    if duplicates:
        report.append("\n### Potential Duplicate Speakers (Same Name)")
        report.append("\n| Speaker Name | Count | Sources |")
        report.append("|--------------|-------|---------|")
        
        for dup in duplicates[:15]:  # Show top 15
            name = dup.get('name', 'Unknown')
            if name:
                sources = ", ".join(dup.get("sources", []))
                report.append(f"| {name[:40]} | {dup.get('count', 0)} | {sources} |")
    
    # Merge Statistics
    report.append("\n## 🔀 Merge Statistics")
    
    report.append("\n```")
    report.append("Source Database Stats:")
    for db_name, stats in sorted(source_stats.items()):
        report.append(f"  {db_name:<25} {stats['document_count']:>6,} documents")
    report.append(f"  {'TOTAL':<25} {total_source_docs:>6,} documents")
    report.append("\n---")
    report.append(f"Unified Collection:         {total_unified_docs:,} unique speakers")
    report.append(f"Total Merged/Deduplicated:  {total_source_docs - total_unified_docs:,} documents")
    report.append(f"Deduplication Rate:         {dedup_rate:.1f}%")
    report.append("```")
    
    # Key Insights
    report.append("\n## 💡 Key Insights")
    
    # Field coverage insights
    high_coverage = []
    low_coverage = []
    
    for field, cov in unified_stats["field_coverage"].items():
        if cov["percentage"] > 80 and "." not in field:
            high_coverage.append((field, cov["percentage"]))
        elif cov["percentage"] < 20 and "." not in field:
            low_coverage.append((field, cov["percentage"]))
    
    report.append("\n### ✅ High Coverage Fields (>80%)")
    for field, pct in sorted(high_coverage, key=lambda x: x[1], reverse=True):
        report.append(f"- **{field}**: {pct:.1f}%")
    
    report.append("\n### ⚠️ Low Coverage Fields (<20%)")
    for field, pct in sorted(low_coverage, key=lambda x: x[1]):
        report.append(f"- **{field}**: {pct:.1f}%")
    
    # Database insights
    report.append("\n### 📊 Database Insights")
    
    largest_source = sorted_sources[0] if sorted_sources else None
    if largest_source:
        report.append(f"- **Largest Source**: {largest_source[0]} ({largest_source[1]['document_count']:,} documents)")
    
    max_fields_db = max(source_stats.items(), key=lambda x: x[1]["max_fields"])
    report.append(f"- **Most Complex Documents**: {max_fields_db[0]} (up to {max_fields_db[1]['max_fields']} fields)")
    
    # Calculate average deduplication by source
    report.append("\n### 🔗 Deduplication by Source")
    dedup_rates = []
    for source, stats in source_stats.items():
        unified_count = source_dist.get(source, 0)
        source_count = stats["document_count"]
        if source_count > 0:
            dedup_rate_source = ((source_count - unified_count) / source_count * 100)
            dedup_rates.append((source, dedup_rate_source, source_count - unified_count))
    
    for source, rate, count in sorted(dedup_rates, key=lambda x: x[1], reverse=True):
        if rate > 0:
            report.append(f"- **{source}**: {rate:.1f}% deduplication ({count:,} merged)")
    
    # Footer
    report.append("\n---")
    report.append("\n*Report generated by comprehensive_analysis.py*")
    
    return "\n".join(report)

def main():
    """Main function"""
    
    print("🔍 Starting Comprehensive Speaker Data Analysis...")
    
    # Connect to MongoDB
    client = MongoClient(MONGO_URI)
    
    # Analyze source databases
    print("\n📁 Analyzing source databases...")
    source_stats = analyze_source_databases(client)
    
    # Analyze unified collection
    print("\n📊 Analyzing unified collection...")
    unified_stats, error = analyze_unified_collection(client)
    
    if error:
        print(f"Error analyzing unified collection: {error}")
        return
    
    # Generate report
    print("\n📝 Generating visual report...")
    report = generate_visual_report(source_stats, unified_stats)
    
    # Save report
    report_filename = "COMPREHENSIVE_SPEAKER_ANALYSIS.md"
    with open(report_filename, "w") as f:
        f.write(report)
    
    print(f"\n✅ Report saved to {report_filename}")
    
    # Save raw data as JSON
    data_filename = "comprehensive_analysis_data.json"
    with open(data_filename, "w") as f:
        json.dump({
            "source_databases": source_stats,
            "unified_collection": unified_stats,
            "analysis_date": datetime.utcnow().isoformat()
        }, f, indent=2)
    
    print(f"📊 Raw data saved to {data_filename}")
    
    # Print summary
    total_source = sum(s["document_count"] for s in source_stats.values())
    total_unified = unified_stats["total_documents"]
    
    print("\n" + "="*60)
    print("ANALYSIS COMPLETE")
    print("="*60)
    print(f"Total source documents:     {total_source:,}")
    print(f"Total unified speakers:     {total_unified:,}")
    print(f"Speakers merged:            {total_source - total_unified:,}")
    print(f"Deduplication rate:         {((total_source - total_unified) / total_source * 100):.1f}%")
    print("="*60)

if __name__ == "__main__":
    main()