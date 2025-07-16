#!/usr/bin/env python3
"""
Compare field coverage between V2 and V3 unified speaker collections

Usage:
    export MONGO_URI="mongodb://admin:password@host:27017/?authSource=admin"
    export TARGET_DATABASE="speaker_database"
    python3 compare_v2_v3_coverage.py
"""

import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

from pymongo import MongoClient
from collections import defaultdict

MONGO_URI = os.getenv("MONGO_URI")
if not MONGO_URI:
    raise ValueError("MONGO_URI environment variable is required. Please set it in .env file")
TARGET_DB = os.getenv("TARGET_DATABASE", "speaker_database")

def count_field_coverage(collection, field_path):
    """Count documents with a specific field populated"""
    
    # Build the query
    query = {field_path: {"$exists": True, "$ne": None}}
    
    # For arrays and dicts, also check they're not empty
    if "." not in field_path:
        # Top level field
        sample = collection.find_one({field_path: {"$exists": True}})
        if sample and field_path in sample:
            if isinstance(sample[field_path], (list, dict)):
                query = {
                    field_path: {
                        "$exists": True, 
                        "$ne": None,
                        "$not": {"$size": 0}
                    }
                }
    
    return collection.count_documents(query)

def analyze_collections(db):
    """Analyze and compare V2 and V3 collections"""
    
    # Check collections exist
    v2_exists = "unified_speakers_v2" in db.list_collection_names()
    v3_exists = "unified_speakers_v3" in db.list_collection_names()
    
    if not v2_exists and not v3_exists:
        print("Neither V2 nor V3 collections found!")
        return
        
    v2_collection = db["unified_speakers_v2"] if v2_exists else None
    v3_collection = db["unified_speakers_v3"] if v3_exists else None
    
    # Get total counts
    v2_total = v2_collection.count_documents({}) if v2_collection is not None else 0
    v3_total = v3_collection.count_documents({}) if v3_collection is not None else 0
    
    print(f"V2 Total Documents: {v2_total:,}")
    print(f"V3 Total Documents: {v3_total:,}")
    
    # Key fields to compare
    fields_to_compare = [
        # Basic fields
        "name",
        "biography", 
        "job_title",
        "topics",
        "location",
        
        # Contact & Social
        "contact",
        "contact.email",
        "contact.phone",
        "contact.website",
        "social_media",
        "social_media.linkedin",
        "social_media.twitter",
        
        # Professional
        "professional_info",
        "professional_info.company",
        "professional_info.education",
        "professional_info.awards",
        "professional_info.certifications",
        
        # Content
        "content",
        "content.presentations",
        "content.keynotes",
        "testimonials",
        "reviews",
        
        # Media
        "media.profile_image",
        "media.videos",
        "media.profile_pdf",
        
        # New V3 fields
        "platform_fields",
        "platform_fields.uid",
        "platform_fields.username",
        "metadata.meta_description",
        "metadata.company",
        "source_info.first_scraped_at",
        "source_info.last_updated"
    ]
    
    results = []
    
    print("\nAnalyzing field coverage...")
    
    for field in fields_to_compare:
        v2_count = count_field_coverage(v2_collection, field) if v2_collection is not None else 0
        v3_count = count_field_coverage(v3_collection, field) if v3_collection is not None else 0
        
        v2_pct = (v2_count / v2_total * 100) if v2_total > 0 else 0
        v3_pct = (v3_count / v3_total * 100) if v3_total > 0 else 0
        
        improvement = v3_count - v2_count
        pct_change = v3_pct - v2_pct
        
        results.append({
            "field": field,
            "v2_count": v2_count,
            "v2_pct": v2_pct,
            "v3_count": v3_count,
            "v3_pct": v3_pct,
            "improvement": improvement,
            "pct_change": pct_change
        })
    
    # Sort by improvement
    results.sort(key=lambda x: x["improvement"], reverse=True)
    
    # Print results
    print("\n" + "="*120)
    print(f"{'Field':<40} {'V2 Count':>12} {'V2 %':>8} {'V3 Count':>12} {'V3 %':>8} {'Change':>12} {'% Change':>10}")
    print("="*120)
    
    for r in results:
        # Color code improvements
        if r["improvement"] > 1000:
            change_str = f"+{r['improvement']:,}"
        elif r["improvement"] > 0:
            change_str = f"+{r['improvement']:,}"
        elif r["improvement"] == 0:
            change_str = "0"
        else:
            change_str = f"{r['improvement']:,}"
            
        print(f"{r['field']:<40} {r['v2_count']:>12,} {r['v2_pct']:>7.1f}% {r['v3_count']:>12,} {r['v3_pct']:>7.1f}% {change_str:>12} {r['pct_change']:>+9.1f}%")
    
    # Summary of new fields in V3
    print("\n## NEW FIELDS IN V3 (not present in V2)")
    print("-"*80)
    
    new_fields = [r for r in results if r["v2_count"] == 0 and r["v3_count"] > 0]
    for r in new_fields:
        print(f"{r['field']:<40} {r['v3_count']:>12,} documents ({r['v3_pct']:>5.1f}%)")
    
    # Fields with significant improvements
    print("\n## FIELDS WITH SIGNIFICANT IMPROVEMENTS (>1000 documents)")
    print("-"*80)
    
    improved_fields = [r for r in results if r["improvement"] > 1000]
    for r in improved_fields:
        print(f"{r['field']:<40} +{r['improvement']:>11,} documents ({r['pct_change']:>+5.1f}% coverage)")
    
    # Overall statistics
    print("\n## OVERALL STATISTICS")
    print("-"*80)
    
    # Count speakers with any social media
    v2_social = v2_collection.count_documents({"social_media": {"$exists": True, "$ne": None}}) if v2_collection is not None else 0
    v3_social = v3_collection.count_documents({"social_media": {"$exists": True, "$ne": None}}) if v3_collection is not None else 0
    
    # Count speakers with any contact info
    v2_contact = v2_collection.count_documents({"contact": {"$exists": True, "$ne": None}}) if v2_collection is not None else 0
    v3_contact = v3_collection.count_documents({"contact": {"$exists": True, "$ne": None}}) if v3_collection is not None else 0
    
    print(f"Speakers with social media: V2={v2_social:,} ({v2_social/v2_total*100:.1f}%) → V3={v3_social:,} ({v3_social/v3_total*100:.1f}%)")
    print(f"Speakers with contact info: V2={v2_contact:,} ({v2_contact/v2_total*100:.1f}%) → V3={v3_contact:,} ({v3_contact/v3_total*100:.1f}%)")
    
    # Calculate data completeness score
    key_fields = ["name", "biography", "topics", "location", "social_media", "contact", "media.profile_image"]
    
    v2_completeness = 0
    v3_completeness = 0
    
    for field in key_fields:
        v2_completeness += count_field_coverage(v2_collection, field) / v2_total if v2_collection is not None and v2_total > 0 else 0
        v3_completeness += count_field_coverage(v3_collection, field) / v3_total if v3_collection is not None and v3_total > 0 else 0
    
    v2_score = (v2_completeness / len(key_fields)) * 100
    v3_score = (v3_completeness / len(key_fields)) * 100
    
    print(f"\nData Completeness Score (key fields): V2={v2_score:.1f}% → V3={v3_score:.1f}% (+{v3_score-v2_score:.1f}%)")

def main():
    """Main function"""
    
    client = MongoClient(MONGO_URI)
    db = client[TARGET_DB]
    
    print(f"Comparing V2 and V3 coverage in {TARGET_DB}")
    print("="*80)
    
    analyze_collections(db)

if __name__ == "__main__":
    main()