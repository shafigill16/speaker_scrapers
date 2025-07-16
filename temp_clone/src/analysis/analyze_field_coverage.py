#!/usr/bin/env python3
"""
Field Coverage Analysis for Unified Speaker Data

This script analyzes the coverage of each field in the unified speaker collection,
showing how many documents have each field populated with meaningful data.

Usage:
    export MONGO_URI="mongodb://admin:password@host:27017/?authSource=admin"
    export TARGET_DATABASE="speaker_database"
    export COLLECTION="unified_speakers_v3"  # or unified_speakers_v2
    python3 analyze_field_coverage.py
"""

import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

import json
from pymongo import MongoClient
from collections import defaultdict
from datetime import datetime

MONGO_URI = os.getenv("MONGO_URI")
if not MONGO_URI:
    raise ValueError("MONGO_URI environment variable is required. Please set it in .env file")
TARGET_DB = os.getenv("TARGET_DATABASE", "speaker_database")
COLLECTION = os.getenv("COLLECTION", "unified_speakers_v3")

def is_meaningful_value(value):
    """Check if a value contains meaningful data"""
    if value is None:
        return False
    if isinstance(value, str) and value.strip() == "":
        return False
    if isinstance(value, (list, dict)) and len(value) == 0:
        return False
    if isinstance(value, str) and value.lower() in ["n/a", "none", "null", "undefined"]:
        return False
    return True

def analyze_field_coverage(collection, sample_size=None):
    """Analyze field coverage in the collection"""
    
    # Get total document count
    total_docs = collection.count_documents({})
    
    if sample_size and sample_size < total_docs:
        print(f"Analyzing sample of {sample_size:,} documents out of {total_docs:,}")
        cursor = collection.find().limit(sample_size)
        analysis_count = sample_size
    else:
        print(f"Analyzing all {total_docs:,} documents")
        cursor = collection.find()
        analysis_count = total_docs
    
    # Track field coverage
    field_counts = defaultdict(int)
    field_examples = defaultdict(list)
    
    # Track nested field coverage
    nested_field_counts = defaultdict(int)
    
    # Process each document
    for doc in cursor:
        # Top-level fields
        for field, value in doc.items():
            if is_meaningful_value(value):
                field_counts[field] += 1
                
                # Collect examples (max 3 per field)
                if len(field_examples[field]) < 3 and field != "_id":
                    if isinstance(value, str) and len(value) > 100:
                        field_examples[field].append(value[:100] + "...")
                    elif isinstance(value, dict):
                        field_examples[field].append(f"Object with {len(value)} fields")
                    elif isinstance(value, list):
                        field_examples[field].append(f"Array with {len(value)} items")
                    else:
                        field_examples[field].append(str(value))
                
                # Analyze nested fields
                if isinstance(value, dict):
                    for nested_field, nested_value in value.items():
                        nested_path = f"{field}.{nested_field}"
                        if is_meaningful_value(nested_value):
                            nested_field_counts[nested_path] += 1
                            
                            # Special handling for deeply nested fields
                            if isinstance(nested_value, dict):
                                for deep_field, deep_value in nested_value.items():
                                    deep_path = f"{nested_path}.{deep_field}"
                                    if is_meaningful_value(deep_value):
                                        nested_field_counts[deep_path] += 1
    
    return {
        "total_documents": total_docs,
        "analyzed_documents": analysis_count,
        "field_counts": dict(field_counts),
        "nested_field_counts": dict(nested_field_counts),
        "field_examples": dict(field_examples)
    }

def generate_coverage_report(analysis):
    """Generate a detailed coverage report"""
    
    total = analysis["analyzed_documents"]
    
    print("\n" + "="*80)
    print(f"FIELD COVERAGE ANALYSIS REPORT")
    print(f"Collection: {COLLECTION}")
    print(f"Total Documents: {analysis['total_documents']:,}")
    print(f"Analyzed: {analysis['analyzed_documents']:,}")
    print("="*80)
    
    # Top-level fields
    print("\n## TOP-LEVEL FIELDS")
    print("-"*80)
    print(f"{'Field':<30} {'Count':>10} {'Coverage':>10} {'Examples'}")
    print("-"*80)
    
    # Sort by coverage percentage
    sorted_fields = sorted(
        analysis["field_counts"].items(),
        key=lambda x: x[1],
        reverse=True
    )
    
    for field, count in sorted_fields:
        coverage = (count / total) * 100
        examples = analysis["field_examples"].get(field, [])
        example_str = " | ".join(examples[:2]) if examples else ""
        if len(example_str) > 40:
            example_str = example_str[:40] + "..."
        
        print(f"{field:<30} {count:>10,} {coverage:>9.1f}% {example_str}")
    
    # Nested fields grouped by parent
    print("\n## NESTED FIELDS BY CATEGORY")
    print("-"*80)
    
    # Group nested fields by parent
    nested_by_parent = defaultdict(list)
    for path, count in analysis["nested_field_counts"].items():
        parent = path.split(".")[0]
        nested_by_parent[parent].append((path, count))
    
    # Sort parents by their coverage
    parent_coverage = {}
    for parent in nested_by_parent:
        if parent in analysis["field_counts"]:
            parent_coverage[parent] = analysis["field_counts"][parent]
    
    sorted_parents = sorted(parent_coverage.items(), key=lambda x: x[1], reverse=True)
    
    for parent, parent_count in sorted_parents:
        parent_coverage_pct = (parent_count / total) * 100
        print(f"\n### {parent} ({parent_count:,} documents, {parent_coverage_pct:.1f}%)")
        print(f"{'Field':<40} {'Count':>10} {'Coverage':>10}")
        print("-"*60)
        
        # Sort nested fields by count
        sorted_nested = sorted(nested_by_parent[parent], key=lambda x: x[1], reverse=True)
        
        for path, count in sorted_nested:
            coverage = (count / total) * 100
            # Show only the nested part
            nested_part = path[len(parent)+1:]
            print(f"  {nested_part:<38} {count:>10,} {coverage:>9.1f}%")
    
    # Special analysis for key fields
    print("\n## KEY FIELD ANALYSIS")
    print("-"*80)
    
    key_fields = {
        "Social Media Coverage": [
            "social_media.twitter",
            "social_media.linkedin",
            "social_media.facebook",
            "social_media.instagram",
            "social_media.youtube"
        ],
        "Contact Information": [
            "contact.email",
            "contact.phone",
            "contact.website",
            "contact.booking_url"
        ],
        "Professional Info": [
            "professional_info.awards",
            "professional_info.certifications",
            "professional_info.education",
            "professional_info.company"
        ],
        "Content Types": [
            "content.keynotes",
            "content.presentations",
            "content.workshops",
            "content.speaking_programs"
        ],
        "Media Assets": [
            "media.profile_image",
            "media.videos",
            "media.image_gallery",
            "media.profile_pdf"
        ]
    }
    
    for category, fields in key_fields.items():
        print(f"\n{category}:")
        any_coverage = 0
        for field in fields:
            count = analysis["nested_field_counts"].get(field, 0)
            coverage = (count / total) * 100
            if count > 0:
                any_coverage = max(any_coverage, count)
                print(f"  {field:<40} {count:>8,} ({coverage:>5.1f}%)")
        
        # Calculate "any of these fields" coverage
        if any_coverage > 0:
            print(f"  {'At least one of above':<40} {any_coverage:>8,} ({(any_coverage/total)*100:>5.1f}%)")
    
    return analysis

def save_detailed_analysis(analysis, filename="field_coverage_analysis.json"):
    """Save detailed analysis to JSON file"""
    
    # Calculate coverage percentages
    total = analysis["analyzed_documents"]
    
    output = {
        "metadata": {
            "database": TARGET_DB,
            "collection": COLLECTION,
            "total_documents": analysis["total_documents"],
            "analyzed_documents": analysis["analyzed_documents"],
            "analysis_date": datetime.utcnow().isoformat()
        },
        "field_coverage": {}
    }
    
    # Add coverage data for each field
    for field, count in analysis["field_counts"].items():
        output["field_coverage"][field] = {
            "count": count,
            "percentage": round((count / total) * 100, 2),
            "examples": analysis["field_examples"].get(field, [])
        }
    
    # Add nested field coverage
    output["nested_field_coverage"] = {}
    for path, count in analysis["nested_field_counts"].items():
        output["nested_field_coverage"][path] = {
            "count": count,
            "percentage": round((count / total) * 100, 2)
        }
    
    # Summary statistics
    output["summary"] = {
        "fields_with_100_percent_coverage": [],
        "fields_with_over_90_percent": [],
        "fields_with_over_50_percent": [],
        "fields_with_under_10_percent": [],
        "empty_fields": []
    }
    
    for field, count in analysis["field_counts"].items():
        coverage = (count / total) * 100
        if coverage == 100:
            output["summary"]["fields_with_100_percent_coverage"].append(field)
        elif coverage >= 90:
            output["summary"]["fields_with_over_90_percent"].append(field)
        elif coverage >= 50:
            output["summary"]["fields_with_over_50_percent"].append(field)
        elif coverage < 10 and coverage > 0:
            output["summary"]["fields_with_under_10_percent"].append(field)
        elif coverage == 0:
            output["summary"]["empty_fields"].append(field)
    
    with open(filename, "w") as f:
        json.dump(output, f, indent=2)
    
    print(f"\nDetailed analysis saved to {filename}")

def main():
    """Main function"""
    
    # Connect to MongoDB
    client = MongoClient(MONGO_URI)
    db = client[TARGET_DB]
    collection = db[COLLECTION]
    
    # Check if collection exists
    if COLLECTION not in db.list_collection_names():
        print(f"Error: Collection '{COLLECTION}' not found in database '{TARGET_DB}'")
        print(f"Available collections: {', '.join(db.list_collection_names())}")
        return
    
    # Run analysis
    print(f"Analyzing field coverage in {TARGET_DB}.{COLLECTION}...")
    analysis = analyze_field_coverage(collection)
    
    # Generate report
    generate_coverage_report(analysis)
    
    # Save detailed analysis
    save_detailed_analysis(analysis)
    
    # Additional insights
    print("\n## INSIGHTS")
    print("-"*80)
    
    total = analysis["analyzed_documents"]
    
    # Fields with 100% coverage
    perfect_fields = [f for f, c in analysis["field_counts"].items() 
                      if c == total and f not in ["_id", "source_info"]]
    if perfect_fields:
        print(f"\nFields with 100% coverage: {', '.join(perfect_fields)}")
    
    # Critical missing fields
    critical_fields = ["name", "biography", "topics", "location"]
    missing_critical = []
    for field in critical_fields:
        count = analysis["field_counts"].get(field, 0)
        if count < total * 0.9:  # Less than 90% coverage
            coverage = (count / total) * 100
            missing_critical.append(f"{field} ({coverage:.1f}%)")
    
    if missing_critical:
        print(f"\nCritical fields with <90% coverage: {', '.join(missing_critical)}")
    
    # Source diversity
    if "source_info.original_source" in analysis["nested_field_counts"]:
        print(f"\nDocuments with source info: {analysis['nested_field_counts']['source_info.original_source']:,}")
    
    print("\n" + "="*80)

if __name__ == "__main__":
    main()