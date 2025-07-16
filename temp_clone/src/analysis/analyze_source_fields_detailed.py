#!/usr/bin/env python3
"""
Analyze all fields in source databases and generate a detailed report
"""

import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

from pymongo import MongoClient
from collections import defaultdict
import json
from datetime import datetime

MONGO_URI = os.getenv("MONGO_URI")
if not MONGO_URI:
    raise ValueError("MONGO_URI environment variable is required. Please set it in .env file")

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

def get_all_fields(collection, sample_size=100):
    """Get all unique fields from a collection sample"""
    fields = set()
    field_types = {}
    field_value_samples = defaultdict(set)
    
    # Get sample documents
    samples = list(collection.find().limit(sample_size))
    
    def extract_fields(obj, prefix=""):
        """Recursively extract all field paths"""
        if isinstance(obj, dict):
            for key, value in obj.items():
                field_path = f"{prefix}.{key}" if prefix else key
                fields.add(field_path)
                
                # Track field types
                if field_path not in field_types:
                    field_types[field_path] = set()
                field_types[field_path].add(type(value).__name__)
                
                # Collect unique value samples
                if isinstance(value, (str, int, float, bool)) and value:
                    value_str = str(value)
                    if len(value_str) > 100:
                        value_str = value_str[:100] + "..."
                    if len(field_value_samples[field_path]) < 5:
                        field_value_samples[field_path].add(value_str)
                
                # Recurse for nested objects
                if isinstance(value, dict):
                    extract_fields(value, field_path)
                elif isinstance(value, list) and value:
                    # Track list field
                    if len(field_value_samples[field_path]) < 5:
                        field_value_samples[field_path].add(f"[{len(value)} items]")
                    
                    # Check first item structure
                    if isinstance(value[0], dict):
                        extract_fields(value[0], field_path + "[0]")
        
    for doc in samples:
        extract_fields(doc)
    
    # Convert sets to lists for JSON serialization
    field_value_samples_list = {k: list(v) for k, v in field_value_samples.items()}
    return sorted(fields), field_types, field_value_samples_list

def analyze_field_patterns(fields):
    """Analyze patterns in field names"""
    patterns = {
        "social_media": [],
        "contact": [],
        "professional": [],
        "media": [],
        "metadata": [],
        "location": [],
        "content": [],
        "credentials": []
    }
    
    for field in fields:
        field_lower = field.lower()
        
        # Social media patterns
        if any(term in field_lower for term in ['social', 'twitter', 'linkedin', 'facebook', 'instagram', 'youtube']):
            patterns["social_media"].append(field)
        
        # Contact patterns
        elif any(term in field_lower for term in ['email', 'phone', 'contact', 'website', 'booking', 'scheduling']):
            patterns["contact"].append(field)
            
        # Professional patterns
        elif any(term in field_lower for term in ['company', 'job', 'title', 'role', 'professional', 'organization']):
            patterns["professional"].append(field)
            
        # Media patterns
        elif any(term in field_lower for term in ['image', 'photo', 'video', 'media', 'gallery', 'pdf', 'download']):
            patterns["media"].append(field)
            
        # Metadata patterns
        elif any(term in field_lower for term in ['scraped', 'created', 'updated', 'meta', 'source', 'id']):
            patterns["metadata"].append(field)
            
        # Location patterns
        elif any(term in field_lower for term in ['location', 'city', 'state', 'country', 'timezone', 'region']):
            patterns["location"].append(field)
            
        # Content patterns
        elif any(term in field_lower for term in ['keynote', 'presentation', 'workshop', 'talk', 'speech', 'program']):
            patterns["content"].append(field)
            
        # Credentials patterns
        elif any(term in field_lower for term in ['award', 'certification', 'credential', 'education', 'degree']):
            patterns["credentials"].append(field)
    
    return patterns

def analyze_database(db_name, collection_name):
    """Analyze fields in a specific database"""
    client = MongoClient(MONGO_URI)
    
    # Check if database exists
    if db_name not in client.list_database_names():
        return None, f"Database {db_name} not found"
    
    db = client[db_name]
    
    # Check if collection exists
    if collection_name not in db.list_collection_names():
        # Try to find any collection with 'speaker' in name
        speaker_collections = [c for c in db.list_collection_names() if 'speaker' in c.lower()]
        if speaker_collections:
            collection_name = speaker_collections[0]
        else:
            return None, f"No speaker collection found in {db_name}"
    
    collection = db[collection_name]
    doc_count = collection.count_documents({})
    
    if doc_count == 0:
        return None, f"Collection {collection_name} is empty"
    
    # Get all fields
    fields, field_types, field_samples = get_all_fields(collection, min(doc_count, 500))
    
    # Analyze patterns
    patterns = analyze_field_patterns(fields)
    
    # Get collection stats
    avg_doc_size = collection.aggregate([
        {"$sample": {"size": min(100, doc_count)}},
        {"$project": {"docSize": {"$bsonSize": "$$ROOT"}}},
        {"$group": {"_id": None, "avgSize": {"$avg": "$docSize"}}}
    ]).next()["avgSize"]
    
    return {
        "database": db_name,
        "collection": collection_name,
        "document_count": doc_count,
        "field_count": len(fields),
        "avg_doc_size_bytes": int(avg_doc_size),
        "fields": fields,
        "field_types": {k: list(v) for k, v in field_types.items()},
        "field_samples": field_samples,
        "patterns": patterns
    }, None

def generate_markdown_report(all_results):
    """Generate a detailed markdown report"""
    
    report = []
    report.append("# Source Database Field Analysis Report")
    report.append(f"\nGenerated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}")
    report.append(f"\nMongoDB URI: {MONGO_URI.split('@')[-1] if '@' in MONGO_URI else 'localhost'}")
    
    # Summary section
    report.append("\n## Executive Summary")
    
    total_docs = sum(r["document_count"] for r in all_results.values())
    total_fields = sum(r["field_count"] for r in all_results.values())
    
    report.append(f"\n- **Total Databases Analyzed**: {len(all_results)}")
    report.append(f"- **Total Documents**: {total_docs:,}")
    report.append(f"- **Total Unique Fields**: {total_fields:,}")
    report.append(f"- **Average Fields per Source**: {total_fields // len(all_results)}")
    
    # Database summary table
    report.append("\n### Database Overview")
    report.append("\n| Database | Collection | Documents | Fields | Avg Doc Size |")
    report.append("|----------|------------|-----------|--------|--------------|")
    
    for db_name, result in sorted(all_results.items(), key=lambda x: x[1]["document_count"], reverse=True):
        report.append(f"| {db_name} | {result['collection']} | {result['document_count']:,} | {result['field_count']} | {result['avg_doc_size_bytes']:,} bytes |")
    
    # Detailed analysis per database
    report.append("\n## Detailed Field Analysis by Database")
    
    for db_name, result in sorted(all_results.items()):
        report.append(f"\n### {db_name}")
        report.append(f"\n**Collection**: `{result['collection']}`")
        report.append(f"**Documents**: {result['document_count']:,}")
        report.append(f"**Total Fields**: {result['field_count']}")
        
        # Field patterns
        report.append("\n#### Field Categories")
        for category, fields in result['patterns'].items():
            if fields:
                report.append(f"\n**{category.replace('_', ' ').title()}** ({len(fields)} fields):")
                for field in sorted(fields)[:10]:  # Show top 10
                    samples = result['field_samples'].get(field, [])
                    sample_str = " | ".join(list(samples)[:2]) if samples else "No samples"
                    if len(sample_str) > 60:
                        sample_str = sample_str[:60] + "..."
                    report.append(f"- `{field}`: {sample_str}")
                if len(fields) > 10:
                    report.append(f"- ... and {len(fields) - 10} more")
        
        # Top-level fields
        top_level = [f for f in result['fields'] if '.' not in f and '[' not in f]
        report.append(f"\n#### Top-Level Fields ({len(top_level)})")
        report.append("```")
        for field in sorted(top_level):
            types = result['field_types'].get(field, [])
            report.append(f"{field} ({', '.join(types)})")
        report.append("```")
        
        # Nested structures
        nested = defaultdict(list)
        for field in result['fields']:
            if '.' in field:
                parent = field.split('.')[0]
                nested[parent].append(field)
        
        if nested:
            report.append("\n#### Nested Field Structures")
            for parent, fields in sorted(nested.items()):
                report.append(f"\n**{parent}** ({len(fields)} nested fields):")
                for field in sorted(fields)[:5]:
                    report.append(f"- {field}")
                if len(fields) > 5:
                    report.append(f"- ... and {len(fields) - 5} more")
    
    # Cross-database analysis
    report.append("\n## Cross-Database Field Analysis")
    
    # Find common fields
    all_field_sets = {db: set(r['fields']) for db, r in all_results.items()}
    
    # Fields present in all databases
    if all_field_sets:
        common_fields = set.intersection(*all_field_sets.values())
        report.append(f"\n### Common Fields (present in all {len(all_results)} databases)")
        if common_fields:
            for field in sorted(common_fields):
                report.append(f"- {field}")
        else:
            report.append("- No fields are common to all databases")
    
    # Find unique fields per database
    report.append("\n### Unique Fields by Database")
    for db_name, fields in all_field_sets.items():
        other_fields = set.union(*[f for d, f in all_field_sets.items() if d != db_name])
        unique = fields - other_fields
        if unique:
            report.append(f"\n**{db_name}** ({len(unique)} unique fields):")
            for field in sorted(unique)[:10]:
                report.append(f"- {field}")
            if len(unique) > 10:
                report.append(f"- ... and {len(unique) - 10} more")
    
    # Key findings
    report.append("\n## Key Findings")
    
    # Social media coverage
    social_coverage = {}
    for db, result in all_results.items():
        social_fields = result['patterns']['social_media']
        if social_fields:
            social_coverage[db] = len(social_fields)
    
    report.append("\n### Social Media Field Coverage")
    for db, count in sorted(social_coverage.items(), key=lambda x: x[1], reverse=True):
        report.append(f"- **{db}**: {count} social media fields")
    
    # Contact info coverage
    contact_coverage = {}
    for db, result in all_results.items():
        contact_fields = result['patterns']['contact']
        if contact_fields:
            contact_coverage[db] = len(contact_fields)
    
    report.append("\n### Contact Information Coverage")
    for db, count in sorted(contact_coverage.items(), key=lambda x: x[1], reverse=True):
        report.append(f"- **{db}**: {count} contact fields")
    
    # Recommendations
    report.append("\n## Recommendations for Standardization")
    
    report.append("\n1. **Priority Fields for Mapping**:")
    report.append("   - Social media links (varies significantly across sources)")
    report.append("   - Contact information (email, phone, website)")
    report.append("   - Professional credentials (awards, certifications)")
    report.append("   - Media assets (images, videos, PDFs)")
    
    report.append("\n2. **Data Quality Considerations**:")
    report.append("   - Standardize date formats across sources")
    report.append("   - Normalize location data (city, state, country)")
    report.append("   - Deduplicate social media URLs")
    report.append("   - Handle missing fields gracefully")
    
    report.append("\n3. **Schema Design Suggestions**:")
    report.append("   - Create unified social_media object")
    report.append("   - Standardize contact information structure")
    report.append("   - Preserve source-specific metadata")
    report.append("   - Implement field-level data quality scores")
    
    return "\n".join(report)

def main():
    print("=== Source Database Field Analysis ===\n")
    
    all_results = {}
    
    for db_name, collection_name in SOURCES.items():
        print(f"\nAnalyzing {db_name}...")
        result, error = analyze_database(db_name, collection_name)
        
        if error:
            print(f"  ERROR: {error}")
            continue
            
        all_results[db_name] = result
        
        print(f"  Database: {result['database']}")
        print(f"  Collection: {result['collection']}")
        print(f"  Documents: {result['document_count']:,}")
        print(f"  Total fields: {result['field_count']}")
        print(f"  Avg doc size: {result['avg_doc_size_bytes']:,} bytes")
    
    # Save JSON results
    with open("source_fields_analysis_detailed.json", "w") as f:
        json.dump(all_results, f, indent=2)
    
    print(f"\nDetailed JSON analysis saved to source_fields_analysis_detailed.json")
    
    # Generate and save markdown report
    report = generate_markdown_report(all_results)
    with open("SOURCE_FIELDS_REPORT.md", "w") as f:
        f.write(report)
    
    print(f"Detailed markdown report saved to SOURCE_FIELDS_REPORT.md")
    
    # Print summary stats
    print("\n=== Summary Statistics ===")
    total_docs = sum(r["document_count"] for r in all_results.values())
    print(f"Total documents across all sources: {total_docs:,}")
    print(f"Databases analyzed: {len(all_results)}")
    
    # Find databases with most fields
    sorted_by_fields = sorted(all_results.items(), key=lambda x: x[1]["field_count"], reverse=True)
    print("\nDatabases by field count:")
    for db, result in sorted_by_fields[:5]:
        print(f"  {db}: {result['field_count']} fields")

if __name__ == "__main__":
    main()