# Speaker Data Standardization Flow

## Visual Data Flow

```
MongoDB Server (5.161.225.172)
├── Database: a_speakers
│   └── Collection: speakers (3,592 docs)
├── Database: allamericanspeakers  
│   └── Collection: speakers (4,666 docs)
├── Database: bigspeak_scraper
│   └── Collection: speaker_profiles (2,178 docs)
├── Database: eventraptor
│   └── Collection: speakers (2,986 docs)
├── Database: freespeakerbureau_scraper
│   └── Collection: speakers_profiles (436 docs)
├── Database: leading_authorities
│   └── Collection: speakers_final_details (1,230 docs)
├── Database: sessionize_scraper
│   └── Collection: speaker_profiles (12,827 docs)
├── Database: speakerhub_scraper
│   └── Collection: speaker_details (18,913 docs)
└── Database: thespeakerhandbook_scraper
    └── Collection: speaker_profiles (3,510 docs)
                    │
                    ▼
         ┌──────────────────────┐
         │  Standardization     │
         │  Process (main.py)   │
         │  ────────────────    │
         │ 1. Connect to each DB│
         │ 2. Transform data    │
         │ 3. Map topics        │
         │ 4. Deduplicate       │
         │ 5. Write to unified  │
         └──────────┬───────────┘
                    │
                    ▼
         Database: speaker_database
         └── Collection: unified_speakers (46,243 docs)
                                      │
                                      ▼
                          ┌───────────────────────┐
                          │   Topic Mapping      │
                          │  ────────────────    │
                          │ "AI" → "Artificial   │
                          │        Intelligence" │
                          │ "ML" → "Artificial   │
                          │        Intelligence" │
                          └──────────┬────────────┘
                                     │
                                     ▼
                          ┌───────────────────────┐
                          │   Deduplication      │
                          │  ────────────────    │
                          │ • Name fingerprint   │
                          │ • Fuzzy matching     │
                          │ • Location check     │
                          └──────────┬────────────┘
                                     │
                                     ▼
                          ┌───────────────────────┐
                          │  Unified Speakers    │
                          │  ────────────────    │
                          │ • Consistent schema  │
                          │ • Source tracking    │
                          │ • Canonical topics   │
                          │ • Single collection  │
                          └───────────────────────┘
```

## Transformation Examples

### Example 1: Name Standardization
```
Input (various sources):
- "Dr. Jane Smith, PhD"
- "Jane Smith"  
- "JANE SMITH"
- "Smith, Jane"

Output (unified):
- name: "Jane Smith"
- display_name: "Dr. Jane Smith, PhD"
```

### Example 2: Location Parsing
```
Input (various formats):
- "New York, NY, USA"
- "travels from New York"
- {"city": "New York", "state": "NY", "country": "USA"}
- "New York City"

Output (unified):
- location: {
    "city": "New York",
    "state": "NY", 
    "country": "USA",
    "full_location": "New York, NY, USA"
  }
```

### Example 3: Topic Mapping
```
Input (raw topics):
- ["AI", "Machine Learning", "Leadership Development", "Digital Transformation"]

Processing:
1. Clean: Remove extra spaces
2. Map: "AI" → "Artificial Intelligence", "Machine Learning" → "Artificial Intelligence"
3. Dedupe: Remove duplicates

Output (unified):
- topics: ["Artificial Intelligence", "Leadership", "Technology"]
- topics_unmapped: ["Digital Transformation"] // If not in mapping
```

### Example 4: Fee Range Normalization
```
Input (various formats):
- "$5,000 - $10,000"
- {"live_event": "$5,000-$10,000", "virtual_event": "$2,500-$5,000"}
- "Please Inquire"
- "Starting at $5000"

Output (unified):
- speaking_info: {
    "fee_ranges": {
      "live_event": "$5,000 - $10,000",
      "virtual_event": "$2,500 - $5,000"
    }
  }
```

## Deduplication Logic

```python
# Simplified deduplication process
def find_duplicate(new_speaker):
    fingerprint = remove_special_chars(new_speaker.name.lower())
    
    candidates = find_speakers_with_similar_fingerprint(fingerprint)
    
    for candidate in candidates:
        name_similarity = fuzzy_match(new_speaker.name, candidate.name)
        
        # Base threshold
        if name_similarity > 90:
            return candidate
            
        # Bonus for matching location
        if same_city(new_speaker, candidate):
            if name_similarity > 85:
                return candidate
    
    return None  # No duplicate found
```

## Data Quality Metrics

### Coverage by Source
```
┌─────────────────────────┬────────┬──────────┬──────────┬─────────┐
│ Field                   │ A-Spk  │ AllAmer  │ BigSpeak │ Event   │
├─────────────────────────┼────────┼──────────┼──────────┼─────────┤
│ Name                    │ ✓      │ ✓        │ ✓        │ ✓       │
│ Biography               │ ✓      │ ✓        │ ✓        │ ✓       │
│ Location                │ ✓      │ ✓        │ ✓        │ ✗       │
│ Topics                  │ ✓      │ ✓        │ ✓        │ ✓       │
│ Fee Range               │ ✓      │ ✓        │ ✓        │ ✗       │
│ Reviews                 │ ✓      │ ✓        │ ✗        │ ✗       │
│ Videos                  │ ✓      │ ✓        │ ✗        │ ✗       │
│ Contact Info            │ ✗      │ ✗        │ ✗        │ ✓       │
└─────────────────────────┴────────┴──────────┴──────────┴─────────┘
```

## Benefits of Standardization

1. **Unified Search**: Search across all sources with one query
2. **Data Completeness**: Combine best data from each source
3. **Consistent Format**: Same structure regardless of source
4. **Topic Discovery**: Find speakers by canonical topics
5. **Deduplication**: Avoid showing same speaker multiple times
6. **Source Attribution**: Always know where data came from
7. **Update Tracking**: Know when records were last updated