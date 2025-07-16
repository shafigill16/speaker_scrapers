# Project Structure

## Directory Layout

```
speaker_data_standardization/
├── .env                     # Environment variables (not in git)
├── .env.example            # Environment template
├── .gitignore              # Git ignore rules
├── README.md               # Main documentation
├── requirements.txt        # Python dependencies
├── run.py                  # Main entry point script
│
├── config/
│   └── topic_mapping.json  # Comprehensive topic mapping (41 canonical topics)
│
├── src/
│   ├── __init__.py
│   ├── utils.py            # Shared utilities and constants
│   │
│   ├── standardization/
│   │   └── main.py         # Core ETL pipeline (V3 - 100% field capture)
│   │
│   └── analysis/
│       ├── __init__.py
│       ├── comprehensive_analysis.py      # Full analysis with visualizations
│       ├── analyze_field_coverage.py      # Field coverage percentages
│       ├── analyze_source_fields_detailed.py  # Source database field discovery
│       ├── compare_v2_v3_coverage.py      # Version comparison
│       └── merge_categories_to_topics.py  # Category-topic unification
│
├── docs/
│   ├── PROJECT_STRUCTURE.md              # This file
│   ├── DATA_FLOW.md                      # ETL process documentation
│   ├── FINAL_ACCOMPLISHMENTS_SUMMARY.md  # Project summary
│   ├── COMPREHENSIVE_SPEAKER_ANALYSIS.md # Analysis results
│   ├── TOPIC_MAPPING_REPORT.md           # Topic standardization details
│   ├── CATEGORIES_MERGE_REPORT.md        # Category merge results
│   └── ANALYSIS_SUMMARY_VISUAL.md        # Visual dashboard
│
├── reports/
│   └── comprehensive_analysis_data.json   # Raw analysis data
│
└── venv/                   # Python virtual environment (not in git)
```

## Key Files

### Configuration
- `.env` - MongoDB credentials and settings
- `config/topic_mapping.json` - Maps 10,200+ topic variations to 41 canonical topics

### Main Scripts
- `run.py` - Entry point with commands for all operations
- `src/standardization/main.py` - V3 standardization with 100% field capture

### Analysis Tools
- `comprehensive_analysis.py` - Complete database analysis
- `analyze_field_coverage.py` - Field population statistics
- `analyze_source_fields_detailed.py` - Source field discovery
- `compare_v2_v3_coverage.py` - Version comparison
- `merge_categories_to_topics.py` - Data structure unification

### Documentation
- `README.md` - Project overview and usage
- `docs/DATA_FLOW.md` - ETL process details
- `docs/FINAL_ACCOMPLISHMENTS_SUMMARY.md` - Achievement summary

## Environment Setup

1. Copy `.env.example` to `.env`
2. Update MongoDB credentials
3. Install dependencies: `pip install -r requirements.txt`

## Usage

```bash
# Run standardization
python run.py standardize

# Run analysis
python run.py analyze

# Check field coverage
python run.py coverage

# Analyze source fields
python run.py sources
```