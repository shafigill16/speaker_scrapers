# Speaker Data Standardization System

A comprehensive ETL pipeline that standardizes speaker data from multiple sources into a unified MongoDB collection with intelligent deduplication and topic normalization.

## 🚀 Features

- **100% Field Capture**: Preserves all fields from 9 different speaker databases
- **Smart Deduplication**: ~10% deduplication rate using fuzzy matching
- **Topic Standardization**: Maps 10,200+ topic variations to 41 canonical categories
- **Rich Data Model**: Captures social media, contact info, professional credentials, and more
- **Analysis Tools**: Comprehensive reporting on data quality and coverage

## 📋 Table of Contents

- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [Data Sources](#data-sources)
- [Architecture](#architecture)
- [Analysis Tools](#analysis-tools)
- [Contributing](#contributing)

## 🛠️ Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd speaker_data_standardization
   ```

2. **Create virtual environment**
   ```bash
   python3 -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Set up environment variables**
   ```bash
   cp .env.example .env
   # Edit .env with your MongoDB credentials
   ```

## ⚙️ Configuration

Create a `.env` file with your MongoDB credentials:

```env
MONGO_URI=mongodb://username:password@host:port/?authSource=admin
TARGET_DATABASE=speaker_database
COLLECTION=unified_speakers_v3
```

## 📊 Usage

### Running the Standardization

```bash
python src/standardization/main.py
```

This will:
1. Connect to all source databases
2. Extract speaker data
3. Apply topic mapping
4. Deduplicate speakers
5. Store in unified collection

### Running Analysis

```bash
# Comprehensive analysis
python src/analysis/comprehensive_analysis.py

# Field coverage analysis
python src/analysis/analyze_field_coverage.py

# Source fields discovery
python src/analysis/analyze_source_fields_detailed.py

# Compare versions
python src/analysis/compare_v2_v3_coverage.py
```

## 📁 Data Sources

The system processes data from 9 speaker databases:

| Source | Documents | Description |
|--------|-----------|-------------|
| speakerhub_scraper | 19,517 | Largest source, comprehensive professional info |
| sessionize_scraper | 12,827 | Tech speakers, rich social media data |
| allamericanspeakers | 6,083 | US-based professional speakers |
| a_speakers | 3,592 | International speakers bureau |
| thespeakerhandbook | 3,510 | UK-based speakers |
| eventraptor | 2,986 | Event-focused speaker profiles |
| bigspeak_scraper | 2,178 | Premium speakers, structured data |
| leading_authorities | 1,230 | High-profile thought leaders |
| freespeakerbureau | 436 | Independent speakers |

## 🏗️ Architecture

### Project Structure

```
speaker_data_standardization/
├── src/
│   ├── standardization/
│   │   └── main.py          # Core standardization pipeline
│   ├── analysis/
│   │   ├── comprehensive_analysis.py
│   │   ├── analyze_field_coverage.py
│   │   └── ...
│   └── utils.py             # Common utilities
├── config/
│   └── topic_mapping.json   # Topic standardization rules
├── docs/                    # Documentation
├── reports/                 # Generated reports
├── .env.example            # Environment template
├── requirements.txt        # Python dependencies
└── README.md
```

### Data Flow

1. **Extract**: Connect to source MongoDB databases
2. **Transform**: 
   - Normalize field names
   - Standardize locations
   - Map topics to canonical categories
   - Extract social media links
3. **Load**: Store in unified collection with deduplication

### Unified Schema

The standardized schema includes:

- **Basic Info**: name, biography, job title, location
- **Contact**: email, phone, website, booking URLs
- **Social Media**: 20+ platforms supported
- **Professional**: company, education, awards, certifications
- **Content**: presentations, workshops, keynotes
- **Media**: images, videos, PDFs
- **Metadata**: source info, timestamps, platform IDs

## 📈 Key Metrics

- **Total Input**: 52,359 documents
- **Unified Output**: 47,320 unique speakers
- **Deduplication Rate**: 9.6%
- **Field Coverage**:
  - Name/Biography: 99%+
  - Profile Image: 84.8%
  - Location: 85.0%
  - Social Media: 36.3%
  - Contact Info: 20.4%

## 🔍 Analysis Tools

### Comprehensive Analysis
Generates detailed reports on:
- Source database statistics
- Field coverage percentages
- Duplicate detection
- Unmapped topics

### Field Coverage Analysis
Analyzes which fields are populated in the unified collection and calculates coverage percentages.

### Source Fields Discovery
Discovers all available fields in source databases to ensure nothing is missed.

## 📝 Reports

Generated reports are saved in the `reports/` directory:

- `COMPREHENSIVE_SPEAKER_ANALYSIS.md` - Full analysis with visualizations
- `TOPIC_MAPPING_REPORT.md` - Topic standardization details
- `comprehensive_analysis_data.json` - Raw analysis data

## 🤝 Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📄 License

This project is proprietary and confidential.

## 👥 Author

Shafi Gill

---

For detailed documentation, see the `docs/` directory.