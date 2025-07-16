#!/usr/bin/env python3
"""
Main entry point for Speaker Data Standardization System

Usage:
    python run.py standardize    # Run standardization
    python run.py analyze        # Run comprehensive analysis
    python run.py coverage       # Run field coverage analysis
    python run.py sources        # Analyze source fields
"""

import sys
import subprocess
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Check if MongoDB URI is set
if not os.getenv("MONGO_URI"):
    print("Error: MONGO_URI not set in .env file")
    print("Please copy .env.example to .env and update with your credentials")
    sys.exit(1)

def run_standardization():
    """Run the main standardization pipeline"""
    print("🚀 Running Speaker Data Standardization...")
    subprocess.run([sys.executable, "src/standardization/main.py"])

def run_analysis():
    """Run comprehensive analysis"""
    print("📊 Running Comprehensive Analysis...")
    subprocess.run([sys.executable, "src/analysis/comprehensive_analysis.py"])

def run_coverage():
    """Run field coverage analysis"""
    print("📈 Running Field Coverage Analysis...")
    subprocess.run([sys.executable, "src/analysis/analyze_field_coverage.py"])

def run_sources():
    """Run source fields analysis"""
    print("🔍 Analyzing Source Fields...")
    subprocess.run([sys.executable, "src/analysis/analyze_source_fields_detailed.py"])

def show_help():
    """Show help message"""
    print(__doc__)
    print("\nAvailable commands:")
    print("  standardize  - Run data standardization pipeline")
    print("  analyze      - Run comprehensive analysis")
    print("  coverage     - Analyze field coverage")
    print("  sources      - Analyze source database fields")
    print("  help         - Show this help message")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        show_help()
        sys.exit(1)
    
    command = sys.argv[1].lower()
    
    commands = {
        "standardize": run_standardization,
        "analyze": run_analysis,
        "coverage": run_coverage,
        "sources": run_sources,
        "help": show_help
    }
    
    if command in commands:
        commands[command]()
    else:
        print(f"Unknown command: {command}")
        show_help()
        sys.exit(1)