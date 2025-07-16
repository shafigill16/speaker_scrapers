#!/bin/bash

echo "EventRaptor Speaker Scraper"
echo "=========================="
echo ""

# Check if virtual environment exists
if [ -d "venv" ]; then
    echo "Using virtual environment Python..."
    PYTHON="venv/bin/python"
elif [ -d "../venv" ]; then
    echo "Using parent virtual environment Python..."
    PYTHON="../venv/bin/python"
else
    echo "Using system Python..."
    PYTHON="python3"
fi

# Check Python version
echo "Python version:"
$PYTHON --version
echo ""

# Check if required packages are installed
echo "Checking required packages..."
$PYTHON -c "import requests; print('✓ requests installed')" 2>/dev/null || echo "✗ requests not installed"
$PYTHON -c "import bs4; print('✓ beautifulsoup4 installed')" 2>/dev/null || echo "✗ beautifulsoup4 not installed"
$PYTHON -c "import pymongo; print('✓ pymongo installed')" 2>/dev/null || echo "✗ pymongo not installed"
echo ""

# Ask user to proceed
echo "Do you want to run the scraper? (y/n)"
read -r response

if [[ "$response" == "y" || "$response" == "Y" ]]; then
    echo ""
    echo "Starting scraper..."
    echo "=================="
    $PYTHON scraper.py
else
    echo "Scraper execution cancelled."
fi