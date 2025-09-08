#!/usr/bin/env python3
"""
Test script to check scraper path
"""
import os

print("🔍 Testing scraper path...")

# Current directory
current_dir = os.getcwd()
print(f"Current directory: {current_dir}")

# Backend directory
backend_dir = os.path.dirname(__file__)
print(f"Backend directory: {backend_dir}")

# Try the path from scraping_api.py
scraping_path = os.path.join(backend_dir, '..', '..', 'scrape_data_from_BVMT')
print(f"Scraping path: {scraping_path}")
print(f"Path exists: {os.path.exists(scraping_path)}")

# Check if BVMT scraper exists
bvmt_scraper_path = os.path.join(scraping_path, 'bvmt_scraper.py')
print(f"BVMT scraper path: {bvmt_scraper_path}")
print(f"BVMT scraper exists: {os.path.exists(bvmt_scraper_path)}")

# List contents of scraping directory if it exists
if os.path.exists(scraping_path):
    print(f"\nContents of {scraping_path}:")
    try:
        files = os.listdir(scraping_path)
        for file in files[:10]:  # Show first 10 files
            print(f"  - {file}")
    except Exception as e:
        print(f"Error listing directory: {e}")
else:
    print(f"\nScraping directory does not exist!")
    
    # Try to find it in parent directories
    parent = os.path.dirname(backend_dir)
    print(f"\nChecking parent directory: {parent}")
    if os.path.exists(parent):
        try:
            files = os.listdir(parent)
            for file in files:
                if 'scrape' in file.lower() or 'bvmt' in file.lower():
                    print(f"  - {file} (potential match)")
        except Exception as e:
            print(f"Error listing parent: {e}")
