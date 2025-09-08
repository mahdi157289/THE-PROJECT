#!/usr/bin/env python3
"""
ULTIMATE SIMPLE bronze runner - just like we did with the scraper!
No complex imports, just run the script directly
"""

import os
import sys

def main():
    print("🚀 ULTIMATE SIMPLE Bronze Runner - Just like scraper!")
    
    # Get the current script directory
    current_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Change to the ETL directory
    etl_dir = os.path.join(current_dir, 'ETL__MAIDALLION')
    os.chdir(etl_dir)
    print(f"📂 Changed to directory: {os.getcwd()}")
    
    # Add the config directory to Python path BEFORE running
    config_path = os.path.join(current_dir, '..', 'config')
    sys.path.insert(0, config_path)
    print(f"📁 Added config path: {config_path}")
    
    # Add the ETL directory to Python path
    sys.path.insert(0, etl_dir)
    print(f"📁 Added ETL path: {etl_dir}")
    
    try:
        # Now run the bronze loader
        print("🚀 Running bronze_loader.py with proper paths...")
        
        # Import and run the bronze loader directly
        import bronze_layer.bronze_loader
        
        print("✅ Bronze loader completed successfully!")
        return 0
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
