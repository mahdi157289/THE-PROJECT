#!/usr/bin/env python3
"""
Simple wrapper script to run bronze loader with correct environment
This avoids import path issues by setting up the environment properly
"""

import os
import sys
import subprocess

def main():
    print("🚀 Starting Bronze Layer with simple wrapper...")
    
    # Get the current script directory
    current_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Add the config directory to Python path
    config_path = os.path.join(current_dir, '..', 'config')
    if config_path not in sys.path:
        sys.path.insert(0, config_path)
    
    # Add the ETL directory to Python path
    etl_path = os.path.join(current_dir, 'ETL__MAIDALLION')
    if etl_path not in sys.path:
        sys.path.insert(0, etl_path)
    
    print(f"📁 Config path: {config_path}")
    print(f"📁 ETL path: {etl_path}")
    
    try:
        # Change to the ETL directory
        os.chdir(etl_path)
        print(f"📂 Changed to directory: {os.getcwd()}")
        
        # Run the bronze loader
        print("🚀 Running bronze_loader.py...")
        result = subprocess.run(
            [sys.executable, 'bronze_layer/bronze_loader.py'],
            capture_output=True,
            text=True,
            cwd=etl_path
        )
        
        print(f"✅ Bronze loader completed with return code: {result.returncode}")
        
        if result.stdout:
            print("📤 Standard output:")
            print(result.stdout)
        
        if result.stderr:
            print("❌ Error output:")
            print(result.stderr)
            
        return result.returncode
        
    except Exception as e:
        print(f"❌ Error running bronze loader: {e}")
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
