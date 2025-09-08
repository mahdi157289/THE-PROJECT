#!/usr/bin/env python3
"""
Working Waitress + Flask Server
"""

import sys
import os

# Add the current directory to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

print("🔍 Step 1: Setting up Python path...")
print(f"Current directory: {os.getcwd()}")
print(f"Python path: {sys.path[:3]}")

print("\n🔍 Step 2: Importing dependencies...")

try:
    from waitress import serve
    print("✅ Waitress imported successfully")
except ImportError as e:
    print(f"❌ Waitress import failed: {e}")
    print("💡 Install with: pip install waitress")
    exit(1)

try:
    from app import app
    print("✅ Flask app imported successfully")
except ImportError as e:
    print(f"❌ Flask app import failed: {e}")
    print(f"💡 Current working directory: {os.getcwd()}")
    print(f"💡 Files in directory: {os.listdir('.')}")
    exit(1)

print(f"\n🔍 Step 3: Checking Flask app routes...")
routes = [rule.rule for rule in app.url_map.iter_rules()]
for route in routes:
    print(f"   ✅ {route}")

print(f"\n🚀 Step 4: Starting Waitress server...")
print("📍 Server will be available at: http://localhost:5000")
print("🔗 Test endpoints:")
print("   - http://localhost:5000/")
print("   - http://localhost:5000/api/health")
print("   - http://localhost:5000/api/etl/status")
print("\nPress Ctrl+C to stop the server")

# Start Waitress server
serve(app, host='127.0.0.1', port=5000, threads=4)
