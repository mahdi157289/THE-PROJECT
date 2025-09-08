#!/usr/bin/env python3
"""
Debug Waitress + Flask integration
"""

print("🔍 Step 1: Testing imports...")

try:
    from waitress import serve
    print("✅ Waitress imported successfully")
except ImportError as e:
    print(f"❌ Waitress import failed: {e}")
    exit(1)

try:
    from app import app
    print("✅ Flask app imported successfully")
except ImportError as e:
    print(f"❌ Flask app import failed: {e}")
    exit(1)

print(f"🔍 Step 2: Checking Flask app routes...")
print(f"Routes found: {[rule.rule for rule in app.url_map.iter_rules()]}")

print(f"🔍 Step 3: Starting Waitress server...")
print("📍 Server will be available at: http://localhost:5000")
print("🔗 Test endpoints:")
print("   - http://localhost:5000/")
print("   - http://localhost:5000/api/health")
print("\nPress Ctrl+C to stop the server")

# Start Waitress server
serve(app, host='127.0.0.1', port=5000, threads=4)
