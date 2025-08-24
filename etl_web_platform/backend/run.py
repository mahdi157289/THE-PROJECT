#!/usr/bin/env python3
"""
Bourse de Tunis ETL Platform Backend
Startup script for development
"""

# Import the Flask app directly
from app import app

if __name__ == '__main__':
    print("🚀 Starting Bourse de Tunis ETL Platform Backend...")
    print("📍 Backend will be available at: http://localhost:5000")
    print("🔗 API Health Check: http://localhost:5000/api/health")
    print("📊 ETL Status: http://localhost:5000/api/etl/status")
    print("🕷️  Scraping Status: http://localhost:5000/api/scraping/status")
    print("💻 System Stats: http://localhost:5000/api/system/stats")
    print("\nPress Ctrl+C to stop the server")
    
    # Run the Flask app
    app.run(
        host='0.0.0.0',
        port=5000,
        debug=True
    )
