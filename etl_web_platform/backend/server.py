#!/usr/bin/env python3
"""
Bourse de Tunis ETL Platform Backend
Production server using Waitress WSGI server
"""

from waitress import serve
from app import app
import os

if __name__ == '__main__':
    print("🚀 Starting Bourse de Tunis ETL Platform Backend with Waitress...")
    print("📍 Backend will be available at: http://localhost:5000")
    print("🔗 API Health Check: http://localhost:5000/api/health")
    print("📊 ETL Status: http://localhost:5000/api/etl/status")
    print("🕷️  Scraping Status: http://localhost:5000/api/scraping/status")
    print("💻 System Stats: http://localhost:5000/api/system/stats")
    print("\nPress Ctrl+C to stop the server")
    
    # Use Waitress instead of Flask's built-in server
    serve(app, host='127.0.0.1', port=5000, threads=4)
