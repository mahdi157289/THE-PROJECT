#!/usr/bin/env python3
"""
Test Flask app with built-in development server
"""

from app import app

if __name__ == '__main__':
    print("🚀 Starting Flask development server...")
    print("📍 Backend will be available at: http://localhost:5000")
    print("🔗 API Health Check: http://localhost:5000/api/health")
    print("\nPress Ctrl+C to stop the server")
    
    # Use Flask's built-in development server
    app.run(host='127.0.0.1', port=5000, debug=True)
