#!/usr/bin/env python3
"""
🌐 Simple HTTP Server for AI Bot Test Page
Serves the test_ai_bot_browser.html file with proper CORS headers
"""

import http.server
import socketserver
import os
import webbrowser
from urllib.parse import urlparse

class CORSHTTPRequestHandler(http.server.SimpleHTTPRequestHandler):
    """Custom handler that adds CORS headers"""
    
    def end_headers(self):
        # Add CORS headers
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type, Authorization')
        self.send_header('Access-Control-Allow-Credentials', 'true')
        super().end_headers()
    
    def do_OPTIONS(self):
        # Handle preflight requests
        self.send_response(200)
        self.end_headers()

def serve_test_page():
    """Start HTTP server and open test page"""
    
    # Change to the directory containing the HTML file
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    
    PORT = 8080
    
    print("🌐 Starting HTTP Server for AI Bot Test Page...")
    print(f"📁 Serving from: {os.getcwd()}")
    print(f"🔗 Server URL: http://localhost:{PORT}")
    print(f"🧪 Test Page: http://localhost:{PORT}/test_ai_bot_browser.html")
    
    try:
        with socketserver.TCPServer(("", PORT), CORSHTTPRequestHandler) as httpd:
            print(f"✅ Server running on port {PORT}")
            print("🚀 Opening test page in browser...")
            
            # Open the test page in the default browser
            webbrowser.open(f'http://localhost:{PORT}/test_ai_bot_browser.html')
            
            print("\n💡 Instructions:")
            print("1. Make sure your backend is running: python server.py")
            print("2. The test page should open automatically")
            print("3. If not, go to: http://localhost:8080/test_ai_bot_browser.html")
            print("4. Press Ctrl+C to stop this server")
            
            httpd.serve_forever()
            
    except KeyboardInterrupt:
        print("\n🛑 Server stopped by user")
    except OSError as e:
        if e.errno == 98:  # Address already in use
            print(f"❌ Port {PORT} is already in use. Try a different port.")
        else:
            print(f"❌ Error starting server: {e}")

if __name__ == "__main__":
    serve_test_page()

