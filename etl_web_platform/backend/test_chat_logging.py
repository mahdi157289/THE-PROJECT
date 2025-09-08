#!/usr/bin/env python3
"""
Test script to verify enhanced chat logging
Makes HTTP requests to see user messages and bot responses in terminal
"""
import requests
import json
import time

# Configuration
BASE_URL = "http://127.0.0.1:5000"
CHAT_ENDPOINT = f"{BASE_URL}/api/ai/chat"
SUGGESTIONS_ENDPOINT = f"{BASE_URL}/api/ai/suggestions"

def test_chat_logging():
    """Test chat endpoint with enhanced logging"""
    print("🧪 TESTING ENHANCED CHAT LOGGING")
    print("=" * 60)
    
    # Test messages
    test_messages = [
        "What is BVMT?",
        "How to invest in BVMT?",
        "Tell me about Tunisian companies"
    ]
    
    for i, message in enumerate(test_messages, 1):
        print(f"\n📝 Test {i}: Sending message to chat endpoint...")
        
        try:
            # Make HTTP request to chat endpoint
            response = requests.post(
                CHAT_ENDPOINT,
                json={
                    "message": message,
                    "session_id": f"test_session_{i}"
                },
                headers={'Content-Type': 'application/json'},
                timeout=10
            )
            
            if response.status_code == 200:
                data = response.json()
                print(f"✅ Response received successfully!")
                print(f"📊 Response length: {len(data['response'])} characters")
                print(f"🕒 Timestamp: {data['timestamp']}")
                print(f"🆔 Session ID: {data['session_id']}")
            else:
                print(f"❌ Error: {response.status_code} - {response.text}")
                
        except Exception as e:
            print(f"❌ Request failed: {e}")
        
        time.sleep(1)  # Small delay between requests
    
    # Test suggestions endpoint
    print(f"\n📝 Testing suggestions endpoint...")
    try:
        response = requests.get(SUGGESTIONS_ENDPOINT, timeout=10)
        if response.status_code == 200:
            data = response.json()
            print(f"✅ Suggestions received: {len(data['suggestions'])} items")
        else:
            print(f"❌ Suggestions error: {response.status_code}")
    except Exception as e:
        print(f"❌ Suggestions request failed: {e}")
    
    print("\n🎉 CHAT LOGGING TEST COMPLETED!")
    print("Check the backend terminal for enhanced logging output!")

if __name__ == "__main__":
    test_chat_logging() 