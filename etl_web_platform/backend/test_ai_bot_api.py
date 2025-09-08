#!/usr/bin/env python3
"""
🧪 AI Bot API Test Script
Tests all AI bot endpoints and functionality
"""

import requests
import json
import time
from datetime import datetime

# Configuration
BASE_URL = "http://127.0.0.1:5000"
AI_API_BASE = f"{BASE_URL}/api/ai"

def print_header(title):
    """Print a formatted header"""
    print(f"\n{'='*60}")
    print(f"🧪 {title}")
    print(f"{'='*60}")

def print_result(test_name, response, expected_status=200):
    """Print test result"""
    status_icon = "✅" if response.status_code == expected_status else "❌"
    print(f"{status_icon} {test_name}")
    print(f"   Status: {response.status_code}")
    
    if response.status_code == expected_status:
        try:
            data = response.json()
            print(f"   Response: {json.dumps(data, indent=2)[:200]}...")
        except:
            print(f"   Response: {response.text[:200]}...")
    else:
        print(f"   Error: {response.text}")
    
    print()

def test_basic_endpoints():
    """Test basic AI bot endpoints"""
    print_header("BASIC AI BOT ENDPOINTS")
    
    # Test 1: Get Suggestions
    try:
        response = requests.get(f"{AI_API_BASE}/suggestions")
        print_result("Get Suggestions", response)
    except Exception as e:
        print(f"❌ Get Suggestions - Connection Error: {e}")
    
    # Test 2: Basic Chat
    try:
        chat_data = {
            "message": "What is BVMT?",
            "session_id": "test_session_001"
        }
        response = requests.post(f"{AI_API_BASE}/chat", json=chat_data)
        print_result("Basic Chat - What is BVMT?", response)
    except Exception as e:
        print(f"❌ Basic Chat - Connection Error: {e}")
    
    # Test 3: Market Analysis Chat
    try:
        chat_data = {
            "message": "BVMT market analysis",
            "session_id": "test_session_002"
        }
        response = requests.post(f"{AI_API_BASE}/chat", json=chat_data)
        print_result("Market Analysis Chat", response)
    except Exception as e:
        print(f"❌ Market Analysis Chat - Connection Error: {e}")
    
    # Test 4: Investment Advice Chat
    try:
        chat_data = {
            "message": "How to invest in BVMT?",
            "session_id": "test_session_003"
        }
        response = requests.post(f"{AI_API_BASE}/chat", json=chat_data)
        print_result("Investment Advice Chat", response)
    except Exception as e:
        print(f"❌ Investment Advice Chat - Connection Error: {e}")

def test_advanced_endpoints():
    """Test advanced AI bot endpoints"""
    print_header("ADVANCED AI BOT ENDPOINTS")
    
    # Test 1: Intelligence Status
    try:
        response = requests.get(f"{AI_API_BASE}/intelligence-status")
        print_result("Intelligence Status", response)
    except Exception as e:
        print(f"❌ Intelligence Status - Connection Error: {e}")
    
    # Test 2: Market Intelligence
    try:
        response = requests.get(f"{AI_API_BASE}/market-intelligence")
        print_result("Market Intelligence", response)
    except Exception as e:
        print(f"❌ Market Intelligence - Connection Error: {e}")
    
    # Test 3: Smart Suggestions
    try:
        response = requests.get(f"{AI_API_BASE}/suggestions-smart")
        print_result("Smart Suggestions", response)
    except Exception as e:
        print(f"❌ Smart Suggestions - Connection Error: {e}")
    
    # Test 4: User Profile
    try:
        response = requests.get(f"{AI_API_BASE}/user-profile/test_user_001")
        print_result("User Profile", response)
    except Exception as e:
        print(f"❌ User Profile - Connection Error: {e}")
    
    # Test 5: Health Check
    try:
        response = requests.get(f"{AI_API_BASE}/health-smart")
        print_result("Health Check", response)
    except Exception as e:
        print(f"❌ Health Check - Connection Error: {e}")

def test_chat_scenarios():
    """Test different chat scenarios"""
    print_header("CHAT SCENARIOS TESTING")
    
    test_messages = [
        "What is BVMT?",
        "Tell me about Tunisian companies",
        "BVMT market analysis",
        "How to invest in BVMT?",
        "Which sectors perform well in Tunisia?",
        "BVMT trading hours",
        "Tunisian stock market trends",
        "Latest BVMT news"
    ]
    
    for i, message in enumerate(test_messages, 1):
        try:
            chat_data = {
                "message": message,
                "session_id": f"scenario_test_{i:03d}"
            }
            response = requests.post(f"{AI_API_BASE}/chat", json=chat_data)
            print_result(f"Chat Scenario {i}: {message[:30]}...", response)
            time.sleep(0.5)  # Small delay between requests
        except Exception as e:
            print(f"❌ Chat Scenario {i} - Connection Error: {e}")

def test_error_handling():
    """Test error handling"""
    print_header("ERROR HANDLING TESTING")
    
    # Test 1: Empty message
    try:
        chat_data = {
            "message": "",
            "session_id": "error_test_001"
        }
        response = requests.post(f"{AI_API_BASE}/chat", json=chat_data)
        print_result("Empty Message (Should return 400)", response, 400)
    except Exception as e:
        print(f"❌ Empty Message Test - Connection Error: {e}")
    
    # Test 2: Invalid endpoint
    try:
        response = requests.get(f"{AI_API_BASE}/invalid-endpoint")
        print_result("Invalid Endpoint (Should return 404)", response, 404)
    except Exception as e:
        print(f"❌ Invalid Endpoint Test - Connection Error: {e}")
    
    # Test 3: Malformed JSON
    try:
        response = requests.post(f"{AI_API_BASE}/chat", 
                               data="invalid json", 
                               headers={'Content-Type': 'application/json'})
        print_result("Malformed JSON (Should return 400)", response, 400)
    except Exception as e:
        print(f"❌ Malformed JSON Test - Connection Error: {e}")

def test_performance():
    """Test API performance"""
    print_header("PERFORMANCE TESTING")
    
    # Test multiple rapid requests
    start_time = time.time()
    successful_requests = 0
    total_requests = 10
    
    for i in range(total_requests):
        try:
            chat_data = {
                "message": f"Performance test message {i}",
                "session_id": f"perf_test_{i:03d}"
            }
            response = requests.post(f"{AI_API_BASE}/chat", json=chat_data)
            if response.status_code == 200:
                successful_requests += 1
        except Exception as e:
            print(f"❌ Performance test {i} - Error: {e}")
    
    end_time = time.time()
    total_time = end_time - start_time
    avg_response_time = total_time / total_requests
    
    print(f"✅ Performance Test Results:")
    print(f"   Total Requests: {total_requests}")
    print(f"   Successful: {successful_requests}")
    print(f"   Success Rate: {(successful_requests/total_requests)*100:.1f}%")
    print(f"   Total Time: {total_time:.2f}s")
    print(f"   Average Response Time: {avg_response_time:.2f}s")

def test_etl_integration():
    """Test integration with ETL data"""
    print_header("ETL INTEGRATION TESTING")
    
    # Test if we can access ETL data through the AI bot
    etl_endpoints = [
        "/api/scraping/status",
        "/api/bronze/status", 
        "/api/silver/status",
        "/api/golden/status",
        "/api/diamond/status"
    ]
    
    for endpoint in etl_endpoints:
        try:
            response = requests.get(f"{BASE_URL}{endpoint}")
            print_result(f"ETL {endpoint.split('/')[-2].upper()} Status", response)
        except Exception as e:
            print(f"❌ ETL {endpoint} - Connection Error: {e}")

def main():
    """Main test function"""
    print("🚀 AI BOT API COMPREHENSIVE TEST SUITE")
    print(f"⏰ Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"🎯 Testing against: {BASE_URL}")
    
    # Check if server is running
    try:
        response = requests.get(f"{BASE_URL}/", timeout=5)
        print(f"✅ Server is running (Status: {response.status_code})")
    except Exception as e:
        print(f"❌ Server is not running or not accessible: {e}")
        print("💡 Make sure to start the backend server with: python server.py")
        return
    
    # Run all tests
    test_basic_endpoints()
    test_advanced_endpoints()
    test_chat_scenarios()
    test_error_handling()
    test_performance()
    test_etl_integration()
    
    print_header("TEST SUITE COMPLETED")
    print(f"⏰ Finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("🎉 All tests completed! Check results above.")

if __name__ == "__main__":
    main()

