#!/usr/bin/env python3
"""
Test script for Hugging Face API connectivity and permissions
"""

import requests
import json

# Configuration
API_KEY = "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"
MODEL_URL = "https://api-inference.huggingface.co/pipeline/text-generation/Qwen/Qwen2.5-3B-Instruct"

def test_connectivity():
    """Test basic network connectivity"""
    print("🔍 Testing network connectivity...")
    try:
        import socket
        ip = socket.gethostbyname("api-inference.huggingface.co")
        print(f"✅ DNS resolution successful: {ip}")
        return True
    except Exception as e:
        print(f"❌ DNS resolution failed: {e}")
        return False

def test_api_permissions():
    """Test API key permissions"""
    print("\n🔍 Testing API key permissions...")
    
    headers = {
        "Authorization": f"Bearer {API_KEY}",
        "Content-Type": "application/json"
    }
    
    # Test with a simple payload
    payload = {
        "inputs": "Hello, how are you?"
    }
    
    try:
        print(f"🌐 Making request to: {MODEL_URL}")
        print(f"🔑 Using API key: {API_KEY[:10]}...")
        
        response = requests.post(MODEL_URL, headers=headers, json=payload, timeout=30)
        
        print(f"📊 Response Status: {response.status_code}")
        print(f"📋 Response Headers: {dict(response.headers)}")
        
        if response.status_code == 200:
            print("✅ API call successful!")
            try:
                result = response.json()
                print(f"📄 Response type: {type(result)}")
                print(f"📄 Response content: {str(result)[:500]}...")
                return True
            except Exception as e:
                print(f"❌ JSON parsing failed: {e}")
                print(f"📄 Raw response: {response.text[:500]}...")
                return False
        else:
            print(f"❌ API call failed with status {response.status_code}")
            print(f"📄 Error response: {response.text[:500]}...")
            return False
            
    except Exception as e:
        print(f"❌ Request failed: {e}")
        return False

def test_simple_model():
    """Test with a simpler, more accessible model"""
    print("\n🔍 Testing with simpler model...")
    
    # Try a different model that might have different permissions
    simple_model = "https://api-inference.huggingface.co/models/gpt2"
    
    headers = {
        "Authorization": f"Bearer {API_KEY}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "inputs": "Hello"
    }
    
    try:
        response = requests.post(simple_model, headers=headers, json=payload, timeout=30)
        print(f"📊 Simple model response: {response.status_code}")
        
        if response.status_code == 200:
            print("✅ Simple model works!")
            return True
        else:
            print(f"❌ Simple model failed: {response.text[:200]}...")
            return False
            
    except Exception as e:
        print(f"❌ Simple model test failed: {e}")
        return False

def main():
    """Run all tests"""
    print("🚀 Hugging Face API Test Suite")
    print("=" * 40)
    
    # Test 1: Network connectivity
    if not test_connectivity():
        print("\n❌ Network connectivity failed. Check your internet connection.")
        return
    
    # Test 2: API permissions
    if test_api_permissions():
        print("\n✅ API is working correctly!")
    else:
        print("\n❌ API permissions issue detected.")
        
        # Test 3: Try simpler model
        if test_simple_model():
            print("\n💡 Simple model works - permission issue with specific model")
        else:
            print("\n❌ All models failing - API key permission issue")
            
        print("\n🔧 Solutions:")
        print("1. Check your Hugging Face API key permissions")
        print("2. Enable 'Inference' permissions for your token")
        print("3. Try creating a new token with proper permissions")

if __name__ == "__main__":
    main()
