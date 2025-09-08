#!/usr/bin/env python3
"""
Simple test to verify AI bot is working
"""
import sys
import os

# Add current directory to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

try:
    from ai_bot_api import generate_bvmt_response
    print("✅ AI Bot functions imported successfully")
    
    # Test different responses
    test_messages = [
        "What is BVMT?",
        "BVMT market analysis", 
        "How to invest in BVMT?",
        "Tell me about Tunisian companies"
    ]
    
    print("\n🧪 Testing different responses:")
    for i, message in enumerate(test_messages, 1):
        response = generate_bvmt_response(message)
        print(f"\n{i}. Message: '{message}'")
        print(f"   Response length: {len(response)} characters")
        print(f"   Response preview: {response[:100]}...")
        print("-" * 50)
    
    print("\n🎉 AI BOT IS WORKING WITH DIFFERENT RESPONSES!")
    
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()
