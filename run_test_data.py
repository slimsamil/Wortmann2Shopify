#!/usr/bin/env python3
"""
Simple script to run the test data insertion
"""

import sys
import os

# Add the app directory to the Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'app'))

try:
    from test_data import insert_test_data
    print("🧪 Starting test data insertion...")
    insert_test_data()
    print("✅ Test data insertion completed!")
    print("\n📋 Next steps:")
    print("1. Start your API: docker-compose up")
    print("2. Test the workflow: curl -X POST http://localhost:8000/test-workflow")
    print("3. Check the response to see if the test data is processed correctly")
    
except ImportError as e:
    print(f"❌ Import error: {e}")
    print("Make sure you're running this from the project root directory")
except Exception as e:
    print(f"❌ Error: {e}")
    print("Check your database connection and configuration") 