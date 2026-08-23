import sys
import os
import requests
import json

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

# First, let's try to get a valid token
print("=" * 50)
print("TESTING STRATEGY API ENDPOINT")
print("=" * 50)

# Try without auth first
print("\n1. Testing WITHOUT authentication:")
try:
    response = requests.get("http://localhost:8000/api/strategies/")
    print(f"   Status: {response.status_code}")
    print(f"   Response: {response.text[:200]}")
except Exception as e:
    print(f"   Error: {e}")

# Try to login and get a token
print("\n2. Trying to login to get a valid token:")
try:
    # Try common test credentials
    login_data = {
        "username": "test@example.com",
        "password": "password"
    }
    response = requests.post(
        "http://localhost:8000/auth/login",
        data=login_data,
        headers={"Content-Type": "application/x-www-form-urlencoded"}
    )
    print(f"   Login Status: {response.status_code}")
    if response.status_code == 200:
        token_data = response.json()
        print(f"   Got token!")
        access_token = token_data.get("access_token")
        
        # Now try with the token
        print("\n3. Testing WITH authentication:")
        headers = {"Authorization": f"Bearer {access_token}"}
        response = requests.get("http://localhost:8000/api/strategies/", headers=headers)
        print(f"   Status: {response.status_code}")
        print(f"   Response: {response.text}")
        
        if response.status_code == 200:
            data = response.json()
            print(f"\n   SUCCESS! Got {len(data)} strategies:")
            for s in data:
                print(f"     - {s.get('name', 'Unknown')}")
    else:
        print(f"   Login failed: {response.text}")
except Exception as e:
    print(f"   Error: {e}")

# Also test the service layer directly
print("\n4. Testing service layer directly:")
from shared_lib.persistence.db import DB
from app.core.strategy_service import StrategyService

db = DB()
service = StrategyService(db)
try:
    strategies = service.list_strategies(user_id="test_user", filters={}, limit=10)
    print(f"   Service returned {len(strategies)} strategies")
    for s in strategies:
        print(f"     - {s['name']}")
except Exception as e:
    print(f"   Service error: {e}")
    import traceback
    traceback.print_exc()
