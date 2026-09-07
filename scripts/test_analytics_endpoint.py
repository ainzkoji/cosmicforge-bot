import requests
import json

# Test the analytics overview endpoint
url = "http://localhost:8000/api/v1/analytics/overview?timeframe=YTD"

# You'll need to replace this with a real token
token = "your_access_token_here"  # Get from browser localStorage

headers = {
    "Authorization": f"Bearer {token}"
}

try:
    response = requests.get(url, headers=headers)
    print(f"Status Code: {response.status_code}")
    print(f"Response: {json.dumps(response.json(), indent=2)}")
except Exception as e:
    print(f"Error: {e}")
    print(f"Response text: {response.text if 'response' in locals() else 'No response'}")
