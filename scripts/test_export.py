import requests
import json
import traceback

# Test the analytics export endpoint
url = "http://localhost:8000/api/v1/analytics/export?timeframe=YTD&format=csv"

# You'll need to replace this with a real token if auth is enabled and enforced
# Assuming the previous script worked without it or I need to mock it. 
# The previous script had a placeholder. I will try to use the one from the user's header if I could, 
# but for now I'll just try to hit it. If 401, I'll know it's auth. 
# But the user got 500, so Auth passed (or isn't enforced for this testing).

token = "your_access_token_here" 

headers = {
    "Authorization": f"Bearer {token}"
}

try:
    print(f"Sending request to {url}...")
    response = requests.get(url, headers=headers)
    print(f"Status Code: {response.status_code}")
    if response.status_code != 200:
        print(f"Response: {response.text}")
    else:
        print("Success! Response content length:", len(response.content))
        print("First 100 bytes:", response.content[:100])
except Exception as e:
    print(f"Error: {e}")
    traceback.print_exc()
