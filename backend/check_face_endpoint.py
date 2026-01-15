import requests
import sys

def check_face_endpoint():
    url = "http://localhost:8000/kyc/face/complete"
    print(f"Checking endpoint: {url} (POST)")
    
    try:
        # We expect 401 because we offer no token
        # If it hangs, we know server is blocked
        response = requests.post(url, json={}, timeout=5)
        print(f"Response Status Code: {response.status_code}")
        
        if response.status_code == 401:
            print("RESULT: 401 Unauthorized -> Server is RESPONSIVE.")
        else:
            print(f"RESULT: Unexpected status {response.status_code}.")
            
    except requests.exceptions.Timeout:
        print("RESULT: Timeout -> Server is HANGING/BLOCKED.")
    except Exception as e:
        print(f"Error connecting to server: {e}")

if __name__ == "__main__":
    check_face_endpoint()
