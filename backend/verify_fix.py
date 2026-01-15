import requests
import sys

def check_server_status():
    url = "http://localhost:8000/kyc/documents/upload/test_check?expires=123&sig=abc"
    print(f"Checking endpoint: {url} (PUT)")
    
    try:
        response = requests.put(url)
        print(f"Response Status Code: {response.status_code}")
        
        if response.status_code == 404:
            print("RESULT: 404 Not Found -> Server is LIKELY STALE (Restart Required).")
            print("The endpoint was not found, meaning the new code is not running.")
        elif response.status_code == 403:
            print("RESULT: 403 Forbidden -> Server is UPDATED.")
            print("The endpoint exists but rejected the invalid signature (Expected).")
        else:
            print(f"RESULT: Unexpected status {response.status_code}.")
            
    except Exception as e:
        print(f"Error connecting to server: {e}")

if __name__ == "__main__":
    check_server_status()
