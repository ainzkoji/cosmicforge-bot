import requests
import sys

try:
    response = requests.post(
        "http://127.0.0.1:8001/auth/login",
        data={
            "username": "favourdan027@gmail.com",
            "password": "password123"
        }
    )
    print(f"Status Code: {response.status_code}")
    print(f"Response Body: {response.text}")
except Exception as e:
    print(f"Request failed: {e}")
