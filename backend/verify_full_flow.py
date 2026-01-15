import requests
import uuid
import sys
import sqlite3

BASE_URL = "http://localhost:8000"

def run_flow():
    session = requests.Session()
    unique = uuid.uuid4().hex[:6]
    email = f"test_{unique}@example.com"
    password = "Password123!"
    
    print(f"1. Registering user {email}...")
    try:
        res = session.post(f"{BASE_URL}/auth/register", json={
            "email": email,
            "password": password
        })
        if res.status_code != 200:
            print(f"FAILED to register: {res.text}")
            return
        user_data = res.json()
        print("   Registered.")
        
        # Manually verify user in DB
        print("   Manually verifying user in DB...")
        with sqlite3.connect("data/bot.db") as conn:
            conn.execute("UPDATE users SET is_verified = 1, status = 'active' WHERE email = ?", (email,))
            conn.commit()
    except Exception as e:
        print(f"FAILED to connect: {e}")
        return

    print("2. Logging in...")
    res = session.post(f"{BASE_URL}/auth/login", data={
        "username": email,
        "password": password
    })
    if res.status_code != 200:
        print(f"FAILED to login: {res.text}")
        return
    token = res.json()["access_token"]
    headers = {"Authorization": f"Bearer {token}"}
    print("   Logged in.")

    print("3. Starting KYC Case...")
    res = session.post(f"{BASE_URL}/kyc/start", headers=headers)
    if res.status_code != 200 and "already exists" not in res.text:
        print(f"FAILED to start KYC: {res.text}")
        return
    print("   KYC Started.")

    print("4. Submitting Personal Info...")
    res = session.post(f"{BASE_URL}/kyc/personal-info", headers=headers, json={
        "full_legal_name": "Test User",
        "date_of_birth": "1990-01-01",
        "nationality": "US",
        "country_of_residence": "US",
        "address_line1": "123 Test St",
        "address_city": "Test City",
        "address_postal_code": "12345"
    })
    if res.status_code != 200:
        print(f"FAILED personal info: {res.text}")
        return
    print("   Personal Info Submitted.")
    
    print("5. Getting Upload URL (Front)...")
    res = session.post(f"{BASE_URL}/kyc/documents/upload-url", headers=headers, json={
        "doc_type": "drivers_license",
        "side": "front"
    })
    if res.status_code != 200:
        print(f"FAILED get upload url: {res.text}")
        return
    data = res.json()
    upload_url = data["upload_url"] # This is relative /kyc/documents/upload/...
    doc_id = data["doc_id"]
    file_ref = data["file_ref"]
    print(f"   Got URL: {upload_url}")

    print("6. Uploading File (using new endpoint)...")
    # Prepend base url if relative
    full_upload_url = f"{BASE_URL}{upload_url}"
    try:
        res = session.put(full_upload_url, data=b"fakeimagecontent", headers={"Content-Type": "image/png"})
        if res.status_code != 200:
            print(f"FAILED upload put: {res.status_code} {res.text}")
            return
        print("   Upload PUT success.")
    except Exception as e:
         print(f"FAILED upload put exception: {e}")
         return

    print("7. Confirming Upload...")
    res = session.post(f"{BASE_URL}/kyc/documents/confirm", headers=headers, json={
        "doc_id": doc_id,
        "file_ref": file_ref,
        "side": "front",
        "file_size_bytes": 16,
        "content_type": "image/png"
    })
    if res.status_code != 200:
         print(f"FAILED confirm: {res.text}")
         return
    print("   Upload confirmed.")

    print("8. Starting Face Verification...")
    res = session.post(f"{BASE_URL}/kyc/face/start", headers=headers, json={"provider": "internal"})
    if res.status_code != 200:
        print(f"FAILED face start: {res.text}")
        return
    face_data = res.json()
    ref = face_data["selfie_upload_ref"]
    print(f"   Face started, ref: {ref}")

    print("9. Completing Face Verification...")
    res = session.post(f"{BASE_URL}/kyc/face/complete", headers=headers, json={
        "selfie_file_ref": ref,
        "passed": True
    })
    if res.status_code != 200:
        print(f"FAILED face complete: {res.text}")
        return
    print("   Face verification COMPLETED successfully.")
    
    print("10. Submitting KYC...")
    res = session.post(f"{BASE_URL}/kyc/submit", headers=headers)
    if res.status_code != 200:
         print(f"FAILED submit: {res.text}")
         return
    print("   KYC Submitted & Approved.")

if __name__ == "__main__":
    run_flow()
