
import requests
import time
import sys
import uuid

API_BASE = "http://localhost:8000"

def r_check(res, name):
    if res.status_code >= 400:
        print(f"[FAIL] {name}: {res.status_code} {res.text}")
        sys.exit(1)
    print(f"[OK] {name}")
    return res.json()

def main():
    print("--- Starting Broker Flow Verification ---")
    
    # 1. Register/Login
    email = f"broker_test_{uuid.uuid4().hex[:6]}@example.com"
    pwd = "password123"
    
    print(f"Creating user: {email}")
    res = requests.post(f"{API_BASE}/auth/register", json={"email": email, "password": pwd})
    r_check(res, "Register")
    
    # Manually verify (simulated)
    # We can skip email verification if we just login? No, login checks active.
    # We use the hack from before or trigger verify endpoint if possible.
    # Actually, let's login then see if we hit a wall.
    
    # Login usually requires verification. Let's use the DB hack or assume we can verify.
    # Or... we can use `verify_email` if we stick the code?
    # Simpler: Use the verify_full_flow.py logic to update DB.
    
    import sqlite3
    db_path = "data/bot.db"
    conn = sqlite3.connect(db_path)
    conn.execute("UPDATE users SET status='active', is_verified=1 WHERE email=?", (email,))
    conn.commit()
    conn.close()
    print("[OK] Manually set user to active in DB")

    res = requests.post(f"{API_BASE}/auth/login", data={"username": email, "password": pwd})
    token = r_check(res, "Login")["access_token"]
    headers = {"Authorization": f"Bearer {token}"}
    
    # 2. Get Catalog
    res = requests.get(f"{API_BASE}/api/brokers", headers=headers)
    cat = r_check(res, "Get Catalog")
    if not cat["brokers"]:
        print("[FAIL] Catalog empty")
        sys.exit(1)
    
    broker_id = cat["brokers"][0]["id"]
    print(f"Selected Broker: {broker_id}")
    
    # 3. Start Connection
    payload = {
        "broker_id": broker_id,
        "market_type": "crypto",
        "label": "My Test Broker"
    }
    res = requests.post(f"{API_BASE}/api/brokers/connect/start", json=payload, headers=headers)
    draft = r_check(res, "Start Connection")
    account_id = draft["account_id"]
    print(f"Draft Account ID: {account_id}")
    
    # 4. Submit Credentials
    creds = {
        "credentials": {
            "api_key": "test_key",
            "api_secret": "test_secret",
            "environment": "demo"
        }
    }
    # Note: frontend sends { credentials: { ... } } structure check?
    # client.ts: submitBrokerCredentials(accountId, credentials) -> fetch body: JSON.stringify(credentials)
    # Backend expects BrokerCredentialsSubmit which has api_key, api_secret fields at top level?
    # Let's check backend schema.
    # Backend api/broker.py: submit_credentials(creds: BrokerCredentialsSubmit)
    # BrokerCredentialsSubmit definition?
    # If client.ts sends { credentials: {...} } then backend needs to unwrap or frontend needs to change.
    
    # Client.ts refactor sent:
    # submitBrokerCredentials: async (accountId, credentials) => { ... body: JSON.stringify(credentials) }
    # BrokerConnection.tsx called: 
    # api.submitBrokerCredentials(accountId!, { ...creds, environment })
    # So the body sent is { "api_key": "...", "api_secret": "...", "environment": "..." }
    
    # My manual script should match that.
    res = requests.post(f"{API_BASE}/api/brokers/connect/{account_id}/submit", json=creds["credentials"], headers=headers)
    r_check(res, "Submit Credentials")
    
    # 5. Validate (Mock)
    res = requests.post(f"{API_BASE}/api/brokers/connect/{account_id}/validate", headers=headers)
    r_check(res, "Validate Connection")
    
    # 6. List Accounts
    res = requests.get(f"{API_BASE}/api/brokers/accounts", headers=headers)
    list_acc = r_check(res, "List Accounts")
    
    found = False
    for acc in list_acc:
        if acc["id"] == account_id:
            found = True
            print(f"Found account: {acc['status']}")
            break
            
    if not found:
        print("[FAIL] Account not found in list")
        sys.exit(1)

    print("--- SUCCESS: Broker Flow Verified ---")

if __name__ == "__main__":
    main()
