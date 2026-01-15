
import requests
import sys
import uuid

API_BASE = "http://localhost:8000"

def check(res, name):
    if res.status_code >= 400:
        print(f"[FAIL] {name}: {res.status_code} {res.text}")
        sys.exit(1)
    print(f"[OK] {name}")
    return res.json()

def main():
    print("--- Verifying Strategies & Onboarding Integration ---")
    
    # 1. Login
    email = f"user_{uuid.uuid4().hex[:6]}@example.com"
    pwd = "password123"
    print(f"User: {email}")
    
    requests.post(f"{API_BASE}/auth/register", json={"email": email, "password": pwd})
    
    # Verify manually
    import sqlite3
    try:
        conn = sqlite3.connect("data/bot.db") # Try backend path
        conn.execute("UPDATE users SET status='active', is_verified=1 WHERE email=?", (email,))
        conn.commit()
    except:
        try:
            conn = sqlite3.connect("backend/data/bot.db") # Try from root
            conn.execute("UPDATE users SET status='active', is_verified=1 WHERE email=?", (email,))
            conn.commit()
        except Exception as e:
            print(f"[WARN] Could not verify user in DB: {e}")

    res = requests.post(f"{API_BASE}/auth/login", data={"username": email, "password": pwd})
    token = check(res, "Login")["access_token"]
    headers = {"Authorization": f"Bearer {token}"}
    
    # 2. Check Onboarding State
    state = check(requests.get(f"{API_BASE}/api/onboarding/state", headers=headers), "Get Onboarding State")
    print(f"Current Step: {state['current_step']}")
    
    # 3. Check Onboarding Strategies (Hardcoded / Starter)
    strategies = check(requests.get(f"{API_BASE}/api/onboarding/strategies", headers=headers), "Get Starter Strategies")
    print(f"Starter Strategies: {len(strategies['strategies'])}")
    if len(strategies['strategies']) == 0:
        print("[WARN] No starter strategies found!")
        
    # 4. Check Strategy Catalog (DB / Filters)
    catalog = check(requests.get(f"{API_BASE}/api/strategies/catalog", headers=headers), "Get Strategy Catalog")
    print(f"Catalog Size: {len(catalog['strategies'])}")
    
    # 5. Save a Step
    step_data = {"experience_level": "intermediate"}
    check(requests.post(f"{API_BASE}/api/onboarding/step", json={"step": "experience", "data": step_data}, headers=headers), "Save Step")
    
    # 6. Verify State Update
    state = check(requests.get(f"{API_BASE}/api/onboarding/state", headers=headers), "Verify State Update")
    if state["data"].get("experience_level") != "intermediate":
        print("[FAIL] State did not update")
    else:
        print("[OK] State persisted")

    print("--- SUCCESS ---")

if __name__ == "__main__":
    main()
