import sys
import os
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

# Add shared lib to path (assuming standard structure)
shared_path = Path(__file__).parent.parent.parent / "shared"
sys.path.insert(0, str(shared_path))

from fastapi.testclient import TestClient
from app.main import app
from app.api.auth import get_current_active_user

# --- Mock Auth ---
TEST_USER_ID = "verify_onb_user_01"

def mock_get_current_active_user():
    return {"id": TEST_USER_ID, "email": "test@example.com", "is_active": True}

app.dependency_overrides[get_current_active_user] = mock_get_current_active_user
client = TestClient(app)

def run_verification():
    print(f"--- Starting Onboarding Verification for User {TEST_USER_ID} ---")

    # 1. Start / Check State
    print("\n[1] Checking Initial State...")
    resp = client.get("/api/onboarding/state")
    if resp.status_code != 200:
        print(f"FAILED: {resp.text}")
        return
    state = resp.json()
    print(f"Current Status: {state['status']}")
    print(f"Current Step: {state.get('current_step')}")

    # 2. Welcome Step
    print("\n[2] Submitting Welcome...")
    resp = client.post("/api/onboarding/step", json={
        "step": "welcome",
        "data": {"accepted_terms": True}
    })
    print(f"Response: {resp.status_code} - {resp.json()}")

    # 3. Experience (Beginner)
    print("\n[3] Submitting Experience (Beginner)...")
    resp = client.post("/api/onboarding/step", json={
        "step": "experience",
        "data": {"experience_level": "beginner"}
    })
    print(f"Response: {resp.status_code} - {resp.json()}")

    # 4. Risk (High - Should be clamped later)
    print("\n[4] Submitting Risk (High)...")
    resp = client.post("/api/onboarding/step", json={
        "step": "risk",
        "data": {"risk_tolerance": "high"}
    })
    print(f"Response: {resp.status_code} - {resp.json()}")

    # 5. Strategy (Safe Trend)
    print("\n[5] Submitting Strategy...")
    resp = client.post("/api/onboarding/step", json={
        "step": "strategy",
        "data": {"strategy_id": "safe_trend"}
    })
    print(f"Response: {resp.status_code} - {resp.json()}")

    # 6. Allocation (Check Limits)
    print("\n[6.a] Submitting Allocation (INVALID > 80%)...")
    # High risk profile has max 80%, but let's try 90%
    resp = client.post("/api/onboarding/step", json={
        "step": "allocation",
        "data": {"amount": 90.0, "type": "percentage"}
    })
    print(f"Response (Should Fail): {resp.status_code} - {resp.json()}")

    print("\n[6.b] Submitting Allocation (VALID 20%)...")
    resp = client.post("/api/onboarding/step", json={
        "step": "allocation",
        "data": {"amount": 20.0, "type": "percentage"}
    })
    print(f"Response: {resp.status_code} - {resp.json()}")

    # 7. Complete & Check Clamping
    print("\n[7] Completing Onboarding...")
    resp = client.post("/api/onboarding/complete")
    if resp.status_code == 200:
        result = resp.json()
        policy = result["risk_policy"]
        print("SUCCESS! Generated Blueprint:")
        print(f"Strategy: {result['strategy_name']}")
        print(f"Risk Profile ID: {policy['id']}")
        print(f"Max Leverage (Clamped?): {policy['max_leverage']} (Expected 1 for beginner)")
        print(f"Max Daily Loss: {policy['max_daily_loss_pct']}%")
        
        if policy['max_leverage'] == 1:
            print(">>> VERIFICATION PASSED: Risk was correctly clamped for beginner.")
        else:
            print(">>> VERIFICATION FAILED: Leverage was not clamped.")
    else:
        print(f"FAILED to complete: {resp.text}")

if __name__ == "__main__":
    run_verification()
