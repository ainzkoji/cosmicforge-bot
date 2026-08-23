import sys
import os
import time
import json
import logging
from datetime import datetime, timedelta, timezone

# Add app to path
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

# Add shared_lib
shared_lib_path = os.path.abspath(os.path.join(parent_dir, "..", "shared"))
sys.path.append(shared_lib_path)

try:
    from app.core import mt_pairing_service
    from app.core import broker_service
    from shared_lib.persistence.db import DB
except ImportError as e:
    print(f"Import Error: {e}")
    sys.exit(1)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

USER_ID = "user_new_flow_test"
DEVICE_SECRET = "test_device_secret_123"

def setup_db():
    print("--- SETUP ---")
    db = DB()
    with db.connect() as conn:
        conn.execute("DELETE FROM mt_pairing_sessions WHERE user_id = ?", (USER_ID,))
        conn.execute("DELETE FROM broker_accounts WHERE user_id = ?", (USER_ID,))
    print("Cleaned up previous test data.")

def run_flow_test():
    print("\n=== Testing New MT Pairing Flow ===")
    
    # 1. Frontend: Start Session
    print(f"[Frontend] Requesting MT5 pairing session...")
    session_data = mt_pairing_service.create_pairing_session(USER_ID, "mt5", "demo")
    session_id = session_data["session_id"]
    print(f"  -> Session Created. ID: {session_id}")
    
    # 2. Connector: Claim Code
    print(f"[Connector] Claiming pairing code with Session ID...")
    try:
        claim_data = mt_pairing_service.claim_pairing_session(session_id, DEVICE_SECRET)
        pairing_code = claim_data["pairing_code"]
        print(f"  -> Claimed Code: {pairing_code}")
    except Exception as e:
        print(f"  -> FAILED to claim code: {e}")
        return

    # 3. Connector: Complete Pairing
    print(f"[Connector] Connecting bridge and completing pairing...")
    try:
        # Note: complete_pairing no longer returns account_id, but session_id (or similar)
        # And it DOES NOT create the broker account yet.
        res = mt_pairing_service.complete_pairing(
            pairing_code=pairing_code,
            bridge_url="https://vps-test.example.com:8443",
            bridge_token="token_secure_test_xyz",
            tls_mode="strict",
            mt_platform="mt5",
            account_login="5005",
            server="MetaQuotes-Demo",
            account_currency="USD",
            account_type="Demo"
        )
        print(f"  -> Bridge Connected. Result: {res}")
    except Exception as e:
        print(f"  -> FAILED to pair: {e}")
        return

    # 4. Frontend: Verify Status (Optional polling step)
    print(f"[Frontend] Polling status...")
    # Use the new method that supports polling by ID (which is what the frontend has)
    status = mt_pairing_service.get_session_by_id(session_id, USER_ID)
    print(f"  -> Status: {status['status']}")
    
    if status['status'] != "paired":
         print(f"  -> FAILED: Expected paired, got {status['status']}")
         # Continue anyway to test finish logic which might fail if status is wrong
    else:
         print(f"  -> PASSED: Status is paired.") 
    
    # 5. Frontend: Finish Pairing
    print(f"[Frontend] Finishing pairing (creating account)...")
    try:
        account_id = mt_pairing_service.finish_pairing(session_id, USER_ID)
        print(f"  -> Pairing Finished! Account ID: {account_id}")
    except Exception as e:
        print(f"  -> FAILED to finish: {e}")
        return

    # 6. Verify Broker Account
    print(f"[Backend] Verifying Account Creation...")
    accounts = broker_service.list_user_broker_accounts(USER_ID)
    acc = next((a for a in accounts if a["id"] == account_id), None)
    if acc:
        print(f"  -> PASSED: Account found: {acc['label']}")
    else:
        print(f"  -> FAILED: Account not found in DB.")

if __name__ == "__main__":
    setup_db()
    run_flow_test()
