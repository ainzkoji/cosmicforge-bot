import sys
import os
import time
import json
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any

# Add app to path - assuming execution from backends/user-backend/tests or root
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

# Also add shared_lib if needed (it's in backends/shared)
shared_lib_path = os.path.abspath(os.path.join(parent_dir, "..", "shared"))
sys.path.append(shared_lib_path)

try:
    from app.core import mt_pairing_service
    from app.core import broker_service
    from shared_lib.persistence.db import DB
except ImportError as e:
    print(f"Import Error: {e}")
    print(f"Sys Path: {sys.path}")
    sys.exit(1)

# Mock Users
USER_A = "user_e2e_a"
USER_B = "user_e2e_b"

def setup_db():
    print("--- SETUP ---")
    db = DB()
    # Clean up previous run
    with db.connect() as conn:
        conn.execute("DELETE FROM mt_pairing_sessions WHERE user_id IN (?, ?)", (USER_A, USER_B))
        conn.execute("DELETE FROM broker_accounts WHERE user_id IN (?, ?)", (USER_A, USER_B))
        # Note: broker_credentials cascade delete usually not set up, so manual cleanup required if strict
        # But for E2E verification logs, this is likely sufficient as we check account IDs
    print("Cleaned up previous test data.")

def run_e1_ui_flow():
    print("\n=== E1) UI Flow Test ===")
    
    # 1. User clicks Connect MT5 -> Generate pairing code
    print(f"[User A] Requesting MT5 pairing session...")
    try:
        session = mt_pairing_service.create_pairing_session(USER_A, "mt5", environment="demo")
    except Exception as e:
        print(f"FAILED to create session: {e}")
        return

    code = session["pairing_code"]
    print(f"  -> Generated Pairing Code: {code}")
    print(f"  -> Session ID: {session['session_id']}")
    
    if "setup_link" in session:
        print(f"  -> Setup Link: {session['setup_link']}")

    # 2. Simulate Polling (Frontend)
    status = mt_pairing_service.get_pairing_session(code, USER_A)
    print(f"[Frontend] Polling status for {code}: {status['status']}")
    if status['status'] != "pending":
         print(f"FAILED: Expected pending, got {status['status']}")
         return

    # 3. Connector runs, detects account, calls complete_pairing
    print(f"[Connector] Detected MT5 Account 1001 on Demo Server")
    print(f"[Connector] Completing pairing with code {code}...")
    
    try:
        # Step 3a: Complete Pairing (Bridge Connect)
        session_id_from_pair = mt_pairing_service.complete_pairing(
            pairing_code=code,
            bridge_url="https://vps-a.example.com:8443",
            bridge_token="token_user_a_secure_123",
            tls_mode="strict",
            mt_platform="mt5",
            account_login="1001",
            server="MetaQuotes-Demo",
            account_currency="USD",
            account_type="Demo"
        )
        print(f"  -> Pairing Connected! Session: {session_id_from_pair}")
        
        # Step 3b: Finish Pairing (Frontend Action)
        print(f"[Frontend] Verifying and Finishing...")
        account_id = mt_pairing_service.finish_pairing(session['session_id'], USER_A)
        print(f"  -> Finish Success! Account ID: {account_id}")

    except Exception as e:
        print(f"  -> FAILED: {e}")
        return

    # 4. UI shows Connected
    status_after = mt_pairing_service.get_pairing_session(code, USER_A)
    print(f"[Frontend] Polling status for {code}: {status_after['status']}")
    
    if status_after['status'] == "paired":
        print(f"  -> PASSED: Status is paired.")
        acc = status_after.get('account', {})
        print(f"  -> Account Login: {acc.get('login')}")
        print(f"  -> Server: {acc.get('server')}")
    else:
        print(f"  -> FAILED: Expected paired, got {status_after['status']}")
    
    # Verify Broker Account Created
    try:
        accounts = broker_service.list_user_broker_accounts(USER_A)
        account = next((a for a in accounts if a["id"] == account_id), None)
        print(f"[Backend] Broker Account Verification:")
        if account:
            print(f"  -> Found Account: {account['label']} (Status: {account['status']})")
        else:
            print(f"  -> FAILED: Broker account not found!")
    except Exception as e:
        print(f"  -> FAILED listing accounts: {e}")

def run_e2_multi_user():
    print("\n=== E2) Multi-User Isolation Test ===")
    
    # User B pairs bridge B
    print(f"[User B] Requesting MT4 pairing session...")
    try:
        session_b = mt_pairing_service.create_pairing_session(USER_B, "mt4", environment="live")
        code_b = session_b["pairing_code"]
        print(f"  -> Generated Pairing Code: {code_b}")
        
        print(f"[Connector] Completing pairing for User B...")
        # 3a. Bridge Connect
        mt_pairing_service.complete_pairing(
            pairing_code=code_b,
            bridge_url="https://vps-b.example.com:8443",
            bridge_token="token_user_b_secure_456",
            tls_mode="strict",
            mt_platform="mt4",
            account_login="2002",
            server="Live-Server-B",
            account_currency="EUR",
            account_type="Real"
        )
        
        # 3b. Finish
        print(f"[Frontend] Finishing User B...")
        account_id_b = mt_pairing_service.finish_pairing(session_b["session_id"], USER_B)
        print(f"  -> User B Paired. Account ID: {account_id_b}")
        
        # Verify User A cannot see User B's account
        print(f"[Security Check] Verifying Isolation...")
        accounts_a = broker_service.list_user_broker_accounts(USER_A)
        accounts_b = broker_service.list_user_broker_accounts(USER_B)
        
        ids_a = [a['id'] for a in accounts_a]
        ids_b = [a['id'] for a in accounts_b]
        
        print(f"  -> User A Accounts: {ids_a}")
        print(f"  -> User B Accounts: {ids_b}")
        
        if account_id_b in ids_a:
            print("  -> FAILED: User A can see User B's account!")
        else:
            print("  -> PASSED: User A cannot see User B's account.")
            
    except Exception as e:
        print(f"FAILED E2: {e}")

def run_e3_failure_cases():
    print("\n=== E3) Failure Cases Test ===")
    
    # 1. Expired Code
    print(f"[Test] Simulating Expired Code...")
    try:
        session = mt_pairing_service.create_pairing_session(USER_A, "mt5")
        code = session["pairing_code"]
        
        # Manually expire it in DB
        db = DB()
        with db.connect() as conn:
            expired_time = (datetime.now(timezone.utc) - timedelta(minutes=11)).isoformat()
            conn.execute("UPDATE mt_pairing_sessions SET expires_at = ? WHERE pairing_code = ?", (expired_time, code))
        
        print(f"  -> Generated code {code} and expired it manually.")
        
        try:
            mt_pairing_service.complete_pairing(
                pairing_code=code,
                bridge_url="...", bridge_token="...", tls_mode="strict", mt_platform="mt5",
                account_login="999", server="Test", account_currency="USD", account_type="Demo"
            )
            print("  -> FAILED: Expired code was accepted!")
        except ValueError as e:
            if "expired" in str(e).lower():
                print(f"  -> PASSED: Connector rejected with error: '{e}'")
            else:
                 print(f"  -> WARNING: Rejected but with unexpected error: '{e}'")

        # 2. Invalid Code
        print(f"[Test] Simulating Invalid Code...")
        try:
            mt_pairing_service.complete_pairing(
                pairing_code="INVALID-CODE",
                bridge_url="...", bridge_token="...", tls_mode="strict", mt_platform="mt5",
                account_login="999", server="Test", account_currency="USD", account_type="Demo"
            )
            print("  -> FAILED: Invalid code was accepted!")
        except ValueError as e:
             print(f"  -> PASSED: Connector rejected with error: '{e}'")
             
    except Exception as e:
         print(f"FAILED E3: {e}")

if __name__ == "__main__":
    setup_db()
    run_e1_ui_flow()
    run_e2_multi_user()
    run_e3_failure_cases()
    print("\n=== ALL TESTS COMPLETED ===")
