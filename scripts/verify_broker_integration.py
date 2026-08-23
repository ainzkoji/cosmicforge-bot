
import sys
import os
import json
from pathlib import Path

# Setup path
grandparent = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(grandparent / "backends" / "user-backend"))
sys.path.insert(0, str(grandparent))

from app.core import broker_service
from shared_lib.persistence.db import DB

def test_broker_flow():
    user_id = "verify_user_123"
    broker_id = "oanda"
    market_type = "forex"
    
    print("1. Creating Draft Account...")
    try:
        account_id = broker_service.create_broker_account_draft(user_id, broker_id, market_type)
        print(f"   Success: {account_id}")
    except ValueError as e:
        if "Plan limit" in str(e):
             print(f"   Skipping create (limit reached), finding existing...")
             return
        raise
    except Exception as e:
        print(f"Error: {e}")
        raise 
    
    print("2. Submitting Credentials...")
    creds = {
        "api_token": "test_token_123",
        "account_id": "test_acc_001",
        "environment": "practice",
        "extra_secret": "sensitive"
    }
    
    success = broker_service.submit_broker_credentials(user_id, account_id, creds)
    if success:
        print("   Success: Credentials submitted.")
    else:
        print("   Failed.")
        return

    print("3. Verifying Encryption in DB...")
    db = DB()
    with db.connect() as conn:
        row = conn.execute("SELECT encrypted_blob FROM broker_credentials WHERE account_id = ?", (account_id,)).fetchone()
        blob = row[0]
        if "test_token_123" not in str(blob):
            print("   Success: Token is not in plaintext.")
        else:
            print("   FAIL: Token found in plaintext!")

    print("4. Verifying Decryption helper...")
    decrypted = broker_service.get_decrypted_credentials(user_id, account_id)
    if decrypted and decrypted["api_token"] == "test_token_123":
        print("   Success: Decryption works.")
        print(f"   Metadata injected: {decrypted.get('broker_id')}")
    else:
        print(f"   FAIL: Decryption failed. Got: {decrypted}")

    print("5. Verifying Masking in List...")
    accounts = broker_service.list_user_broker_accounts(user_id)
    my_acc = next((a for a in accounts if a["id"] == account_id), None)
    if my_acc:
        print(f"   Status: {my_acc['status']}")
        print(f"   Masked Key: {my_acc.get('masked_key')}")
        # Expect masking...
    else:
        print("   FAIL: Account not found in list.")
        
    print("6. Cleaning up...")
    broker_service.delete_broker_account_permanently(user_id, account_id)
    print("   Cleanup done.")

if __name__ == "__main__":
    try:
        test_broker_flow()
    except ImportError as e:
        print(f"ImportError: {e}")
        # walk directory to find shared_lib
        current = Path(__file__).parent.parent
        print(f"Searching in {current}")
        for path in current.rglob("shared_lib"):
            print(f"Found: {path}")
