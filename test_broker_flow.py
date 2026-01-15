"""
Test script for broker connection flow
Tests the broker service layer directly without authentication
"""
import sys
import os

# Add backend to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'backend'))

from app.core import broker_service
from app.persistence.db import DB
import json

def test_broker_catalog():
    """Test 1: Get broker catalog"""
    print("=" * 60)
    print("TEST 1: Broker Catalog")
    print("=" * 60)
    
    catalog = broker_service.get_broker_catalog("test_user")
    print(f"✓ Found {len(catalog)} supported brokers:")
    for broker in catalog:
        print(f"  - {broker['display_name']} ({broker['broker_id']})")
        print(f"    Markets: {', '.join(broker['market_types'])}")
        print(f"    Permissions: {', '.join(broker['required_permissions'])}")
    print()
    return catalog

def test_create_account():
    """Test 2: Create broker account draft"""
    print("=" * 60)
    print("TEST 2: Create Broker Account Draft")
    print("=" * 60)
    
    try:
        account_id = broker_service.create_broker_account_draft(
            user_id="test_user_123",
            broker_id="binance",
            market_type="crypto",
            label="Test Binance Account"
        )
        print(f"✓ Created account: {account_id}")
        print()
        return account_id
    except Exception as e:
        print(f"✗ Error: {e}")
        print()
        return None

def test_submit_credentials(account_id):
    """Test 3: Submit credentials (with fake data)"""
    print("=" * 60)
    print("TEST 3: Submit Broker Credentials")
    print("=" * 60)
    
    if not account_id:
        print("✗ Skipped (no account_id)")
        print()
        return False
    
    try:
        fake_credentials = {
            "api_key": "test_fake_api_key_12345",
            "api_secret": "test_fake_secret_12345",
            "environment": "demo"
        }
        
        success = broker_service.submit_broker_credentials(
            user_id="test_user_123",
            account_id=account_id,
            credentials=fake_credentials
        )
        
        if success:
            print(f"✓ Credentials submitted and encrypted")
            
            # Verify encryption
            db = DB()
            with db.connect() as conn:
                row = conn.execute(
                    "SELECT encrypted_blob, key_metadata FROM broker_credentials WHERE account_id = ?",
                    (account_id,)
                ).fetchone()
                
                if row:
                    print(f"✓ Found encrypted credentials in database")
                    print(f"  Encryption method: {row['key_metadata']}")
                    print(f"  Encrypted blob length: {len(row['encrypted_blob'])} chars")
                    
                # Check masked key
                account_row = conn.execute(
                    "SELECT masked_key, status FROM broker_accounts WHERE id = ?",
                    (account_id,)
                ).fetchone()
                
                if account_row:
                    print(f"✓ Account status: {account_row['status']}")
                    print(f"✓ Masked key: {account_row['masked_key']}")
        else:
            print("✗ Failed to submit credentials")
        
        print()
        return success
    except Exception as e:
        print(f"✗ Error: {e}")
        import traceback
        traceback.print_exc()
        print()
        return False

def test_list_accounts():
    """Test 4: List user broker accounts"""
    print("=" * 60)
    print("TEST 4: List User Broker Accounts")
    print("=" * 60)
    
    try:
        accounts = broker_service.list_user_broker_accounts("test_user_123")
        print(f"✓ Found {len(accounts)} account(s)")
        for account in accounts:
            print(f"\n  Account: {account['label']}")
            print(f"    ID: {account['id']}")
            print(f"    Broker: {account['broker_id']}")
            print(f"    Status: {account['status']}")
            print(f"    Masked Key: {account.get('masked_key', 'N/A')}")
            print(f"    Environment: {account.get('environment', 'N/A')}")
        print()
        return accounts
    except Exception as e:
        print(f"✗ Error: {e}")
        print()
        return []

def test_disconnect(account_id):
    """Test 5: Disconnect broker account"""
    print("=" * 60)
    print("TEST 5: Disconnect Broker Account")
    print("=" * 60)
    
    if not account_id:
        print("✗ Skipped (no account_id)")
        print()
        return
    
    try:
        # First verify credentials exist
        db = DB()
        with db.connect() as conn:
            before = conn.execute(
                "SELECT COUNT(*) as count FROM broker_credentials WHERE account_id = ?",
                (account_id,)
            ).fetchone()['count']
            print(f"  Credentials before disconnect: {before}")
        
        # Disconnect
        success = broker_service.disconnect_broker_account(
            user_id="test_user_123",
            account_id=account_id
        )
        
        if success:
            print(f"✓ Account disconnected")
            
            # Verify credentials deleted
            with db.connect() as conn:
                after = conn.execute(
                    "SELECT COUNT(*) as count FROM broker_credentials WHERE account_id = ?",
                    (account_id,)
                ).fetchone()['count']
                print(f"✓ Credentials after disconnect: {after} (should be 0)")
                
                # Check account status
                account = conn.execute(
                    "SELECT status FROM broker_accounts WHERE id = ?",
                    (account_id,)
                ).fetchone()
                if account:
                    print(f"✓ Account status: {account['status']} (should be 'disconnected')")
        else:
            print("✗ Failed to disconnect")
        
        print()
    except Exception as e:
        print(f"✗ Error: {e}")
        import traceback
        traceback.print_exc()
        print()

def main():
    print("\n")
    print("🔧 BROKER CONNECTION SERVICE TEST")
    print("=" * 60)
    print()
    
    # Run tests
    catalog = test_broker_catalog()
    account_id = test_create_account()
    test_submit_credentials(account_id)
    accounts = test_list_accounts()
    test_disconnect(account_id)
    
    print("=" * 60)
    print("✅ ALL TESTS COMPLETED")
    print("=" * 60)
    print()

if __name__ == "__main__":
    main()
