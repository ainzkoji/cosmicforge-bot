"""
Test script for broker connection flow

This script simulates the entire broker connection flow:
1. Fetch broker catalog
2. Create draft account
3. Submit credentials
4. Validate connection
5. List connected accounts

NOTE: This requires a valid user_id and real API keys to fully test.
For testing without real keys, it will show the flow but fail at validation.
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from app.core.broker_service import (
    get_broker_catalog,
    create_broker_account_draft,
    submit_broker_credentials,
    validate_broker_account,
    list_user_broker_accounts,
    disconnect_broker_account
)

def test_broker_flow():
    print("=" * 60)
    print("BROKER CONNECTION FLOW TEST")
    print("=" * 60)
    
    # Step 1: Get Catalog
    print("\n1. Fetching Broker Catalog...")
    try:
        catalog = get_broker_catalog("test_user_123")
        print(f"   ✅ Found {len(catalog)} brokers:")
        for broker in catalog:
            print(f"      - {broker['display_name']} ({broker['broker_id']})")
            print(f"        Features: {', '.join(broker['features'])}")
    except Exception as e:
        print(f"   ❌ Error: {e}")
        return
    
    # Step 2: Create Draft (mock - requires billing system)
    print("\n2. Creating Draft Broker Account (Binance)...")
    try:
        # This will fail if billing/subscription system isn't initialized
        # but shows the flow
        account_id = create_broker_account_draft(
            user_id="test_user_123",
            broker_id="binance",
            market_type="crypto",
            label="My Binance Account"
        )
        print(f"   ✅ Created draft account: {account_id}")
        
        # Step 3: Submit Credentials (mock)
        print("\n3. Submitting Credentials...")
        mock_credentials = {
            "api_key": "test_key_12345",
            "api_secret": "test_secret_67890",
            "environment": "demo"
        }
        
        success = submit_broker_credentials(
            user_id="test_user_123",
            account_id=account_id,
            credentials=mock_credentials
        )
        
        if success:
            print(f"   ✅ Credentials submitted successfully")
        else:
            print(f"   ❌ Failed to submit credentials")
            return
        
        # Step 4: Validate Connection
        print("\n4. Validating Connection...")
        print("   Note: This will fail with test credentials but shows the flow")
        result = validate_broker_account(
            user_id="test_user_123",
            account_id=account_id
        )
        
        if result["success"]:
            print(f"   ✅ Validation successful")
            print(f"      Status: {result['status']}")
            print(f"      Capabilities: {result['capabilities']}")
        else:
            print(f"   ⚠️  Validation failed (expected with test keys)")
            print(f"      Error: {result.get('error')}")
        
        # Step 5: List Accounts
        print("\n5. Listing User Broker Accounts...")
        accounts = list_user_broker_accounts("test_user_123")
        print(f"   ✅ Found {len(accounts)} account(s)")
        for acc in accounts:
            print(f"      - {acc['label']} ({acc['broker_id']}) - Status: {acc['status']}")
        
        # Cleanup
        print("\n6. Cleanup - Disconnecting Account...")
        disconnect_success = disconnect_broker_account("test_user_123", account_id)
        if disconnect_success:
            print(f"   ✅ Account disconnected")
        
    except ValueError as e:
        if "Plan limit reached" in str(e):
            print(f"   ⚠️  Billing system not initialized: {e}")
            print(f"   💡 This is expected if running migration hasn't created subscription")
        else:
            print(f"   ❌ Error: {e}")
    except Exception as e:
        print(f"   ❌ Error: {e}")
        import traceback
        traceback.print_exc()
    
    print("\n" + "=" * 60)
    print("TEST COMPLETE")
    print("=" * 60)

if __name__ == "__main__":
    test_broker_flow()
