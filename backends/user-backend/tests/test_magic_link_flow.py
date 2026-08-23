#!/usr/bin/env python3
"""
End-to-End test for Magic Link Connector Flow
Tests the complete workflow from frontend to connector.
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from shared_lib.persistence.db import DB
from app.core import mt_pairing_service
import time

def test_magic_link_flow():
    print("=" * 60)
    print("  MAGIC LINK CONNECTOR FLOW TEST")
    print("=" * 60)
    print()
    
    # Test setup
    test_user = "test_magic_link_user"
    broker_id = "mt5"
    environment = "demo"
    
    # Cleanup
    db = DB()
    with db.connect() as conn:
        conn.execute("DELETE FROM mt_pairing_sessions WHERE user_id = ?", (test_user,))
    
    print("Step 1: User clicks 'Connect MT5' in frontend")
    print("-" * 60)
    
    # Frontend creates session
    session = mt_pairing_service.create_pairing_session(test_user, broker_id, environment)
    
    print(f"✓ Session created:")
    print(f"  - Session ID: {session['session_id']}")
    print(f"  - Pairing Code: {session['pairing_code']}")
    print(f"  - Connector Token: {session['connector_link_token'][:20]}...")
    print(f"  - Setup Link: {session['setup_link']}")
    print(f"  - Expires: {session['expires_at']}")
    print()
    
    print("Step 2: Frontend displays setup link to user")
    print("-" * 60)
    print(f"Frontend UI shows: {session['setup_link']}")
    print("User copies this link and pastes it in connector")
    print()
    
    print("Step 3: Connector claims the token")
    print("-" * 60)
    
    # Connector calls claim endpoint
    claimed_session = mt_pairing_service.get_session_by_connector_token(
        session['connector_link_token']
    )
    
    if claimed_session:
        print(f"✓ Connector authenticated:")
        print(f"  - Platform: {claimed_session['broker_id']}")
        print(f"  - Environment: {claimed_session['environment']}")
        print(f"  - Pairing Code: {claimed_session['pairing_code']}")
    else:
        print("✗ Failed to claim token!")
        return False
    
    print()
    
    print("Step 4: Connector completes pairing")
    print("-" * 60)
    
    # Connector calls complete pairing
    try:
        account_id = mt_pairing_service.complete_pairing(
            pairing_code=claimed_session['pairing_code'],
            bridge_url="https://test-tunnel.trycloudflare.com",
            bridge_token="test_token_abc123",
            tls_mode="strict",
            mt_platform="mt5",
            account_login="99999",
            server="MetaQuotes-Demo",
            account_currency="USD",
            account_type="Demo"
        )
        print(f"✓ Pairing completed successfully!")
        print(f"  - Account ID: {account_id}")
    except Exception as e:
        print(f"✗ Pairing failed: {e}")
        return False
    
    print()
    
    print("Step 5: Frontend polls and sees 'connected'")
    print("-" * 60)
    
    # Frontend polls status
    status = mt_pairing_service.get_pairing_session(
        claimed_session['pairing_code'],
        test_user
    )
    
    if status['status'] == 'paired':
        print(f"✓ Frontend sees paired status:")
        print(f"  - Account Login: {status.get('account', {}).get('login')}")
        print(f"  - Server: {status.get('account', {}).get('server')}")
        print(f"  - Platform: {status.get('account', {}).get('platform')}")
    else:
        print(f"✗ Unexpected status: {status['status']}")
        return False
    
    print()
    print("=" * 60)
    print("  ✅ MAGIC LINK FLOW TEST PASSED")
    print("=" * 60)
    
    # Clean up
    with db.connect() as conn:
        conn.execute("DELETE FROM mt_pairing_sessions WHERE user_id = ?", (test_user,))
        conn.execute("DELETE FROM broker_accounts WHERE user_id = ?", (test_user,))
    
    return True

if __name__ == "__main__":
    try:
        success = test_magic_link_flow()
        sys.exit(0 if success else 1)
    except Exception as e:
        print(f"\n❌ CRITICAL FAILURE: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
