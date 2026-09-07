import sys
import os
import asyncio
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.api.brokers import (
    get_broker_catalog, 
    start_connection, 
    submit_credentials, 
    validate_connection,
    ConnectRequest, 
    CredentialsRequest
)

async def test_flow():
    print("--- Testing Broker Catalog ---")
    catalog = await get_broker_catalog()
    ibkr = next((b for b in catalog.brokers if b.id == "ibkr"), None)
    if ibkr:
        print("PASS: IBKR found in catalog")
        print(f"Auth fields: {[f.name for f in ibkr.auth_fields]}")
    else:
        print("FAIL: IBKR not found")
        return

    print("\n--- Testing Connection Flow ---")
    # 1. Start Connection
    connect_req = ConnectRequest(broker_id="ibkr", market_type="forex")
    conn_res = await start_connection(connect_req)
    account_id = conn_res.account_id
    print(f"PASS: Started connection, Account ID: {account_id}, Status: {conn_res.status}")

    # 2. Submit Credentials (Empty for IBKR as per new requirement)
    # The frontend sending {} is valid.
    creds_req = CredentialsRequest(credentials={}) 
    try:
        sub_res = await submit_credentials(account_id, creds_req)
        print(f"PASS: Credentials submitted (Empty): {sub_res}")
    except Exception as e:
        print(f"FAIL: Credentials submission error: {e}")

    # 3. Validate (should fail/mock depending on implementation)
    # Since we don't have a real gateway, we expect it to try and fail or if we mocked it...
    # In my implementation: 
    # test-connection relies on IBKRAdapter. 
    # validate_connection relies on IBKRAdapter.
    # I should expect an error or I need to mock IBKRAdapter.
    
    # We will just run it and see if it crashes.
    print("Attempting validation (expected to fail without Gateway)...")
    res = await validate_connection(account_id)
    print(f"Validation Result: {res}")

if __name__ == "__main__":
    asyncio.run(test_flow())
