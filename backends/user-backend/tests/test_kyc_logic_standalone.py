
import sys
import os
import asyncio
from unittest.mock import MagicMock, patch
from fastapi import HTTPException

# Add app to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Import AFTER path setup
from app.api import auto_pilot_proxy
from app.api.auto_pilot_proxy import deploy_auto_pilot, DeployAutoPilotRequest, AllocationParams

async def run_test():
    print("Running KYC Gating Test...")
    
    # 1. Setup Request
    req_body = DeployAutoPilotRequest(
        broker_account_ids=["acc_1"],
        risk_mode="medium",
        allocation=AllocationParams(total_capital_budget=1000, trade_amount_per_position=100),
        execution_mode="live", # CRITICAL
        market_type="forex",   # CRITICAL
        forex_config={"allowlist": ["EUR_USD"]}
    )
    
    # 2. Setup User (Unverified)
    unverified_user = {
        "id": "u1",
        "is_verified": False # CRITICAL
    }
    
    # 3. Setup User (Verified)
    verified_user = {
        "id": "u2",
        "is_verified": True
    }
    
    # Mock Request object (not used in logic but required by sig)
    mock_req = MagicMock()
    
    # TEST 1: Unverified User -> Should Fail
    try:
        # We assume proxy_request will be called if check passes, so we mock it to avoid actual network call
        with patch("app.api.auto_pilot_proxy.proxy_request", new_callable=MagicMock) as mock_proxy:
            # We also need to mock get_decrypted_credentials to satisfy the credential injection loop
            with patch("app.core.broker_service.get_decrypted_credentials", return_value={"api_key": "k"}):
                 await deploy_auto_pilot(mock_req, req_body, unverified_user)
        
        print("❌ FAILED: Unverified user did NOT raise exception")
        sys.exit(1)
        
    except HTTPException as e:
        if e.status_code == 403 and "verification" in str(e.detail):
            print("✅ SUCCESS: Unverified user blocked (403)")
        else:
            print(f"❌ FAILED: Wrong exception raised: {e}")
            sys.exit(1)
            
    # TEST 2: Verified User -> Should Pass (mock proxy called)
    try:
        from unittest.mock import AsyncMock
        with patch("app.api.auto_pilot_proxy.proxy_request", new_callable=AsyncMock) as mock_proxy:
            # Mock return of proxy call
            mock_proxy.return_value = {"status": "ok"}
            with patch("app.core.broker_service.get_decrypted_credentials", return_value={"api_key": "k"}):
                await deploy_auto_pilot(mock_req, req_body, verified_user)
                
        print("✅ SUCCESS: Verified user allowed")
        
    except Exception as e:
        print(f"❌ FAILED: Verified user raised exception: {e}")
        sys.exit(1)

    print("All tests passed.")

if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    loop.run_until_complete(run_test())
