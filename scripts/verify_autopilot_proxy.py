
import sys
import os
import asyncio
from unittest.mock import MagicMock, patch
from pathlib import Path

# Setup path
grandparent = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(grandparent / "backends" / "user-backend"))
sys.path.insert(0, str(grandparent))

from app.api.auto_pilot_proxy import deploy_auto_pilot, DeployAutoPilotRequest, AllocationParams

async def test_autopilot_proxy():
    print("1. Setup Mocks...")
    
    # Mock Request
    mock_request = MagicMock()
    
    # Mock User
    mock_user = {"id": "test_user_123"}
    
    # Mock Broker Service
    with patch("app.core.broker_service.get_decrypted_credentials") as mock_get_creds:
        mock_get_creds.return_value = {"api_token": "decrypted_token"}
        
        # Mock Proxy Request
        with patch("app.api.auto_pilot_proxy.proxy_request") as mock_proxy:
            mock_proxy.return_value = {"status": "success", "data": []}
            
            print("2. Constructing Payload...")
            body = DeployAutoPilotRequest(
                broker_account_ids=["brk_1", "brk_2"],
                risk_mode="aggressive",
                allocation=AllocationParams(total_capital_budget=1000, trade_amount_per_position=100),
                execution_mode="paper",
                market_type="forex",
                forex_config={"allowlist": ["EUR_USD"]}
            )
            
            print("3. Calling deploy_auto_pilot...")
            await deploy_auto_pilot(mock_request, body, mock_user)
            
            print("4. Verifying Backend Payload...")
            args, kwargs = mock_proxy.call_args
            backend_payload = kwargs["json_body"]
            
            # Checks
            assert backend_payload["risk_level"] == "aggressive"
            assert backend_payload["market_type"] == "forex"
            assert backend_payload["forex_config"] == {"allowlist": ["EUR_USD"]}
            assert "broker_credentials_map" in backend_payload
            assert backend_payload["broker_credentials_map"]["brk_1"] == {"api_token": "decrypted_token"}
            assert backend_payload["broker_credentials_map"]["brk_2"] == {"api_token": "decrypted_token"}
            
            print("   SUCCESS: Credentials injected and params mapped correctly.")

if __name__ == "__main__":
    try:
        asyncio.run(test_autopilot_proxy())
    except Exception as e:
        print(f"FAILED: {e}")
        import traceback
        traceback.print_exc()
