"""
Integration Tests for Auto Pilot

Verifies the Auto Pilot deployment flow.
"""
import pytest
import requests
import uuid

# Test configuration
USER_BACKEND_URL = "http://localhost:8000"
BOT_BACKEND_URL = "http://localhost:9000"
TEST_TIMEOUT = 10

@pytest.fixture(scope="module")
def auth_token() -> str:
    # return "dummy_token" if no real login available
    return "dummy_token_for_testing"

class TestAutoPilotDeployment:
    """Test Auto Pilot deployment endpoint."""
    
    def test_deploy_validation_error(self, auth_token):
        """Test deployment with invalid parameters (e.g., bad allocation type, forbidden fields)."""
        # Test 1: Invalid risk_mode
        payload_bad_risk = {
            "risk_mode": "aggresive", # Typo
            "allocation": {
                "total_capital_budget": 1000,
                "trade_amount_per_position": 100
            },
            "broker_account_ids": ["broker_1"],
            "execution_mode": "paper",
            "symbol_universe_mode": "auto"
        }
        response = requests.post(
            f"{USER_BACKEND_URL}/api/v1/auto-pilot/deploy",
            headers={"Authorization": f"Bearer {auth_token}"},
            json=payload_bad_risk,
            timeout=TEST_TIMEOUT
        )
        assert response.status_code == 422, f"Expected 422 for bad enum, got {response.status_code}"

        # Test 2: Forbidden field (strategy_id)
        payload_forbidden = {
            "risk_mode": "medium",
            "allocation": {
                "total_capital_budget": 1000,
                "trade_amount_per_position": 100
            },
            "broker_account_ids": ["broker_1"],
            "execution_mode": "paper",
            "symbol_universe_mode": "auto",
            "strategy_id": "custom_strategy" # Forbidden
        }
        response = requests.post(
            f"{USER_BACKEND_URL}/api/v1/auto-pilot/deploy",
            headers={"Authorization": f"Bearer {auth_token}"},
            json=payload_forbidden,
            timeout=TEST_TIMEOUT
        )
        assert response.status_code == 422, f"Expected 422 for forbidden field, got {response.status_code}"

    def test_deploy_auto_pilot_flow(self, auth_token):
        """
        Test a valid deployment request with strict contract.
        """
        payload = {
            "risk_mode": "medium", # Maps to 'balanced'
            "allocation": {
                "total_capital_budget": 1000,
                "trade_amount_per_position": 100
            },
            "broker_account_ids": [str(uuid.uuid4())],
            "execution_mode": "paper",
            "symbol_universe_mode": "auto"
        }
        
        response = requests.post(
            f"{USER_BACKEND_URL}/api/v1/auto-pilot/deploy",
            headers={"Authorization": f"Bearer {auth_token}"},
            json=payload,
            timeout=TEST_TIMEOUT
        )
        
        # If backend is up:
        # 401: Auth failed (token invalid)
        # 500: Deployment failed (broker not found in DB or other logic error)
        # 200: Success
        
        # We expect validation to pass (not 422)
        assert response.status_code != 422, f"Validation failed: {response.text}"
        assert response.status_code != 404, "Auto Pilot endpoint not found!"
        
        if response.status_code == 200:
            data = response.json()
            assert isinstance(data, list)
            assert len(data) > 0
            assert "strategy_id" in data[0]
            assert data[0]["strategy_id"] == "master_ensemble"
