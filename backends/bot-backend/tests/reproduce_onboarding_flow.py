
import asyncio
import json
from datetime import datetime, timezone
import sys
import os

# Add project root to path
sys.path.append(os.getcwd())
sys.path.append(os.path.join(os.getcwd(), "app"))

from shared_lib.persistence.db import DB
from app.core import onboarding_service
from app.api.auth import get_current_active_user
from unittest.mock import patch

async def mock_user():
    return {"id": "test_user_repro", "email": "test@example.com"}

def setup_db():
    print("Setting up DB...")
    db = DB()
    # Clean up test user
    with db.connect() as conn:
        conn.execute("DELETE FROM onboarding_profiles WHERE user_id = 'test_user_repro'")
        
async def run_test():
    setup_db()
    user_id = "test_user_repro"
    
    # Mock KYC Gate to always return True (or False if we want to test that path)
    # We patch it inside the test execution
    with patch('shared_lib.core.policy.kyc_policy.check_kyc_gate', return_value=(False, "KYC Required")):
        print("\n--- 1. Start Onboarding ---")
        state = onboarding_service.get_onboarding_state(user_id)
        print(f"Status: {state['status']}, Step: {state['current_step']}")
        assert state['status'] == "not_started"
        
        print("\n--- 2. Set Experience Level ---")
        onboarding_service.update_onboarding_step(user_id, "experience_level", {"experience_level": "intermediate"})
        state = onboarding_service.get_onboarding_state(user_id)
        print(f"Experience Saved: {state['data'].get('experience_level')}")
        assert state['data']['experience_level'] == "intermediate"

        print("\n--- 3. Set Risk Tolerance ---")
        onboarding_service.update_onboarding_step(user_id, "risk_tolerance", {"risk_tolerance": "medium"})
        state = onboarding_service.get_onboarding_state(user_id)
        print(f"Risk Saved: {state['data'].get('risk_tolerance')}")
        
        print("\n--- 4. List Strategies ---")
        strategies = onboarding_service.get_strategy_catalog()
        print(f"Found {len(strategies)} strategies")
        if len(strategies) > 0:
            strat_id = strategies[0].id
            print(f"Selecting strategy: {strat_id}")
            
            print("\n--- 5. Save Strategy ---")
            onboarding_service.update_onboarding_step(user_id, "strategy_preference", {"strategy_preference": strat_id})
        else:
            print("!! No strategies found in registry. Skipping strategy save.")

        print("\n--- 6. Test Capital Allocation (Negative - Should Fail) ---")
        try:
            onboarding_service.update_onboarding_step(user_id, "capital_allocation", {"capital_allocation": -100})
            print("ERROR: Negative allocation was accepted!")
        except ValueError as e:
            print(f"SUCCESS: Negative allocation rejected with: {e}")

        print("\n--- 7. Test Capital Allocation (Valid) ---")
        onboarding_service.update_onboarding_step(user_id, "capital_allocation", {"capital_allocation": 500})
        state = onboarding_service.get_onboarding_state(user_id)
        print(f"Allocation Saved: {state['data'].get('capital_allocation')}")

        print("\n--- 8. Complete Onboarding ---")
        result = onboarding_service.complete_onboarding(user_id)
        print("Onboarding Complete.")
        print("Generated Defaults:")
        print(json.dumps(result, indent=2))
        
        # Verify risk mapping
        risk = result['risk_policy']
        expected_leverage = 5 # Medium risk
        if risk['max_leverage'] == expected_leverage:
            print(f"SUCCESS: Risk leverage correct ({expected_leverage})")
        else:
            print(f"FAILURE: Risk leverage incorrect. Got {risk['max_leverage']}, expected {expected_leverage}")

        print("\n--- 9. Check Next Steps ---")
        next_steps = onboarding_service.get_next_steps(user_id)
        print("Next Steps:")
        print(json.dumps(next_steps, indent=2))
        
        if "NO_BROKER" in next_steps['blockers']:
             print("SUCCESS: NO_BROKER blocker detected.")
        else:
             print("FAILURE: NO_BROKER blocker missing.")
        
        # Verify KYC mocked result
        if "KYC_REQUIRED" in next_steps['blockers']:
             print("SUCCESS: KYC_REQUIRED blocker detected (Mocked).")
        else:
             print("FAILURE: KYC_REQUIRED blocker missing (Mocked).")

if __name__ == "__main__":
    asyncio.run(run_test())
