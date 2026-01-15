import sys
import uuid
import json
from app.persistence.db import DB
from app.core import billing_service

def verify_billing_flow():
    print("--- Verifying Billing Flow ---")
    
    # 1. Setup User
    user_id = f"test_user_{uuid.uuid4().hex[:8]}"
    print(f"Test User ID: {user_id}")
    
    # 2. List Plans
    plans = billing_service.get_public_plans()
    print(f"Plans available: {[p.id for p in plans]}")
    assert len(plans) >= 3
    
    # 3. Create Checkout Session (Mock)
    tgt_plan = "plan_pro"
    print(f"Selecting plan: {tgt_plan}")
    
    checkout = billing_service.create_checkout_session(user_id, tgt_plan)
    print(f"Checkout created: {checkout['id']}")
    assert checkout['url'].startswith("http")
    
    # Verify Intent Check
    db = DB()
    with db.connect() as conn:
        intent = conn.execute("SELECT * FROM pricing_intents WHERE user_id = ?", (user_id,)).fetchone()
        assert intent is not None
        assert intent["plan_id"] == tgt_plan
        print("Pricing Intent verified in DB.")

    # 4. Simulate Webhook (Success)
    print("Simulating Webhook Success...")
    # Mock payload for 'checkout.session.completed'
    payload = {
        "id": "evt_test_123",
        "type": "checkout.session.completed",
        "data": {
            "object": {
                "id": checkout['id'],
                "client_reference_id": user_id, # stripe standard
                "metadata": {"plan_id": tgt_plan},
                "subscription": "sub_test_stripe_123"
            }
        }
    }
    
    # We call the service handler directly to simulate webhook receipt
    billing_service.handle_webhook_event("checkout.session.completed", payload, provider="stripe")
    
    # 5. Verify Subscription Active
    sub = billing_service.get_user_subscription(user_id)
    print(f"Subscription Status: {sub['status']}")
    assert sub['status'] == "active"
    assert sub['plan']['id'] == tgt_plan
    assert sub['entitlements']['live_trading'] is True
    print("Subscription verified active in DB.")
    
    # 6. Check Entitlements (Logic)
    # Pro plan allows 5 bots.
    can_trade = billing_service.check_entitlement(user_id, "live_trading")
    print(f"Can Live Trade? {can_trade}")
    assert can_trade is True
    
    # 7. Check Invoices
    invoices = billing_service.list_invoices(user_id)
    print(f"Invoices found: {len(invoices)}")
    assert len(invoices) >= 1
    assert invoices[0]['amount'] == 29.0
    
    # 8. Cancel Subscription
    print("Canceling Subscription...")
    billing_service.cancel_subscription(user_id)
    
    sub_after = billing_service.get_user_subscription(user_id)
    print(f"Cancel at period end: {sub_after['cancel_at_period_end']}")
    assert sub_after['cancel_at_period_end'] is True
    
    # 9. Verify Billing Audit Log
    with db.connect() as conn:
        evt = conn.execute("SELECT * FROM billing_events WHERE event_id = 'evt_test_123'").fetchone()
        assert evt is not None
        print("Billing Event audit log verified.")

    print("--- Billing Verification Passed ---")

if __name__ == "__main__":
    verify_billing_flow()
