from fastapi import APIRouter, Depends, HTTPException, Body, Request
from typing import List, Dict, Any

from app.api.auth import get_current_active_user
from app.core import billing_service
from app.schemas.billing import (
    PlanCatalogResponse,
    CheckoutRequest,
    CheckoutResponse,
    SubscriptionStatus,
    BillingHistoryResponse,
    SubscriptionActionRequest
)

router = APIRouter()

@router.get("/plans", response_model=PlanCatalogResponse)
async def get_plans():
    """
    Public Endpoint: Get list of available subscription plans.
    """
    plans = billing_service.get_public_plans()
    return {"plans": plans}

@router.post("/checkout", response_model=CheckoutResponse)
async def create_checkout(
    req: CheckoutRequest,
    current_user: dict = Depends(get_current_active_user)
):
    """
    Start payment flow for a plan.
    """
    try:
        result = billing_service.create_checkout_session(
            user_id=current_user["id"], 
            plan_id=req.plan_id
        )
        return {
            "checkout_url": result["url"],
            "session_id": result["id"]
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.post("/webhook")
async def billing_webhook(request: Request):
    """
    Receives events from Payment Provider (mocked).
    In prod, verifying the signature is crucial.
    """
    payload = await request.json()
    
    # Simple Mock Logic based on our mock provider
    event_type = payload.get("type")
    
    if event_type == "checkout.session.completed":
        # Extract data
        data = payload.get("data", {})
        # In real stripe, we pass metadata to link user_id
        # For mock, we'll expect user_id in payload or infer
        user_id = data.get("client_reference_id") # standard stripe field
        plan_id = data.get("metadata", {}).get("plan_id")
        session_id = data.get("id")
        
        if user_id and plan_id:
            billing_service.handle_checkout_success(user_id, plan_id, session_id)
            return {"status": "success"}
            
    return {"status": "ignored"}

# Helper endpoint to simulate webhook for frontend dev
@router.post("/test-simulate-success")
async def simulate_success(
    req: CheckoutRequest, 
    current_user: dict = Depends(get_current_active_user)
):
    """
    DEV ONLY: Simulate a successful payment webhook for a plan.
    """
    session_id = "mock_sess_" + billing_service.utc_now_iso()
    billing_service.handle_checkout_success(current_user["id"], req.plan_id, session_id)
    return {"status": "simulated", "plan": req.plan_id}

@router.get("/subscription", response_model=SubscriptionStatus)
async def get_subscription(
    current_user: dict = Depends(get_current_active_user)
):
    """
    Get current user's subscription status and entitlements.
    """
    return billing_service.get_user_subscription(current_user["id"])

@router.get("/history", response_model=BillingHistoryResponse)
async def get_billing_history(
    current_user: dict = Depends(get_current_active_user)
):
    """
    Get invoice history.
    """
    invoices = billing_service.list_invoices(current_user["id"])
    # Map fields if needed (db naming vs schema naming)
    # Our DB fields map pretty well to Schema fields
    return {"invoices": invoices}

@router.post("/subscription/manage")
async def manage_subscription(
    req: SubscriptionActionRequest,
    current_user: dict = Depends(get_current_active_user)
):
    """
    Cancel, Resume, or Upgrade subscription.
    """
    if req.action == "cancel":
        success = billing_service.cancel_subscription(current_user["id"])
        if not success:
            raise HTTPException(status_code=400, detail="Failed to cancel subscription or no active subscription.")
        return {"status": "canceled", "message": "Subscription will cancel at period end."}
    
    elif req.action == "upgrade":
        # Simplified: In real stripe, we might create a new checkout session for upgrade proration,
        # or update the subscription directly if payment method is on file.
        # For this MVP, we redirect to checkout for the new plan.
        if not req.plan_id:
            raise HTTPException(status_code=400, detail="Plan ID required for upgrade.")
            
        result = billing_service.create_checkout_session(
            user_id=current_user["id"], 
            plan_id=req.plan_id
        )
        return {
            "status": "upgrade_initiated",
            "checkout_url": result["url"],
            "session_id": result["id"]
        }
        
    raise HTTPException(status_code=400, detail="Invalid action")
