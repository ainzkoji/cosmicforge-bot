import logging

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.concurrency import run_in_threadpool

from app.api.auth import get_current_active_user
from app.core import billing_service
from app.schemas.billing import (
    PlanCatalogResponse,
    CheckoutRequest,
    CheckoutResponse,
    CheckoutStatusResponse,
    SubscriptionStatus,
    BillingHistoryResponse,
    SubscriptionActionRequest
)
from shared_lib.billing.webhooks import (
    WebhookNotConfiguredError,
    WebhookPayloadError,
    WebhookSignatureError,
)

log = logging.getLogger("cosmicforge.billing.api")

router = APIRouter()


def _http_error(exc: Exception) -> HTTPException:
    """Map a billing_service error to its HTTP response."""
    if isinstance(exc, billing_service.BillingNotConfigured):
        # The reason is for operators (log); clients get a stable message.
        log.error("billing not configured: %s", exc)
        return HTTPException(status_code=503, detail="Billing is not configured.")
    if isinstance(exc, billing_service.BillingProviderError):
        return HTTPException(status_code=502, detail=str(exc))
    if isinstance(exc, billing_service.BillingConflict):
        return HTTPException(status_code=409, detail=str(exc))
    return HTTPException(status_code=400, detail=str(exc))


_BILLING_ERRORS = (
    billing_service.BillingNotConfigured,
    billing_service.BillingProviderError,
    ValueError,  # includes BillingError / BillingConflict
)


@router.get("/plans", response_model=PlanCatalogResponse)
async def get_plans():
    """
    Public Endpoint: Get list of available subscription plans.
    """
    plans = billing_service.get_public_plans()
    return {"plans": plans}

@router.post("/checkout", response_model=CheckoutResponse)
def create_checkout(
    req: CheckoutRequest,
    current_user: dict = Depends(get_current_active_user)
):
    """
    Start payment flow for a plan.

    The price and the redirect URLs come from server configuration; the client
    only names the plan. Nothing is granted here: the plan is activated by the
    signed provider webhook once the payment has actually been made.
    """
    try:
        result = billing_service.create_checkout_session(
            user_id=current_user["id"], 
            plan_id=req.plan_id,
            email=current_user.get("email"),
        )
        return {
            "checkout_url": result["url"],
            "session_id": result["id"]
        }
    except _BILLING_ERRORS as e:
        raise _http_error(e)

@router.post("/webhook")
async def billing_webhook(request: Request):
    """
    Stripe webhook. Authenticated by the ``Stripe-Signature`` header, verified
    over the raw request body with ``STRIPE_WEBHOOK_SECRET``. Without a
    configured secret every request is rejected.
    """
    payload = await request.body()
    signature = request.headers.get("stripe-signature")
    try:
        result = await run_in_threadpool(billing_service.handle_stripe_webhook, payload, signature)
    except WebhookNotConfiguredError:
        log.error("billing webhook rejected: STRIPE_WEBHOOK_SECRET is not configured")
        raise HTTPException(status_code=503, detail="Billing webhook is not configured.")
    except WebhookSignatureError as e:
        log.warning("billing webhook rejected: %s", e)
        raise HTTPException(status_code=400, detail="Invalid webhook signature.")
    except WebhookPayloadError as e:
        raise HTTPException(status_code=400, detail=f"Invalid webhook payload: {e}")

    # "processed", "duplicate" (already applied) or "ignored" (unhandled type).
    return {"status": result["status"]}

@router.get("/checkout-status", response_model=CheckoutStatusResponse)
def get_checkout_status(
    session_id: str = Query(..., min_length=1, max_length=255),
    current_user: dict = Depends(get_current_active_user)
):
    """
    Whether a checkout session started by the logged-in user has been activated.
    Read-only: reports what the webhook recorded, never grants anything.
    """
    status = billing_service.get_checkout_status(current_user["id"], session_id)
    if status is None:
        raise HTTPException(status_code=404, detail="Checkout session not found.")
    return status

@router.get("/subscription", response_model=SubscriptionStatus)
def get_subscription(
    current_user: dict = Depends(get_current_active_user)
):
    """
    Get current user's subscription status, entitlements and usage.
    """
    # The one read that also stores a lapsed subscription's downgrade (no other
    # write transaction is open here; see get_user_subscription).
    return billing_service.get_user_subscription(current_user["id"], persist=True)

@router.get("/history", response_model=BillingHistoryResponse)
def get_billing_history(
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
def manage_subscription(
    req: SubscriptionActionRequest,
    current_user: dict = Depends(get_current_active_user)
):
    """
    Cancel, Resume, or Upgrade subscription.
    """
    user_id = current_user["id"]
    try:
        if req.action == "cancel":
            success = billing_service.cancel_subscription(user_id)
            if not success:
                raise HTTPException(status_code=400, detail="Failed to cancel subscription or no active subscription.")
            return {"status": "canceled", "message": "Subscription will cancel at period end."}

        elif req.action == "resume":
            success = billing_service.resume_subscription(user_id)
            if not success:
                raise HTTPException(status_code=400, detail="No pending cancellation to resume.")
            return {"status": "resumed", "message": "Subscription will renew at period end."}

        elif req.action == "upgrade":
            if not req.plan_id:
                raise HTTPException(status_code=400, detail="Plan ID required for upgrade.")

            if billing_service.has_provider_subscription(user_id):
                # Change the price of the existing subscription instead of
                # creating a second one. The plan switches when the provider's
                # signed subscription-updated webhook arrives.
                billing_service.change_plan(user_id, req.plan_id)
                return {
                    "status": "plan_change_requested",
                    "message": "Your plan change is being processed."
                }

            result = billing_service.create_checkout_session(
                user_id=user_id, 
                plan_id=req.plan_id,
                email=current_user.get("email"),
            )
            return {
                "status": "upgrade_initiated",
                "checkout_url": result["url"],
                "session_id": result["id"]
            }
    except _BILLING_ERRORS as e:
        raise _http_error(e)

    raise HTTPException(status_code=400, detail="Invalid action")
