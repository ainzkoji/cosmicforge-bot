from typing import Optional, List, Dict, Any
from pydantic import BaseModel
from datetime import datetime

class PlanFeature(BaseModel):
    name: str
    included: bool
    limit: Optional[str] = None # e.g. "5 bots"

class Plan(BaseModel):
    id: str
    name: str
    price: float
    currency: str
    interval: str # month/year
    features: List[PlanFeature]
    limits: Dict[str, Any] # machine-readable limits
    entitlements: Dict[str, str] # frontend-friendly strings
    is_popular: bool = False

class PlanCatalogResponse(BaseModel):
    plans: List[Plan]

class CheckoutRequest(BaseModel):
    # Only the plan is chosen by the client. Price and redirect URLs are server
    # configuration (a client-supplied success_url would be an open redirect).
    plan_id: str

class CheckoutResponse(BaseModel):
    checkout_url: str
    session_id: str

class CheckoutStatusResponse(BaseModel):
    session_id: str
    status: str # active (webhook confirmed the payment) | pending
    plan_id: str # plan the checkout was started for
    current_plan_id: str # plan the user is on right now

class SubscriptionUsage(BaseModel):
    bots: int = 0
    brokers: int = 0

class SubscriptionStatus(BaseModel):
    plan: Optional[Plan]
    status: str # active, trialing, past_due
    current_period_end: Optional[str] = None
    cancel_at_period_end: bool = False
    grace_period_end: Optional[str] = None # set while past_due: access ends then
    entitlements: Dict[str, Any] # computed capabilities
    usage: SubscriptionUsage = SubscriptionUsage() # resources counted against the limits

class Invoice(BaseModel):
    id: str
    amount: float
    amount_cents: Optional[int] = None # exact amount in the currency's minor unit
    currency: str
    status: str
    date: str
    pdf_url: Optional[str]

class BillingHistoryResponse(BaseModel):
    invoices: List[Invoice]

class SubscriptionActionRequest(BaseModel):
    action: str # cancel, resume, upgrade
    plan_id: Optional[str] = None # required for upgrade
    reason: Optional[str] = None
