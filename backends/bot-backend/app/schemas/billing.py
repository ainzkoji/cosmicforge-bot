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
    plan_id: str
    success_url: Optional[str] = None
    cancel_url: Optional[str] = None

class CheckoutResponse(BaseModel):
    checkout_url: str
    session_id: str

class SubscriptionStatus(BaseModel):
    plan: Optional[Plan]
    status: str # active, trialing, etc.
    current_period_end: Optional[str] = None
    cancel_at_period_end: bool = False
    entitlements: Dict[str, Any] # computed capabilities

class Invoice(BaseModel):
    id: str
    amount: float
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
