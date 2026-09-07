from pydantic import BaseModel, Field
from typing import Optional, Dict, Any, List
from datetime import datetime

# --- CMS Content Schemas ---
class ContentBlock(BaseModel):
    key: str
    locale: str
    content: Dict[str, Any]
    updated_at: str

class FAQItem(BaseModel):
    q: str
    a: str

class StepItem(BaseModel):
    step: int
    title: str
    description: str
    icon: str

# --- Tracking Schemas ---
class MarketingSessionCreate(BaseModel):
    landing_page: str
    utm_source: Optional[str] = None
    utm_medium: Optional[str] = None
    utm_campaign: Optional[str] = None
    utm_content: Optional[str] = None
    utm_term: Optional[str] = None
    ref_code: Optional[str] = None
    aff_broker: Optional[str] = None

class MarketingSessionResponse(BaseModel):
    session_id: str

class TrackEventRequest(BaseModel):
    session_id: str
    event_type: str
    page: str
    metadata: Optional[Dict[str, Any]] = None

# --- Pricing Schemas ---
class PlanEntitlements(BaseModel):
    max_bots: str
    max_accounts: str
    live_trading: bool
    backtesting: str
    api_access: bool
    copy_trading: bool
    advanced_reports: bool
    dedicated_support: Optional[bool] = False
    custom_integrations: Optional[bool] = False

class Plan(BaseModel):
    id: str
    name: str
    price: float
    billing_period: str
    currency: str
    status: str
    description: Optional[str] = None
    badge: Optional[str] = None
    entitlements: Dict[str, str]  # Simplified key-value map

class PricingIntentCreate(BaseModel):
    marketing_session_id: str
    plan_id: str

class PricingIntentResponse(BaseModel):
    intent_id: str
