"""Plan identifiers and their machine-readable limits.

This table is the single source of truth for what a plan allows. The marketing
copy (``PlanFeature`` lists and ``entitlements`` strings in the user-backend's
``billing_service``) describes these same numbers; the limits are never taken
from a JWT claim or from anything a client sends.
"""
from __future__ import annotations

from typing import Any, Dict, Optional, Tuple

FREE_PLAN_ID = "plan_free"

#: Rendered as "Unlimited". The user frontend shows the infinity sign above 100.
UNLIMITED = 999

_FREE = {"max_bots": 1, "max_brokers": 1, "live_trading": False, "api_access": False}
_PRO = {"max_bots": 5, "max_brokers": 3, "live_trading": True, "api_access": True}
_WHALE = {"max_bots": UNLIMITED, "max_brokers": UNLIMITED, "live_trading": True, "api_access": True}

PLAN_LIMITS: Dict[str, Dict[str, Any]] = {
    FREE_PLAN_ID: _FREE,
    "plan_pro": _PRO,
    "plan_pro_yearly": _PRO,
    "plan_whale": _WHALE,
    "plan_whale_yearly": _WHALE,
}

#: plan id -> (tier, billing interval). The free plan is not billable.
PLAN_BILLING: Dict[str, Tuple[str, str]] = {
    "plan_pro": ("pro", "month"),
    "plan_pro_yearly": ("pro", "year"),
    "plan_whale": ("whale", "month"),
    "plan_whale_yearly": ("whale", "year"),
}

#: plan id -> name of the environment variable holding its Stripe Price ID.
PLAN_PRICE_ENV: Dict[str, str] = {
    "plan_pro": "STRIPE_PRICE_PRO_MONTHLY",
    "plan_pro_yearly": "STRIPE_PRICE_PRO_YEARLY",
    "plan_whale": "STRIPE_PRICE_WHALE_MONTHLY",
    "plan_whale_yearly": "STRIPE_PRICE_WHALE_YEARLY",
}


def is_known_plan(plan_id: Optional[str]) -> bool:
    return bool(plan_id) and plan_id in PLAN_LIMITS


def is_paid_plan(plan_id: Optional[str]) -> bool:
    return bool(plan_id) and plan_id in PLAN_BILLING


def limits_for(plan_id: Optional[str]) -> Dict[str, Any]:
    """Limits of ``plan_id``. An unknown or missing plan gets the free limits."""
    return dict(PLAN_LIMITS.get(plan_id or FREE_PLAN_ID, _FREE))


def interval_for(plan_id: Optional[str]) -> Optional[str]:
    billing = PLAN_BILLING.get(plan_id or "")
    return billing[1] if billing else None
