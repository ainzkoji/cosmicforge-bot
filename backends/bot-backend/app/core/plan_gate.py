"""Plan (subscription) gates shared by the bot API routers.

The plan is read from the shared ``subscriptions`` table at request time via
``shared_lib.billing.entitlements`` -- never from the JWT's ``entitlements``
claim, which is a snapshot from login and goes stale on a downgrade. A user
with no subscription row is on the free plan.

Refusals are HTTP 403 with a machine-readable detail::

    {"error_code": "LIVE_TRADING_NOT_IN_PLAN", "message": "...", "plan_id": "plan_free"}
    {"error_code": "BOT_LIMIT_REACHED", "message": "...", "plan_id": "...", "used": 1, "limit": 1}

Only LIVE bots are subject to the live-trading gate; paper/demo bots never are.

Step 1.5: while ``BILLING_ENFORCED`` is false (the default), neither gate
refuses anything -- entitlements do not block the permitted demo path. Every
other gate (authentication, ownership, KYC for live, exchange capability,
order submission switches, trading safety) is independent of this module.
"""
from __future__ import annotations

from typing import Any, Dict

from fastapi import HTTPException

from shared_lib.billing.enforcement import billing_enforced

LIVE_TRADING_NOT_IN_PLAN = "LIVE_TRADING_NOT_IN_PLAN"
BOT_LIMIT_REACHED = "BOT_LIMIT_REACHED"


def is_live_mode(mode: Any) -> bool:
    return str(mode or "").strip().lower() == "live"


def live_trading_allowed(db: Any, user_id: str) -> bool:
    from shared_lib.billing import entitlements as plan_entitlements

    return plan_entitlements.can_trade_live(db, user_id)


def live_trading_denied_detail(db: Any, user_id: str, action: str = "trade live") -> Dict[str, Any]:
    from shared_lib.billing import entitlements as plan_entitlements

    plan_id = plan_entitlements.get_effective_plan(db, user_id, persist=False)["plan_id"]
    return {
        "error_code": LIVE_TRADING_NOT_IN_PLAN,
        "message": f"Live trading is not included in your plan. Upgrade your plan to {action}.",
        "plan_id": plan_id,
    }


def require_live_trading(db: Any, user_id: str, action: str = "trade live") -> None:
    """Raise 403 unless the user's current plan includes live trading (billing enforced only)."""
    if not billing_enforced():
        return
    if not live_trading_allowed(db, user_id):
        raise HTTPException(status_code=403, detail=live_trading_denied_detail(db, user_id, action))


def require_bot_slots(db: Any, user_id: str, requested: int = 1) -> None:
    """Raise 403 unless ``requested`` more bots fit within the plan's bot limit (billing enforced only)."""
    if not billing_enforced():
        return
    from shared_lib.billing import entitlements as plan_entitlements

    quota = plan_entitlements.bot_quota(db, user_id)
    if quota["used"] + max(int(requested), 0) > quota["limit"]:
        raise HTTPException(status_code=403, detail={
            "error_code": BOT_LIMIT_REACHED,
            "message": (f"Plan limit reached. You have {quota['used']} bots, "
                        f"limit is {quota['limit']}. Upgrade your plan."),
            "plan_id": quota["plan_id"],
            "used": quota["used"],
            "limit": quota["limit"],
        })


__all__ = [
    "LIVE_TRADING_NOT_IN_PLAN",
    "BOT_LIMIT_REACHED",
    "billing_enforced",
    "is_live_mode",
    "live_trading_allowed",
    "live_trading_denied_detail",
    "require_live_trading",
    "require_bot_slots",
]
