"""The billing enforcement switch (Step 1.5): ``BILLING_ENFORCED``.

Default ``false``. While billing is not enforced, subscription entitlements do
not block the permitted Step 1 demo operations: a customer without a paid
plan can connect an eligible demo broker account and deploy within the demo
path. Nothing else is touched by this switch -- authentication, ownership and
tenant isolation, KYC for live trading, the exchange capability gate, the
order-submission gates and every trading safety gate are independent of
billing and keep applying. The Stripe integration stays installed and keeps
recording what it is told; it collects nothing on its own.

With ``BILLING_ENFORCED=true`` the existing entitlement rules apply exactly as
before (plan bot slots, broker slots, live-trading plan requirement, expiry
and grace handling).

Resolution order: the process environment variable, then the running
backend's ``settings`` object (both backends declare the field on the shared
``ProductionSettings``), then ``false``.
"""
from __future__ import annotations

import os
from typing import Any, Optional

ENV_NAME = "BILLING_ENFORCED"
_TRUE = frozenset({"1", "true", "yes", "on"})


def _as_bool(value: Any) -> Optional[bool]:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text == "":
        return None
    return text in _TRUE


def billing_enforced(settings: Any = None) -> bool:
    """Whether subscription entitlements gate operations right now."""
    from_env = _as_bool(os.environ.get(ENV_NAME))
    if from_env is not None:
        return from_env
    if settings is None:
        try:  # the running backend's settings (each backend has its own app.core.config)
            from app.core.config import settings as running  # type: ignore
            settings = running
        except Exception:
            settings = None
    if settings is not None:
        from_settings = _as_bool(getattr(settings, ENV_NAME, None))
        if from_settings is not None:
            return from_settings
    return False


__all__ = ["ENV_NAME", "billing_enforced"]
