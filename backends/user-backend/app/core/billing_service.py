import os
import sqlite3
import uuid
import logging
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
from urllib.parse import urlparse

from shared_lib.persistence.db import DB
from shared_lib.billing import entitlements as plan_entitlements
from shared_lib.billing import webhooks as billing_webhooks
from shared_lib.billing.plans import (
    FREE_PLAN_ID,
    PLAN_PRICE_ENV,
    interval_for,
    is_paid_plan,
    limits_for,
)
from app.schemas.billing import Plan, PlanFeature
from app.core.config import settings

log = logging.getLogger("cosmicforge.billing")

# Try importing stripe
try:
    import stripe
except ImportError:
    stripe = None

#: Days of access kept after a failed renewal payment before the downgrade.
GRACE_PERIOD_DAYS = plan_entitlements.GRACE_PERIOD_DAYS

# Frontend routes the provider redirects back to (see user-frontend App.tsx).
CHECKOUT_SUCCESS_PATH = "/payment/success"
CHECKOUT_CANCEL_PATH = "/dashboard/subscription"


class BillingError(ValueError):
    """The request cannot be served as asked (HTTP 400)."""


class BillingConflict(BillingError):
    """The request conflicts with the user's current subscription (HTTP 409)."""


class BillingNotConfigured(RuntimeError):
    """Billing cannot run with the current configuration (HTTP 503)."""


class BillingProviderError(RuntimeError):
    """The payment provider rejected or failed a call (HTTP 502)."""


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _db() -> DB:
    return DB()


def _cfg(name: str) -> str:
    """A billing setting: the loaded Settings first, then the process environment."""
    value = getattr(settings, name, None)
    if value is None or value == "":
        value = os.environ.get(name, "")
    return str(value or "").strip()


def _flag(name: str) -> bool:
    return _cfg(name).lower() in ("1", "true", "yes", "on")


def is_production() -> bool:
    # Anything that is not explicitly TEST/DEVELOPMENT is production (fail closed).
    return bool(getattr(settings, "production", True))


def _stripe_key_mode(secret_key: str) -> str:
    return "test" if "_test_" in secret_key else "live"


def _test_mode_allowed() -> bool:
    """Stripe test mode (test keys, test-card events) may only grant plans outside
    production, unless the operator explicitly opts in for a pre-launch dry run."""
    return (not is_production()) or _flag("BILLING_ALLOW_STRIPE_TEST_MODE")


def stripe_price_ids() -> Dict[str, str]:
    """Server-side price configuration: plan id -> Stripe Price ID (from env)."""
    prices: Dict[str, str] = {}
    for plan_id, env_name in PLAN_PRICE_ENV.items():
        price_id = _cfg(env_name)
        if price_id:
            prices[plan_id] = price_id
    return prices


def price_to_plan_map() -> Dict[str, str]:
    """Stripe Price ID -> plan id. A price configured for two plans maps to neither."""
    mapping: Dict[str, str] = {}
    ambiguous = set()
    for plan_id, price_id in stripe_price_ids().items():
        if price_id in mapping:
            ambiguous.add(price_id)
        mapping[price_id] = plan_id
    for price_id in ambiguous:
        log.error("Stripe price %s is configured for more than one plan; ignoring it", price_id)
        mapping.pop(price_id, None)
    return mapping


def _price_id_for(plan_id: str) -> str:
    price_id = stripe_price_ids().get(plan_id)
    if not price_id or price_id not in price_to_plan_map():
        raise BillingNotConfigured(f"{PLAN_PRICE_ENV.get(plan_id, plan_id)} is not configured")
    return price_id


def _frontend_base_url() -> str:
    base = _cfg("PUBLIC_APP_URL") or _cfg("FRONTEND_URL")
    if not base:
        if is_production():
            raise BillingNotConfigured("FRONTEND_URL is not configured")
        base = "http://localhost:5173"
    base = base.rstrip("/")
    parsed = urlparse(base)
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        raise BillingNotConfigured("FRONTEND_URL is not a valid http(s) URL")
    return base


def checkout_urls() -> Dict[str, str]:
    """Redirect targets, built only from server configuration (never from the client)."""
    base = _frontend_base_url()
    return {
        "success_url": f"{base}{CHECKOUT_SUCCESS_PATH}?session_id={{CHECKOUT_SESSION_ID}}",
        "cancel_url": f"{base}{CHECKOUT_CANCEL_PATH}?checkout=cancelled",
    }

# ============================================================================
# 1. Plan Catalog
# ============================================================================
# Machine limits live in shared_lib.billing.plans (one table for both backends)
# and match the advertised features below:
#   free  : 1 bot,  1 broker,  no live trading
#   pro   : 5 bots, 3 brokers, live trading
#   whale : unlimited bots / brokers, live trading

PLANS = [
    Plan(
        id="plan_free",
        name="Star Gazer",
        price=0.00,
        currency="USD",
        interval="month",
        features=[
            PlanFeature(name="Bot Profiles", included=True, limit="1 bot"),
            PlanFeature(name="Connected Brokers", included=True, limit="1 broker"),
            PlanFeature(name="Live Trading", included=False),
            PlanFeature(name="Backtesting", included=True, limit="Basic"),
        ],
        limits=limits_for("plan_free"),
        entitlements={
            "max_bots": "1", 
            "max_accounts": "1", 
            "live_trading": "false", 
            "backtesting": "basic", 
            "copy_trading": "false", 
            "api_access": "false",
            "advanced_reports": "false", 
            "dedicated_support": "false"
        },
        is_popular=False
    ),
    Plan(
        id="plan_pro",
        name="Nebula Voyager",
        price=29.00,
        currency="USD",
        interval="month",
        features=[
            PlanFeature(name="Bot Profiles", included=True, limit="5 bots"),
            PlanFeature(name="Connected Brokers", included=True, limit="3 brokers"),
            PlanFeature(name="Live Trading", included=True),
            PlanFeature(name="Advanced Backtesting", included=True),
            PlanFeature(name="Priority Support", included=True),
        ],
        limits=limits_for("plan_pro"),
        entitlements={
            "max_bots": "5", 
            "max_accounts": "3", 
            "live_trading": "true", 
            "backtesting": "advanced", 
            "copy_trading": "true", 
            "api_access": "true",
            "advanced_reports": "true", 
            "dedicated_support": "false"
        },
        is_popular=True
    ),
    Plan(
        id="plan_whale",
        name="Galactic Tycoon",
        price=99.00,
        currency="USD",
        interval="month",
        features=[
            PlanFeature(name="Bot Profiles", included=True, limit="Unlimited"),
            PlanFeature(name="Connected Brokers", included=True, limit="Unlimited"),
            PlanFeature(name="Live Trading", included=True),
            PlanFeature(name="Institutional API", included=True),
            PlanFeature(name="Dedicated Account Manager", included=True),
        ],
        limits=limits_for("plan_whale"),
        entitlements={
            "max_bots": "unlimited", 
            "max_accounts": "unlimited", 
            "live_trading": "true", 
            "backtesting": "advanced", 
            "copy_trading": "true", 
            "api_access": "true",
            "advanced_reports": "true", 
            "dedicated_support": "true"
        },
        is_popular=False
    ),
    # --- Yearly Plans (20% Discount) ---
    Plan(
        id="plan_pro_yearly",
        name="Nebula Voyager (Yearly)",
        price=279.00, # $29/mo * 12 * 0.8 ~= $279
        currency="USD",
        interval="year",
        features=[
            PlanFeature(name="Bot Profiles", included=True, limit="5 bots"),
            PlanFeature(name="Connected Brokers", included=True, limit="3 brokers"),
            PlanFeature(name="Live Trading", included=True),
            PlanFeature(name="Advanced Backtesting", included=True),
            PlanFeature(name="Priority Support", included=True),
        ],
        limits=limits_for("plan_pro_yearly"),
        entitlements={
            "max_bots": "5", 
            "max_accounts": "3", 
            "live_trading": "true", 
            "backtesting": "advanced", 
            "copy_trading": "true", 
            "api_access": "true",
            "advanced_reports": "true", 
            "dedicated_support": "false"
        },
        is_popular=True
    ),
    Plan(
        id="plan_whale_yearly",
        name="Galactic Tycoon (Yearly)",
        price=950.00, # $99/mo * 12 * 0.8 ~= $950
        currency="USD",
        interval="year",
        features=[
            PlanFeature(name="Bot Profiles", included=True, limit="Unlimited"),
            PlanFeature(name="Connected Brokers", included=True, limit="Unlimited"),
            PlanFeature(name="Live Trading", included=True),
            PlanFeature(name="Institutional API", included=True),
            PlanFeature(name="Dedicated Account Manager", included=True),
        ],
        limits=limits_for("plan_whale_yearly"),
        entitlements={
            "max_bots": "unlimited", 
            "max_accounts": "unlimited", 
            "live_trading": "true", 
            "backtesting": "advanced", 
            "copy_trading": "true", 
            "api_access": "true",
            "advanced_reports": "true", 
            "dedicated_support": "true"
        },
        is_popular=False
    )
]

def get_public_plans() -> List[Plan]:
    return PLANS

def get_plan_by_id(plan_id: str) -> Optional[Plan]:
    for p in PLANS:
        if p.id == plan_id:
            return p
    return None

# ============================================================================
# 2. Payment Provider Abstraction
# ============================================================================

class PaymentProvider(ABC):
    name = "abstract"
    mode = "test"

    @abstractmethod
    def create_checkout_session(
        self,
        *,
        plan_id: str,
        user_id: str,
        price_id: Optional[str],
        success_url: str,
        cancel_url: str,
        customer_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        pass

    @abstractmethod
    def cancel_subscription(self, provider_sub_id: str) -> bool:
        pass

    @abstractmethod
    def resume_subscription(self, provider_sub_id: str) -> bool:
        pass

    @abstractmethod
    def change_plan(self, provider_sub_id: str, price_id: str, plan_id: str) -> bool:
        pass


class MockPaymentProvider(PaymentProvider):
    """Local development only. It never grants a plan: the returned URL simply
    lands on the frontend's payment page, where the (unpaid) session stays
    pending. Refuses to exist in production."""

    name = "mock"

    def __init__(self):
        if is_production():
            raise BillingNotConfigured("the mock payment provider is not available in production")

    def create_checkout_session(
        self,
        *,
        plan_id: str,
        user_id: str,
        price_id: Optional[str],
        success_url: str,
        cancel_url: str,
        customer_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        session_id = f"cs_mock_{uuid.uuid4().hex}"
        mock_url = success_url.replace("{CHECKOUT_SESSION_ID}", session_id) + "&mock_payment=true"
        return {"id": session_id, "url": mock_url}

    def cancel_subscription(self, provider_sub_id: str) -> bool:
        return True

    def resume_subscription(self, provider_sub_id: str) -> bool:
        return True

    def change_plan(self, provider_sub_id: str, price_id: str, plan_id: str) -> bool:
        return False


class StripePaymentProvider(PaymentProvider):
    name = "stripe"

    def __init__(self, secret_key: str):
        if not stripe:
            raise BillingNotConfigured("the stripe library is not installed")
        stripe.api_key = secret_key
        self.mode = _stripe_key_mode(secret_key)

    def create_customer(self, user_id: str, email: Optional[str]) -> str:
        params: Dict[str, Any] = {"metadata": {"user_id": user_id}}
        if email:
            params["email"] = email
        customer = stripe.Customer.create(**params)
        return customer.id

    def create_checkout_session(
        self,
        *,
        plan_id: str,
        user_id: str,
        price_id: Optional[str],
        success_url: str,
        cancel_url: str,
        customer_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        if not price_id:
            raise BillingNotConfigured(f"no Stripe price configured for {plan_id}")
        # The webhook maps the paid session back to this user and plan through
        # these server-set references; nothing here comes from the browser.
        metadata = {
            "user_id": str(user_id),
            "plan_id": plan_id,
            "interval": interval_for(plan_id) or "month",
        }
        params: Dict[str, Any] = dict(
            mode="subscription",
            payment_method_types=["card"],
            line_items=[{"price": price_id, "quantity": 1}],
            success_url=success_url,
            cancel_url=cancel_url,
            client_reference_id=str(user_id),
            metadata=metadata,
            subscription_data={"metadata": metadata},
        )
        if customer_id:
            params["customer"] = customer_id
        session = stripe.checkout.Session.create(**params)
        return {"id": session.id, "url": session.url}

    def cancel_subscription(self, provider_sub_id: str) -> bool:
        try:
            stripe.Subscription.modify(provider_sub_id, cancel_at_period_end=True)
            return True
        except Exception as e:
            log.error(f"Stripe cancel failed: {e}")
            return False

    def resume_subscription(self, provider_sub_id: str) -> bool:
        try:
            stripe.Subscription.modify(provider_sub_id, cancel_at_period_end=False)
            return True
        except Exception as e:
            log.error(f"Stripe resume failed: {e}")
            return False

    def change_plan(self, provider_sub_id: str, price_id: str, plan_id: str) -> bool:
        """Move the existing subscription to another price (no second subscription).

        An upgrade is invoiced and charged immediately (``always_invoice``):
        with ``create_prorations`` the difference would only be billed on the
        next invoice, so upgrading and then cancelling would give the higher
        plan for the rest of the period without ever paying for it. A
        downgrade keeps the deferred proration (a credit on the next invoice).
        Which of the two it is comes from the price Stripe currently has on the
        subscription; when that cannot be mapped to a plan it is charged now.
        """
        try:
            items = stripe.SubscriptionItem.list(subscription=provider_sub_id, limit=1)
            item = items.data[0]
            item_id = item.id
            current_price_id = getattr(getattr(item, "price", None), "id", None)
            current_plan_id = price_to_plan_map().get(current_price_id or "")
            upgrade = current_plan_id is None or is_plan_upgrade(current_plan_id, plan_id)
            stripe.Subscription.modify(
                provider_sub_id,
                items=[{"id": item_id, "price": price_id}],
                proration_behavior="always_invoice" if upgrade else "create_prorations",
                cancel_at_period_end=False,
                metadata={"plan_id": plan_id, "interval": interval_for(plan_id) or "month"},
            )
            return True
        except Exception as e:
            log.error(f"Stripe plan change failed: {e}")
            return False


def get_provider() -> PaymentProvider:
    """The configured payment provider.

    Stripe whenever ``STRIPE_SECRET_KEY`` is set. Without it (or without the SDK)
    production gets :class:`BillingNotConfigured` -- never the mock; the mock is
    returned only outside production.
    """
    secret_key = _cfg("STRIPE_SECRET_KEY")
    if secret_key:
        if stripe is None:
            raise BillingNotConfigured("STRIPE_SECRET_KEY is set but the stripe library is not installed")
        if _stripe_key_mode(secret_key) == "test" and not _test_mode_allowed():
            raise BillingNotConfigured("a Stripe test-mode key is configured in production")
        return StripePaymentProvider(secret_key)
    if is_production():
        raise BillingNotConfigured("STRIPE_SECRET_KEY is not configured")
    return MockPaymentProvider()

# ============================================================================
# 3. Billing Service
# ============================================================================

def _has_provider_subscription(state: Dict[str, Any]) -> bool:
    """The user currently holds a live Stripe subscription."""
    return bool(
        state.get("is_paid")
        and state.get("provider") == "stripe"
        and state.get("provider_sub_id")
    )


def has_provider_subscription(user_id: str) -> bool:
    return _has_provider_subscription(plan_entitlements.get_effective_plan(_db(), user_id))


def _is_missing_customer_error(exc: Exception) -> bool:
    return getattr(exc, "param", None) == "customer" or "no such customer" in str(exc).lower()


def _stored_customer_id(db: DB, user_id: str, mode: str) -> Optional[str]:
    with db.connect() as conn:
        row = conn.execute(
            "SELECT provider_customer_id FROM billing_customers "
            "WHERE user_id = ? AND mode = ? AND provider = 'stripe'",
            (user_id, mode),
        ).fetchone()
    return row[0] if row else None


def _ensure_customer(db: DB, provider: "StripePaymentProvider", user_id: str, email: Optional[str]) -> str:
    """Reuse the user's stored Stripe customer, creating and storing one if needed.

    Stored before the redirect, so every later event for that customer can be
    mapped to the user whatever order the events arrive in.
    """
    customer_id = _stored_customer_id(db, user_id, provider.mode)
    if customer_id:
        return customer_id
    customer_id = provider.create_customer(user_id, email)
    now = utc_now_iso()
    with db.connect() as conn:
        conn.execute(
            """
            INSERT INTO billing_customers (user_id, mode, provider, provider_customer_id, created_at, updated_at)
            VALUES (?, ?, 'stripe', ?, ?, ?)
            ON CONFLICT(user_id, mode) DO UPDATE SET
                provider_customer_id = excluded.provider_customer_id,
                updated_at = excluded.updated_at
            """,
            (user_id, provider.mode, customer_id, now, now),
        )
    return customer_id


def _forget_customer(db: DB, user_id: str, mode: str) -> None:
    with db.connect() as conn:
        conn.execute(
            "DELETE FROM billing_customers WHERE user_id = ? AND mode = ? AND provider = 'stripe'",
            (user_id, mode),
        )


def create_checkout_session(user_id: str, plan_id: str, email: Optional[str] = None) -> Dict[str, Any]:
    """Start a subscription checkout. Grants nothing: activation is webhook-only."""
    user_id = str(user_id)

    # 1. Validate Plan (server-side catalog; the price comes from server config)
    plan = get_plan_by_id(plan_id)
    if not plan:
        raise BillingError("Invalid Plan ID")
    if not is_paid_plan(plan_id):
        raise BillingError("The free plan does not need a checkout.")

    # 2. Provider and redirect targets (raises BillingNotConfigured)
    provider = get_provider()
    urls = checkout_urls()
    price_id = _price_id_for(plan_id) if provider.name == "stripe" else None

    # 3. One subscription per user: an existing one is changed, not duplicated.
    db = _db()
    if _has_provider_subscription(plan_entitlements.get_effective_plan(db, user_id)):
        raise BillingConflict(
            "You already have an active subscription. Change or cancel it from the subscription page."
        )

    # 4. Track Intent
    intent_id = f"pi_{uuid.uuid4().hex[:12]}"
    with db.connect() as conn:
        conn.execute(
            "INSERT INTO pricing_intents (id, user_id, plan_id, session_id, created_at) VALUES (?, ?, ?, ?, ?)",
            (intent_id, user_id, plan_id, None, utc_now_iso())
        )

    # 5. Create Session
    def _create(customer_id: Optional[str]) -> Dict[str, Any]:
        return provider.create_checkout_session(
            plan_id=plan_id,
            user_id=user_id,
            price_id=price_id,
            success_url=urls["success_url"],
            cancel_url=urls["cancel_url"],
            customer_id=customer_id,
        )

    try:
        if provider.name == "stripe":
            try:
                result = _create(_ensure_customer(db, provider, user_id, email))
            except Exception as exc:
                if not _is_missing_customer_error(exc):
                    raise
                # Stored customer no longer exists in this Stripe account.
                log.warning("stored Stripe customer for user %s is gone; creating a new one", user_id)
                _forget_customer(db, user_id, provider.mode)
                result = _create(_ensure_customer(db, provider, user_id, email))
        else:
            result = _create(None)
    except BillingNotConfigured:
        raise
    except Exception as exc:
        log.exception("checkout session creation failed for user %s plan %s", user_id, plan_id)
        raise BillingProviderError("The payment provider could not start the checkout.") from exc

    # Update intent with session ID (the webhook cross-checks the session against it)
    with db.connect() as conn:
        conn.execute(
            "UPDATE pricing_intents SET session_id = ? WHERE id = ?", 
            (result["id"], intent_id)
        )
    
    return result


def handle_stripe_webhook(payload: bytes, signature_header: Optional[str]) -> Dict[str, str]:
    """Verify and apply one Stripe webhook delivery.

    Raises ``WebhookNotConfiguredError`` / ``WebhookSignatureError`` /
    ``WebhookPayloadError`` (see shared_lib.billing.webhooks). The signature is
    checked over the raw body before anything is parsed; with no
    ``STRIPE_WEBHOOK_SECRET`` every delivery is rejected, in every environment.
    """
    billing_webhooks.verify_stripe_signature(payload, signature_header, _cfg("STRIPE_WEBHOOK_SECRET"))
    event = billing_webhooks.parse_event(payload)
    return billing_webhooks.process_stripe_event(
        _db(),
        event,
        price_to_plan=price_to_plan_map(),
        require_livemode=not _test_mode_allowed(),
    )


def _set_cancel_flag(db: DB, user_id: str, value: int) -> None:
    with db.connect() as conn:
        conn.execute(
            "UPDATE subscriptions SET cancel_at_period_end = ?, updated_at = ? WHERE user_id = ? AND plan_id != ?",
            (value, utc_now_iso(), user_id, FREE_PLAN_ID)
        )


def _stripe_provider_for(state: Dict[str, Any]) -> Optional["StripePaymentProvider"]:
    """Stripe provider when the subscription lives in Stripe, else None."""
    if not _has_provider_subscription(state):
        return None
    provider = get_provider()
    if provider.name != "stripe":
        raise BillingNotConfigured("this subscription is managed by Stripe, which is not configured")
    return provider


def cancel_subscription(user_id: str) -> bool:
    """Cancel at period end. Returns False when there is no paid subscription.

    For a Stripe subscription the cancellation is set in Stripe first; Stripe
    then ends it at the period end and ``customer.subscription.deleted``
    downgrades the user. Local-only rows expire through the period-end check.
    """
    user_id = str(user_id)
    db = _db()
    state = plan_entitlements.get_effective_plan(db, user_id)
    if not state["is_paid"]:
        return False

    provider = _stripe_provider_for(state)
    if provider is not None and not provider.cancel_subscription(state["provider_sub_id"]):
        raise BillingProviderError("The payment provider could not cancel the subscription.")

    _set_cancel_flag(db, user_id, 1)
    return True


def resume_subscription(user_id: str) -> bool:
    """Undo a pending cancel-at-period-end."""
    user_id = str(user_id)
    db = _db()
    state = plan_entitlements.get_effective_plan(db, user_id)
    if not state["is_paid"] or not state["cancel_at_period_end"]:
        return False

    provider = _stripe_provider_for(state)
    if provider is not None and not provider.resume_subscription(state["provider_sub_id"]):
        raise BillingProviderError("The payment provider could not resume the subscription.")

    _set_cancel_flag(db, user_id, 0)
    return True


def _plan_rank(plan_id: Optional[str]) -> tuple:
    """Orders plans by what they grant (free < pro < whale; yearly == monthly)."""
    limits = limits_for(plan_id)
    return (
        int(limits.get("max_bots", 0) or 0),
        int(limits.get("max_brokers", 0) or 0),
        bool(limits.get("live_trading", False)),
        bool(limits.get("api_access", False)),
    )


def is_plan_upgrade(current_plan_id: Optional[str], new_plan_id: str) -> bool:
    return _plan_rank(new_plan_id) > _plan_rank(current_plan_id)


def change_plan(user_id: str, plan_id: str) -> bool:
    """Move the user's existing Stripe subscription to ``plan_id``.

    Only asks Stripe to change the price; the plan itself changes when the
    signed ``customer.subscription.updated`` webhook arrives.
    """
    user_id = str(user_id)
    if not get_plan_by_id(plan_id) or not is_paid_plan(plan_id):
        raise BillingError("Invalid Plan ID")
    state = plan_entitlements.get_effective_plan(_db(), user_id)
    provider = _stripe_provider_for(state)
    if provider is None:
        raise BillingError("No active subscription to change.")
    if state["plan_id"] == plan_id and not state["cancel_at_period_end"]:
        raise BillingConflict("You are already on this plan.")
    price_id = _price_id_for(plan_id)
    if not provider.change_plan(state["provider_sub_id"], price_id, plan_id):
        raise BillingProviderError("The payment provider could not change the plan.")
    return True


def _usage(db: DB, user_id: str) -> Dict[str, int]:
    usage = {"bots": 0, "brokers": 0}
    try:
        with db.connect() as conn:
            usage["bots"] = plan_entitlements.count_user_bots(conn, user_id)
            usage["brokers"] = plan_entitlements.count_user_brokers(conn, user_id)
    except sqlite3.Error as exc:
        # Usage is informational here; enforcement uses check_entitlement.
        log.warning("could not read plan usage for user %s: %s", user_id, exc)
    return usage


def get_user_subscription(user_id: str, *, persist: bool = False) -> Dict[str, Any]:
    """Current plan, status, limits and usage, read from the database.

    A subscription whose period (plus grace) has run out is reported as the
    free plan -- see shared_lib.billing.entitlements. Read-only by default, so
    it is safe to call while the request holds an open write transaction (token
    creation at login does). Only ``persist=True`` (the billing status
    endpoint) also stores that downgrade, and never waits long for the write
    lock to do so.
    """
    user_id = str(user_id)
    db = _db()
    state = plan_entitlements.get_effective_plan(db, user_id, persist=persist)
    plan = get_plan_by_id(state["plan_id"]) or get_plan_by_id(FREE_PLAN_ID)

    return {
        "plan": plan.dict(),
        "status": state["status"],  # the free plan is always "active"
        "current_period_end": state["current_period_end"],
        "cancel_at_period_end": bool(state["cancel_at_period_end"]),
        "grace_period_end": state["grace_period_end"],
        "entitlements": dict(state["limits"]),
        "usage": _usage(db, user_id),
    }


def get_checkout_status(user_id: str, session_id: str) -> Optional[Dict[str, Any]]:
    """Whether ``session_id`` (a checkout this user started) has been activated.

    Read-only: it reports what the webhook has already recorded and never
    grants anything itself. ``None`` when the session is not this user's.
    """
    user_id = str(user_id)
    db = _db()
    with db.connect() as conn:
        intent = conn.execute(
            "SELECT plan_id FROM pricing_intents WHERE session_id = ? AND user_id = ?",
            (session_id, user_id),
        ).fetchone()
    if not intent:
        return None

    state = plan_entitlements.get_effective_plan(db, user_id)
    activated = bool(state["is_paid"]) and state.get("checkout_session_id") == session_id
    return {
        "session_id": session_id,
        "status": "active" if activated else "pending",
        "plan_id": intent[0],
        "current_plan_id": state["plan_id"],
    }


def list_invoices(user_id: str) -> List[Dict[str, Any]]:
    db = _db()
    with db.connect() as conn:
        rows = conn.execute(
            "SELECT * FROM invoices WHERE user_id = ? ORDER BY created_at DESC", 
            (user_id,)
        ).fetchall()
        
        # map to schema format
        results = []
        for r in rows:
            d = dict(r)
            results.append({
                "id": d["id"],
                "amount": d["amount"] if d["amount"] is not None else 0.0,
                "amount_cents": d.get("amount_cents"),
                "currency": d["currency"] or "USD",
                "status": d["status"],
                "date": d["created_at"],
                "pdf_url": d.get("hosted_invoice_url") # or None
            })
        return results

# ============================================================================
# 4. Enforce Entitlements (Action Gating)
# ============================================================================

def check_entitlement(user_id: str, action: str) -> bool:
    """
    Checks if user is allowed to perform action, from the database.
    actions: 'create_bot', 'live_trading', 'add_broker', 'api_access'
    """
    user_id = str(user_id)
    db = _db()

    if action == "create_bot":
        return plan_entitlements.can_create_bot(db, user_id)

    if action == "live_trading":
        return plan_entitlements.can_trade_live(db, user_id)

    if action == "add_broker":
        return plan_entitlements.can_add_broker(db, user_id)

    if action == "api_access":
        limits = plan_entitlements.get_effective_plan(db, user_id)["limits"]
        return bool(limits.get("api_access", False))

    return False
