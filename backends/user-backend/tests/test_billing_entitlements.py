"""Plan limits, database-backed entitlements, expiry, and the billing service.

Covers shared_lib.billing.entitlements (used by both backends) and
app.core.billing_service. No network: the payment provider is a local fake.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import sqlite3
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "backends" / "shared"))
sys.path.insert(0, str(ROOT / "backends" / "user-backend"))

from app.core import billing_service  # noqa: E402
from shared_lib.billing import entitlements  # noqa: E402
from shared_lib.billing.plans import PLAN_LIMITS, PLAN_PRICE_ENV, limits_for  # noqa: E402
from shared_lib.billing.webhooks import (  # noqa: E402
    WebhookNotConfiguredError,
    WebhookSignatureError,
)
from shared_lib.persistence.db import DB  # noqa: E402
from shared_lib.persistence.migrations import migrate  # noqa: E402

NOW = datetime(2026, 6, 1, 12, 0, tzinfo=timezone.utc)
SECRET = "whsec_test_only_not_a_real_secret"
PRICE_CFG = {
    "STRIPE_PRICE_PRO_MONTHLY": "price_pro_m",
    "STRIPE_PRICE_PRO_YEARLY": "price_pro_y",
    "STRIPE_PRICE_WHALE_MONTHLY": "price_whale_m",
    "STRIPE_PRICE_WHALE_YEARLY": "price_whale_y",
}


# --------------------------------------------------------------------------- helpers

def _make_db(tmp_path: Path, users=("user-1",)) -> DB:
    path = tmp_path / "billing_entitlements.db"
    migrate(str(path))
    db = DB(path=str(path))
    with db.connect() as conn:
        for user_id in users:
            conn.execute(
                "INSERT INTO users (id, email, hashed_password, status, created_at, updated_at) "
                "VALUES (?, ?, 'x', 'active', ?, ?)",
                (user_id, f"{user_id}@example.test", NOW.isoformat(), NOW.isoformat()),
            )
    return db


def _set_sub(db: DB, user_id="user-1", *, plan_id="plan_pro", status="active", period_end=None,
             provider="stripe", provider_sub_id="sub_1", cancel_at_period_end=0, grace_period_end=None) -> None:
    period_end = period_end if period_end is not None else NOW + timedelta(days=10)
    with db.connect() as conn:
        conn.execute("DELETE FROM subscriptions WHERE user_id = ?", (user_id,))
        conn.execute(
            """
            INSERT INTO subscriptions (user_id, plan_id, status, provider_sub_id, current_period_end,
                                       cancel_at_period_end, created_at, updated_at, provider, grace_period_end)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (user_id, plan_id, status, provider_sub_id,
             period_end.isoformat() if period_end else None, cancel_at_period_end,
             NOW.isoformat(), NOW.isoformat(), provider,
             grace_period_end.isoformat() if grace_period_end else None),
        )


def _sub_row(db: DB, user_id="user-1") -> dict | None:
    with db.connect() as conn:
        row = conn.execute("SELECT * FROM subscriptions WHERE user_id = ?", (user_id,)).fetchone()
        return dict(row) if row else None


def _add_bot(db: DB, bot_id: str, user_id="user-1", status="active") -> None:
    with db.connect() as conn:
        conn.execute(
            """
            INSERT INTO bot_instances (id, user_id, broker_account_id, market_type, strategy_id, strategy_version,
                                       config_id, risk_profile_id, symbols_json, timeframes_json, allocation_type,
                                       allocation_value, mode, status, created_at, updated_at)
            VALUES (?, ?, 'brk_1', 'CRYPTO', 'cati', '1.0.0', 'cfg', 'risk', '[]', '["15m"]', 'fixed_amount',
                    100.0, 'paper', ?, ?, ?)
            """,
            (bot_id, user_id, status, NOW.isoformat(), NOW.isoformat()),
        )


def _add_broker(db: DB, account_id: str, user_id="user-1", status="connected") -> None:
    with db.connect() as conn:
        conn.execute(
            "INSERT INTO broker_accounts (id, user_id, broker_id, market_type, status, created_at, updated_at) "
            "VALUES (?, ?, 'binance', 'crypto', ?, ?, ?)",
            (account_id, user_id, status, NOW.isoformat(), NOW.isoformat()),
        )


class FakeProvider(billing_service.PaymentProvider):
    """Stands in for Stripe: records calls, never touches the network."""

    name = "stripe"
    mode = "test"

    def __init__(self):
        self.sessions = []
        self.customers = []
        self.cancelled = []
        self.resumed = []
        self.plan_changes = []
        self.cancel_ok = True
        self.fail_customers = set()

    def create_customer(self, user_id, email):
        customer_id = f"cus_fake_{len(self.customers) + 1}"
        self.customers.append({"id": customer_id, "user_id": user_id, "email": email})
        return customer_id

    def create_checkout_session(self, *, plan_id, user_id, price_id, success_url, cancel_url, customer_id=None):
        if customer_id in self.fail_customers:
            raise RuntimeError(f"No such customer: '{customer_id}'")
        session_id = f"cs_test_fake_{len(self.sessions) + 1}"
        self.sessions.append(dict(id=session_id, plan_id=plan_id, user_id=user_id, price_id=price_id,
                                  success_url=success_url, cancel_url=cancel_url, customer_id=customer_id))
        return {"id": session_id, "url": f"https://checkout.stripe.test/{session_id}"}

    def cancel_subscription(self, provider_sub_id):
        self.cancelled.append(provider_sub_id)
        return self.cancel_ok

    def resume_subscription(self, provider_sub_id):
        self.resumed.append(provider_sub_id)
        return True

    def change_plan(self, provider_sub_id, price_id, plan_id):
        self.plan_changes.append((provider_sub_id, price_id, plan_id))
        return True


def _service(tmp_path, monkeypatch, *, production=False, provider=None, users=("user-1",), **cfg):
    """billing_service wired to a throw-away DB and an explicit configuration."""
    db = _make_db(tmp_path, users=users)
    config = {"FRONTEND_URL": "https://app.example.test", **PRICE_CFG, **cfg}
    monkeypatch.setattr(billing_service, "_db", lambda: db)
    monkeypatch.setattr(billing_service, "_cfg", lambda name: str(config.get(name, "") or "").strip())
    monkeypatch.setattr(billing_service, "is_production", lambda: production)
    if provider is not None:
        monkeypatch.setattr(billing_service, "get_provider", lambda: provider)
    return db, config


def _signed(event: dict, secret: str = SECRET, ts: int | None = None) -> tuple[bytes, str]:
    payload = json.dumps(event).encode()
    ts = int(time.time()) if ts is None else ts
    digest = hmac.new(secret.encode(), f"{ts}.".encode() + payload, hashlib.sha256).hexdigest()
    return payload, f"t={ts},v1={digest}"


def _checkout_event(session_id: str, *, user_id="user-1", plan_id="plan_pro", sub="sub_1", cus="cus_fake_1",
                    event_id="evt_checkout_1", livemode=False) -> dict:
    return {
        "id": event_id, "object": "event", "type": "checkout.session.completed",
        "created": int(time.time()), "livemode": livemode,
        "data": {"object": {
            "id": session_id, "object": "checkout.session", "mode": "subscription", "payment_status": "paid",
            "client_reference_id": user_id, "customer": cus, "subscription": sub,
            "metadata": {"user_id": user_id, "plan_id": plan_id, "interval": "month"},
        }},
    }


# --------------------------------------------------------------------------- plan limits

def test_plan_limits_table():
    assert limits_for("plan_free") == {"max_bots": 1, "max_brokers": 1, "live_trading": False, "api_access": False}
    assert limits_for("plan_pro") == {"max_bots": 5, "max_brokers": 3, "live_trading": True, "api_access": True}
    assert limits_for("plan_pro_yearly") == limits_for("plan_pro")
    assert limits_for("plan_whale") == {"max_bots": 999, "max_brokers": 999, "live_trading": True, "api_access": True}
    assert limits_for("plan_whale_yearly") == limits_for("plan_whale")
    # Unknown or missing plans never get more than the free plan.
    assert limits_for("plan_made_up") == limits_for("plan_free")
    assert limits_for(None) == limits_for("plan_free")
    # A caller mutating the returned dict cannot change the table.
    limits_for("plan_free")["max_bots"] = 50
    assert PLAN_LIMITS["plan_free"]["max_bots"] == 1


def test_catalog_limits_match_shared_table_and_advertised_features():
    plans = {p.id: p for p in billing_service.get_public_plans()}
    assert set(plans) == set(PLAN_LIMITS)
    for plan_id, plan in plans.items():
        assert plan.limits == limits_for(plan_id)
        features = {f.name: f for f in plan.features}
        bots, brokers = plan.limits["max_bots"], plan.limits["max_brokers"]
        bot_text = features["Bot Profiles"].limit
        broker_text = features["Connected Brokers"].limit
        assert bot_text == ("Unlimited" if bots >= 999 else f"{bots} bot{'s' if bots != 1 else ''}")
        assert broker_text == ("Unlimited" if brokers >= 999 else f"{brokers} broker{'s' if brokers != 1 else ''}")
        assert features["Live Trading"].included is plan.limits["live_trading"]
        # The frontend-facing strings say the same thing.
        assert plan.entitlements["max_bots"] == ("unlimited" if bots >= 999 else str(bots))
        assert plan.entitlements["max_accounts"] == ("unlimited" if brokers >= 999 else str(brokers))
        assert plan.entitlements["live_trading"] == str(plan.limits["live_trading"]).lower()


def test_every_paid_plan_has_a_price_env_var():
    paid = {p.id for p in billing_service.get_public_plans() if p.price > 0}
    assert set(PLAN_PRICE_ENV) == paid
    assert set(PLAN_PRICE_ENV.values()) == set(PRICE_CFG)


# --------------------------------------------------------------------------- effective plan / expiry

def test_user_without_subscription_is_on_free_plan(tmp_path):
    db = _make_db(tmp_path)
    plan = entitlements.get_effective_plan(db, "user-1", now=NOW)
    assert plan["plan_id"] == "plan_free"
    assert plan["status"] == "active"
    assert plan["is_paid"] is False
    assert plan["limits"] == limits_for("plan_free")


def test_active_subscription_within_period(tmp_path):
    db = _make_db(tmp_path)
    _set_sub(db, plan_id="plan_whale")
    plan = entitlements.get_effective_plan(db, "user-1", now=NOW)
    assert (plan["plan_id"], plan["status"], plan["is_paid"]) == ("plan_whale", "active", True)
    assert plan["limits"]["max_bots"] == 999


def test_overdue_active_subscription_gets_grace_then_expires_and_is_persisted(tmp_path):
    db = _make_db(tmp_path)
    period_end = NOW - timedelta(days=1)
    _set_sub(db, period_end=period_end)

    # The renewal webhook may simply be late: still entitled inside the grace window.
    inside = entitlements.get_effective_plan(db, "user-1", now=NOW)
    assert inside["plan_id"] == "plan_pro"
    assert _sub_row(db)["plan_id"] == "plan_pro"

    after = period_end + timedelta(days=entitlements.GRACE_PERIOD_DAYS, seconds=1)
    # A plain read reports the free plan without touching the row...
    assert entitlements.get_effective_plan(db, "user-1", now=after)["plan_id"] == "plan_free"
    assert _sub_row(db)["plan_id"] == "plan_pro"
    # ...the downgrade is stored only when the caller asks for it.
    expired = entitlements.get_effective_plan(db, "user-1", now=after, persist=True)
    assert (expired["plan_id"], expired["status"], expired["is_paid"]) == ("plan_free", "active", False)
    assert expired["limits"] == limits_for("plan_free")

    row = _sub_row(db)
    assert (row["plan_id"], row["status"], row["previous_plan_id"]) == ("plan_free", "expired", "plan_pro")
    assert row["updated_at"] == after.isoformat()
    # Reading again is stable.
    assert entitlements.get_effective_plan(db, "user-1", now=after)["plan_id"] == "plan_free"


def test_cancel_at_period_end_gets_no_grace(tmp_path):
    db = _make_db(tmp_path)
    period_end = NOW - timedelta(minutes=1)
    _set_sub(db, period_end=period_end, cancel_at_period_end=1)
    assert entitlements.get_effective_plan(db, "user-1", now=period_end)["plan_id"] == "plan_pro"
    assert entitlements.get_effective_plan(db, "user-1", now=NOW, persist=True)["plan_id"] == "plan_free"
    assert _sub_row(db)["status"] == "expired"


def test_row_not_backed_by_a_provider_gets_no_grace(tmp_path):
    # Rows written before webhooks existed (no payment behind them).
    db = _make_db(tmp_path)
    _set_sub(db, period_end=NOW - timedelta(minutes=1), provider=None, provider_sub_id="sub_mock")
    assert entitlements.get_effective_plan(db, "user-1", now=NOW)["plan_id"] == "plan_free"
    _set_sub(db, period_end=NOW + timedelta(days=1), provider=None, provider_sub_id="sub_mock")
    assert entitlements.get_effective_plan(db, "user-1", now=NOW)["plan_id"] == "plan_pro"


def test_past_due_keeps_access_until_grace_period_end(tmp_path):
    db = _make_db(tmp_path)
    grace_end = NOW + timedelta(days=2)
    _set_sub(db, status="past_due", period_end=NOW - timedelta(days=5), grace_period_end=grace_end)
    during = entitlements.get_effective_plan(db, "user-1", now=NOW)
    assert (during["plan_id"], during["status"]) == ("plan_pro", "past_due")
    assert during["grace_period_end"] == grace_end.isoformat()

    after = entitlements.get_effective_plan(db, "user-1", now=grace_end + timedelta(seconds=1), persist=True)
    assert after["plan_id"] == "plan_free"
    assert _sub_row(db)["status"] == "expired"


def test_non_entitling_statuses_are_free_and_left_untouched(tmp_path):
    db = _make_db(tmp_path)
    for status in ("canceled", "unpaid", "incomplete", "incomplete_expired", "paused", "expired", "garbage", ""):
        _set_sub(db, status=status)
        plan = entitlements.get_effective_plan(db, "user-1", now=NOW)
        assert plan["plan_id"] == "plan_free", status
        assert plan["limits"] == limits_for("plan_free")
        assert _sub_row(db)["status"] == status  # provider state is not rewritten


def test_stripe_row_without_period_end_is_provisionally_entitled(tmp_path):
    # Stripe said "active" in a payload that carried no period end (newer API
    # versions keep it on the items): not an expired subscription.
    db = _make_db(tmp_path)
    for status in ("active", "trialing"):
        _set_sub(db, status=status)
        with db.connect() as conn:
            conn.execute("UPDATE subscriptions SET current_period_end = NULL")
        for when in (NOW, NOW + timedelta(days=400)):
            plan = entitlements.get_effective_plan(db, "user-1", now=when, persist=True)
            assert (plan["plan_id"], plan["status"], plan["is_paid"]) == ("plan_pro", status, True), status
            assert plan["current_period_end"] is None
        row = _sub_row(db)
        assert (row["plan_id"], row["status"]) == ("plan_pro", status)  # never downgraded in place

    # A status that grants nothing still grants nothing without a period end.
    _set_sub(db, status="past_due")
    with db.connect() as conn:
        conn.execute("UPDATE subscriptions SET current_period_end = NULL")
    assert entitlements.get_effective_plan(db, "user-1", now=NOW)["plan_id"] == "plan_free"


def test_legacy_row_without_provider_or_period_end_is_not_entitled(tmp_path):
    # Rows granted by the old mock path lapse at their recorded period end;
    # with none recorded there is nothing to be entitled until.
    db = _make_db(tmp_path)
    _set_sub(db, provider=None, provider_sub_id="sub_mock")
    with db.connect() as conn:
        conn.execute("UPDATE subscriptions SET current_period_end = NULL")
    assert entitlements.get_effective_plan(db, "user-1", now=NOW)["plan_id"] == "plan_free"


def test_unknown_plan_id_is_free(tmp_path):
    db = _make_db(tmp_path)
    _set_sub(db, plan_id="plan_enterprise_secret")
    assert entitlements.get_effective_plan(db, "user-1", now=NOW)["plan_id"] == "plan_free"


def test_reading_the_plan_does_not_write_unless_asked(tmp_path):
    db = _make_db(tmp_path)
    _set_sub(db, period_end=NOW - timedelta(days=30))
    before = _sub_row(db)
    for kwargs in ({}, {"persist": False}):
        plan = entitlements.get_effective_plan(db, "user-1", now=NOW, **kwargs)
        assert (plan["plan_id"], plan["is_paid"]) == ("plan_free", False)
        assert plan["previous_plan_id"] == "plan_pro"
    # The gates built on it are read-only too.
    assert entitlements.can_trade_live(db, "user-1", now=NOW) is False
    assert entitlements.bot_quota(db, "user-1", now=NOW)["limit"] == 1
    assert entitlements.can_add_broker(db, "user-1", now=NOW) is True
    assert _sub_row(db) == before


def test_persisting_an_expiry_never_waits_long_for_the_write_lock(tmp_path):
    """Another connection holds SQLite's single write lock (the trading engine,
    or the request's own open transaction): the answer is still the free plan,
    promptly, and the downgrade is simply stored on a later read."""
    db = _make_db(tmp_path)
    _set_sub(db, period_end=NOW - timedelta(days=30))

    writer = sqlite3.connect(db.path, timeout=0.1)
    try:
        writer.execute("BEGIN IMMEDIATE")
        writer.execute("UPDATE users SET updated_at = 'held' WHERE id = 'user-1'")

        started = time.monotonic()
        plan = entitlements.get_effective_plan(db, "user-1", now=NOW, persist=True)
        elapsed = time.monotonic() - started

        assert (plan["plan_id"], plan["is_paid"]) == ("plan_free", False)
        assert plan["limits"] == limits_for("plan_free")
        assert elapsed < 3, f"waited {elapsed:.1f}s for the write lock"
    finally:
        writer.rollback()
        writer.close()
    assert _sub_row(db)["plan_id"] == "plan_pro"  # not stored while the lock was held

    # Lock released: the next persisting read stores it, and the connection it
    # used got its normal busy timeout back.
    assert entitlements.get_effective_plan(db, "user-1", now=NOW, persist=True)["plan_id"] == "plan_free"
    assert _sub_row(db)["status"] == "expired"
    conn = sqlite3.connect(db.path, timeout=7)
    try:
        _set = entitlements._try_persist_expiry(conn, {"user_id": "nobody", "plan_id": "x", "status": "y",
                                                       "updated_at": "z"}, NOW)
        assert _set is False
        assert conn.execute("PRAGMA busy_timeout").fetchone()[0] == 7000
    finally:
        conn.close()


def test_expiry_does_not_overwrite_a_concurrent_renewal(tmp_path):
    db = _make_db(tmp_path)
    _set_sub(db, period_end=NOW - timedelta(days=30))
    stale = _sub_row(db)
    # A renewal webhook lands between the read and the downgrade write.
    renewed_end = (NOW + timedelta(days=30)).isoformat()
    with db.connect() as conn:
        conn.execute("UPDATE subscriptions SET current_period_end = ?, updated_at = ?", (renewed_end, "later"))
        assert entitlements._persist_expiry(conn, stale, NOW) is False
    row = _sub_row(db)
    assert (row["plan_id"], row["status"], row["current_period_end"]) == ("plan_pro", "active", renewed_end)


def test_accepts_a_raw_sqlite_connection(tmp_path):
    db = _make_db(tmp_path)
    _set_sub(db)
    _add_bot(db, "bot_1")
    conn = sqlite3.connect(db.path)  # default row factory: plain tuples
    try:
        assert entitlements.get_effective_plan(conn, "user-1", now=NOW)["plan_id"] == "plan_pro"
        assert entitlements.count_user_bots(conn, "user-1") == 1
        assert entitlements.can_trade_live(conn, "user-1", now=NOW) is True
    finally:
        conn.close()


# --------------------------------------------------------------------------- gating

def test_can_create_bot_counts_bots_in_the_database(tmp_path):
    db = _make_db(tmp_path, users=("user-1", "user-2"))
    assert entitlements.can_create_bot(db, "user-1", now=NOW) is True  # free: 0 of 1

    _add_bot(db, "bot_1")
    assert entitlements.can_create_bot(db, "user-1", now=NOW) is False  # free: 1 of 1
    assert entitlements.bot_quota(db, "user-1", now=NOW) == {
        "plan_id": "plan_free", "limit": 1, "used": 1, "remaining": 0,
    }
    # Another user's bots do not count.
    assert entitlements.can_create_bot(db, "user-2", now=NOW) is True

    _set_sub(db)  # Pro: 5 bots
    for i in range(2, 5):
        _add_bot(db, f"bot_{i}", status="paused" if i % 2 else "stopped")
    assert entitlements.count_user_bots(db, "user-1") == 4
    assert entitlements.can_create_bot(db, "user-1", now=NOW) is True
    assert entitlements.can_create_bot(db, "user-1", requested=2, now=NOW) is False
    _add_bot(db, "bot_5")
    assert entitlements.can_create_bot(db, "user-1", now=NOW) is False

    # Deleted and archived bots free their slot.
    _add_bot(db, "bot_deleted", status="deleted")
    _add_bot(db, "bot_archived", status="archived")
    assert entitlements.count_user_bots(db, "user-1") == 5
    with db.connect() as conn:
        conn.execute("UPDATE bot_instances SET status = 'deleted' WHERE id = 'bot_5'")
    assert entitlements.can_create_bot(db, "user-1", now=NOW) is True


def test_downgrade_takes_effect_immediately_for_bot_creation(tmp_path):
    db = _make_db(tmp_path)
    _set_sub(db)
    _add_bot(db, "bot_1")
    assert entitlements.can_create_bot(db, "user-1", now=NOW) is True
    # Subscription ends (webhook wrote "canceled"): no stale allowance remains.
    with db.connect() as conn:
        conn.execute("UPDATE subscriptions SET plan_id = 'plan_free', status = 'canceled'")
    assert entitlements.can_create_bot(db, "user-1", now=NOW) is False
    assert entitlements.can_trade_live(db, "user-1", now=NOW) is False


def test_can_trade_live_follows_the_plan(tmp_path):
    db = _make_db(tmp_path)
    assert entitlements.can_trade_live(db, "user-1", now=NOW) is False
    _set_sub(db)
    assert entitlements.can_trade_live(db, "user-1", now=NOW) is True
    _set_sub(db, status="unpaid")
    assert entitlements.can_trade_live(db, "user-1", now=NOW) is False


def test_can_add_broker_respects_plan_limit(tmp_path):
    db = _make_db(tmp_path)
    assert entitlements.can_add_broker(db, "user-1", now=NOW) is True
    _add_broker(db, "brk_1")
    assert entitlements.can_add_broker(db, "user-1", now=NOW) is False  # free: 1 broker
    _add_broker(db, "brk_gone", status="disconnected")
    assert entitlements.count_user_brokers(db, "user-1") == 1
    _set_sub(db)  # Pro: 3 brokers
    _add_broker(db, "brk_2")
    assert entitlements.can_add_broker(db, "user-1", now=NOW) is True
    _add_broker(db, "brk_3")
    assert entitlements.can_add_broker(db, "user-1", now=NOW) is False


# --------------------------------------------------------------------------- billing_service: reads

def test_get_user_subscription_free_default(tmp_path, monkeypatch):
    _service(tmp_path, monkeypatch)
    sub = billing_service.get_user_subscription("user-1")
    assert sub["plan"]["id"] == "plan_free"
    assert sub["status"] == "active"
    assert sub["cancel_at_period_end"] is False
    assert sub["entitlements"] == {"max_bots": 1, "max_brokers": 1, "live_trading": False, "api_access": False}
    assert sub["usage"] == {"bots": 0, "brokers": 0}


def test_get_user_subscription_reports_real_usage(tmp_path, monkeypatch):
    db, _ = _service(tmp_path, monkeypatch)
    _set_sub(db, period_end=datetime.now(timezone.utc) + timedelta(days=5))
    _add_bot(db, "bot_1")
    _add_bot(db, "bot_2")
    _add_bot(db, "bot_3", status="deleted")
    _add_broker(db, "brk_1")
    sub = billing_service.get_user_subscription("user-1")
    assert sub["plan"]["id"] == "plan_pro"
    assert sub["usage"] == {"bots": 2, "brokers": 1}
    assert sub["entitlements"]["max_bots"] == 5


def test_get_user_subscription_downgrades_expired_subscription(tmp_path, monkeypatch):
    db, _ = _service(tmp_path, monkeypatch)
    _set_sub(db, period_end=datetime.now(timezone.utc) - timedelta(days=30))
    # Default (what token creation uses): correct answer, no write.
    sub = billing_service.get_user_subscription("user-1")
    assert sub["plan"]["id"] == "plan_free"
    assert sub["status"] == "active"
    assert sub["entitlements"]["live_trading"] is False
    assert _sub_row(db)["status"] == "active"
    # The billing status endpoint asks for the downgrade to be stored.
    sub = billing_service.get_user_subscription("user-1", persist=True)
    assert sub["plan"]["id"] == "plan_free"
    assert _sub_row(db)["status"] == "expired"


def test_check_entitlement_reads_the_database(tmp_path, monkeypatch):
    db, _ = _service(tmp_path, monkeypatch)
    assert billing_service.check_entitlement("user-1", "create_bot") is True
    assert billing_service.check_entitlement("user-1", "live_trading") is False
    assert billing_service.check_entitlement("user-1", "api_access") is False
    assert billing_service.check_entitlement("user-1", "add_broker") is True
    assert billing_service.check_entitlement("user-1", "something_else") is False

    _add_bot(db, "bot_1")
    _add_broker(db, "brk_1")
    assert billing_service.check_entitlement("user-1", "create_bot") is False
    assert billing_service.check_entitlement("user-1", "add_broker") is False

    _set_sub(db, period_end=datetime.now(timezone.utc) + timedelta(days=5))
    assert billing_service.check_entitlement("user-1", "create_bot") is True
    assert billing_service.check_entitlement("user-1", "live_trading") is True
    assert billing_service.check_entitlement("user-1", "api_access") is True


# --------------------------------------------------------------------------- billing_service: provider selection

def test_production_without_stripe_key_is_not_configured(tmp_path, monkeypatch):
    _service(tmp_path, monkeypatch, production=True)
    with pytest.raises(billing_service.BillingNotConfigured):
        billing_service.get_provider()
    with pytest.raises(billing_service.BillingNotConfigured):
        billing_service.create_checkout_session("user-1", "plan_pro")


def test_mock_provider_cannot_exist_in_production(tmp_path, monkeypatch):
    _service(tmp_path, monkeypatch, production=True)
    with pytest.raises(billing_service.BillingNotConfigured):
        billing_service.MockPaymentProvider()


def test_missing_stripe_sdk_never_falls_back_to_mock(tmp_path, monkeypatch):
    for production in (True, False):
        _service(tmp_path / str(production), monkeypatch, production=production,
                 STRIPE_SECRET_KEY="sk_live_placeholder_not_a_key")
        monkeypatch.setattr(billing_service, "stripe", None)
        with pytest.raises(billing_service.BillingNotConfigured):
            billing_service.get_provider()


def test_non_production_without_key_uses_mock_that_grants_nothing(tmp_path, monkeypatch):
    db, _ = _service(tmp_path, monkeypatch, production=False)
    assert isinstance(billing_service.get_provider(), billing_service.MockPaymentProvider)
    result = billing_service.create_checkout_session("user-1", "plan_whale")
    assert result["id"].startswith("cs_mock_")
    assert result["url"].startswith("https://app.example.test/payment/success?session_id=cs_mock_")
    # No plan was granted by starting (or "finishing") a mock checkout.
    assert _sub_row(db) is None
    assert billing_service.get_user_subscription("user-1")["plan"]["id"] == "plan_free"
    assert billing_service.get_checkout_status("user-1", result["id"])["status"] == "pending"


def test_test_mode_key_rejected_in_production_unless_opted_in(tmp_path, monkeypatch):
    fake_stripe = SimpleNamespace(api_key=None)
    _service(tmp_path / "a", monkeypatch, production=True, STRIPE_SECRET_KEY="sk_test_placeholder_not_a_key")
    monkeypatch.setattr(billing_service, "stripe", fake_stripe)
    with pytest.raises(billing_service.BillingNotConfigured):
        billing_service.get_provider()

    _service(tmp_path / "b", monkeypatch, production=True, STRIPE_SECRET_KEY="sk_test_placeholder_not_a_key",
             BILLING_ALLOW_STRIPE_TEST_MODE="true")
    provider = billing_service.get_provider()
    assert (provider.name, provider.mode) == ("stripe", "test")

    _service(tmp_path / "c", monkeypatch, production=True, STRIPE_SECRET_KEY="sk_live_placeholder_not_a_key")
    provider = billing_service.get_provider()
    assert (provider.name, provider.mode) == ("stripe", "live")
    assert fake_stripe.api_key == "sk_live_placeholder_not_a_key"


def test_cfg_falls_back_to_process_environment(monkeypatch):
    monkeypatch.setenv("BILLING_TEST_ONLY_SETTING", "  from-env  ")
    assert billing_service._cfg("BILLING_TEST_ONLY_SETTING") == "from-env"
    monkeypatch.delenv("BILLING_TEST_ONLY_SETTING")
    assert billing_service._cfg("BILLING_TEST_ONLY_SETTING") == ""


# --------------------------------------------------------------------------- billing_service: checkout

def test_checkout_uses_server_side_price_customer_and_urls(tmp_path, monkeypatch):
    provider = FakeProvider()
    db, _ = _service(tmp_path, monkeypatch, provider=provider)

    result = billing_service.create_checkout_session("user-1", "plan_pro_yearly", email="user-1@example.test")
    assert result == {"id": "cs_test_fake_1", "url": "https://checkout.stripe.test/cs_test_fake_1"}

    session = provider.sessions[0]
    assert session["price_id"] == "price_pro_y"
    assert session["plan_id"] == "plan_pro_yearly"
    assert session["user_id"] == "user-1"
    assert session["customer_id"] == "cus_fake_1"
    assert session["success_url"] == "https://app.example.test/payment/success?session_id={CHECKOUT_SESSION_ID}"
    assert session["cancel_url"] == "https://app.example.test/dashboard/subscription?checkout=cancelled"
    assert provider.customers == [{"id": "cus_fake_1", "user_id": "user-1", "email": "user-1@example.test"}]

    with db.connect() as conn:
        intents = [dict(r) for r in conn.execute("SELECT * FROM pricing_intents").fetchall()]
        customers = [dict(r) for r in conn.execute("SELECT * FROM billing_customers").fetchall()]
    assert [(i["user_id"], i["plan_id"], i["session_id"]) for i in intents] == [
        ("user-1", "plan_pro_yearly", "cs_test_fake_1")
    ]
    assert [(c["user_id"], c["mode"], c["provider_customer_id"]) for c in customers] == [
        ("user-1", "test", "cus_fake_1")
    ]
    # Starting a checkout grants nothing.
    assert _sub_row(db) is None

    # A second checkout reuses the stored customer instead of creating another.
    billing_service.create_checkout_session("user-1", "plan_pro")
    assert len(provider.customers) == 1
    assert provider.sessions[1]["customer_id"] == "cus_fake_1"
    assert provider.sessions[1]["price_id"] == "price_pro_m"


def test_checkout_rejects_free_and_unknown_plans(tmp_path, monkeypatch):
    provider = FakeProvider()
    _service(tmp_path, monkeypatch, provider=provider)
    for plan_id in ("plan_free", "plan_enterprise", "", "price_pro_m"):
        with pytest.raises(billing_service.BillingError):
            billing_service.create_checkout_session("user-1", plan_id)
    assert provider.sessions == []


def test_checkout_requires_a_configured_price(tmp_path, monkeypatch):
    provider = FakeProvider()
    db, _ = _service(tmp_path, monkeypatch, provider=provider, STRIPE_PRICE_WHALE_MONTHLY="")
    with pytest.raises(billing_service.BillingNotConfigured):
        billing_service.create_checkout_session("user-1", "plan_whale")
    assert provider.sessions == []
    with db.connect() as conn:
        assert conn.execute("SELECT COUNT(*) FROM pricing_intents").fetchone()[0] == 0


def test_same_price_for_two_plans_is_refused(tmp_path, monkeypatch):
    provider = FakeProvider()
    _service(tmp_path, monkeypatch, provider=provider, STRIPE_PRICE_WHALE_MONTHLY="price_pro_m")
    assert "price_pro_m" not in billing_service.price_to_plan_map()
    for plan_id in ("plan_pro", "plan_whale"):
        with pytest.raises(billing_service.BillingNotConfigured):
            billing_service.create_checkout_session("user-1", plan_id)


def test_checkout_needs_frontend_url_in_production(tmp_path, monkeypatch):
    provider = FakeProvider()
    _service(tmp_path / "a", monkeypatch, provider=provider, production=True, FRONTEND_URL="")
    with pytest.raises(billing_service.BillingNotConfigured):
        billing_service.create_checkout_session("user-1", "plan_pro")
    _service(tmp_path / "b", monkeypatch, provider=provider, production=True, FRONTEND_URL="javascript:alert(1)")
    with pytest.raises(billing_service.BillingNotConfigured):
        billing_service.create_checkout_session("user-1", "plan_pro")
    assert provider.sessions == []

    # Outside production the local dev frontend is the default.
    _service(tmp_path / "c", monkeypatch, provider=provider, production=False, FRONTEND_URL="")
    assert billing_service.checkout_urls()["success_url"].startswith("http://localhost:5173/payment/success?")
    # PUBLIC_APP_URL wins over FRONTEND_URL; trailing slashes are dropped.
    _service(tmp_path / "d", monkeypatch, provider=provider, PUBLIC_APP_URL="https://app.example.test/")
    assert billing_service.checkout_urls()["cancel_url"] == (
        "https://app.example.test/dashboard/subscription?checkout=cancelled"
    )


def test_checkout_blocked_while_a_stripe_subscription_is_live(tmp_path, monkeypatch):
    provider = FakeProvider()
    db, _ = _service(tmp_path, monkeypatch, provider=provider)
    _set_sub(db, period_end=datetime.now(timezone.utc) + timedelta(days=5))
    with pytest.raises(billing_service.BillingConflict):
        billing_service.create_checkout_session("user-1", "plan_whale")
    assert provider.sessions == []
    assert issubclass(billing_service.BillingConflict, billing_service.BillingError)


def test_stale_stored_customer_is_replaced(tmp_path, monkeypatch):
    provider = FakeProvider()
    db, _ = _service(tmp_path, monkeypatch, provider=provider)
    with db.connect() as conn:
        conn.execute(
            "INSERT INTO billing_customers (user_id, mode, provider, provider_customer_id, created_at, updated_at) "
            "VALUES ('user-1', 'test', 'stripe', 'cus_deleted', ?, ?)", (NOW.isoformat(), NOW.isoformat()),
        )
    provider.fail_customers.add("cus_deleted")
    result = billing_service.create_checkout_session("user-1", "plan_pro")
    assert result["id"] == "cs_test_fake_1"
    assert provider.sessions[0]["customer_id"] == "cus_fake_1"
    with db.connect() as conn:
        stored = conn.execute("SELECT provider_customer_id FROM billing_customers").fetchall()
    assert [r[0] for r in stored] == ["cus_fake_1"]


def test_provider_failure_is_a_provider_error(tmp_path, monkeypatch):
    provider = FakeProvider()

    def explode(**kwargs):
        raise RuntimeError("card network unreachable")

    provider.create_checkout_session = explode
    db, _ = _service(tmp_path, monkeypatch, provider=provider)
    with pytest.raises(billing_service.BillingProviderError):
        billing_service.create_checkout_session("user-1", "plan_pro")
    assert _sub_row(db) is None


def test_stripe_provider_builds_a_subscription_session_from_a_price_id(tmp_path, monkeypatch):
    calls = {}

    def create_session(**params):
        calls["session"] = params
        return SimpleNamespace(id="cs_test_sdk", url="https://checkout.stripe.test/cs_test_sdk")

    def create_customer(**params):
        calls["customer"] = params
        return SimpleNamespace(id="cus_sdk")

    fake_stripe = SimpleNamespace(
        api_key=None,
        checkout=SimpleNamespace(Session=SimpleNamespace(create=create_session)),
        Customer=SimpleNamespace(create=create_customer),
    )
    _service(tmp_path, monkeypatch, STRIPE_SECRET_KEY="sk_test_placeholder_not_a_key")
    monkeypatch.setattr(billing_service, "stripe", fake_stripe)

    result = billing_service.create_checkout_session("user-1", "plan_whale", email="user-1@example.test")
    assert result == {"id": "cs_test_sdk", "url": "https://checkout.stripe.test/cs_test_sdk"}
    assert calls["customer"] == {"metadata": {"user_id": "user-1"}, "email": "user-1@example.test"}

    params = calls["session"]
    assert params["mode"] == "subscription"
    assert params["line_items"] == [{"price": "price_whale_m", "quantity": 1}]
    assert "price_data" not in json.dumps(params)  # no amounts built on the fly
    assert params["client_reference_id"] == "user-1"
    assert params["customer"] == "cus_sdk"
    assert params["metadata"] == {"user_id": "user-1", "plan_id": "plan_whale", "interval": "month"}
    assert params["subscription_data"] == {"metadata": params["metadata"]}
    assert params["success_url"] == "https://app.example.test/payment/success?session_id={CHECKOUT_SESSION_ID}"
    assert params["cancel_url"].startswith("https://app.example.test/dashboard/subscription")


# --------------------------------------------------------------------------- billing_service: webhook + status

def test_webhook_activates_and_checkout_status_follows(tmp_path, monkeypatch):
    provider = FakeProvider()
    db, _ = _service(tmp_path, monkeypatch, provider=provider, users=("user-1", "user-2"),
                     STRIPE_WEBHOOK_SECRET=SECRET)
    session = billing_service.create_checkout_session("user-1", "plan_pro")
    status = billing_service.get_checkout_status("user-1", session["id"])
    assert status == {"session_id": session["id"], "status": "pending",
                      "plan_id": "plan_pro", "current_plan_id": "plan_free"}
    # Asking for the status grants nothing, and other users cannot see the session.
    assert _sub_row(db) is None
    assert billing_service.get_checkout_status("user-2", session["id"]) is None
    assert billing_service.get_checkout_status("user-1", "cs_test_unknown") is None

    payload, header = _signed(_checkout_event(session["id"]))
    assert billing_service.handle_stripe_webhook(payload, header)["status"] == "processed"
    assert billing_service.handle_stripe_webhook(payload, header)["status"] == "duplicate"

    status = billing_service.get_checkout_status("user-1", session["id"])
    assert (status["status"], status["current_plan_id"]) == ("active", "plan_pro")
    assert billing_service.get_user_subscription("user-1")["plan"]["id"] == "plan_pro"


def test_webhook_rejects_bad_or_missing_signature_and_grants_nothing(tmp_path, monkeypatch):
    db, _ = _service(tmp_path, monkeypatch, STRIPE_WEBHOOK_SECRET=SECRET)
    event = _checkout_event("cs_test_forged", plan_id="plan_whale")
    payload, header = _signed(event, secret="whsec_attacker_guess")
    with pytest.raises(WebhookSignatureError):
        billing_service.handle_stripe_webhook(payload, header)
    with pytest.raises(WebhookSignatureError):
        billing_service.handle_stripe_webhook(payload, None)
    stale_payload, stale_header = _signed(event, ts=int(time.time()) - 301)
    with pytest.raises(WebhookSignatureError):
        billing_service.handle_stripe_webhook(stale_payload, stale_header)
    assert _sub_row(db) is None
    with db.connect() as conn:
        assert conn.execute("SELECT COUNT(*) FROM billing_events").fetchone()[0] == 0


def test_webhook_without_configured_secret_rejects_everything(tmp_path, monkeypatch):
    for production in (True, False):
        db, _ = _service(tmp_path / str(production), monkeypatch, production=production, STRIPE_WEBHOOK_SECRET="")
        payload, header = _signed(_checkout_event("cs_test_1"), secret="")
        with pytest.raises(WebhookNotConfiguredError):
            billing_service.handle_stripe_webhook(payload, header)
        assert _sub_row(db) is None


def test_production_ignores_test_mode_events(tmp_path, monkeypatch):
    db, _ = _service(tmp_path, monkeypatch, production=True, STRIPE_WEBHOOK_SECRET=SECRET)
    payload, header = _signed(_checkout_event("cs_test_1", livemode=False))
    assert billing_service.handle_stripe_webhook(payload, header)["status"] == "ignored"
    assert _sub_row(db) is None
    payload, header = _signed(_checkout_event("cs_live_1", livemode=True, event_id="evt_live"))
    assert billing_service.handle_stripe_webhook(payload, header)["status"] == "processed"
    assert _sub_row(db)["plan_id"] == "plan_pro"


# --------------------------------------------------------------------------- billing_service: cancel / resume / change

def test_cancel_sets_cancel_at_period_end_in_stripe_then_locally(tmp_path, monkeypatch):
    provider = FakeProvider()
    db, _ = _service(tmp_path, monkeypatch, provider=provider)
    assert billing_service.cancel_subscription("user-1") is False  # free plan: nothing to cancel

    _set_sub(db, period_end=datetime.now(timezone.utc) + timedelta(days=5))
    assert billing_service.cancel_subscription("user-1") is True
    assert provider.cancelled == ["sub_1"]
    row = _sub_row(db)
    assert row["cancel_at_period_end"] == 1
    assert (row["plan_id"], row["status"]) == ("plan_pro", "active")  # access continues until period end

    assert billing_service.resume_subscription("user-1") is True
    assert provider.resumed == ["sub_1"]
    assert _sub_row(db)["cancel_at_period_end"] == 0
    assert billing_service.resume_subscription("user-1") is False


def test_cancel_failure_at_provider_does_not_pretend_success(tmp_path, monkeypatch):
    provider = FakeProvider()
    provider.cancel_ok = False
    db, _ = _service(tmp_path, monkeypatch, provider=provider)
    _set_sub(db, period_end=datetime.now(timezone.utc) + timedelta(days=5))
    with pytest.raises(billing_service.BillingProviderError):
        billing_service.cancel_subscription("user-1")
    assert _sub_row(db)["cancel_at_period_end"] == 0


def test_cancel_of_local_only_row_expires_at_period_end(tmp_path, monkeypatch):
    provider = FakeProvider()
    db, _ = _service(tmp_path, monkeypatch, provider=provider)
    period_end = datetime.now(timezone.utc) + timedelta(days=5)
    _set_sub(db, period_end=period_end, provider=None, provider_sub_id="sub_mock")
    assert billing_service.cancel_subscription("user-1") is True
    assert provider.cancelled == []  # nothing in Stripe to cancel
    assert _sub_row(db)["cancel_at_period_end"] == 1
    later = period_end + timedelta(seconds=1)
    assert entitlements.get_effective_plan(db, "user-1", now=later)["plan_id"] == "plan_free"


def test_change_plan_modifies_existing_subscription_without_granting(tmp_path, monkeypatch):
    provider = FakeProvider()
    db, _ = _service(tmp_path, monkeypatch, provider=provider)
    with pytest.raises(billing_service.BillingError):
        billing_service.change_plan("user-1", "plan_whale")  # no subscription yet

    _set_sub(db, period_end=datetime.now(timezone.utc) + timedelta(days=5))
    assert billing_service.has_provider_subscription("user-1") is True
    assert billing_service.change_plan("user-1", "plan_whale") is True
    assert provider.plan_changes == [("sub_1", "price_whale_m", "plan_whale")]
    # The plan only changes when Stripe's signed subscription.updated arrives.
    assert _sub_row(db)["plan_id"] == "plan_pro"

    with pytest.raises(billing_service.BillingConflict):
        billing_service.change_plan("user-1", "plan_pro")
    with pytest.raises(billing_service.BillingError):
        billing_service.change_plan("user-1", "plan_free")


def test_list_invoices_exposes_integer_minor_units(tmp_path, monkeypatch):
    db, _ = _service(tmp_path, monkeypatch)
    with db.connect() as conn:
        conn.execute(
            "INSERT INTO invoices (id, user_id, amount, amount_cents, currency, status, hosted_invoice_url, created_at) "
            "VALUES ('in_1', 'user-1', 29.0, 2900, 'USD', 'paid', 'https://invoice.stripe.test/in_1', ?)",
            (NOW.isoformat(),),
        )
    assert billing_service.list_invoices("user-1") == [{
        "id": "in_1", "amount": 29.0, "amount_cents": 2900, "currency": "USD", "status": "paid",
        "date": NOW.isoformat(), "pdf_url": "https://invoice.stripe.test/in_1",
    }]


def test_upgrades_are_recognised_by_what_the_plan_grants():
    up = billing_service.is_plan_upgrade
    assert up("plan_pro", "plan_whale") is True
    assert up("plan_pro_yearly", "plan_whale") is True
    assert up("plan_pro", "plan_whale_yearly") is True
    assert up("plan_free", "plan_pro") is True
    # Downgrades and same-tier interval changes keep the deferred proration.
    assert up("plan_whale", "plan_pro") is False
    assert up("plan_whale_yearly", "plan_pro") is False
    assert up("plan_pro", "plan_pro_yearly") is False
    assert up("plan_whale_yearly", "plan_whale") is False


def test_stripe_provider_invoices_upgrades_immediately(tmp_path, monkeypatch):
    calls = []
    current = {"price": "price_pro_m"}

    def list_items(**params):
        assert params == {"subscription": "sub_1", "limit": 1}
        return SimpleNamespace(data=[SimpleNamespace(id="si_1", price=SimpleNamespace(id=current["price"]))])

    def modify(sub_id, **params):
        calls.append((sub_id, params))
        return SimpleNamespace(id=sub_id)

    fake_stripe = SimpleNamespace(
        api_key=None,
        SubscriptionItem=SimpleNamespace(list=list_items),
        Subscription=SimpleNamespace(modify=modify),
    )
    _service(tmp_path, monkeypatch, STRIPE_SECRET_KEY="sk_test_placeholder_not_a_key")
    monkeypatch.setattr(billing_service, "stripe", fake_stripe)
    provider = billing_service.get_provider()

    # pro -> whale: charged now, so "upgrade, then cancel" cannot yield an unpaid higher plan.
    assert provider.change_plan("sub_1", "price_whale_m", "plan_whale") is True
    # whale -> pro and pro -> pro yearly: the usual deferred proration.
    current["price"] = "price_whale_m"
    assert provider.change_plan("sub_1", "price_pro_m", "plan_pro") is True
    current["price"] = "price_pro_m"
    assert provider.change_plan("sub_1", "price_pro_y", "plan_pro_yearly") is True
    # A current price that is not one of ours: charge now rather than guess.
    current["price"] = "price_unknown"
    assert provider.change_plan("sub_1", "price_pro_m", "plan_pro") is True

    assert [c[1]["proration_behavior"] for c in calls] == [
        "always_invoice", "create_prorations", "create_prorations", "always_invoice"]
    for sub_id, params in calls:
        assert sub_id == "sub_1"
        assert params["items"][0]["id"] == "si_1"
        assert params["cancel_at_period_end"] is False
