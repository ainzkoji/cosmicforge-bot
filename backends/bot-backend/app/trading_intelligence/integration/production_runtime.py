"""Canonical CATI production collection, execution and broker reconciliation."""
from __future__ import annotations

import asyncio
import json
import logging
import time
from types import SimpleNamespace

from app.core.config import settings
from shared_lib.broker.environment import BrokerEnvironment, normalize_environment
from shared_lib.broker.resolver import resolve_broker_auth
from shared_lib.broker.client_factory import build_client_from_auth
from .residual_prospective import FAMILY, REGISTRY_HASH, owner_current, schedule as residual_schedule
from .forward_observe import schedule as forward_schedule

logger = logging.getLogger(__name__)


def live_accounts(db):
    with db.connect() as c:
        rows = [dict(r) for r in c.execute("SELECT id,user_id,broker_id,environment,status FROM broker_accounts")]
    return [r for r in rows if str(r["status"]).lower() in {"connected", "active"}
            and str(r["environment"]).lower() in {"live", "mainnet", "production"}]


def initialize(db):
    with db.connect() as c:
        c.execute("""CREATE TABLE IF NOT EXISTS cati_production_state (
            account_id TEXT PRIMARY KEY, user_id TEXT, observed_at INTEGER NOT NULL, document TEXT NOT NULL)""")


def sync_account(db, account, *, factory=build_client_from_auth, execute=False):
    """Only broker GETs. A failed partial read never becomes a fresh snapshot."""
    auth = resolve_broker_auth(account["id"], account["user_id"], db)
    if normalize_environment(auth.environment) != BrokerEnvironment.LIVE:
        raise ValueError("PRODUCTION_REQUIRES_LIVE_BROKER_ACCOUNT")
    client = factory(auth)
    client._production_credential_version = getattr(auth, "credential_version", None)
    balance = client.get_balance()
    positions = client.position_risk()
    orders = client.open_orders()
    if not isinstance(balance, dict) or not isinstance(positions, list) or not isinstance(orders, list):
        raise ValueError("BROKER_READ_SHAPE_INVALID")
    # Reuse canonical position reconciliation; it changes local projections,
    # never the exchange. Each projection keeps its LIVE account provenance.
    from app.execution.position_reconciliation import parse_broker_positions, reconcile_position_rows, _position_spec
    with db.connect() as c:
        bots = c.execute("SELECT id FROM bot_instances WHERE broker_account_id=? AND status='active'",
                         (account["id"],)).fetchall()
    reconciliation = []
    # Multiple bot owners on the same account cannot each claim its full book.
    if len(bots) == 1:
        hedge = any(str(p.get("positionSide", "BOTH")).upper() in {"LONG", "SHORT"} for p in positions)
        reconciliation.append(reconcile_position_rows(db, bot_instance_id=bots[0][0], broker_account_id=account["id"],
            broker_positions=parse_broker_positions(positions, hedge_mode=hedge),
            position_mode="HEDGE" if hedge else "ONE_WAY", spec_resolver=lambda s: _position_spec(client, s),
            execution_mode="broker", broker_environment="live"))
    from app.activation.account_status import refresh_if_stale
    discovery = refresh_if_stale(db, auth, client_factory=factory)
    now = int(time.time() * 1000)
    result = {"status": "SYNCED", "environment": "LIVE", "balance": balance, "positions": positions,
              "orders": orders, "discovery": discovery, "reconciliation": reconciliation,
              "reconciliation_status": "SYNCED" if len(bots) == 1 else "ACCOUNT_OWNER_MAPPING_REQUIRED",
              "risk": {"daily_hard_loss_fraction": min(.025, settings.ADAPTIVE_DAILY_RISK_MAX_DAILY_LOSS_PCT),
                       "entry_permission": "BLOCKED", "reason": "LIVE_ORDER_SUBMISSION_DISABLED"
                       if not settings.LIVE_ORDER_SUBMISSION_ENABLED else "ACCOUNT_RISK_PENDING"},
              "observed_at": now}
    if execute:
        from .production_execution import process_account
        try:
            result["execution"] = process_account(db, account, client, result)
            result["risk"] = result["execution"].get("risk", result["risk"])
        except Exception as exc:
            code = getattr(exc, "reason_code", None)
            if not code and isinstance(exc, ValueError):
                candidate = str(exc)
                code = candidate if candidate.replace("_", "").isalnum() and candidate.upper() == candidate else None
            result["execution"] = {"execution_permission": "BLOCKED_ACCOUNT", "reason":
                code or type(exc).__name__}
            if not settings.LIVE_ORDER_SUBMISSION_ENABLED:
                result["execution"].update(execution_permission="BLOCKED_ORDER_GATE",
                    block_reason_before_order_gate=result["execution"]["reason"], reason="LIVE_ORDER_SUBMISSION_DISABLED")
    result["credential_version"] = getattr(auth, "credential_version", None)
    save(db, account["id"], account["user_id"], now, result)
    return result


def save(db, account_id, user_id, now, result):
    with db.connect() as c:
        c.execute("INSERT OR REPLACE INTO cati_production_state VALUES(?,?,?,?)",
                  (account_id, user_id, now, json.dumps(result, default=str)))


def sync(db):
    if not owner_current(db):
        return
    initialize(db)
    accounts = live_accounts(db)
    for account in accounts:
        if not owner_current(db):
            return
        try:
            sync_account(db, account, execute=True)
        except Exception as exc:
            # Never publish credentials or signed exception URLs.
            save(db, account["id"], account["user_id"], int(time.time()*1000),
                 {"status": "READ_FAILED", "environment": "LIVE", "reason": type(exc).__name__,
                  "balance": None, "positions": None, "orders": None})
            logger.warning("[CATI_PRODUCTION] broker read failed: %s", type(exc).__name__)


def status(db, *, user_id=None):
    accounts = live_accounts(db)
    if user_id is not None:
        accounts = [a for a in accounts if a["user_id"] == user_id]
    states = []
    with db.connect() as c:
        exists = c.execute("SELECT 1 FROM sqlite_master WHERE name='cati_production_state'").fetchone()
        for account in accounts:
            row = c.execute("SELECT observed_at,document FROM cati_production_state WHERE account_id=? AND user_id=?",
                            (account["id"], account["user_id"])).fetchone() if exists else None
            if row:
                data = json.loads(row[1])
                data["age_seconds"] = max(0, (time.time()*1000-row[0])/1000)
                if data["age_seconds"] > 120:
                    data["status"] = "STALE"
                    data["execution"] = {"execution_permission": "BLOCKED_ACCOUNT", "reason": "BROKER_SNAPSHOT_STALE"}
                states.append({"account_id": account["id"], **data})
            else:
                states.append({"account_id": account["id"], "status": "AWAITING_FIRST_LIVE_SYNC"})
    return {"configuration": settings.configuration_matrix(), "strategy": FAMILY, "registry_hash": REGISTRY_HASH,
            "status": "LIVE_ACCOUNTS_PRESENT" if accounts else "LIVE_ACCOUNT_REQUIRED",
            "accounts": states, "order_submission_enabled": settings.LIVE_ORDER_SUBMISSION_ENABLED,
            "cati_mode": "LIVE", "trading": "ACTIVE", "broker_environment": "LIVE",
            "execution_permission": "BLOCKED_ORDER_GATE" if not settings.LIVE_ORDER_SUBMISSION_ENABLED else
                next((s.get("execution", {}).get("execution_permission") for s in states
                      if s.get("execution", {}).get("execution_permission")), "BLOCKED_ACCOUNT"),
            "daily_hard_loss_fraction": min(.025, settings.ADAPTIVE_DAILY_RISK_MAX_DAILY_LOSS_PCT)}


async def run(db):
    if not settings.production:
        raise ValueError("CATI_PRODUCTION_PROFILE_REQUIRED")
    runner = SimpleNamespace(db=db)
    fx_check = 0.
    while True:
        try:
            if owner_current(db):
                residual_schedule(runner)
                forward_schedule(runner)
                if time.monotonic() - fx_check > 60:
                    from .residual_simulation import ensure_fx_watcher
                    await asyncio.to_thread(ensure_fx_watcher)
                    fx_check = time.monotonic()
                await asyncio.to_thread(sync, db)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("[CATI_PRODUCTION] collection cycle failed; broker mutations remain guarded")
        await asyncio.sleep(30)
