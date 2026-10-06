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
from shared_lib.core.production import order_submission_gate
from .residual_prospective import FAMILY, REGISTRY_HASH, owner_current, schedule as residual_schedule
from .forward_observe import schedule as forward_schedule

logger = logging.getLogger(__name__)


def execution_accounts(db):
    with db.connect() as c:
        rows = [dict(r) for r in c.execute("SELECT id,user_id,broker_id,environment,status FROM broker_accounts")]
    accounts = []
    for row in rows:
        if str(row["status"]).lower() not in {"connected", "active"}:
            continue
        try:
            row["environment"] = normalize_environment(row["environment"]).value.upper()
        except ValueError:
            continue
        accounts.append(row)
    return accounts


def account_identity(account):
    from shared_lib.broker.environment import resolve_base_url
    env = normalize_environment(account["environment"])
    try:
        url = resolve_base_url(account["broker_id"], env)
    except ValueError:
        url = None
    return {"account_id": account["id"], "broker": account["broker_id"].upper(), "environment": env.value.upper(),
            "canonical_base_url": url, "order_submission_gate": order_submission_gate(env)}


def initialize(db):
    with db.connect() as c:
        c.execute("""CREATE TABLE IF NOT EXISTS cati_production_state (
            account_id TEXT PRIMARY KEY, user_id TEXT, observed_at INTEGER NOT NULL, document TEXT NOT NULL)""")


def sync_account(db, account, *, factory=build_client_from_auth, execute=False):
    """Read account-scoped broker truth; optionally dispatch the canonical boundary."""
    auth = resolve_broker_auth(account["id"], account["user_id"], db)
    env = normalize_environment(auth.environment)
    if account.get("environment") and normalize_environment(account["environment"]) != env:
        raise ValueError("BROKER_ENVIRONMENT_MISMATCH")
    account = {**account, "environment": env.value.upper(),
               "broker_id": account.get("broker_id", getattr(auth, "broker_type", "binance"))}
    gate = order_submission_gate(env)
    client = factory(auth)
    client._production_credential_version = getattr(auth, "credential_version", None)
    balance = client.get_balance()
    positions = client.position_risk()
    orders = client.open_orders()
    if not isinstance(balance, dict) or not isinstance(positions, list) or not isinstance(orders, list):
        raise ValueError("BROKER_READ_SHAPE_INVALID")
    # Reuse canonical position reconciliation; it changes local projections,
    # never the exchange. Each projection keeps its resolved account provenance.
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
            execution_mode="broker", broker_environment=env.value.upper().lower()))
    from app.activation.account_status import refresh_if_stale
    discovery = refresh_if_stale(db, auth, client_factory=factory)
    now = int(time.time() * 1000)
    result = {**account_identity(account), "status": "SYNCED", "balance": balance, "positions": positions,
              "orders": orders, "discovery": discovery, "reconciliation": reconciliation,
              "reconciliation_status": "SYNCED" if len(bots) == 1 else "ACCOUNT_EXECUTION_OWNER_AMBIGUOUS" if bots else "ACCOUNT_OWNER_MAPPING_REQUIRED",
              "credential": "READY",
              "risk": {"daily_loss_limit_source": "PER_BOT_EFFECTIVE_POLICY",
                       "entry_permission": "BLOCKED", "reason": gate["reason"]
                       if not gate["enabled"] else "ACCOUNT_RISK_PENDING"},
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
            result["execution"] = getattr(exc, 'production_evaluation', None) or {
                "execution_permission": "BLOCKED_ACCOUNT", "reason": code or type(exc).__name__}
            if not gate["enabled"]:
                result["execution"].update(execution_permission="BLOCKED_"+env.value.upper()+"_ORDER_GATE",
                    block_reason_before_order_gate=result["execution"]["reason"], reason=gate["reason"])
    result["execution_permission"] = result.get("execution", {}).get("execution_permission", "BLOCKED_ACCOUNT")
    result["credential_version"] = getattr(auth, "credential_version", None)
    save(db, account["id"], account["user_id"], now, result)
    return result


def save(db, account_id, user_id, now, result):
    with db.connect() as c:
        c.execute("INSERT OR REPLACE INTO cati_production_state VALUES(?,?,?,?)",
                  (account_id, user_id, now, json.dumps(result, default=str)))
        columns = {r[1] for r in c.execute("PRAGMA table_info(bot_instances)")}
        if {"bot_health_status", "bot_health_reason_code", "bot_health_message"} <= columns:
            execution = result.get("execution", {})
            health = execution.get("execution_permission", result.get("execution_permission", "BLOCKED_ACCOUNT"))
            reason = execution.get("reason", result.get("reason", "BROKER_SYNC_PENDING"))
            c.execute("UPDATE bot_instances SET bot_health_status=?,bot_health_reason_code=?,bot_health_message=? WHERE broker_account_id=? AND user_id=? AND status='active'",
                (health, reason, "CATI account-scoped production state", account_id, user_id))


def sync(db):
    if not owner_current(db):
        return
    initialize(db)
    accounts = execution_accounts(db)
    for account in accounts:
        if not owner_current(db):
            return
        if account["broker_id"].lower() not in {"binance", "bybit", "bingx"}:
            save(db, account["id"], account["user_id"], int(time.time()*1000),
                 {**account_identity(account), "status": "CAPABILITY_UNAVAILABLE",
                  "execution_permission": "BLOCKED_ACCOUNT", "execution": {
                      "execution_permission": "BLOCKED_ACCOUNT", "reason": "DEMO_ADAPTER_UNVALIDATED"
                      if account["environment"] == "DEMO" else "EXECUTION_ADAPTER_UNVALIDATED"},
                  "balance": None, "positions": None, "orders": None})
            continue
        try:
            sync_account(db, account, execute=True)
        except Exception as exc:
            # Never publish credentials or signed exception URLs.
            save(db, account["id"], account["user_id"], int(time.time()*1000),
                 {**account_identity(account), "status": "READ_FAILED", "reason":
                  getattr(exc, "reason_code", None) or type(exc).__name__,
                  "execution_permission": "BLOCKED_ACCOUNT", "execution": {"execution_permission": "BLOCKED_ACCOUNT",
                      "reason": getattr(exc, "reason_code", None) or type(exc).__name__},
                  "balance": None, "positions": None, "orders": None})
            logger.warning("[CATI_PRODUCTION] broker read failed: %s", type(exc).__name__)


def status(db, *, user_id=None):
    accounts = execution_accounts(db)
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
                if data.get("environment") and normalize_environment(data["environment"]) != normalize_environment(account["environment"]):
                    data = {"status": "ENVIRONMENT_CHANGED", "execution": {
                        "execution_permission": "BLOCKED_ACCOUNT", "reason": "BROKER_ENVIRONMENT_MISMATCH"}}
                data["age_seconds"] = max(0, (time.time()*1000-row[0])/1000)
                if data["age_seconds"] > 120 and data.get("status") == "SYNCED":
                    data["status"] = "STALE"
                    data["execution"] = {"execution_permission": "BLOCKED_ACCOUNT", "reason": "BROKER_SNAPSHOT_STALE"}
                gate = order_submission_gate(account["environment"])
                if not gate["enabled"]:
                    data["execution"] = {**data.get("execution", {}),
                        "block_reason_before_order_gate": data.get("execution", {}).get("reason"),
                        "execution_permission": "BLOCKED_"+account["environment"]+"_ORDER_GATE",
                        "reason": gate["reason"]}
                data["execution_permission"] = data.get("execution", {}).get("execution_permission", "BLOCKED_ACCOUNT")
                data["auto_trading"] = data.get("execution", {}).get("auto_trading", {"state": "UNKNOWN", "enabled": False})
                states.append({**data, **account_identity(account)})
            else:
                identity = account_identity(account)
                gate = identity["order_submission_gate"]
                states.append({**identity, "status": "AWAITING_FIRST_BROKER_SYNC",
                    "execution_permission": "BLOCKED_ACCOUNT" if gate["enabled"] else
                        "BLOCKED_"+account["environment"]+"_ORDER_GATE",
                    "reason": "AWAITING_FIRST_BROKER_SYNC" if gate["enabled"] else gate["reason"]})
    return {"configuration": settings.configuration_matrix(), "strategy": FAMILY, "registry_hash": REGISTRY_HASH,
            "status": "EXECUTION_ACCOUNTS_PRESENT" if accounts else "BROKER_ACCOUNT_REQUIRED",
            "accounts": states, "broker_execution_scope": "ACCOUNT_SCOPED",
            "demo_order_submission_enabled": settings.DEMO_ORDER_SUBMISSION_ENABLED,
            "live_order_submission_enabled": settings.LIVE_ORDER_SUBMISSION_ENABLED,
            "cati_mode": "LIVE", "trading": "ACTIVE",
            "execution_permission": "ACCOUNT_SCOPED",
            "daily_loss_limit_source": "PER_BOT_EFFECTIVE_POLICY"}


def health_summary(db):
    """Public aggregate truth; no credentials or private account identifiers."""
    accounts = status(db)["accounts"]
    summary = {"broker_execution_scope": "ACCOUNT_SCOPED", "strategy": FAMILY,
        "demo_system_gate": order_submission_gate("demo")["enabled"],
        "live_system_gate": order_submission_gate("live")["enabled"],
        "discovered_accounts": len(accounts),
        "demo_accounts": sum(a["environment"] == "DEMO" for a in accounts),
        "live_accounts": sum(a["environment"] == "LIVE" for a in accounts),
        "synced_accounts": sum(a.get("status") == "SYNCED" for a in accounts),
        "blocked_accounts": sum(str(a.get("execution_permission", "")).startswith("BLOCKED") for a in accounts),
        "risk_engine_health": "HEALTHY" if accounts and all(a.get("status") == "SYNCED" and "equity" in a.get("risk", {}) for a in accounts) else "AWAITING_ACCOUNT_RISK",
        "reconciliation_health": "SYNCED" if accounts and all(a.get("status") == "SYNCED" and a.get("reconciliation_status") == "SYNCED" for a in accounts) else "NOT_SYNCED"}
    with db.connect() as c:
        exists = c.execute("SELECT 1 FROM sqlite_master WHERE name='cati_residual_tracker'").fetchone()
        tracker = c.execute("SELECT heartbeat_at,status FROM cati_residual_tracker WHERE registry_hash=?", (REGISTRY_HASH,)).fetchone() if exists else None
    summary["market_data_status"] = tracker[1] if tracker and 0 <= int(time.time()*1000)-tracker[0] <= 120000 else "STALE"
    summary["status"] = ("ok" if summary["risk_engine_health"] == "HEALTHY"
        and summary["reconciliation_health"] == "SYNCED" and summary["market_data_status"] != "STALE" else "degraded")
    return summary


async def run(db):
    if not settings.production:
        raise ValueError("CATI_PRODUCTION_PROFILE_REQUIRED")
    runner = SimpleNamespace(db=db)
    fx_check = 0.
    while True:
        cycle_started = time.monotonic()
        try:
            if owner_current(db):
                residual_schedule(runner)
                forward_schedule(runner)
                from app.execution.demo_transport_smoke import process_local_request
                await asyncio.to_thread(process_local_request, db)
                from app.execution.demo_boundary_certification import process_local_request as certify_boundary
                await asyncio.to_thread(certify_boundary, db)
                if time.monotonic() - fx_check > 60:
                    from .residual_simulation import ensure_fx_watcher
                    await asyncio.to_thread(ensure_fx_watcher)
                    fx_check = time.monotonic()
                await asyncio.to_thread(sync, db)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("[CATI_PRODUCTION] collection cycle failed; broker mutations remain guarded")
        await asyncio.sleep(max(1.,30.-(time.monotonic()-cycle_started)))
