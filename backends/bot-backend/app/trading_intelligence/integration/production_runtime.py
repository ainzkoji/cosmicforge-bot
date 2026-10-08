"""Canonical CATI production collection, execution and broker reconciliation."""
from __future__ import annotations

import asyncio
import json
import logging
import threading
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

#: How old the collector's heartbeat may be before market data is called stale.
#: It beats every minute, except at the top of each hour, when one pass walks
#: the whole universe (two to three minutes) and beats only at the end. A
#: 120-second limit therefore reported STALE -- and the whole runtime as
#: degraded -- for a minute or two of every healthy hour.
COLLECTOR_HEARTBEAT_FRESH_MS = 360_000

#: Set while no broker cycle is running. A stop waits on it so an entry that
#: has reached the broker gets its protection before the process exits.
_idle = threading.Event()
_idle.set()
#: Serialises everything that can mutate at a broker: the ~30 s broker cycle
#: and an operator emergency flatten take the SAME lock, so a flatten can never
#: interleave with a cycle on the same account (or share a client with it).
_cycle_lock = threading.RLock()

#: The broker client of each account, reused across cycles. Building one costs
#: an exchangeInfo download and a time sync, which used to be paid per account
#: per cycle. Reuse is only ever for the factory's own client of the SAME
#: credentials; any doubt rebuilds.
_DEFAULT_CLIENT_FACTORY = build_client_from_auth
CLIENT_CACHE_MAX_AGE_SECONDS = 600.
_clients = {}


def _client_key(db, auth):
    """Identity of the credentials a cached client was built from, or None when
    a change could not be detected (then nothing is cached)."""
    version, fingerprint = getattr(auth, "credential_version", None), getattr(auth, "key_fingerprint", None)
    if version is None and not fingerprint:
        return None
    return (getattr(db, "path", None), getattr(auth, "account_id", None), getattr(auth, "user_id", None),
            str(getattr(auth, "broker_type", "")), str(getattr(auth, "environment", "")),
            getattr(auth, "base_url", None), version, fingerprint)


#: Exceptions raised by the production path about THIS process or its data --
#: never about the broker connection. A cycle that ends with one of these
#: learned nothing bad about the client, so rebuilding it (an exchangeInfo
#: download and a time sync, every cycle for as long as the gate holds) would
#: only add request weight. Everything else -- transport errors, venue errors,
#: malformed answers, unknown exceptions -- still rebuilds on any doubt.
LOCAL_GATE_CODES = frozenset({
    "CATI_PRODUCTION_PROFILE_REQUIRED", "CANONICAL_RUNTIME_LEASE_REQUIRED", "RUNTIME_SHUTDOWN_IN_PROGRESS",
    "PRODUCTION_REQUIRES_CATI_BOT", "PRODUCTION_REJECTS_PAPER_BOT", "MAINTENANCE_BOUNDARY_UNAVAILABLE",
    "PERSISTED_RISK_STATE_REQUIRED", "PERSISTED_WEEKLY_RISK_BASIS_REQUIRED", "PERSISTED_MONTHLY_RISK_BASIS_REQUIRED",
    "ACCOUNT_DAILY_LOSS_POLICY_UNAVAILABLE", "CLOSE_ACCOUNT_OWNERSHIP_UNCONFIRMED",
    "BROKER_EXECUTION_CAPABILITY_INCOMPLETE", "BROKER_ACCOUNT_NOT_FOUND", "BROKER_ACCOUNT_ACCESS_DENIED",
    "BROKER_ENVIRONMENT_MISMATCH", "BROKER_ACCOUNT_OWNERSHIP_MISMATCH", "EMERGENCY_FLATTEN_UNSUPPORTED_BROKER",
    "PROTECTION_STATE_UNKNOWN",  # the read failure underneath was already classified; the state is durable
})


def client_doubt(exc) -> bool:
    """True when ``exc`` is a reason to rebuild the account's broker client."""
    code = getattr(exc, "reason_code", None)
    if not code and isinstance(exc, ValueError):
        text = str(exc)
        if text and text.replace("_", "").isalnum() and text.upper() == text:
            code = text
    return not (isinstance(exc, ValueError) and code in LOCAL_GATE_CODES)


def invalidate_client(account_id):
    """Drop an account's cached client (an error, or credentials changed).

    Rate-limit backoff is NOT lost with it: the Binance client keeps that state
    per venue endpoint for the whole process (limits are per IP), so the
    rebuilt client -- and every other account's -- still honours a Retry-After
    and does not touch a banned IP again."""
    _clients.pop(account_id, None)


def account_client(db, account_id, auth, factory=build_client_from_auth):
    """This account's broker client. Only the production factory's clients are
    cached; an injected factory (tests, tools) is always called."""
    key = _client_key(db, auth) if factory is _DEFAULT_CLIENT_FACTORY else None
    if key is None:
        return factory(auth)
    cached = _clients.get(account_id)
    if cached and cached[0] == key and time.monotonic() - cached[2] < CLIENT_CACHE_MAX_AGE_SECONDS:
        client = cached[1]
        # Lineage identity is per call: never inherit the previous cycle's.
        client._production_intent_identity = None
        return client
    _clients.pop(account_id, None)
    client = factory(auth)
    _clients[account_id] = (key, client, time.monotonic())
    return client
#: Liveness evidence for the runtime supervisor (monotonic seconds).
_progress = {"loop_started": None, "cycle_started": None, "cycle_completed": None,
             "cycles": 0, "cycles_without_lease": 0, "last_error": None}


def wait_idle(timeout):
    """True once no broker cycle is in flight (immediately when none is)."""
    return _idle.wait(timeout)


def _age(stamp, now):
    return None if stamp is None else round(now - stamp, 1)


def progress():
    """When the loop last finished a cycle, for the supervisor and /health."""
    now = time.monotonic()
    return {"loop_running_seconds": _age(_progress["loop_started"], now),
            "last_cycle_started_age_seconds": _age(_progress["cycle_started"], now),
            "last_cycle_completed_age_seconds": _age(_progress["cycle_completed"], now),
            "cycles_completed": _progress["cycles"],
            "cycles_without_lease": _progress["cycles_without_lease"],
            "cycle_in_flight": not _idle.is_set(), "last_error": _progress["last_error"]}


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
    """Every production table, created once per database (``production_schema``)."""
    from app.execution.production_schema import ensure
    ensure(db)


def _account_document(client):
    """The venue's full account document when the client offers one (Binance
    ``/fapi/v2/account``; Bybit / BingX ``account()`` return the same keys), so
    the balance view and the account-risk evaluation share ONE read. None when
    the client has no such read or did not answer with the document."""
    read = getattr(client, "account", None)
    if not callable(read):
        return None
    raw = read()
    return raw if isinstance(raw, dict) and "totalMarginBalance" in raw and "totalWalletBalance" in raw else None


def _balance_view(raw):
    from decimal import Decimal
    return {"wallet": Decimal(str(raw.get("totalWalletBalance", 0))), "equity": Decimal(str(raw.get("totalMarginBalance", 0))),
            "available": Decimal(str(raw.get("availableBalance", 0)))}


def sync_account(db, account, *, factory=build_client_from_auth, execute=False):
    """Read account-scoped broker truth; optionally dispatch the canonical boundary."""
    auth = resolve_broker_auth(account["id"], account["user_id"], db)
    env = normalize_environment(auth.environment)
    if account.get("environment") and normalize_environment(account["environment"]) != env:
        raise ValueError("BROKER_ENVIRONMENT_MISMATCH")
    account = {**account, "environment": env.value.upper(),
               "broker_id": account.get("broker_id", getattr(auth, "broker_type", "binance"))}
    gate = order_submission_gate(env)
    client = account_client(db, account["id"], auth, factory)
    from contextlib import nullcontext
    scope = getattr(db, "scope", None)
    try:
        # One database connection for the whole account cycle (every atomic
        # unit inside keeps its own commit / rollback, see DB.scope).
        with (scope() if callable(scope) else nullcontext()):
            return _sync_with_client(db, account, auth, env, gate, client, factory, execute)
    except Exception as exc:
        # Safe invalidation: whatever went wrong at the broker, the next cycle
        # starts from a freshly built client (new time sync, new session,
        # current metadata). A local gate says nothing about the client.
        if client_doubt(exc):
            invalidate_client(account["id"])
        from app.execution import production_schema
        production_schema.missing_table(db, exc)     # a replaced database is initialised again on the next cycle
        raise


def _sync_with_client(db, account, auth, env, gate, client, factory, execute):
    client._production_credential_version = getattr(auth, "credential_version", None)
    account_document = _account_document(client)
    balance = _balance_view(account_document) if account_document is not None else client.get_balance()
    positions = client.position_risk()
    if not isinstance(balance, dict) or not isinstance(positions, list):
        raise ValueError("BROKER_READ_SHAPE_INVALID")
    # The account-wide open-order book (the costliest read of the cycle) is
    # needed to evaluate an entry or maintain a position. An account with no
    # active bot, no position and no unresolved lineage has neither; it still
    # gets its position read every cycle, so a manual position or a newly
    # deployed bot is seen on the next cycle.
    idle = False
    if execute:
        from .production_execution import idle_account
        idle = idle_account(db, account, positions)
    orders = [] if idle else client.open_orders()
    if not isinstance(orders, list):
        raise ValueError("BROKER_READ_SHAPE_INVALID")
    try:
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
    except Exception:
        # A local projection or instrument discovery failed. The account still
        # fails closed for this cycle (the error is re-raised, no entry is
        # evaluated) -- but an existing position's protection / fail-safe close
        # does not depend on either, so that maintenance still gets its turn.
        if execute:
            try:
                from .production_execution import maintain_only
                maintain_only(db, account, client)
            except Exception as exc:
                logger.warning("[CATI_PRODUCTION] position maintenance failed: %s", type(exc).__name__)
        raise
    now = int(time.time() * 1000)
    result = {**account_identity(account), "status": "SYNCED", "balance": balance, "positions": positions,
              "orders": orders, "orders_read": "SKIPPED_IDLE_ACCOUNT" if idle else "ACCOUNT_WIDE",
              "account": account_document, "discovery": discovery, "reconciliation": reconciliation,
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
            # Same safe invalidation as a failed read: rebuild the client next cycle.
            if client_doubt(exc):
                invalidate_client(account["id"])
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
    if execute:
        # Step 1.6: the customer's equity history. Never on the order path; a
        # recorder failure is logged inside and cannot touch the cycle.
        from app.observability import account_recorder
        account_recorder.record_cycle(db, account, result, now, bot_instance_id=bots[0][0] if len(bots) == 1 else None,
                                      fills=account_recorder.fills_in_document(result))
    return result


_HEALTH_COLUMNS = {}


def _bot_health_columns_present(db, c):
    """Whether bot_instances carries the health columns; answered once per database."""
    key = getattr(db, "path", None) or id(db)
    present = _HEALTH_COLUMNS.get(key)
    if present is None:
        columns = {r[1] for r in c.execute("PRAGMA table_info(bot_instances)")}
        present = {"bot_health_status", "bot_health_reason_code", "bot_health_message"} <= columns
        if present:  # a missing column may still be added by a migration; only presence is final
            _HEALTH_COLUMNS[key] = True
    return present


def save(db, account_id, user_id, now, result):
    with db.connect() as c:
        c.execute("INSERT OR REPLACE INTO cati_production_state VALUES(?,?,?,?)",
                  (account_id, user_id, now, json.dumps(result, default=str)))
        if _bot_health_columns_present(db, c):
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
        from app.ops.runtime_shutdown import stop_requested
        if stop_requested():
            # A stop lets the account in flight finish; it does not start another.
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
    # Positions whose exchange-side protection could not be verified (Step 1.0a):
    # preserved, retried every cycle, and shown rather than hidden.
    from app.execution.protection_state import uncertain_positions
    uncertain = 0
    for state in states:
        state["protection_uncertain"] = uncertain_positions(db, state["account_id"])
        uncertain += len(state["protection_uncertain"])
    return {"configuration": settings.configuration_matrix(), "strategy": FAMILY, "registry_hash": REGISTRY_HASH,
            "status": "EXECUTION_ACCOUNTS_PRESENT" if accounts else "BROKER_ACCOUNT_REQUIRED",
            "accounts": states, "broker_execution_scope": "ACCOUNT_SCOPED",
            "protection_uncertain_positions": uncertain,
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
    summary["market_data_status"] = (tracker[1] if tracker and
        0 <= int(time.time()*1000)-tracker[0] <= COLLECTOR_HEARTBEAT_FRESH_MS else "STALE")
    summary["status"] = ("ok" if summary["risk_engine_health"] == "HEALTHY"
        and summary["reconciliation_health"] == "SYNCED" and summary["market_data_status"] != "STALE" else "degraded")
    return summary


def _isolated(name, step, *args):
    """Run one auxiliary step. Its failure is logged and never costs the cycle.

    Collection, the labelled certification requests and the FX watcher used to
    share one try block with the broker sync, so an exception in any of them
    skipped trading and reconciliation for that cycle -- and one that failed
    every cycle would have stopped trading while the loop kept running.
    """
    try:
        step(*args)
    except Exception as exc:
        _progress["last_error"] = f"{name}:{type(exc).__name__}"
        logger.exception("[CATI_PRODUCTION] %s failed; the broker cycle continues", name)


def broker_cycle(db):
    """Everything in a cycle that can reach the broker, as one unit of work.

    Runs on a worker thread and marks itself in flight, so a stop can wait for
    an entry's protection instead of exiting between the fill and the stop.
    """
    _idle.clear()
    try:
        with _cycle_lock:  # shared with flatten(): one broker-mutating actor at a time
            from app.ops.runtime_shutdown import stop_requested
            if not stop_requested():
                from app.execution.demo_transport_smoke import process_local_request
                _isolated("demo_transport_request", process_local_request, db)
                from app.execution.demo_boundary_certification import process_local_request as certify_boundary
                _isolated("demo_certification_request", certify_boundary, db)
            sync(db)
    finally:
        _idle.set()


class EngineUnavailable(RuntimeError):
    """The production engine in THIS process cannot act on a broker right now."""


#: How long an emergency flatten waits for the cycle in flight to finish.
FLATTEN_LOCK_WAIT_SECONDS = 60.


def flatten(db, *, account_id=None, request_id, factory=build_client_from_auth, lock_wait=FLATTEN_LOCK_WAIT_SECONDS):
    """Operator emergency flatten of one account or of every execution account.

    Runs under the broker-cycle lock, through the same durable reduce-only
    close as the fail-safe (``production_execution.flatten_account``). It
    submits no entry and bypasses no gate. Raises ``EngineUnavailable`` when
    this process cannot act at all -- it never reports success for nothing.
    Returns one result per account position (or one failed row per account)."""
    if not settings.production:
        raise EngineUnavailable("CATI_PRODUCTION_PROFILE_REQUIRED")
    if not owner_current(db):
        # Closes are only ever sent by the process that holds the runtime lease.
        raise EngineUnavailable("CANONICAL_RUNTIME_LEASE_REQUIRED")
    accounts = execution_accounts(db)
    if account_id is not None:
        accounts = [a for a in accounts if a["id"] == account_id]
        if not accounts:
            raise EngineUnavailable("EXECUTION_ACCOUNT_NOT_FOUND")
    if not accounts:
        raise EngineUnavailable("BROKER_ACCOUNT_REQUIRED")
    if not any(order_submission_gate(a["environment"])["enabled"] for a in accounts):
        raise EngineUnavailable("ORDER_SUBMISSION_DISABLED")
    if not _cycle_lock.acquire(timeout=lock_wait):
        raise EngineUnavailable("BROKER_CYCLE_BUSY")
    try:
        from .production_execution import ACCOUNT_LEVEL, flatten_account
        results = []
        for account in accounts:
            try:
                if account["broker_id"].lower() not in {"binance", "bybit", "bingx"}:
                    # No account-scoped read exists here for these venues.
                    raise ValueError("EMERGENCY_FLATTEN_UNSUPPORTED_BROKER")
                auth = resolve_broker_auth(account["id"], account["user_id"], db)
                env = normalize_environment(auth.environment)
                if normalize_environment(account["environment"]) != env:
                    raise ValueError("BROKER_ENVIRONMENT_MISMATCH")
                scoped = {**account, "environment": env.value.upper()}
                client = account_client(db, account["id"], auth, factory)
                client._production_credential_version = getattr(auth, "credential_version", None)
                results.extend(flatten_account(db, scoped, client, request_id=request_id))
            except Exception as exc:
                if client_doubt(exc):
                    invalidate_client(account["id"])
                code = getattr(exc, "reason_code", None)
                if not code and isinstance(exc, ValueError):
                    candidate = str(exc)
                    code = candidate if candidate.replace("_", "").isalnum() and candidate.upper() == candidate else None
                # Never publish credentials or signed exception URLs.
                results.append({"account_id": account["id"], "symbol": ACCOUNT_LEVEL, "status": "failed",
                                "detail": code or type(exc).__name__})
        return results
    finally:
        _cycle_lock.release()


def operations_summary(db):
    """What the runtime is doing about trading right now, in aggregate.

    Public: counts and states only -- never credentials or account identifiers.
    """
    accounts = status(db)["accounts"]
    executions = [a.get("execution") or {} for a in accounts]
    ages = [a["age_seconds"] for a in accounts if a.get("age_seconds") is not None]
    summary = {
        "auto_trading_enabled_accounts": sum(bool((a.get("auto_trading") or {}).get("enabled")) for a in accounts),
        "kill_switch_engaged_accounts": sum(bool(e.get("kill_switch")) for e in executions),
        "execution_permissions": sorted({str(a.get("execution_permission")) for a in accounts}),
        "execution_reasons": sorted({str(e["reason"]) for e in executions if e.get("reason")}),
        "execution_portfolio_states": sorted({str((e.get("execution_portfolio") or {}).get("state", "UNKNOWN"))
                                              for e in executions}),
        "accounts_with_open_execution": sum(bool(e.get("execution_portfolio_active")) for e in executions),
        "broker_sync_max_age_seconds": round(max(ages), 1) if ages else None,
        "latest_decision": None,
    }
    with db.connect() as c:
        if c.execute("SELECT 1 FROM sqlite_master WHERE name='cati_residual_decisions'").fetchone():
            row = c.execute("SELECT decision_time,recorded_at,selected_symbol,side,entry_reference,risk,"
                            "risk_state_json FROM cati_residual_decisions WHERE registry_hash=? "
                            "ORDER BY decision_time DESC LIMIT 1", (REGISTRY_HASH,)).fetchone()
            if row:
                summary["latest_decision"] = {
                    "decision_time_ms": row[0], "recorded_at_ms": row[1],
                    "age_seconds": round((time.time()*1000 - row[0]) / 1000, 1),
                    "reason": json.loads(row[6]).get("reason"), "symbol": row[2], "side": row[3],
                    "structural_stop_fraction": (row[5] / row[4]) if row[4] and row[5] else None}
    return summary


async def run(db):
    if not settings.production:
        raise ValueError("CATI_PRODUCTION_PROFILE_REQUIRED")
    runner = SimpleNamespace(db=db)
    fx_check = 0.
    _progress["loop_started"] = time.monotonic()
    while True:
        cycle_started = _progress["cycle_started"] = time.monotonic()
        _progress["last_error"] = None  # what is reported is the latest cycle's, not history
        try:
            if owner_current(db):
                _isolated("residual_collection_schedule", residual_schedule, runner)
                _isolated("forward_observation_schedule", forward_schedule, runner)
                if time.monotonic() - fx_check > 60:
                    from .residual_simulation import ensure_fx_watcher
                    fx_check = time.monotonic()
                    await asyncio.to_thread(_isolated, "fx_watcher", ensure_fx_watcher)
                await asyncio.to_thread(broker_cycle, db)
                _progress["cycles"] += 1
            else:
                _progress["cycles_without_lease"] += 1
            _progress["cycle_completed"] = time.monotonic()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _progress["last_error"] = f"cycle:{type(exc).__name__}"
            logger.exception("[CATI_PRODUCTION] collection cycle failed; broker mutations remain guarded")
        await asyncio.sleep(max(1.,30.-(time.monotonic()-cycle_started)))
