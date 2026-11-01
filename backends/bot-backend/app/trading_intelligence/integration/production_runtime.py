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
from app.trading_intelligence.pipeline.coordination import PreemptionGate
from app.platform_workers.assignments import AssignmentLost
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
#: An emergency flatten that is waiting for (or holds) the cycle lock. The
#: cycle asks it at its safe points -- between two accounts, and before an
#: account's entry-capable step -- and hands the lock over there instead of
#: finishing its round first (Step 3.13). In-process only: the one process
#: that may flatten is the lease owner, which is the one that runs the cycle.
_preemption = PreemptionGate()
#: How long a cycle that yielded waits for the flatten before it queues on the
#: lock again (it then simply blocks behind the flatten; nothing is skipped).
PREEMPTION_RESUME_WAIT_SECONDS = 300.

#: The broker client of each account, reused across cycles. Building one costs
#: an exchangeInfo download and a time sync, which used to be paid per account
#: per cycle. Reuse is only ever for the factory's own client of the SAME
#: credentials; any doubt rebuilds.
_DEFAULT_CLIENT_FACTORY = build_client_from_auth
_HTTP_CLIENT_FACTORY = build_client_from_auth
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
    "EMERGENCY_ASSIGNMENT_PREEMPTION_FAILED",  # the platform database, not the broker connection
})


def client_doubt(exc) -> bool:
    """True when ``exc`` is a reason to rebuild the account's broker client."""
    if isinstance(exc, AssignmentLost):
        return False  # about this worker's ownership of the account, never about the connection
    from .broker_read_diagnosis import is_local_budget_refusal
    if is_local_budget_refusal(exc):
        # This server's own budget refused before anything was sent. Rebuilding
        # would only spend more weight (exchangeInfo + time sync) on a full budget.
        return False
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
    from app.exchange.platform_adapter.http_budget import build_budgeted_client
    construct = (lambda resolved: build_budgeted_client(db, resolved)) if factory is _HTTP_CLIENT_FACTORY else factory
    key = _client_key(db, auth) if factory is _DEFAULT_CLIENT_FACTORY else None
    if key is None:
        return construct(auth)
    cached = _clients.get(account_id)
    if cached and cached[0] == key and time.monotonic() - cached[2] < CLIENT_CACHE_MAX_AGE_SECONDS:
        client = cached[1]
        # Lineage identity is per call: never inherit the previous cycle's.
        client._production_intent_identity = None
        return client
    _clients.pop(account_id, None)
    client = construct(auth)
    _clients[account_id] = (key, client, time.monotonic())
    return client
#: Liveness evidence for the runtime supervisor (monotonic seconds).
_progress = {"loop_started": None, "cycle_started": None, "cycle_completed": None,
             "cycles": 0, "cycles_without_lease": 0, "last_error": None}


def wait_idle(timeout):
    """True once no broker cycle is in flight (immediately when none is).

    Called by the graceful stop (``runtime_shutdown.quiesce``) after it forbade new work: once the account in
    flight has finished -- or the wait gave up on it -- this worker's account assignments are released (DRAIN)
    so a successor takes them over at once. A step still running finds its fence released at its next check."""
    idle = _idle.wait(timeout)
    from app.ops.runtime_shutdown import stop_requested
    if stop_requested():
        _drain_assignments()
    return idle


#: Sentinel: "the process's assignment layer" (``broker_assignment.installed()``).
_PROCESS_LAYER = object()


def _layer(layer=_PROCESS_LAYER):
    """The broker cycle's account-assignment layer (``app.platform_workers.broker_assignment``), or None: no
    platform database in ``owner`` mode -- the compatibility path, exactly as before the layer existed."""
    if layer is not _PROCESS_LAYER:
        return layer
    from app.platform_workers import broker_assignment
    return broker_assignment.installed()


def _drain_assignments():
    layer = _layer()
    if layer is not None:
        _isolated("assignment_drain", layer.drain)


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
            "cycle_in_flight": not _idle.is_set(), "last_error": _progress["last_error"],
            "account_assignment": _assignment_view()}


def _assignment_view():
    """The broker cycle's account-assignment layer as the supervisor reports it (None: the compatibility path)."""
    try:
        layer = _layer()
        return None if layer is None else layer.describe()
    except Exception as exc:  # noqa: BLE001 -- a report, never a failure
        return {"error": type(exc).__name__}


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


def sync_account(db, account, *, factory=build_client_from_auth, execute=False, recovery_only=False):
    """Read account-scoped broker truth; optionally dispatch the canonical boundary.

    ``recovery_only`` (Step 5.2): the first step of an account this worker was just assigned (NEW / TAKEOVER).
    The same reads and reconciliation run, then ``production_execution.recover_account`` instead of the
    entry-capable ``process_account``: nothing that increases risk is sent."""
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
            return _sync_with_client(db, account, auth, env, gate, client, factory, execute, recovery_only)
    except Exception as exc:
        # Safe invalidation: whatever went wrong at the broker, the next cycle
        # starts from a freshly built client (new time sync, new session,
        # current metadata). A local gate says nothing about the client.
        if client_doubt(exc):
            invalidate_client(account["id"])
        from app.execution import production_schema
        production_schema.missing_table(db, exc)     # a replaced database is initialised again on the next cycle
        raise


def _sync_with_client(db, account, auth, env, gate, client, factory, execute, recovery_only=False):
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
    from app.platform_workers.account_fence import require as fence_check
    fence_check(account["id"])  # Step 5.2: projections are written only by the account's current owner
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
        discovery_factory = (lambda resolved: client) if factory is _DEFAULT_CLIENT_FACTORY else factory
        discovery = refresh_if_stale(db, auth, client_factory=discovery_factory)
    except Exception:
        # A local projection or instrument discovery failed. The account still
        # fails closed for this cycle (the error is re-raised, no entry is
        # evaluated) -- but an existing position's protection / fail-safe close
        # does not depend on either, so that maintenance still gets its turn.
        if execute:
            try:
                from .production_execution import maintain_only
                maintain_only(db, account, client)
            except AssignmentLost:
                raise  # this worker no longer owns the account: nothing more is done or written for it
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
    if execute and _preemption.covers(account["id"]):
        # Safe point inside an account: an emergency flatten is waiting for THIS
        # account. Its entry-capable step is not started -- the kill switch the
        # flatten set first already refuses the entry at the boundary; this hands
        # the account over without first spending its venue reads. The flatten
        # reads the actual positions and runs the same maintenance itself, and
        # the next ordinary cycle processes the account as usual.
        result["execution"] = {"execution_permission": "BLOCKED_ACCOUNT", "reason": "CATI_NEW_ENTRY_KILL_SWITCH",
                               "emergency_preempted": True}
    elif execute:
        from .production_execution import process_account, recover_account
        try:
            if recovery_only:
                # Takeover / new assignment: reconstruct exchange truth and durable work first; no entry.
                result["execution"] = recover_account(db, account, client)
            else:
                result["execution"] = process_account(db, account, client, result)
            result["risk"] = result["execution"].get("risk", result["risk"])
        except AssignmentLost:
            raise  # nothing is written for an account this worker no longer owns
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
    from app.platform_workers.account_fence import require as fence_check
    fence_check(account["id"])  # Step 5.2: a worker that lost the account does not publish its state
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


def sync(db, *, layer=_PROCESS_LAYER):
    """One round over every execution account. Returns None when the round is
    over, or the accounts it had not started when it yielded to an emergency
    flatten (``broker_cycle`` resumes those once the flatten has run).

    With an account-assignment layer (a platform database is configured, Step
    5.2) the round covers only the accounts this worker holds after the
    registry sync and its claim: in ``owner`` mode the runtime owner claims
    every account no other live worker holds; in ``assigned`` mode its fair
    share. A layer that refuses (``assigned`` without a platform database) does
    no account work at all."""
    layer = _layer(layer)
    if layer is None:
        if not owner_current(db):
            return
        initialize(db)
        return _sync_accounts(db, execution_accounts(db), layer=None)
    if layer.refused:
        _progress["assignment_refused"] = layer.refused
        return
    if not _round_authority(db, layer):
        return
    initialize(db)
    return _sync_accounts(db, layer.begin_round(execution_accounts(db)), layer=layer)


def _round_authority(db, layer):
    """May this process go on with account work in this round? The canonical
    runtime lease without a layer and in ``owner`` mode; an undrained layer in
    ``assigned`` mode (there each account step is authorized by its own fence)."""
    if layer is None:
        return owner_current(db)
    if layer.refused or layer.draining:
        return False
    from app.platform_workers.broker_assignment import ASSIGNED
    return True if layer.mode == ASSIGNED else owner_current(db)


def _sync_accounts(db, accounts, *, layer=_PROCESS_LAYER):
    layer = _layer(layer)
    for index, account in enumerate(accounts):
        if not _round_authority(db, layer):
            return
        from app.ops.runtime_shutdown import stop_requested
        if stop_requested():
            # A stop lets the account in flight finish; it does not start another.
            return
        if _preemption.pending():
            # Safe point between accounts: the account in flight has finished
            # (entry, protection and all); the flatten gets the lock now.
            return accounts[index:]
        if account["broker_id"].lower() not in {"binance", "bybit", "bingx"}:
            save(db, account["id"], account["user_id"], int(time.time()*1000),
                 {**account_identity(account), "status": "CAPABILITY_UNAVAILABLE",
                  "execution_permission": "BLOCKED_ACCOUNT", "execution": {
                      "execution_permission": "BLOCKED_ACCOUNT", "reason": "DEMO_ADAPTER_UNVALIDATED"
                      if account["environment"] == "DEMO" else "EXECUTION_ADAPTER_UNVALIDATED"},
                  "balance": None, "positions": None, "orders": None})
            continue
        try:
            if layer is None:
                sync_account(db, account, execute=True)
            else:
                # Step 5.2: the account step runs under its durable assignment's
                # fence; a NEW / TAKEOVER assignment's first step is recovery only.
                layer.keepalive()
                layer.run_account(account, lambda recovery_only, _account=account: sync_account(
                    db, _account, execute=True, recovery_only=recovery_only))
        except AssignmentLost as lost:
            # Another worker owns the account now (takeover, emergency
            # preemption, removal, drain): it is not touched and nothing is
            # written for it -- its owner reports its state.
            _progress["assignments_lost"] = _progress.get("assignments_lost", 0) + 1
            logger.warning("[CATI_PRODUCTION] account assignment lost (%s); account skipped", lost.reason)
        except Exception as exc:
            # Never publish credentials or signed exception URLs: the diagnosis
            # keeps only the exception class and sanitized tokens / status codes.
            from .broker_read_diagnosis import classify
            diagnosis = classify(exc)
            save(db, account["id"], account["user_id"], int(time.time()*1000),
                 {**account_identity(account), "status": "READ_FAILED", "reason": diagnosis["reason"],
                  "read_diagnostic": diagnosis["diagnostic"],
                  "execution_permission": "BLOCKED_ACCOUNT", "execution": {"execution_permission": "BLOCKED_ACCOUNT",
                      "reason": diagnosis["reason"]},
                  "balance": None, "positions": None, "orders": None})
            logger.warning("[CATI_PRODUCTION] broker read failed account=%s reason=%s diagnostic=%s",
                           account["id"], diagnosis["reason"], diagnosis["diagnostic"])


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
            if not stop_requested() and not _preemption.pending():   # no labelled order work ahead of a flatten
                from app.execution.demo_transport_smoke import process_local_request
                _isolated("demo_transport_request", process_local_request, db)
                from app.execution.demo_boundary_certification import process_local_request as certify_boundary
                _isolated("demo_certification_request", certify_boundary, db)
            remaining = sync(db)
        while isinstance(remaining, list) and remaining:
            # The round yielded to an emergency flatten between two accounts. The
            # lock is free here, so the flatten runs now; the accounts it did not
            # reach are then taken up again under the lock, each one checked
            # (lease, stop, another emergency) exactly as in an ordinary round.
            _progress["preemptions"] = _progress.get("preemptions", 0) + 1
            _preemption.wait_clear(PREEMPTION_RESUME_WAIT_SECONDS)
            with _cycle_lock:
                remaining = _sync_accounts(db, remaining)
        layer = _layer()
        if layer is not None:
            from app.ops.runtime_shutdown import stop_requested
            if stop_requested():
                # The account in flight has finished: hand every account over now (DRAIN).
                _isolated("assignment_drain", layer.drain)
            else:
                # A safe point between rounds: accounts above the fair share go back (assigned mode).
                _isolated("assignment_rebalance", layer.end_round)
    finally:
        _idle.set()


class EngineUnavailable(RuntimeError):
    """The production engine in THIS process cannot act on a broker right now."""


#: The longest an emergency flatten waits for the broker-cycle lock. The cycle
#: hands the lock over at its next safe point, so the ordinary wait is what is
#: left of ONE account, not of a round; this bound is the fallback for an
#: account whose venue calls hang (the flatten then answers BROKER_CYCLE_BUSY
#: and reports that nothing was closed -- it never claims otherwise).
FLATTEN_LOCK_WAIT_SECONDS = 60.


def _emergency_step(db, step, request_id, **kw):
    """Advance the durable emergency record (Step 3.13). Best effort by design:
    the record coordinates OTHER workers and keeps the audit trail; a flatten
    that could not write it still runs (the kill switch is already on)."""
    try:
        from app.trading_intelligence.pipeline.coordination import EmergencyControl
        return getattr(EmergencyControl(db), step)(request_id, **kw)
    except Exception as exc:
        logger.warning("[EMERGENCY_FLATTEN] request=%s coordination record step %s failed (%s)", request_id, step,
                       type(exc).__name__)
        return None


def _emergency_lease(db, account_id, request_id):
    """The account's durable lease, taken WITH PRIORITY for the flatten: an
    ordinary worker that holds it loses it and stops at its next check. None
    when it could not be taken (logged; the flatten goes on under the cycle lock)."""
    try:
        from app.trading_intelligence.pipeline.coordination import (
            EMERGENCY, EMERGENCY_LEASE_TTL_MS, AccountLeases)
        leases = AccountLeases(db)
        claim = leases.acquire(account_id, "emergency:" + str(request_id), ttl_ms=EMERGENCY_LEASE_TTL_MS, purpose=EMERGENCY)
        if claim:
            return leases, claim
        logger.warning("[EMERGENCY_FLATTEN] request=%s account=%s lease not taken (%s)", request_id, account_id, claim.reason)
    except Exception as exc:
        logger.warning("[EMERGENCY_FLATTEN] request=%s account=%s lease unavailable (%s)", request_id, account_id,
                       type(exc).__name__)
    return None


def flatten(db, *, account_id=None, request_id, factory=build_client_from_auth, lock_wait=FLATTEN_LOCK_WAIT_SECONDS):
    """Operator emergency flatten of one account or of every execution account.

    Runs under the broker-cycle lock, through the same durable reduce-only
    close as the fail-safe (``production_execution.flatten_account``). It
    submits no entry and bypasses no gate. Raises ``EngineUnavailable`` when
    this process cannot act at all -- it never reports success for nothing.
    Returns one result per account position (or one failed row per account).

    It PREEMPTS the broker cycle (Step 3.13): from the moment the request is
    registered the cycle starts no further account and no entry-capable step
    of an account in scope, and releases the lock at that safe point. Nothing
    is interrupted mid-account: an entry that has reached the broker still
    gets its protection, and the flatten then reads the ACTUAL positions."""
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
    ticket = _preemption.request(request_id, account_id)       # the cycle yields at its next safe point
    outcome = "FAILED"
    try:
        _emergency_step(db, "begin", request_id, account_id=account_id)
        if not _cycle_lock.acquire(timeout=lock_wait):
            outcome = "ENGINE_UNAVAILABLE:BROKER_CYCLE_BUSY"
            raise EngineUnavailable("BROKER_CYCLE_BUSY")
        try:
            from contextlib import nullcontext
            from app.platform_workers.account_fence import active as fenced
            from .production_execution import ACCOUNT_LEVEL, flatten_account
            layer = _layer()
            results = []
            for account in accounts:
                held = _emergency_lease(db, account["id"], request_id)
                preempted = None
                try:
                    if account["broker_id"].lower() not in {"binance", "bybit", "bingx"}:
                        # No account-scoped read exists here for these venues.
                        raise ValueError("EMERGENCY_FLATTEN_UNSUPPORTED_BROKER")
                    if layer is not None and layer.enforced:
                        # Step 5.2: the account is taken from whichever worker holds it
                        # (higher fencing token); that worker stops at its next check.
                        try:
                            preempted = layer.preempt(account, request_id)
                        except Exception as exc:
                            logger.error("[EMERGENCY_FLATTEN] request=%s account=%s assignment not preempted (%s)",
                                         request_id, account["id"], type(exc).__name__)
                            raise ValueError("EMERGENCY_ASSIGNMENT_PREEMPTION_FAILED") from None
                    auth = resolve_broker_auth(account["id"], account["user_id"], db)
                    env = normalize_environment(auth.environment)
                    if normalize_environment(account["environment"]) != env:
                        raise ValueError("BROKER_ENVIRONMENT_MISMATCH")
                    scoped = {**account, "environment": env.value.upper()}
                    client = account_client(db, account["id"], auth, factory)
                    client._production_credential_version = getattr(auth, "credential_version", None)
                    with (fenced(layer.emergency_fence(preempted)) if preempted is not None else nullcontext()):
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
                finally:
                    if held is not None:
                        try:
                            held[0].release(held[1])
                        except Exception as exc:
                            logger.warning("[EMERGENCY_FLATTEN] request=%s account=%s lease release failed (%s)",
                                           request_id, account["id"], type(exc).__name__)
                    if preempted is not None:
                        # The next owner claims the account as NEW and recovers it first; the
                        # kill switch the request raised keeps every entry refused (no reopen).
                        try:
                            layer.release_emergency(preempted)
                        except Exception as exc:
                            logger.warning("[EMERGENCY_FLATTEN] request=%s account=%s assignment release failed (%s); "
                                           "it expires on its own", request_id, account["id"], type(exc).__name__)
                    _emergency_step(db, "heartbeat", request_id)
            outcome = "COMPLETED" if results and all(
                r.get("status") in ("closed", "submitted", "no_position") for r in results) else "INCOMPLETE"
            return results
        finally:
            _cycle_lock.release()
    finally:
        _preemption.release(ticket)
        _emergency_step(db, "finish", request_id, outcome=outcome)


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
    from app.ops import process_role
    if not process_role.runs_trading():
        # An api-role process never runs the broker cycle and never claims an account.
        raise ValueError("PROCESS_ROLE_API_RUNS_NO_BROKER_CYCLE")
    import os
    from app.platform_workers import broker_assignment
    # Step 5.2: the broker cycle's account-assignment layer (None without a
    # platform database in owner mode: the compatibility path). The singletons
    # below stay with the canonical runtime-owner lease only.
    layer = broker_assignment.configure(build_version=os.environ.get("BUILD_VERSION", "unknown"))
    from .platform_runtime import ShadowScheduler
    from .venue_runtime import VenueRuntime
    from app.exchange.platform_adapter.http_budget import build_budgeted_client

    scheduler = ShadowScheduler(db, owns_runtime=owner_current)
    services = [scheduler]
    # A single conservative scope shares the budget when the deployment hasn't
    # supplied its egress IP identity. It never creates a per-customer allowance.
    services.append(VenueRuntime(db, owns_runtime=owner_current,
                                 server_address=os.environ.get("CATI_EGRESS_SCOPE", "shared-runtime-egress"),
                                 accounts=execution_accounts, resolve=resolve_broker_auth,
                                 build=lambda auth: build_budgeted_client(db, auth)))
    workers = [asyncio.create_task(service.run()) for service in services]
    try:
        await _broker_loop(db)
    finally:
        for service in services:
            service.request_stop()
        for worker in workers:
            worker.cancel()
        await asyncio.gather(*workers, return_exceptions=True)
        if layer is not None:
            # Whatever stopped the loop, the accounts are handed over now rather than after one TTL.
            _isolated("assignment_drain", layer.drain)


def _assigned_worker(layer):
    """This process runs the broker cycle without the runtime lease: an undrained ``assigned``-mode layer."""
    if layer is None or not layer.enforced or layer.draining:
        return False
    from app.platform_workers.broker_assignment import ASSIGNED
    return layer.mode == ASSIGNED


def cycle_lock():
    """The lock every broker-mutating actor of this process takes (the broker cycle, the emergency flatten, the
    durable job handlers' broker sections)."""
    return _cycle_lock


def preemption_gate():
    """This process's emergency-flatten preemption gate (``PreemptionGate``)."""
    return _preemption


def _run_jobs(db, layer):
    """Due durable jobs of this worker (``app.platform_workers.runtime_jobs``, when installed), after the broker
    cycle has returned and OUTSIDE the cycle lock: each handler takes it itself around its broker section
    (``RuntimeServices.broker_section``). Work is scheduled for the accounts this worker holds; a job acts on an
    account only through the fence of an assignment this worker holds and has recovered (``layer.job_fence``)."""
    from app.ops.runtime_shutdown import stop_requested
    if stop_requested() or _preemption.pending() or layer.draining:
        return None
    runner = layer.job_runner(db)
    if runner is None:
        return None
    runner.schedule(layer.owned_accounts())
    if stop_requested() or _preemption.pending():
        return None
    return runner.run_once(limit=10)


async def _broker_loop(db):
    runner = SimpleNamespace(db=db)
    fx_check = 0.
    _progress["loop_started"] = time.monotonic()
    while True:
        cycle_started = _progress["cycle_started"] = time.monotonic()
        _progress["last_error"] = None  # what is reported is the latest cycle's, not history
        try:
            owner = owner_current(db)
            if owner:
                # Singletons: only ever the canonical runtime owner, in every mode.
                _isolated("residual_collection_schedule", residual_schedule, runner)
                _isolated("forward_observation_schedule", forward_schedule, runner)
                if time.monotonic() - fx_check > 60:
                    from .residual_simulation import ensure_fx_watcher
                    fx_check = time.monotonic()
                    await asyncio.to_thread(_isolated, "fx_watcher", ensure_fx_watcher)
            layer = _layer()
            if owner or _assigned_worker(layer):
                # assigned mode: every trading-role process runs the cycle over
                # the accounts it holds, lease or not (its fences authorize it).
                await asyncio.to_thread(broker_cycle, db)
                _progress["cycles"] += 1
                if layer is not None and layer.enforced:
                    await asyncio.to_thread(_isolated, "runtime_jobs", _run_jobs, db, layer)
            else:
                _progress["cycles_without_lease"] += 1
            _progress["cycle_completed"] = time.monotonic()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _progress["last_error"] = f"cycle:{type(exc).__name__}"
            logger.exception("[CATI_PRODUCTION] collection cycle failed; broker mutations remain guarded")
        await asyncio.sleep(max(1.,30.-(time.monotonic()-cycle_started)))
