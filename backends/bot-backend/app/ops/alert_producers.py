"""Alert producers over durable state (Section H Step 8.5, F1).

Every producer here is an EVALUATOR: it reads what the trading platform already wrote (the legacy runtime database
through ``shared_lib.persistence.db.DB``, the platform ``Database``, backup markers) and turns it into alerts through
the shared engine. Nothing here is called from the order path, nothing here writes to a trading table, and no
trading code waits for an alert:

* a producer cannot slow or break an order -- it runs in the operations tick (``python -m app.ops.alerting``);
* it survives restarts -- the state it reads is durable, and the little it keeps between ticks (when a condition was
  first seen) is persisted by the tick lease;
* it **auto-resolves**: ``publish`` raises every firing signal and resolves every alert the same producer raised
  earlier whose signal is gone, with a RECOVERED notification;
* instantaneous events (a kill switch engaged, an emergency flatten) are keyed by their durable record and raised
  **once** per record (``Signal.once``), even when the event began and ended between two ticks.

Titles and details never carry a customer identifier, e-mail, IP or credential: accounts appear as
``pseudonym(account_id)``; the raw account id is stored only in the alert's ``account_ref`` (admin console). The
engine redacts again at storage and before any sink.

A table that does not exist yet is no evidence (no alert); any other read error is raised, so the tick reports the
producer as failing instead of resolving alerts on a database it could not read.
"""
from __future__ import annotations

import json
import logging
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from shared_lib.operations.alerting import CRITICAL, HIGH, MEDIUM, AlertEngine, fingerprint, pseudonym

logger = logging.getLogger(__name__)

MINUTE, HOUR, DAY = 60_000, 3_600_000, 86_400_000


# ── signals and publication ──────────────────────────────────────────────────────────────────────────────
@dataclass
class Signal:
    """One firing condition. ``key`` identifies it (hashed into the fingerprint, never stored in clear);
    ``hold_ms``: raise only once the condition was seen continuously this long; ``once``: one durable event."""
    category: str
    key: Any
    title: str
    detail: Dict[str, Any] = field(default_factory=dict)
    severity: Optional[str] = None
    account_ref: Optional[str] = None
    once: bool = False
    hold_ms: int = 0


@dataclass
class ProducerContext:
    engine: AlertEngine
    runtime_db: Any = None                 # legacy runtime database (``DB``): cati_*, auth, billing tables
    platform_db: Any = None                # platform ``Database``
    now_ms: Optional[int] = None           # injected clock (tests); otherwise this host's clock, which is the
                                           # clock that wrote the legacy rows (one SQLite file, one host)
    state: Dict[str, Any] = field(default_factory=dict)   # persisted between ticks by the tick lease

    def now(self) -> int:
        return int(self.now_ms) if self.now_ms is not None else int(time.time() * 1000)

    def platform_now(self) -> int:
        if self.now_ms is not None or self.platform_db is None:
            return self.now()
        with self.platform_db.transaction(write=False) as conn:
            return conn.now_ms()


def publish(ctx: ProducerContext, source: str, categories: Sequence[str], signals: Iterable[Signal]) -> Dict[str, Any]:
    """Raise every firing signal, resolve what this producer raised earlier and no longer sees."""
    engine, now = ctx.engine, ctx.now()
    pending_all = ctx.state.setdefault("pending", {})
    pending = pending_all.get(source, {})
    keep: set = set()
    next_pending: Dict[str, int] = {}
    open_fps: Optional[set] = None
    created = held = 0
    for s in signals:
        fp = fingerprint(s.category, s.key)
        if fp in keep:
            continue
        keep.add(fp)
        if s.hold_ms > 0:
            first = int(pending.get(fp, now))
            next_pending[fp] = first
            if now - first < s.hold_ms:
                if open_fps is None:
                    open_fps = set(engine.open_fingerprints(source=source))
                if fp not in open_fps:
                    held += 1
                    continue
        result = engine.raise_alert(s.category, key=s.key, title=s.title, source=source, detail=s.detail,
                                    severity=s.severity, account_ref=s.account_ref, now_ms=ctx.now_ms, once=s.once)
        created += int(result.created)
    if next_pending:
        pending_all[source] = next_pending
    else:
        pending_all.pop(source, None)
    resolved = engine.resolve_absent(categories, source=source, keep=keep, actor=f"evaluator:{source}",
                                     now_ms=ctx.now_ms)
    return {"source": source, "firing": len(keep) - held, "held": held, "created": created, "resolved": len(resolved)}


# ── reading durable state ────────────────────────────────────────────────────────────────────────────────
def rows(db: Any, sql: str, params: Sequence[Any] = ()) -> List[tuple]:
    """Rows from the legacy runtime database. A table (or a column added by a later schema step) that does not
    exist yet means the subsystem never ran here: no evidence. Any other error is raised."""
    if db is None:
        return []
    try:
        with db.connect() as conn:
            return [tuple(r) for r in conn.execute(sql, tuple(params)).fetchall()]
    except sqlite3.OperationalError as exc:
        text = str(exc).lower()
        if "no such table" in text or "no such column" in text:
            logger.debug("alert producer: %s", text)
            return []
        raise


def table_exists(db: Any, table: str) -> bool:
    return bool(rows(db, "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)))


def _json(value: Any) -> Dict[str, Any]:
    try:
        out = json.loads(value) if value else {}
    except (TypeError, ValueError):
        return {}
    return out if isinstance(out, dict) else {}


def iso_ms(value: Any) -> Optional[int]:
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    return int((dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)).timestamp() * 1000)


def ms_iso(ms: int) -> str:
    """The ISO-8601 UTC form the legacy writers use (``datetime.now(timezone.utc).isoformat()``), for range scans."""
    return datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc).isoformat()


def _short(value: Any, n: int = 160) -> str:
    """Free text from a durable record, redacted BEFORE it is shortened (a cut must not split a secret past the
    redaction patterns); the engine redacts again at storage and on the way to a sink."""
    from shared_lib.core.security.redaction import redact_text
    return redact_text(str(value or ""))[:n]


# ── 1. worker crash (legacy runtime owner, platform cycle driver) ─────────────────────────────────────────
WORKER_OUTAGE = "WORKER_OUTAGE"


def evaluate_runtime_workers(ctx: ProducerContext, *, owner_stale_ms: int = 150_000,
                             cycle_stall_ms: int = 120_000) -> Dict[str, Any]:
    """The trading runtime that holds the ownership lease stopped renewing it (crashed, frozen, host lost), or a
    platform cycle stopped heartbeating. Bounds: ``runtime_supervisor`` (lease stale 150 s, cycle stall 120 s).
    Platform workers (``platform_workers``) are ``app.ops.alerting.evaluate_worker_heartbeats``."""
    now, db, signals = ctx.now(), ctx.runtime_db, []
    for lease_name, heartbeat_at in rows(db, "SELECT lease_name, heartbeat_at FROM runtime_ownership "
                                             "WHERE released_at IS NULL"):
        hb = iso_ms(heartbeat_at)
        if hb is not None and now - hb > owner_stale_ms:
            signals.append(Signal(WORKER_OUTAGE, {"runtime_owner": lease_name},
                                  f"Trading runtime '{lease_name}' has not renewed its ownership lease for "
                                  f"{(now - hb) // 1000}s", {"lease_name": lease_name, "heartbeat_age_ms": now - hb,
                                                            "bound_ms": owner_stale_ms}))
    for running, oldest in rows(db, "SELECT COUNT(*), MIN(heartbeat_at) FROM cati_platform_cycles "
                                    "WHERE status IN ('STARTED', 'MARKET_DECIDED')"):
        if running and oldest is not None and now - int(oldest) > cycle_stall_ms:
            signals.append(Signal(WORKER_OUTAGE, "platform_cycle:stalled",
                                  f"A platform cycle has not heartbeated for {(now - int(oldest)) // 1000}s",
                                  {"running_cycles": int(running), "heartbeat_age_ms": now - int(oldest),
                                   "bound_ms": cycle_stall_ms}))
    return publish(ctx, "runtime_workers", (WORKER_OUTAGE,), signals)


# ── 2. failed lease takeover (legacy per-account leases) ──────────────────────────────────────────────────
def evaluate_account_leases(ctx: ProducerContext, *, grace_ms: int = 120_000) -> Dict[str, Any]:
    """Per-account runtime leases (``cati_account_leases``) that expired and were never taken over. Platform
    assignments are ``app.ops.alerting.evaluate_lease_takeovers``."""
    now = ctx.now()
    found = rows(ctx.runtime_db, "SELECT account_id, expires_at FROM cati_account_leases WHERE released_at IS NULL "
                                 "AND expires_at <= ? ORDER BY expires_at", (now - grace_ms,))
    signals = []
    if found:
        signals.append(Signal("LEASE_TAKEOVER_FAILURE", "account_leases:abandoned",
                              f"{len(found)} account lease(s) expired more than {grace_ms // 1000}s ago and were "
                              "never taken over", {"abandoned_leases": len(found),
                                                   "accounts": [pseudonym(a) for a, _ in found[:20]],
                                                   "oldest_expired_age_ms": now - int(found[0][1]),
                                                   "grace_ms": grace_ms}, severity=HIGH))
    return publish(ctx, "account_leases", ("LEASE_TAKEOVER_FAILURE",), signals)


# ── 4. database outage (the legacy runtime database file) ─────────────────────────────────────────────────
class RuntimeDatabaseProbe:
    """``health()`` of the legacy SQLite runtime database, read-only (for ``evaluate_database``)."""

    def __init__(self, path: Optional[str]) -> None:
        self.path = path

    def health(self) -> Dict[str, Any]:
        if not self.path or not Path(self.path).is_file():
            return {"ready": False, "dialect": "sqlite", "error": "RUNTIME_DATABASE_FILE_MISSING"}
        started = time.perf_counter()
        try:
            conn = sqlite3.connect(f"file:{Path(self.path).as_posix()}?mode=ro", uri=True, timeout=5)
            try:
                conn.execute("SELECT COUNT(*) FROM sqlite_master").fetchone()
            finally:
                conn.close()
        except Exception as exc:  # noqa: BLE001 -- any failure is the answer
            return {"ready": False, "dialect": "sqlite", "error": f"{type(exc).__name__}: {exc}"[:300]}
        return {"ready": True, "dialect": "sqlite", "latency_ms": round((time.perf_counter() - started) * 1000, 3)}


# ── 5. exchange connection loss ───────────────────────────────────────────────────────────────────────────
CONNECTIVITY = "EXCHANGE_CONNECTIVITY_LOSS"


def evaluate_exchange_connectivity(ctx: ProducerContext, *, hold_ms: int = 120_000,
                                   rate_closure_ms: int = 120_000) -> Dict[str, Any]:
    """Account reads failing (``cati_production_state`` READ_FAILED), a user stream that was connected and is down
    (``cati_user_stream_state``), a venue refusing this deployment's requests (``cati_rate_budgets``: a ban, or a
    rate-limit closure longer than ``rate_closure_ms``). Account and stream signals must persist ``hold_ms``."""
    now, db, signals = ctx.now(), ctx.runtime_db, []
    for account_id, observed_at, document in rows(db, "SELECT account_id, observed_at, document "
                                                      "FROM cati_production_state"):
        doc = _json(document)
        if doc.get("status") == "READ_FAILED":
            ref = pseudonym(account_id)
            signals.append(Signal(CONNECTIVITY, {"account": account_id, "kind": "account_read"},
                                  f"Exchange account reads are failing ({ref})",
                                  {"account": ref, "reason": _short(doc.get("reason"), 120), "observed_at": observed_at},
                                  account_ref=str(account_id), hold_ms=hold_ms))
    for venue, env, account, health, reason, lease_until, last_frame, updated, connects in rows(
            db, "SELECT venue, environment, account_ref, health, health_reason, lease_until_ms, last_frame_at_ms, "
                "updated_at_ms, connects FROM cati_user_stream_state"):
        if not connects or reason in ("CLOSED", "STREAM_HELD_BY_ANOTHER_WORKER"):
            continue                     # never connected, closed on purpose, or another worker's row
        owner_silent = health == "LIVE" and int(lease_until or 0) <= now
        if health in ("DISCONNECTED", "STALE") or owner_silent:
            ref = pseudonym(account)
            signals.append(Signal(CONNECTIVITY, {"account": account, "kind": "user_stream", "venue": venue, "env": env},
                                  f"{venue}/{env} user stream is down ({ref})",
                                  {"account": ref, "venue": venue, "environment": env,
                                   "health": "STREAM_OWNER_NOT_REPORTING" if owner_silent else health,
                                   "reason": _short(reason, 80),
                                   "last_frame_age_ms": None if last_frame is None else now - int(last_frame)},
                                  account_ref=str(account), hold_ms=hold_ms))
    for venue, scope_key, scope, until, reason, status, closed_at in rows(
            db, "SELECT venue, scope_key, scope, closed_until_ms, closed_reason, closed_status, closed_at_ms "
                "FROM cati_rate_budgets WHERE closed_until_ms > ?", (now,)):
        if reason != "BANNED" and now - int(closed_at or now) < rate_closure_ms:
            continue                     # an ordinary short rate-limit pause
        account = scope_key[4:] if scope == "UID" and str(scope_key).startswith("uid:") else None
        who = f" ({pseudonym(account)})" if account else ""
        signals.append(Signal(CONNECTIVITY, {"rate_closure": venue, "scope_key": scope_key},
                              f"{venue} refuses requests ({reason}) for another {(int(until) - now) // 1000}s{who}",
                              {"venue": venue, "scope": scope, "closed_reason": reason, "http_status": status,
                               "reopens_in_ms": int(until) - now,
                               **({"account": pseudonym(account)} if account else {})},
                              account_ref=account))
    return publish(ctx, "exchange_connectivity", (CONNECTIVITY,), signals)


# ── 6. stale market data ──────────────────────────────────────────────────────────────────────────────────
def evaluate_market_data(ctx: ProducerContext, *, collector_stale_ms: int = 360_000,
                         feed_stale_ms: int = 36 * HOUR, feed_stuck_ms: int = 6 * HOUR) -> Dict[str, Any]:
    """The hourly collector's heartbeat (``cati_residual_tracker``; the runtime's own freshness bound is 360 s) and
    the daily bar feed (``cati_feed_updates``: last update failed / stuck, or no successful update for 36 h)."""
    now, db, signals = ctx.now(), ctx.runtime_db, []
    for registry_hash, status, heartbeat_at, activated_at in rows(
            db, "SELECT registry_hash, status, heartbeat_at, activated_at FROM cati_residual_tracker"):
        seen = heartbeat_at if heartbeat_at is not None else activated_at
        if seen is not None and now - int(seen) > collector_stale_ms:
            signals.append(Signal("STALE_MARKET_DATA", {"collector": registry_hash},
                                  f"The hourly market-data collector has not heartbeated for {(now - int(seen)) // 1000}s",
                                  {"collector": str(registry_hash)[:16], "status": status,
                                   "heartbeat_age_ms": now - int(seen), "bound_ms": collector_stale_ms}))
    latest = rows(db, "SELECT update_id, status, started_at, completed_at FROM cati_feed_updates "
                      "ORDER BY update_id DESC LIMIT 1")
    if latest:
        update_id, status, started_at, _ = latest[0]
        if status in ("FAILED", "ABANDONED"):
            signals.append(Signal("STALE_MARKET_DATA", "daily_feed:last_update_failed",
                                  f"The latest daily-bar feed update ended {status}",
                                  {"update_id": update_id, "status": status}))
        elif status == "RUNNING" and started_at is not None and now - int(started_at) > feed_stuck_ms:
            signals.append(Signal("STALE_MARKET_DATA", "daily_feed:update_stuck",
                                  f"A daily-bar feed update has been running for {(now - int(started_at)) // HOUR} h",
                                  {"update_id": update_id, "running_ms": now - int(started_at)}))
        for (last_ok,) in rows(db, "SELECT MAX(completed_at) FROM cati_feed_updates "
                                   "WHERE status IN ('COMPLETED', 'PARTIAL')"):
            if last_ok is not None and now - int(last_ok) > feed_stale_ms:
                signals.append(Signal("STALE_MARKET_DATA", "daily_feed:stale",
                                      f"No successful daily-bar feed update for {(now - int(last_ok)) // HOUR} h",
                                      {"last_success_age_ms": now - int(last_ok), "bound_ms": feed_stale_ms}))
    return publish(ctx, "market_data", ("STALE_MARKET_DATA",), signals)


# ── 7. stale instrument rules ─────────────────────────────────────────────────────────────────────────────
def evaluate_instrument_rules(ctx: ProducerContext, *, max_age_ms: Optional[int] = None, in_use_ms: int = 2 * DAY,
                              failing_after: int = 3, catalogs: bool = True) -> Dict[str, Any]:
    """Venue rules for planning (``cati_instrument_rule_refreshes``): no successful refresh within the rules
    cache's max age (planning then refuses with RULES_STALE), or ``failing_after`` refresh failures in a row.
    Only venues/environments still being refreshed (an attempt within ``in_use_ms``) count. ``catalogs`` adds the
    legacy instrument catalog of venues with connected accounts (``venue_instruments``)."""
    from app.exchange.platform_adapter.rules_cache import DEFAULT_MAX_AGE_MS
    bound = int(max_age_ms or DEFAULT_MAX_AGE_MS)
    now, db, signals = ctx.now(), ctx.runtime_db, []
    for venue, env, last_attempt, last_success, failures, last_error in rows(
            db, "SELECT venue, environment, last_attempt_ms, last_success_ms, consecutive_failures, last_error "
                "FROM cati_instrument_rule_refreshes"):
        if last_attempt is None or now - int(last_attempt) > in_use_ms:
            continue
        age = None if last_success is None else now - int(last_success)
        detail = {"venue": venue, "environment": env, "last_success_age_ms": age, "bound_ms": bound,
                  "consecutive_failures": int(failures or 0), "last_error": _short(last_error, 120)}
        if age is None or age > bound:
            signals.append(Signal("STALE_INSTRUMENT_RULES", {"venue": venue, "env": env, "kind": "stale"},
                                  f"{venue}/{env} instrument rules are stale "
                                  f"({'never refreshed' if age is None else f'{age // HOUR} h old'})", detail))
        elif int(failures or 0) >= failing_after:
            signals.append(Signal("STALE_INSTRUMENT_RULES", {"venue": venue, "env": env, "kind": "failing"},
                                  f"{venue}/{env} instrument-rule refresh failed {int(failures)} times in a row",
                                  detail, severity=MEDIUM))
    if catalogs and db is not None:
        from app.ops.multi_asset_alerts import _stale_catalogs
        for a in _stale_catalogs(db, now):
            signals.append(Signal("STALE_INSTRUMENT_RULES", {"catalog": a.dedupe_key}, a.message,
                                  {k: v for k, v in dict(a.details).items()}, severity=MEDIUM))
    return publish(ctx, "instrument_rules", ("STALE_INSTRUMENT_RULES",), signals)


# ── 8. reconciliation mismatch ────────────────────────────────────────────────────────────────────────────
def evaluate_reconciliation(ctx: ProducerContext, *, recent_ms: int = HOUR) -> Dict[str, Any]:
    """Venue state and the ledger disagree: the latest user-stream reconciliation of an account FAILED or found
    differences (``cati_user_stream_reconciliations``); the latest transfer reconciliation left transfers unresolved
    (``broker_transfer_reconciliations``); position corrections or quantity mismatches were recorded in the last
    ``recent_ms`` (``reconciliation_events``, ``data_quality_events``)."""
    now, db, signals = ctx.now(), ctx.runtime_db, []
    for venue, env, account, status, error_code, document in rows(
            db, "SELECT r.venue, r.environment, r.account_ref, r.status, r.error_code, r.document "
                "FROM cati_user_stream_reconciliations r WHERE r.started_at_ms = (SELECT MAX(x.started_at_ms) "
                "FROM cati_user_stream_reconciliations x WHERE x.venue = r.venue AND x.environment = r.environment "
                "AND x.account_ref = r.account_ref)"):
        differences = int(_json(document).get("difference_count") or 0)
        if status == "FAILED" or differences > 0:
            ref = pseudonym(account)
            signals.append(Signal("RECONCILIATION_MISMATCH", {"account": account, "venue": venue, "env": env,
                                                              "kind": "stream_reconcile"},
                                  f"{venue}/{env} reconciliation {'failed' if status == 'FAILED' else 'found differences'}"
                                  f" ({ref})", {"account": ref, "venue": venue, "environment": env, "status": status,
                                               "error_code": _short(error_code, 80), "difference_count": differences},
                                  account_ref=str(account)))
    for account, status, unresolved, run_at in rows(
            db, "SELECT r.broker_account_id, r.status, r.still_unresolved, r.run_at FROM broker_transfer_reconciliations r "
                "WHERE r.run_at = (SELECT MAX(x.run_at) FROM broker_transfer_reconciliations x "
                "WHERE x.broker_account_id = r.broker_account_id)"):
        if int(unresolved or 0) > 0 or str(status or "").upper() not in ("OK", "COMPLETED"):
            ref = pseudonym(account)
            signals.append(Signal("RECONCILIATION_MISMATCH", {"account": account, "kind": "transfer_reconcile"},
                                  f"Transfer reconciliation left {int(unresolved or 0)} transfer(s) unresolved ({ref})",
                                  {"account": ref, "status": status, "still_unresolved": int(unresolved or 0),
                                   "run_at": run_at}, account_ref=str(account)))
    cutoff = ms_iso(now - recent_ms)
    corrections: Dict[str, Dict[str, int]] = {}
    for bot, n in rows(db, "SELECT bot_instance_id, COUNT(*) FROM reconciliation_events WHERE occurred_at >= ? "
                           "GROUP BY bot_instance_id", (cutoff,)):
        corrections.setdefault(bot or "", {})["position_corrections"] = int(n)
    for bot, n in rows(db, "SELECT bot_instance_id, COUNT(*) FROM data_quality_events WHERE "
                           "event_type = 'POSITION_QTY_MISMATCH' AND detected_at >= ? GROUP BY bot_instance_id", (cutoff,)):
        corrections.setdefault(bot or "", {})["quantity_mismatches"] = int(n)
    accounts = dict(rows(db, "SELECT id, broker_account_id FROM bot_instances")) if corrections else {}
    for bot, counts in sorted(corrections.items()):
        account = accounts.get(bot)
        ref = pseudonym(account) if account else pseudonym(bot, "bot")
        signals.append(Signal("RECONCILIATION_MISMATCH", {"bot": bot, "kind": "position_corrections"},
                              f"{sum(counts.values())} position correction(s)/mismatch(es) recorded in the last "
                              f"{recent_ms // MINUTE} min ({ref})", {"subject": ref, **counts},
                              account_ref=str(account) if account else None))
    return publish(ctx, "reconciliation", ("RECONCILIATION_MISMATCH",), signals)


# ── 9. unknown order outcome ──────────────────────────────────────────────────────────────────────────────
def evaluate_execution_uncertainty(ctx: ProducerContext, *, grace_ms: int = 60_000,
                                   submitting_ms: int = 120_000) -> Dict[str, Any]:
    """Orders whose venue outcome is unknown, per account, from every durable execution ledger: CATI reservations
    RESOLUTION_PENDING, CATI execution attempts whose latest state is SUBMIT_UNKNOWN / PENDING_SUBMIT, pipeline
    intents UNKNOWN (or SUBMITTING too long), the reservation ledger UNKNOWN, legacy pending entries SUBMIT_UNKNOWN,
    and platform order intents left SUBMITTING (platform database, its clock)."""
    now, db = ctx.now(), ctx.runtime_db
    per: Dict[str, Dict[str, Any]] = {}

    def add(account: Any, source: str, since: Any) -> None:
        if account is None:
            account = "unknown-account"
        entry = per.setdefault(str(account), {"by_source": {}, "oldest": None})
        entry["by_source"][source] = entry["by_source"].get(source, 0) + 1
        if since is not None and (entry["oldest"] is None or int(since) < entry["oldest"]):
            entry["oldest"] = int(since)

    for account, since in rows(db, "SELECT broker_account_id, COALESCE(pending_since, updated_at) FROM "
                                   "cati_portfolio_reservations WHERE status = 'RESOLUTION_PENDING'"):
        if since is None or now - int(since) >= grace_ms:
            add(account, "reservation_resolution_pending", since)
    for account, status, recorded_at in rows(
            db, "SELECT a.broker_account_id, a.status, a.recorded_at FROM cati_execution_attempts a "
                "WHERE a.status IN ('SUBMIT_UNKNOWN', 'PENDING_SUBMIT') AND a.sequence = (SELECT MAX(b.sequence) "
                "FROM cati_execution_attempts b WHERE b.execution_attempt_id = a.execution_attempt_id)"):
        if now - int(recorded_at) >= grace_ms:
            add(account, f"attempt_{str(status).lower()}", recorded_at)
    for account, state, since in rows(db, "SELECT account_id, state, COALESCE(submitted_at, updated_at) FROM "
                                          "cati_execution_intents WHERE state IN ('UNKNOWN', 'SUBMITTING')"):
        if state == "UNKNOWN" or (since is not None and now - int(since) >= submitting_ms):
            add(account, f"intent_{str(state).lower()}", since)
    for account, since in rows(db, "SELECT account_id, COALESCE(unknown_since, updated_at) FROM "
                                   "cati_reservation_ledger WHERE state = 'UNKNOWN'"):
        add(account, "ledger_unknown", since)
    for account, bot, submitted in rows(db, "SELECT b.broker_account_id, p.bot_id, p.submitted_at_ms FROM "
                                            "pending_entries p LEFT JOIN bot_instances b ON b.id = p.bot_id WHERE "
                                            "p.submit_state = 'SUBMIT_UNKNOWN' AND p.state = 'PENDING_OPEN'"):
        add(account or f"bot:{bot}", "pending_entry_submit_unknown", submitted or None)
    if ctx.platform_db is not None:
        pnow = ctx.platform_now()
        with ctx.platform_db.transaction(write=False) as conn:
            for account, n, oldest in conn.all("SELECT account_id, COUNT(*), MIN(updated_at) FROM platform_order_intents "
                                               "WHERE status = 'SUBMITTING' AND updated_at <= ? GROUP BY account_id",
                                               (pnow - submitting_ms,)):
                for _ in range(int(n)):
                    add(account, "platform_intent_submitting", oldest)
    signals = []
    for account, entry in sorted(per.items()):
        total = sum(entry["by_source"].values())
        ref = pseudonym(account)
        signals.append(Signal("EXCHANGE_EXECUTION_UNCERTAINTY", {"account": account},
                              f"{total} order(s) with unknown venue outcome ({ref})",
                              {"account": ref, "orders": total, "by_source": entry["by_source"],
                               "oldest_unknown_age_ms": None if entry["oldest"] is None else max(0, now - entry["oldest"])},
                              account_ref=account))
    return publish(ctx, "execution_uncertainty", ("EXCHANGE_EXECUTION_UNCERTAINTY",), signals)


# ── 10. missing protective stop ───────────────────────────────────────────────────────────────────────────
def evaluate_protection(ctx: ProducerContext, *, unknown_grace_ms: int = 60_000,
                        pending_grace_ms: int = 120_000) -> Dict[str, Any]:
    """Positions without a confirmed protective stop, per account: the production path's uncertainty register
    (``cati_production_protection_uncertainty``, protection UNKNOWN) and the pipeline's protection state
    (``cati_protection_state`` UNCERTAIN / UNDER_PROTECTED / ABSENT, or PENDING too long)."""
    now, db = ctx.now(), ctx.runtime_db
    per: Dict[str, Dict[str, Any]] = {}
    for account, symbol, reason, first_seen in rows(
            db, "SELECT account_id, symbol, reason, first_seen_at FROM cati_production_protection_uncertainty "
                "WHERE first_seen_at <= ?", (now - unknown_grace_ms,)):
        e = per.setdefault(str(account), {"positions": [], "oldest": int(first_seen)})
        e["positions"].append({"symbol": symbol, "state": "UNKNOWN", "reason": _short(reason, 60)})
        e["oldest"] = min(e["oldest"], int(first_seen))
    for account, symbol, status, updated_at in rows(
            db, "SELECT account_id, symbol, status, updated_at FROM cati_protection_state WHERE status IN "
                "('UNCERTAIN', 'UNDER_PROTECTED', 'ABSENT') OR (status = 'PENDING' AND updated_at <= ?)",
            (now - pending_grace_ms,)):
        e = per.setdefault(str(account), {"positions": [], "oldest": int(updated_at)})
        e["positions"].append({"symbol": symbol, "state": status})
        e["oldest"] = min(e["oldest"], int(updated_at))
    signals = []
    for account, e in sorted(per.items()):
        ref = pseudonym(account)
        signals.append(Signal("UNPROTECTED_POSITION", {"account": account},
                              f"{len(e['positions'])} position(s) without a confirmed protective stop ({ref})",
                              {"account": ref, "positions": e["positions"][:20], "oldest_age_ms": now - e["oldest"]},
                              account_ref=account))
    return publish(ctx, "protection", ("UNPROTECTED_POSITION",), signals)


# ── 11. emergency flatten ─────────────────────────────────────────────────────────────────────────────────
def evaluate_emergency_flattens(ctx: ProducerContext, *, lookback_ms: int = DAY, incomplete_lookback_ms: int = 7 * DAY,
                                stalled_ms: Optional[int] = None) -> Dict[str, Any]:
    """Every emergency flatten (``cati_emergency_requests``) raises one alert, keyed by its request id, even if it
    started and finished between two ticks; it resolves (RECOVERED) when the flatten COMPLETED. A flatten that is
    stalled (no heartbeat within the emergency TTL) or ended in any other outcome raises a CRITICAL alert of its
    own; an incomplete one stays until a human resolves it (or ``incomplete_lookback_ms`` passes)."""
    from app.trading_intelligence.pipeline.coordination import EMERGENCY_ACTIVE_TTL_MS
    stalled_ms = int(stalled_ms or EMERGENCY_ACTIVE_TTL_MS)
    now = ctx.now()
    found = rows(ctx.runtime_db,
                 "SELECT request_id, scope, account_id, state, outcome, requested_at, heartbeat_at, reason FROM "
                 "cati_emergency_requests WHERE state IN ('REQUESTED', 'IN_PROGRESS') OR requested_at >= ? OR "
                 "(state = 'DONE' AND COALESCE(outcome, '') <> 'COMPLETED' AND requested_at >= ?)",
                 (now - lookback_ms, now - incomplete_lookback_ms))
    event_keys = {r[0]: {"emergency_request": r[0]} for r in found}
    seen = ctx.engine.raised_before(fingerprint("KILL_SWITCH_ACTIVATED", k) for k in event_keys.values())
    signals = []
    for request_id, scope, account, state, outcome, requested_at, heartbeat_at, reason in found:
        active = state in ("REQUESTED", "IN_PROGRESS")
        account_ref = str(account) if scope == "ACCOUNT" and account else None
        where = f"account {pseudonym(account)}" if account_ref else "ALL accounts"
        base = {"scope": scope, **({"account": pseudonym(account)} if account_ref else {}), "state": state,
                "outcome": outcome, "reason": _short(reason, 200), "requested_age_ms": now - int(requested_at)}
        key = event_keys[request_id]
        incomplete = state == "DONE" and (outcome or "") != "COMPLETED"
        if active or incomplete or fingerprint("KILL_SWITCH_ACTIVATED", key) not in seen:
            signals.append(Signal("KILL_SWITCH_ACTIVATED", key, f"Emergency flatten requested for {where} ({state})",
                                  base, account_ref=account_ref, once=True))
        if active and heartbeat_at is not None and now - int(heartbeat_at) > stalled_ms:
            signals.append(Signal("KILL_SWITCH_ACTIVATED", {"emergency_request": request_id, "kind": "stalled"},
                                  f"Emergency flatten for {where} has stalled ({(now - int(heartbeat_at)) // 1000}s "
                                  "without progress)", base, severity=CRITICAL, account_ref=account_ref))
        if incomplete and int(requested_at) >= now - incomplete_lookback_ms:
            signals.append(Signal("KILL_SWITCH_ACTIVATED", {"emergency_request": request_id, "kind": "incomplete"},
                                  f"Emergency flatten for {where} did not complete ({_short(outcome, 60)}): positions "
                                  "may still be open", base, severity=CRITICAL, account_ref=account_ref, once=True))
    return publish(ctx, "emergency_flatten", ("KILL_SWITCH_ACTIVATED",), signals)


# ── 12. platform kill switch (global, account, family) ────────────────────────────────────────────────────
def evaluate_kill_switches(ctx: ProducerContext, *, lookback_ms: int = DAY) -> Dict[str, Any]:
    """Every activation of the new-entry kill switch (``cati_governance_controls``: GLOBAL or one account) and of a
    family kill switch (``cati_family_events``) raises one alert keyed by its control record; it resolves when the
    switch is released. An activation released between two ticks is still raised once."""
    now, db = ctx.now(), ctx.runtime_db
    controls = rows(db, "SELECT control_event_id, state, scope, recorded_at, payload FROM cati_governance_controls "
                        "WHERE control = 'KILL_NEW_CATI_ENTRIES' ORDER BY recorded_at, rowid")
    families = rows(db, "SELECT seq, family_id, event_type, recorded_at FROM cati_family_events WHERE event_type IN "
                        "('FAMILY_KILL_SWITCH_ON', 'FAMILY_KILL_SWITCH_OFF') ORDER BY seq")
    latest_control = {scope: event_id for event_id, _, scope, _, _ in controls}
    latest_family = {family: seq for seq, family, _, _ in families}
    candidates: List[Tuple[Dict[str, Any], bool, Signal]] = []
    for event_id, state, scope, recorded_at, payload in controls:
        if state != "ON":
            continue
        current = latest_control.get(scope) == event_id
        if not current and int(recorded_at) < now - lookback_ms:
            continue
        glob = scope == "GLOBAL"
        key = {"kill_switch": event_id}
        title = "Global new-entry kill switch engaged" if glob else f"Account kill switch engaged ({pseudonym(scope)})"
        detail = {"scope": "GLOBAL" if glob else "ACCOUNT", **({} if glob else {"account": pseudonym(scope)}),
                  "reason": _short(_json(payload).get("reason"), 200), "engaged_age_ms": now - int(recorded_at),
                  "released": not current}
        candidates.append((key, current, Signal("KILL_SWITCH_ACTIVATED", key, title, detail,
                                                account_ref=None if glob else str(scope), once=True)))
    for seq, family, event_type, recorded_at in families:
        if event_type != "FAMILY_KILL_SWITCH_ON":
            continue
        current = latest_family.get(family) == seq
        if not current and int(recorded_at) < now - lookback_ms:
            continue
        key = {"family_kill_switch": seq, "family": family}
        candidates.append((key, current, Signal(
            "KILL_SWITCH_ACTIVATED", key, f"Family kill switch engaged: {family}",
            {"scope": "FAMILY", "family_id": family, "engaged_age_ms": now - int(recorded_at), "released": not current},
            severity=MEDIUM, once=True)))
    seen = ctx.engine.raised_before(fingerprint("KILL_SWITCH_ACTIVATED", k) for k, _, _ in candidates)
    signals = [s for key, current, s in candidates if current or fingerprint("KILL_SWITCH_ACTIVATED", key) not in seen]
    return publish(ctx, "kill_switch", ("KILL_SWITCH_ACTIVATED",), signals)


# ── 13. risk-brake activation ─────────────────────────────────────────────────────────────────────────────
def evaluate_risk_brakes(ctx: ProducerContext) -> Dict[str, Any]:
    """An account brake engaged (``cati_brake_state``): daily-loss pause and drawdown halving (MEDIUM), drawdown stop
    and equity exhausted (HIGH, latched until an authorized restart). One alert per activation (keyed by when it
    triggered); it resolves when the brake clears."""
    signals = []
    for account, scope, brake, halted, reason, triggered_at, triggered_day, drawdown in rows(
            ctx.runtime_db, "SELECT account_id, scope, active_brake, halted, reason, triggered_at, triggered_day, "
                            "drawdown FROM cati_brake_state WHERE active_brake <> 'NONE' OR halted = 1"):
        severe = bool(halted) or brake in ("DRAWDOWN_STOP", "EQUITY_EXHAUSTED")
        ref = pseudonym(account)
        signals.append(Signal("RISK_BRAKE_ACTIVATED", {"account": account, "scope": scope, "brake": brake,
                                                       "since": triggered_at or triggered_day},
                              f"Risk brake {brake}{' (trading halted)' if halted else ''} engaged ({ref})",
                              {"account": ref, "scope": "account" if scope == "account" else "scoped", "brake": brake,
                               "halted": bool(halted), "reason": _short(reason, 120), "triggered_day": triggered_day,
                               "drawdown": drawdown}, severity=HIGH if severe else None, account_ref=str(account),
                              once=True))
    return publish(ctx, "risk_brakes", ("RISK_BRAKE_ACTIVATED",), signals)


# ── 14. family certification expiry ───────────────────────────────────────────────────────────────────────
def evaluate_family_certification(ctx: ProducerContext, *, registry: Any = None) -> Dict[str, Any]:
    """A family that holds order authority (governance stage at or above the demo stage) whose research
    certification is no longer CERTIFIED (revoked, failed, missing, register unreadable). Families have no
    calendar expiry: certification is re-read from the research register every time, so "expired" means the
    register no longer supports the stage. Every family at M0 (today: all of them) is out of scope."""
    if ctx.runtime_db is None or not table_exists(ctx.runtime_db, "cati_family_events"):
        return publish(ctx, "family_certification", ("EXPIRED_FAMILY_CERTIFICATION",), [])
    from app.trading_intelligence.governance.phases import phase_index
    from app.trading_intelligence.pipeline import family_registry as FR
    from app.trading_intelligence.pipeline.family_governance import DEMO_STAGE
    registry = registry or FR.FamilyRegistry(ctx.runtime_db)
    signals = []
    for family_id in registry.governance.families():
        state = registry.governance.state(family_id)
        if phase_index(state.stage) < phase_index(DEMO_STAGE):
            continue
        entry = registry.entry(family_id)
        status = entry.certification_status if entry is not None else "UNKNOWN"
        if status != FR.CERTIFIED:
            reason = (entry.certification_detail or {}).get("reason") if entry is not None else "DEFINITION_MISSING"
            signals.append(Signal("EXPIRED_FAMILY_CERTIFICATION", {"family": family_id, "stage": state.stage},
                                  f"Family {family_id} holds stage {state.stage} without a valid certification ({status})",
                                  {"family_id": family_id, "stage": state.stage, "certification_status": status,
                                   "reason": _short(reason, 160)}))
    return publish(ctx, "family_certification", ("EXPIRED_FAMILY_CERTIFICATION",), signals)


# ── 15. venue certification expiry ────────────────────────────────────────────────────────────────────────
def evaluate_venue_certification(ctx: ProducerContext, *, warn_before_ms: int = 14 * DAY,
                                  matrix: Optional[Callable[[Any, int], Mapping[str, Any]]] = None) -> Dict[str, Any]:
    """The adapter certification register (``cati_adapter_cert_events`` via ``certification_matrix``): a venue /
    environment that was certified and is now EXPIRED (90 days, version change), one expiring within
    ``warn_before_ms`` (MEDIUM), or a register whose evidence no longer verifies. Never-certified pairs are silent."""
    if matrix is None:
        if ctx.runtime_db is None or not table_exists(ctx.runtime_db, "cati_adapter_cert_events"):
            return publish(ctx, "venue_certification", ("EXPIRED_EXCHANGE_CERTIFICATION",), [])
        from app.exchange.platform_adapter.certification import certification_matrix as matrix
    now, signals = ctx.now(), []
    for pair, st in sorted(matrix(ctx.runtime_db, now).items()):
        detail = {"venue_environment": pair, "status": st.status, "reason": _short(st.reason, 160),
                  "expires_at_ms": st.expires_at_ms, "run_id": st.run_id}
        reason = str(st.reason or "")
        if st.status == "EXPIRED":
            signals.append(Signal("EXPIRED_EXCHANGE_CERTIFICATION", {"venue_certification": pair, "run": st.run_id},
                                  f"{pair} adapter certification expired ({reason})", detail, once=True))
        elif st.status == "CERTIFIED" and st.expires_at_ms is not None and int(st.expires_at_ms) - now <= warn_before_ms:
            days = max(0, (int(st.expires_at_ms) - now) // DAY)
            signals.append(Signal("EXPIRED_EXCHANGE_CERTIFICATION", {"venue_certification": pair, "run": st.run_id,
                                                                     "kind": "expiring"},
                                  f"{pair} adapter certification expires in {days} day(s)", detail, severity=MEDIUM))
        elif reason.startswith(("REGISTER_INTEGRITY_FAILED", "RUN_EVIDENCE_INTEGRITY_FAILED")):
            signals.append(Signal("EXPIRED_EXCHANGE_CERTIFICATION", {"venue_certification": pair, "kind": "integrity"},
                                  f"{pair} adapter certification evidence does not verify ({reason[:60]})", detail))
    return publish(ctx, "venue_certification", ("EXPIRED_EXCHANGE_CERTIFICATION",), signals)


# ── 16. billing failures ──────────────────────────────────────────────────────────────────────────────────
def evaluate_billing(ctx: ProducerContext, *, window_ms: int = DAY, unprocessed_after_ms: int = 10 * MINUTE,
                     payment_failure_burst: int = 5, rejection_window_ms: int = HOUR,
                     signature_rejection_burst: int = 10) -> Dict[str, Any]:
    """Billing provider events that could not be matched to a customer, events stored but never processed,
    refunds/disputes awaiting an operator decision, a burst of failed payments (``billing_events``,
    ``billing_payment_reviews``), and rejected webhook deliveries (``billing_webhook_rejections``, counted per
    minute by the API): any delivery refused because the webhook secret is missing, or a burst of invalid
    signatures (a misconfigured secret, or someone forging deliveries)."""
    now, db, signals = ctx.now(), ctx.runtime_db, []
    cutoff = time.strftime("%Y-%m-%dT%H:%M:00", time.gmtime((now - rejection_window_ms) / 1000))
    rejected = dict(rows(db, "SELECT reason, SUM(count) FROM billing_webhook_rejections WHERE bucket_start >= ? "
                             "GROUP BY reason", (cutoff,)))
    minutes = rejection_window_ms // MINUTE
    if int(rejected.get("NOT_CONFIGURED") or 0):
        signals.append(Signal("BILLING_WEBHOOK_FAILURE", "billing:webhook_not_configured",
                              f"{int(rejected['NOT_CONFIGURED'])} webhook delivery(ies) refused in the last {minutes} min: "
                              "STRIPE_WEBHOOK_SECRET is not configured",
                              {"rejected_deliveries": int(rejected["NOT_CONFIGURED"]), "window_minutes": minutes}))
    if int(rejected.get("SIGNATURE_INVALID") or 0) >= signature_rejection_burst:
        signals.append(Signal("BILLING_WEBHOOK_FAILURE", "billing:signature_rejections",
                              f"{int(rejected['SIGNATURE_INVALID'])} webhook deliveries with an invalid signature in the "
                              f"last {minutes} min",
                              {"rejected_deliveries": int(rejected["SIGNATURE_INVALID"]), "window_minutes": minutes,
                               "threshold": signature_rejection_burst}))
    since = ms_iso(now - window_ms)
    unmatched = rows(db, "SELECT result, COUNT(*) FROM billing_events WHERE result LIKE 'unmatched:%' AND created_at >= ? "
                         "AND event_type NOT LIKE 'internal.%' GROUP BY result", (since,))
    if unmatched:
        total = sum(int(n) for _, n in unmatched)
        signals.append(Signal("BILLING_WEBHOOK_FAILURE", "billing:unmatched_events",
                              f"{total} billing provider event(s) could not be matched in the last {window_ms // HOUR} h",
                              {"unmatched_events": total, "by_result": {str(r)[:60]: int(n) for r, n in unmatched}}))
    for (n,) in rows(db, "SELECT COUNT(*) FROM billing_events WHERE processed_at IS NULL AND created_at <= ? "
                         "AND event_type NOT LIKE 'internal.%'", (ms_iso(now - unprocessed_after_ms),)):
        if int(n):
            signals.append(Signal("BILLING_WEBHOOK_FAILURE", "billing:unprocessed_events",
                                  f"{int(n)} billing provider event(s) stored but never processed",
                                  {"unprocessed_events": int(n), "after_ms": unprocessed_after_ms}))
    reviews = rows(db, "SELECT kind, COUNT(*) FROM billing_payment_reviews WHERE review_status = 'OPEN' GROUP BY kind")
    if reviews:
        signals.append(Signal("BILLING_WEBHOOK_FAILURE", "billing:payment_reviews_open",
                              f"{sum(int(n) for _, n in reviews)} refund/dispute review(s) await an operator decision",
                              {"open_reviews": {str(k): int(n) for k, n in reviews}}, severity=MEDIUM))
    for (n,) in rows(db, "SELECT COUNT(*) FROM billing_events WHERE event_type = 'invoice.payment_failed' "
                         "AND created_at >= ?", (since,)):
        if int(n) >= payment_failure_burst:
            signals.append(Signal("BILLING_WEBHOOK_FAILURE", "billing:payment_failure_burst",
                                  f"{int(n)} failed payments in the last {window_ms // HOUR} h",
                                  {"payment_failures": int(n), "threshold": payment_failure_burst}, severity=MEDIUM))
    return publish(ctx, "billing", ("BILLING_WEBHOOK_FAILURE",), signals)


# ── 17. authentication anomalies ──────────────────────────────────────────────────────────────────────────
def evaluate_authentication(ctx: ProducerContext, *, window_ms: int = 15 * MINUTE, login_failure_burst: int = 50,
                            admin_mfa_failures: int = 3, lockout_attempts: int = 5,
                            user_lockouts: int = 3) -> Dict[str, Any]:
    """Token misuse and failure bursts in the last ``window_ms`` (``auth_audit_log``, ``login_attempts``): refresh
    token reuse (user: HIGH, admin: CRITICAL), admin MFA failures, admin accounts reaching the lockout threshold,
    several user accounts locked out, a burst of failed logins. Only counts leave the database -- never an e-mail,
    user id or IP."""
    now, db, signals = ctx.now(), ctx.runtime_db, []
    since = ms_iso(now - window_ms)
    counts = {e: (int(n), int(d)) for e, n, d in rows(
        db, "SELECT event_type, COUNT(*), COUNT(DISTINCT COALESCE(user_id, email, '')) FROM auth_audit_log WHERE "
            "created_at >= ? AND event_type IN ('refresh_reuse_detected', 'admin_refresh_reuse_detected', "
            "'admin_login_failed_mfa', 'login_failed', 'login_failed_2fa') GROUP BY event_type", (since,))}
    minutes = window_ms // MINUTE
    if counts.get("admin_refresh_reuse_detected"):
        n, d = counts["admin_refresh_reuse_detected"]
        signals.append(Signal("AUTHENTICATION_ANOMALY", "auth:admin_refresh_reuse",
                              f"Admin refresh-token reuse detected ({n} in {minutes} min): possible session theft",
                              {"events": n, "distinct_admins": d}, severity=CRITICAL))
    if counts.get("refresh_reuse_detected"):
        n, d = counts["refresh_reuse_detected"]
        signals.append(Signal("AUTHENTICATION_ANOMALY", "auth:refresh_reuse",
                              f"Refresh-token reuse detected for {d} user(s) in {minutes} min",
                              {"events": n, "distinct_users": d}))
    n, d = counts.get("admin_login_failed_mfa", (0, 0))
    if n >= admin_mfa_failures:
        signals.append(Signal("AUTHENTICATION_ANOMALY", "auth:admin_mfa_failures",
                              f"{n} failed admin MFA checks in {minutes} min", {"events": n, "distinct_admins": d,
                                                                              "threshold": admin_mfa_failures}))
    failed = sum(counts.get(e, (0, 0))[0] for e in ("login_failed", "login_failed_2fa"))
    if failed >= login_failure_burst:
        signals.append(Signal("AUTHENTICATION_ANOMALY", "auth:login_failure_burst",
                              f"{failed} failed logins in {minutes} min",
                              {"failed_logins": failed, "threshold": login_failure_burst}))
    admin_locked = user_locked = 0
    for key, n in rows(db, "SELECT email, COUNT(*) FROM login_attempts WHERE success = 0 AND attempted_at >= ? "
                           "AND email NOT LIKE 'cleared:%' GROUP BY email HAVING COUNT(*) >= ?",
                       (since, lockout_attempts)):
        if str(key).startswith(("admin:", "admin-mfa:")):
            admin_locked += 1
        else:
            user_locked += 1
    if admin_locked:
        signals.append(Signal("AUTHENTICATION_ANOMALY", "auth:admin_lockout",
                              f"{admin_locked} admin account(s) reached the login lockout threshold in {minutes} min",
                              {"admin_accounts": admin_locked, "attempts_threshold": lockout_attempts}))
    if user_locked >= user_lockouts:
        signals.append(Signal("AUTHENTICATION_ANOMALY", "auth:user_lockouts",
                              f"{user_locked} user account(s) reached the login lockout threshold in {minutes} min",
                              {"user_accounts": user_locked, "attempts_threshold": lockout_attempts}))
    return publish(ctx, "authentication", ("AUTHENTICATION_ANOMALY",), signals)


# ── 20. unusual execution latency ─────────────────────────────────────────────────────────────────────────
def _p95(values: List[int]) -> int:
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(round(0.95 * (len(ordered) - 1))))]


def evaluate_execution_latency(ctx: ProducerContext, *, window_ms: int = 15 * MINUTE, p95_bound_ms: int = 5_000,
                               max_bound_ms: int = 30_000, min_samples: int = 3, scan_rows: int = 5_000) -> Dict[str, Any]:
    """Venue round trips over the last ``window_ms``: the venue journal (``cati_execution_events``: VENUE_AFTER minus
    its VENUE_BEFORE, the latest ``scan_rows`` events) and legacy entry confirmations (``pending_entries``:
    confirmed minus submitted). Fires when the p95 exceeds ``p95_bound_ms`` with at least ``min_samples`` samples,
    or any one call exceeds ``max_bound_ms``. (CATI execution attempts stamp submit and acknowledgement with the
    same cycle time, so they carry no latency.)"""
    now, db, signals = ctx.now(), ctx.runtime_db, []
    series: Dict[str, List[int]] = {"venue_calls": [], "entry_confirmations": []}
    top = rows(db, "SELECT MAX(seq) FROM cati_execution_events")
    if top and top[0][0] is not None:
        before: Dict[int, int] = {}
        for seq, event_type, recorded_at, detail in rows(
                db, "SELECT seq, event_type, recorded_at, detail FROM cati_execution_events WHERE seq > ? AND "
                    "event_type IN ('VENUE_BEFORE', 'VENUE_AFTER') AND recorded_at >= ? ORDER BY seq",
                (int(top[0][0]) - scan_rows, now - window_ms - max_bound_ms)):
            if event_type == "VENUE_BEFORE":
                before[int(seq)] = int(recorded_at)
            else:
                start = before.get(int(_json(detail).get("before_seq") or -1))
                if start is not None and int(recorded_at) >= now - window_ms:
                    series["venue_calls"].append(max(0, int(recorded_at) - start))
    for submitted, confirmed in rows(db, "SELECT submitted_at_ms, confirmed_at_ms FROM pending_entries WHERE "
                                         "confirmed_at_ms IS NOT NULL AND submitted_at_ms > 0 AND confirmed_at_ms >= ?",
                                     (now - window_ms,)):
        series["entry_confirmations"].append(max(0, int(confirmed) - int(submitted)))
    for name, values in series.items():
        if not values:
            continue
        p95, worst = _p95(values), max(values)
        if (len(values) >= min_samples and p95 > p95_bound_ms) or worst > max_bound_ms:
            signals.append(Signal("EXCESSIVE_LATENCY", {"latency": name},
                                  f"Execution latency ({name.replace('_', ' ')}): p95 {p95} ms, max {worst} ms over "
                                  f"{len(values)} call(s) in {window_ms // MINUTE} min",
                                  {"series": name, "samples": len(values), "p95_ms": p95, "max_ms": worst,
                                   "p95_bound_ms": p95_bound_ms, "max_bound_ms": max_bound_ms}))
    return publish(ctx, "execution_latency", ("EXCESSIVE_LATENCY",), signals)


# ── 19. governance violation attempts ─────────────────────────────────────────────────────────────────────
#: Refusals only an attempt to get round governance produces: a gate, authority or adapter that is not the real
#: one in production, family code that differs from its registration, a forged or altered decision, a LIVE entry
#: reaching the go-live gate while live trading is not authorized. The refusals every M0 cycle produces
#: (no order authority, not certified, failed research, kill switch, stale decision) are NOT in this list.
GOVERNANCE_VIOLATION_CODES: Tuple[str, ...] = (
    "FAMILY_GATE_NOT_AUTHORITATIVE", "SUBMISSION_GATE_NOT_AUTHORITATIVE", "GOVERNANCE_AUTHORITY_NOT_AUTHORITATIVE",
    "ORDER_SPREADING_GATE_NOT_AUTHORITATIVE", "TEST_ADAPTER_NOT_ALLOWED", "FAMILY_RULE_HASH_MISMATCH",
    "DECISION_TAMPERED", "DECISION_ACCOUNT_MISMATCH", "DECISION_PLAN_MISMATCH", "DECISION_FUTURE_DATED",
    "GO_LIVE_GATE:")


def _violation_code(reason: Any) -> Optional[str]:
    text = str(reason or "")
    for code in GOVERNANCE_VIOLATION_CODES:
        if code in text:
            return code.rstrip(":")
    return None


def evaluate_governance_violations(ctx: ProducerContext, *, window_ms: int = HOUR) -> Dict[str, Any]:
    """Refusals of the execution boundary and the portfolio gate that mean someone or something tried to get
    round research or execution governance (``GOVERNANCE_VIOLATION_CODES``), from the venue journal's GATE rows
    (``cati_execution_events``) and the production evaluations (``cati_execution_evaluations``). One alert per
    code; only counts leave the database (accounts are counted, never named)."""
    now, db, signals = ctx.now(), ctx.runtime_db, []
    since = now - window_ms
    found: Dict[str, Dict[str, Any]] = {}

    def add(code: Optional[str], account: Any, where: str) -> None:
        if code is None:
            return
        entry = found.setdefault(code, {"refusals": 0, "accounts": set(), "sources": set()})
        entry["refusals"] += 1
        entry["accounts"].add(str(account))
        entry["sources"].add(where)
    for account, outcome in rows(db, "SELECT account_id, outcome FROM cati_execution_events WHERE event_type = 'GATE' "
                                     "AND recorded_at >= ? AND outcome IS NOT NULL", (since,)):
        add(_violation_code(outcome), account, "portfolio_gate")
    for account, reason in rows(db, "SELECT account_id, reason FROM cati_execution_evaluations WHERE observed_at >= ? "
                                    "AND reason IS NOT NULL", (since,)):
        add(_violation_code(reason), account, "execution_boundary")
    for code, entry in sorted(found.items()):
        signals.append(Signal("GOVERNANCE_VIOLATION_ATTEMPT", {"governance": code},
                              f"{entry['refusals']} order(s) refused with {code} in the last {window_ms // MINUTE} min",
                              {"code": code, "refusals": entry["refusals"], "accounts": len(entry["accounts"]),
                               "sources": sorted(entry["sources"]), "window_minutes": window_ms // MINUTE}))
    return publish(ctx, "governance", ("GOVERNANCE_VIOLATION_ATTEMPT",), signals)


# ── 20. public API health ─────────────────────────────────────────────────────────────────────────────────
def _http_status(url: str, timeout_s: float) -> Tuple[Optional[int], Optional[str]]:
    import urllib.error
    import urllib.request
    request = urllib.request.Request(url, headers={"User-Agent": "cosmicforge-ops-tick"})
    try:
        with urllib.request.urlopen(request, timeout=timeout_s) as response:  # noqa: S310 -- operator-configured URL
            return int(response.status), None
    except urllib.error.HTTPError as exc:
        return int(exc.code), None
    except Exception as exc:  # noqa: BLE001 -- refused, timed out, DNS: the API does not answer
        return None, type(exc).__name__


def parse_health_probes(values: Iterable[str]) -> List[Tuple[str, str]]:
    """``name=url`` entries (``--health-url``, or ``OPS_HEALTH_PROBES`` separated by commas). Only http(s)."""
    out: List[Tuple[str, str]] = []
    for value in values:
        for item in str(value or "").split(","):
            name, sep, url = item.strip().partition("=")
            if not item.strip():
                continue
            if not sep or not name.strip() or not url.strip().startswith(("http://", "https://")):
                raise ValueError(f"health probe must be name=http(s)://host:port/path, got {item.strip()[:80]!r}")
            out.append((name.strip(), url.strip()))
    return out


def evaluate_api_health(ctx: ProducerContext, probes: Sequence[Tuple[str, str]], *, timeout_s: float = 5.0,
                        hold_ms: int = 2 * MINUTE, fetch: Optional[Callable[[str, float], Tuple[Optional[int],
                                                                                         Optional[str]]]] = None
                        ) -> Dict[str, Any]:
    """Each configured API's health check (``probes``: ``(name, url)``). A probe that does not answer 200 for
    ``hold_ms`` raises API_OUTAGE (CRITICAL) for that API; it resolves when the API answers again. The ops tick
    runs on its own schedule, so a portal outage is reported even while trading runs normally, and it never
    touches the order path. Only the API name and the status or error class are recorded."""
    fetch = fetch or _http_status
    signals = []
    for name, url in probes:
        status, error = fetch(url, timeout_s)
        if status == 200:
            continue
        signals.append(Signal("API_OUTAGE", {"api": name},
                              f"{name} does not answer its health check ({error or f'HTTP {status}'})",
                              {"api": name, "status": status, "error": error}, hold_ms=hold_ms))
    return publish(ctx, "api_health", ("API_OUTAGE",), signals)


# ── registry ──────────────────────────────────────────────────────────────────────────────────────────────
def runtime_producers(ctx: ProducerContext) -> List[Callable[[], Any]]:
    """The producers that read the legacy runtime database (and, for unknown outcomes, the platform database).
    Each is a zero-argument callable for ``app.ops.alerting.run_cycle``, which isolates failures."""
    def bind(fn: Callable[[ProducerContext], Any]) -> Callable[[], Any]:
        call = lambda: fn(ctx)  # noqa: E731
        call.__name__ = fn.__name__
        return call
    return [bind(f) for f in (evaluate_runtime_workers, evaluate_account_leases, evaluate_exchange_connectivity,
                              evaluate_market_data, evaluate_instrument_rules, evaluate_reconciliation,
                              evaluate_execution_uncertainty, evaluate_protection, evaluate_emergency_flattens,
                              evaluate_kill_switches, evaluate_risk_brakes, evaluate_family_certification,
                              evaluate_venue_certification, evaluate_billing, evaluate_authentication,
                              evaluate_execution_latency, evaluate_governance_violations)]


__all__ = ["GOVERNANCE_VIOLATION_CODES", "ProducerContext", "RuntimeDatabaseProbe", "Signal", "evaluate_account_leases",
           "evaluate_api_health", "evaluate_authentication", "evaluate_governance_violations", "parse_health_probes",
           "evaluate_billing", "evaluate_emergency_flattens", "evaluate_exchange_connectivity",
           "evaluate_execution_latency", "evaluate_execution_uncertainty", "evaluate_family_certification",
           "evaluate_instrument_rules", "evaluate_kill_switches", "evaluate_market_data", "evaluate_protection",
           "evaluate_reconciliation", "evaluate_risk_brakes", "evaluate_runtime_workers", "evaluate_venue_certification",
           "iso_ms", "ms_iso", "publish", "rows", "runtime_producers", "table_exists"]
