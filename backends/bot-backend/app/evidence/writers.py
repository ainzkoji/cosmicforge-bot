"""Writers for the canonical evidence streams (Phases 9 & 11).

Every function here is small on purpose: the runtime should never build SQL
inline, and every row must carry provenance so readiness reporting can filter
test, replay and validation evidence out of organic paper-forward results.

The append-only streams (position_events, risk_events, reconciliation_events,
data_quality_events, bot lifecycle events) are only ever INSERTed. Nothing in
this module updates or deletes them.
"""
from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from shared_lib.persistence.evidence_schema import PAPER_FORWARD

logger = logging.getLogger(__name__)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _uid(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:20]}"


def _json(value: Any) -> str | None:
    if value is None:
        return None
    try:
        return json.dumps(value, sort_keys=True, default=str)
    except Exception:
        return json.dumps(str(value))


def _insert(db: Any, table: str, row: Mapping[str, Any]) -> None:
    columns = ", ".join(row)
    placeholders = ", ".join("?" for _ in row)
    with db.connect() as conn:
        conn.execute(
            f"INSERT OR REPLACE INTO {table} ({columns}) VALUES ({placeholders})",
            tuple(row.values()),
        )


# ── Runtime lineage ─────────────────────────────────────────────────────────


def open_runtime_session(
    db: Any,
    *,
    database_role: str,
    database_path: str,
    schema_version: int | None = None,
    process_execution_mode: str | None = None,
    environment_name: str | None = None,
    code_revision: str | None = None,
    branch: str | None = None,
    working_tree_dirty: bool | None = None,
) -> str:
    """Record that this process started, and return its runtime_session_id."""
    session_id = _uid("rts")
    _insert(db, "runtime_sessions", {
        "runtime_session_id": session_id,
        "started_at": _now(),
        "pid": os.getpid(),
        "parent_pid": os.getppid() if hasattr(os, "getppid") else None,
        "python_executable": sys.executable,
        "python_version": sys.version.split()[0],
        "working_directory": str(Path.cwd()),
        "code_revision": code_revision,
        "branch": branch,
        "working_tree_dirty": None if working_tree_dirty is None else int(working_tree_dirty),
        "database_role": database_role,
        "database_path": str(database_path),
        "schema_version": schema_version,
        "process_execution_mode": process_execution_mode,
        "environment_name": environment_name,
        "status": "RUNNING",
    })
    return session_id


def close_runtime_session(db: Any, session_id: str, *, reason: str = "SHUTDOWN") -> None:
    with db.connect() as conn:
        conn.execute(
            "UPDATE runtime_sessions SET stopped_at=?, status='STOPPED', shutdown_reason=? "
            "WHERE runtime_session_id=?",
            (_now(), reason, session_id),
        )


def open_bot_run(
    db: Any,
    *,
    run_id: str,
    bot_instance_id: str,
    runtime_session_id: str | None = None,
    user_id: str | None = None,
    policy_hash: str | None = None,
    provenance: str = PAPER_FORWARD,
    execution_mode: str | None = None,
    broker_environment: str | None = None,
) -> str:
    _insert(db, "bot_runs", {
        "run_id": run_id,
        "runtime_session_id": runtime_session_id,
        "bot_instance_id": bot_instance_id,
        "user_id": user_id,
        "policy_hash": policy_hash,
        "provenance": provenance,
        "execution_mode": execution_mode,
        "broker_environment": broker_environment,
        "started_at": _now(),
        "status": "RUNNING",
    })
    return run_id


def record_trading_cycle(db: Any, summary: Mapping[str, Any]) -> str:
    """Persist one cycle summary so cycle health is queryable, not log-scraped."""
    row = dict(summary)
    cycle_id = row.get("cycle_id") or _uid("cyc")
    row["cycle_id"] = cycle_id
    row.setdefault("started_at", _now())
    row.setdefault("provenance", PAPER_FORWARD)
    if "reason_counts" in row:
        row["reason_counts_json"] = _json(row.pop("reason_counts"))
    _insert(db, "trading_cycles", row)
    return cycle_id


def record_market_snapshot(
    db: Any,
    snapshot: Any,
    *,
    bot_instance_id: str | None = None,
    provenance: str = PAPER_FORWARD,
) -> str | None:
    """Persist snapshot lineage by reference and hash, not by copying candles."""
    if snapshot is None:
        return None
    snapshot_id = getattr(snapshot, "market_snapshot_id", None)
    if not snapshot_id:
        return None
    _insert(db, "market_snapshots", {
        "market_snapshot_id": snapshot_id,
        "bot_instance_id": bot_instance_id,
        "symbol": snapshot.symbol,
        "timeframe": snapshot.timeframe,
        "closed_candle_open_time": getattr(snapshot, "latest_closed_candle_open_time", None),
        "closed_candle_close_time": snapshot.latest_closed_candle_time,
        "reference_price": snapshot.reference_price,
        "source": snapshot.source,
        "source_environment": getattr(snapshot, "source_environment", None),
        "provenance": provenance,
        "fetched_at": snapshot.fetched_at,
        "data_hash": getattr(snapshot, "data_hash", None),
        "higher_timeframe": snapshot.higher_timeframe,
        "higher_timeframe_closed_candle_time": getattr(
            snapshot, "higher_timeframe_closed_candle_time", None
        ),
        "higher_timeframe_aligned": int(bool(snapshot.htf_is_timestamp_aligned()))
        if hasattr(snapshot, "htf_is_timestamp_aligned") else None,
        "candle_count": len(snapshot.candles),
    })
    return snapshot_id


# ── Execution attempts (§29) ────────────────────────────────────────────────


def open_execution_attempt(
    db: Any,
    *,
    decision_id: str | None,
    bot_instance_id: str,
    symbol: str,
    requested_action: str,
    execution_mode: str | None = None,
    broker_environment: str | None = None,
    provenance: str = PAPER_FORWARD,
    **extra: Any,
) -> str:
    """Open an attempt BEFORE talking to any executor.

    This is what closes the gap between "the decision was approved" and "an
    order exists": if the executor never returns an order id, the attempt row
    still proves we tried, and why it failed.
    """
    attempt_id = _uid("exa")
    _insert(db, "execution_attempts", {
        "execution_attempt_id": attempt_id,
        "decision_id": decision_id,
        "bot_instance_id": bot_instance_id,
        "symbol": str(symbol).upper(),
        "requested_action": requested_action,
        "execution_mode": execution_mode,
        "broker_environment": broker_environment,
        "provenance": provenance,
        "started_at": _now(),
        "result": "IN_PROGRESS",
        **{k: v for k, v in extra.items() if v is not None},
    })
    return attempt_id


def complete_execution_attempt(
    db: Any,
    attempt_id: str,
    *,
    result: str,
    broker_order_id: str | None = None,
    client_order_id: str | None = None,
    position_id: str | None = None,
    primary_reason: str | None = None,
    error_class: str | None = None,
    error_detail: str | None = None,
) -> None:
    with db.connect() as conn:
        conn.execute(
            """UPDATE execution_attempts
               SET completed_at=?, result=?, broker_order_id=?, client_order_id=?,
                   position_id=?, primary_reason=?, error_class=?, error_detail=?
               WHERE execution_attempt_id=?""",
            (_now(), result, broker_order_id, client_order_id, position_id,
             primary_reason, error_class, error_detail, attempt_id),
        )


# ── Positions and their append-only event stream (§31) ──────────────────────


def record_position_opened(
    db: Any,
    *,
    position_id: str,
    bot_instance_id: str,
    symbol: str,
    side: str,
    original_qty: float,
    entry_price: float,
    provenance: str = PAPER_FORWARD,
    **extra: Any,
) -> str:
    _insert(db, "positions", {
        "position_id": position_id,
        "bot_instance_id": bot_instance_id,
        "symbol": str(symbol).upper(),
        "side": str(side).upper(),
        "original_qty": float(original_qty),
        "remaining_qty": float(original_qty),
        "realized_qty": 0.0,
        "entry_price": float(entry_price),
        "status": "OPEN",
        "opened_at": _now(),
        "updated_at": _now(),
        "provenance": provenance,
        **{k: v for k, v in extra.items() if v is not None},
    })
    record_position_event(
        db, position_id=position_id, bot_instance_id=bot_instance_id, symbol=symbol,
        event_type="OPENED", quantity=original_qty, remaining_qty=original_qty,
        price=entry_price, provenance=provenance,
    )
    return position_id


def update_position_quantities(
    db: Any,
    position_id: str,
    *,
    remaining_qty: float,
    realized_qty: float,
    realized_pnl: float | None = None,
    fees: float | None = None,
    status: str | None = None,
    close_reason: str | None = None,
) -> None:
    sets = ["remaining_qty=?", "realized_qty=?", "updated_at=?"]
    args: list[Any] = [float(remaining_qty), float(realized_qty), _now()]
    if realized_pnl is not None:
        sets.append("realized_pnl=?")
        args.append(float(realized_pnl))
    if fees is not None:
        sets.append("fees=?")
        args.append(float(fees))
    if status is not None:
        sets.append("status=?")
        args.append(status)
        if status in {"CLOSED", "FLAT"}:
            sets.append("closed_at=?")
            args.append(_now())
    if close_reason is not None:
        sets.append("close_reason=?")
        args.append(close_reason)
    args.append(position_id)
    with db.connect() as conn:
        conn.execute(f"UPDATE positions SET {', '.join(sets)} WHERE position_id=?", tuple(args))


def record_position_event(
    db: Any,
    *,
    position_id: str,
    bot_instance_id: str,
    symbol: str,
    event_type: str,
    provenance: str = PAPER_FORWARD,
    detail: Mapping[str, Any] | None = None,
    **fields: Any,
) -> str:
    """Append one immutable lifecycle event. Never updated, never deleted."""
    event_id = _uid("pev")
    _insert(db, "position_events", {
        "event_id": event_id,
        "position_id": position_id,
        "bot_instance_id": bot_instance_id,
        "symbol": str(symbol).upper(),
        "event_type": event_type,
        "occurred_at": _now(),
        "provenance": provenance,
        "detail_json": _json(detail),
        **{k: v for k, v in fields.items() if v is not None},
    })
    return event_id


# ── Risk / reconciliation / data-quality streams ────────────────────────────


def record_risk_event(
    db: Any, *, bot_instance_id: str, event_type: str,
    provenance: str = PAPER_FORWARD, detail: Mapping[str, Any] | None = None, **fields: Any,
) -> str:
    event_id = _uid("rev")
    _insert(db, "risk_events", {
        "event_id": event_id, "bot_instance_id": bot_instance_id,
        "event_type": event_type, "occurred_at": _now(), "provenance": provenance,
        "detail_json": _json(detail),
        **{k: v for k, v in fields.items() if v is not None},
    })
    return event_id


def record_reconciliation_event(
    db: Any, *, bot_instance_id: str, expected: Any, observed: Any,
    action: str, reason: str, result: str, provenance: str = PAPER_FORWARD,
    detail: Mapping[str, Any] | None = None, **fields: Any,
) -> str:
    """A mismatch is always recorded before anything is repaired."""
    event_id = _uid("rcv")
    _insert(db, "reconciliation_events", {
        "event_id": event_id, "bot_instance_id": bot_instance_id,
        "occurred_at": _now(), "expected": str(expected), "observed": str(observed),
        "difference": str(expected) + " -> " + str(observed),
        "action": action, "reason": reason, "result": result,
        "provenance": provenance, "detail_json": _json(detail),
        **{k: v for k, v in fields.items() if v is not None},
    })
    return event_id


def record_data_quality_event(
    db: Any, *, event_type: str, detail: str,
    severity: str = "WARNING", **fields: Any,
) -> str:
    event_id = _uid("dqe")
    _insert(db, "data_quality_events", {
        "event_id": event_id, "event_type": event_type, "severity": severity,
        "detected_at": _now(), "detail": detail,
        **{k: v for k, v in fields.items() if v is not None},
    })
    return event_id


# ── Bot lifecycle attribution (§34 / §56) ───────────────────────────────────


class MissingAttributionError(ValueError):
    """Raised when a bot lifecycle change is recorded without an actor or reason."""


def record_bot_lifecycle_event(
    db: Any,
    *,
    bot_instance_id: str,
    event_type: str,
    actor: str,
    reason: str,
    previous_state: str | None = None,
    new_state: str | None = None,
    correlation_id: str | None = None,
    detail: Mapping[str, Any] | None = None,
) -> str:
    """Record an attributable bot lifecycle change.

    The September incident — a bot going active -> deleted with a replacement
    appearing 25 seconds later and no record of the caller — is exactly what
    this refuses to allow. An event without an actor or a reason is rejected
    rather than written as an anonymous state change.
    """
    if not actor or not str(actor).strip():
        raise MissingAttributionError(
            f"{event_type} for {bot_instance_id} requires an actor/source"
        )
    if not reason or not str(reason).strip():
        raise MissingAttributionError(
            f"{event_type} for {bot_instance_id} requires a reason"
        )

    event_id = _uid("ble")
    payload = {
        "actor": actor,
        "reason": reason,
        "previous_state": previous_state,
        "new_state": new_state,
        "correlation_id": correlation_id,
        **(dict(detail) if detail else {}),
    }
    _insert(db, "bot_system_events", {
        "event_id": event_id,
        "bot_instance_id": bot_instance_id,
        "user_id": (detail or {}).get("user_id"),
        "run_id": correlation_id,
        "event_type": event_type,
        "severity": "INFO",
        "reason_code": reason,
        "message": f"{event_type}: {previous_state or '-'} -> {new_state or '-'} by {actor}",
        "details_json": _json(payload),
        "provenance": (detail or {}).get("provenance", PAPER_FORWARD),
        "created_at": _now(),
    })
    return event_id


def _git(*args: str) -> str | None:
    try:
        out = subprocess.run(["git", *args], capture_output=True, text=True, timeout=5)
        return (out.stdout or "").strip() or None
    except Exception:
        return None
