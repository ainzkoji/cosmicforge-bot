"""Operational health (§14) and candle-coverage diagnostics (§10/§18).

One endpoint that answers, in seconds: *is this bot actually working?*

The distinction that matters throughout: ``status = active`` means the bot is
configured to run. It says nothing about whether the strategy clock is
advancing. A twelve-hour window once reported ``WAITING_FOR_SIGNAL`` while six
of those hours contained zero candle evaluations. These routes exist so that
cannot happen unnoticed again.
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from typing import Any

from fastapi import APIRouter, Depends, Query

from app.core.auth import require_admin
from app.core.config import settings
from app.ops.runtime_ownership import TRADING_SCHEDULER, current_owner
from app.ops.runtime_watchdog import (
    DEGRADED,
    ERROR,
    HEALTHY,
    candle_coverage,
    get_watchdog,
)
from shared_lib.persistence.db import DB

router = APIRouter(prefix="/api/v1/admin/trading/operations", tags=["Trading Operations"])


def get_db() -> DB:
    return DB()


def _worst(states: list[str]) -> str:
    if ERROR in states:
        return ERROR
    if DEGRADED in states:
        return DEGRADED
    return HEALTHY


def _age(ts: str | None) -> float | None:
    if not ts:
        return None
    try:
        when = datetime.fromisoformat(ts)
    except Exception:
        return None
    if when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)
    return round((datetime.now(timezone.utc) - when).total_seconds(), 1)


def _active_bots(db: DB) -> list[dict[str, Any]]:
    with db.connect() as conn:
        rows = conn.execute(
            """SELECT id, user_id, mode, status, symbols_json, timeframes_json,
                      bot_health_status, bot_health_reason_code, last_run_at
               FROM bot_instances WHERE status='active'"""
        ).fetchall()
    return [dict(r) for r in rows]


@router.get("/health")
def operations_health(db: DB = Depends(get_db), admin: dict = Depends(require_admin)):
    """Whole-runtime health: process, DB, ownership, scheduler, bots, evidence."""
    watchdog = get_watchdog()
    snapshot = watchdog.snapshot()
    states: list[str] = []

    # ── Process ─────────────────────────────────────────────────────────────
    process = {
        "pid": os.getpid(),
        "started_at": snapshot["started_at"],
        "uptime_seconds": _age(snapshot["started_at"]),
    }

    # ── Database ────────────────────────────────────────────────────────────
    db_path = db.path
    db_exists = os.path.exists(db_path)
    database = {
        "path": db_path,
        "role": str(getattr(settings, "DATABASE_ROLE", "development")),
        "exists": db_exists,
        "size_bytes": os.path.getsize(db_path) if db_exists else 0,
    }
    states.append(HEALTHY if db_exists else ERROR)

    # ── Runtime ownership ───────────────────────────────────────────────────
    owner = current_owner(db, db_path, TRADING_SCHEDULER)
    ownership = {
        "lease_name": TRADING_SCHEDULER,
        "held": owner is not None,
        "runtime_owner_id": (owner or {}).get("runtime_owner_id"),
        "pid": (owner or {}).get("pid"),
        "hostname": (owner or {}).get("hostname"),
        "heartbeat_at": (owner or {}).get("heartbeat_at"),
        "heartbeat_age_seconds": _age((owner or {}).get("heartbeat_at")),
        "is_this_process": bool(owner and int(owner.get("pid", -1)) == os.getpid()),
    }
    states.append(HEALTHY if ownership["held"] else DEGRADED)

    # ── Scheduler ───────────────────────────────────────────────────────────
    scheduler = {
        "health": snapshot["scheduler_health"],
        "reason": snapshot["scheduler_health_reason"],
        "heartbeat_at": snapshot["scheduler_heartbeat_at"],
        "heartbeat_age_seconds": snapshot["scheduler_heartbeat_age_seconds"],
        "last_successful_iteration_at": snapshot["last_successful_iteration_at"],
        "iteration_count": snapshot["iteration_count"],
        "last_error": snapshot["last_error"],
    }
    states.append(snapshot["scheduler_health"])

    # ── Bots, market data and strategy clocks ───────────────────────────────
    bots: list[dict[str, Any]] = []
    market_data_states: list[str] = []
    clock_states: list[str] = []
    for instance in _active_bots(db):
        bot_id = instance["id"]
        runtime = snapshot["bots"].get(bot_id, {})
        health = runtime.get("health", DEGRADED)
        states.append(health)
        symbols = runtime.get("symbols", {})
        for clock in symbols.values():
            market_data_states.append(ERROR if clock.get("last_market_data_error") else HEALTHY)
            clock_states.append(ERROR if clock.get("behind_iterations", 0) >= 3 else HEALTHY)
        bots.append({
            "bot_instance_id": bot_id,
            "status": instance["status"],
            "mode": instance["mode"],
            "stored_health": instance["bot_health_status"],
            "stored_reason": instance["bot_health_reason_code"],
            "runtime_health": health,
            "runtime_reason": runtime.get("health_reason"),
            "runner_present": runtime.get("runner_present", False),
            "runner_initialization_status": runtime.get("runner_initialization_status"),
            "last_cycle_at": runtime.get("last_cycle_at"),
            "last_cycle_age_seconds": _age(runtime.get("last_cycle_at")),
            "run_id": runtime.get("run_id"),
            "policy_hash": runtime.get("policy_hash"),
            "runtime_session_id": runtime.get("runtime_session_id"),
            "last_execution_attempt_at": runtime.get("last_execution_attempt_at"),
            "symbols": symbols,
        })

    # ── Canonical evidence ──────────────────────────────────────────────────
    since = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
    with db.connect() as conn:
        decisions = conn.execute(
            "SELECT COUNT(*) FROM trading_decisions WHERE evaluated_at > ?", (since,)
        ).fetchone()[0]
        cycles = conn.execute(
            "SELECT COUNT(*) FROM trading_cycles WHERE started_at > ?", (since,)
        ).fetchone()[0]
        incomplete = conn.execute(
            "SELECT COUNT(*) FROM trading_decisions WHERE complete=0 AND evaluated_at < ?",
            ((datetime.now(timezone.utc) - timedelta(seconds=120)).isoformat(),),
        ).fetchone()[0]
    evidence = {
        "decisions_last_hour": decisions,
        "cycles_last_hour": cycles,
        "incomplete_decisions": incomplete,
    }
    states.append(ERROR if incomplete else HEALTHY)

    # ── Positions and execution ─────────────────────────────────────────────
    with db.connect() as conn:
        open_positions = conn.execute(
            "SELECT COUNT(*) FROM positions WHERE status='OPEN'"
        ).fetchone()[0]
        attempts = conn.execute(
            "SELECT COUNT(*) FROM execution_attempts WHERE started_at > ?", (since,)
        ).fetchone()[0]

    return {
        "overall": _worst(states),
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "process": process,
        "database": database,
        "runtime_ownership": ownership,
        "scheduler": scheduler,
        "market_data": {"overall": _worst(market_data_states or [HEALTHY])},
        "strategy_clock": {"overall": _worst(clock_states or [HEALTHY])},
        "active_bots": bots,
        "canonical_evidence": evidence,
        "positions": {"open": open_positions},
        "execution": {"attempts_last_hour": attempts},
    }


@router.get("/coverage/{bot_id}")
def evaluation_coverage(
    bot_id: str,
    hours: int = Query(24, ge=1, le=24 * 30),
    timeframe: str = "15m",
    db: DB = Depends(get_db),
    admin: dict = Depends(require_admin),
):
    """Expected closed candles vs actual canonical evaluations (§10/§18).

    A missing slot is a real gap — either the runtime was not running, or it
    was running and failed to evaluate. Both must be visible.
    """
    with db.connect() as conn:
        row = conn.execute(
            "SELECT symbols_json, timeframes_json FROM bot_instances WHERE id=?", (bot_id,)
        ).fetchone()
    symbols: list[str] = []
    if row and row["symbols_json"]:
        try:
            symbols = list(json.loads(row["symbols_json"]))
        except Exception:
            symbols = []

    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    since_ms = now_ms - hours * 3_600_000
    return candle_coverage(
        db, bot_instance_id=bot_id, symbols=symbols, timeframe=timeframe,
        since_ms=since_ms, until_ms=now_ms,
    )


@router.get("/metrics/{bot_id}")
def runtime_metrics(
    bot_id: str,
    hours: int = Query(24, ge=1, le=24 * 30),
    db: DB = Depends(get_db),
    admin: dict = Depends(require_admin),
):
    """The §18 integrity metrics for one bot over a window."""
    since = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
    with db.connect() as conn:
        reasons = {
            r["primary_reason"]: r["n"]
            for r in conn.execute(
                """SELECT primary_reason, COUNT(*) n FROM trading_decisions
                   WHERE bot_instance_id=? AND evaluated_at > ?
                   GROUP BY primary_reason""",
                (bot_id, since),
            )
        }
        cycles = conn.execute(
            """SELECT COUNT(*) c, COALESCE(SUM(no_new_candle_count),0) hb,
                      COALESCE(SUM(new_candle_evaluations),0) ev,
                      COALESCE(SUM(error_count),0) err
               FROM trading_cycles WHERE bot_instance_id=? AND started_at > ?""",
            (bot_id, since),
        ).fetchone()
        attempts = conn.execute(
            "SELECT COUNT(*) FROM execution_attempts WHERE bot_instance_id=? AND started_at > ?",
            (bot_id, since),
        ).fetchone()[0]
        opens = conn.execute(
            """SELECT COUNT(*) FROM position_events
               WHERE bot_instance_id=? AND event_type='OPENED' AND occurred_at > ?""",
            (bot_id, since),
        ).fetchone()[0]
        partials = conn.execute(
            """SELECT COUNT(*) FROM position_events
               WHERE bot_instance_id=? AND event_type IN ('TP1','PARTIAL_CLOSE')
                 AND occurred_at > ?""",
            (bot_id, since),
        ).fetchone()[0]
        closes = conn.execute(
            """SELECT COUNT(*) FROM position_events
               WHERE bot_instance_id=? AND event_type IN ('FINAL_CLOSE','DAILY_CLOSE',
                     'KILL_SWITCH_CLOSE') AND occurred_at > ?""",
            (bot_id, since),
        ).fetchone()[0]
        sessions = conn.execute(
            "SELECT COUNT(*) FROM runtime_sessions WHERE started_at > ?", (since,)
        ).fetchone()[0]

    real_evaluations = sum(v for k, v in reasons.items() if k != "NO_NEW_CANDLE")
    return {
        "bot_instance_id": bot_id,
        "window_hours": hours,
        "cycles": cycles["c"],
        "no_new_candle_heartbeats": cycles["hb"],
        "real_strategy_evaluations": real_evaluations,
        "reason_counts": reasons,
        "market_data_failures": reasons.get("MARKET_DATA_UNAVAILABLE", 0)
        + reasons.get("EXECUTION_DATA_STALE", 0),
        "execution_attempts": attempts,
        "positions_opened": opens,
        "partial_closes": partials,
        "closes": closes,
        "cycle_errors": cycles["err"],
        "process_restarts": sessions,
    }
