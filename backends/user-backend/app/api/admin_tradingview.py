from __future__ import annotations

import os
from typing import Any

from fastapi import APIRouter, Depends, Query

from app.core.deps import require_admin
from shared_lib.persistence.db import DB
from shared_lib.persistence.tradingview import (
    get_processor_heartbeat,
    list_alerts,
    list_decisions,
    list_external_signal_queue,
    list_processor_heartbeats,
    list_webhooks,
)


router = APIRouter()


def _get_db() -> DB:
    return DB()


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except Exception:
        return default


def _env_csv(name: str, default: str = "") -> list[str]:
    return [p.strip().upper() for p in os.getenv(name, default).split(",") if p.strip()]


@router.get("/admin/tradingview/webhooks")
def get_tradingview_webhooks(
    limit: int = Query(100, ge=1, le=500),
    _admin: dict[str, Any] = Depends(require_admin),
    db: DB = Depends(_get_db),
):
    return {"items": list_webhooks(db, limit=limit)}


@router.get("/admin/tradingview/alerts")
def get_tradingview_alerts(
    limit: int = Query(100, ge=1, le=500),
    _admin: dict[str, Any] = Depends(require_admin),
    db: DB = Depends(_get_db),
):
    return {"items": list_alerts(db, limit=limit)}


@router.get("/admin/tradingview/decisions")
def get_tradingview_decisions(
    limit: int = Query(100, ge=1, le=500),
    _admin: dict[str, Any] = Depends(require_admin),
    db: DB = Depends(_get_db),
):
    return {"items": list_decisions(db, limit=limit)}


@router.get("/admin/tradingview/external-signals")
def get_tradingview_external_signals(
    limit: int = Query(100, ge=1, le=500),
    _admin: dict[str, Any] = Depends(require_admin),
    db: DB = Depends(_get_db),
):
    return {"items": list_external_signal_queue(db, limit=limit)}


@router.get("/admin/tradingview/processor-status")
def get_tradingview_processor_status(
    bot_instance_id: str | None = Query(None, description="Filter by bot instance ID"),
    _admin: dict[str, Any] = Depends(require_admin),
    db: DB = Depends(_get_db),
):
    """
    Read-only view of the Phase 4 TradingView external signal processor state.

    Returns per-bot heartbeat rows written by the runner after each cycle.
    Fields:
      bot_instance_id     — which bot this status belongs to
      processor_enabled   — whether Phase 4 is enabled for this bot
      env_gate_reason     — reason string if last run was gated/blocked (null = allowed)
      last_started_at     — ISO timestamp of last processor run start
      last_finished_at    — ISO timestamp of last processor run finish
      last_processed_count— signals executed (EXECUTED or EXECUTED_PAPER) in last run
      last_rejected_count — signals rejected by safety gates in last run
      last_failed_count   — signals that failed execution in last run
      last_skipped_count  — signals skipped (race-condition) in last run
      last_skipped_reason — reason the whole run was skipped (e.g. PREVIOUS_RUN_STILL_ACTIVE)
      last_result_json    — [{symbol: outcome}, ...] summary of last run
      last_error          — error message if last run raised an exception
      updated_at          — when this row was last written
    """
    if bot_instance_id:
        hb = get_processor_heartbeat(db, bot_instance_id)
        if hb is None:
            return {"items": [], "note": f"No heartbeat found for bot_instance_id={bot_instance_id!r}"}
        return {"items": [hb]}

    return {"items": list_processor_heartbeats(db)}


@router.get("/admin/tradingview/limited-status")
def get_tradingview_limited_status(
    bot_instance_id: str | None = Query(None, description="Filter by bot instance ID"),
    _admin: dict[str, Any] = Depends(require_admin),
    db: DB = Depends(_get_db),
):
    """Read-only Phase 6 limited-mode status. Never exposes tokens or secrets."""
    with db.connect() as conn:
        lockout_rows = conn.execute(
            """
            SELECT * FROM tradingview_safety_lockouts
            WHERE (? IS NULL OR bot_instance_id = ?)
            ORDER BY updated_at DESC
            """,
            (bot_instance_id, bot_instance_id),
        ).fetchall() if conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='tradingview_safety_lockouts'"
        ).fetchone() else []
        queue_status = conn.execute(
            """
            SELECT status, COUNT(*) AS count
            FROM external_signal_queue
            WHERE (? IS NULL OR bot_id = ?)
            GROUP BY status
            """
            ,
            (bot_instance_id, bot_instance_id),
        ).fetchall()
        today_exec = conn.execute(
            """
            SELECT COUNT(*) AS count
            FROM tradingview_signal_decisions
            WHERE (? IS NULL OR bot_id = ?)
              AND final_status IN ('PROCESSED_EXECUTED','PROCESSED_EXECUTED_PROTECTED')
              AND created_at >= datetime('now','-1 day')
            """,
            (bot_instance_id, bot_instance_id),
        ).fetchone()
        unprotected = conn.execute(
            """
            SELECT COUNT(*) AS count
            FROM position_lifecycle_state
            WHERE (? IS NULL OR bot_instance_id = ?)
              AND COALESCE(exchange_position_active, 0) = 1
              AND (
                sl_order_id IS NULL OR tp_order_id IS NULL
                OR sl_order_id LIKE '%DUPLICATE_4130%'
                OR tp_order_id LIKE '%DUPLICATE_4130%'
              )
            """,
            (bot_instance_id, bot_instance_id),
        ).fetchone()

    return {
        "phase": "Phase 6 — Live-Mode Limited TradingView Candidate Mode",
        "enabled": _env_bool("TRADINGVIEW_EXTERNAL_SIGNALS_ENABLED", False)
        and _env_bool("TRADINGVIEW_LIVE_MODE_LIMITED_ENABLED", False),
        "live_limited_mode_enabled": _env_bool("TRADINGVIEW_LIVE_MODE_LIMITED_ENABLED", False),
        "allowed_symbols": _env_csv("TRADINGVIEW_ALLOWED_SYMBOLS", "BTCUSDT,ETHUSDT,BNBUSDT"),
        "allowed_actions": _env_csv("TRADINGVIEW_ALLOWED_ACTIONS", "BUY,SELL"),
        "max_trade_usdt_cap": _env_float("TRADINGVIEW_MAX_TRADE_USDT_CAP", 150.0),
        "hourly_signal_limit": _env_int("TRADINGVIEW_MAX_SIGNALS_PER_HOUR", 5),
        "daily_signal_limit": _env_int("TRADINGVIEW_MAX_SIGNALS_PER_DAY", 20),
        "daily_execution_cap": _env_int("TRADINGVIEW_MAX_EXECUTIONS_PER_DAY", 3),
        "max_queue_per_cycle": _env_int("TRADINGVIEW_MAX_QUEUE_PER_CYCLE", 1),
        "executions_used_today": int(today_exec["count"] if today_exec else 0),
        "queue_status": [dict(r) for r in queue_status],
        "safety_lockouts": [dict(r) for r in lockout_rows],
        "safety_lockout_active": any(int(r["is_locked"] or 0) for r in lockout_rows),
        "unprotected_positions_count": int(unprotected["count"] if unprotected else 0),
        "forbidden_capabilities": {
            "close": _env_bool("TRADINGVIEW_ALLOW_CLOSE", False),
            "reverse": _env_bool("TRADINGVIEW_ALLOW_REVERSE", False),
            "reduce": _env_bool("TRADINGVIEW_ALLOW_REDUCE", False),
            "cancel": _env_bool("TRADINGVIEW_ALLOW_CANCEL", False),
            "external_sltp": _env_bool("TRADINGVIEW_ALLOW_EXTERNAL_SLTP", False),
            "external_size": _env_bool("TRADINGVIEW_ALLOW_EXTERNAL_SIZE", False),
            "risk_override": _env_bool("TRADINGVIEW_ALLOW_RISK_OVERRIDE", False),
        },
    }
