from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from app.persistence.db import AdminDB


DEFAULT_RUN_LIMIT = 20
MAX_RUN_LIMIT = 100
EVENT_LIMIT = 100
TRACE_LIMIT = 50
LIVE_DECISION_LIMIT = 10
LIVE_EVENT_LIMIT = 20


def _bounded_limit(limit: int | None, *, default: int = DEFAULT_RUN_LIMIT, maximum: int = MAX_RUN_LIMIT) -> int:
    try:
        parsed = int(limit or default)
    except (TypeError, ValueError):
        parsed = default
    return max(1, min(parsed, maximum))


def _table_exists(conn, table_name: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name = ?",
        (table_name,),
    ).fetchone()
    return row is not None


def _scalar(conn, query: str, params: tuple[Any, ...] = (), default: Any = 0) -> Any:
    row = conn.execute(query, params).fetchone()
    if not row:
        return default
    value = row[0]
    return default if value is None else value


def _parse_ts(value: Any) -> datetime | None:
    if not value:
        return None
    raw = str(value).strip()
    try:
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        parsed = datetime.fromisoformat(raw)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except ValueError:
        return None


def _safe_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_json_object(value: Any) -> dict[str, Any]:
    if not value:
        return {}
    if isinstance(value, dict):
        return value
    try:
        parsed = json.loads(str(value))
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _sizing_cap_message(sizing: dict[str, Any]) -> str | None:
    if not sizing:
        return None
    if sizing.get("admin_message"):
        return str(sizing.get("admin_message"))
    if sizing.get("risk_warning"):
        final_margin = _safe_float(sizing.get("final_margin_usdt") or sizing.get("final_margin"))
        risk_pct = _safe_float(sizing.get("theoretical_risk_pct"))
        risk_level = sizing.get("risk_level_label") or sizing.get("risk_level") or "configured"
        account_risk_pct = _safe_float(sizing.get("account_risk_pct"))
        if final_margin is not None and risk_pct is not None and account_risk_pct is not None:
            return (
                f"Your {final_margin:.2f} USDT fixed margin was respected. "
                f"Estimated risk is {risk_pct:.2f}% of equity, above selected {risk_level} "
                f"risk limit {account_risk_pct:.2f}%. Risk cap was NOT applied because "
                "fixed_amount strict mode is active."
            )
    if not sizing.get("cap_applied"):
        return None
    base_margin = _safe_float(sizing.get("base_margin_usdt") or sizing.get("base_margin"))
    final_margin = _safe_float(sizing.get("final_margin_usdt") or sizing.get("final_margin"))
    risk_level = sizing.get("risk_level_label") or sizing.get("risk_level") or "configured"
    account_risk_pct = _safe_float(sizing.get("account_risk_pct"))
    stop_distance_pct = _safe_float(sizing.get("stop_distance_pct"))
    if base_margin is None or final_margin is None:
        return sizing.get("cap_reason")
    risk_text = f"{account_risk_pct:.2f}%" if account_risk_pct is not None else "the configured"
    stop_text = f"{stop_distance_pct:.2f}%" if stop_distance_pct is not None else "this trade's"
    return (
        f"Your {base_margin:.2f} USDT fixed margin was reduced to {final_margin:.2f} USDT "
        f"because {risk_level} risk allows max {risk_text} account risk and this trade's "
        f"ATR stop distance was {stop_text}."
    )


def _sizing_cap_view(row: dict[str, Any]) -> dict[str, Any] | None:
    sizing = _parse_json_object(row.get("sizing_json"))
    if not (
        sizing.get("cap_applied")
        or sizing.get("risk_warning")
        or sizing.get("sizing_method") == "fixed_amount_strict"
    ):
        return None
    return {
        "trace_id": row.get("trace_id"),
        "timestamp_utc": row.get("ts"),
        "symbol": row.get("symbol"),
        "position_id": row.get("position_id"),
        "signal": row.get("signal"),
        "confidence": row.get("confidence"),
        "allocation_type": sizing.get("allocation_type") or sizing.get("allocation_mode"),
        "user_fixed_margin_usdt": sizing.get("user_fixed_margin_usdt"),
        "base_margin_usdt": sizing.get("base_margin_usdt") or sizing.get("base_margin"),
        "final_margin_usdt": sizing.get("final_margin_usdt") or sizing.get("final_margin"),
        "base_notional_usdt": sizing.get("base_notional_usdt"),
        "final_notional_usdt": sizing.get("final_notional_usdt"),
        "leverage": sizing.get("leverage") or sizing.get("leverage_used_for_cap"),
        "cap_applied": bool(sizing.get("cap_applied")),
        "cap_reason": sizing.get("cap_reason"),
        "atr_cap_margin_usdt": sizing.get("atr_cap_margin_usdt") or sizing.get("margin_cap_usdt"),
        "account_risk_pct": sizing.get("account_risk_pct"),
        "stop_distance_pct": sizing.get("stop_distance_pct"),
        "theoretical_risk_usdt": sizing.get("theoretical_risk_usdt"),
        "theoretical_risk_pct": sizing.get("theoretical_risk_pct"),
        "risk_warning": bool(sizing.get("risk_warning")),
        "max_risk_capital": sizing.get("max_risk_capital"),
        "risk_level": sizing.get("risk_level"),
        "risk_level_label": sizing.get("risk_level_label"),
        "sizing_method": sizing.get("sizing_method"),
        "admin_message": _sizing_cap_message(sizing),
    }


def get_bot_overview(db: AdminDB) -> dict[str, Any]:
    latest_run = None
    daily_pnl = None
    active_positions = 0
    recent_events = 0
    today = datetime.now(timezone.utc).date().isoformat()

    with db.connect() as conn:
        if _table_exists(conn, "runs"):
            latest_run = conn.execute(
                """
                SELECT run_id, started_at, stopped_at, status, mode
                FROM runs
                ORDER BY started_at DESC
                LIMIT 1
                """
            ).fetchone()
        if _table_exists(conn, "daily_state"):
            daily_pnl = conn.execute(
                "SELECT realized_pnl, trade_count FROM daily_state WHERE day = ?",
                (today,),
            ).fetchone()
        if _table_exists(conn, "symbol_state"):
            active_positions = _scalar(
                conn,
                "SELECT COUNT(*) FROM symbol_state WHERE position != 'NONE'",
                default=0,
            )
        if _table_exists(conn, "events"):
            recent_events = _scalar(
                conn,
                "SELECT COUNT(*) FROM events WHERE timestamp_utc > datetime('now', '-1 hour')",
                default=0,
            )

    status = "stopped"
    uptime_seconds = 0
    if latest_run and not latest_run["stopped_at"]:
        status = "running"
        started = _parse_ts(latest_run["started_at"])
        if started:
            uptime_seconds = max(0, int((datetime.now(timezone.utc) - started).total_seconds()))

    return {
        "status": status,
        "uptime_seconds": uptime_seconds,
        "active_run_id": latest_run["run_id"] if latest_run else None,
        "active_positions": int(active_positions or 0),
        "daily_pnl": daily_pnl["realized_pnl"] if daily_pnl else 0,
        "daily_trades": daily_pnl["trade_count"] if daily_pnl else 0,
        "recent_events_1h": int(recent_events or 0),
    }


def list_bot_runs(db: AdminDB, *, limit: int = DEFAULT_RUN_LIMIT) -> dict[str, Any]:
    with db.connect() as conn:
        if not _table_exists(conn, "runs"):
            return {"runs": [], "count": 0}
        rows = conn.execute(
            """
            SELECT
                r.run_id,
                r.started_at,
                r.stopped_at,
                r.mode,
                r.status,
                r.config_json,
                rs.cycles,
                rs.trades,
                rs.realized_pnl,
                rs.win_trades,
                rs.loss_trades
            FROM runs r
            LEFT JOIN run_summary rs ON r.run_id = rs.run_id
            ORDER BY r.started_at DESC
            LIMIT ?
            """,
            (_bounded_limit(limit),),
        ).fetchall() if _table_exists(conn, "run_summary") else conn.execute(
            """
            SELECT
                run_id,
                started_at,
                stopped_at,
                mode,
                status,
                config_json,
                NULL AS cycles,
                NULL AS trades,
                NULL AS realized_pnl,
                NULL AS win_trades,
                NULL AS loss_trades
            FROM runs
            ORDER BY started_at DESC
            LIMIT ?
            """,
            (_bounded_limit(limit),),
        ).fetchall()

    runs = [dict(row) for row in rows]
    return {"runs": runs, "count": len(runs)}


def get_bot_run_details(db: AdminDB, run_id: str) -> dict[str, Any] | None:
    with db.connect() as conn:
        if not _table_exists(conn, "runs"):
            return None
        run = conn.execute("SELECT * FROM runs WHERE run_id = ?", (run_id,)).fetchone()
        if not run:
            return None

        summary = (
            conn.execute("SELECT * FROM run_summary WHERE run_id = ?", (run_id,)).fetchone()
            if _table_exists(conn, "run_summary")
            else None
        )
        events = conn.execute(
            """
            SELECT event_type, symbol, action, timestamp_utc, details_json
            FROM events
            WHERE run_id = ?
            ORDER BY timestamp_utc DESC
            LIMIT ?
            """,
            (run_id, EVENT_LIMIT),
        ).fetchall() if _table_exists(conn, "events") else []
        traces = conn.execute(
            """
            SELECT trace_id, symbol, signal, confidence, intended_action,
                   execution_status, final_position, ts
            FROM decision_traces
            WHERE run_id = ?
            ORDER BY ts DESC
            LIMIT ?
            """,
            (run_id, TRACE_LIMIT),
        ).fetchall() if _table_exists(conn, "decision_traces") else []

    return {
        "run": dict(run),
        "summary": dict(summary) if summary else None,
        "events": [dict(row) for row in events],
        "traces": [dict(row) for row in traces],
    }


def get_bot_live_status(db: AdminDB) -> dict[str, Any]:
    with db.connect() as conn:
        positions = conn.execute(
            """
            SELECT symbol, position, entry_price, entry_qty, last_signal,
                   last_action, updated_at
            FROM symbol_state
            WHERE position != 'NONE'
            ORDER BY updated_at DESC
            """
        ).fetchall() if _table_exists(conn, "symbol_state") else []
        latest_decisions = conn.execute(
            """
            SELECT trace_id, symbol, signal, confidence, intended_action,
                   execution_status, ts, sizing_json
            FROM decision_traces
            ORDER BY ts DESC
            LIMIT ?
            """,
            (LIVE_DECISION_LIMIT,),
        ).fetchall() if _table_exists(conn, "decision_traces") else []
        latest_events = conn.execute(
            """
            SELECT event_type, symbol, action, timestamp_utc, details_json
            FROM events
            ORDER BY timestamp_utc DESC
            LIMIT ?
            """,
            (LIVE_EVENT_LIMIT,),
        ).fetchall() if _table_exists(conn, "events") else []

    decision_items: list[dict[str, Any]] = []
    for row in latest_decisions:
        item = dict(row)
        item["sizing_cap_event"] = _sizing_cap_view(item)
        decision_items.append(item)

    return {
        "positions": [dict(row) for row in positions],
        "latest_decisions": decision_items,
        "latest_events": [dict(row) for row in latest_events],
    }
