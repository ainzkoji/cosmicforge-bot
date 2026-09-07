from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from app.persistence.db import AdminDB


SNAPSHOT_TABLES = (
    "admin_profitability_daily_summary",
    "admin_profitability_symbol_summary",
    "admin_profitability_sizing_events",
)


def _table_exists(conn, table_name: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    ).fetchone()
    return row is not None


def _safe_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _pct(numerator: int | float, denominator: int | float) -> float | None:
    if not denominator:
        return None
    return round(float(numerator) * 100.0 / float(denominator), 4)


def _empty_recent_window() -> dict[str, Any]:
    return {
        "closed_trades": 0,
        "total_realized_pnl": 0.0,
        "win_rate_pct": None,
        "average_pnl": None,
        "profit_factor": None,
    }


def _empty_report(generated_at: str, warning: str | None = None) -> dict[str, Any]:
    return {
        "generated_at": generated_at,
        "scope": "snapshot: admin_profitability_*; excludes account_id='backfill' and initiator_type='SHADOW'",
        "snapshot_metadata": {
            "snapshot_source": "admin_profitability_*",
            "snapshot_generated_at": None,
            "snapshot_stale": True,
            "snapshot_warning": warning or "Profitability snapshots have not been generated yet.",
        },
        "overall": {
            "total_fills": 0,
            "total_trades": 0,
            "closed_trades": 0,
            "open_trades": 0,
            "raw_open_fills": 0,
            "raw_close_fills": 0,
            "position_linked_closed_trades": 0,
            "total_realized_pnl": 0.0,
            "win_rate_pct": None,
            "average_win": None,
            "average_loss": None,
            "profit_factor": None,
            "average_r_multiple": None,
            "best_trade": None,
            "worst_trade": None,
        },
        "per_symbol": [],
        "recent": {
            "last_24h": _empty_recent_window(),
            "last_48h": _empty_recent_window(),
            "last_7d": _empty_recent_window(),
        },
        "risk_execution_quality": _empty_risk_quality(),
        "sizing_cap_events": [],
    }


def _empty_risk_quality() -> dict[str, Any]:
    return {
        "duplicate_order_id_action_symbol_groups": 0,
        "duplicate_exact_fill_groups": 0,
        "missing_run_id_count": 0,
        "missing_position_id_count": 0,
        "closed_fills_null_exit_reason": 0,
        "closed_fills_null_r_multiple": 0,
        "average_slippage_pct": None,
        "biggest_abs_slippage_pct": None,
        "biggest_slippage_fill": None,
    }


def _row_value(row: Any, key: str, default: Any = None) -> Any:
    if not row:
        return default
    return row[key] if key in row.keys() else default


def _latest_snapshot_timestamp(daily_rows: list[Any], symbol_rows: list[Any], sizing_rows: list[Any]) -> str | None:
    candidates: list[str] = []
    for row in daily_rows:
        value = _row_value(row, "updated_at") or _row_value(row, "created_at")
        if value:
            candidates.append(str(value))
    for row in symbol_rows:
        value = _row_value(row, "updated_at") or _row_value(row, "created_at")
        if value:
            candidates.append(str(value))
    for row in sizing_rows:
        value = _row_value(row, "created_at")
        if value:
            candidates.append(str(value))
    return max(candidates) if candidates else None


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    raw = value.strip()
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _snapshot_stale(latest_timestamp: str | None, now: datetime) -> bool:
    latest = _parse_iso(latest_timestamp)
    if latest is None:
        return True
    return latest < now - timedelta(hours=24)


def _recent_from_daily(daily_rows: list[Any], *, now: datetime, delta: timedelta) -> dict[str, Any]:
    cutoff_date = (now - delta).date().isoformat()
    rows = [row for row in daily_rows if str(row["date"] or "") >= cutoff_date]
    closed = sum(int(row["closed_trades"] or 0) for row in rows)
    wins = sum(int(row["winning_trades"] or 0) for row in rows)
    losses = sum(int(row["losing_trades"] or 0) for row in rows)
    pnl = sum(float(row["total_realized_pnl"] or 0.0) for row in rows)
    return {
        "closed_trades": closed,
        "total_realized_pnl": pnl,
        "win_rate_pct": _pct(wins, wins + losses),
        "average_pnl": (pnl / closed) if closed else None,
        "profit_factor": None,
    }


def _sizing_event_view(row: Any) -> dict[str, Any]:
    return {
        "trace_id": _row_value(row, "trace_id"),
        "timestamp_utc": _row_value(row, "ts"),
        "symbol": _row_value(row, "symbol"),
        "position_id": None,
        "signal": None,
        "confidence": None,
        "allocation_type": _row_value(row, "sizing_method"),
        "user_fixed_margin_usdt": _safe_float(_row_value(row, "configured_margin")),
        "base_margin_usdt": _safe_float(_row_value(row, "configured_margin")),
        "final_margin_usdt": _safe_float(_row_value(row, "final_margin")),
        "base_notional_usdt": _safe_float(_row_value(row, "base_notional")),
        "final_notional_usdt": _safe_float(_row_value(row, "final_notional")),
        "leverage": _safe_float(_row_value(row, "leverage")),
        "cap_applied": bool(_row_value(row, "cap_applied", 0)),
        "cap_reason": _row_value(row, "explanation"),
        "atr_cap_margin_usdt": None,
        "account_risk_pct": _safe_float(_row_value(row, "risk_cap_pct")),
        "stop_distance_pct": _safe_float(_row_value(row, "atr_stop_distance_pct")),
        "theoretical_risk_usdt": None,
        "theoretical_risk_pct": _safe_float(_row_value(row, "risk_cap_pct")),
        "risk_warning": False,
        "max_risk_capital": None,
        "risk_level": None,
        "risk_level_label": None,
        "sizing_method": _row_value(row, "sizing_method"),
        "admin_message": _row_value(row, "explanation"),
    }


def get_profitability_report(db: AdminDB) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    generated_at = now.isoformat()

    with db.connect() as conn:
        missing_tables = [table for table in SNAPSHOT_TABLES if not _table_exists(conn, table)]
        if missing_tables:
            return _empty_report(
                generated_at,
                warning=f"Profitability snapshot tables are missing: {', '.join(missing_tables)}.",
            )

        daily_rows = conn.execute(
            """
            SELECT *
            FROM admin_profitability_daily_summary
            WHERE account_scope = 'live'
            ORDER BY date ASC
            """
        ).fetchall()
        symbol_rows = conn.execute(
            """
            SELECT *
            FROM admin_profitability_symbol_summary
            WHERE account_scope = 'live'
            ORDER BY total_realized_pnl DESC
            """
        ).fetchall()
        sizing_rows = conn.execute(
            """
            SELECT *
            FROM admin_profitability_sizing_events
            ORDER BY ts DESC
            LIMIT 25
            """
        ).fetchall()

    if not daily_rows and not symbol_rows and not sizing_rows:
        return _empty_report(generated_at)

    total_fills = sum(int(row["fills_count"] or 0) for row in daily_rows)
    closed_trades = sum(int(row["closed_trades"] or 0) for row in daily_rows)
    winning_trades = sum(int(row["winning_trades"] or 0) for row in daily_rows)
    losing_trades = sum(int(row["losing_trades"] or 0) for row in daily_rows)
    total_realized_pnl = sum(float(row["total_realized_pnl"] or 0.0) for row in daily_rows)
    weighted_r_sum = sum(
        float(row["avg_r_multiple"] or 0.0) * int(row["closed_trades"] or 0)
        for row in daily_rows
        if row["avg_r_multiple"] is not None
    )
    weighted_r_count = sum(
        int(row["closed_trades"] or 0)
        for row in daily_rows
        if row["avg_r_multiple"] is not None
    )

    per_symbol = [
        {
            "symbol": row["symbol"] or "UNKNOWN",
            "trades": int(row["closed_trades"] or 0),
            "win_rate_pct": _safe_float(row["win_rate"]),
            "total_pnl": float(row["total_realized_pnl"] or 0.0),
            "average_pnl": _safe_float(row["avg_pnl"]),
            "average_r_multiple": _safe_float(row["avg_r_multiple"]),
            "sl_count": int(row["sl_count"] or 0),
            "tp_count": int(row["tp_count"] or 0),
            "time_exit_count": int(row["time_exit_count"] or 0),
            "other_count": int(row["other_exit_count"] or 0),
        }
        for row in symbol_rows
    ]

    latest_snapshot = _latest_snapshot_timestamp(daily_rows, symbol_rows, sizing_rows)
    warning = None
    if not daily_rows or not symbol_rows:
        warning = "Profitability snapshots are partially populated; some fields use safe defaults."

    return {
        "generated_at": generated_at,
        "scope": "snapshot: admin_profitability_*; excludes account_id='backfill' and initiator_type='SHADOW'",
        "snapshot_metadata": {
            "snapshot_source": "admin_profitability_*",
            "snapshot_generated_at": latest_snapshot,
            "snapshot_stale": _snapshot_stale(latest_snapshot, now),
            "snapshot_warning": warning,
        },
        "overall": {
            "total_fills": total_fills,
            "total_trades": closed_trades,
            "closed_trades": closed_trades,
            "open_trades": 0,
            "raw_open_fills": 0,
            "raw_close_fills": closed_trades,
            "position_linked_closed_trades": 0,
            "total_realized_pnl": total_realized_pnl,
            "win_rate_pct": _pct(winning_trades, winning_trades + losing_trades),
            "average_win": None,
            "average_loss": None,
            "profit_factor": None,
            "average_r_multiple": (weighted_r_sum / weighted_r_count) if weighted_r_count else None,
            "best_trade": None,
            "worst_trade": None,
        },
        "per_symbol": per_symbol,
        "recent": {
            "last_24h": _recent_from_daily(daily_rows, now=now, delta=timedelta(hours=24)),
            "last_48h": _recent_from_daily(daily_rows, now=now, delta=timedelta(hours=48)),
            "last_7d": _recent_from_daily(daily_rows, now=now, delta=timedelta(days=7)),
        },
        "risk_execution_quality": _empty_risk_quality(),
        "sizing_cap_events": [_sizing_event_view(row) for row in sizing_rows],
    }
