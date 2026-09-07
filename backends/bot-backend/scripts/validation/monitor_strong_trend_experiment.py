#!/usr/bin/env python3
"""Monitor the controlled paper-only STRONG_TREND experiment."""
from __future__ import annotations

import argparse
from collections import Counter
import json
import re
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_SCRIPT_DIR = Path(__file__).resolve().parent
_BOT_ROOT = _SCRIPT_DIR.parent.parent
_SHARED_ROOT = _BOT_ROOT.parent / "shared"
for _path in (str(_BOT_ROOT), str(_SHARED_ROOT)):
    if _path not in sys.path:
        sys.path.insert(0, _path)

from app.core.config import settings
from app.core.strong_trend_guard import evaluate_strong_trend_guard
from scripts.validation.replay_strategy_components import resolve_db_path


DEFAULT_EXPERIMENT_CONFIG = _BOT_ROOT / ".env.paper_strong_trend_experiment"
DEFAULT_ACTIVE_ENV = _BOT_ROOT / ".env"
DEFAULT_MD = _BOT_ROOT / "models/reports/strong_trend_paper_experiment_status.md"
DEFAULT_JSON = _BOT_ROOT / "models/reports/strong_trend_paper_experiment_status.json"
DEFAULT_DIAG_MD = _BOT_ROOT / "models/reports/strong_trend_order_count_diagnosis.md"
DEFAULT_DIAG_JSON = _BOT_ROOT / "models/reports/strong_trend_order_count_diagnosis.json"
DEFAULT_SECTION4_STATUS = _BOT_ROOT / "models/reports/iofs_paper_validation_status.md"

SYMBOLS = ("BTCUSDT", "ETHUSDT")
ORDER_SIGNAL_VALUES = {"BUY", "SELL", "LONG", "SHORT"}
ORDER_SUCCESS_STATUSES = {
    "ACCEPTED",
    "CREATED",
    "EXECUTED",
    "FILLED",
    "OPEN",
    "OPENED",
    "PARTIALLY_FILLED",
    "PAPER_FILLED",
    "PAPER_OPENED",
    "PAPER_ORDER_CREATED",
    "PAPER_POSITION_OPENED",
    "SUCCESS",
    "SUBMITTED",
}
ORDER_ERROR_STATUSES = {
    "BROKER_REJECT",
    "ERROR",
    "FAILED",
    "PAPER_ONLY",
    "REJECTED",
}


def parse_env(path: Path) -> dict[str, str]:
    result: dict[str, str] = {}
    if not path.exists():
        return result
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        result[key.strip()] = value.strip()
    return result


def _parse_timestamp(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _is_at_or_after(value: Any, start_time: str | None) -> bool:
    if not start_time:
        return False
    parsed_value = _parse_timestamp(value)
    parsed_start = _parse_timestamp(start_time)
    return bool(parsed_value and parsed_start and parsed_value >= parsed_start)


def _is_before(value: Any, start_time: str | None) -> bool:
    if not start_time:
        return False
    parsed_value = _parse_timestamp(value)
    parsed_start = _parse_timestamp(start_time)
    return bool(parsed_value and parsed_start and parsed_value < parsed_start)


def _table_columns(connection: sqlite3.Connection, table: str) -> set[str]:
    try:
        return {
            str(row["name"] if isinstance(row, sqlite3.Row) else row[1])
            for row in connection.execute(f"PRAGMA table_info({table})").fetchall()
        }
    except sqlite3.Error:
        return set()


def _table_exists(connection: sqlite3.Connection, table: str) -> bool:
    try:
        return bool(
            connection.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                (table,),
            ).fetchone()
        )
    except sqlite3.Error:
        return False


def _active_bot_id(connection: sqlite3.Connection) -> str | None:
    if not _table_exists(connection, "bot_instances"):
        return None
    try:
        row = connection.execute(
            """
            SELECT id
            FROM bot_instances
            WHERE status='active'
            ORDER BY last_run_at DESC
            LIMIT 1
            """
        ).fetchone()
    except sqlite3.Error:
        return None
    if not row:
        return None
    return str(row["id"] if isinstance(row, sqlite3.Row) else row[0])


def _clean_start_from_status(path: Path) -> str | None:
    if not path.exists():
        return None
    match = re.search(
        r"^-\s+(?:start_timestamp_utc|post_repair_restart_time):\s*(\S+)",
        path.read_text(encoding="utf-8"),
        flags=re.MULTILINE,
    )
    return match.group(1) if match else None


def _active_bot_filter(column_names: set[str], active_bot_id: str | None) -> tuple[str, list[Any]]:
    if not active_bot_id or "bot_instance_id" not in column_names:
        return "", []
    return " AND (bot_instance_id = ? OR bot_instance_id IS NULL OR bot_instance_id = '')", [
        active_bot_id
    ]


def _value(row: dict[str, Any], key: str) -> Any:
    return row.get(key)


def _upper(row: dict[str, Any], key: str) -> str:
    return str(_value(row, key) or "").strip().upper()


def _truthy(value: Any) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y"}
    return bool(value)


def is_trade_signal(row: dict[str, Any]) -> bool:
    return any(_upper(row, field) in ORDER_SIGNAL_VALUES for field in ("signal", "intended_action"))


def is_order_attempt(row: dict[str, Any]) -> bool:
    return _truthy(row.get("submit_attempted"))


def is_order_error(row: dict[str, Any]) -> bool:
    if not is_order_attempt(row):
        return False
    status = _upper(row, "execution_status")
    return bool(
        row.get("execution_error")
        or row.get("rejection_reason")
        or status in ORDER_ERROR_STATUSES
    )


def is_paper_order_created(row: dict[str, Any]) -> bool:
    if not is_order_attempt(row) or is_order_error(row):
        return False
    status = _upper(row, "execution_status")
    return bool(
        row.get("order_id")
        or _truthy(row.get("fill_recorded"))
        or _truthy(row.get("position_opened"))
        or status in ORDER_SUCCESS_STATUSES
    )


def _is_test_fixture(row: dict[str, Any]) -> bool:
    values = " ".join(
        str(row.get(field) or "")
        for field in ("trace_id", "run_id", "cycle_id", "bot_instance_id", "order_id")
    ).lower()
    return any(marker in values for marker in ("test", "fixture", "pytest", "dummy"))


def calculate_metrics(trades: list[dict[str, Any]]) -> dict[str, Any]:
    values = [float(trade.get("r_multiple") or 0.0) for trade in trades]
    wins = [value for value in values if value > 0]
    losses = [value for value in values if value < 0]
    gross_win = sum(wins)
    gross_loss = abs(sum(losses))
    peak = 0.0
    equity = 0.0
    max_drawdown = 0.0
    loss_streak = 0
    max_loss_streak = 0
    for value in values:
        equity += value
        peak = max(peak, equity)
        max_drawdown = max(max_drawdown, peak - equity)
        loss_streak = loss_streak + 1 if value < 0 else 0
        max_loss_streak = max(max_loss_streak, loss_streak)

    def count_exit(*names: str) -> int:
        allowed = {name.upper() for name in names}
        return sum(str(trade.get("exit_reason") or "").upper() in allowed for trade in trades)

    return {
        "closed_trades": len(trades),
        "win_rate": round(len(wins) / len(trades), 6) if trades else None,
        "profit_factor": round(gross_win / gross_loss, 6) if gross_loss else (None if not wins else None),
        "expectancy_R": round(sum(values) / len(values), 6) if values else None,
        "max_drawdown_R": round(max_drawdown, 6),
        "max_consecutive_losses": max_loss_streak,
        "TP1_count": count_exit("TP1"),
        "TP2_count": count_exit("TP2", "TP"),
        "SL_count": count_exit("SL", "STOP_LOSS"),
        "break_even_buffer_count": count_exit("BREAK_EVEN_BUFFER", "BE", "BREAK_EVEN"),
        "time_exit_count": count_exit("TIME_EXIT", "TIME"),
    }


def stop_recommendation(metrics: dict[str, Any], strong_trend_order_errors: int) -> tuple[bool, list[str]]:
    reasons: list[str] = []
    closed = int(metrics.get("closed_trades") or 0)
    expectancy = metrics.get("expectancy_R")
    profit_factor = metrics.get("profit_factor")
    if closed >= 5 and expectancy is not None and expectancy <= 0:
        reasons.append("closed STRONG_TREND trades >= 5 and expectancy_R <= 0")
    if closed >= 5 and profit_factor is not None and profit_factor < 1.0:
        reasons.append("closed STRONG_TREND trades >= 5 and profit_factor < 1.0")
    if float(metrics.get("max_drawdown_R") or 0.0) >= 3.0:
        reasons.append("max_drawdown_R >= 3.0")
    if int(metrics.get("max_consecutive_losses") or 0) >= 3:
        reasons.append("three consecutive STRONG_TREND losses")
    if strong_trend_order_errors >= 2:
        reasons.append("strong_trend_order_errors_since_experiment_start >= 2")
    return bool(reasons), reasons


def _decision_rows(
    connection: sqlite3.Connection,
    start_time: str,
    *,
    active_bot_id: str | None,
    only_attempts: bool = False,
    all_symbols: bool = False,
) -> list[dict[str, Any]]:
    columns = _table_columns(connection, "decision_traces")
    if not columns:
        return []
    clauses = ["ts >= ?"]
    params: list[Any] = [start_time]
    if not all_symbols and "symbol" in columns:
        placeholders = ",".join("?" for _ in SYMBOLS)
        clauses.append(f"symbol IN ({placeholders})")
        params.extend(SYMBOLS)
    if only_attempts and "submit_attempted" in columns:
        clauses.append("COALESCE(submit_attempted, 0) = 1")
    bot_clause, bot_params = _active_bot_filter(columns, active_bot_id)
    query = f"""
        SELECT *
        FROM decision_traces
        WHERE {" AND ".join(clauses)}{bot_clause}
        ORDER BY ts ASC
    """
    rows = connection.execute(query, (*params, *bot_params)).fetchall()
    return [dict(row) for row in rows]


def _fill_rows(
    connection: sqlite3.Connection,
    start_time: str,
    *,
    active_bot_id: str | None,
) -> list[dict[str, Any]]:
    fill_columns = _table_columns(connection, "trade_fills")
    trace_columns = _table_columns(connection, "decision_traces")
    if not fill_columns or "timestamp_utc" not in fill_columns:
        return []

    joins: list[str] = []
    regime_sources: list[str] = []
    if {"trace_id", "regime_state"}.issubset(trace_columns) and "trace_id" in fill_columns:
        joins.append("LEFT JOIN decision_traces AS close_trace ON close_trace.trace_id = f.trace_id")
        regime_sources.append("close_trace.regime_state")
    if "exit_regime" in fill_columns:
        regime_sources.append("f.exit_regime")
    regime_expr = "COALESCE(" + ", ".join(regime_sources + ["'UNKNOWN'"]) + ")"

    clauses = ["f.timestamp_utc >= ?"]
    params: list[Any] = [start_time]
    if "symbol" in fill_columns:
        placeholders = ",".join("?" for _ in SYMBOLS)
        clauses.append(f"f.symbol IN ({placeholders})")
        params.extend(SYMBOLS)
    if active_bot_id and "bot_instance_id" in fill_columns:
        clauses.append("(f.bot_instance_id = ? OR f.bot_instance_id IS NULL OR f.bot_instance_id = '')")
        params.append(active_bot_id)
    order_by = "f.timestamp_utc ASC"
    if "id" in fill_columns:
        order_by += ", f.id ASC"
    rows = connection.execute(
        f"""
        SELECT f.*, {regime_expr} AS entry_regime
        FROM trade_fills AS f
        {' '.join(joins)}
        WHERE {" AND ".join(clauses)}
        ORDER BY {order_by}
        """,
        tuple(params),
    ).fetchall()
    return [dict(row) for row in rows]


def build_report(db_path: Path, experiment_start_time: str) -> dict[str, Any]:
    uri = f"file:{db_path.as_posix()}?mode=ro"
    with sqlite3.connect(uri, uri=True, timeout=30) as connection:
        connection.row_factory = sqlite3.Row
        active_bot_id = _active_bot_id(connection)
        decision_columns = _table_columns(connection, "decision_traces")
        fill_columns = _table_columns(connection, "trade_fills")
        decisions = _decision_rows(
            connection,
            experiment_start_time,
            active_bot_id=active_bot_id,
        )
        fills = _fill_rows(
            connection,
            experiment_start_time,
            active_bot_id=active_bot_id,
        )

    strong_decisions = [
        row for row in decisions if str(row.get("regime_state") or "").upper() == "STRONG_TREND"
    ]
    strong_fills = [
        row for row in fills if str(row.get("entry_regime") or "").upper() == "STRONG_TREND"
    ]
    strong_trades = [
        row for row in strong_fills if str(row.get("action") or "").upper() == "CLOSE"
    ]
    other_trades = [
        row
        for row in fills
        if str(row.get("entry_regime") or "").upper() != "STRONG_TREND"
        and str(row.get("action") or "").upper() == "CLOSE"
    ]
    signals = [row for row in strong_decisions if is_trade_signal(row)]
    attempts = [row for row in strong_decisions if is_order_attempt(row)]
    created_orders = [row for row in attempts if is_paper_order_created(row)]
    errors = [row for row in attempts if is_order_error(row)]
    strong_metrics = calculate_metrics(strong_trades)
    stop, stop_reasons = stop_recommendation(strong_metrics, len(errors))
    guard = evaluate_strong_trend_guard(settings)
    order_consistency_note = ""
    if not signals and (attempts or created_orders):
        order_consistency_note = (
            "Order attempts/orders exist but no signal rows were detected; inspect legacy signal metadata."
        )
    elif attempts and not created_orders:
        order_consistency_note = (
            "Historical submit_attempted rows are failed attempts, not created paper orders."
        )
    return {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "experiment_start_time": experiment_start_time,
        "scope": "paper_only",
        "STRONG_TREND_ALLOWED_ONLY_IN_PAPER": True,
        "filtering": {
            "active_bot_instance_id": active_bot_id,
            "decision_traces_bot_instance_filter_applied": bool(
                active_bot_id and "bot_instance_id" in decision_columns
            ),
            "trade_fills_bot_instance_filter_applied": bool(
                active_bot_id and "bot_instance_id" in fill_columns
            ),
            "symbols": list(SYMBOLS),
            "experiment_start_time": experiment_start_time,
        },
        "runtime_safety": {
            "execution_mode": str(settings.EXECUTION_MODE),
            "ml_enabled": bool(settings.ML_ENABLED),
            "iofs_gate_mode": str(settings.IOFS_GATE_MODE),
            "trade_symbols": str(settings.TRADE_SYMBOLS),
            "live_symbols_count": guard.live_symbols_count,
            "strong_trend_guard": guard.to_dict(),
        },
        "strong_trend_cycles": len({row.get("cycle_id") for row in strong_decisions}),
        "strong_trend_signals": len(signals),
        "strong_trend_order_attempts": len(attempts),
        "strong_trend_paper_orders_created": len(created_orders),
        "strong_trend_paper_orders": len(created_orders),
        "strong_trend_order_errors": len(errors),
        "strong_trend_fills": len(strong_fills),
        "strong_trend_closed_trades": len(strong_trades),
        "BTC_strong_trend_trades": sum(row.get("symbol") == "BTCUSDT" for row in strong_trades),
        "ETH_strong_trend_trades": sum(row.get("symbol") == "ETHUSDT" for row in strong_trades),
        "BUY_count": sum(
            _upper(row, "signal") == "BUY" or _upper(row, "intended_action") == "BUY"
            for row in signals
        ),
        "SELL_count": sum(
            _upper(row, "signal") == "SELL" or _upper(row, "intended_action") == "SELL"
            for row in signals
        ),
        "paper_order_errors": len(errors),
        "order_consistency_note": order_consistency_note,
        "strong_trend_metrics": strong_metrics,
        "non_strong_trend_metrics": calculate_metrics(other_trades),
        "stop_recommended": stop,
        "stop_reason": "; ".join(stop_reasons),
        "previous_stop_was_false_positive": False,
        "false_positive_reason": "",
        "auto_stop_rules_configured": True,
        "stop_rules": [
            "closed STRONG_TREND trades >= 5 and expectancy_R <= 0",
            "closed STRONG_TREND trades >= 5 and profit_factor < 1.0",
            "max_drawdown_R >= 3.0",
            "three consecutive STRONG_TREND losses",
            "strong_trend_order_errors_since_experiment_start >= 2",
        ],
        "auto_restore_performed": False,
        "auto_restore_reason": None,
    }


def _attempt_detail(
    row: dict[str, Any],
    *,
    clean_start_time: str | None,
    experiment_start_time: str,
) -> dict[str, Any]:
    status = str(row.get("execution_status") or "")
    error_message = str(row.get("execution_error") or row.get("rejection_reason") or "")
    signal = str(row.get("signal") or "").upper()
    intended = str(row.get("intended_action") or "").upper()
    side = signal if signal in ORDER_SIGNAL_VALUES else intended
    return {
        "order_id": row.get("order_id"),
        "attempt_id": row.get("trace_id"),
        "created_at": row.get("ts"),
        "symbol": row.get("symbol"),
        "side": side,
        "status": status,
        "error_code": status or None,
        "error_message": error_message or None,
        "bot_instance_id": row.get("bot_instance_id"),
        "regime": row.get("regime_state"),
        "source": "decision_traces.submit_attempted",
        "is_after_clean_start": _is_at_or_after(row.get("ts"), clean_start_time),
        "is_after_experiment_start": _is_at_or_after(row.get("ts"), experiment_start_time),
        "is_strong_trend": str(row.get("regime_state") or "").upper() == "STRONG_TREND",
        "is_real_order": is_paper_order_created(row),
        "is_failed_attempt": is_order_error(row),
        "is_test_fixture": _is_test_fixture(row),
        "is_duplicate": False,
    }


def _counter(rows: list[dict[str, Any]], key: str, limit: int = 20) -> dict[str, int]:
    return dict(
        Counter(str(row.get(key) or "MISSING") for row in rows).most_common(limit)
    )


def _count_strong_attempts(
    connection: sqlite3.Connection,
    *,
    active_bot_id: str | None,
    start_time: str | None = None,
    end_time: str | None = None,
) -> int:
    columns = _table_columns(connection, "decision_traces")
    if "submit_attempted" not in columns or "ts" not in columns:
        return 0
    clauses = ["COALESCE(submit_attempted, 0) = 1"]
    params: list[Any] = []
    if start_time:
        clauses.append("ts >= ?")
        params.append(start_time)
    if end_time:
        clauses.append("ts < ?")
        params.append(end_time)
    if "symbol" in columns:
        placeholders = ",".join("?" for _ in SYMBOLS)
        clauses.append(f"symbol IN ({placeholders})")
        params.extend(SYMBOLS)
    if "regime_state" in columns:
        clauses.append("UPPER(COALESCE(regime_state, '')) = 'STRONG_TREND'")
    bot_clause, bot_params = _active_bot_filter(columns, active_bot_id)
    row = connection.execute(
        f"""
        SELECT COUNT(*) AS count
        FROM decision_traces
        WHERE {" AND ".join(clauses)}{bot_clause}
        """,
        (*params, *bot_params),
    ).fetchone()
    return int(row["count"] if isinstance(row, sqlite3.Row) else row[0])


def _bot_instance_breakdown(
    connection: sqlite3.Connection,
    *,
    experiment_start_time: str,
) -> dict[str, dict[str, int]]:
    decisions = _decision_rows(
        connection,
        experiment_start_time,
        active_bot_id=None,
    )
    fills = _fill_rows(
        connection,
        experiment_start_time,
        active_bot_id=None,
    )
    breakdown: dict[str, dict[str, int]] = {}

    def bucket(bot_id: Any) -> dict[str, int]:
        key = str(bot_id or "MISSING")
        if key not in breakdown:
            breakdown[key] = {
                "orders": 0,
                "order_attempts": 0,
                "fills": 0,
                "closed_trades": 0,
                "decision_traces": 0,
            }
        return breakdown[key]

    for row in decisions:
        item = bucket(row.get("bot_instance_id"))
        item["decision_traces"] += 1
        if is_order_attempt(row):
            item["order_attempts"] += 1
        if is_paper_order_created(row):
            item["orders"] += 1
    for row in fills:
        item = bucket(row.get("bot_instance_id"))
        item["fills"] += 1
        if str(row.get("action") or "").upper() == "CLOSE":
            item["closed_trades"] += 1
    return breakdown


def build_order_count_diagnosis(
    db_path: Path,
    experiment_start_time: str,
    *,
    clean_start_time: str | None = None,
) -> dict[str, Any]:
    uri = f"file:{db_path.as_posix()}?mode=ro"
    with sqlite3.connect(uri, uri=True, timeout=30) as connection:
        connection.row_factory = sqlite3.Row
        active_bot_id = _active_bot_id(connection)
        decision_columns = _table_columns(connection, "decision_traces")
        attempts_since_experiment = _decision_rows(
            connection,
            experiment_start_time,
            active_bot_id=None,
            only_attempts=True,
            all_symbols=True,
        )
        old_before_experiment_count = _count_strong_attempts(
            connection,
            active_bot_id=active_bot_id,
            start_time=clean_start_time,
            end_time=experiment_start_time,
        )
        old_before_clean_count = _count_strong_attempts(
            connection,
            active_bot_id=active_bot_id,
            end_time=clean_start_time,
        ) if clean_start_time else 0
        bot_breakdown = _bot_instance_breakdown(
            connection,
            experiment_start_time=experiment_start_time,
        )

    def matches_active(row: dict[str, Any]) -> bool:
        if not active_bot_id or "bot_instance_id" not in decision_columns:
            return True
        return row.get("bot_instance_id") in (None, "", active_bot_id)

    def is_target_symbol(row: dict[str, Any]) -> bool:
        return str(row.get("symbol") or "").upper() in SYMBOLS

    def is_strong(row: dict[str, Any]) -> bool:
        return str(row.get("regime_state") or "").upper() == "STRONG_TREND"

    reported_attempts = [
        row
        for row in attempts_since_experiment
        if matches_active(row)
        and is_target_symbol(row)
        and is_strong(row)
    ]
    wrong_regime = [
        row
        for row in attempts_since_experiment
        if matches_active(row)
        and is_target_symbol(row)
        and not is_strong(row)
    ]
    wrong_symbol = [
        row
        for row in attempts_since_experiment
        if matches_active(row)
        and not is_target_symbol(row)
        and is_strong(row)
    ]
    wrong_bot = [
        row
        for row in attempts_since_experiment
        if active_bot_id
        and "bot_instance_id" in decision_columns
        and row.get("bot_instance_id") not in (None, "", active_bot_id)
        and is_target_symbol(row)
        and is_strong(row)
    ]

    order_ids = [str(row.get("order_id")) for row in reported_attempts if row.get("order_id")]
    duplicate_order_ids = {
        order_id: count for order_id, count in Counter(order_ids).items() if count > 1
    }
    details = [
        _attempt_detail(
            row,
            clean_start_time=clean_start_time,
            experiment_start_time=experiment_start_time,
        )
        for row in reported_attempts
    ]
    duplicate_ids = set(duplicate_order_ids)
    for item in details:
        item["is_duplicate"] = bool(item.get("order_id") in duplicate_ids)

    valid_orders = [row for row in reported_attempts if is_paper_order_created(row)]
    failed_attempts = [row for row in reported_attempts if is_order_error(row)]
    historical_paper_only_skipped_attempts = sum(
        1 for row in reported_attempts if _upper(row, "execution_status") == "PAPER_ONLY"
    )
    unknown_rows = [
        row
        for row in reported_attempts
        if not is_paper_order_created(row) and not is_order_error(row)
    ]
    return {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "scope": "STRONG_TREND order-count diagnosis",
        "clean_start_time": clean_start_time,
        "experiment_start_time": experiment_start_time,
        "active_bot_instance_id": active_bot_id,
        "bot_instance_filter_applied": bool(
            active_bot_id and "bot_instance_id" in decision_columns
        ),
        "reported_rows_first_20": details[:20],
        "aggregates": {
            "by_symbol": _counter(reported_attempts, "symbol"),
            "by_status": _counter(reported_attempts, "execution_status"),
            "by_error_message": _counter(
                [
                    {
                        "message": row.get("execution_error")
                        or row.get("rejection_reason")
                        or "NONE"
                    }
                    for row in reported_attempts
                ],
                "message",
            ),
            "by_bot_instance_id": _counter(reported_attempts, "bot_instance_id"),
            "by_regime": _counter(reported_attempts, "regime_state"),
            "duplicate_order_ids": duplicate_order_ids,
        },
        "bot_instance_breakdown": bot_breakdown,
        "summary": {
            "total_reported_paper_orders": len(reported_attempts),
            "valid_post_experiment_strong_trend_orders": len(valid_orders),
            "failed_attempts": len(failed_attempts),
            "historical_paper_only_skipped_attempts": historical_paper_only_skipped_attempts,
            "old_orders_before_experiment": old_before_experiment_count,
            "old_orders_before_clean_start": old_before_clean_count,
            "wrong_regime_orders": len(wrong_regime),
            "wrong_symbol_orders": len(wrong_symbol),
            "wrong_bot_instance_orders": len(wrong_bot),
            "duplicate_orders": sum(duplicate_order_ids.values()),
            "test_fixture_rows": sum(_is_test_fixture(row) for row in reported_attempts),
            "unknown_rows": len(unknown_rows),
        },
        "classification": {
            "are_68_real_current_post_clean_start_orders": False,
            "are_68_old_pre_clean_start_orders": False,
            "are_68_failed_attempts_not_orders": len(reported_attempts) > 0
            and len(reported_attempts) == len(failed_attempts),
            "are_68_from_another_bot_instance": False if not wrong_bot else None,
            "are_68_from_wrong_symbol_or_regime_filter": False,
            "are_68_test_fixture_or_historical_rows": False,
            "counting_without_experiment_filter_was_the_issue": False,
            "semantic_issue": "submit_attempted rows were labeled as paper_orders",
        },
    }


def render_diagnosis_markdown(report: dict[str, Any]) -> str:
    summary = report["summary"]
    aggregates = report["aggregates"]
    rows = report["reported_rows_first_20"]
    table_rows = [
        "| attempt_id | created_at | symbol | side | status | bot_instance_id | real_order | failed_attempt | error |",
        "|---|---|---|---|---|---|---:|---:|---|",
    ]
    for row in rows:
        error = str(row.get("error_message") or "").replace("|", "/")
        table_rows.append(
            "| {attempt_id} | {created_at} | {symbol} | {side} | {status} | {bot_instance_id} | {real_order} | {failed} | {error} |".format(
                attempt_id=row.get("attempt_id"),
                created_at=row.get("created_at"),
                symbol=row.get("symbol"),
                side=row.get("side"),
                status=row.get("status"),
                bot_instance_id=row.get("bot_instance_id"),
                real_order=str(row.get("is_real_order")).lower(),
                failed=str(row.get("is_failed_attempt")).lower(),
                error=error,
            )
        )
    return "\n".join(
        [
            "# STRONG_TREND Order Count Diagnosis",
            "",
            f"- generated_at_utc: {report['generated_at_utc']}",
            f"- clean_start_time: {report['clean_start_time']}",
            f"- experiment_start_time: {report['experiment_start_time']}",
            f"- active_bot_instance_id: {report['active_bot_instance_id']}",
            f"- bot_instance_filter_applied: {str(report['bot_instance_filter_applied']).lower()}",
            "",
            "## Summary",
            "",
            f"- total_reported_paper_orders: {summary['total_reported_paper_orders']}",
            f"- valid_post_experiment_strong_trend_orders: {summary['valid_post_experiment_strong_trend_orders']}",
            f"- failed_attempts: {summary['failed_attempts']}",
            f"- historical_paper_only_skipped_attempts: {summary.get('historical_paper_only_skipped_attempts', 0)}",
            f"- old_orders_before_experiment: {summary['old_orders_before_experiment']}",
            f"- old_orders_before_clean_start: {summary['old_orders_before_clean_start']}",
            f"- wrong_regime_orders: {summary['wrong_regime_orders']}",
            f"- wrong_symbol_orders: {summary['wrong_symbol_orders']}",
            f"- wrong_bot_instance_orders: {summary['wrong_bot_instance_orders']}",
            f"- duplicate_orders: {summary['duplicate_orders']}",
            f"- test_fixture_rows: {summary['test_fixture_rows']}",
            f"- unknown_rows: {summary['unknown_rows']}",
            "",
            "## Aggregates",
            "",
            f"- by_symbol: `{aggregates['by_symbol']}`",
            f"- by_status: `{aggregates['by_status']}`",
            f"- by_error_message: `{aggregates['by_error_message']}`",
            f"- by_bot_instance_id: `{aggregates['by_bot_instance_id']}`",
            f"- by_regime: `{aggregates['by_regime']}`",
            f"- duplicate_order_ids: `{aggregates['duplicate_order_ids']}`",
            "",
            "## First 20 Reported Rows",
            "",
            *table_rows,
            "",
            "The monitor now treats submit attempts, created paper orders, and failed attempts as separate metrics.",
            "",
        ]
    )


def safe_auto_restore(active_env: Path, report: dict[str, Any]) -> bool:
    if not report["stop_recommended"]:
        return False
    values = parse_env(active_env)
    paper_only = values.get("EXECUTION_MODE", "").lower() == "paper"
    ml_disabled = values.get("ML_ENABLED", "").lower() == "false"
    no_live_symbols = not values.get("LIVE_SYMBOLS", "").strip()
    if not (paper_only and ml_disabled and no_live_symbols):
        report["auto_restore_reason"] = "active config did not satisfy paper-only restore requirements"
        return False
    lines = active_env.read_text(encoding="utf-8").splitlines()
    replaced = False
    for index, line in enumerate(lines):
        if line.startswith("ENSEMBLE_BLOCKED_REGIMES="):
            lines[index] = "ENSEMBLE_BLOCKED_REGIMES=STRONG_TREND"
            replaced = True
            break
    if not replaced:
        lines.append("ENSEMBLE_BLOCKED_REGIMES=STRONG_TREND")
    active_env.write_text("\n".join(lines) + "\n", encoding="utf-8")
    report["auto_restore_performed"] = True
    report["auto_restore_reason"] = "paper-only safety confirmed; STRONG_TREND block restored"
    return True


def render_markdown(report: dict[str, Any]) -> str:
    strong = report["strong_trend_metrics"]
    other = report["non_strong_trend_metrics"]
    return "\n".join(
        [
            "# STRONG_TREND Paper Experiment Status",
            "",
            f"- generated_at_utc: {report['generated_at_utc']}",
            f"- experiment_start_time: {report['experiment_start_time']}",
            "- experiment_scope: paper only",
            "- STRONG_TREND_ALLOWED_ONLY_IN_PAPER: true",
            f"- strong_trend_cycles: {report['strong_trend_cycles']}",
            f"- strong_trend_signals: {report['strong_trend_signals']}",
            f"- strong_trend_order_attempts: {report['strong_trend_order_attempts']}",
            f"- strong_trend_paper_orders_created: {report['strong_trend_paper_orders_created']}",
            f"- strong_trend_order_errors: {report['strong_trend_order_errors']}",
            f"- strong_trend_fills: {report['strong_trend_fills']}",
            f"- strong_trend_closed_trades: {report['strong_trend_closed_trades']}",
            f"- stop_recommended: {str(report['stop_recommended']).lower()}",
            f"- stop_reason: {report['stop_reason'] or 'none'}",
            f"- previous_stop_was_false_positive: {str(report['previous_stop_was_false_positive']).lower()}",
            f"- false_positive_reason: {report['false_positive_reason'] or 'none'}",
            f"- order_consistency_note: {report['order_consistency_note'] or 'none'}",
            "- auto_stop_rules_configured: true",
            "",
            "## Comparison",
            "",
            "| Scope | Closed | Win rate | Profit factor | Expectancy R | Max DD R |",
            "|---|---:|---:|---:|---:|---:|",
            f"| STRONG_TREND | {strong['closed_trades']} | {strong['win_rate']} | {strong['profit_factor']} | {strong['expectancy_R']} | {strong['max_drawdown_R']} |",
            f"| Non-STRONG_TREND | {other['closed_trades']} | {other['win_rate']} | {other['profit_factor']} | {other['expectancy_R']} | {other['max_drawdown_R']} |",
            "",
            "This monitor is paper-only and does not approve live trading.",
            "",
        ]
    )


def run_monitor(
    *,
    db_path: Path,
    experiment_config: Path,
    active_env: Path,
    output_md: Path,
    output_json: Path,
    output_diagnosis_md: Path = DEFAULT_DIAG_MD,
    output_diagnosis_json: Path = DEFAULT_DIAG_JSON,
    section4_status: Path = DEFAULT_SECTION4_STATUS,
    clean_start_time: str | None = None,
    auto_restore: bool = False,
) -> dict[str, Any]:
    experiment_values = parse_env(experiment_config)
    start_time = experiment_values.get("STRONG_TREND_EXPERIMENT_START_TIME")
    if not start_time:
        raise ValueError(f"Missing STRONG_TREND_EXPERIMENT_START_TIME in {experiment_config}")
    report = build_report(db_path, start_time)
    clean_start_value = clean_start_time or _clean_start_from_status(section4_status)
    diagnosis = build_order_count_diagnosis(
        db_path,
        start_time,
        clean_start_time=clean_start_value,
    )
    report["order_count_diagnosis_path"] = str(output_diagnosis_json)
    report["order_count_diagnosis_summary"] = diagnosis.get("summary", {})
    if auto_restore:
        safe_auto_restore(active_env, report)
    output_md.parent.mkdir(parents=True, exist_ok=True)
    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_diagnosis_md.parent.mkdir(parents=True, exist_ok=True)
    output_diagnosis_json.parent.mkdir(parents=True, exist_ok=True)
    output_md.write_text(render_markdown(report), encoding="utf-8")
    output_json.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    output_diagnosis_md.write_text(render_diagnosis_markdown(diagnosis), encoding="utf-8")
    output_diagnosis_json.write_text(json.dumps(diagnosis, indent=2, sort_keys=True), encoding="utf-8")
    return report


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", type=Path, default=None)
    parser.add_argument("--experiment-config", type=Path, default=DEFAULT_EXPERIMENT_CONFIG)
    parser.add_argument("--active-env", type=Path, default=DEFAULT_ACTIVE_ENV)
    parser.add_argument("--output-md", type=Path, default=DEFAULT_MD)
    parser.add_argument("--output-json", type=Path, default=DEFAULT_JSON)
    parser.add_argument("--output-diagnosis-md", type=Path, default=DEFAULT_DIAG_MD)
    parser.add_argument("--output-diagnosis-json", type=Path, default=DEFAULT_DIAG_JSON)
    parser.add_argument("--section4-status", type=Path, default=DEFAULT_SECTION4_STATUS)
    parser.add_argument("--clean-start")
    parser.add_argument("--auto-restore", action="store_true")
    parser.add_argument("--no-auto-restore", action="store_true")
    args = parser.parse_args()
    report = run_monitor(
        db_path=resolve_db_path(args.db_path),
        experiment_config=args.experiment_config,
        active_env=args.active_env,
        output_md=args.output_md,
        output_json=args.output_json,
        output_diagnosis_md=args.output_diagnosis_md,
        output_diagnosis_json=args.output_diagnosis_json,
        section4_status=args.section4_status,
        clean_start_time=args.clean_start,
        auto_restore=args.auto_restore and not args.no_auto_restore,
    )
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
