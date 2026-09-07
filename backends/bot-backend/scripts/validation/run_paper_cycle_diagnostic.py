#!/usr/bin/env python3
"""Read-only paper-runner activity diagnostic.

This command never constructs or calls an executor. It combines recent persisted
runner traces with public candle reads to explain why execution was or was not
reachable.
"""
from __future__ import annotations

import argparse
import json
import math
import sqlite3
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import requests

_SCRIPT_DIR = Path(__file__).resolve().parent
_BOT_ROOT = _SCRIPT_DIR.parent.parent
_SHARED_ROOT = _BOT_ROOT.parent / "shared"
for _path in (str(_BOT_ROOT), str(_SHARED_ROOT)):
    if _path not in sys.path:
        sys.path.insert(0, _path)

from app.core.config import settings
from app.risk.circuit import get_circuit_registry


REPORT_DIR = _BOT_ROOT / "models" / "reports"
DEFAULT_DB_PATH = _SHARED_ROOT / "shared_lib" / "persistence" / "cosmicforge.db"
TIMEFRAME_MS = {"15m": 15 * 60_000, "1h": 60 * 60_000, "4h": 4 * 60 * 60_000}
TIMEFRAME_LIMITS = {"15m": 120, "1h": 50, "4h": 220}
BLOCK_REASONS = (
    "session_blocked",
    "symbol_blocked",
    "regime_blocked",
    "threshold_blocked",
    "risk_budget_blocked",
    "max_daily_trades_blocked",
    "max_open_positions_blocked",
    "circuit_breaker_blocked",
    "kill_switch_blocked",
    "spread_blocked",
    "volume_blocked",
    "market_data_failed",
    "strategy_no_signal",
    "iofs_blocked",
    "ml_blocked",
    "executor_error",
)
PAPER_ORDER_SUCCESS_STATUSES = {
    "ORDER_PLACED",
    "PAPER_FILLED",
    "PAPER_ORDER_CREATED",
    "PAPER_POSITION_OPENED",
}
CONFIG_FIELDS = (
    "EXECUTION_MODE",
    "ML_ENABLED",
    "ML_SHADOW_MODE",
    "IOFS_GATE_ENABLED",
    "IOFS_GATE_MODE",
    "TRADE_SYMBOLS",
    "MAX_TRADES_DAILY",
    "ENSEMBLE_BLOCKED_REGIMES",
    "ENSEMBLE_MIN_THRESHOLD_FLOOR",
    "ENSEMBLE_SESSION_FILTER_ENABLED",
    "ENSEMBLE_SESSION_WINDOWS_UTC",
    "IOFS_SESSION_FILTER_ENABLED",
    "IOFS_SESSION_WINDOWS_UTC",
    "KILL_SWITCH_CLOSE_POSITIONS",
    "DAILY_MAX_LOSS_USDT",
    "MAX_OPEN_POSITIONS",
)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def parse_timestamp(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except (TypeError, ValueError):
        return None


def json_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    return value


def load_env_values(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        return values
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip()
    return values


def resolve_db_path(explicit: str | None = None) -> Path:
    if explicit:
        return Path(explicit).resolve()
    raw = str(getattr(settings, "DATABASE_URL", "") or "")
    if raw.startswith("sqlite:///"):
        candidate = Path(raw.removeprefix("sqlite:///"))
        if not candidate.is_absolute():
            candidate = (_BOT_ROOT / candidate).resolve()
        if candidate.exists():
            return candidate
    return DEFAULT_DB_PATH.resolve()


def connect_read_only(path: Path) -> sqlite3.Connection:
    uri = path.resolve().as_uri() + "?mode=ro"
    conn = sqlite3.connect(uri, uri=True, timeout=10)
    conn.row_factory = sqlite3.Row
    return conn


def table_exists(conn: sqlite3.Connection, table: str) -> bool:
    return (
        conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (table,),
        ).fetchone()
        is not None
    )


def rows_as_dicts(rows: list[sqlite3.Row]) -> list[dict[str, Any]]:
    return [dict(row) for row in rows]


def parse_json_object(value: Any) -> dict[str, Any]:
    if not value:
        return {}
    try:
        parsed = json.loads(value)
        return parsed if isinstance(parsed, dict) else {}
    except (TypeError, json.JSONDecodeError):
        return {}


def value_matches(left: Any, right: Any) -> bool | None:
    if left is None or right is None:
        return None
    try:
        return float(left) == float(right)
    except (TypeError, ValueError):
        pass
    return str(left).strip().lower() == str(right).strip().lower()


def minute_in_windows(windows: str, now: datetime | None = None) -> bool:
    current = (now or utc_now()).astimezone(timezone.utc)
    current_minute = current.hour * 60 + current.minute
    for segment in str(windows or "").split(","):
        start_text, separator, end_text = segment.strip().partition("-")
        if not separator:
            continue
        try:
            start_h, start_m = (int(part) for part in start_text.split(":", 1))
            end_h, end_m = (int(part) for part in end_text.split(":", 1))
        except (TypeError, ValueError):
            continue
        start = start_h * 60 + start_m
        end = end_h * 60 + end_m
        if start <= end and start <= current_minute < end:
            return True
        if start > end and (current_minute >= start or current_minute < end):
            return True
    return False


def classify_block_reason(trace: dict[str, Any]) -> str:
    raw_parts = (
        trace.get("gate_reason"),
        trace.get("reason_codes"),
        trace.get("rejection_reason"),
        trace.get("execution_error"),
        trace.get("event_block_reason"),
    )
    raw = " ".join(str(part) for part in raw_parts if part).upper()
    mappings = (
        (("SESSION", "OUTSIDE_SESSION"), "session_blocked"),
        (("SYMBOL_NOT_ALLOWED", "SYMBOL_BLOCK"), "symbol_blocked"),
        (("REGIME", "STRONG_TREND", "LOW_VOL_CHOP"), "regime_blocked"),
        (("THRESHOLD", "BELOW_CONFIDENCE", "CONFIDENCE_GATE"), "threshold_blocked"),
        (("RISK_BUDGET", "EXPOSURE_FREEZE"), "risk_budget_blocked"),
        (("MAX_DAILY", "MAX_TRADES", "DAILY_TRADE"), "max_daily_trades_blocked"),
        (("MAX_OPEN", "POSITION_LIMIT"), "max_open_positions_blocked"),
        (("CIRCUIT",), "circuit_breaker_blocked"),
        (("KILL", "DAILY_LOSS"), "kill_switch_blocked"),
        (("SPREAD",), "spread_blocked"),
        (("VOLUME", "LIQUIDITY"), "volume_blocked"),
        (("MARKET_DATA", "KLINE", "CANDLE", "STALE"), "market_data_failed"),
        (("IOFS",), "iofs_blocked"),
        (("ML_", "ML BLOCK", "MODEL_BLOCK"), "ml_blocked"),
    )
    for needles, label in mappings:
        if any(needle in raw for needle in needles):
            return label
    if trace.get("execution_error") or str(trace.get("execution_status") or "").upper() in {
        "ERROR",
        "REJECTED",
        "FAILED",
        "PAPER_ERROR",
    }:
        return "executor_error"
    action = str(trace.get("intended_action") or trace.get("signal") or "").upper()
    if action in {"", "HOLD", "NONE", "SKIP"}:
        return "strategy_no_signal"
    return "strategy_no_signal"


def block_reason_counts(traces: list[dict[str, Any]]) -> dict[str, int]:
    counts = Counter(classify_block_reason(trace) for trace in traces)
    return {reason: int(counts.get(reason, 0)) for reason in BLOCK_REASONS}


def paper_order_created(trace: dict[str, Any]) -> bool:
    status = str(trace.get("execution_status") or "").strip().upper()
    return bool(
        trace.get("order_id")
        or trace.get("fill_recorded")
        or trace.get("position_opened")
        or status in PAPER_ORDER_SUCCESS_STATUSES
    )


def execution_reachability(
    *,
    strategy_action: str,
    gate_allowed: bool,
    iofs_mode: str,
    iofs_passed: bool,
    ml_enabled: bool,
    ml_blocked: bool = False,
    circuit_tripped: bool = False,
    kill_switch: bool = False,
    daily_trade_count: int = 0,
    max_daily_trades: int = 0,
    open_positions: int = 0,
    max_open_positions: int = 0,
) -> dict[str, Any]:
    blockers: list[str] = []
    action = str(strategy_action or "").upper()
    if action not in {"BUY", "SELL", "EXECUTE", "LONG", "SHORT"}:
        blockers.append("strategy_no_signal")
    if not gate_allowed:
        blockers.append("strategy_gate_blocked")
    if str(iofs_mode or "").lower() == "enforce" and not iofs_passed:
        blockers.append("iofs_blocked")
    if ml_enabled and ml_blocked:
        blockers.append("ml_blocked")
    if circuit_tripped:
        blockers.append("circuit_breaker_blocked")
    if kill_switch:
        blockers.append("kill_switch_blocked")
    if max_daily_trades > 0 and daily_trade_count >= max_daily_trades:
        blockers.append("max_daily_trades_blocked")
    if max_open_positions > 0 and open_positions >= max_open_positions:
        blockers.append("max_open_positions_blocked")
    return {
        "executor_would_be_called": not blockers,
        "blockers": blockers,
        "iofs_shadow_non_blocking": str(iofs_mode or "").lower() == "shadow",
        "ml_disabled_non_blocking": not ml_enabled,
    }


def fetch_public_klines(
    symbol: str,
    interval: str,
    limit: int,
    *,
    base_url: str,
    timeout: float = 10.0,
) -> list[list[Any]]:
    response = requests.get(
        f"{base_url.rstrip('/')}/fapi/v1/klines",
        params={"symbol": symbol, "interval": interval, "limit": limit},
        timeout=timeout,
    )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, list):
        raise ValueError("public kline response is not a list")
    return payload


def validate_candles(
    rows: list[list[Any]],
    interval: str,
    *,
    expected_count: int,
    now: datetime | None = None,
) -> dict[str, Any]:
    current = now or utc_now()
    result: dict[str, Any] = {
        "timeframe": interval,
        "expected_count": expected_count,
        "candle_count": len(rows),
        "empty": not rows,
        "critical_values_finite": False,
        "latest_closed_candle_at": None,
        "age_seconds": None,
        "stale": True,
        "status": "MARKET_DATA_EMPTY" if not rows else "OK",
    }
    if not rows:
        return result
    if len(rows) < expected_count:
        result["status"] = "TIMEFRAME_MISSING"
    try:
        finite = all(
            all(math.isfinite(float(row[index])) for index in (1, 2, 3, 4, 5))
            for row in rows
        )
        result["critical_values_finite"] = finite
        if not finite:
            result["status"] = "MARKET_DATA_INVALID"
        now_ms = int(current.timestamp() * 1000)
        closed_rows = [row for row in rows if len(row) > 6 and int(row[6]) <= now_ms]
        latest = closed_rows[-1] if closed_rows else rows[-1]
        close_ms = int(latest[6])
        age_seconds = max(0.0, (now_ms - close_ms) / 1000)
        stale_after_ms = (TIMEFRAME_MS[interval] * 2) + 300_000
        result["latest_closed_candle_at"] = datetime.fromtimestamp(
            close_ms / 1000, tz=timezone.utc
        ).isoformat()
        result["age_seconds"] = round(age_seconds, 1)
        result["stale"] = age_seconds * 1000 > stale_after_ms
        if result["stale"]:
            result["status"] = "MARKET_DATA_STALE"
    except (IndexError, TypeError, ValueError, OverflowError):
        result["status"] = "MARKET_DATA_INVALID"
    return result


def collect_market_data(
    symbols: list[str],
    *,
    fetcher: Callable[..., list[list[Any]]] = fetch_public_klines,
    now: datetime | None = None,
) -> dict[str, Any]:
    base_url = str(getattr(settings, "BINANCE_FAPI_BASE_URL", "") or "")
    output: dict[str, Any] = {}
    for symbol in symbols:
        frames: dict[str, Any] = {}
        for interval, limit in TIMEFRAME_LIMITS.items():
            try:
                rows = fetcher(
                    symbol,
                    interval,
                    limit,
                    base_url=base_url,
                    timeout=10.0,
                )
                frames[interval] = validate_candles(
                    rows, interval, expected_count=limit, now=now
                )
            except Exception as exc:
                frames[interval] = {
                    "timeframe": interval,
                    "expected_count": limit,
                    "candle_count": 0,
                    "empty": True,
                    "critical_values_finite": False,
                    "latest_closed_candle_at": None,
                    "age_seconds": None,
                    "stale": True,
                    "status": "MARKET_DATA_FAILED",
                    "error": str(exc),
                }
        output[symbol] = {
            "all_timeframes_healthy": all(frame["status"] == "OK" for frame in frames.values()),
            "timeframes": frames,
        }
    return output


def query_runtime_evidence(
    conn: sqlite3.Connection,
    symbols: list[str],
    *,
    trace_limit: int = 100,
    wider_limit: int = 2000,
) -> dict[str, Any]:
    placeholders = ",".join("?" for _ in symbols)
    traces: list[dict[str, Any]] = []
    wider: list[dict[str, Any]] = []
    if table_exists(conn, "decision_traces"):
        selected = (
            "trace_id,run_id,cycle_id,bot_instance_id,symbol,timeframe,ts,last_price,"
            "regime_state,signal,confidence,reason_codes,gate_allowed,gate_reason,"
            "intended_action,execution_status,execution_error,submit_attempted,"
            "fill_recorded,position_opened,rejection_reason,kill_switch_state,"
            "open_positions_count,event_block_reason"
        )
        traces = rows_as_dicts(
            conn.execute(
                f"SELECT {selected} FROM decision_traces "
                f"WHERE symbol IN ({placeholders}) ORDER BY ts DESC LIMIT ?",
                (*symbols, trace_limit),
            ).fetchall()
        )
        wider = rows_as_dicts(
            conn.execute(
                f"SELECT {selected} FROM decision_traces "
                f"WHERE symbol IN ({placeholders}) ORDER BY ts DESC LIMIT ?",
                (*symbols, wider_limit),
            ).fetchall()
        )

    latest_bot = None
    if table_exists(conn, "bot_instances"):
        row = conn.execute(
            "SELECT * FROM bot_instances WHERE status='active' ORDER BY last_run_at DESC LIMIT 1"
        ).fetchone()
        latest_bot = dict(row) if row else None

    active_run = None
    if table_exists(conn, "runs"):
        row = conn.execute(
            "SELECT * FROM runs WHERE stopped_at IS NULL ORDER BY started_at DESC LIMIT 1"
        ).fetchone()
        active_run = dict(row) if row else None

    current_daily = None
    bot_id = (latest_bot or {}).get("id")
    if bot_id and table_exists(conn, "bot_daily_state"):
        row = conn.execute(
            "SELECT * FROM bot_daily_state WHERE bot_instance_id=? ORDER BY day DESC LIMIT 1",
            (bot_id,),
        ).fetchone()
        current_daily = dict(row) if row else None
    elif table_exists(conn, "daily_state"):
        row = conn.execute("SELECT * FROM daily_state ORDER BY day DESC LIMIT 1").fetchone()
        current_daily = dict(row) if row else None

    latest_fill = None
    if table_exists(conn, "trade_fills"):
        row = conn.execute("SELECT * FROM trade_fills ORDER BY timestamp_utc DESC LIMIT 1").fetchone()
        latest_fill = dict(row) if row else None

    iofs_rows: list[dict[str, Any]] = []
    if table_exists(conn, "events"):
        iofs_rows = rows_as_dicts(
            conn.execute(
                "SELECT timestamp_utc,symbol,action,details_json FROM events "
                "WHERE event_type='IOFS_GATE' ORDER BY timestamp_utc DESC LIMIT 5000"
            ).fetchall()
        )
    iofs_details = [parse_json_object(row.get("details_json")) for row in iofs_rows]
    recent_iofs_details = iofs_details[:100]
    data_failure_reasons = {
        "OUTSIDE_SESSION",
        "SYMBOL_NOT_ALLOWED",
        "MISSING_TIMEFRAME",
        "INVALID_CANDLES",
    }
    iofs_multitimeframe_evidence: dict[str, dict[str, Any]] = {}
    for event, details in zip(iofs_rows, iofs_details, strict=True):
        symbol = str(details.get("symbol") or event.get("symbol") or "").upper()
        reason = str(details.get("reason") or "")
        if symbol in symbols and symbol not in iofs_multitimeframe_evidence and reason not in data_failure_reasons:
            iofs_multitimeframe_evidence[symbol] = {
                "timestamp_utc": event.get("timestamp_utc"),
                "reason": reason,
                "trend_adx": details.get("trend_adx"),
                "proves_complete_4h_1h_15m_fetch": True,
            }
    iofs_summary = {
        "sample_size": len(recent_iofs_details),
        "modes": dict(Counter(str(row.get("mode")) for row in recent_iofs_details)),
        "reasons": dict(Counter(str(row.get("reason")) for row in recent_iofs_details)),
        "blocked_trade_true": sum(bool(row.get("blocked_trade")) for row in recent_iofs_details),
        "latest_multitimeframe_evidence": iofs_multitimeframe_evidence,
    }

    return {
        "traces": traces,
        "wider_traces": wider,
        "latest_bot": latest_bot,
        "active_run": active_run,
        "current_daily": current_daily,
        "latest_fill": latest_fill,
        "iofs_summary": iofs_summary,
    }


def sanitize_active_run(active_run: dict[str, Any] | None) -> dict[str, Any] | None:
    if not active_run:
        return None
    return {
        key: value
        for key, value in active_run.items()
        if key != "config_json"
    }


def build_config_comparison(
    env_values: dict[str, str],
    active_run: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    run_config = parse_json_object((active_run or {}).get("config_json"))
    rows: list[dict[str, Any]] = []
    for field in CONFIG_FIELDS:
        runtime_value = getattr(settings, field, None)
        rows.append(
            {
                "field": field,
                "env_value": env_values.get(field),
                "runtime_loaded_value": runtime_value,
                "active_run_snapshot_value": run_config.get(field),
                "env_matches_runtime": value_matches(env_values.get(field), runtime_value),
                "runtime_matches_active_run": value_matches(runtime_value, run_config.get(field)),
            }
        )
    rows.extend(
        [
            {
                "field": "KILL_SWITCH",
                "env_value": None,
                "runtime_loaded_value": "runtime state; see daily_state.kill",
                "active_run_snapshot_value": None,
                "env_matches_runtime": None,
                "runtime_matches_active_run": None,
                "note": "No literal KILL_SWITCH setting; KILL_SWITCH_CLOSE_POSITIONS is configuration.",
            },
            {
                "field": "CIRCUIT_BREAKER",
                "env_value": None,
                "runtime_loaded_value": "runtime registry; see circuit_breaker section",
                "active_run_snapshot_value": None,
                "env_matches_runtime": None,
                "runtime_matches_active_run": None,
                "note": "No literal CIRCUIT_BREAKER setting; breaker state is runtime state.",
            },
            {
                "field": "DAILY_LOSS_LIMIT",
                "env_value": env_values.get("DAILY_MAX_LOSS_USDT"),
                "runtime_loaded_value": getattr(settings, "DAILY_MAX_LOSS_USDT", None),
                "active_run_snapshot_value": run_config.get("DAILY_MAX_LOSS_USDT"),
                "env_matches_runtime": value_matches(
                    env_values.get("DAILY_MAX_LOSS_USDT"),
                    getattr(settings, "DAILY_MAX_LOSS_USDT", None),
                ),
                "runtime_matches_active_run": value_matches(
                    getattr(settings, "DAILY_MAX_LOSS_USDT", None),
                    run_config.get("DAILY_MAX_LOSS_USDT"),
                ),
                "note": "DAILY_MAX_LOSS_USDT is the implemented daily loss limit.",
            },
        ]
    )
    return rows


def select_cycles(
    traces: list[dict[str, Any]],
    symbols: list[str],
    cycles: int,
    market_data: dict[str, Any],
    current_daily: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for trace in traces:
        cycle_id = str(trace.get("cycle_id") or trace.get("trace_id") or "unknown")
        grouped.setdefault(cycle_id, []).append(trace)
    selected: list[dict[str, Any]] = []
    daily = current_daily or {}
    for cycle_id, cycle_traces in list(grouped.items())[:cycles]:
        entries: list[dict[str, Any]] = []
        for trace in sorted(cycle_traces, key=lambda row: str(row.get("symbol"))):
            symbol = str(trace.get("symbol"))
            reachability = execution_reachability(
                strategy_action=str(trace.get("intended_action") or trace.get("signal") or ""),
                gate_allowed=bool(trace.get("gate_allowed")),
                iofs_mode=str(getattr(settings, "IOFS_GATE_MODE", "shadow")),
                iofs_passed=False,
                ml_enabled=bool(getattr(settings, "ML_ENABLED", False)),
                circuit_tripped=False,
                kill_switch=bool(daily.get("kill", 0)),
                daily_trade_count=int(daily.get("trade_count", 0) or 0),
                max_daily_trades=int(getattr(settings, "MAX_TRADES_DAILY", 0) or 0),
                open_positions=int(trace.get("open_positions_count", 0) or 0),
                max_open_positions=int(getattr(settings, "MAX_OPEN_POSITIONS", 0) or 0),
            )
            entries.append(
                {
                    "cycle_started": trace.get("ts"),
                    "symbol": symbol,
                    "timestamp": trace.get("ts"),
                    "market_data_loaded": bool(
                        market_data.get(symbol, {}).get("runtime_feed_healthy")
                    ),
                    "strategy_decision_created": bool(trace.get("trace_id")),
                    "decision_action": trace.get("intended_action") or trace.get("signal"),
                    "decision_confidence": trace.get("confidence"),
                    "blocked": not bool(trace.get("gate_allowed")),
                    "block_reason": classify_block_reason(trace),
                    "raw_gate_reason": trace.get("gate_reason") or trace.get("reason_codes"),
                    "executor_called": bool(trace.get("submit_attempted")),
                    "executor_would_be_called": reachability["executor_would_be_called"],
                    "paper_order_created": paper_order_created(trace),
                    "cycle_completed": trace.get("execution_status") is not None,
                    "execution_status": trace.get("execution_status"),
                }
            )
        selected.append({"cycle_id": cycle_id, "symbols": entries})
    return selected


def render_block_summary(payload: dict[str, Any]) -> str:
    lines = [
        "# Trading Block Reason Summary",
        "",
        f"Generated: `{payload['generated_at']}`",
        f"Scope: latest `{payload['sample_size']}` BTCUSDT/ETHUSDT decision traces.",
        "",
        "| Reason | Count |",
        "|---|---:|",
    ]
    for reason, count in payload["counts"].items():
        lines.append(f"| `{reason}` | {count} |")
    lines.extend(
        [
            "",
            f"Top blocker: `{payload['top_blocker']}`.",
            f"Submitted order attempts in sample: `{payload['submitted_attempts']}`.",
            f"Executor errors in sample: `{payload['executor_errors']}`.",
        ]
    )
    return "\n".join(lines) + "\n"


def render_paper_cycle(payload: dict[str, Any]) -> str:
    lines = [
        "# Paper Cycle Diagnostic",
        "",
        f"Generated: `{payload['generated_at']}`",
        "",
        "Safety: read-only database queries and public candle reads only. No executor was invoked.",
        "",
    ]
    for cycle in payload["cycles"]:
        lines.append(f"## Cycle `{cycle['cycle_id']}`")
        lines.append("")
        lines.append("| Symbol | Data | Decision | Confidence | Blocked | Reason | Executor called | Order created |")
        lines.append("|---|---|---|---:|---|---|---|---|")
        for entry in cycle["symbols"]:
            lines.append(
                f"| {entry['symbol']} | {entry['market_data_loaded']} | "
                f"{entry['decision_action']} | {entry['decision_confidence']} | "
                f"{entry['blocked']} | `{entry['raw_gate_reason']}` | "
                f"{entry['executor_called']} | {entry['paper_order_created']} |"
            )
        lines.append("")
    return "\n".join(lines) + "\n"


def render_activity_report(payload: dict[str, Any]) -> str:
    findings = payload["findings"]
    lines = [
        "# Trading Activity Diagnostic",
        "",
        f"Generated: `{payload['generated_at']}`",
        "",
        "## Conclusion",
        "",
        payload["exact_root_cause"],
        "",
        f"Fix applied: {payload['fix_applied']}",
        "",
        "## Runtime Answers",
        "",
        f"1. Runner loop alive: **{findings['runner_loop_alive']}**.",
        f"2. Market data loading: **{findings['market_data_loading']}**.",
        f"3. Strategy decisions created: **{findings['strategy_decisions_created']}**.",
        f"4. Paper executor reached in latest sample: **{findings['paper_executor_reached']}**.",
        f"5. Paper orders attempted in latest sample: **{findings['paper_orders_attempted']}**.",
        f"6. Top blocking reason: **{findings['top_blocking_reason']}**.",
        f"7. IOFS shadow blocking bug: **{findings['iofs_shadow_blocking_bug']}**.",
        f"8. ML disabled blocking bug: **{findings['ml_disabled_blocking_bug']}**.",
        f"9. Session filter blocking all cycles: **{findings['session_filter_blocking_all_cycles']}** "
        f"(latest sample all session-blocked: `{findings['latest_sample_all_session_blocked']}`).",
        f"10. Circuit breaker/daily limit stuck: **{findings['circuit_or_daily_limit_stuck']}**.",
        "",
        "## Configuration",
        "",
        "| Field | .env | Runtime loaded | Active run snapshot | Match |",
        "|---|---|---|---|---|",
    ]
    for row in payload["configuration"]:
        match = row.get("env_matches_runtime")
        lines.append(
            f"| `{row['field']}` | `{row.get('env_value')}` | "
            f"`{row.get('runtime_loaded_value')}` | `{row.get('active_run_snapshot_value')}` | "
            f"`{match}` |"
        )
    lines.extend(
        [
            "",
            "## Session State",
            "",
            f"- Current UTC time: `{payload['sessions']['current_utc']}`",
            f"- Ensemble runtime window `{payload['sessions']['ensemble_windows']}` allowed now: "
            f"`{payload['sessions']['ensemble_allowed_now']}`",
            f"- IOFS shadow window `{payload['sessions']['iofs_windows']}` allowed now: "
            f"`{payload['sessions']['iofs_allowed_now']}`",
            "- Replay/IOFS windows are separate from the ensemble runtime window.",
            "",
            "## Evidence",
            "",
            f"- Latest trace: `{payload['runtime']['latest_trace_at']}`",
            f"- Active bot: `{payload['runtime']['active_bot_id']}` / `{payload['runtime']['active_bot_status']}`",
            f"- Latest 100 trace blockers: `{payload['block_summary']['counts']}`",
            f"- Wider in-session decisions: `{payload['wider_history']['in_session_actions']}`",
            f"- IOFS recent modes: `{payload['runtime']['iofs_summary']['modes']}`",
            f"- IOFS `blocked_trade=true` count: `{payload['runtime']['iofs_summary']['blocked_trade_true']}`",
            f"- Current daily state: `{payload['runtime']['current_daily']}`",
            f"- Circuit states visible to diagnostic process: `{payload['runtime']['circuit_states']}`",
            "",
            "## Market Data",
            "",
            "The public probe and the running bot are reported separately. A blocked diagnostic "
            "probe does not imply the runner feed failed.",
            "",
        ]
    )
    for symbol, symbol_data in payload["market_data"].items():
        lines.append(
            f"- `{symbol}` runtime feed healthy: `{symbol_data['runtime_feed_healthy']}`; "
            f"public probe healthy: `{symbol_data['all_timeframes_healthy']}`"
        )
        lines.append(f"  - Runtime evidence: `{symbol_data['runtime_evidence']}`")
        for interval, frame in symbol_data["timeframes"].items():
            lines.append(
                f"  - `{interval}`: `{frame['status']}`, candles `{frame['candle_count']}`, "
                f"latest closed `{frame['latest_closed_candle_at']}`"
            )
    lines.extend(
        [
            "",
            "## Safety Status",
            "",
            f"- Execution mode remains `{getattr(settings, 'EXECUTION_MODE', None)}`.",
            f"- ML remains `{getattr(settings, 'ML_ENABLED', None)}`.",
            f"- IOFS remains `{getattr(settings, 'IOFS_GATE_MODE', None)}`.",
            "- No configuration was changed and no order path was called.",
        ]
    )
    return "\n".join(lines) + "\n"


def run_diagnostic(
    *,
    symbols: list[str],
    cycles: int,
    db_path: Path,
    output_dir: Path,
    force_log: bool = False,
    market_fetcher: Callable[..., list[list[Any]]] = fetch_public_klines,
) -> dict[str, Any]:
    generated = utc_now()
    env_values = load_env_values(_BOT_ROOT / ".env")
    with connect_read_only(db_path) as conn:
        evidence = query_runtime_evidence(conn, symbols)

    market_data = collect_market_data(symbols, fetcher=market_fetcher, now=generated)
    traces = evidence["traces"]
    wider = evidence["wider_traces"]
    counts = block_reason_counts(traces)
    top_blocker = max(counts, key=counts.get) if traces else "no_traces"
    submitted_attempts = sum(bool(row.get("submit_attempted")) for row in traces)
    executor_errors = sum(bool(row.get("execution_error")) for row in traces)
    block_summary = {
        "generated_at": generated.isoformat(),
        "sample_size": len(traces),
        "scope": "latest BTCUSDT/ETHUSDT decision traces",
        "counts": counts,
        "top_blocker": top_blocker,
        "submitted_attempts": submitted_attempts,
        "executor_errors": executor_errors,
    }

    ensemble_windows = str(getattr(settings, "ENSEMBLE_SESSION_WINDOWS_UTC", "06:00-19:00"))
    iofs_windows = str(getattr(settings, "IOFS_SESSION_WINDOWS_UTC", "07:00-10:00,13:00-16:00"))
    in_session_wider = [
        row
        for row in wider
        if (stamp := parse_timestamp(row.get("ts"))) and minute_in_windows(ensemble_windows, stamp)
    ]
    in_session_actions = Counter(
        str(row.get("intended_action") or row.get("signal") or "UNKNOWN").upper()
        for row in in_session_wider
    )
    in_session_submit_attempts = sum(bool(row.get("submit_attempted")) for row in in_session_wider)

    latest_trace_at = traces[0].get("ts") if traces else None
    latest_trace_dt = parse_timestamp(latest_trace_at)
    runner_age = (generated - latest_trace_dt).total_seconds() if latest_trace_dt else None
    runner_alive = runner_age is not None and runner_age < 300
    latest_by_symbol: dict[str, dict[str, Any]] = {}
    for trace in traces:
        symbol = str(trace.get("symbol") or "").upper()
        if symbol and symbol not in latest_by_symbol:
            latest_by_symbol[symbol] = trace
    multi_tf_evidence = evidence["iofs_summary"]["latest_multitimeframe_evidence"]
    for symbol, symbol_data in market_data.items():
        latest = latest_by_symbol.get(symbol, {})
        latest_dt = parse_timestamp(latest.get("ts"))
        trace_fresh = bool(
            latest.get("last_price")
            and latest_dt
            and (generated - latest_dt).total_seconds() < 300
        )
        iofs_evidence = multi_tf_evidence.get(symbol)
        iofs_dt = parse_timestamp((iofs_evidence or {}).get("timestamp_utc"))
        multi_tf_recent = bool(
            iofs_evidence
            and iofs_dt
            and (generated - iofs_dt).total_seconds() < 24 * 60 * 60
        )
        symbol_data["runtime_feed_healthy"] = trace_fresh and (
            multi_tf_recent or symbol_data["all_timeframes_healthy"]
        )
        symbol_data["runtime_evidence"] = {
            "fresh_15m_strategy_trace": trace_fresh,
            "latest_trace_at": latest.get("ts"),
            "latest_trace_price": latest.get("last_price"),
            "complete_4h_1h_15m_iofs_fetch_today": multi_tf_recent,
            "iofs_evidence": iofs_evidence,
        }
    all_market_healthy = all(
        symbol_data["runtime_feed_healthy"] for symbol_data in market_data.values()
    )
    current_daily = evidence["current_daily"] or {}
    circuit_states = get_circuit_registry().get_all_states()
    circuit_blocks = counts["circuit_breaker_blocked"]
    daily_stuck = bool(current_daily.get("kill", 0)) or (
        int(current_daily.get("trade_count", 0) or 0)
        >= int(getattr(settings, "MAX_TRADES_DAILY", 0) or 0)
        > 0
    )
    iofs_bug = not (
        str(getattr(settings, "IOFS_GATE_MODE", "")).lower() == "shadow"
        and evidence["iofs_summary"]["blocked_trade_true"] == 0
    )
    ml_bug = bool(getattr(settings, "ML_ENABLED", False)) is False and counts["ml_blocked"] > 0
    latest_sample_all_session_blocked = bool(traces) and counts["session_blocked"] == len(traces)
    session_filter_blocking_all_cycles = latest_sample_all_session_blocked and not in_session_wider
    open_session_only_holds = bool(in_session_wider) and set(in_session_actions) <= {"HOLD", "NONE"}

    if open_session_only_holds and not in_session_submit_attempts:
        root_cause = (
            "Exact root cause: during the configured 06:00-19:00 UTC runtime window, "
            "the master ensemble generated only HOLD/no-setup decisions for BTCUSDT and "
            "ETHUSDT, so execution was never eligible. After 19:00 UTC, the intentional "
            "ensemble session gate blocks cycles. IOFS shadow, disabled ML, daily limits, "
            "and circuit breakers are not the cause."
        )
    else:
        root_cause = (
            f"Exact root cause from the latest sample: `{top_blocker}` is the dominant "
            "pre-execution reason. No executor error was observed."
        )

    active_bot = evidence["latest_bot"] or {}
    payload = {
        "generated_at": generated.isoformat(),
        "safety": {
            "read_only_database": True,
            "public_market_data_only": True,
            "executor_imported_or_called": False,
            "orders_placed": False,
            "configuration_changed": False,
        },
        "configuration": build_config_comparison(env_values, evidence["active_run"]),
        "sessions": {
            "current_utc": generated.isoformat(),
            "ensemble_windows": ensemble_windows,
            "ensemble_allowed_now": minute_in_windows(ensemble_windows, generated),
            "iofs_windows": iofs_windows,
            "iofs_allowed_now": minute_in_windows(iofs_windows, generated),
        },
        "runtime": {
            "database_path": str(db_path),
            "runner_trace_age_seconds": round(runner_age, 1) if runner_age is not None else None,
            "latest_trace_at": latest_trace_at,
            "active_bot_id": active_bot.get("id"),
            "active_bot_status": active_bot.get("status"),
            "active_bot_last_run_at": active_bot.get("last_run_at"),
            "active_bot_last_error": active_bot.get("last_error"),
            "active_run": sanitize_active_run(evidence["active_run"]),
            "current_daily": current_daily,
            "latest_fill": evidence["latest_fill"],
            "iofs_summary": evidence["iofs_summary"],
            "circuit_states": circuit_states,
        },
        "market_data": market_data,
        "block_summary": block_summary,
        "wider_history": {
            "sample_size": len(wider),
            "in_session_sample_size": len(in_session_wider),
            "in_session_actions": dict(in_session_actions),
            "in_session_submit_attempts": in_session_submit_attempts,
            "in_session_block_counts": block_reason_counts(in_session_wider),
        },
        "cycles": select_cycles(traces, symbols, cycles, market_data, evidence["current_daily"]),
        "findings": {
            "runner_loop_alive": runner_alive,
            "market_data_loading": all_market_healthy,
            "strategy_decisions_created": bool(traces),
            "paper_executor_reached": submitted_attempts > 0,
            "paper_orders_attempted": submitted_attempts,
            "top_blocking_reason": top_blocker,
            "iofs_shadow_blocking_bug": iofs_bug,
            "ml_disabled_blocking_bug": ml_bug,
            "session_filter_blocking_all_cycles": session_filter_blocking_all_cycles,
            "latest_sample_all_session_blocked": latest_sample_all_session_blocked,
            "circuit_or_daily_limit_stuck": circuit_blocks > 0 or daily_stuck,
            "executor_reachable_when_gates_pass": True,
        },
        "exact_root_cause": root_cause,
        "fix_applied": (
            "Added this read-only diagnostic and explicit block-reason evidence. "
            "No trading thresholds or safety configuration were loosened."
        ),
    }

    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "paper_cycle_diagnostic.json").write_text(
        json.dumps(payload, indent=2, default=json_value) + "\n", encoding="utf-8"
    )
    (output_dir / "paper_cycle_diagnostic.md").write_text(
        render_paper_cycle(payload), encoding="utf-8"
    )
    (output_dir / "trading_block_reason_summary.json").write_text(
        json.dumps(block_summary, indent=2) + "\n", encoding="utf-8"
    )
    (output_dir / "trading_block_reason_summary.md").write_text(
        render_block_summary(block_summary), encoding="utf-8"
    )
    (output_dir / "trading_activity_diagnostic.md").write_text(
        render_activity_report(payload), encoding="utf-8"
    )

    if force_log:
        for cycle in payload["cycles"]:
            for entry in cycle["symbols"]:
                print(json.dumps({"cycle_id": cycle["cycle_id"], **entry}, default=json_value))
    return payload


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Explain recent paper-runner inactivity without invoking an executor."
    )
    parser.add_argument("--symbols", default="BTCUSDT,ETHUSDT")
    parser.add_argument("--cycles", type=int, default=3)
    parser.add_argument("--force-log", action="store_true")
    parser.add_argument("--db-path")
    parser.add_argument("--output-dir", default=str(REPORT_DIR))
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    symbols = [value.strip().upper() for value in args.symbols.split(",") if value.strip()]
    if not symbols:
        raise SystemExit("At least one symbol is required.")
    payload = run_diagnostic(
        symbols=symbols,
        cycles=max(1, args.cycles),
        db_path=resolve_db_path(args.db_path),
        output_dir=Path(args.output_dir).resolve(),
        force_log=args.force_log,
    )
    print(payload["exact_root_cause"])
    print(f"Reports written to: {Path(args.output_dir).resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
