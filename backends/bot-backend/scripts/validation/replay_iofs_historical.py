#!/usr/bin/env python3
"""Replay the production IOFS gate against historical candles, offline only."""
from __future__ import annotations

import argparse
import bisect
import csv
import json
import math
import sqlite3
import sys
from collections import Counter, defaultdict
from dataclasses import replace
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from statistics import median
from typing import Any, Iterable

_SCRIPT_DIR = Path(__file__).resolve().parent
_BOT_ROOT = _SCRIPT_DIR.parent.parent
_SHARED_ROOT = _BOT_ROOT.parent / "shared"
for _path in (str(_BOT_ROOT), str(_SHARED_ROOT)):
    if _path not in sys.path:
        sys.path.insert(0, _path)

from app.strategy.iofs_components.indicators import calculate_atr
from app.strategy.iofs_components.models import Candle, IOFSGateResult
from app.strategy.iofs_components.scorer import QUALITY_THRESHOLDS
from app.strategy.iofs_gate import IOFSGateEvaluator, gate_result_details, make_gate_failure
from scripts.validation.iofs_trade_simulator import (
    DEFAULT_MAX_HOLD_CANDLES,
    FIFTEEN_MINUTES_MS,
    create_trade_plan,
    simulate_trade,
)


INTERVAL_MS = {"15m": FIFTEEN_MINUTES_MS, "1h": 60 * 60 * 1000, "4h": 4 * 60 * 60 * 1000}
LOOKBACKS = {"4h": 220, "1h": 50, "15m": 30}
PROFILE_ORDER = ("conservative", "balanced", "aggressive")
FAILURE_REASONS = (
    "TREND_NOT_ALIGNED",
    "STRUCTURE_NOT_ACTIVE",
    "TRIGGER_NOT_CONFIRMED",
    "QUALITY_SCORE_TOO_LOW",
    "MISSING_TIMEFRAME",
    "ATR_UNAVAILABLE",
    "INVALID_CANDLES",
    "SESSION_BLOCKED",
    "SYMBOL_BLOCKED",
    "INVALID_RISK",
)
SCORE_BUCKETS = ("0-49", "50-64", "65-71", "72-79", "80-100")


def parse_sessions(value: str) -> list[tuple[time, time, str]]:
    windows: list[tuple[time, time, str]] = []
    for segment in str(value or "").split(","):
        start_text, separator, end_text = segment.strip().partition("-")
        if not separator:
            raise ValueError(f"Invalid session window: {segment!r}")
        start, end = time.fromisoformat(start_text), time.fromisoformat(end_text)
        if end <= start:
            raise ValueError(f"Session end must be after start: {segment!r}")
        windows.append((start, end, f"{start_text}-{end_text}"))
    if not windows:
        raise ValueError("At least one session window is required.")
    return windows


def session_window(timestamp_ms: int, sessions: list[tuple[time, time, str]]) -> str | None:
    current = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc).time()
    for start, end, label in sessions:
        if start <= current < end:
            return label
    return None


def score_bucket(score: int | float | None) -> str:
    value = int(score or 0)
    if value < 50:
        return "0-49"
    if value < 65:
        return "50-64"
    if value < 72:
        return "65-71"
    if value < 80:
        return "72-79"
    return "80-100"


def build_window(candles: list[Candle], signal_time_ms: int, interval: str, count: int) -> list[Candle]:
    """Return candles whose close time is at or before signal_time_ms."""
    duration = INTERVAL_MS[interval]
    close_times = [int(candle.open_time or 0) + duration for candle in candles]
    end = bisect.bisect_right(close_times, signal_time_ms)
    return candles[max(0, end - count) : end]


def derive_4h_candles(candles_1h: list[Candle]) -> list[Candle]:
    """Aggregate complete UTC-aligned sets of four one-hour candles."""
    grouped: dict[int, dict[int, Candle]] = defaultdict(dict)
    duration = INTERVAL_MS["4h"]
    for candle in candles_1h:
        open_time = int(candle.open_time or 0)
        bucket = (open_time // duration) * duration
        grouped[bucket][open_time] = candle

    result: list[Candle] = []
    for bucket in sorted(grouped):
        expected = [bucket + (offset * INTERVAL_MS["1h"]) for offset in range(4)]
        if any(timestamp not in grouped[bucket] for timestamp in expected):
            continue
        rows = [grouped[bucket][timestamp] for timestamp in expected]
        result.append(
            Candle(
                bucket,
                rows[0].open,
                max(row.high for row in rows),
                min(row.low for row in rows),
                rows[-1].close,
                sum(row.volume for row in rows),
            )
        )
    return result


def resolve_db_path(explicit: str | None = None) -> Path:
    if explicit:
        return Path(explicit).resolve()
    for raw_line in (_BOT_ROOT / ".env").read_text(encoding="utf-8").splitlines():
        if raw_line.startswith("DATABASE_URL=sqlite:///"):
            relative = raw_line.split("sqlite:///", 1)[1].strip()
            return (_BOT_ROOT / relative).resolve()
    return (_SHARED_ROOT / "shared_lib" / "persistence" / "cosmicforge.db").resolve()


def load_historical_candles(
    db_path: Path,
    symbols: list[str],
    start_ms: int,
    end_ms: int,
) -> dict[str, dict[str, list[Candle]]]:
    if not db_path.exists():
        raise FileNotFoundError(f"Historical candle database not found: {db_path}")
    placeholders = ",".join("?" for _ in symbols)
    query = f"""
        SELECT symbol, interval, open_time, open, high, low, close, volume, id
        FROM historical_candles
        WHERE symbol IN ({placeholders})
          AND interval IN ('15m', '1h')
          AND open_time >= ?
          AND open_time < ?
          AND market_type = 'crypto'
        ORDER BY symbol, interval, open_time, id
    """
    uri = f"file:{db_path.as_posix()}?mode=ro"
    with sqlite3.connect(uri, uri=True, timeout=30) as connection:
        connection.row_factory = sqlite3.Row
        rows = connection.execute(query, (*symbols, start_ms, end_ms)).fetchall()

    deduped: dict[str, dict[str, dict[int, Candle]]] = {
        symbol: {"15m": {}, "1h": {}} for symbol in symbols
    }
    for row in rows:
        symbol, interval = row["symbol"], row["interval"]
        deduped[symbol][interval][int(row["open_time"])] = Candle(
            int(row["open_time"]),
            float(row["open"]),
            float(row["high"]),
            float(row["low"]),
            float(row["close"]),
            float(row["volume"]),
        )

    loaded: dict[str, dict[str, list[Candle]]] = {}
    for symbol in symbols:
        candles_15m = list(deduped[symbol]["15m"].values())
        candles_1h = list(deduped[symbol]["1h"].values())
        if not candles_15m or not candles_1h:
            missing = [name for name, values in (("15m", candles_15m), ("1h", candles_1h)) if not values]
            raise RuntimeError(f"{symbol}: required historical timeframes missing: {missing}")
        loaded[symbol] = {
            "15m": candles_15m,
            "1h": candles_1h,
            "4h": derive_4h_candles(candles_1h),
        }
    return loaded


def apply_score_override(result: IOFSGateResult, threshold: int | None) -> IOFSGateResult:
    if threshold is None or result.reason not in {"OK", "QUALITY_SCORE_TOO_LOW"}:
        return result
    passed = result.score >= threshold
    return replace(
        result,
        passed=passed,
        reason="OK" if passed else "QUALITY_SCORE_TOO_LOW",
        threshold=threshold,
    )


def replay_data(
    candles_by_symbol: dict[str, dict[str, list[Candle]]],
    *,
    symbols: list[str],
    start_ms: int,
    end_ms: int,
    profiles: list[str],
    sessions: list[tuple[time, time, str]],
    score_threshold_override: int | None = None,
    max_cycles: int | None = None,
    max_holding_candles: int = DEFAULT_MAX_HOLD_CANDLES,
    evaluator: IOFSGateEvaluator | None = None,
) -> dict[str, Any]:
    evaluator = evaluator or IOFSGateEvaluator()
    profile_results: dict[str, dict[str, Any]] = {}

    for profile in profiles:
        cycles: list[dict[str, Any]] = []
        trades: list[dict[str, Any]] = []
        baseline_trades: list[dict[str, Any]] = []
        stopped = False
        for symbol in symbols:
            data = candles_by_symbol.get(symbol)
            if not data:
                cycles.append(_cycle_failure(symbol, start_ms, profile, "SYMBOL_BLOCKED", None))
                continue
            candles_15m = data["15m"]
            for signal_index, signal_candle in enumerate(candles_15m):
                signal_time = int(signal_candle.open_time or 0) + FIFTEEN_MINUTES_MS
                if signal_time < start_ms or signal_time >= end_ms:
                    continue
                if max_cycles is not None and len(cycles) >= max_cycles:
                    stopped = True
                    break
                session = session_window(signal_time, sessions)
                if session is None:
                    cycles.append(_cycle_failure(symbol, signal_time, profile, "SESSION_BLOCKED", None))
                    continue

                windows = {
                    interval: build_window(data[interval], signal_time, interval, LOOKBACKS[interval])
                    for interval in ("4h", "1h", "15m")
                }
                if any(len(windows[name]) < LOOKBACKS[name] for name in LOOKBACKS):
                    result = make_gate_failure("MISSING_TIMEFRAME", profile)
                else:
                    result = evaluator.evaluate(windows, profile)
                    result = apply_score_override(result, score_threshold_override)
                cycle = gate_result_details(
                    symbol, "historical_replay", result, blocked_trade=not result.passed,
                    timestamp_utc=datetime.fromtimestamp(signal_time / 1000, tz=timezone.utc),
                )
                cycle["session_window"] = session
                cycle["score_bucket"] = score_bucket(result.score)
                cycles.append(cycle)

                baseline_candidate = bool(result.trend and result.structure and result.trigger and result.trigger.is_confirmed)
                if result.passed or baseline_candidate:
                    trade, simulation_failure = _simulate_result(
                        symbol=symbol,
                        profile=profile,
                        signal_time=signal_time,
                        signal_index=signal_index,
                        session=session,
                        result=result,
                        windows=windows,
                        candles_15m=candles_15m,
                        max_holding_candles=max_holding_candles,
                    )
                    if simulation_failure:
                        cycle["simulation_failure_reason"] = simulation_failure
                    if trade is not None and baseline_candidate:
                        baseline_trades.append(dict(trade))
                    if trade is not None and result.passed:
                        trades.append(trade)
            if stopped:
                break

        report = summarize_profile(profile, cycles, trades, baseline_trades)
        report["setups"] = trades
        profile_results[profile] = report

    warnings: list[str] = [
        "Historical replay does not replace Section 4 forward paper validation.",
        "Overlapping historical trades are allowed and may overstate practical capacity.",
        "No fees or slippage are included in R-multiple outcomes.",
    ]
    if len(profiles) > 1:
        counts = {profile: profile_results[profile]["metrics"]["accepted_trades"] for profile in profiles}
        if not (
            counts.get("conservative", 0)
            <= counts.get("balanced", 0)
            <= counts.get("aggressive", 0)
        ):
            warnings.append("Risk profile trade counts are not monotonic.")

    return {
        "profiles": profile_results,
        "risk_profile_comparison": [
            {
                "risk_profile": profile,
                **{
                    key: profile_results[profile]["metrics"][key]
                    for key in (
                        "accepted_trades",
                        "pass_rate",
                        "win_rate",
                        "profit_factor_r",
                        "expectancy_r",
                        "max_drawdown_r",
                    )
                },
            }
            for profile in profiles
        ],
        "warnings": warnings,
    }


def _simulate_result(
    *,
    symbol: str,
    profile: str,
    signal_time: int,
    signal_index: int,
    session: str,
    result: IOFSGateResult,
    windows: dict[str, list[Candle]],
    candles_15m: list[Candle],
    max_holding_candles: int,
) -> tuple[dict[str, Any] | None, str | None]:
    next_index = signal_index + 1
    if next_index >= len(candles_15m) or result.structure is None:
        return None, "MISSING_FUTURE_CANDLE"
    atr_15m = calculate_atr(windows["15m"])
    if atr_15m is None or result.structure.level is None:
        return None, "INVALID_RISK"
    entry_candle = candles_15m[next_index]
    if int(entry_candle.open_time or 0) != signal_time:
        return None, "MISSING_FUTURE_CANDLE"
    plan = create_trade_plan(
        direction=result.direction,
        structure_level=result.structure.level,
        atr_15m=atr_15m,
        entry_candle=entry_candle,
    )
    if not plan.get("valid"):
        return None, "INVALID_RISK"
    future = candles_15m[next_index : next_index + max_holding_candles]
    outcome = simulate_trade(plan, future, max_holding_candles=max_holding_candles)
    details = gate_result_details(
        symbol, "historical_replay", result, blocked_trade=not result.passed,
        timestamp_utc=datetime.fromtimestamp(signal_time / 1000, tz=timezone.utc),
    )
    details.update(
        {
            "signal_time": _iso(signal_time),
            "entry_time": _iso(int(entry_candle.open_time or 0)),
            "exit_time": _iso(outcome["exit_time"]) if outcome["exit_time"] else None,
            "session_window": session,
            "score_bucket": score_bucket(result.score),
            "entry": plan["entry"],
            "sl": plan["sl"],
            "tp1": plan["tp1"],
            "tp2": plan["tp2"],
            **outcome,
        }
    )
    return details, None


def _cycle_failure(
    symbol: str, signal_time: int, profile: str, reason: str, session: str | None
) -> dict[str, Any]:
    result = make_gate_failure(reason, profile)
    details = gate_result_details(
        symbol, "historical_replay", result, blocked_trade=True,
        timestamp_utc=datetime.fromtimestamp(signal_time / 1000, tz=timezone.utc),
    )
    details["session_window"] = session
    details["score_bucket"] = score_bucket(0)
    return details


def summarize_profile(
    profile: str,
    cycles: list[dict[str, Any]],
    trades: list[dict[str, Any]],
    baseline_trades: list[dict[str, Any]],
) -> dict[str, Any]:
    metrics = summarize_metrics(cycles, trades)
    baseline_metrics = summarize_metrics([], baseline_trades)
    metrics["baseline_accepted_trades"] = baseline_metrics["accepted_trades"]
    metrics["baseline_expectancy_r"] = baseline_metrics["expectancy_r"]
    metrics["expectancy_improvement_vs_baseline_r"] = _subtract(
        metrics["expectancy_r"], baseline_metrics["expectancy_r"]
    )
    blocking_reasons = historical_replay_gate(metrics)
    return {
        "risk_profile": profile,
        "threshold": QUALITY_THRESHOLDS[profile],
        "historical_replay_passed": not blocking_reasons,
        "blocking_reasons": blocking_reasons,
        "metrics": metrics,
        "baseline_metrics": baseline_metrics,
        "failure_reason_counts": {
            reason: sum(
                1
                for cycle in cycles
                if cycle.get("reason") == reason
                or cycle.get("simulation_failure_reason") == reason
            )
            for reason in FAILURE_REASONS
        },
        "groups": {
            "symbol": group_metrics(cycles, trades, "symbol"),
            "risk_profile": group_metrics(cycles, trades, "risk_profile"),
            "session_window": group_metrics(cycles, trades, "session_window"),
            "score_bucket": group_metrics(cycles, trades, "score_bucket", SCORE_BUCKETS),
            "direction": group_metrics(cycles, trades, "direction"),
            "failure_reason": group_metrics(cycles, trades, "reason"),
            "trigger_pattern": group_metrics(cycles, trades, "trigger_pattern"),
            "trend_direction": group_metrics(cycles, trades, "trend_direction"),
        },
        "insights": build_insights(cycles, trades),
    }


def summarize_metrics(cycles: list[dict[str, Any]], trades: list[dict[str, Any]]) -> dict[str, Any]:
    passed = [cycle for cycle in cycles if cycle.get("passed")]
    evaluated = [
        cycle for cycle in cycles
        if cycle.get("reason") not in {"SESSION_BLOCKED", "SYMBOL_BLOCKED", "MISSING_TIMEFRAME"}
    ]
    r_values = [float(trade["r_multiple"]) for trade in trades if _finite(trade.get("r_multiple"))]
    positives = [value for value in r_values if value > 0]
    negatives = [value for value in r_values if value < 0]
    tp1_count = sum(bool(trade.get("tp1_hit")) for trade in trades)
    tp2_count = sum(trade.get("outcome") == "TP2" for trade in trades)
    scores = [float(cycle["score"]) for cycle in evaluated if _finite(cycle.get("score"))]
    return {
        "total_cycles": len(cycles),
        "evaluated_cycles": len(evaluated),
        "passed_count": len(passed),
        "blocked_count": len(cycles) - len(passed),
        "pass_rate": _ratio(len(passed), len(evaluated)),
        "accepted_trades": len(trades),
        "closed_trades": len(trades),
        "win_count": len(positives),
        "loss_count": len(negatives),
        "win_rate": _ratio(len(positives), len(trades)),
        "tp1_count": tp1_count,
        "tp2_count": tp2_count,
        "sl_count": sum(trade.get("outcome") == "SL" for trade in trades),
        "break_even_buffer_count": sum(
            trade.get("outcome") == "BREAK_EVEN_BUFFER" for trade in trades
        ),
        "time_exit_count": sum(trade.get("outcome") == "TIME_EXIT" for trade in trades),
        "ambiguous_candle_count": sum(bool(trade.get("ambiguous_candle")) for trade in trades),
        "tp1_to_tp2_ratio": _ratio(tp1_count, tp2_count),
        "expectancy_r": _mean(r_values),
        "profit_factor_r": _ratio(sum(positives), abs(sum(negatives))),
        "average_r_multiple": _mean(r_values),
        "max_drawdown_r": max_drawdown_r(trades),
        "average_score": _mean(scores),
        "median_score": round(median(scores), 6) if scores else None,
    }


def group_metrics(
    cycles: list[dict[str, Any]],
    trades: list[dict[str, Any]],
    key: str,
    required_keys: Iterable[str] = (),
) -> dict[str, dict[str, Any]]:
    values = {str(item.get(key)) for item in cycles + trades if item.get(key) is not None}
    values.update(required_keys)
    return {
        value: summarize_metrics(
            [item for item in cycles if str(item.get(key)) == value],
            [item for item in trades if str(item.get(key)) == value],
        )
        for value in sorted(values)
    }


def historical_replay_gate(metrics: dict[str, Any]) -> list[str]:
    reasons: list[str] = []
    if metrics.get("accepted_trades", 0) < 20:
        reasons.append("accepted_trades < 20")
    if (metrics.get("win_rate") or 0) < 0.58:
        reasons.append("win_rate < 58%")
    if (metrics.get("profit_factor_r") or 0) <= 1.2:
        reasons.append("profit_factor_r <= 1.2")
    if (metrics.get("expectancy_r") or 0) <= 0:
        reasons.append("expectancy_r <= 0")
    ratio = metrics.get("tp1_to_tp2_ratio")
    if ratio is None or ratio >= 20:
        reasons.append("tp1_to_tp2_ratio is unavailable or >= 20")
    return reasons


def build_insights(cycles: list[dict[str, Any]], trades: list[dict[str, Any]]) -> dict[str, Any]:
    score_groups = group_metrics(cycles, trades, "score_bucket", SCORE_BUCKETS)
    eligible_buckets = [
        (bucket, values)
        for bucket, values in score_groups.items()
        if values.get("accepted_trades", 0) > 0 and values.get("expectancy_r") is not None
    ]
    best_bucket = (
        max(eligible_buckets, key=lambda item: item[1]["expectancy_r"])[0]
        if eligible_buckets
        else None
    )
    failure_counts = Counter(
        cycle.get("simulation_failure_reason") or cycle.get("reason")
        for cycle in cycles
        if not cycle.get("passed") or cycle.get("simulation_failure_reason")
    )
    worst_failure = failure_counts.most_common(1)[0][0] if failure_counts else None
    evaluated_failure_counts = Counter(
        cycle.get("simulation_failure_reason") or cycle.get("reason")
        for cycle in cycles
        if (
            cycle.get("simulation_failure_reason")
            or (
                not cycle.get("passed")
                and cycle.get("reason")
                not in {"SESSION_BLOCKED", "SYMBOL_BLOCKED", "MISSING_TIMEFRAME"}
            )
        )
    )
    worst_evaluated_failure = (
        evaluated_failure_counts.most_common(1)[0][0] if evaluated_failure_counts else None
    )
    trigger_groups = group_metrics(cycles, trades, "trigger_pattern")
    eligible_patterns = [
        (pattern, values)
        for pattern, values in trigger_groups.items()
        if values.get("accepted_trades", 0) > 0 and values.get("expectancy_r") is not None
    ]
    worst_pattern = (
        min(eligible_patterns, key=lambda item: item[1]["expectancy_r"])[0]
        if eligible_patterns
        else None
    )
    return {
        "best_performing_score_bucket": best_bucket,
        "most_common_failure_reason": worst_failure,
        "most_common_evaluated_failure_reason": worst_evaluated_failure,
        "worst_performing_trigger_pattern": worst_pattern,
    }


def max_drawdown_r(trades: list[dict[str, Any]]) -> float:
    ordered = sorted(trades, key=lambda trade: trade.get("exit_time") or trade.get("signal_time") or "")
    equity = peak = drawdown = 0.0
    for trade in ordered:
        equity += float(trade.get("r_multiple") or 0.0)
        peak = max(peak, equity)
        drawdown = max(drawdown, peak - equity)
    return round(drawdown, 6)


def build_report(
    replay: dict[str, Any],
    *,
    db_path: Path,
    start_date: str,
    end_date: str,
    symbols: list[str],
    sessions: str,
    profiles: list[str],
    score_threshold_override: int | None,
) -> dict[str, Any]:
    passed = all(item["historical_replay_passed"] for item in replay["profiles"].values())
    blocking = {
        profile: item["blocking_reasons"]
        for profile, item in replay["profiles"].items()
        if item["blocking_reasons"]
    }
    recommendation = replay_recommendation(replay["profiles"])
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "validation_type": "historical_replay_only",
        "does_not_replace_forward_paper_validation": True,
        "capital_deployment_allowed": False,
        "ml_enabled_or_changed": False,
        "database_path": str(db_path),
        "date_range": {"start": start_date, "end": end_date},
        "symbols": symbols,
        "sessions_utc": sessions,
        "risk_profiles": profiles,
        "score_threshold_override": score_threshold_override,
        "historical_replay_passed": passed,
        "recommendation": recommendation,
        "blocking_reasons": blocking,
        "warnings": replay["warnings"],
        "risk_profile_comparison": replay["risk_profile_comparison"],
        "profiles": {
            profile: {key: value for key, value in item.items() if key != "setups"}
            for profile, item in replay["profiles"].items()
        },
    }


def render_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# IOFS Historical Replay Report",
        "",
        f"- Date range: {report['date_range']['start']} to {report['date_range']['end']}",
        f"- Symbols: {', '.join(report['symbols'])}",
        f"- Sessions UTC: {report['sessions_utc']}",
        f"- Risk profiles: {', '.join(report['risk_profiles'])}",
        f"- Historical replay passed: {str(report['historical_replay_passed']).lower()}",
        f"- Recommendation: {report['recommendation']}",
        "- Capital deployment allowed: false",
        "",
        "Historical replay is fast validation only. It does not replace Section 4 forward paper validation.",
        "",
        "## Profile Comparison",
        "",
        "| Profile | Evaluated | Accepted | Pass rate | Win rate | Profit factor | Expectancy R | Max DD R |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in report["risk_profile_comparison"]:
        profile = report["profiles"][row["risk_profile"]]
        metrics = profile["metrics"]
        lines.append(
            f"| {row['risk_profile']} | {metrics['evaluated_cycles']} | {row['accepted_trades']} | "
            f"{_pct(row['pass_rate'])} | {_pct(row['win_rate'])} | {_fmt(row['profit_factor_r'])} | "
            f"{_fmt(row['expectancy_r'])} | {_fmt(row['max_drawdown_r'])} |"
        )

    for profile_name, profile in report["profiles"].items():
        metrics = profile["metrics"]
        lines.extend(
            [
                "",
                f"## {profile_name.title()}",
                "",
                f"- Total cycles: {metrics['total_cycles']}",
                f"- Evaluated cycles: {metrics['evaluated_cycles']}",
                f"- IOFS pass rate: {_pct(metrics['pass_rate'])}",
                f"- Accepted trades: {metrics['accepted_trades']}",
                f"- Win rate: {_pct(metrics['win_rate'])}",
                f"- Profit factor R: {_fmt(metrics['profit_factor_r'])}",
                f"- Expectancy R: {_fmt(metrics['expectancy_r'])}",
                f"- Baseline expectancy R: {_fmt(metrics['baseline_expectancy_r'])}",
                f"- Expectancy improvement vs baseline R: {_fmt(metrics['expectancy_improvement_vs_baseline_r'])}",
                f"- TP1 / TP2 / SL: {metrics['tp1_count']} / {metrics['tp2_count']} / {metrics['sl_count']}",
                f"- Break-even buffer / time exit: {metrics['break_even_buffer_count']} / {metrics['time_exit_count']}",
                f"- TP1:TP2 ratio: {_fmt(metrics['tp1_to_tp2_ratio'])}",
                f"- Max drawdown R: {_fmt(metrics['max_drawdown_r'])}",
                f"- Best-performing score bucket: {profile['insights']['best_performing_score_bucket'] or 'none'}",
                f"- Most common failure reason: {profile['insights']['most_common_failure_reason'] or 'none'}",
                f"- Most common evaluated failure reason: {profile['insights']['most_common_evaluated_failure_reason'] or 'none'}",
                f"- Worst-performing trigger pattern: {profile['insights']['worst_performing_trigger_pattern'] or 'none'}",
                f"- Replay passed: {str(profile['historical_replay_passed']).lower()}",
                f"- Blocking reasons: {', '.join(profile['blocking_reasons']) or 'none'}",
                "",
                "### Score Buckets",
                "",
                "| Bucket | Accepted | Win rate | Profit factor | Expectancy R |",
                "|---|---:|---:|---:|---:|",
            ]
        )
        for bucket in SCORE_BUCKETS:
            item = profile["groups"]["score_bucket"].get(bucket, {})
            lines.append(
                f"| {bucket} | {item.get('accepted_trades', 0)} | {_pct(item.get('win_rate'))} | "
                f"{_fmt(item.get('profit_factor_r'))} | {_fmt(item.get('expectancy_r'))} |"
            )
        lines.extend(["", "### Failure Reasons", "", "| Reason | Count |", "|---|---:|"])
        for reason, count in profile["failure_reason_counts"].items():
            lines.append(f"| {reason} | {count} |")
    lines.extend(["", "## Warnings", ""])
    lines.extend(f"- {warning}" for warning in report["warnings"])
    return "\n".join(lines) + "\n"


def replay_recommendation(profiles: dict[str, dict[str, Any]]) -> str:
    if profiles and all(profile["historical_replay_passed"] for profile in profiles.values()):
        return "continue as-is while Section 4 forward paper validation continues"
    sufficiently_sampled = [
        profile
        for profile in profiles.values()
        if profile["metrics"].get("accepted_trades", 0) >= 20
    ]
    if any((profile["metrics"].get("expectancy_r") or 0) > 0 for profile in sufficiently_sampled):
        return "tune IOFS"
    if sufficiently_sampled:
        return "reject current parameters"
    return "continue building and collect more evidence"


def save_setups(path: Path, setups: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.suffix.lower() == ".csv":
        fields = sorted({key for setup in setups for key in setup})
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fields)
            writer.writeheader()
            writer.writerows(setups)
        return
    with path.open("w", encoding="utf-8") as handle:
        for setup in setups:
            handle.write(json.dumps(setup, sort_keys=True) + "\n")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--symbols", default="BTCUSDT,ETHUSDT")
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument(
        "--risk-profile",
        choices=(*PROFILE_ORDER, "all"),
        default="balanced",
    )
    parser.add_argument("--sessions", default="07:00-10:00,13:00-16:00")
    parser.add_argument("--score-threshold", type=int)
    parser.add_argument("--max-cycles", type=int)
    parser.add_argument("--max-holding-candles", type=int, default=DEFAULT_MAX_HOLD_CANDLES)
    parser.add_argument("--db-path")
    parser.add_argument("--output", required=True)
    parser.add_argument("--summary-md")
    parser.add_argument("--save-setups")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    symbols = [symbol.strip().upper() for symbol in args.symbols.split(",") if symbol.strip()]
    sessions = parse_sessions(args.sessions)
    profiles = list(PROFILE_ORDER) if args.risk_profile == "all" else [args.risk_profile]
    start = _date_start(args.start_date)
    end = _date_start(args.end_date) + timedelta(days=1)
    start_ms, end_ms = _timestamp_ms(start), _timestamp_ms(end)
    load_start_ms = _timestamp_ms(start - timedelta(days=45))
    load_end_ms = end_ms + (args.max_holding_candles * FIFTEEN_MINUTES_MS)
    db_path = resolve_db_path(args.db_path)

    print(f"[IOFS_REPLAY] database={db_path}")
    print(f"[IOFS_REPLAY] date_range={args.start_date}..{args.end_date}")
    print(f"[IOFS_REPLAY] symbols={','.join(symbols)} profiles={','.join(profiles)}")
    candles = load_historical_candles(db_path, symbols, load_start_ms, load_end_ms)
    for symbol, data in candles.items():
        print(
            f"[IOFS_REPLAY] {symbol}: 15m={len(data['15m'])} "
            f"1h={len(data['1h'])} derived_4h={len(data['4h'])}"
        )
    replay = replay_data(
        candles,
        symbols=symbols,
        start_ms=start_ms,
        end_ms=end_ms,
        profiles=profiles,
        sessions=sessions,
        score_threshold_override=args.score_threshold,
        max_cycles=args.max_cycles,
        max_holding_candles=args.max_holding_candles,
    )
    report = build_report(
        replay,
        db_path=db_path,
        start_date=args.start_date,
        end_date=args.end_date,
        symbols=symbols,
        sessions=args.sessions,
        profiles=profiles,
        score_threshold_override=args.score_threshold,
    )

    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2), encoding="utf-8")
    if args.summary_md:
        summary = Path(args.summary_md)
        summary.parent.mkdir(parents=True, exist_ok=True)
        summary.write_text(render_markdown(report), encoding="utf-8")
    if args.save_setups:
        all_setups = [
            setup
            for profile in profiles
            for setup in replay["profiles"][profile]["setups"]
        ]
        save_setups(Path(args.save_setups), all_setups)

    print(f"[IOFS_REPLAY] historical_replay_passed={report['historical_replay_passed']}")
    for row in report["risk_profile_comparison"]:
        print(
            f"[IOFS_REPLAY] {row['risk_profile']}: accepted={row['accepted_trades']} "
            f"win_rate={_pct(row['win_rate'])} profit_factor={_fmt(row['profit_factor_r'])} "
            f"expectancy_r={_fmt(row['expectancy_r'])} max_dd_r={_fmt(row['max_drawdown_r'])}"
        )
    return 0


def _date_start(value: str) -> datetime:
    return datetime.combine(date.fromisoformat(value), time.min, tzinfo=timezone.utc)


def _timestamp_ms(value: datetime) -> int:
    return int(value.timestamp() * 1000)


def _iso(value: int) -> str:
    return datetime.fromtimestamp(value / 1000, tz=timezone.utc).isoformat()


def _ratio(numerator: float, denominator: float) -> float | None:
    if not denominator:
        return None
    return round(float(numerator) / float(denominator), 6)


def _mean(values: list[float]) -> float | None:
    return round(sum(values) / len(values), 6) if values else None


def _subtract(left: float | None, right: float | None) -> float | None:
    if left is None or right is None:
        return None
    return round(left - right, 6)


def _finite(value: Any) -> bool:
    try:
        return math.isfinite(float(value))
    except (TypeError, ValueError, OverflowError):
        return False


def _pct(value: float | None) -> str:
    return "N/A" if value is None else f"{value * 100:.2f}%"


def _fmt(value: float | None) -> str:
    return "N/A" if value is None else f"{value:.4f}"


if __name__ == "__main__":
    raise SystemExit(main())
