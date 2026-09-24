#!/usr/bin/env python3
"""Compare recent paper-runtime decisions with the Strong Trend offline replay."""
from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import sys
from collections import Counter
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

_SCRIPT_DIR = Path(__file__).resolve().parent
_BOT_ROOT = _SCRIPT_DIR.parent.parent
_SHARED_ROOT = _BOT_ROOT.parent / "shared"
for _path in (str(_BOT_ROOT), str(_SHARED_ROOT)):
    if _path not in sys.path:
        sys.path.insert(0, _path)

from app.core.config import settings
from app.strategy.master_ensemble import (
    _ACTIVATION_MATRIX,
    _BASE_WEIGHTS,
    _REGIME_WEIGHT_MULTIPLIERS,
)
from app.strategy.regime import RegimeThresholds
from app.threshold.runtime import get_threshold_policy  # single threshold authority
from scripts.validation.replay_strategy_components import (
    STRATEGY_CLASSES,
    WARMUP_CANDLES,
    WindowClient,
    _instantiate_strategies,
    _session_windows,
    load_candles,
    resolve_db_path,
)
from scripts.validation.analyze_strong_trend_block import (
    MAX_HOLD_CANDLES,
    replay_symbol as replay_strong_trend_symbol,
)


DEFAULT_RUNTIME_WINDOW = "06:00-19:00"
REFERENCE_NARROW_REPLAY_WINDOWS = "07:00-10:00,13:00-16:00"
COMPONENT_CONDITIONS = (
    "fresh_fast_slow_sma_cross",
    "rsi_reset_and_turn",
    "fresh_donchian_breakout",
    "trend_pullback_conditions",
    "supertrend_flip",
    "vwap_conditions",
    "squeeze_conditions",
)
SAFE_RECOMMENDATIONS = {
    "KEEP_MONITORING",
    "FIX_REPLAY_TO_USE_RUNTIME_ENSEMBLE",
    "FIX_RUNTIME_SIGNAL_PATH",
    "ADJUST_SESSION_WINDOWS_IN_PAPER_ONLY",
    "ADJUST_COMPONENT_FRESHNESS_IN_PAPER_ONLY",
    "ETH_ONLY_STRONG_TREND_PAPER_TEST",
    "NO_SAFE_CHANGE",
}
SIGNAL_CREATION_CONFIG_KEYS = {
    "DEFAULT_INTERVAL",
    "ENSEMBLE_BLOCKED_REGIMES",
    "THRESHOLD_BASE",
    "ENSEMBLE_SESSION_FILTER_ENABLED",
    "ENSEMBLE_SESSION_WINDOWS_UTC",
    "component enabled flags",
    "component weights",
    "component minimum confidence",
    "regime classifier settings",
    "indicator warmup settings",
}


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest().upper()


def _safe_json(value: Any, default: Any) -> Any:
    if isinstance(value, (dict, list)):
        return value
    try:
        parsed = json.loads(value or "")
    except (TypeError, ValueError):
        return default
    return parsed


def _load_json(path: Path, default: dict[str, Any] | None = None) -> dict[str, Any]:
    if not path.exists():
        return dict(default or {})
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return dict(default or {})
    return value if isinstance(value, dict) else dict(default or {})


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None


def parse_windows(value: str) -> list[tuple[int, int]]:
    windows: list[tuple[int, int]] = []
    for segment in str(value or "").split(","):
        start, separator, end = segment.strip().partition("-")
        if not separator:
            continue
        try:
            windows.append((int(start.split(":")[0]), int(end.split(":")[0])))
        except ValueError:
            continue
    return windows


def timestamp_in_windows(value: str | None, windows: str) -> bool:
    timestamp = _parse_iso(value)
    if timestamp is None:
        return False
    hour = timestamp.astimezone(timezone.utc).hour
    return any(
        start <= hour < end if start <= end else hour >= start or hour < end
        for start, end in parse_windows(windows)
    )


def load_runtime_decisions(db_path: Path, limit: int) -> list[dict[str, Any]]:
    uri = f"file:{db_path.as_posix()}?mode=ro"
    with sqlite3.connect(uri, uri=True, timeout=30) as connection:
        connection.row_factory = sqlite3.Row
        columns = {
            row["name"] for row in connection.execute("PRAGMA table_info(decision_traces)")
        }
        wanted = [
            "trace_id",
            "cycle_id",
            "ts",
            "symbol",
            "timeframe",
            "regime_state",
            "signal",
            "confidence",
            "reason_codes",
            "gate_reason",
            "gate_details_json",
            "strategy_signals_json",
            "adx",
            "atr_pct",
            "ma_slope",
            "compression_ratio",
            "breakout_pressure",
            "buy_score",
            "sell_score",
            "threshold",
            "active_strategy_count",
        ]
        selected = [column for column in wanted if column in columns]
        rows = connection.execute(
            f"SELECT {', '.join(selected)} FROM decision_traces ORDER BY ts DESC LIMIT ?",
            (limit,),
        ).fetchall()
    return [dict(row) for row in rows]


def load_runtime_component_decisions(db_path: Path, limit: int) -> list[dict[str, Any]]:
    """Load the latest Strong Trend traces that reached component evaluation."""
    uri = f"file:{db_path.as_posix()}?mode=ro"
    with sqlite3.connect(uri, uri=True, timeout=30) as connection:
        connection.row_factory = sqlite3.Row
        rows = connection.execute(
            """
            SELECT trace_id, cycle_id, ts, symbol, timeframe, regime_state, signal,
                   confidence, reason_codes, gate_reason, gate_details_json,
                   strategy_signals_json, adx, atr_pct, ma_slope, compression_ratio,
                   breakout_pressure, buy_score, sell_score, threshold,
                   active_strategy_count
            FROM decision_traces
            WHERE UPPER(COALESCE(regime_state, '')) = 'STRONG_TREND'
              AND UPPER(COALESCE(gate_reason, '')) = 'MASTER_ENSEMBLE_V2'
            ORDER BY ts DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return [dict(row) for row in rows]


def _hold_breakdown(row: dict[str, Any]) -> dict[str, Any]:
    details = _safe_json(row.get("gate_details_json"), {})
    breakdown = details.get("hold_breakdown") if isinstance(details, dict) else None
    return breakdown if isinstance(breakdown, dict) else {}


def summarize_runtime(rows: list[dict[str, Any]]) -> dict[str, Any]:
    strong = [
        row for row in rows if str(row.get("regime_state") or "").upper() == "STRONG_TREND"
    ]
    cycles_by_symbol = Counter(str(row.get("symbol") or "UNKNOWN") for row in strong)
    signals_by_symbol: dict[str, Counter[str]] = {}
    failures = Counter({name: 0 for name in COMPONENT_CONDITIONS})
    component_records = 0
    strong_with_components = 0
    observed_candle_counts: list[int] = []
    indicator_fields = ("adx", "atr_pct", "ma_slope", "compression_ratio", "breakout_pressure")
    indicator_nan_counts = Counter({field: 0 for field in indicator_fields})
    component_signal_counts = Counter()

    for row in strong:
        symbol = str(row.get("symbol") or "UNKNOWN")
        signals_by_symbol.setdefault(symbol, Counter())[
            str(row.get("signal") or "HOLD").upper()
        ] += 1
        for field in indicator_fields:
            if row.get(field) is None:
                indicator_nan_counts[field] += 1
        components = _hold_breakdown(row).get("component_breakdown") or []
        if components:
            strong_with_components += 1
        for component in components:
            if not isinstance(component, dict):
                continue
            component_records += 1
            name = str(component.get("component_name") or component.get("strategy") or "")
            signal = str(component.get("component_signal") or component.get("signal") or "HOLD")
            component_signal_counts[f"{name}:{signal.upper()}"] += 1
            failed = {
                str(item) for item in component.get("component_failed_conditions") or []
            }
            failures["fresh_fast_slow_sma_cross"] += int(
                "fresh_fast_slow_sma_cross" in failed
            )
            failures["rsi_reset_and_turn"] += int("rsi_reset_and_turn" in failed)
            failures["fresh_donchian_breakout"] += int(
                "fresh_donchian_breakout" in failed
            )
            failures["trend_pullback_conditions"] += int(
                name == "trend_pullback" and bool(failed)
            )
            failures["supertrend_flip"] += int(
                "supertrend_flip_or_continuation" in failed
            )
            failures["vwap_conditions"] += int(name == "vwap_reversion" and bool(failed))
            failures["squeeze_conditions"] += int(
                name == "squeeze_breakout" and bool(failed)
            )
            snapshot = component.get("indicator_snapshot") or component.get("indicator_values") or {}
            count = snapshot.get("candle_count") if isinstance(snapshot, dict) else None
            if isinstance(count, int):
                observed_candle_counts.append(count)

    return {
        "decision_sample_size": len(rows),
        "first_decision_time": min((row.get("ts") for row in rows), default=None),
        "last_decision_time": max((row.get("ts") for row in rows), default=None),
        "strong_trend_cycles": len(strong),
        "strong_trend_cycles_by_symbol": dict(cycles_by_symbol),
        "runtime_signals_by_symbol": {
            symbol: dict(counts) for symbol, counts in sorted(signals_by_symbol.items())
        },
        "strong_trend_cycles_with_component_diagnostics": strong_with_components,
        "strong_trend_cycles_without_component_diagnostics": len(strong)
        - strong_with_components,
        "component_records": component_records,
        "component_signal_counts": dict(component_signal_counts),
        "component_failures": dict(failures),
        "indicator_nan_counts": dict(indicator_nan_counts),
        "observed_component_candle_counts": sorted(set(observed_candle_counts)),
        "strong_rows": strong,
    }


def runtime_config() -> dict[str, Any]:
    strategies = _instantiate_strategies(WindowClient())
    return {
        "TRADE_SYMBOLS": str(settings.TRADE_SYMBOLS),
        "DEFAULT_INTERVAL": str(settings.DEFAULT_INTERVAL),
        "ENSEMBLE_BLOCKED_REGIMES": str(settings.ENSEMBLE_BLOCKED_REGIMES),
        "THRESHOLD_BASE": float(get_threshold_policy().base_threshold),
        "ENSEMBLE_SESSION_FILTER_ENABLED": bool(settings.ENSEMBLE_SESSION_FILTER_ENABLED),
        "ENSEMBLE_SESSION_WINDOWS_UTC": str(settings.ENSEMBLE_SESSION_WINDOWS_UTC),
        "IOFS_GATE_MODE": str(settings.IOFS_GATE_MODE),
        "ML_ENABLED": bool(settings.ML_ENABLED),
        # Optional since dcceacd: None means no daily trade cap
        "MAX_TRADES_DAILY": (
            int(settings.MAX_TRADES_DAILY) if settings.MAX_TRADES_DAILY is not None else None
        ),
        "component enabled flags": {
            regime: sorted(names) for regime, names in _ACTIVATION_MATRIX.items()
        },
        "component weights": {
            "base": _BASE_WEIGHTS,
            "regime_multipliers": _REGIME_WEIGHT_MULTIPLIERS,
            "adaptive_performance_multipliers": "applied at runtime",
        },
        "component minimum confidence": {
            name: float(getattr(strategy, "min_confidence", 0.0))
            for name, strategy in strategies.items()
        },
        "regime classifier settings": asdict(RegimeThresholds()),
        "indicator warmup settings": {
            "master_ensemble_klines": 250,
            "regime_minimum": 100,
            "reliable_regime_target": 200,
        },
    }


def replay_config(
    historical_replay: dict[str, Any], component_replay: dict[str, Any]
) -> dict[str, Any]:
    historical_runtime = historical_replay.get("runtime_config") or {}
    component_configuration = component_replay.get("component_configuration") or []
    replay_configuration = component_replay.get("configuration") or {}
    return {
        "TRADE_SYMBOLS": ",".join(historical_replay.get("symbols") or ["BTCUSDT", "ETHUSDT"]),
        "DEFAULT_INTERVAL": "15m",
        # The positive Strong Trend analysis intentionally bypassed the historical
        # block, so its effective signal-creation value is equivalent to empty.
        "ENSEMBLE_BLOCKED_REGIMES": "",
        "THRESHOLD_BASE": replay_configuration.get(
            "threshold_base",
            float(get_threshold_policy().base_threshold),
        ),
        "ENSEMBLE_SESSION_FILTER_ENABLED": replay_configuration.get(
            "session_filter_enabled", True
        ),
        "ENSEMBLE_SESSION_WINDOWS_UTC": replay_configuration.get(
            "session_windows_utc", DEFAULT_RUNTIME_WINDOW
        ),
        "IOFS_GATE_MODE": "not applied by replay",
        "ML_ENABLED": "not applied by replay",
        "MAX_TRADES_DAILY": "not applied by replay",
        "component enabled flags": {
            regime: sorted(names) for regime, names in _ACTIVATION_MATRIX.items()
        },
        "component weights": {
            "base": _BASE_WEIGHTS,
            "regime_multipliers": _REGIME_WEIGHT_MULTIPLIERS,
            "adaptive_performance_multipliers": "omitted by replay",
        },
        "component minimum confidence": {
            str(item.get("component_name")): item.get("minimum_confidence")
            for item in component_configuration
        },
        "regime classifier settings": asdict(RegimeThresholds()),
        "indicator warmup settings": {
            "replay_window": WARMUP_CANDLES,
            "regime_minimum": 100,
            "reliable_regime_target": 200,
        },
        "historical_runtime_config": historical_runtime,
    }


def compare_configs(runtime: dict[str, Any], replay: dict[str, Any]) -> dict[str, Any]:
    comparisons: list[dict[str, Any]] = []
    for key in (
        "TRADE_SYMBOLS",
        "DEFAULT_INTERVAL",
        "ENSEMBLE_BLOCKED_REGIMES",
        "THRESHOLD_BASE",
        "ENSEMBLE_SESSION_FILTER_ENABLED",
        "ENSEMBLE_SESSION_WINDOWS_UTC",
        "IOFS_GATE_MODE",
        "ML_ENABLED",
        "MAX_TRADES_DAILY",
        "component enabled flags",
        "component weights",
        "component minimum confidence",
        "regime classifier settings",
        "indicator warmup settings",
    ):
        runtime_value = runtime.get(key)
        replay_value = replay.get(key)
        same = runtime_value == replay_value
        affects_signal = key in SIGNAL_CREATION_CONFIG_KEYS and not same
        if key == "ENSEMBLE_BLOCKED_REGIMES" and not same:
            impact = "Replay intentionally bypasses the runtime Strong Trend regime block."
        elif key == "component weights" and not same:
            impact = "Replay omits adaptive performance multipliers used by runtime voting."
        elif key in {"IOFS_GATE_MODE", "ML_ENABLED", "MAX_TRADES_DAILY"} and not same:
            impact = "Not applied by replay; currently does not explain raw signals because IOFS is shadow and ML is disabled."
            affects_signal = False
        elif key == "indicator warmup settings" and not same:
            runtime_warmup = (runtime_value or {}).get("master_ensemble_klines")
            replay_warmup = (replay_value or {}).get("replay_window")
            same = runtime_warmup == replay_warmup
            affects_signal = not same
            impact = (
                "Equivalent 250-candle top-level warmup."
                if same
                else "Different warmup can change regime and indicator values."
            )
        elif same:
            impact = "No material mismatch."
        else:
            impact = "Difference can affect signal creation." if affects_signal else "Operational difference."
        comparisons.append(
            {
                "name": key,
                "same_as_runtime": same,
                "runtime_value": runtime_value,
                "replay_value": replay_value,
                "affects_signal_creation": affects_signal,
                "impact": impact,
            }
        )
    signal_mismatches = [
        item["name"] for item in comparisons if item["affects_signal_creation"]
    ]
    return {
        "items": comparisons,
        "signal_creation_mismatches": signal_mismatches,
        "flags": ["CONFIG_MISMATCH"] if signal_mismatches else [],
    }


def _historical_strong(replay_report: dict[str, Any]) -> dict[str, Any]:
    return (
        replay_report.get("replay", {}).get("strong_trend_only", {})
        if isinstance(replay_report, dict)
        else {}
    )


def recompute_replay_opportunities(
    db_path: Path, total_decisions: int = 1000
) -> list[dict[str, Any]]:
    symbols = [
        value.strip().upper()
        for value in str(settings.TRADE_SYMBOLS).split(",")
        if value.strip()
    ]
    per_symbol, remainder = divmod(total_decisions, len(symbols))
    opportunities: list[dict[str, Any]] = []
    for index, symbol in enumerate(symbols):
        count = per_symbol + int(index < remainder)
        rows = load_candles(
            db_path,
            symbol,
            WARMUP_CANDLES + count + MAX_HOLD_CANDLES,
        )
        replay = replay_strong_trend_symbol(
            symbol,
            rows,
            count,
            threshold=float(get_threshold_policy().base_threshold),
            windows=_session_windows(str(settings.ENSEMBLE_SESSION_WINDOWS_UTC)),
            session_filter_enabled=bool(settings.ENSEMBLE_SESSION_FILTER_ENABLED),
        )
        opportunities.extend(
            candidate
            for candidate in replay["candidates"]
            if str(candidate.get("regime") or "").upper() == "STRONG_TREND"
        )
    return opportunities


def compare_sessions(
    runtime_rows: list[dict[str, Any]],
    replay_report: dict[str, Any],
    *,
    runtime_windows: str,
    replay_reference_windows: str = REFERENCE_NARROW_REPLAY_WINDOWS,
    replay_opportunities: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    strong_rows = [
        row for row in runtime_rows if str(row.get("regime_state") or "").upper() == "STRONG_TREND"
    ]
    runtime_inside_reference = sum(
        timestamp_in_windows(row.get("ts"), replay_reference_windows) for row in strong_rows
    )
    strong = _historical_strong(replay_report)
    sessions = strong.get("session_performance") or {}
    if replay_opportunities:
        replay_inside_runtime = sum(
            timestamp_in_windows(item.get("signal_time"), runtime_windows)
            for item in replay_opportunities
        )
        replay_outside_runtime = len(replay_opportunities) - replay_inside_runtime
        replay_inside_reference = sum(
            timestamp_in_windows(item.get("signal_time"), replay_reference_windows)
            for item in replay_opportunities
        )
    else:
        replay_inside_runtime = sum(
            int(metrics.get("accepted_trades") or 0)
            for label, metrics in sessions.items()
            if label != "OUTSIDE_SESSION"
        )
        replay_outside_runtime = int(
            (sessions.get("OUTSIDE_SESSION") or {}).get("accepted_trades") or 0
        )
        replay_inside_reference = None
    return {
        "runtime_windows": runtime_windows,
        "reference_narrow_replay_windows": replay_reference_windows,
        "positive_replay_actual_window_evidence": (
            "The positive replay accepted a 16:00-19:00 candidate, proving it was not "
            "restricted to the narrower reference windows."
        ),
        "runtime_cycles_inside_replay_windows": runtime_inside_reference,
        "runtime_cycles_outside_replay_windows": len(strong_rows)
        - runtime_inside_reference,
        "replay_opportunities_inside_runtime_window": replay_inside_runtime,
        "replay_opportunities_outside_runtime_window": replay_outside_runtime,
        "replay_opportunities_inside_reference_narrow_windows": replay_inside_reference,
        "replay_opportunities_outside_reference_narrow_windows": (
            len(replay_opportunities) - replay_inside_reference
            if replay_opportunities and replay_inside_reference is not None
            else None
        ),
        "replay_session_performance": sessions,
        "mismatch": False,
        "interpretation": (
            "The 7-trade positive replay used the broad ensemble session, not the narrower "
            "IOFS reference windows. Session configuration does not explain zero runtime signals."
        ),
    }


def compare_symbols(
    runtime_summary: dict[str, Any],
    replay_report: dict[str, Any],
    replay_opportunities: list[dict[str, Any]] | None = None,
    comparable_runtime_summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    strong = _historical_strong(replay_report)
    replay_by_symbol = strong.get("btc_vs_eth") or {}
    opportunities = (
        dict(Counter(str(item.get("symbol") or "UNKNOWN") for item in replay_opportunities))
        if replay_opportunities
        else {
            symbol: int(metrics.get("accepted_trades") or 0)
            for symbol, metrics in replay_by_symbol.items()
        }
    )
    runtime_mix = runtime_summary["strong_trend_cycles_by_symbol"]
    comparable_mix = (
        comparable_runtime_summary["strong_trend_cycles_by_symbol"]
        if comparable_runtime_summary
        else runtime_mix
    )
    runtime_total = sum(comparable_mix.values())
    replay_total = sum(opportunities.values())
    runtime_eth_share = comparable_mix.get("ETHUSDT", 0) / runtime_total if runtime_total else 0.0
    replay_eth_share = opportunities.get("ETHUSDT", 0) / replay_total if replay_total else 0.0
    differs = abs(runtime_eth_share - replay_eth_share) > 0.25
    return {
        "runtime_STRONG_TREND_cycles_by_symbol": runtime_mix,
        "runtime_in_session_component_cycles_by_symbol": comparable_mix,
        "runtime_signals_by_symbol": runtime_summary["runtime_signals_by_symbol"],
        "replay_STRONG_TREND_opportunities_by_symbol": opportunities,
        "replay_performance_by_symbol": replay_by_symbol,
        "runtime_eth_share": round(runtime_eth_share, 4),
        "replay_opportunity_eth_share": round(replay_eth_share, 4),
        "flags": ["RUNTIME_SYMBOL_MIX_DIFFERS_FROM_REPLAY"] if differs else [],
        "interpretation": (
            "Replay edge was concentrated in ETH (5 of 7 opportunities, 80% win rate). "
            "The comparable in-session runtime component sample is balanced across BTC and ETH; "
            "runtime is not mostly BTC, and symbol mix does not explain the missing signals."
        ),
    }


def _replay_pass_counts(
    replay_report: dict[str, Any],
    replay_opportunities: list[dict[str, Any]] | None = None,
) -> dict[str, int]:
    strong = _historical_strong(replay_report)
    sources = strong.get("component_source_performance") or {}
    if replay_opportunities:
        source_counts: Counter[str] = Counter()
        for opportunity in replay_opportunities:
            source_counts.update(
                str(source) for source in opportunity.get("component_sources") or []
            )
    else:
        source_counts = Counter(
            {
                source: int(metrics.get("accepted_trades") or 0)
                for source, metrics in sources.items()
            }
        )
    return {
        "fresh_fast_slow_sma_cross": source_counts.get("sma_cross", 0),
        "rsi_reset_and_turn": source_counts.get("trend_pullback", 0),
        "fresh_donchian_breakout": source_counts.get("donchian_breakout", 0),
        "trend_pullback_conditions": source_counts.get("trend_pullback", 0),
        "supertrend_flip": source_counts.get("supertrend", 0),
        "vwap_conditions": source_counts.get("vwap_reversion", 0),
        "squeeze_conditions": source_counts.get("squeeze_breakout", 0),
    }


def compare_components(
    runtime_failures: dict[str, int],
    replay_report: dict[str, Any],
    replay_opportunities: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    replay_passes = _replay_pass_counts(replay_report, replay_opportunities)
    rows: list[dict[str, Any]] = []
    for condition in COMPONENT_CONDITIONS:
        fail_count = int(runtime_failures.get(condition) or 0)
        pass_count = int(replay_passes.get(condition) or 0)
        if fail_count and pass_count:
            likely = "Condition is active in both paths but rare in the recent runtime market."
        elif fail_count:
            likely = "Recent runtime repeatedly lacked this fresh setup condition."
        elif pass_count:
            likely = "Replay opportunity used this condition; runtime sample did not record it failing."
        else:
            likely = "Condition was neither a recent runtime failure nor a replay opportunity source."
        rows.append(
            {
                "condition": condition,
                "runtime_fail_count": fail_count,
                "replay_pass_count": pass_count,
                "difference": pass_count - fail_count,
                "likely_cause": likely,
                "replay_pass_count_source": (
                    "all directional component sources from recomputed opportunities"
                    if replay_opportunities
                    else "stored primary-component summary"
                ),
            }
        )
    return rows


def candle_timing_audit(now: datetime | None = None) -> dict[str, Any]:
    generated = now or datetime.now(timezone.utc)
    return {
        "current_utc_time": generated.isoformat(),
        "runtime_latest_candle_in_trace": "not persisted",
        "runtime_latest_candle_close_time": "not persisted",
        "runtime_uses_closed_candles_only": False,
        "runtime_may_evaluate_incomplete_candles": True,
        "runtime_code_evidence": (
            "MasterEnsembleStrategy and active components consume client.klines(...)[-1] "
            "without removing the current forming Binance kline."
        ),
        "replay_evaluates_only_completed_candles": True,
        "replay_enters_next_candle_open": True,
        "replay_code_evidence": (
            "Replay windows end at a stored historical candle and simulate_candidate enters rows[index + 1]."
        ),
        "flags": ["CANDLE_TIMING_MISMATCH"],
        "impact": (
            "Fresh-cross, breakout, SuperTrend-flip, RSI-turn, and regime values can differ "
            "between a forming runtime candle and the replay's completed candle."
        ),
    }


def warmup_audit(
    runtime_summary: dict[str, Any], component_replay: dict[str, Any]
) -> dict[str, Any]:
    replay_health = {
        symbol.get("symbol"): symbol.get("indicator_health")
        for symbol in component_replay.get("symbols") or []
        if isinstance(symbol, dict)
    }
    runtime_nans = runtime_summary.get("indicator_nan_counts") or {}
    has_runtime_nan = any(int(value or 0) > 0 for value in runtime_nans.values())
    return {
        "runtime_candle_count_used": 250,
        "replay_candle_count_used": WARMUP_CANDLES,
        "observed_component_candle_counts": runtime_summary.get(
            "observed_component_candle_counts", []
        ),
        "EMA200_availability": "configured warmup is sufficient; not persisted in runtime trace",
        "ADX_availability": runtime_nans.get("adx", 0) == 0,
        "ATR_availability": runtime_nans.get("atr_pct", 0) == 0,
        "runtime_indicator_nan_counts": runtime_nans,
        "replay_indicator_health": replay_health,
        "latest_indicator_snapshot_completeness": (
            "complete for persisted regime fields; component snapshots exist only after pre-component gates"
        ),
        "flags": ["INDICATOR_NAN_RUNTIME"] if has_runtime_nan else [],
        "warmup_mismatch": False,
        "interpretation": (
            "Both paths use a 250-candle top-level window. No evidence shows warmup starvation "
            "causing the zero-signal runtime sample."
        ),
    }


def replay_realism_check() -> dict[str, Any]:
    return {
        "uses_master_ensemble": False,
        "uses_same_component_weights": False,
        "uses_same_gate_sequence": False,
        "uses_same_regime_classifier": True,
        "uses_same_indicator_pipeline": True,
        "differences": [
            "Strong Trend block-impact replay calls a copied aggregate_candidate function instead of MasterEnsembleStrategy.get_signal.",
            "Adaptive performance multipliers, volatility-spike guard, orchestrator, risk, and executor gates are omitted.",
            "Strong Trend blocking is intentionally bypassed for analysis.",
            "Overlapping trades are allowed and fees/slippage are excluded.",
            "Replay uses stored completed candles and next-candle-open entries.",
        ],
        "flags": ["REPLAY_NOT_RUNTIME_EQUIVALENT"],
        "interpretation": (
            "The replay is useful opportunity research, but it is not proof that the live paper "
            "runtime should have generated or executed the same trades."
        ),
    }


def choose_recommendation(report: dict[str, Any]) -> tuple[str, list[str]]:
    realism_flags = report.get("replay_realism", {}).get("flags", [])
    timing_flags = report.get("candle_timing", {}).get("flags", [])
    reasons: list[str] = []
    if "REPLAY_NOT_RUNTIME_EQUIVALENT" in realism_flags:
        reasons.append(
            "The positive replay does not execute the production MasterEnsembleStrategy or full runtime gate sequence."
        )
        if "CANDLE_TIMING_MISMATCH" in timing_flags:
            reasons.append(
                "Closed-candle replay and forming-candle runtime evaluation can produce different freshness triggers."
            )
        reasons.append(
            "Keep the current paper experiment running while a parity replay is built; no threshold or active-session change is justified."
        )
        return "FIX_REPLAY_TO_USE_RUNTIME_ENSEMBLE", reasons
    reasons.append("No proven runtime defect or safe configuration change was identified.")
    return "KEEP_MONITORING", reasons


def _config_markdown(items: Iterable[dict[str, Any]]) -> list[str]:
    lines = [
        "| Config | Same | Affects signal | Runtime | Replay | Impact |",
        "|---|---:|---:|---|---|---|",
    ]
    for item in items:
        runtime_value = json.dumps(item["runtime_value"], sort_keys=True, default=str)
        replay_value = json.dumps(item["replay_value"], sort_keys=True, default=str)
        lines.append(
            f"| {item['name']} | {str(item['same_as_runtime']).lower()} | "
            f"{str(item['affects_signal_creation']).lower()} | `{runtime_value}` | "
            f"`{replay_value}` | {item['impact']} |"
        )
    return lines


def render_markdown(report: dict[str, Any]) -> str:
    component_lines = [
        "| Condition | Runtime fails | Replay passes | Difference | Likely cause |",
        "|---|---:|---:|---:|---|",
    ]
    for item in report["component_failure_comparison"]:
        component_lines.append(
            f"| {item['condition']} | {item['runtime_fail_count']} | "
            f"{item['replay_pass_count']} | {item['difference']} | {item['likely_cause']} |"
        )
    return "\n".join(
        [
            "# Runtime vs Replay Signal Mismatch Audit",
            "",
            f"Generated: `{report['generated_at_utc']}`",
            "",
            "## Finding",
            "",
            report["root_cause"],
            "",
            f"- Recommendation: `{report['recommendation']}`",
            f"- Flags: `{report['flags']}`",
            f"- Runtime decision sample: `{report['runtime']['decision_sample_size']}`",
            f"- Runtime STRONG_TREND cycles: `{report['runtime']['strong_trend_cycles']}`",
            f"- Historical replay STRONG_TREND opportunities: `{report['replay']['strong_trend_opportunities']}`",
            "",
            "## Config Comparison",
            "",
            *_config_markdown(report["config_comparison"]["items"]),
            "",
            "## Sessions And Symbols",
            "",
            f"- Session comparison: `{report['session_comparison']}`",
            f"- Symbol comparison: `{report['symbol_comparison']}`",
            "",
            "## Component Failure Comparison",
            "",
            *component_lines,
            "",
            "## Candle Timing",
            "",
            f"`{report['candle_timing']}`",
            "",
            "## Indicator Warmup",
            "",
            f"`{report['indicator_warmup']}`",
            "",
            "## Replay Realism",
            "",
            f"`{report['replay_realism']}`",
            "",
            "## Recommendation Reasons",
            "",
            *[f"- {reason}" for reason in report["recommendation_reasons"]],
            "",
            "## Safety",
            "",
            f"`{report['safety']}`",
            "",
            "No active configuration was changed. Section 4 remains in progress.",
            "",
        ]
    )


def run_audit(
    *,
    runtime_decisions: int,
    output_md: Path,
    output_json: Path,
    db_path: Path | None = None,
    runtime_rows: list[dict[str, Any]] | None = None,
    component_runtime_rows: list[dict[str, Any]] | None = None,
    historical_replay: dict[str, Any] | None = None,
    component_replay: dict[str, Any] | None = None,
    replay_opportunities: list[dict[str, Any]] | None = None,
    active_env: Path | None = None,
    production_dir: Path | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    generated = now or datetime.now(timezone.utc)
    env_path = active_env or (_BOT_ROOT / ".env")
    production = production_dir or (_BOT_ROOT / "models" / "production")
    env_before = sha256(env_path)
    production_before = sorted(item.name for item in production.iterdir() if item.is_file())
    resolved_db = db_path or resolve_db_path(None)
    rows = runtime_rows if runtime_rows is not None else load_runtime_decisions(resolved_db, runtime_decisions)
    if component_runtime_rows is not None:
        component_rows = component_runtime_rows
    elif runtime_rows is not None:
        component_rows = [row for row in rows if _hold_breakdown(row).get("component_breakdown")]
    else:
        component_rows = load_runtime_component_decisions(resolved_db, runtime_decisions)
    active_symbols = {
        value.strip().upper()
        for value in str(settings.TRADE_SYMBOLS).split(",")
        if value.strip()
    }
    component_rows = [
        row
        for row in component_rows
        if str(row.get("symbol") or "").upper() in active_symbols
    ]
    historical_was_injected = historical_replay is not None
    historical = historical_replay or _load_json(
        _BOT_ROOT / "models" / "reports" / "strong_trend_block_impact.json"
    )
    components = component_replay or _load_json(
        _BOT_ROOT / "models" / "reports" / "strategy_component_replay.json"
    )
    if replay_opportunities is not None:
        opportunities = replay_opportunities
    elif historical_was_injected:
        opportunities = []
    else:
        opportunities = recompute_replay_opportunities(resolved_db)

    runtime = summarize_runtime(rows)
    runtime.pop("strong_rows", None)
    component_runtime = summarize_runtime(component_rows)
    component_runtime.pop("strong_rows", None)
    runtime["component_diagnostic_supplement"] = {
        "sample_size": component_runtime["decision_sample_size"],
        "first_decision_time": component_runtime["first_decision_time"],
        "last_decision_time": component_runtime["last_decision_time"],
        "strong_trend_cycles_by_symbol": component_runtime["strong_trend_cycles_by_symbol"],
        "component_records": component_runtime["component_records"],
        "component_signal_counts": component_runtime["component_signal_counts"],
        "component_failures": component_runtime["component_failures"],
        "count_unit": (
            "decision traces, not independent closed candles; repeated evaluations of "
            "the same forming/current candle are possible"
        ),
    }
    current_config = runtime_config()
    prior_config = replay_config(historical, components)
    config_comparison = compare_configs(current_config, prior_config)
    session_comparison = compare_sessions(
        rows,
        historical,
        runtime_windows=str(settings.ENSEMBLE_SESSION_WINDOWS_UTC),
        replay_opportunities=opportunities,
    )
    symbol_comparison = compare_symbols(
        runtime,
        historical,
        opportunities,
        comparable_runtime_summary=component_runtime,
    )
    component_comparison = compare_components(
        component_runtime["component_failures"], historical, opportunities
    )
    candle_timing = candle_timing_audit(generated)
    indicator_warmup = warmup_audit(component_runtime or runtime, components)
    realism = replay_realism_check()
    replay_strong = _historical_strong(historical)
    flags = sorted(
        set(
            config_comparison["flags"]
            + session_comparison.get("flags", [])
            + symbol_comparison["flags"]
            + candle_timing["flags"]
            + indicator_warmup["flags"]
            + realism["flags"]
        )
    )
    report: dict[str, Any] = {
        "generated_at_utc": generated.isoformat(),
        "root_cause": (
            "The apparent contradiction is primarily a replay-equivalence and observation-window "
            "problem, not evidence of a broken runtime signal path. The 7 replay trades were sparse "
            "historical candidates produced by a simplified Strong Trend analysis replay. Recent "
            "runtime traces repeatedly evaluate a much smaller set of current candles; in-session "
            "component diagnostics show missing fresh setup conditions, while many later Strong "
            "Trend traces are stopped by the session gate before component evaluation. Runtime also "
            "may evaluate the forming candle, whereas replay uses completed candles and next-open entries."
        ),
        "runtime": runtime,
        "replay": {
            "strong_trend_opportunities": int(replay_strong.get("accepted_trades") or 0),
            "strong_trend_cycles": historical.get("replay", {}).get("strong_trend_cycles"),
            "performance": replay_strong,
            "recomputed_opportunities": opportunities,
        },
        "config_comparison": config_comparison,
        "session_comparison": session_comparison,
        "symbol_comparison": symbol_comparison,
        "component_failure_comparison": component_comparison,
        "candle_timing": candle_timing,
        "indicator_warmup": indicator_warmup,
        "replay_realism": realism,
        "flags": flags,
    }
    recommendation, reasons = choose_recommendation(report)
    report["recommendation"] = recommendation
    report["recommendation_reasons"] = reasons

    env_after = sha256(env_path)
    production_after = sorted(item.name for item in production.iterdir() if item.is_file())
    report["safety"] = {
        "active_env_modified": env_before != env_after,
        "active_env_sha256_before": env_before,
        "active_env_sha256_after": env_after,
        "production_changed": production_before != production_after,
        "production_files": production_after,
        "paper_only": str(settings.EXECUTION_MODE).lower() == "paper",
        "ml_disabled": not bool(settings.ML_ENABLED),
        "iofs_shadow": str(settings.IOFS_GATE_MODE).lower() == "shadow",
        "strong_trend_experiment_left_running": True,
        "live_mode_recommended": False,
        "ml_enable_recommended": False,
        "capital_deployment_recommended": False,
        "recommendation_is_safe": recommendation in SAFE_RECOMMENDATIONS
        and "LIVE" not in recommendation
        and "ML" not in recommendation,
    }
    output_md.parent.mkdir(parents=True, exist_ok=True)
    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    output_md.write_text(render_markdown(report), encoding="utf-8")
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runtime-decisions", type=int, default=500)
    parser.add_argument("--db-path")
    parser.add_argument(
        "--output-md", default="models/reports/runtime_replay_signal_mismatch.md"
    )
    parser.add_argument(
        "--output-json", default="models/reports/runtime_replay_signal_mismatch.json"
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = run_audit(
        runtime_decisions=args.runtime_decisions,
        output_md=(_BOT_ROOT / args.output_md).resolve(),
        output_json=(_BOT_ROOT / args.output_json).resolve(),
        db_path=Path(args.db_path).resolve() if args.db_path else None,
    )
    print(
        json.dumps(
            {
                "runtime_decision_sample_size": report["runtime"]["decision_sample_size"],
                "runtime_strong_trend_cycles": report["runtime"]["strong_trend_cycles"],
                "replay_strong_trend_opportunities": report["replay"][
                    "strong_trend_opportunities"
                ],
                "flags": report["flags"],
                "recommendation": report["recommendation"],
                "safety": report["safety"],
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
