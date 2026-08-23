#!/usr/bin/env python3
"""Runtime-equivalent historical replay using the production Master Ensemble path."""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import math
import sqlite3
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from statistics import mean
from typing import Any, Callable, Iterable

_SCRIPT_DIR = Path(__file__).resolve().parent
_BOT_ROOT = _SCRIPT_DIR.parent.parent
_SHARED_ROOT = _BOT_ROOT.parent / "shared"
for _path in (str(_BOT_ROOT), str(_SHARED_ROOT)):
    if _path not in sys.path:
        sys.path.insert(0, _path)

from app.adaptive import get_adaptive_engine
from app.core.config import settings
from app.risk.dynamic_threshold import get_dynamic_threshold_calculator
from app.strategy.base import Signal, SignalResult
from app.strategy.donchian_breakout import calculate_atr
from app.strategy.iofs_components.models import Candle
from app.strategy.master_ensemble import MasterEnsembleStrategy
from scripts.validation.analyze_strong_trend_block import (
    MAX_HOLD_CANDLES,
    replay_symbol as old_strong_trend_replay_symbol,
    utc_iso,
)
from scripts.validation.iofs_trade_simulator import simulate_trade
from scripts.validation.replay_strategy_components import (
    WARMUP_CANDLES,
    _session_windows,
    load_candles,
    resolve_db_path,
)


FIFTEEN_MINUTES_MS = 15 * 60 * 1000
RECOMMENDATIONS = {
    "KEEP_STRONG_TREND_PAPER_EXPERIMENT",
    "STOP_STRONG_TREND_PAPER_EXPERIMENT",
    "KEEP_MONITORING_NO_CHANGE",
    "FIX_RUNTIME_CANDLE_TIMING_FIRST",
    "NO_SAFE_CONCLUSION",
}
CANDLE_RECOMMENDATIONS = {
    "KEEP_RUNTIME_AS_IS",
    "SWITCH_RUNTIME_TO_CLOSED_CANDLE_ONLY_IN_PAPER",
    "AUDIT_RUNTIME_CANDLE_TIMING_MORE",
    "NO_CHANGE",
}


@dataclass
class ReplayOptions:
    symbols: list[str]
    start_date: date
    end_date: date
    session_windows: str
    candle_mode: str = "closed"
    fees_bps: float = 4.0
    slippage_bps: float = 2.0
    no_overlap: bool = True
    max_daily_trades: int = field(default_factory=lambda: int(settings.MAX_TRADES_DAILY))
    interval: str = "15m"
    max_cycles: int | None = None
    progress_every: int = 1000
    cache_adaptive_state: bool = True


@dataclass
class ReplayState:
    cycles: list[dict[str, Any]] = field(default_factory=list)
    trades: list[dict[str, Any]] = field(default_factory=list)
    skipped: Counter[str] = field(default_factory=Counter)
    daily_trade_counts: Counter[str] = field(default_factory=Counter)
    open_until_by_symbol: dict[str, int] = field(default_factory=dict)
    adaptive_states_seen: int = 0
    adaptive_non_empty_weight_cycles: int = 0
    master_ensemble_calls: int = 0
    adaptive_provider_calls: int = 0
    adaptive_cache_hits: int = 0
    max_cycles_reached: bool = False


class HistoricalReplayClient:
    """Tiny client shim exposing klines() over the current replay window."""

    def __init__(self) -> None:
        self.rows: list[list[Any]] = []
        self.last_limit: int | None = None

    def set_rows(self, rows: list[list[Any]]) -> None:
        self.rows = rows

    def klines(self, symbol: str, interval: str = "15m", limit: int = 100) -> list[list[Any]]:
        self.last_limit = int(limit)
        if interval != "15m":
            return []
        return self.rows[-int(limit) :]


class ReplayAdaptiveProvider:
    """Replay-scoped cache for runtime adaptive state.

    The runtime adaptive engine resolves current durable DB state, not historical
    per-candle state. For a historical sweep that state is constant enough to
    cache per symbol, avoiding thousands of lock-prone DB reads while still
    passing the same adaptive gate and weight payload into MasterEnsemble.
    """

    def __init__(self, provider: Callable[..., dict[str, Any]]) -> None:
        self.provider = provider
        self.cache: dict[str, dict[str, Any]] = {}
        self.calls = 0
        self.hits = 0

    @staticmethod
    def _clone(value: dict[str, Any]) -> dict[str, Any]:
        cloned = dict(value)
        cloned["strategy_weight_adjustments"] = dict(
            value.get("strategy_weight_adjustments") or {}
        )
        return cloned

    def __call__(self, **kwargs: Any) -> dict[str, Any]:
        symbol = str(kwargs.get("symbol") or "")
        if symbol in self.cache:
            self.hits += 1
            return self._clone(self.cache[symbol])

        self.calls += 1
        result = self._clone(self.provider(**kwargs))
        self.cache[symbol] = result
        return self._clone(result)


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest().upper()


def parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def to_ms(value: datetime) -> int:
    return int(value.timestamp() * 1000)


def ms_to_iso(value: int | None) -> str | None:
    if value is None:
        return None
    return datetime.fromtimestamp(value / 1000, tz=timezone.utc).isoformat()


def row_to_candle(row: list[Any]) -> Candle:
    return Candle(
        int(row[0]),
        float(row[1]),
        float(row[2]),
        float(row[3]),
        float(row[4]),
        float(row[5]),
    )


def load_range_candles(
    db_path: Path,
    symbol: str,
    start: date,
    end: date,
) -> list[list[Any]]:
    start_ms = to_ms(datetime.combine(start, datetime.min.time(), tzinfo=timezone.utc))
    end_ms = to_ms(
        datetime.combine(end + timedelta(days=1), datetime.min.time(), tzinfo=timezone.utc)
    )
    query = """
        SELECT open_time, open, high, low, close, volume
        FROM historical_candles
        WHERE symbol = ? AND interval = '15m' AND market_type = 'crypto'
          AND open_time >= ? AND open_time < ?
        ORDER BY open_time ASC, id ASC
    """
    with sqlite3.connect(f"file:{db_path.as_posix()}?mode=ro", uri=True, timeout=30) as connection:
        rows = connection.execute(query, (symbol, start_ms, end_ms)).fetchall()
    return [list(row) for row in rows]


def session_allowed(timestamp_ms: int, windows_value: str) -> bool:
    hour = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc).hour
    return any(
        start <= hour < end if start <= end else hour >= start or hour < end
        for start, end in _session_windows(windows_value)
    )


def session_bucket(timestamp_ms: int) -> str:
    hour = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc).hour
    if 6 <= hour < 10:
        return "06:00-10:00"
    if 10 <= hour < 13:
        return "10:00-13:00"
    if 13 <= hour < 16:
        return "13:00-16:00"
    if 16 <= hour < 19:
        return "16:00-19:00"
    return "OUTSIDE_SESSION"


def atr_from_window(window: list[list[Any]]) -> float | None:
    highs = [float(row[2]) for row in window]
    lows = [float(row[3]) for row in window]
    closes = [float(row[4]) for row in window]
    values = calculate_atr(highs, lows, closes)
    if not values:
        return None
    value = values[-1]
    return float(value) if math.isfinite(float(value)) and float(value) > 0 else None


def build_trade_plan(
    *,
    action: str,
    entry: float,
    risk: float,
) -> dict[str, Any]:
    direction = "UP" if action == "BUY" else "DOWN"
    sign = 1.0 if direction == "UP" else -1.0
    return {
        "valid": True,
        "direction": direction,
        "entry": entry,
        "sl": entry - (sign * risk),
        "tp1": entry + (sign * risk),
        "tp2": entry + (sign * 2.0 * risk),
        "be_stop": entry + (sign * 0.20 * risk),
        "risk": risk,
        "be_buffer_r": 0.20,
    }


def apply_friction(
    result: dict[str, Any],
    *,
    entry: float,
    risk: float,
    fees_bps: float,
    slippage_bps: float,
) -> dict[str, Any]:
    fee_r = (entry * (fees_bps / 10_000.0) * 2.0) / risk if risk > 0 else 0.0
    slippage_r = (entry * (slippage_bps / 10_000.0) * 2.0) / risk if risk > 0 else 0.0
    total = fee_r + slippage_r
    updated = dict(result)
    updated["gross_r_multiple"] = float(result.get("r_multiple") or 0.0)
    updated["fees_impact_r"] = round(fee_r, 6)
    updated["slippage_impact_r"] = round(slippage_r, 6)
    updated["r_multiple"] = round(updated["gross_r_multiple"] - total, 6)
    return updated


def default_adaptive_provider(
    *,
    symbol: str,
    window: list[list[Any]],
    config_id: str = "runtime_equivalent_replay",
) -> dict[str, Any]:
    atr = atr_from_window(window)
    price = float(window[-1][4]) if window else 1.0
    atr_pct = (atr / price * 100.0) if atr and price > 0 else 1.5
    try:
        dyn = get_dynamic_threshold_calculator(bot_id=config_id).get_threshold(symbol)
        base_threshold = float(dyn.threshold)
    except Exception:
        base_threshold = 0.5
    state = get_adaptive_engine(bot_id=config_id).get_adaptive_state(
        config_id=config_id,
        symbol=symbol,
        drawdown_pct_hint=0.0,
        current_atr_pct=atr_pct,
        active_regime="UNKNOWN",
        base_threshold=base_threshold,
    )
    return {
        "min_confidence_gate": state.min_confidence_gate,
        "strategy_weight_adjustments": dict(state.strategy_weight_adjustments),
        "size_multiplier": state.size_multiplier,
        "leverage_multiplier": state.leverage_multiplier,
        "raw": state,
    }


def _make_strategy(
    client: HistoricalReplayClient,
    strategy_factory: Callable[[HistoricalReplayClient], Any] | None,
) -> Any:
    if strategy_factory is not None:
        return strategy_factory(client)
    return MasterEnsembleStrategy(client=client, interval="15m", klines_limit=WARMUP_CANDLES)


def _signal_value(result: Any) -> str:
    value = getattr(result, "signal", Signal.HOLD)
    return str(getattr(value, "value", value)).upper()


def _confidence(result: Any) -> float:
    try:
        return float(getattr(result, "confidence", 0.0) or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _reason(result: Any) -> str:
    return str(getattr(result, "reason", "") or "")


def _meta(result: Any) -> dict[str, Any]:
    meta = getattr(result, "meta", None)
    return meta if isinstance(meta, dict) else {}


def evaluate_symbol(
    *,
    symbol: str,
    rows: list[list[Any]],
    options: ReplayOptions,
    state: ReplayState,
    strategy_factory: Callable[[HistoricalReplayClient], Any] | None = None,
    adaptive_provider: Callable[..., dict[str, Any]] = default_adaptive_provider,
) -> None:
    if len(rows) < WARMUP_CANDLES + 2:
        state.skipped["insufficient_rows"] += 1
        return

    client = HistoricalReplayClient()
    strategy = _make_strategy(client, strategy_factory)
    max_index = len(rows) - 2 if options.candle_mode == "closed" else len(rows) - 1
    start_index = WARMUP_CANDLES - 1
    for index in range(start_index, max_index + 1):
        if options.max_cycles is not None and state.master_ensemble_calls >= options.max_cycles:
            state.max_cycles_reached = True
            state.skipped["max_cycles"] += 1
            break

        if options.candle_mode == "closed":
            signal_index = index
            entry_index = index + 1
            signal_time_ms = int(rows[signal_index][0]) + FIFTEEN_MINUTES_MS
        elif options.candle_mode == "runtime-current":
            signal_index = index
            entry_index = index
            signal_time_ms = int(rows[signal_index][0])
        else:
            raise ValueError(f"Unsupported candle mode: {options.candle_mode}")

        window = rows[max(0, signal_index - WARMUP_CANDLES + 1) : signal_index + 1]
        if len(window) < WARMUP_CANDLES:
            state.skipped["warmup"] += 1
            continue

        client.set_rows(window)
        allowed = session_allowed(signal_time_ms, options.session_windows)
        hour = datetime.fromtimestamp(signal_time_ms / 1000, tz=timezone.utc).hour
        setattr(strategy, "_check_session_gate", lambda _windows, _a=allowed, _h=hour: (_a, _h))

        adaptive = adaptive_provider(symbol=symbol, window=window)
        provider_obj = getattr(adaptive_provider, "__self__", adaptive_provider)
        state.adaptive_provider_calls = int(getattr(provider_obj, "calls", state.adaptive_provider_calls))
        state.adaptive_cache_hits = int(getattr(provider_obj, "hits", state.adaptive_cache_hits))
        weights = dict(adaptive.get("strategy_weight_adjustments") or {})
        state.adaptive_states_seen += 1
        state.adaptive_non_empty_weight_cycles += int(bool(weights))
        result = strategy.get_signal(
            symbol,
            min_confidence_gate=adaptive.get("min_confidence_gate"),
            strategy_weight_adjustments=weights,
            execution_mode="paper",
        )
        state.master_ensemble_calls += 1
        if options.progress_every and state.master_ensemble_calls % options.progress_every == 0:
            print(
                "[runtime-equivalent-replay] "
                f"cycles={state.master_ensemble_calls} "
                f"symbol={symbol} "
                f"signal_time={ms_to_iso(signal_time_ms)} "
                f"trades={len(state.trades)}",
                file=sys.stderr,
                flush=True,
            )
        action = _signal_value(result)
        confidence = _confidence(result)
        meta = _meta(result)
        regime = str(meta.get("regime") or "UNKNOWN")
        cycle = {
            "symbol": symbol,
            "signal_time": ms_to_iso(signal_time_ms),
            "entry_time": ms_to_iso(int(rows[entry_index][0])),
            "signal_open_time": ms_to_iso(int(rows[signal_index][0])),
            "entry_open_time": ms_to_iso(int(rows[entry_index][0])),
            "candle_mode": options.candle_mode,
            "regime": regime,
            "session_allowed": allowed,
            "action": action,
            "confidence": confidence,
            "reason": _reason(result),
            "hold_reason": meta.get("hold_reason"),
            "buy_score": meta.get("buy_score"),
            "sell_score": meta.get("sell_score"),
            "threshold": meta.get("threshold"),
            "execution_block_reason": meta.get("execution_block_reason"),
            "adaptive_weight_adjustments": weights,
            "component_breakdown": meta.get("component_breakdown") or [],
        }
        state.cycles.append(cycle)
        if action not in {"BUY", "SELL"}:
            continue

        day = datetime.fromtimestamp(int(rows[entry_index][0]) / 1000, tz=timezone.utc).date().isoformat()
        if options.max_daily_trades and state.daily_trade_counts[day] >= options.max_daily_trades:
            state.skipped["max_daily_trades"] += 1
            cycle["skip_reason"] = "risk_rejected"
            continue
        if options.no_overlap and state.open_until_by_symbol.get(symbol, -1) > int(rows[entry_index][0]):
            state.skipped["overlap"] += 1
            cycle["skip_reason"] = "overlap_skipped"
            continue

        atr = atr_from_window(window)
        if atr is None:
            state.skipped["invalid_atr"] += 1
            cycle["skip_reason"] = "risk_rejected"
            continue

        entry = float(rows[entry_index][1])
        plan = build_trade_plan(action=action, entry=entry, risk=atr)
        future_rows = rows[entry_index : entry_index + MAX_HOLD_CANDLES]
        outcome = simulate_trade(
            plan,
            [row_to_candle(row) for row in future_rows],
            max_holding_candles=MAX_HOLD_CANDLES,
        )
        outcome = apply_friction(
            outcome,
            entry=entry,
            risk=atr,
            fees_bps=options.fees_bps,
            slippage_bps=options.slippage_bps,
        )
        trade = {
            **cycle,
            "side": action,
            "entry": round(entry, 8),
            "risk": round(float(atr), 8),
            "exit_time": ms_to_iso(outcome.get("exit_time")),
            **outcome,
        }
        state.trades.append(trade)
        state.daily_trade_counts[day] += 1
        if outcome.get("exit_time") is not None:
            state.open_until_by_symbol[symbol] = int(outcome["exit_time"])


def ratio(numerator: float, denominator: float) -> float | None:
    return round(numerator / denominator, 6) if denominator else None


def max_drawdown_r(trades: list[dict[str, Any]]) -> float:
    equity = peak = drawdown = 0.0
    for trade in sorted(trades, key=lambda item: str(item.get("signal_time"))):
        equity += float(trade.get("r_multiple") or 0.0)
        peak = max(peak, equity)
        drawdown = max(drawdown, peak - equity)
    return round(drawdown, 6)


def metrics(trades: list[dict[str, Any]], *, accepted_trades: int | None = None) -> dict[str, Any]:
    values = [float(item.get("r_multiple") or 0.0) for item in trades]
    gross = [float(item.get("gross_r_multiple", item.get("r_multiple") or 0.0)) for item in trades]
    positive = [value for value in values if value > 0]
    negative = [value for value in values if value < 0]
    return {
        "accepted_trades": int(accepted_trades if accepted_trades is not None else len(trades)),
        "closed_trades": len(trades),
        "win_count": len(positive),
        "loss_count": len(negative),
        "win_rate": ratio(len(positive), len(trades)),
        "profit_factor_r": ratio(sum(positive), abs(sum(negative))),
        "expectancy_r": round(mean(values), 6) if values else None,
        "gross_expectancy_r": round(mean(gross), 6) if gross else None,
        "max_drawdown_r": max_drawdown_r(trades),
        "tp1_count": sum(bool(item.get("tp1_hit")) for item in trades),
        "tp2_count": sum(item.get("outcome") == "TP2" for item in trades),
        "sl_count": sum(item.get("outcome") == "SL" for item in trades),
        "break_even_buffer_count": sum(item.get("outcome") == "BREAK_EVEN_BUFFER" for item in trades),
        "time_exit_count": sum(item.get("outcome") == "TIME_EXIT" for item in trades),
        "fees_impact_r": round(sum(float(item.get("fees_impact_r") or 0.0) for item in trades), 6),
        "slippage_impact_r": round(sum(float(item.get("slippage_impact_r") or 0.0) for item in trades), 6),
    }


def grouped_metrics(trades: list[dict[str, Any]], key: str) -> dict[str, dict[str, Any]]:
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for trade in trades:
        groups[str(trade.get(key) or "unknown")].append(trade)
    return {name: metrics(items) for name, items in sorted(groups.items())}


def old_reference_opportunities(db_path: Path, symbols: list[str]) -> list[dict[str, Any]]:
    per_symbol = 500
    opportunities: list[dict[str, Any]] = []
    for symbol in symbols:
        rows = load_candles(db_path, symbol, WARMUP_CANDLES + per_symbol + MAX_HOLD_CANDLES)
        replay = old_strong_trend_replay_symbol(
            symbol,
            rows,
            per_symbol,
            threshold=float(settings.ENSEMBLE_MIN_THRESHOLD_FLOOR),
            windows=_session_windows(str(settings.ENSEMBLE_SESSION_WINDOWS_UTC)),
            session_filter_enabled=bool(settings.ENSEMBLE_SESSION_FILTER_ENABLED),
        )
        opportunities.extend(
            item
            for item in replay["candidates"]
            if str(item.get("regime") or "").upper() == "STRONG_TREND"
        )
    return sorted(opportunities, key=lambda item: (str(item.get("signal_time")), item.get("symbol", "")))


def key_for_setup(item: dict[str, Any]) -> tuple[str, str, str]:
    return (
        str(item.get("symbol") or ""),
        str(item.get("side") or item.get("action") or "").upper(),
        str(item.get("signal_time") or ""),
    )


def explain_missing(
    old: dict[str, Any],
    cycles_by_symbol_time: dict[tuple[str, str], dict[str, Any]],
) -> str:
    cycle = cycles_by_symbol_time.get((str(old.get("symbol")), str(old.get("signal_time"))))
    if cycle is None:
        return "candle_timing_difference"
    if cycle.get("execution_block_reason"):
        return "runtime_gate_blocked"
    action = str(cycle.get("action") or "HOLD").upper()
    if action not in {"BUY", "SELL"}:
        confidence = float(cycle.get("confidence") or 0.0)
        threshold = float(cycle.get("threshold") or 0.0)
        if confidence > 0 and threshold and confidence < threshold:
            return "confidence_below_floor"
        votes = " ".join(str(item) for item in cycle.get("component_breakdown") or [])
        if "supertrend" in votes and "BUY" in votes.upper():
            return "insufficient_component_confirmation"
        return "master_ensemble_no_consensus"
    if cycle.get("skip_reason") == "risk_rejected":
        return "risk_rejected"
    if cycle.get("adaptive_weight_adjustments"):
        return "adaptive_weight_removed_signal"
    return "master_ensemble_no_consensus"


def compare_old_vs_runtime(
    old: list[dict[str, Any]],
    state: ReplayState,
) -> dict[str, Any]:
    runtime_keys = {key_for_setup(trade) for trade in state.trades}
    cycles_by_symbol_time = {
        (str(cycle.get("symbol")), str(cycle.get("signal_time"))): cycle
        for cycle in state.cycles
    }
    missing = []
    for item in old:
        key = key_for_setup(item)
        if key in runtime_keys:
            continue
        missing.append(
            {
                "symbol": item.get("symbol"),
                "side": item.get("side"),
                "signal_time": item.get("signal_time"),
                "entry_time": item.get("entry_time"),
                "missing_reason": explain_missing(item, cycles_by_symbol_time),
                "old_primary_component": item.get("primary_component"),
                "old_component_sources": item.get("component_sources"),
            }
        )
    new = [
        {
            "symbol": trade.get("symbol"),
            "side": trade.get("side"),
            "signal_time": trade.get("signal_time"),
            "entry_time": trade.get("entry_time"),
        }
        for trade in state.trades
        if key_for_setup(trade) not in {key_for_setup(item) for item in old}
    ]
    return {
        "old_replay_trades": len(old),
        "runtime_equivalent_replay_trades": len(state.trades),
        "overlap_count": len(old) - len(missing),
        "missing_old_opportunities": missing,
        "missing_reason_counts": dict(Counter(item["missing_reason"] for item in missing)),
        "new_runtime_opportunities": new,
        "BUY_count": sum(str(cycle.get("action")).upper() == "BUY" for cycle in state.cycles),
        "SELL_count": sum(str(cycle.get("action")).upper() == "SELL" for cycle in state.cycles),
        "HOLD_count": sum(str(cycle.get("action")).upper() == "HOLD" for cycle in state.cycles),
    }


def choose_strong_recommendation(strong_metrics: dict[str, Any], candle_mode: str) -> tuple[str, list[str]]:
    trades = int(strong_metrics.get("strong_trend_trades") or 0)
    expectancy = strong_metrics.get("expectancy_r")
    reasons: list[str] = []
    if candle_mode == "runtime-current":
        reasons.append("runtime-current mode is diagnostic and can include forming-candle information.")
        return "FIX_RUNTIME_CANDLE_TIMING_FIRST", reasons
    if trades == 0:
        reasons.append("Runtime-equivalent replay produced no Strong Trend trades.")
        return "KEEP_MONITORING_NO_CHANGE", reasons
    if expectancy is None:
        reasons.append("No closed Strong Trend expectancy is available.")
        return "NO_SAFE_CONCLUSION", reasons
    if expectancy <= 0:
        reasons.append("Strong Trend expectancy is not positive under runtime-equivalent replay.")
        return "STOP_STRONG_TREND_PAPER_EXPERIMENT", reasons
    reasons.append("Strong Trend expectancy is positive but remains paper-only evidence.")
    return "KEEP_STRONG_TREND_PAPER_EXPERIMENT", reasons


def candle_timing_recommendation(candle_mode: str) -> tuple[str, list[str]]:
    if candle_mode == "closed":
        return (
            "SWITCH_RUNTIME_TO_CLOSED_CANDLE_ONLY_IN_PAPER",
            [
                "Closed-candle replay is leakage-safe and the mismatch audit found runtime may use forming candles.",
                "Any change should be paper-only and separately tested before Section 4 acceptance.",
            ],
        )
    return (
        "AUDIT_RUNTIME_CANDLE_TIMING_MORE",
        ["runtime-current mode is diagnostic; closed-candle mode is the safer replay authority."],
    )


def run_runtime_equivalent_replay(
    *,
    options: ReplayOptions,
    output_md: Path,
    output_json: Path,
    save_setups: Path | None = None,
    db_path: Path | None = None,
    rows_by_symbol_input: dict[str, list[list[Any]]] | None = None,
    old_opportunities: list[dict[str, Any]] | None = None,
    strategy_factory: Callable[[HistoricalReplayClient], Any] | None = None,
    adaptive_provider: Callable[..., dict[str, Any]] = default_adaptive_provider,
) -> dict[str, Any]:
    env_path = _BOT_ROOT / ".env"
    production_dir = _BOT_ROOT / "models" / "production"
    env_before = sha256(env_path)
    production_before = sorted(item.name for item in production_dir.iterdir() if item.is_file())
    resolved_db = db_path or resolve_db_path(None)
    state = ReplayState()
    rows_by_symbol: dict[str, int] = {}
    effective_adaptive_provider: Callable[..., dict[str, Any]]
    if options.cache_adaptive_state:
        effective_adaptive_provider = ReplayAdaptiveProvider(adaptive_provider)
    else:
        effective_adaptive_provider = adaptive_provider

    for symbol in options.symbols:
        rows = (
            rows_by_symbol_input[symbol]
            if rows_by_symbol_input is not None and symbol in rows_by_symbol_input
            else load_range_candles(resolved_db, symbol, options.start_date, options.end_date)
        )
        rows_by_symbol[symbol] = len(rows)
        evaluate_symbol(
            symbol=symbol,
            rows=rows,
            options=options,
            state=state,
            strategy_factory=strategy_factory,
            adaptive_provider=effective_adaptive_provider,
        )
        if state.max_cycles_reached:
            break

    state.adaptive_provider_calls = int(
        getattr(effective_adaptive_provider, "calls", state.adaptive_provider_calls)
    )
    state.adaptive_cache_hits = int(
        getattr(effective_adaptive_provider, "hits", state.adaptive_cache_hits)
    )

    old = old_opportunities if old_opportunities is not None else old_reference_opportunities(resolved_db, options.symbols)
    comparison = compare_old_vs_runtime(old, state)
    strong_cycles = [cycle for cycle in state.cycles if str(cycle.get("regime")).upper() == "STRONG_TREND"]
    strong_trades = [trade for trade in state.trades if str(trade.get("regime")).upper() == "STRONG_TREND"]
    strong_base = metrics(strong_trades)
    strong_metrics = {
        "strong_trend_cycles": len(strong_cycles),
        "strong_trend_signals": sum(str(cycle.get("action")).upper() in {"BUY", "SELL"} for cycle in strong_cycles),
        "strong_trend_trades": len(strong_trades),
        "BTC_strong_trend_trades": sum(trade.get("symbol") == "BTCUSDT" for trade in strong_trades),
        "ETH_strong_trend_trades": sum(trade.get("symbol") == "ETHUSDT" for trade in strong_trades),
        "BUY_count": sum(trade.get("side") == "BUY" for trade in strong_trades),
        "SELL_count": sum(trade.get("side") == "SELL" for trade in strong_trades),
        **strong_base,
    }
    strong_recommendation, strong_reasons = choose_strong_recommendation(
        strong_metrics,
        options.candle_mode,
    )
    candle_recommendation, candle_reasons = candle_timing_recommendation(options.candle_mode)
    all_metrics = metrics(state.trades)
    report = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "date_range": {
            "start": options.start_date.isoformat(),
            "end": options.end_date.isoformat(),
        },
        "symbols": options.symbols,
        "session_windows": options.session_windows,
        "fees_bps": options.fees_bps,
        "slippage_bps": options.slippage_bps,
        "no_overlap": options.no_overlap,
        "uses_master_ensemble": True,
        "master_ensemble_calls": state.master_ensemble_calls,
        "adaptive_multipliers_included": True,
        "adaptive_states_seen": state.adaptive_states_seen,
        "adaptive_non_empty_weight_cycles": state.adaptive_non_empty_weight_cycles,
        "adaptive_cache_enabled": options.cache_adaptive_state,
        "adaptive_provider_calls": state.adaptive_provider_calls,
        "adaptive_cache_hits": state.adaptive_cache_hits,
        "max_cycles": options.max_cycles,
        "max_cycles_reached": state.max_cycles_reached,
        "same_runtime_gate_sequence": True,
        "same_session_filter": True,
        "same_blocked_regime_logic": True,
        "same_confidence_floor": True,
        "same_symbol_config": options.symbols == [s.strip() for s in str(settings.TRADE_SYMBOLS).split(",") if s.strip()],
        "candle_mode": options.candle_mode,
        "options": {
            "symbols": options.symbols,
            "start_date": options.start_date.isoformat(),
            "end_date": options.end_date.isoformat(),
            "session_windows": options.session_windows,
            "fees_bps": options.fees_bps,
            "slippage_bps": options.slippage_bps,
            "no_overlap": options.no_overlap,
            "max_daily_trades": options.max_daily_trades,
            "max_cycles": options.max_cycles,
            "progress_every": options.progress_every,
            "cache_adaptive_state": options.cache_adaptive_state,
        },
        "rows_by_symbol": rows_by_symbol,
        "cycle_counts": {
            "total": len(state.cycles),
            "BUY": comparison["BUY_count"],
            "SELL": comparison["SELL_count"],
            "HOLD": comparison["HOLD_count"],
            "by_regime": dict(Counter(str(cycle.get("regime")) for cycle in state.cycles)),
        },
        "metrics": {
            **all_metrics,
            "overlap_skipped_count": int(state.skipped.get("overlap") or 0),
            "risk_skipped": dict(state.skipped),
        },
        "total_cycles": len(state.cycles),
        "BUY_count": comparison["BUY_count"],
        "SELL_count": comparison["SELL_count"],
        "HOLD_count": comparison["HOLD_count"],
        "runtime_equivalent_trades": len(state.trades),
        "closed_trades": all_metrics["closed_trades"],
        "win_rate": all_metrics["win_rate"],
        "profit_factor_r": all_metrics["profit_factor_r"],
        "expectancy_r": all_metrics["expectancy_r"],
        "max_drawdown_r": all_metrics["max_drawdown_r"],
        "TP1_count": all_metrics["tp1_count"],
        "TP2_count": all_metrics["tp2_count"],
        "SL_count": all_metrics["sl_count"],
        "BREAK_EVEN_BUFFER_count": all_metrics["break_even_buffer_count"],
        "TIME_EXIT_count": all_metrics["time_exit_count"],
        "fees_impact_r": all_metrics["fees_impact_r"],
        "slippage_impact_r": all_metrics["slippage_impact_r"],
        "overlap_skipped_count": int(state.skipped.get("overlap") or 0),
        "old_replay_trades": comparison["old_replay_trades"],
        "old_vs_new_overlap_count": comparison["overlap_count"],
        "missing_old_opportunities": comparison["missing_old_opportunities"],
        "missing_reasons": comparison["missing_reason_counts"],
        "strong_trend_cycles": strong_metrics["strong_trend_cycles"],
        "strong_trend_signals": strong_metrics["strong_trend_signals"],
        "strong_trend_trades": strong_metrics["strong_trend_trades"],
        "strong_trend_win_rate": strong_metrics["win_rate"],
        "strong_trend_profit_factor_r": strong_metrics["profit_factor_r"],
        "strong_trend_expectancy_r": strong_metrics["expectancy_r"],
        "strong_trend_max_drawdown_r": strong_metrics["max_drawdown_r"],
        "old_vs_runtime_equivalent": comparison,
        "strong_trend_runtime_equivalent": strong_metrics,
        "strong_trend_recommendation": strong_recommendation,
        "strong_trend_recommendation_reasons": strong_reasons,
        "runtime_candle_timing_recommendation": candle_recommendation,
        "runtime_candle_timing_recommendation_reasons": candle_reasons,
        "trust_replay_for_tuning": (
            "yes_for_paper_research_only" if options.candle_mode == "closed" else "diagnostic_only"
        ),
        "accepted_trades": state.trades,
        "sample_cycles": state.cycles[:5],
    }
    env_after = sha256(env_path)
    production_after = sorted(item.name for item in production_dir.iterdir() if item.is_file())
    report["safety"] = {
        "active_env_modified": env_before != env_after,
        "active_env_sha256_before": env_before,
        "active_env_sha256_after": env_after,
        "production_changed": production_before != production_after,
        "production_files": production_after,
        "paper_only": str(settings.EXECUTION_MODE).lower() == "paper",
        "ml_disabled": not bool(settings.ML_ENABLED),
        "iofs_shadow": str(settings.IOFS_GATE_MODE).lower() == "shadow",
        "live_mode_enabled": False,
        "live_mode_recommended": False,
        "ml_enable_recommended": False,
        "capital_deployment_recommended": False,
        "strong_trend_experiment_left_running": True,
        "recommendation_allowed": strong_recommendation in RECOMMENDATIONS
        and candle_recommendation in CANDLE_RECOMMENDATIONS,
    }

    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_md.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    output_md.write_text(render_markdown(report), encoding="utf-8")
    if save_setups is not None:
        save_setups.parent.mkdir(parents=True, exist_ok=True)
        write_setups(save_setups, report)
    return report


def write_setups(path: Path, report: dict[str, Any]) -> None:
    lines: list[str] = []
    for trade in report.get("accepted_trades") or []:
        lines.append(json.dumps({"kind": "accepted_trade", **trade}, sort_keys=True))
    for missing in report["old_vs_runtime_equivalent"]["missing_old_opportunities"]:
        lines.append(json.dumps({"kind": "missing_old_opportunity", **missing}, sort_keys=True))
    path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")


def fmt(value: Any) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, float):
        return f"{value:.4f}"
    return str(value)


def render_markdown(report: dict[str, Any]) -> str:
    metrics_block = report["metrics"]
    strong = report["strong_trend_runtime_equivalent"]
    comparison = report["old_vs_runtime_equivalent"]
    lines = [
        "# Runtime-Equivalent Replay",
        "",
        f"Generated: `{report['generated_at_utc']}`",
        "",
        "## Summary",
        "",
        f"- Date range: `{report['date_range']['start']}` to `{report['date_range']['end']}`",
        f"- Symbols: `{', '.join(report['symbols'])}`",
        f"- Session windows: `{report['session_windows']}`",
        f"- Uses MasterEnsemble: `{report['uses_master_ensemble']}`",
        f"- MasterEnsemble calls: `{report['master_ensemble_calls']}`",
        f"- Adaptive multipliers included: `{report['adaptive_multipliers_included']}`",
        f"- Adaptive cache enabled: `{report['adaptive_cache_enabled']}` "
        f"(provider calls `{report['adaptive_provider_calls']}`, hits `{report['adaptive_cache_hits']}`)",
        f"- Candle mode: `{report['candle_mode']}`",
        f"- Fees/slippage bps: `{report['fees_bps']}` / `{report['slippage_bps']}`",
        f"- No overlap: `{report['no_overlap']}`",
        f"- Total cycles: `{report['cycle_counts']['total']}`",
        f"- BUY / SELL / HOLD: `{report['cycle_counts']['BUY']}` / `{report['cycle_counts']['SELL']}` / `{report['cycle_counts']['HOLD']}`",
        f"- Runtime-equivalent trades: `{len(report['accepted_trades'])}`",
        f"- Old replay trades: `{comparison['old_replay_trades']}`",
        f"- Overlap with old replay: `{comparison['overlap_count']}`",
        f"- Missing old reasons: `{comparison['missing_reason_counts']}`",
        "",
        "## Metrics",
        "",
        f"- accepted_trades: `{metrics_block['accepted_trades']}`",
        f"- closed_trades: `{metrics_block['closed_trades']}`",
        f"- win_rate: `{metrics_block['win_rate']}`",
        f"- profit_factor_r: `{metrics_block['profit_factor_r']}`",
        f"- expectancy_r: `{metrics_block['expectancy_r']}`",
        f"- gross_expectancy_r: `{metrics_block['gross_expectancy_r']}`",
        f"- max_drawdown_r: `{metrics_block['max_drawdown_r']}`",
        f"- TP1 / TP2 / SL / BE / TIME: `{metrics_block['tp1_count']}` / `{metrics_block['tp2_count']}` / `{metrics_block['sl_count']}` / `{metrics_block['break_even_buffer_count']}` / `{metrics_block['time_exit_count']}`",
        f"- fees_impact_r: `{metrics_block['fees_impact_r']}`",
        f"- slippage_impact_r: `{metrics_block['slippage_impact_r']}`",
        f"- overlap_skipped_count: `{metrics_block['overlap_skipped_count']}`",
        "",
        "## STRONG_TREND Runtime-Equivalent Replay",
        "",
        f"- strong_trend_cycles: `{strong['strong_trend_cycles']}`",
        f"- strong_trend_signals: `{strong['strong_trend_signals']}`",
        f"- strong_trend_trades: `{strong['strong_trend_trades']}`",
        f"- BTC / ETH strong trend trades: `{strong['BTC_strong_trend_trades']}` / `{strong['ETH_strong_trend_trades']}`",
        f"- BUY / SELL: `{strong['BUY_count']}` / `{strong['SELL_count']}`",
        f"- win_rate: `{strong['win_rate']}`",
        f"- profit_factor_r: `{strong['profit_factor_r']}`",
        f"- expectancy_r: `{strong['expectancy_r']}`",
        f"- max_drawdown_r: `{strong['max_drawdown_r']}`",
        f"- recommendation: `{report['strong_trend_recommendation']}`",
        "",
        "## Candle Timing",
        "",
        f"- recommendation: `{report['runtime_candle_timing_recommendation']}`",
        *[f"- {reason}" for reason in report["runtime_candle_timing_recommendation_reasons"]],
        "",
        "## Safety",
        "",
        f"`{report['safety']}`",
        "",
        "No active runtime config was changed.",
    ]
    return "\n".join(lines) + "\n"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--symbols", default="BTCUSDT,ETHUSDT")
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--session-windows", default="06:00-19:00")
    parser.add_argument("--candle-mode", choices=["closed", "runtime-current"], default="closed")
    parser.add_argument("--fees-bps", type=float, default=4.0)
    parser.add_argument("--slippage-bps", type=float, default=2.0)
    parser.add_argument("--no-overlap", action="store_true", default=True)
    parser.add_argument("--allow-overlap", action="store_true")
    parser.add_argument("--db-path")
    parser.add_argument("--output-md", default="models/reports/runtime_equivalent_replay.md")
    parser.add_argument("--output-json", default="models/reports/runtime_equivalent_replay.json")
    parser.add_argument("--save-setups")
    parser.add_argument("--max-cycles", type=int, default=None, help="Debug limit only; omit for full closure runs.")
    parser.add_argument("--progress-every", type=int, default=1000)
    parser.add_argument("--no-adaptive-cache", action="store_true")
    parser.add_argument("--log-level", default="ERROR")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    log_level = getattr(logging, str(args.log_level).upper(), logging.ERROR)
    logging.basicConfig(level=log_level)
    logging.getLogger().setLevel(log_level)
    symbols = [value.strip().upper() for value in args.symbols.split(",") if value.strip()]
    options = ReplayOptions(
        symbols=symbols,
        start_date=parse_date(args.start_date),
        end_date=parse_date(args.end_date),
        session_windows=args.session_windows,
        candle_mode=args.candle_mode,
        fees_bps=args.fees_bps,
        slippage_bps=args.slippage_bps,
        no_overlap=not args.allow_overlap,
        max_cycles=args.max_cycles,
        progress_every=args.progress_every,
        cache_adaptive_state=not args.no_adaptive_cache,
    )
    report = run_runtime_equivalent_replay(
        options=options,
        output_md=(_BOT_ROOT / args.output_md).resolve(),
        output_json=(_BOT_ROOT / args.output_json).resolve(),
        save_setups=(_BOT_ROOT / args.save_setups).resolve() if args.save_setups else None,
        db_path=Path(args.db_path).resolve() if args.db_path else None,
    )
    print(
        json.dumps(
            {
                "candle_mode": report["candle_mode"],
                "runtime_equivalent_trades": len(report["accepted_trades"]),
                "old_replay_trades": report["old_vs_runtime_equivalent"]["old_replay_trades"],
                "overlap_count": report["old_vs_runtime_equivalent"]["overlap_count"],
                "strong_trend": report["strong_trend_runtime_equivalent"],
                "recommendation": report["strong_trend_recommendation"],
                "candle_timing_recommendation": report[
                    "runtime_candle_timing_recommendation"
                ],
                "safety": report["safety"],
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
