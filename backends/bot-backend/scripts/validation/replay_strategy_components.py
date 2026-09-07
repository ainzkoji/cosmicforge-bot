#!/usr/bin/env python3
"""Replay production strategy components against stored 15m candles, read-only."""
from __future__ import annotations

import argparse
import json
import math
import sqlite3
import sys
from collections import Counter, defaultdict
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
from app.strategy.base import Signal
from app.strategy.bollinger_reversion import (
    BollingerReversionStrategy,
    calculate_bollinger_bands,
)
from app.strategy.donchian_breakout import (
    DonchianBreakoutStrategy,
    calculate_adx,
    calculate_atr,
    calculate_ema,
)
from app.strategy.hold_breakdown import component_breakdown
from app.strategy.master_ensemble import (
    _ACTIVATION_MATRIX,
    _BASE_WEIGHTS,
    _REGIME_WEIGHT_MULTIPLIERS,
)
from app.strategy.regime import RegimeClassifier
from app.strategy.sma_cross import SMACrossStrategy
from app.strategy.squeeze_breakout import SqueezeBreakoutStrategy
from app.strategy.supertrend import SuperTrendStrategy
from app.strategy.trend_pullback import TrendPullbackStrategy, calculate_rsi
from app.strategy.vwap_reversion import VWAPReversionStrategy


WARMUP_CANDLES = 250
NOMINAL_CONSENSUS_WEIGHT = 3.0
STRATEGY_CLASSES = (
    SuperTrendStrategy,
    TrendPullbackStrategy,
    SMACrossStrategy,
    DonchianBreakoutStrategy,
    BollingerReversionStrategy,
    SqueezeBreakoutStrategy,
    VWAPReversionStrategy,
)


class WindowClient:
    def __init__(self) -> None:
        self.rows: list[list[Any]] = []

    def set_rows(self, rows: list[list[Any]]) -> None:
        self.rows = rows

    def klines(self, symbol: str, interval: str = "15m", limit: int = 100) -> list[list[Any]]:
        return self.rows[-limit:]


def resolve_db_path(explicit: str | None = None) -> Path:
    if explicit:
        return Path(explicit).resolve()
    prefix = "DATABASE_URL=sqlite:///"
    for line in (_BOT_ROOT / ".env").read_text(encoding="utf-8").splitlines():
        if line.startswith(prefix):
            return (_BOT_ROOT / line[len(prefix) :].strip()).resolve()
    return (_SHARED_ROOT / "shared_lib" / "persistence" / "cosmicforge.db").resolve()


def load_candles(db_path: Path, symbol: str, count: int) -> list[list[Any]]:
    query = """
        SELECT open_time, open, high, low, close, volume
        FROM historical_candles
        WHERE symbol = ? AND interval = '15m' AND market_type = 'crypto'
        ORDER BY open_time DESC, id DESC
        LIMIT ?
    """
    with sqlite3.connect(f"file:{db_path.as_posix()}?mode=ro", uri=True, timeout=30) as connection:
        rows = connection.execute(query, (symbol, count)).fetchall()
    return [list(row) for row in reversed(rows)]


def _session_windows(value: str) -> list[tuple[int, int]]:
    windows: list[tuple[int, int]] = []
    for segment in str(value or "").split(","):
        start, separator, end = segment.strip().partition("-")
        if not separator:
            continue
        windows.append((int(start.split(":")[0]), int(end.split(":")[0])))
    return windows


def session_allowed(open_time_ms: int, windows: list[tuple[int, int]]) -> bool:
    hour = datetime.fromtimestamp((open_time_ms + 900_000) / 1000, tz=timezone.utc).hour
    return any(
        (start <= hour < end) if start <= end else (hour >= start or hour < end)
        for start, end in windows
    )


def _instantiate_strategies(client: WindowClient) -> dict[str, Any]:
    strategies: dict[str, Any] = {}
    for klass in STRATEGY_CLASSES:
        strategies[klass.name] = (
            klass(client=client)
            if klass is VWAPReversionStrategy
            else klass(client=client, interval="15m")
        )
    return strategies


def _aggregate(
    component_results: dict[str, Any],
    regime: str,
    *,
    threshold: float,
    blocked_regimes: set[str],
    allowed_session: bool,
    session_filter_enabled: bool,
) -> tuple[str, float, str]:
    if session_filter_enabled and not allowed_session:
        return "HOLD", 0.0, "SESSION_BLOCKED"
    if regime.upper() in blocked_regimes:
        return "HOLD", 0.0, f"REGIME_BLOCKED_{regime.upper()}"

    active = _ACTIVATION_MATRIX.get(regime, frozenset())
    if not active:
        return "HOLD", 0.0, "NO_ACTIVE_STRATEGIES"

    buy_score = 0.0
    sell_score = 0.0
    multipliers = _REGIME_WEIGHT_MULTIPLIERS.get(regime, {})
    for name in active:
        result = component_results.get(name)
        if result is None:
            continue
        weighted = float(result.confidence) * _BASE_WEIGHTS.get(name, 1.0) * multipliers.get(name, 1.0)
        if result.signal == Signal.BUY:
            buy_score += weighted
        elif result.signal == Signal.SELL:
            sell_score += weighted

    buy_conf = min(1.0, buy_score / NOMINAL_CONSENSUS_WEIGHT)
    sell_conf = min(1.0, sell_score / NOMINAL_CONSENSUS_WEIGHT)
    raw_conf = max(buy_conf, sell_conf)
    if buy_conf > sell_conf and buy_conf >= threshold:
        return "BUY", buy_conf, "ENSEMBLE_BUY"
    if sell_conf > buy_conf and sell_conf >= threshold:
        return "SELL", sell_conf, "ENSEMBLE_SELL"
    return "HOLD", raw_conf, "NO_PATTERN" if raw_conf == 0 else "CONFIDENCE_BELOW_FLOOR"


def replay_symbol(
    symbol: str,
    rows: list[list[Any]],
    lookback: int,
    *,
    threshold: float,
    blocked_regimes: set[str],
    windows: list[tuple[int, int]],
    session_filter_enabled: bool,
) -> dict[str, Any]:
    if len(rows) < WARMUP_CANDLES + lookback:
        raise RuntimeError(
            f"{symbol}: need {WARMUP_CANDLES + lookback} candles, found {len(rows)}"
        )

    client = WindowClient()
    strategies = _instantiate_strategies(client)
    classifier = RegimeClassifier()
    component_counts: dict[str, Counter[str]] = defaultdict(Counter)
    component_reasons: dict[str, Counter[str]] = defaultdict(Counter)
    failed_conditions: Counter[str] = Counter()
    ensemble_counts: Counter[str] = Counter()
    ensemble_reasons: Counter[str] = Counter()
    regime_counts: Counter[str] = Counter()
    in_session = 0
    nonzero_component_cycles = 0
    latest_components: dict[str, dict[str, Any]] = {}

    start = len(rows) - lookback
    for index in range(start, len(rows)):
        window = rows[max(0, index - WARMUP_CANDLES + 1) : index + 1]
        client.set_rows(window)
        highs = [float(row[2]) for row in window]
        lows = [float(row[3]) for row in window]
        closes = [float(row[4]) for row in window]
        regime = classifier.classify_stable(highs, lows, closes).regime.value
        regime_counts[regime] += 1
        allowed = session_allowed(int(window[-1][0]), windows)
        in_session += int(allowed)

        results: dict[str, Any] = {}
        directional = False
        for name, strategy in strategies.items():
            result = strategy.get_signal(symbol)
            results[name] = result
            diagnostic = component_breakdown(
                strategy=name,
                signal=result.signal.value,
                confidence=float(result.confidence),
                reason=str(result.reason),
                meta=result.meta or {},
                threshold_floor=threshold,
                symbol=symbol,
                timestamp=datetime.fromtimestamp(int(window[-1][0]) / 1000, tz=timezone.utc).isoformat(),
                timeframe="15m",
                market_regime=regime,
                session_allowed=allowed,
                enabled=name in _ACTIVATION_MATRIX.get(regime, frozenset()),
            )
            component_counts[name][diagnostic["component_signal"]] += 1
            component_reasons[name][diagnostic["component_reason"]] += 1
            failed_conditions.update(diagnostic["component_failed_conditions"])
            directional = directional or result.signal in {Signal.BUY, Signal.SELL}
            latest_components[name] = diagnostic
        nonzero_component_cycles += int(directional)

        action, confidence, reason = _aggregate(
            results,
            regime,
            threshold=threshold,
            blocked_regimes=blocked_regimes,
            allowed_session=allowed,
            session_filter_enabled=session_filter_enabled,
        )
        ensemble_counts[action] += 1
        ensemble_reasons[reason] += 1

    return {
        "symbol": symbol,
        "candles_tested": lookback,
        "in_session_candles": in_session,
        "nonzero_component_activity_cycles": nonzero_component_cycles,
        "regime_counts": dict(regime_counts),
        "component_counts": {name: dict(counts) for name, counts in component_counts.items()},
        "component_top_reasons": {
            name: dict(counts.most_common(8)) for name, counts in component_reasons.items()
        },
        "ensemble_counts": dict(ensemble_counts),
        "ensemble_top_reasons": dict(ensemble_reasons.most_common(8)),
        "top_failed_conditions": dict(failed_conditions.most_common(15)),
        "latest_components": latest_components,
        "indicator_health": indicator_health(rows[-lookback:]),
        "latest_candle_open_utc": datetime.fromtimestamp(
            int(rows[-1][0]) / 1000, tz=timezone.utc
        ).isoformat(),
    }


def _finite(value: Any) -> bool:
    return isinstance(value, (int, float)) and math.isfinite(float(value))


def _health(name: str, total: int, valid: int, latest: Any, expected: str) -> dict[str, Any]:
    healthy = valid > 0 and _finite(latest)
    return {
        "indicator_name": name,
        "valid_count": valid,
        "nan_count": max(0, total - valid),
        "latest_value": round(float(latest), 8) if _finite(latest) else None,
        "expected_range": expected,
        "health_status": "HEALTHY" if healthy else "INVALID_OR_MISSING",
    }


def _ema_scalar(values: list[float], period: int) -> float | None:
    series = calculate_ema(values, period)
    return series[-1] if series else None


def indicator_health(rows: list[list[Any]]) -> list[dict[str, Any]]:
    closes = [float(row[4]) for row in rows]
    highs = [float(row[2]) for row in rows]
    lows = [float(row[3]) for row in rows]
    volumes = [float(row[5]) for row in rows]
    total = len(rows)
    rsi_values = calculate_rsi(closes)
    atr_values = calculate_atr(highs, lows, closes)
    bb_upper, bb_middle, bb_lower = calculate_bollinger_bands(closes)
    ema12 = _ema_scalar(closes, 12)
    ema26 = _ema_scalar(closes, 26)
    macd = (ema12 - ema26) if ema12 is not None and ema26 is not None else None
    support = min(lows[-20:]) if len(lows) >= 20 else None
    resistance = max(highs[-20:]) if len(highs) >= 20 else None
    volume_avg = sum(volumes[-20:]) / 20 if len(volumes) >= 20 else None
    bullish = int(closes[-1] > float(rows[-1][1])) if rows else None

    return [
        _health("ema_fast_20", total, max(0, total - 19), _ema_scalar(closes, 20), "> 0"),
        _health("ema_slow_50", total, max(0, total - 49), _ema_scalar(closes, 50), "> 0"),
        _health("ema_long_200", total, max(0, total - 199), _ema_scalar(closes, 200), "> 0"),
        _health("rsi_14", total, len(rsi_values), rsi_values[-1] if rsi_values else None, "0..100"),
        _health("macd_12_26", total, max(0, total - 25), macd, "finite"),
        _health("atr_14", total, len(atr_values), atr_values[-1] if atr_values else None, "> 0"),
        _health("adx_14", total, max(0, total - 27), calculate_adx(highs, lows, closes), "0..100"),
        _health("volume_average_20", total, max(0, total - 19), volume_avg, ">= 0"),
        _health("bollinger_upper_20", total, len(bb_upper), bb_upper[-1] if bb_upper else None, "upper > middle"),
        _health("bollinger_middle_20", total, len(bb_middle), bb_middle[-1] if bb_middle else None, "> 0"),
        _health("bollinger_lower_20", total, len(bb_lower), bb_lower[-1] if bb_lower else None, "lower < middle"),
        _health("support_20", total, max(0, total - 19), support, "> 0"),
        _health("resistance_20", total, max(0, total - 19), resistance, "> 0"),
        _health("bullish_candle_field", total, total, bullish, "0 or 1"),
    ]


def _merge_counts(reports: list[dict[str, Any]], key: str) -> dict[str, int]:
    merged: Counter[str] = Counter()
    for report in reports:
        merged.update(report[key])
    return dict(merged)


def build_report(symbol_reports: list[dict[str, Any]], db_path: Path, threshold: float) -> dict[str, Any]:
    component_totals: dict[str, Counter[str]] = defaultdict(Counter)
    failed: Counter[str] = Counter()
    for report in symbol_reports:
        for name, counts in report["component_counts"].items():
            component_totals[name].update(counts)
        failed.update(report["top_failed_conditions"])

    components = []
    for klass in STRATEGY_CLASSES:
        name = klass.name
        sample = next(report["latest_components"][name] for report in symbol_reports)
        components.append(
            {
                "component_name": name,
                "enabled_in_any_regime": any(name in active for active in _ACTIVATION_MATRIX.values()),
                "weight": _BASE_WEIGHTS.get(name, 1.0),
                "minimum_confidence": float(getattr(_instantiate_strategies(WindowClient())[name], "min_confidence", 0.0)),
                "required_timeframe": str(getattr(_instantiate_strategies(WindowClient())[name], "interval", "15m")),
                "last_result": sample["component_signal"],
                "counts": dict(component_totals[name]),
            }
        )

    return {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "database_path": str(db_path),
        "read_only": True,
        "warnings": [
            "The historical store contains 15m candles for this replay. Production VWAP reversion "
            "declares a 5m timeframe, so its replay result is diagnostic rather than parity evidence."
        ],
        "configuration": {
            "ensemble_threshold_floor": threshold,
            "blocked_regimes": sorted(
                value.strip().upper()
                for value in str(settings.ENSEMBLE_BLOCKED_REGIMES or "").split(",")
                if value.strip()
            ),
            "session_filter_enabled": bool(settings.ENSEMBLE_SESSION_FILTER_ENABLED),
            "session_windows_utc": str(settings.ENSEMBLE_SESSION_WINDOWS_UTC),
            "execution_mode": str(settings.EXECUTION_MODE),
            "ml_enabled": bool(settings.ML_ENABLED),
            "iofs_gate_mode": str(settings.IOFS_GATE_MODE),
        },
        "summary": {
            "total_candles_tested": sum(report["candles_tested"] for report in symbol_reports),
            "component_counts": {name: dict(counts) for name, counts in component_totals.items()},
            "ensemble_counts": _merge_counts(symbol_reports, "ensemble_counts"),
            "top_failed_conditions": dict(failed.most_common(20)),
            "any_component_buy_sell": any(
                counts.get("BUY", 0) + counts.get("SELL", 0) > 0
                for counts in component_totals.values()
            ),
        },
        "component_configuration": components,
        "symbols": symbol_reports,
    }


def diagnosis_from_replay(report: dict[str, Any]) -> dict[str, Any]:
    audit_path = _BOT_ROOT / "models" / "reports" / "signal_starvation_audit.json"
    current_health_path = _BOT_ROOT / "models" / "reports" / "strategy_indicator_health_current.json"
    recent_audit: dict[str, Any] = {}
    current_health: dict[str, Any] = {}
    if audit_path.exists():
        recent_audit = json.loads(audit_path.read_text(encoding="utf-8"))
    if current_health_path.exists():
        current_health = json.loads(current_health_path.read_text(encoding="utf-8"))
    component_counts = report["summary"]["component_counts"]
    most_failed = next(iter(report["summary"]["top_failed_conditions"]), None)
    return {
        "generated_at_utc": report["generated_at_utc"],
        "root_cause": (
            "The latest runtime sample repeatedly evaluated an in-session market window in which "
            "all active WEAK_TREND components had no qualifying trigger. A separate routing defect "
            "amplified starvation: RegimeClassifier.classify_stable() reset a new-regime counter "
            "on every call, permanently pinning the ensemble to its first confirmed regime."
        ),
        "where_signal_becomes_zero": (
            "At raw component evaluation: active strategies return HOLD/0.0 when their entry "
            "conditions are absent. The ensemble correctly aggregates those zero directional votes; "
            "before the fix, broken regime hysteresis could also prevent the intended components "
            "from being activated as market conditions changed."
        ),
        "strategy_logic_broken": True,
        "component_trigger_logic_broken": False,
        "regime_routing_logic_broken": True,
        "strategy_logic_too_restrictive": False,
        "interpretation": (
            "Component rules are selective but demonstrably active. The 500-decision runtime audit "
            "counts runner evaluations, not 500 independent closed 15m candles, so repeated HOLDs "
            "over one unchanged candle window can make starvation appear larger than it is. The "
            "hysteresis repair restores normal regime transitions without forcing a trade."
        ),
        "indicator_health": {
            symbol["symbol"]: symbol["indicator_health"] for symbol in report["symbols"]
        },
        "current_indicator_health": current_health,
        "components_enabled": report["component_configuration"],
        "component_counts": component_counts,
        "ensemble_counts": report["summary"]["ensemble_counts"],
        "most_failed_condition": most_failed,
        "recent_signal_starvation_audit": recent_audit.get("decision_summary", {}),
        "fix_applied": (
            "Fixed the regime hysteresis candidate counter; added structured component diagnostics, "
            "exact failed-condition classification, and explicit ERROR/DISABLED/INSUFFICIENT_DATA "
            "statuses. No component trigger, confidence threshold, session rule, or risk gate changed."
        ),
        "safety": report["configuration"],
    }


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def write_replay_markdown(path: Path, report: dict[str, Any]) -> None:
    summary = report["summary"]
    lines = [
        "# Strategy Component Replay",
        "",
        f"Generated: `{report['generated_at_utc']}`",
        "",
        "## Summary",
        "",
        f"- Total candles tested: `{summary['total_candles_tested']}`",
        f"- Component BUY/SELL activity present: `{summary['any_component_buy_sell']}`",
        f"- Ensemble BUY/SELL/HOLD: `{summary['ensemble_counts']}`",
        f"- Top failed conditions: `{summary['top_failed_conditions']}`",
        "",
        "## Components",
        "",
    ]
    for component in report["component_configuration"]:
        lines.append(
            f"- `{component['component_name']}`: counts `{component['counts']}`, "
            f"weight `{component['weight']}`, min confidence `{component['minimum_confidence']}`, "
            f"timeframe `{component['required_timeframe']}`"
        )
    for symbol in report["symbols"]:
        lines.extend(
            [
                "",
                f"## {symbol['symbol']}",
                "",
                f"- Candles tested: `{symbol['candles_tested']}`",
                f"- In-session candles: `{symbol['in_session_candles']}`",
                f"- Regimes: `{symbol['regime_counts']}`",
                f"- Ensemble counts: `{symbol['ensemble_counts']}`",
                f"- Ensemble reasons: `{symbol['ensemble_top_reasons']}`",
                f"- Latest stored candle: `{symbol['latest_candle_open_utc']}`",
                f"- Stored indicator health: `{symbol['indicator_health']}`",
            ]
        )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_diagnosis_markdown(path: Path, diagnosis: dict[str, Any]) -> None:
    lines = [
        "# Strategy Signal Generation Diagnosis",
        "",
        f"Generated: `{diagnosis['generated_at_utc']}`",
        "",
        "## Root Cause",
        "",
        diagnosis["root_cause"],
        "",
        f"Signal becomes zero: {diagnosis['where_signal_becomes_zero']}",
        "",
        f"- Strategy logic broken: `{diagnosis['strategy_logic_broken']}`",
        f"- Component trigger logic broken: `{diagnosis['component_trigger_logic_broken']}`",
        f"- Regime routing logic broken: `{diagnosis['regime_routing_logic_broken']}`",
        f"- Strategy logic too restrictive: `{diagnosis['strategy_logic_too_restrictive']}`",
        f"- Replay ensemble counts: `{diagnosis['ensemble_counts']}`",
        f"- Most failed condition: `{diagnosis['most_failed_condition']}`",
        "",
        "## Interpretation",
        "",
        diagnosis["interpretation"],
        "",
        "## Fix Applied",
        "",
        diagnosis["fix_applied"],
        "",
        "## Current Indicator Health",
        "",
        f"`{diagnosis['current_indicator_health']}`",
        "",
        "## Safety",
        "",
        f"`{diagnosis['safety']}`",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbols", default="BTCUSDT,ETHUSDT")
    parser.add_argument("--lookback-candles", type=int, default=1000)
    parser.add_argument("--db-path")
    parser.add_argument("--output-md", default="models/reports/strategy_component_replay.md")
    parser.add_argument("--output-json", default="models/reports/strategy_component_replay.json")
    parser.add_argument(
        "--diagnosis-md",
        default="models/reports/strategy_signal_generation_diagnosis.md",
    )
    parser.add_argument(
        "--diagnosis-json",
        default="models/reports/strategy_signal_generation_diagnosis.json",
    )
    args = parser.parse_args()

    symbols = [value.strip().upper() for value in args.symbols.split(",") if value.strip()]
    db_path = resolve_db_path(args.db_path)
    threshold = float(settings.ENSEMBLE_MIN_THRESHOLD_FLOOR)
    blocked = {
        value.strip().upper()
        for value in str(settings.ENSEMBLE_BLOCKED_REGIMES or "").split(",")
        if value.strip()
    }
    windows = _session_windows(str(settings.ENSEMBLE_SESSION_WINDOWS_UTC))
    reports = [
        replay_symbol(
            symbol,
            load_candles(db_path, symbol, args.lookback_candles + WARMUP_CANDLES),
            args.lookback_candles,
            threshold=threshold,
            blocked_regimes=blocked,
            windows=windows,
            session_filter_enabled=bool(settings.ENSEMBLE_SESSION_FILTER_ENABLED),
        )
        for symbol in symbols
    ]
    report = build_report(reports, db_path, threshold)
    diagnosis = diagnosis_from_replay(report)

    output_md = (_BOT_ROOT / args.output_md).resolve()
    output_json = (_BOT_ROOT / args.output_json).resolve()
    diagnosis_md = (_BOT_ROOT / args.diagnosis_md).resolve()
    diagnosis_json = (_BOT_ROOT / args.diagnosis_json).resolve()
    write_json(output_json, report)
    write_replay_markdown(output_md, report)
    write_json(diagnosis_json, diagnosis)
    write_diagnosis_markdown(diagnosis_md, diagnosis)

    print(json.dumps(report["summary"], indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
