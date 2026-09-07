#!/usr/bin/env python3
"""Audit the STRONG_TREND block with an offline, read-only strategy replay."""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import sqlite3
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean
from typing import Any

_SCRIPT_DIR = Path(__file__).resolve().parent
_BOT_ROOT = _SCRIPT_DIR.parent.parent
_SHARED_ROOT = _BOT_ROOT.parent / "shared"
for _path in (str(_BOT_ROOT), str(_SHARED_ROOT)):
    if _path not in sys.path:
        sys.path.insert(0, _path)

from app.core.config import settings
from app.strategy.base import Signal
from app.strategy.donchian_breakout import calculate_atr
from app.strategy.master_ensemble import (
    _ACTIVATION_MATRIX,
    _BASE_WEIGHTS,
    _REGIME_WEIGHT_MULTIPLIERS,
)
from app.strategy.iofs_components.models import Candle
from app.strategy.regime import RegimeClassifier
from app.symbols.universe import parse_symbols
from scripts.validation.iofs_trade_simulator import simulate_trade
from scripts.validation.replay_strategy_components import (
    NOMINAL_CONSENSUS_WEIGHT,
    WARMUP_CANDLES,
    WindowClient,
    _instantiate_strategies,
    _session_windows,
    load_candles,
    resolve_db_path,
    session_allowed,
)


MAX_HOLD_CANDLES = 48
RECOMMENDATIONS = {
    "KEEP_STRONG_TREND_BLOCKED",
    "ALLOW_STRONG_TREND_IN_PAPER_ONLY",
    "ALLOW_STRONG_TREND_WITH_IOFS_CONFIRMATION_ONLY",
    "ALLOW_STRONG_TREND_WITH_HIGHER_THRESHOLD_ONLY",
    "NO_SAFE_CHANGE",
}


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest().upper()


def utc_iso(timestamp_ms: int) -> str:
    return datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc).isoformat()


def score_bucket(confidence: float) -> str:
    if confidence < 0.60:
        return "0.55-0.59"
    if confidence < 0.70:
        return "0.60-0.69"
    if confidence < 0.80:
        return "0.70-0.79"
    return "0.80-1.00"


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


def aggregate_candidate(
    component_results: dict[str, Any],
    regime: str,
    *,
    threshold: float,
    allowed_session: bool,
    session_filter_enabled: bool,
) -> dict[str, Any]:
    if session_filter_enabled and not allowed_session:
        return {"action": "HOLD", "confidence": 0.0, "reason": "SESSION_BLOCKED"}
    active = _ACTIVATION_MATRIX.get(regime, frozenset())
    if not active:
        return {"action": "HOLD", "confidence": 0.0, "reason": "NO_ACTIVE_STRATEGIES"}

    contributions: list[dict[str, Any]] = []
    buy_score = sell_score = 0.0
    multipliers = _REGIME_WEIGHT_MULTIPLIERS.get(regime, {})
    for name in active:
        result = component_results.get(name)
        if result is None or result.signal not in {Signal.BUY, Signal.SELL}:
            continue
        weighted = (
            float(result.confidence)
            * _BASE_WEIGHTS.get(name, 1.0)
            * multipliers.get(name, 1.0)
        )
        action = result.signal.value.upper()
        contributions.append(
            {
                "component": name,
                "action": action,
                "confidence": round(float(result.confidence), 6),
                "weighted_score": round(weighted, 6),
            }
        )
        if action == "BUY":
            buy_score += weighted
        else:
            sell_score += weighted

    buy_conf = min(1.0, buy_score / NOMINAL_CONSENSUS_WEIGHT)
    sell_conf = min(1.0, sell_score / NOMINAL_CONSENSUS_WEIGHT)
    raw_conf = max(buy_conf, sell_conf)
    if buy_conf > sell_conf and buy_conf >= threshold:
        action, reason = "BUY", "ENSEMBLE_BUY"
    elif sell_conf > buy_conf and sell_conf >= threshold:
        action, reason = "SELL", "ENSEMBLE_SELL"
    else:
        action = "HOLD"
        reason = "NO_PATTERN" if raw_conf == 0 else "CONFIDENCE_BELOW_FLOOR"
    matching = [item for item in contributions if item["action"] == action]
    primary = max(matching, key=lambda item: item["weighted_score"])["component"] if matching else None
    return {
        "action": action,
        "confidence": round(raw_conf, 6),
        "reason": reason,
        "buy_score": round(buy_score, 6),
        "sell_score": round(sell_score, 6),
        "components": contributions,
        "primary_component": primary,
    }


def simulate_candidate(
    *,
    symbol: str,
    regime: str,
    candidate: dict[str, Any],
    rows: list[list[Any]],
    index: int,
    window: list[list[Any]],
) -> dict[str, Any] | None:
    if candidate["action"] not in {"BUY", "SELL"} or index + 1 >= len(rows):
        return None
    highs = [float(row[2]) for row in window]
    lows = [float(row[3]) for row in window]
    closes = [float(row[4]) for row in window]
    atr_values = calculate_atr(highs, lows, closes)
    if not atr_values or not math.isfinite(atr_values[-1]) or atr_values[-1] <= 0:
        return None

    entry_row = rows[index + 1]
    entry = float(entry_row[1])
    risk = float(atr_values[-1])
    direction = "UP" if candidate["action"] == "BUY" else "DOWN"
    sign = 1.0 if direction == "UP" else -1.0
    plan = {
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
    future = [
        Candle(int(row[0]), float(row[1]), float(row[2]), float(row[3]), float(row[4]), float(row[5]))
        for row in rows[index + 1 : index + 1 + MAX_HOLD_CANDLES]
    ]
    if not future:
        return None
    outcome = simulate_trade(plan, future, max_holding_candles=MAX_HOLD_CANDLES)
    signal_time = int(rows[index][0]) + 900_000
    return {
        "symbol": symbol,
        "regime": regime,
        "side": candidate["action"],
        "confidence": candidate["confidence"],
        "score_bucket": score_bucket(candidate["confidence"]),
        "session": session_bucket(signal_time),
        "primary_component": candidate["primary_component"] or "unknown",
        "component_sources": [item["component"] for item in candidate["components"]],
        "signal_time": utc_iso(signal_time),
        "entry_time": utc_iso(int(entry_row[0])),
        "entry": round(entry, 8),
        "risk": round(risk, 8),
        **outcome,
    }


def replay_symbol(
    symbol: str,
    rows: list[list[Any]],
    decision_count: int,
    *,
    threshold: float,
    windows: list[tuple[int, int]],
    session_filter_enabled: bool,
    legacy_pinned: bool = False,
) -> dict[str, Any]:
    required = WARMUP_CANDLES + decision_count + MAX_HOLD_CANDLES
    if len(rows) < required:
        raise RuntimeError(f"{symbol}: need {required} candles, found {len(rows)}")
    client = WindowClient()
    strategies = _instantiate_strategies(client)
    classifier = RegimeClassifier()
    pinned_regime: str | None = None
    cycles: list[dict[str, Any]] = []
    candidates: list[dict[str, Any]] = []
    end = len(rows) - MAX_HOLD_CANDLES
    start = end - decision_count

    for index in range(start, end):
        window = rows[index - WARMUP_CANDLES + 1 : index + 1]
        client.set_rows(window)
        highs = [float(row[2]) for row in window]
        lows = [float(row[3]) for row in window]
        closes = [float(row[4]) for row in window]
        if legacy_pinned:
            raw_regime = classifier.classify(highs, lows, closes).regime.value
            pinned_regime = pinned_regime or raw_regime
            regime = pinned_regime
        else:
            regime = classifier.classify_stable(highs, lows, closes).regime.value
        allowed = session_allowed(int(window[-1][0]), windows)
        results = {name: strategy.get_signal(symbol) for name, strategy in strategies.items()}
        candidate = aggregate_candidate(
            results,
            regime,
            threshold=threshold,
            allowed_session=allowed,
            session_filter_enabled=session_filter_enabled,
        )
        cycle = {
            "symbol": symbol,
            "timestamp": utc_iso(int(window[-1][0]) + 900_000),
            "regime": regime,
            "session_allowed": allowed,
            "candidate_action": candidate["action"],
            "candidate_confidence": candidate["confidence"],
            "candidate_reason": candidate["reason"],
        }
        cycles.append(cycle)
        trade = simulate_candidate(
            symbol=symbol,
            regime=regime,
            candidate=candidate,
            rows=rows,
            index=index,
            window=window,
        )
        if trade:
            candidates.append(trade)
    return {"cycles": cycles, "candidates": candidates}


def ratio(numerator: float, denominator: float) -> float | None:
    return round(numerator / denominator, 6) if denominator else None


def max_drawdown_r(trades: list[dict[str, Any]]) -> float:
    equity = peak = drawdown = 0.0
    for trade in sorted(trades, key=lambda item: item["signal_time"]):
        equity += float(trade["r_multiple"])
        peak = max(peak, equity)
        drawdown = max(drawdown, peak - equity)
    return round(drawdown, 6)


def metrics(trades: list[dict[str, Any]]) -> dict[str, Any]:
    values = [float(item["r_multiple"]) for item in trades]
    positive = [value for value in values if value > 0]
    negative = [value for value in values if value < 0]
    return {
        "accepted_trades": len(trades),
        "win_count": len(positive),
        "loss_count": len(negative),
        "win_rate": ratio(len(positive), len(trades)),
        "profit_factor_r": ratio(sum(positive), abs(sum(negative))),
        "expectancy_r": round(mean(values), 6) if values else None,
        "average_r": round(mean(values), 6) if values else None,
        "max_drawdown_r": max_drawdown_r(trades),
        "tp1_count": sum(bool(item.get("tp1_hit")) for item in trades),
        "tp2_count": sum(item.get("outcome") == "TP2" for item in trades),
        "sl_count": sum(item.get("outcome") == "SL" for item in trades),
        "break_even_buffer_count": sum(item.get("outcome") == "BREAK_EVEN_BUFFER" for item in trades),
        "time_exit_count": sum(item.get("outcome") == "TIME_EXIT" for item in trades),
    }


def grouped_metrics(trades: list[dict[str, Any]], key: str) -> dict[str, dict[str, Any]]:
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for trade in trades:
        groups[str(trade.get(key) or "unknown")].append(trade)
    return {name: metrics(items) for name, items in sorted(groups.items())}


def recent_runtime_blocks(db_path: Path, lookback: int) -> dict[str, Any]:
    uri = f"file:{db_path.as_posix()}?mode=ro"
    with sqlite3.connect(uri, uri=True, timeout=30) as connection:
        connection.row_factory = sqlite3.Row
        rows = connection.execute(
            """
            SELECT cycle_id, ts, symbol, reason_codes, gate_reason
            FROM decision_traces
            WHERE bot_instance_id = 'bot_e5fe913972a9'
            ORDER BY ts DESC
            LIMIT ?
            """,
            (lookback,),
        ).fetchall()
    blocked = [
        row for row in rows
        if "REGIME_BLOCKED_STRONG_TREND" in (
            f"{row['reason_codes'] or ''} {row['gate_reason'] or ''}".upper()
        )
    ]
    return {
        "lookback_decisions": len(rows),
        "blocked_decisions": len(blocked),
        "blocked_cycles": len({row["cycle_id"] for row in blocked}),
        "blocked_by_symbol": dict(Counter(row["symbol"] for row in blocked)),
        "first_timestamp": min((row["ts"] for row in rows), default=None),
        "last_timestamp": max((row["ts"] for row in rows), default=None),
    }


def choose_recommendation(strong: dict[str, Any]) -> tuple[str, list[str]]:
    reasons: list[str] = []
    count = int(strong.get("accepted_trades") or 0)
    expectancy = strong.get("expectancy_r")
    drawdown = strong.get("max_drawdown_r")
    profit_factor = strong.get("profit_factor_r")
    if count == 0:
        reasons.append("No STRONG_TREND trade candidates were available for outcome analysis.")
        return "NO_SAFE_CHANGE", reasons
    if expectancy is None or expectancy <= 0:
        reasons.append("STRONG_TREND expectancy is not positive.")
        return "KEEP_STRONG_TREND_BLOCKED", reasons
    if drawdown is None or drawdown > 5.0:
        reasons.append("STRONG_TREND max drawdown exceeds the 5R audit ceiling.")
        return "KEEP_STRONG_TREND_BLOCKED", reasons
    if profit_factor is None or profit_factor <= 1.2:
        reasons.append("Expectancy is positive but profit factor is not above 1.2.")
        return "ALLOW_STRONG_TREND_WITH_HIGHER_THRESHOLD_ONLY", reasons
    reasons.append("Positive expectancy, profit factor above 1.2, and max drawdown at or below 5R.")
    if count < 20:
        reasons.append(
            "The sample has fewer than 20 trades, so any trial must remain paper-only for data collection."
        )
    return "ALLOW_STRONG_TREND_IN_PAPER_ONLY", reasons


def build_report(
    *,
    symbols: list[str],
    lookback_decisions: int,
    db_path: Path,
    repaired: list[dict[str, Any]],
    legacy: list[dict[str, Any]],
    env_hash_before: str,
    env_hash_after: str,
) -> dict[str, Any]:
    cycles = [item for result in repaired for item in result["cycles"]]
    candidates = [item for result in repaired for item in result["candidates"]]
    legacy_candidates = [item for result in legacy for item in result["candidates"]]
    blocked_regimes = {
        value.strip().upper()
        for value in str(settings.ENSEMBLE_BLOCKED_REGIMES or "").split(",")
        if value.strip()
    }
    current_trades = [trade for trade in candidates if trade["regime"].upper() not in blocked_regimes]
    strong_trades = [trade for trade in candidates if trade["regime"].upper() == "STRONG_TREND"]
    legacy_strong = [trade for trade in legacy_candidates if trade["regime"].upper() == "STRONG_TREND"]
    current = metrics(current_trades)
    allowed = metrics(candidates)
    strong = metrics(strong_trades)
    legacy_strong_metrics = metrics(legacy_strong)
    recommendation, recommendation_reasons = choose_recommendation(strong)
    report = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "analysis_mode_only": True,
        "active_env_modified": env_hash_before != env_hash_after,
        "active_env_sha256_before": env_hash_before,
        "active_env_sha256_after": env_hash_after,
        "symbols": symbols,
        "lookback_decisions_total": lookback_decisions,
        "lookback_decisions_per_symbol": {symbol: len(result["cycles"]) for symbol, result in zip(symbols, repaired)},
        "runtime_config": {
            "EXECUTION_MODE": str(settings.EXECUTION_MODE),
            "ML_ENABLED": bool(settings.ML_ENABLED),
            "IOFS_GATE_MODE": str(settings.IOFS_GATE_MODE),
            "TRADE_SYMBOLS_raw": str(settings.TRADE_SYMBOLS),
            "TRADE_SYMBOLS_parsed": parse_symbols(settings.TRADE_SYMBOLS, settings.MAX_SYMBOLS),
            "ENSEMBLE_BLOCKED_REGIMES": str(settings.ENSEMBLE_BLOCKED_REGIMES),
        },
        "health_reporting_audit": {
            "previous_count_source": "len(settings.TRADE_SYMBOLS), which counted characters",
            "previous_count": len(str(settings.TRADE_SYMBOLS)),
            "correct_count_source": "len(parse_symbols(settings.TRADE_SYMBOLS, settings.MAX_SYMBOLS))",
            "correct_count": len(parse_symbols(settings.TRADE_SYMBOLS, settings.MAX_SYMBOLS)),
            "display_source": "comma-joined parsed trade-symbol list",
            "runner_actual_symbols": symbols,
        },
        "recent_runtime_strong_trend_blocks": recent_runtime_blocks(db_path, lookback_decisions),
        "replay": {
            "method": {
                "classifier": "repaired RegimeClassifier.classify_stable()",
                "entry": "next 15m candle open",
                "risk_model": "1 ATR stop, 1R TP1, 2R TP2, 0.2R break-even buffer",
                "max_holding_candles": MAX_HOLD_CANDLES,
                "overlapping_trades_allowed": True,
                "fees_and_slippage_included": False,
            },
            "strong_trend_cycles": sum(item["regime"] == "STRONG_TREND" for item in cycles),
            "strong_trend_trade_candidates_blocked_by_current_config": len(strong_trades),
            "with_strong_trend_blocked": current,
            "with_strong_trend_allowed_analysis_only": {
                **allowed,
                "additional_trades_from_strong_trend": len(strong_trades),
            },
            "strong_trend_only": {
                **strong,
                "long_vs_short": grouped_metrics(strong_trades, "side"),
                "btc_vs_eth": grouped_metrics(strong_trades, "symbol"),
                "session_performance": grouped_metrics(strong_trades, "session"),
                "score_bucket_performance": grouped_metrics(strong_trades, "score_bucket"),
                "component_source_performance": grouped_metrics(strong_trades, "primary_component"),
            },
            "legacy_pinned_classifier_counterfactual": {
                "description": "Models the pre-repair bug by pinning each symbol to its first classified regime.",
                "strong_trend_only": legacy_strong_metrics,
                "difference_after_repair": {
                    "trade_count_delta": strong["accepted_trades"] - legacy_strong_metrics["accepted_trades"],
                    "expectancy_r_delta": _subtract(strong["expectancy_r"], legacy_strong_metrics["expectancy_r"]),
                    "max_drawdown_r_delta": _subtract(strong["max_drawdown_r"], legacy_strong_metrics["max_drawdown_r"]),
                },
            },
        },
        "recommendation": recommendation,
        "recommendation_reasons": recommendation_reasons,
        "safety": {
            "recommendation_is_allowed_value": recommendation in RECOMMENDATIONS,
            "paper_only": str(settings.EXECUTION_MODE).lower() == "paper",
            "ml_disabled": not bool(settings.ML_ENABLED),
            "iofs_shadow": str(settings.IOFS_GATE_MODE).lower() == "shadow",
            "live_use_recommended": False,
            "ml_enable_recommended": False,
            "capital_deployment_allowed": False,
        },
        "warnings": [
            "This is an offline comparative replay and does not replace Section 4 forward paper validation.",
            "The ATR-normalized simulator is uniform comparison evidence, not exact production execution parity.",
            "Overlapping trades are allowed; fees and slippage are excluded.",
        ],
    }
    return report


def _subtract(left: float | None, right: float | None) -> float | None:
    if left is None or right is None:
        return None
    return round(float(left) - float(right), 6)


def fmt(value: Any) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, float):
        return f"{value:.4f}"
    return str(value)


def render_markdown(report: dict[str, Any]) -> str:
    replay = report["replay"]
    blocked = replay["with_strong_trend_blocked"]
    allowed = replay["with_strong_trend_allowed_analysis_only"]
    strong = replay["strong_trend_only"]
    lines = [
        "# Strong Trend Block Impact Audit",
        "",
        f"Generated: `{report['generated_at_utc']}`",
        "",
        "## Recommendation",
        "",
        f"`{report['recommendation']}`",
        "",
        *[f"- {reason}" for reason in report["recommendation_reasons"]],
        "- Live use recommended: false",
        "- Active `.env` modified: false",
        "",
        "## Runtime Consistency",
        "",
        f"- Raw TRADE_SYMBOLS: `{report['runtime_config']['TRADE_SYMBOLS_raw']}`",
        f"- Parsed/runtime symbols: `{report['runtime_config']['TRADE_SYMBOLS_parsed']}`",
        f"- Current blocked regimes: `{report['runtime_config']['ENSEMBLE_BLOCKED_REGIMES']}`",
        f"- Health count before fix: `{report['health_reporting_audit']['previous_count']}`",
        f"- Correct health count: `{report['health_reporting_audit']['correct_count']}`",
        f"- Recent blocked decisions/cycles: `{report['recent_runtime_strong_trend_blocks']['blocked_decisions']}` / `{report['recent_runtime_strong_trend_blocks']['blocked_cycles']}`",
        "",
        "## Replay Comparison",
        "",
        "| Scenario | Trades | Win rate | Profit factor R | Expectancy R | Max DD R | TP1 / TP2 / SL |",
        "|---|---:|---:|---:|---:|---:|---:|",
        _metrics_row("STRONG_TREND blocked", blocked),
        _metrics_row("STRONG_TREND allowed (analysis only)", allowed),
        _metrics_row("STRONG_TREND only", strong),
        "",
        f"- Strong-trend cycles: `{replay['strong_trend_cycles']}`",
        f"- Strong-trend trade candidates blocked by current config: `{replay['strong_trend_trade_candidates_blocked_by_current_config']}`",
        f"- Additional allowed-scenario trades: `{allowed['additional_trades_from_strong_trend']}`",
        "",
        "## Strong Trend Breakdown",
        "",
    ]
    for label, key in (
        ("Long vs short", "long_vs_short"),
        ("BTC vs ETH", "btc_vs_eth"),
        ("Session", "session_performance"),
        ("Score bucket", "score_bucket_performance"),
        ("Primary component", "component_source_performance"),
    ):
        lines.extend([f"### {label}", "", "| Group | Trades | Win rate | PF R | Expectancy R | Max DD R |", "|---|---:|---:|---:|---:|---:|"])
        for name, item in strong[key].items():
            lines.append(
                f"| {name} | {item['accepted_trades']} | {fmt(item['win_rate'])} | "
                f"{fmt(item['profit_factor_r'])} | {fmt(item['expectancy_r'])} | {fmt(item['max_drawdown_r'])} |"
            )
        lines.append("")
    legacy = replay["legacy_pinned_classifier_counterfactual"]
    lines.extend(
        [
            "## Repair Comparison",
            "",
            legacy["description"],
            "",
            f"- Legacy pinned strong-trend trades: `{legacy['strong_trend_only']['accepted_trades']}`",
            f"- Repaired strong-trend trades: `{strong['accepted_trades']}`",
            f"- Trade-count delta after repair: `{legacy['difference_after_repair']['trade_count_delta']}`",
            f"- Expectancy delta after repair: `{fmt(legacy['difference_after_repair']['expectancy_r_delta'])}`",
            "",
            "## Safety And Limitations",
            "",
        ]
    )
    lines.extend(f"- {warning}" for warning in report["warnings"])
    return "\n".join(lines) + "\n"


def _metrics_row(label: str, item: dict[str, Any]) -> str:
    return (
        f"| {label} | {item['accepted_trades']} | {fmt(item['win_rate'])} | "
        f"{fmt(item['profit_factor_r'])} | {fmt(item['expectancy_r'])} | "
        f"{fmt(item['max_drawdown_r'])} | {item['tp1_count']} / {item['tp2_count']} / {item['sl_count']} |"
    )


def run_audit(
    *,
    symbols: list[str],
    lookback_decisions: int,
    output_md: Path,
    output_json: Path,
    db_path: Path | None = None,
) -> dict[str, Any]:
    env_path = _BOT_ROOT / ".env"
    env_hash_before = sha256(env_path)
    resolved_db = db_path or resolve_db_path(None)
    threshold = float(settings.ENSEMBLE_MIN_THRESHOLD_FLOOR)
    windows = _session_windows(str(settings.ENSEMBLE_SESSION_WINDOWS_UTC))
    per_symbol, remainder = divmod(lookback_decisions, len(symbols))
    decision_counts = [per_symbol + (1 if index < remainder else 0) for index in range(len(symbols))]
    repaired: list[dict[str, Any]] = []
    legacy: list[dict[str, Any]] = []
    for symbol, count in zip(symbols, decision_counts):
        rows = load_candles(resolved_db, symbol, WARMUP_CANDLES + count + MAX_HOLD_CANDLES)
        kwargs = {
            "threshold": threshold,
            "windows": windows,
            "session_filter_enabled": bool(settings.ENSEMBLE_SESSION_FILTER_ENABLED),
        }
        repaired.append(replay_symbol(symbol, rows, count, **kwargs))
        legacy.append(replay_symbol(symbol, rows, count, legacy_pinned=True, **kwargs))
    env_hash_after = sha256(env_path)
    report = build_report(
        symbols=symbols,
        lookback_decisions=lookback_decisions,
        db_path=resolved_db,
        repaired=repaired,
        legacy=legacy,
        env_hash_before=env_hash_before,
        env_hash_after=env_hash_after,
    )
    output_md.parent.mkdir(parents=True, exist_ok=True)
    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_md.write_text(render_markdown(report), encoding="utf-8")
    output_json.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--symbols", default="BTCUSDT,ETHUSDT")
    parser.add_argument("--lookback-decisions", type=int, default=1000)
    parser.add_argument("--db-path")
    parser.add_argument("--output-md", default="models/reports/strong_trend_block_impact.md")
    parser.add_argument("--output-json", default="models/reports/strong_trend_block_impact.json")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    symbols = parse_symbols(args.symbols, 100)
    report = run_audit(
        symbols=symbols,
        lookback_decisions=args.lookback_decisions,
        output_md=(_BOT_ROOT / args.output_md).resolve(),
        output_json=(_BOT_ROOT / args.output_json).resolve(),
        db_path=Path(args.db_path).resolve() if args.db_path else None,
    )
    print(json.dumps({
        "recommendation": report["recommendation"],
        "blocked": report["replay"]["with_strong_trend_blocked"],
        "allowed": report["replay"]["with_strong_trend_allowed_analysis_only"],
        "strong_trend_only": report["replay"]["strong_trend_only"],
        "active_env_modified": report["active_env_modified"],
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
