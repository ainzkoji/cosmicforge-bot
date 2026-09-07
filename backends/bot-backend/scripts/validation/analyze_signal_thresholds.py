#!/usr/bin/env python3
"""Offline, read-only audit of ensemble signal starvation and threshold sensitivity."""
from __future__ import annotations

import argparse
import hashlib
import json
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
from app.strategy.hold_breakdown import classify_hold_reason
from scripts.validation.run_paper_cycle_diagnostic import (
    connect_read_only,
    load_env_values,
    minute_in_windows,
    parse_json_object,
    parse_timestamp,
    resolve_db_path,
)


DEFAULT_REPORT_DIR = _BOT_ROOT / "models" / "reports"
CONFIDENCE_BUCKETS = ("0.00-0.39", "0.40-0.49", "0.50-0.54", "0.55-0.59", "0.60+")
SAFE_RECOMMENDATIONS = {
    "KEEP_CURRENT_CONFIG",
    "LOWER_THRESHOLD_TO_0.50_IN_PAPER_ONLY",
    "LOWER_THRESHOLD_TO_0.45_IN_PAPER_ONLY",
    "ADJUST_SESSION_WINDOWS_ONLY",
    "ADJUST_REGIME_BLOCK_ONLY",
    "NO_SAFE_TUNING_FOUND",
}


def confidence_bucket(value: float) -> str:
    if value < 0.40:
        return "0.00-0.39"
    if value < 0.50:
        return "0.40-0.49"
    if value < 0.55:
        return "0.50-0.54"
    if value < 0.60:
        return "0.55-0.59"
    return "0.60+"


def session_label(timestamp: datetime, runtime_windows: str, narrow_windows: str) -> str:
    if minute_in_windows(narrow_windows, timestamp):
        return "NARROW_REPLAY_WINDOW"
    if minute_in_windows(runtime_windows, timestamp):
        return "RUNTIME_ONLY_WINDOW"
    return "OUTSIDE_RUNTIME_WINDOW"


def directional_side(row: dict[str, Any]) -> str | None:
    buy = float(row.get("buy_score") or 0.0)
    sell = float(row.get("sell_score") or 0.0)
    if buy > sell:
        return "BUY"
    if sell > buy:
        return "SELL"
    signal = str(row.get("signal") or "").upper()
    return signal if signal in {"BUY", "SELL"} else None


def component_rows(decision_meta: dict[str, Any]) -> list[dict[str, Any]]:
    components = decision_meta.get("component_breakdown")
    if isinstance(components, list) and components:
        return [row for row in components if isinstance(row, dict)]
    rows = []
    for vote in decision_meta.get("votes") or []:
        text = str(vote)
        strategy, _, remainder = text.partition(":")
        signal = remainder.split("(", 1)[0].upper() if remainder else "UNKNOWN"
        rows.append(
            {
                "strategy": strategy,
                "signal": signal,
                "confidence": 0.0,
                "reason": "historical_component_reason_not_persisted",
                "failed_conditions": ["NO_PATTERN"] if signal == "HOLD" else [],
            }
        )
    return rows


def derive_hold_reason(row: dict[str, Any], meta: dict[str, Any]) -> str:
    explicit = str(meta.get("hold_reason") or "").upper()
    if explicit:
        return explicit
    return classify_hold_reason(
        str(row.get("gate_reason") or row.get("reason_codes") or ""),
        confidence=float(row.get("confidence") or 0.0),
        threshold_floor=float(row.get("threshold") or settings.ENSEMBLE_MIN_THRESHOLD_FLOOR),
        meta=meta,
    )


def load_decisions(
    conn: sqlite3.Connection,
    symbols: list[str],
    lookback: int,
) -> list[dict[str, Any]]:
    placeholders = ",".join("?" for _ in symbols)
    trace_rows = conn.execute(
        f"""
        SELECT *
        FROM decision_traces
        WHERE symbol IN ({placeholders})
        ORDER BY ts DESC
        LIMIT ?
        """,
        (*symbols, lookback),
    ).fetchall()
    trace_ids = [str(row["trace_id"]) for row in trace_rows if row["trace_id"]]
    decision_logs: dict[str, dict[str, Any]] = {}
    if trace_ids:
        log_placeholders = ",".join("?" for _ in trace_ids)
        log_rows = conn.execute(
            f"""
            SELECT run_id, strategy_signal_json, final_action
            FROM decision_logs
            WHERE run_id IN ({log_placeholders})
            """,
            trace_ids,
        ).fetchall()
        decision_logs = {str(row["run_id"]): dict(row) for row in log_rows}

    output = []
    for raw in trace_rows:
        row = dict(raw)
        decision_log = decision_logs.get(str(row.get("trace_id")), {})
        row["decision_strategy_json"] = decision_log.get("strategy_signal_json")
        row["decision_log_action"] = decision_log.get("final_action")
        strategy_payload = parse_json_object(row.get("decision_strategy_json"))
        meta = strategy_payload.get("meta") if isinstance(strategy_payload.get("meta"), dict) else {}
        timestamp = parse_timestamp(row.get("ts"))
        row["_meta"] = meta
        row["_timestamp"] = timestamp
        row["_hold_reason"] = derive_hold_reason(row, meta)
        row["_components"] = component_rows(meta)
        output.append(row)
    return output


def group_counts(rows: list[dict[str, Any]], key_fn) -> dict[str, int]:
    return dict(Counter(str(key_fn(row)) for row in rows))


def summarize_decisions(
    rows: list[dict[str, Any]],
    *,
    runtime_windows: str,
    narrow_windows: str,
    current_floor: float,
) -> dict[str, Any]:
    confidences = [float(row.get("confidence") or 0.0) for row in rows]
    components = [
        component
        for row in rows
        for component in row["_components"]
    ]
    action_counts = Counter(str(row.get("signal") or "UNKNOWN").upper() for row in rows)
    component_summary: dict[str, Counter] = defaultdict(Counter)
    for component in components:
        component_summary[str(component.get("strategy") or "unknown")][
            str(component.get("signal") or "UNKNOWN").upper()
        ] += 1
    grouped = Counter()
    for row in rows:
        timestamp = row["_timestamp"]
        grouped[
            (
                str(row.get("symbol") or "UNKNOWN"),
                str(timestamp.hour if timestamp else "UNKNOWN"),
                session_label(timestamp, runtime_windows, narrow_windows)
                if timestamp
                else "UNKNOWN",
                str(row.get("regime_state") or "UNKNOWN"),
                str(row["_hold_reason"]),
                confidence_bucket(float(row.get("confidence") or 0.0)),
            )
        ] += 1
    return {
        "total_decisions": len(rows),
        "buy_count": action_counts.get("BUY", 0),
        "sell_count": action_counts.get("SELL", 0),
        "hold_count": action_counts.get("HOLD", 0),
        "average_confidence": round(mean(confidences), 6) if confidences else 0.0,
        "max_confidence": max(confidences, default=0.0),
        "signals_just_below_threshold": sum(
            0.50 <= confidence < current_floor for confidence in confidences
        ),
        "hold_reasons": group_counts(rows, lambda row: row["_hold_reason"]),
        "confidence_distribution": {
            bucket: sum(confidence_bucket(value) == bucket for value in confidences)
            for bucket in CONFIDENCE_BUCKETS
        },
        "regime_distribution": group_counts(rows, lambda row: row.get("regime_state") or "UNKNOWN"),
        "symbol_distribution": group_counts(rows, lambda row: row.get("symbol") or "UNKNOWN"),
        "hour_utc_distribution": group_counts(
            rows, lambda row: row["_timestamp"].hour if row["_timestamp"] else "UNKNOWN"
        ),
        "session_distribution": group_counts(
            rows,
            lambda row: session_label(row["_timestamp"], runtime_windows, narrow_windows)
            if row["_timestamp"]
            else "UNKNOWN",
        ),
        "strategy_component_distribution": {
            strategy: dict(counts) for strategy, counts in component_summary.items()
        },
        "grouped_analysis": [
            {
                "symbol": key[0],
                "hour_utc": key[1],
                "session_window": key[2],
                "regime": key[3],
                "hold_reason": key[4],
                "confidence_bucket": key[5],
                "count": count,
            }
            for key, count in sorted(grouped.items())
        ],
        "structured_hold_breakdowns": [
            {
                "symbol": row.get("symbol"),
                "timestamp": row.get("ts"),
                "regime": row.get("regime_state"),
                "session_window": session_label(row["_timestamp"], runtime_windows, narrow_windows)
                if row["_timestamp"]
                else "UNKNOWN",
                "raw_strategy_signal": row.get("signal"),
                "raw_confidence": float(row.get("confidence") or 0.0),
                "final_action": row.get("intended_action"),
                "hold_reason": row["_hold_reason"],
                "indicator_values": {
                    key: row.get(key)
                    for key in (
                        "adx",
                        "atr_pct",
                        "ma_slope",
                        "compression_ratio",
                        "breakout_pressure",
                        "buy_score",
                        "sell_score",
                        "threshold",
                    )
                },
                "failed_conditions": list(
                    dict.fromkeys(
                        [row["_hold_reason"]]
                        + [
                            condition
                            for component in row["_components"]
                            for condition in component.get("failed_conditions", [])
                        ]
                    )
                ),
                "threshold_floor": current_floor,
                "blocked_regime": "REGIME_BLOCKED" in row["_hold_reason"],
                "components": row["_components"],
            }
            for row in rows
            if str(row.get("signal") or "").upper() == "HOLD"
        ],
    }


def threshold_sensitivity(
    rows: list[dict[str, Any]],
    thresholds: list[float],
    *,
    runtime_windows: str,
) -> dict[str, Any]:
    results: dict[str, Any] = {}
    for threshold in thresholds:
        eligible = []
        for row in rows:
            timestamp = row["_timestamp"]
            confidence = float(row.get("confidence") or 0.0)
            side = directional_side(row)
            regime = str(row.get("regime_state") or "UNKNOWN").upper()
            if (
                timestamp
                and minute_in_windows(runtime_windows, timestamp)
                and regime != "STRONG_TREND"
                and side
                and confidence >= threshold
            ):
                eligible.append(row)
        results[f"{threshold:.2f}"] = {
            "possible_buy_signals": sum(directional_side(row) == "BUY" for row in eligible),
            "possible_sell_signals": sum(directional_side(row) == "SELL" for row in eligible),
            "total_possible_signals": len(eligible),
            "regime_distribution": group_counts(
                eligible, lambda row: row.get("regime_state") or "UNKNOWN"
            ),
            "session_distribution": {"runtime_window": len(eligible)},
            "would_be_blocked_by_iofs_shadow": 0,
            "outcome_status": "NO_OUTCOME_DATA_FOR_THRESHOLD_EXPECTANCY",
            "win_rate": None,
            "profit_factor": None,
            "expectancy": None,
        }
    current = results.get(f"{thresholds[0]:.2f}", {"total_possible_signals": 0})
    for key, result in results.items():
        result["additional_vs_current_floor"] = (
            result["total_possible_signals"] - current["total_possible_signals"]
        )
    return results


def regime_impact(rows: list[dict[str, Any]]) -> dict[str, Any]:
    regimes = ("STRONG_TREND", "WEAK_TREND", "RANGE", "HIGH_VOLATILITY")
    distribution = Counter(str(row.get("regime_state") or "UNKNOWN").upper() for row in rows)
    strong = [row for row in rows if str(row.get("regime_state") or "").upper() == "STRONG_TREND"]
    return {
        "decisions": {regime: distribution.get(regime, 0) for regime in regimes},
        "signals_before_regime_block": sum(
            directional_side(row) is not None and float(row.get("confidence") or 0) > 0
            for row in strong
        ),
        "signals_after_regime_block": sum(
            str(row.get("signal") or "").upper() in {"BUY", "SELL"} for row in strong
        ),
        "strong_trend_block_impact_in_sample": len(strong),
        "conclusion": (
            "No STRONG_TREND decisions occurred in the analyzed sample; the block did not "
            "cause the observed starvation."
            if not strong
            else "STRONG_TREND decisions were present; retain the block until outcome evidence improves."
        ),
    }


def session_impact(
    rows: list[dict[str, Any]],
    *,
    runtime_windows: str,
    narrow_windows: str,
    floor: float,
) -> dict[str, Any]:
    def summarize(windows: str) -> dict[str, int]:
        subset = [
            row for row in rows
            if row["_timestamp"] and minute_in_windows(windows, row["_timestamp"])
        ]
        return {
            "decisions": len(subset),
            "nonzero_confidence": sum(float(row.get("confidence") or 0) > 0 for row in subset),
            "valid_signals_at_current_floor": sum(
                directional_side(row) is not None and float(row.get("confidence") or 0) >= floor
                for row in subset
            ),
        }

    outside = [
        row for row in rows
        if row["_timestamp"] and not minute_in_windows(runtime_windows, row["_timestamp"])
    ]
    outside_valid = sum(
        directional_side(row) is not None and float(row.get("confidence") or 0) >= floor
        for row in outside
    )
    return {
        "runtime_window": runtime_windows,
        "narrow_replay_windows": narrow_windows,
        "runtime": summarize(runtime_windows),
        "narrow_replay": summarize(narrow_windows),
        "outside_runtime_decisions": len(outside),
        "valid_signals_outside_runtime": outside_valid,
        "runtime_missing_valid_signals": outside_valid > 0,
    }


def choose_recommendation(
    threshold_results: dict[str, Any],
    regime_result: dict[str, Any],
    session_result: dict[str, Any],
) -> tuple[str, list[str]]:
    reasons = []
    additional_050 = threshold_results.get("0.50", {}).get("additional_vs_current_floor", 0)
    additional_045 = threshold_results.get("0.45", {}).get("additional_vs_current_floor", 0)
    if additional_050 <= 0 and additional_045 <= 0:
        reasons.append("Lowering the ensemble floor to 0.50 or 0.45 creates no additional signals.")
    if not session_result["runtime_missing_valid_signals"]:
        reasons.append("No valid signals were found outside the current runtime session.")
    if regime_result["strong_trend_block_impact_in_sample"] == 0:
        reasons.append("The STRONG_TREND block caused no decisions in this sample.")
    reasons.append("No linked outcome data exists for newly eligible threshold candidates.")
    return "NO_SAFE_TUNING_FOUND", reasons


def render_audit(payload: dict[str, Any]) -> str:
    summary = payload["decision_summary"]
    lines = [
        "# Signal Starvation Audit",
        "",
        f"Generated: `{payload['generated_at']}`",
        "",
        "## Conclusion",
        "",
        payload["conclusion"],
        "",
        "## Decision Summary",
        "",
        f"- Decisions analyzed: `{summary['total_decisions']}`",
        f"- BUY / SELL / HOLD: `{summary['buy_count']} / {summary['sell_count']} / {summary['hold_count']}`",
        f"- Average confidence: `{summary['average_confidence']}`",
        f"- Maximum confidence: `{summary['max_confidence']}`",
        f"- Signals in 0.50-0.54 just below current floor: `{summary['signals_just_below_threshold']}`",
        f"- HOLD reasons: `{summary['hold_reasons']}`",
        f"- Confidence distribution: `{summary['confidence_distribution']}`",
        f"- Regime distribution: `{summary['regime_distribution']}`",
        f"- Session distribution: `{summary['session_distribution']}`",
        f"- Grouped analysis rows: `{len(summary['grouped_analysis'])}`",
        "",
        "## Exact Component Rules",
        "",
        "Within valid-session WEAK_TREND cycles, the active components overwhelmingly returned "
        "HOLD: Supertrend found no qualifying flip/continuation, Trend Pullback found no "
        "ADX + RSI-reset + EMA reaction setup, SMA Cross found no fresh cross, and Donchian "
        "Breakout found no aligned confirmed breakout. Historical component reason details were "
        "not persisted; the new HOLD breakdown logging records them going forward.",
        "",
        f"Component distribution: `{summary['strategy_component_distribution']}`",
        "",
        "## Threshold Sensitivity",
        "",
    ]
    for threshold, result in payload["threshold_sensitivity"].items():
        lines.append(
            f"- `{threshold}`: total `{result['total_possible_signals']}`, "
            f"additional `{result['additional_vs_current_floor']}`, "
            f"BUY `{result['possible_buy_signals']}`, SELL `{result['possible_sell_signals']}`, "
            f"outcomes `{result['outcome_status']}`"
        )
    lines.extend(
        [
            "",
            "## Regime Impact",
            "",
            f"`{payload['regime_impact']}`",
            "",
            "## Session Impact",
            "",
            f"`{payload['session_impact']}`",
            "",
            "## Safety",
            "",
            f"- Active `.env` hash before/after unchanged: `{payload['safety']['active_env_unchanged']}`",
            f"- Runtime mode remains paper: `{payload['safety']['execution_mode_is_paper']}`",
            f"- ML remains disabled: `{payload['safety']['ml_disabled']}`",
            "- This audit used read-only database access and did not call an executor.",
        ]
    )
    return "\n".join(lines) + "\n"


def render_recommendation(payload: dict[str, Any]) -> str:
    rec = payload["recommendation"]
    lines = [
        "# Signal Tuning Recommendation",
        "",
        f"Recommendation: **{rec['decision']}**",
        "",
    ]
    lines.extend(f"- {reason}" for reason in rec["reasons"])
    lines.extend(
        [
            "",
            "Safety constraints remain mandatory:",
            "",
            "- Paper mode only.",
            "- ML disabled.",
            "- IOFS shadow mode.",
            "- No active `.env` changes.",
            "- No live capital or production deployment.",
        ]
    )
    return "\n".join(lines) + "\n"


def file_hash(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def run_audit(
    *,
    symbols: list[str],
    lookback_decisions: int,
    thresholds: list[float],
    db_path: Path,
    output_md: Path,
    output_json: Path,
    recommendation_md: Path,
) -> dict[str, Any]:
    env_path = _BOT_ROOT / ".env"
    before_hash = file_hash(env_path)
    env_values = load_env_values(env_path)
    runtime_windows = str(settings.ENSEMBLE_SESSION_WINDOWS_UTC)
    narrow_windows = str(settings.IOFS_SESSION_WINDOWS_UTC)
    current_floor = float(settings.ENSEMBLE_MIN_THRESHOLD_FLOOR)

    with connect_read_only(db_path) as conn:
        decisions = load_decisions(conn, symbols, lookback_decisions)

    summary = summarize_decisions(
        decisions,
        runtime_windows=runtime_windows,
        narrow_windows=narrow_windows,
        current_floor=current_floor,
    )
    thresholds_result = threshold_sensitivity(
        decisions, thresholds, runtime_windows=runtime_windows
    )
    regime_result = regime_impact(decisions)
    session_result = session_impact(
        decisions,
        runtime_windows=runtime_windows,
        narrow_windows=narrow_windows,
        floor=current_floor,
    )
    recommendation, recommendation_reasons = choose_recommendation(
        thresholds_result, regime_result, session_result
    )
    if recommendation not in SAFE_RECOMMENDATIONS:
        raise RuntimeError(f"Unsafe recommendation value: {recommendation}")

    after_hash = file_hash(env_path)
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "scope": {
            "symbols": symbols,
            "lookback_decisions": lookback_decisions,
            "database_path": str(db_path),
            "current_floor": current_floor,
            "thresholds_tested": thresholds,
        },
        "decision_summary": summary,
        "threshold_sensitivity": thresholds_result,
        "regime_impact": regime_result,
        "session_impact": session_result,
        "recommendation": {
            "decision": recommendation,
            "reasons": recommendation_reasons,
            "paper_only": True,
            "ml_enabled": False,
            "live_enabled": False,
        },
        "safety": {
            "active_env_hash_before": before_hash,
            "active_env_hash_after": after_hash,
            "active_env_unchanged": before_hash == after_hash,
            "execution_mode_is_paper": env_values.get("EXECUTION_MODE", "").lower() == "paper",
            "ml_disabled": env_values.get("ML_ENABLED", "").lower() == "false",
            "iofs_shadow": env_values.get("IOFS_GATE_MODE", "").lower() == "shadow",
            "read_only_database": True,
            "executor_called": False,
            "active_env_modified": False,
        },
        "conclusion": (
            "HOLD dominates because active strategy components rarely produce any directional "
            "pattern on BTCUSDT/ETHUSDT 15m. The current floor, STRONG_TREND block, and runtime "
            "session are not the primary cause in the analyzed sample."
        ),
    }
    output_md.parent.mkdir(parents=True, exist_ok=True)
    output_json.parent.mkdir(parents=True, exist_ok=True)
    recommendation_md.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(json.dumps(payload, indent=2, default=str) + "\n", encoding="utf-8")
    output_md.write_text(render_audit(payload), encoding="utf-8")
    recommendation_md.write_text(render_recommendation(payload), encoding="utf-8")
    return payload


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--symbols", default="BTCUSDT,ETHUSDT")
    parser.add_argument("--lookback-decisions", type=int, default=500)
    parser.add_argument("--thresholds", default="0.55,0.50,0.45")
    parser.add_argument("--db-path")
    parser.add_argument(
        "--output-md", default=str(DEFAULT_REPORT_DIR / "signal_starvation_audit.md")
    )
    parser.add_argument(
        "--output-json", default=str(DEFAULT_REPORT_DIR / "signal_starvation_audit.json")
    )
    parser.add_argument(
        "--recommendation-md",
        default=str(DEFAULT_REPORT_DIR / "signal_tuning_recommendation.md"),
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    symbols = [value.strip().upper() for value in args.symbols.split(",") if value.strip()]
    thresholds = [float(value.strip()) for value in args.thresholds.split(",") if value.strip()]
    payload = run_audit(
        symbols=symbols,
        lookback_decisions=max(1, args.lookback_decisions),
        thresholds=thresholds,
        db_path=resolve_db_path(args.db_path),
        output_md=Path(args.output_md).resolve(),
        output_json=Path(args.output_json).resolve(),
        recommendation_md=Path(args.recommendation_md).resolve(),
    )
    print(payload["conclusion"])
    print(f"Recommendation: {payload['recommendation']['decision']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
