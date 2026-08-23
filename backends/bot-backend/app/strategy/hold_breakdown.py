from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


COMPONENT_REQUIRED_CONDITIONS: dict[str, list[str]] = {
    "supertrend": [
        "supertrend_flip_or_continuation",
        "ema_slope_confirmation",
    ],
    "trend_pullback": [
        "adx_above_threshold",
        "ema_trend_alignment",
        "rsi_reset_and_turn",
        "ema_reaction",
    ],
    "sma_cross": ["fresh_fast_slow_sma_cross"],
    "donchian_breakout": [
        "adx_above_threshold",
        "ema50_ema200_trend_alignment",
        "fresh_donchian_breakout",
        "confirmed_close_within_atr_distance",
    ],
    "bollinger_reversion": [
        "bollinger_extreme",
        "rsi_extreme",
        "reversal_candle",
    ],
    "vwap_reversion": [
        "adx_below_reversion_max",
        "vwap_deviation",
        "rsi_extreme",
        "reversion_candle",
    ],
    "squeeze_breakout": [
        "recent_squeeze",
        "squeeze_release",
        "directional_momentum",
    ],
}


HOLD_REASONS = {
    "NO_PATTERN",
    "CONFIDENCE_BELOW_FLOOR",
    "REGIME_BLOCKED",
    "TREND_FILTER_FAILED",
    "MOMENTUM_FILTER_FAILED",
    "VOLUME_FILTER_FAILED",
    "VOLATILITY_FILTER_FAILED",
    "RISK_REWARD_INVALID",
    "SESSION_BLOCKED",
    "DATA_INSUFFICIENT",
}


def _component_signal(signal: str, reason: str, *, enabled: bool) -> str:
    normalized = str(signal).upper().replace("SIGNAL.", "")
    text = str(reason or "").lower()
    if not enabled or normalized == "DISABLED":
        return "DISABLED"
    if normalized == "ERROR" or text.startswith("error:") or text == "strategy_error":
        return "ERROR"
    if normalized == "INSUFFICIENT_DATA" or any(
        token in text
        for token in ("insufficient", "data_error", "calc_failed", "invalid_candle")
    ):
        return "INSUFFICIENT_DATA"
    return normalized if normalized in {"BUY", "SELL", "HOLD"} else "ERROR"


def _failed_component_conditions(
    strategy: str,
    signal: str,
    reason: str,
    meta: dict[str, Any],
) -> list[str]:
    if signal in {"BUY", "SELL", "DISABLED"}:
        return []
    if signal == "ERROR":
        return ["component_execution_error"]
    if signal == "INSUFFICIENT_DATA":
        return ["required_indicator_data"]

    text = " ".join([str(reason or ""), *[str(value) for value in meta.get("reasons", []) or []]]).lower()
    failed: list[str] = []

    if "adx_too_low" in text:
        failed.append("adx_above_threshold")
    if "adx_too_high" in text:
        failed.append("adx_below_reversion_max")
    if strategy == "sma_cross":
        failed.append("fresh_fast_slow_sma_cross")
    elif strategy == "supertrend":
        if "ema_slope_weak" in text:
            failed.append("ema_slope_confirmation")
        if not any(token in text for token in ("cross", "continuation", "supertrend_bullish", "supertrend_bearish")):
            failed.append("supertrend_flip_or_continuation")
    elif strategy == "trend_pullback":
        if not any(token in text for token in ("uptrend", "downtrend")):
            failed.append("ema_trend_alignment")
        elif "rsi_reset" not in text:
            failed.append("rsi_reset_and_turn")
        elif not any(token in text for token in ("price_crossed_ema20", "price_bounce_ema20", "price_reject_ema20")):
            failed.append("ema_reaction")
    elif strategy == "donchian_breakout":
        if "breakout_" not in text:
            failed.append("fresh_donchian_breakout")
        elif "trend_not_aligned" in text:
            failed.append("ema50_ema200_trend_alignment")
        elif "late_breakout" in text:
            failed.append("confirmed_close_within_atr_distance")
        elif "confirmed_close" not in text:
            failed.append("confirmed_close_within_atr_distance")
    elif strategy == "bollinger_reversion":
        if not any(token in text for token in ("at_lower_band", "at_upper_band")):
            failed.append("bollinger_extreme")
        elif not any(token in text for token in ("rsi_oversold", "rsi_overbought")):
            failed.append("rsi_extreme")
        elif "reversal_candle" not in text:
            failed.append("reversal_candle")
    elif strategy == "vwap_reversion":
        if not any(token in text for token in ("below_vwap", "above_vwap")):
            failed.append("vwap_deviation")
        elif not any(token in text for token in ("rsi_oversold", "rsi_overbought")):
            failed.append("rsi_extreme")
        elif "reversion" not in text:
            failed.append("reversion_candle")
    elif strategy == "squeeze_breakout":
        if not bool(meta.get("was_in_squeeze")):
            failed.append("recent_squeeze")
        elif bool(meta.get("in_squeeze")):
            failed.append("squeeze_release")
        elif not any(token in text for token in ("bullish_momentum", "bearish_momentum")):
            failed.append("directional_momentum")

    if not failed:
        failed.append(classify_hold_reason(reason, meta=meta))
    return list(dict.fromkeys(failed))


def classify_hold_reason(
    reason: str | None,
    *,
    confidence: float = 0.0,
    threshold_floor: float = 0.0,
    meta: dict[str, Any] | None = None,
) -> str:
    """Map internal strategy reasons to a stable signal-starvation category."""
    details = meta or {}
    text = " ".join(
        [
            str(reason or ""),
            str(details.get("execution_block_reason") or ""),
            " ".join(str(value) for value in details.get("reasons", []) or []),
        ]
    ).lower()

    if "session" in text or "outside_session" in text:
        return "SESSION_BLOCKED"
    if "regime_blocked" in text or "low_vol_chop" in text or "no_active_strategies" in text:
        return "REGIME_BLOCKED"
    if any(value in text for value in ("insufficient", "data_error", "invalid_candle", "calc_failed", "no_valid_votes")):
        return "DATA_INSUFFICIENT"
    if any(value in text for value in ("volatility", "atr_too", "spike")):
        return "VOLATILITY_FILTER_FAILED"
    if any(value in text for value in ("volume", "liquidity")):
        return "VOLUME_FILTER_FAILED"
    if any(value in text for value in ("risk_reward", "risk/reward", "rr_invalid")):
        return "RISK_REWARD_INVALID"
    if any(value in text for value in ("momentum", "rsi")):
        return "MOMENTUM_FILTER_FAILED"
    if any(value in text for value in ("trend", "adx", "ema_slope", "ema20", "ema50", "ema200", "htf_opposed")):
        return "TREND_FILTER_FAILED"
    if confidence > 0 and threshold_floor > 0 and confidence < threshold_floor:
        return "CONFIDENCE_BELOW_FLOOR"
    return "NO_PATTERN"


def component_breakdown(
    *,
    strategy: str,
    signal: str,
    confidence: float,
    reason: str,
    meta: dict[str, Any] | None,
    threshold_floor: float,
    symbol: str = "",
    timestamp: str | None = None,
    timeframe: str = "",
    market_regime: str = "UNKNOWN",
    session_allowed: bool = True,
    enabled: bool = True,
) -> dict[str, Any]:
    component_meta = meta or {}
    component_signal = _component_signal(signal, reason, enabled=enabled)
    failed = _failed_component_conditions(
        strategy,
        component_signal,
        reason,
        component_meta,
    )
    canonical_failed = []
    if component_signal in {"HOLD", "ERROR", "INSUFFICIENT_DATA"}:
        canonical_failed.append(
            classify_hold_reason(
                reason,
                confidence=confidence,
                threshold_floor=threshold_floor,
                meta=component_meta,
            )
        )
    required = COMPONENT_REQUIRED_CONDITIONS.get(strategy, [])
    diagnostic_timestamp = timestamp or datetime.now(timezone.utc).isoformat()
    return {
        "symbol": symbol,
        "timestamp": diagnostic_timestamp,
        "timeframe": timeframe,
        "market_regime": market_regime,
        "session_allowed": bool(session_allowed),
        "component_name": strategy,
        "component_enabled": bool(enabled),
        "component_signal": component_signal,
        "component_confidence": float(confidence),
        "component_reason": reason,
        "component_required_conditions": required,
        "component_failed_conditions": failed,
        "indicator_snapshot": component_meta,
        # Backward-compatible aliases consumed by the existing starvation audit.
        "strategy": strategy,
        "signal": component_signal,
        "confidence": float(confidence),
        "reason": reason,
        "indicator_values": component_meta,
        "failed_conditions": canonical_failed,
    }


def build_hold_breakdown(
    *,
    symbol: str,
    raw_strategy_signal: str,
    raw_confidence: float,
    final_action: str,
    reason: str,
    meta: dict[str, Any] | None,
    timestamp: str | None = None,
) -> dict[str, Any]:
    details = meta or {}
    threshold_floor = float(
        details.get("ensemble_threshold_floor", details.get("threshold", 0.0)) or 0.0
    )
    components = details.get("component_breakdown") or []
    failed_conditions = list(
        dict.fromkeys(
            condition
            for component in components
            for condition in component.get("failed_conditions", [])
        )
    )
    hold_reason = str(details.get("hold_reason") or "").upper()
    if hold_reason not in HOLD_REASONS:
        hold_reason = classify_hold_reason(
            reason,
            confidence=raw_confidence,
            threshold_floor=threshold_floor,
            meta=details,
        )
    if hold_reason not in failed_conditions:
        failed_conditions.append(hold_reason)
    blocked_regimes = details.get("regime_gate_blocked_regimes") or []
    regime = str(details.get("regime") or "UNKNOWN")
    return {
        "symbol": symbol,
        "timestamp": timestamp or datetime.now(timezone.utc).isoformat(),
        "regime": regime,
        "session_allowed": details.get("session_gate_result") in {"allowed", "disabled"},
        "raw_strategy_signal": str(raw_strategy_signal),
        "raw_confidence": float(raw_confidence),
        "final_action": str(final_action),
        "hold_reason": hold_reason,
        "indicator_values": {
            key: details.get(key)
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
        "failed_conditions": failed_conditions,
        "threshold_floor": threshold_floor,
        "blocked_regime": regime.upper() in {str(value).upper() for value in blocked_regimes},
        "component_breakdown": components,
    }
