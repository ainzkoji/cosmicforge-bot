from __future__ import annotations

from statistics import mean
from typing import Any


SCORE_WEIGHTS = {
    "trend_alignment": 20,
    "market_structure": 20,
    "momentum_confirmation": 15,
    "volatility_suitability": 15,
    "risk_reward_quality": 15,
    "event_news_safety": 10,
    "spread_liquidity_quality": 5,
}


def ema(values: list[float], period: int) -> float:
    if not values:
        return 0.0
    if period <= 1 or len(values) == 1:
        return float(values[-1])
    multiplier = 2 / (period + 1)
    current = float(values[0])
    for value in values[1:]:
        current = (float(value) * multiplier) + (current * (1 - multiplier))
    return current


def rsi(values: list[float], period: int = 14) -> float:
    if len(values) < period + 1:
        return 50.0
    gains: list[float] = []
    losses: list[float] = []
    recent = values[-(period + 1) :]
    for prev, curr in zip(recent, recent[1:]):
        change = curr - prev
        gains.append(max(change, 0.0))
        losses.append(abs(min(change, 0.0)))
    avg_gain = mean(gains) if gains else 0.0
    avg_loss = mean(losses) if losses else 0.0
    if avg_loss == 0:
        return 100.0 if avg_gain > 0 else 50.0
    rs = avg_gain / avg_loss
    return round(100 - (100 / (1 + rs)), 4)


def average_true_range(candles: list[dict[str, float]], period: int = 14) -> float:
    if len(candles) < 2:
        return 0.0
    true_ranges: list[float] = []
    for prev, curr in zip(candles, candles[1:]):
        high = float(curr["high"])
        low = float(curr["low"])
        prev_close = float(prev["close"])
        true_ranges.append(max(high - low, abs(high - prev_close), abs(low - prev_close)))
    if not true_ranges:
        return 0.0
    return round(mean(true_ranges[-period:]), 8)


def candle_momentum(candles: list[dict[str, float]], lookback: int = 5) -> float:
    if len(candles) < lookback + 1:
        return 0.0
    start = float(candles[-(lookback + 1)]["close"])
    end = float(candles[-1]["close"])
    if start <= 0:
        return 0.0
    return (end - start) / start


def calculate_market_features(candles: list[dict[str, float]]) -> dict[str, Any]:
    closes = [float(c["close"]) for c in candles]
    highs = [float(c["high"]) for c in candles]
    lows = [float(c["low"]) for c in candles]
    current_close = closes[-1]
    atr = average_true_range(candles)
    swing_window = min(20, len(candles))
    return {
        "close": current_close,
        "ema_fast": ema(closes, 9),
        "ema_slow": ema(closes, 21),
        "rsi": rsi(closes),
        "atr": atr,
        "atr_pct": atr / current_close if current_close > 0 else 0.0,
        "recent_swing_high": max(highs[-swing_window:]),
        "recent_swing_low": min(lows[-swing_window:]),
        "momentum": candle_momentum(candles),
        "support": min(lows[-swing_window:]),
        "resistance": max(highs[-swing_window:]),
    }


def score_signal(*, side: str, features: dict[str, Any], risk_reward: float) -> tuple[float, dict[str, Any]]:
    side = side.upper()
    close = float(features["close"])
    ema_fast = float(features["ema_fast"])
    ema_slow = float(features["ema_slow"])
    current_rsi = float(features["rsi"])
    momentum = float(features["momentum"])
    atr_pct = float(features["atr_pct"])
    support = float(features["support"])
    resistance = float(features["resistance"])
    range_mid = (support + resistance) / 2 if support and resistance else close

    notes: list[str] = []

    buy_aligned = close > ema_fast > ema_slow
    sell_aligned = close < ema_fast < ema_slow
    trend_ok = (side == "BUY" and buy_aligned) or (side == "SELL" and sell_aligned)
    trend_points = SCORE_WEIGHTS["trend_alignment"] if trend_ok else 4
    notes.append(
        "Trend aligned with EMA fast/slow stack."
        if trend_ok
        else "Trend is not clearly aligned with the requested side."
    )

    structure_ok = (side == "BUY" and close >= range_mid) or (side == "SELL" and close <= range_mid)
    structure_points = SCORE_WEIGHTS["market_structure"] if structure_ok else 8
    notes.append(
        "Market structure supports the setup relative to recent support/resistance."
        if structure_ok
        else "Market structure is not ideal relative to recent support/resistance."
    )

    momentum_ok = (side == "BUY" and current_rsi >= 55 and momentum > 0) or (
        side == "SELL" and current_rsi <= 45 and momentum < 0
    )
    momentum_points = SCORE_WEIGHTS["momentum_confirmation"] if momentum_ok else 5
    notes.append(
        "RSI and candle momentum confirm the setup."
        if momentum_ok
        else "Momentum confirmation is weak or mixed."
    )

    volatility_ok = 0.001 <= atr_pct <= 0.06
    volatility_points = SCORE_WEIGHTS["volatility_suitability"] if volatility_ok else (6 if atr_pct <= 0.10 else 0)
    notes.append(
        "ATR volatility is within the acceptable manual-signal range."
        if volatility_ok
        else "ATR volatility is outside the ideal range for a clean manual signal."
    )

    rr_points = SCORE_WEIGHTS["risk_reward_quality"] if risk_reward >= 2.0 else (10 if risk_reward >= 1.8 else 0)
    notes.append(
        "Risk/reward meets or exceeds the preferred 2R target."
        if risk_reward >= 2.0
        else "Risk/reward only meets the minimum threshold."
        if risk_reward >= 1.8
        else "Risk/reward is below the minimum threshold."
    )

    # V1 has no event blackout feed connected to this standalone module yet.
    event_points = 7
    liquidity_points = SCORE_WEIGHTS["spread_liquidity_quality"]
    notes.append("Event/news safety is neutral because full event filtering is not connected to this signal engine yet.")
    notes.append("Liquidity is assumed from the V1 allowed crypto symbol list.")

    total = trend_points + structure_points + momentum_points + volatility_points + rr_points + event_points + liquidity_points
    details = {
        "trend_alignment": trend_points,
        "market_structure": structure_points,
        "momentum_confirmation": momentum_points,
        "volatility_suitability": volatility_points,
        "risk_reward_quality": rr_points,
        "event_news_safety": event_points,
        "spread_liquidity_quality": liquidity_points,
        "confidence_score": round(float(total), 2),
        "component_weights": dict(SCORE_WEIGHTS),
        "notes": notes,
        "event_news_note": "Event/news safety is not yet connected; neutral V1 score applied.",
        "liquidity_note": "Liquidity assumed from V1 allowed crypto symbol list.",
    }
    return round(float(total), 2), details
