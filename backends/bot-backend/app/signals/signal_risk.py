from __future__ import annotations

from typing import Any


MIN_STOP_DISTANCE_PCT = 0.0015
MAX_STOP_DISTANCE_PCT = 0.05
MAX_VOLATILITY_ATR_PCT = 0.10
MIN_CONFIDENCE_SCORE = 70.0
MIN_RISK_REWARD = 1.8
MAX_EXPECTED_SIGNAL_DURATION_HOURS = 24
MAX_EXPECTED_SIGNAL_DURATION_MINUTES = MAX_EXPECTED_SIGNAL_DURATION_HOURS * 60
EXPIRY_MINUTES_BY_TIMEFRAME = {
    "15m": 45,
    "30m": 90,
    "1h": 120,
    "4h": 360,
}
DEFAULT_EXPIRY_MINUTES = 120
MIN_VALIDITY_MINUTES = 15
TIMEFRAME_MINUTES = {
    "15m": 15,
    "30m": 30,
    "1h": 60,
    "4h": 240,
}


def calculate_entry_zone(entry_price: float, atr: float | None = None) -> tuple[float, float]:
    if entry_price <= 0:
        raise ValueError("INVALID_ENTRY")
    buffer = max((atr or 0.0) * 0.15, entry_price * 0.001)
    return round(entry_price - buffer, 8), round(entry_price + buffer, 8)


def calculate_stop_loss(
    *,
    side: str,
    entry_price: float,
    recent_swing_low: float,
    recent_swing_high: float,
    atr: float,
) -> float:
    side = side.upper()
    atr_buffer = max(atr * 1.25, entry_price * MIN_STOP_DISTANCE_PCT)
    if side == "BUY":
        stop = min(recent_swing_low, entry_price - atr_buffer)
        return round(stop, 8)
    if side == "SELL":
        stop = max(recent_swing_high, entry_price + atr_buffer)
        return round(stop, 8)
    raise ValueError("INVALID_SIDE")


def calculate_take_profits(*, side: str, entry_price: float, stop_loss: float) -> tuple[float, float, float]:
    side = side.upper()
    risk = abs(entry_price - stop_loss)
    if risk <= 0:
        raise ValueError("INVALID_SL")
    if side == "BUY":
        return (
            round(entry_price + (1.5 * risk), 8),
            round(entry_price + (2.0 * risk), 8),
            round(entry_price + (3.0 * risk), 8),
        )
    if side == "SELL":
        return (
            round(entry_price - (1.5 * risk), 8),
            round(entry_price - (2.0 * risk), 8),
            round(entry_price - (3.0 * risk), 8),
        )
    raise ValueError("INVALID_SIDE")


def calculate_risk_reward(*, entry_price: float, stop_loss: float, take_profit_2: float) -> float:
    risk = abs(entry_price - stop_loss)
    if risk <= 0:
        return 0.0
    reward = abs(take_profit_2 - entry_price)
    return round(reward / risk, 4)


def get_signal_expiry_minutes(timeframe: str | None) -> int:
    """Latest recommended manual entry window by timeframe.

    expires_at represents the latest recommended entry time for the user,
    not necessarily the expected full trade duration.
    """

    return EXPIRY_MINUTES_BY_TIMEFRAME.get(str(timeframe or "").lower(), DEFAULT_EXPIRY_MINUTES)


def calculate_dynamic_validity_minutes(
    *,
    timeframe: str | None,
    atr: float | None,
    entry_price: float,
    entry_zone_low: float | None = None,
    entry_zone_high: float | None = None,
    stop_loss: float,
    recent_volatility: float | None = None,
    trend_strength: float | None = None,
    market_speed: float | None = None,
) -> dict[str, Any]:
    """Return a bounded entry-validity window for a manual signal.

    The window describes how long the entry remains fresh, not the expected
    trade duration. V1 starts with shorter timeframe caps, then adjusts by ATR
    volatility and stop-distance risk.
    """

    del entry_zone_low, entry_zone_high, trend_strength, market_speed
    normalized_timeframe = str(timeframe or "").lower()
    base_minutes = get_signal_expiry_minutes(normalized_timeframe)
    max_minutes = get_signal_expiry_minutes(normalized_timeframe)
    if entry_price <= 0 or stop_loss <= 0:
        return {
            "valid": False,
            "validity_minutes": MIN_VALIDITY_MINUTES,
            "reason": "Invalid entry/stop data; minimum validity fallback applied.",
            "rejection_reason": "INVALID_ENTRY" if entry_price <= 0 else "INVALID_SL",
        }

    atr_percent = None
    if atr is not None and atr > 0:
        atr_percent = atr / entry_price
    elif recent_volatility is not None:
        atr_percent = recent_volatility

    multiplier = 1.0
    notes = [
        f"{normalized_timeframe or 'unknown'} base latest-entry window {base_minutes}m.",
        "expires_at is the latest recommended entry time, not the full expected trade duration.",
    ]
    if atr_percent is None:
        notes.append("ATR volatility unavailable; no volatility adjustment applied.")
    elif atr_percent < 0.003:
        multiplier *= 1.25
        notes.append("Low ATR volatility increased the window by 25%.")
    elif atr_percent <= 0.015:
        notes.append("Normal ATR volatility kept the window unchanged.")
    elif atr_percent <= 0.03:
        multiplier *= 0.75
        notes.append("High ATR volatility reduced the window by 25%.")
    else:
        multiplier *= 0.50
        notes.append("Extreme ATR volatility reduced the window by 50%.")

    risk_percent = abs(entry_price - stop_loss) / entry_price
    if risk_percent <= 0:
        return {
            "valid": False,
            "validity_minutes": MIN_VALIDITY_MINUTES,
            "reason": "Stop distance is zero or negative; minimum validity fallback applied.",
            "rejection_reason": "INVALID_SL",
        }
    if risk_percent < 0.003:
        multiplier *= 0.75
        notes.append("Tight stop distance reduced the window by 25%.")
    elif risk_percent <= 0.025:
        notes.append("Stop distance is in the normal entry-validity range.")
    elif risk_percent > 0.05:
        multiplier *= 0.50
        notes.append("Very wide stop distance heavily reduced the window.")
    else:
        notes.append("Wide stop distance kept quality validation responsible for final acceptance.")

    final_minutes = round(base_minutes * multiplier)
    final_minutes = max(MIN_VALIDITY_MINUTES, min(max_minutes, final_minutes))
    notes.append(f"Final bounded entry window is {final_minutes}m.")
    return {
        "valid": True,
        "validity_minutes": int(final_minutes),
        "reason": " ".join(notes),
        "atr_percent": atr_percent,
        "risk_percent": risk_percent,
    }


def estimate_expected_signal_duration_minutes(
    *,
    timeframe: str | None,
    atr: float | None,
    entry_price: float,
    take_profit_2: float | None,
) -> dict[str, Any]:
    """Estimate whether TP2 is close enough for an intraday signal.

    This is not a prediction or guarantee. It simply prevents publishing setups
    whose TP2 distance is mathematically too large for daily signal handling.
    """

    normalized_timeframe = str(timeframe or "").lower()
    candle_minutes = TIMEFRAME_MINUTES.get(normalized_timeframe)
    if candle_minutes is None:
        return {
            "valid": False,
            "reason": "UNSUPPORTED_TIMEFRAME",
            "estimated_minutes": None,
            "notes": [f"Unsupported timeframe for expected-duration estimate: {timeframe}."],
        }
    if atr is None or atr <= 0:
        return {
            "valid": False,
            "reason": "INSUFFICIENT_MARKET_DATA",
            "estimated_minutes": None,
            "notes": ["ATR is required to estimate expected signal duration."],
        }
    if entry_price <= 0 or take_profit_2 is None or float(take_profit_2) <= 0:
        return {
            "valid": False,
            "reason": "INVALID_TP",
            "estimated_minutes": None,
            "notes": ["Entry price and TP2 are required to estimate expected signal duration."],
        }

    distance_to_tp2 = abs(float(take_profit_2) - float(entry_price))
    estimated_candles = distance_to_tp2 / float(atr)
    estimated_minutes = estimated_candles * candle_minutes
    if estimated_minutes > MAX_EXPECTED_SIGNAL_DURATION_MINUTES:
        return {
            "valid": False,
            "reason": "EXPECTED_DURATION_TOO_LONG",
            "estimated_minutes": round(estimated_minutes, 2),
            "estimated_candles": round(estimated_candles, 4),
            "notes": [
                f"Estimated TP2 duration is {estimated_minutes:.1f} minutes, "
                f"above the {MAX_EXPECTED_SIGNAL_DURATION_HOURS}h intraday maximum."
            ],
        }
    return {
        "valid": True,
        "reason": None,
        "estimated_minutes": round(estimated_minutes, 2),
        "estimated_candles": round(estimated_candles, 4),
        "notes": [
            f"Estimated TP2 duration is {estimated_minutes:.1f} minutes, "
            f"within the {MAX_EXPECTED_SIGNAL_DURATION_HOURS}h intraday maximum."
        ],
    }


def validate_stop_distance(
    *,
    entry_price: float,
    stop_loss: float,
    min_pct: float = MIN_STOP_DISTANCE_PCT,
    max_pct: float = MAX_STOP_DISTANCE_PCT,
    atr: float | None = None,
) -> tuple[bool, str | None]:
    if entry_price <= 0 or stop_loss <= 0:
        return False, "INVALID_SL"
    stop_distance = abs(entry_price - stop_loss)
    stop_pct = stop_distance / entry_price
    if stop_distance <= 0:
        return False, "INVALID_SL"
    if stop_pct > max_pct:
        return False, "STOP_TOO_WIDE"
    if stop_pct < min_pct:
        return False, "STOP_TOO_TIGHT"
    if atr is not None and atr > 0:
        if stop_distance < atr * 0.35:
            return False, "STOP_TOO_TIGHT"
        if stop_distance > atr * 10:
            return False, "STOP_TOO_WIDE"
    return True, None


def validate_signal_prices(data: dict[str, Any], *, atr: float | None = None) -> tuple[bool, str | None]:
    side = str(data.get("side") or "").upper()
    entry = data.get("entry_price")
    stop = data.get("stop_loss")
    tp1 = data.get("take_profit_1")
    tp2 = data.get("take_profit_2")
    tp3 = data.get("take_profit_3")
    if side not in {"BUY", "SELL"}:
        return False, "INVALID_SIDE"
    if entry is None or float(entry) <= 0:
        return False, "INVALID_ENTRY"
    if stop is None or float(stop) <= 0:
        return False, "INVALID_SL"
    if tp1 is None or float(tp1) <= 0:
        return False, "INVALID_TP"
    values = [float(entry), float(stop), float(tp1)]
    if tp2 is not None:
        values.append(float(tp2))
    if tp3 is not None:
        values.append(float(tp3))
    if any(value <= 0 for value in values):
        return False, "INVALID_TP"
    entry_f = float(entry)
    stop_f = float(stop)
    tp1_f = float(tp1)
    tp2_f = float(tp2) if tp2 is not None else None
    tp3_f = float(tp3) if tp3 is not None else None
    if side == "BUY":
        if stop_f >= entry_f:
            return False, "INVALID_SL"
        if tp1_f <= entry_f:
            return False, "INVALID_TP"
        if tp2_f is not None and tp2_f <= tp1_f:
            return False, "INVALID_TP"
        if tp3_f is not None and (tp2_f is None or tp3_f <= tp2_f):
            return False, "INVALID_TP"
    if side == "SELL":
        if stop_f <= entry_f:
            return False, "INVALID_SL"
        if tp1_f >= entry_f:
            return False, "INVALID_TP"
        if tp2_f is not None and tp2_f >= tp1_f:
            return False, "INVALID_TP"
        if tp3_f is not None and (tp2_f is None or tp3_f >= tp2_f):
            return False, "INVALID_TP"
    return validate_stop_distance(entry_price=entry_f, stop_loss=stop_f, atr=atr)


def validate_signal_quality(
    data: dict[str, Any],
    *,
    atr: float | None = None,
    atr_pct: float | None = None,
    min_confidence: float = MIN_CONFIDENCE_SCORE,
    min_risk_reward: float = MIN_RISK_REWARD,
) -> dict[str, Any]:
    notes: list[str] = []
    if data.get("entry_price") is None:
        return {"valid": False, "reason": "INVALID_ENTRY", "notes": ["Missing entry price."]}
    if data.get("stop_loss") is None:
        return {"valid": False, "reason": "INVALID_SL", "notes": ["Missing stop loss."]}
    if data.get("take_profit_1") is None:
        return {"valid": False, "reason": "INVALID_TP", "notes": ["Missing take profit 1."]}
    if data.get("take_profit_2") is None:
        return {"valid": False, "reason": "INVALID_TP", "notes": ["Missing take profit 2."]}
    if not data.get("expires_at"):
        return {"valid": False, "reason": "MISSING_EXPIRY", "notes": ["Missing signal expiry."]}

    price_valid, price_reason = validate_signal_prices(data, atr=atr)
    if not price_valid:
        return {"valid": False, "reason": price_reason, "notes": [f"Price validation failed: {price_reason}."]}

    confidence = float(data.get("confidence_score") or 0.0)
    if confidence < min_confidence:
        return {
            "valid": False,
            "reason": "LOW_CONFIDENCE",
            "notes": [f"Confidence score {confidence:.2f} is below required minimum {min_confidence:.2f}."],
        }

    risk_reward = float(data.get("risk_reward") or 0.0)
    if risk_reward < min_risk_reward:
        return {
            "valid": False,
            "reason": "RISK_REWARD_TOO_LOW",
            "notes": [f"Risk/reward {risk_reward:.2f} is below required minimum {min_risk_reward:.2f}."],
        }

    if atr_pct is None and atr is not None and float(data.get("entry_price") or 0) > 0:
        atr_pct = atr / float(data["entry_price"])
    if atr_pct is None:
        return {"valid": False, "reason": "INSUFFICIENT_MARKET_DATA", "notes": ["ATR volatility data is missing."]}
    if atr_pct > MAX_VOLATILITY_ATR_PCT:
        return {
            "valid": False,
            "reason": "VOLATILITY_TOO_HIGH",
            "notes": [f"ATR volatility {atr_pct * 100:.2f}% exceeds maximum {MAX_VOLATILITY_ATR_PCT * 100:.2f}%."],
        }

    duration = estimate_expected_signal_duration_minutes(
        timeframe=str(data.get("timeframe") or "1h"),
        atr=atr,
        entry_price=float(data["entry_price"]),
        take_profit_2=float(data["take_profit_2"]),
    )
    if not duration["valid"]:
        return {"valid": False, "reason": duration["reason"], "notes": duration["notes"]}

    notes.append("Signal passed price, stop-distance, volatility, confidence, risk/reward, and intraday-duration checks.")
    notes.extend(duration.get("notes") or [])
    return {"valid": True, "reason": None, "notes": notes}


def explain_risk_setup(data: dict[str, Any]) -> str:
    side = str(data.get("side") or "").upper()
    entry = float(data.get("entry_price") or 0.0)
    stop = float(data.get("stop_loss") or 0.0)
    tp1 = data.get("take_profit_1")
    tp2 = data.get("take_profit_2")
    tp3 = data.get("take_profit_3")
    rr = data.get("risk_reward")
    stop_pct = (abs(entry - stop) / entry * 100) if entry > 0 and stop > 0 else 0.0
    return (
        f"{side} risk setup: entry={entry:.8f}, stop_loss={stop:.8f} "
        f"({stop_pct:.2f}% stop distance), TP1={float(tp1):.8f}, "
        f"TP2={float(tp2):.8f}, TP3={float(tp3):.8f}, risk_reward={float(rr):.2f}."
    )
