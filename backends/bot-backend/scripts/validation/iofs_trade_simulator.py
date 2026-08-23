#!/usr/bin/env python3
"""Deterministic, conservative IOFS historical trade simulator."""
from __future__ import annotations

import math
from typing import Any

from app.strategy.iofs_components.models import Candle


FIFTEEN_MINUTES_MS = 15 * 60 * 1000
DEFAULT_MAX_HOLD_CANDLES = 48
DEFAULT_ATR_BUFFER_MULTIPLIER = 0.20
DEFAULT_BE_BUFFER_R = 0.20


def create_trade_plan(
    *,
    direction: str,
    structure_level: float,
    atr_15m: float,
    entry_candle: Candle,
    atr_buffer_multiplier: float = DEFAULT_ATR_BUFFER_MULTIPLIER,
    be_buffer_r: float = DEFAULT_BE_BUFFER_R,
) -> dict[str, Any]:
    """Create 1R/2R levels using a structure stop plus a conservative ATR buffer."""
    normalized = str(direction).upper()
    if normalized not in {"UP", "DOWN"}:
        return {"valid": False, "reason": "INVALID_DIRECTION"}
    values = (structure_level, atr_15m, entry_candle.open, atr_buffer_multiplier, be_buffer_r)
    if not all(_finite(value) for value in values) or atr_15m <= 0 or atr_buffer_multiplier < 0:
        return {"valid": False, "reason": "INVALID_RISK"}

    entry = float(entry_candle.open)
    buffer = float(atr_15m) * float(atr_buffer_multiplier)
    if normalized == "UP":
        stop_loss = float(structure_level) - buffer
        risk = entry - stop_loss
        tp1 = entry + risk
        tp2 = entry + (2.0 * risk)
        be_stop = entry + (float(be_buffer_r) * risk)
    else:
        stop_loss = float(structure_level) + buffer
        risk = stop_loss - entry
        tp1 = entry - risk
        tp2 = entry - (2.0 * risk)
        be_stop = entry - (float(be_buffer_r) * risk)

    if not _finite(risk) or risk <= 0:
        return {"valid": False, "reason": "INVALID_RISK"}
    return {
        "valid": True,
        "reason": "OK",
        "direction": normalized,
        "entry_time": entry_candle.open_time,
        "entry": entry,
        "sl": stop_loss,
        "tp1": tp1,
        "tp2": tp2,
        "be_stop": be_stop,
        "risk": risk,
        "be_buffer_r": float(be_buffer_r),
    }


def simulate_trade(
    plan: dict[str, Any],
    future_candles: list[Candle],
    *,
    max_holding_candles: int = DEFAULT_MAX_HOLD_CANDLES,
) -> dict[str, Any]:
    """Walk future candles and resolve outcomes with conservative ambiguity rules."""
    if not plan.get("valid"):
        return _result("INVALID_RISK", 0.0, None, False, False, 0)
    if not future_candles or max_holding_candles <= 0:
        return _result("TIME_EXIT", 0.0, None, False, False, 0)

    direction = plan["direction"]
    entry = float(plan["entry"])
    risk = float(plan["risk"])
    stop = float(plan["sl"])
    tp1 = float(plan["tp1"])
    tp2 = float(plan["tp2"])
    be_stop = float(plan["be_stop"])
    be_buffer_r = float(plan["be_buffer_r"])
    tp1_hit = False
    ambiguous = False
    walked = 0

    for candle in future_candles[:max_holding_candles]:
        walked += 1
        if not tp1_hit:
            sl_touched = _stop_touched(candle, stop, direction)
            tp1_touched = _target_touched(candle, tp1, direction)
            if sl_touched and tp1_touched:
                return _result("SL", -1.0, candle.open_time, False, True, walked)
            if sl_touched:
                return _result("SL", -1.0, candle.open_time, False, ambiguous, walked)
            if not tp1_touched:
                continue
            tp1_hit = True

            tp2_touched = _target_touched(candle, tp2, direction)
            be_touched = _stop_touched(candle, be_stop, direction)
            if tp2_touched and be_touched:
                ambiguous = True
                if _candle_favors_target(candle, direction):
                    return _result("TP2", 1.5, candle.open_time, True, True, walked)
                return _result(
                    "BREAK_EVEN_BUFFER",
                    0.5 + (0.5 * be_buffer_r),
                    candle.open_time,
                    True,
                    True,
                    walked,
                )
            if tp2_touched:
                return _result("TP2", 1.5, candle.open_time, True, ambiguous, walked)
            if be_touched:
                ambiguous = True
                if _candle_favors_target(candle, direction):
                    continue
                return _result(
                    "BREAK_EVEN_BUFFER",
                    0.5 + (0.5 * be_buffer_r),
                    candle.open_time,
                    True,
                    True,
                    walked,
                )
            continue

        tp2_touched = _target_touched(candle, tp2, direction)
        be_touched = _stop_touched(candle, be_stop, direction)
        if tp2_touched and be_touched:
            ambiguous = True
            if _candle_favors_target(candle, direction):
                return _result("TP2", 1.5, candle.open_time, True, True, walked)
            return _result(
                "BREAK_EVEN_BUFFER",
                0.5 + (0.5 * be_buffer_r),
                candle.open_time,
                True,
                True,
                walked,
            )
        if be_touched:
            return _result(
                "BREAK_EVEN_BUFFER",
                0.5 + (0.5 * be_buffer_r),
                candle.open_time,
                True,
                ambiguous,
                walked,
            )
        if tp2_touched:
            return _result("TP2", 1.5, candle.open_time, True, ambiguous, walked)

    final = future_candles[min(len(future_candles), max_holding_candles) - 1]
    mark_r = _mark_r(final.close, entry, risk, direction)
    if tp1_hit:
        mark_r = 0.5 + (0.5 * mark_r)
    return _result("TIME_EXIT", mark_r, final.open_time, tp1_hit, ambiguous, walked)


def _target_touched(candle: Candle, target: float, direction: str) -> bool:
    return candle.high >= target if direction == "UP" else candle.low <= target


def _stop_touched(candle: Candle, stop: float, direction: str) -> bool:
    return candle.low <= stop if direction == "UP" else candle.high >= stop


def _candle_favors_target(candle: Candle, direction: str) -> bool:
    return candle.close > candle.open if direction == "UP" else candle.close < candle.open


def _mark_r(close: float, entry: float, risk: float, direction: str) -> float:
    raw = (close - entry) / risk if direction == "UP" else (entry - close) / risk
    return max(-1.0, min(2.0, raw))


def _result(
    outcome: str,
    r_multiple: float,
    exit_time: int | None,
    tp1_hit: bool,
    ambiguous_candle: bool,
    candles_held: int,
) -> dict[str, Any]:
    return {
        "outcome": outcome,
        "r_multiple": round(float(r_multiple), 6),
        "exit_time": exit_time,
        "tp1_hit": bool(tp1_hit),
        "ambiguous_candle": bool(ambiguous_candle),
        "candles_held": int(candles_held),
    }


def _finite(value: Any) -> bool:
    try:
        return math.isfinite(float(value))
    except (TypeError, ValueError, OverflowError):
        return False
