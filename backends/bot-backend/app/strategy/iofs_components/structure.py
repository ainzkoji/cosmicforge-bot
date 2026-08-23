from __future__ import annotations

import math
from dataclasses import dataclass

from app.strategy.iofs_components.models import (
    Candle,
    StructureResult,
    validate_candles,
)

LOOKBACK = 20
SWING_STRENGTH = 3


@dataclass(frozen=True)
class _BrokenLevel:
    price: float
    break_index: int
    candles_ago: int


def find_structure_retest(
    candles_1h: list[Candle],
    direction: str,
    atr: float,
) -> StructureResult:
    """Find a recent structure break, active retest, and wick rejection."""
    empty = StructureResult(False, None, None, 0.0, None, "EMPTY_CANDLES")
    if not candles_1h:
        return empty
    if not isinstance(candles_1h, list):
        return StructureResult(False, None, None, 0.0, None, "BAD_CANDLES")

    normalized_direction = str(direction).upper()
    if normalized_direction not in {"UP", "DOWN"}:
        return StructureResult(False, None, None, 0.0, None, "INVALID_DIRECTION")
    try:
        atr = float(atr)
    except (TypeError, ValueError, OverflowError):
        return StructureResult(False, None, None, 0.0, None, "INVALID_ATR")
    if not math.isfinite(atr) or atr <= 0:
        return StructureResult(False, None, None, 0.0, None, "INVALID_ATR")
    if len(candles_1h) < (SWING_STRENGTH * 2) + 2:
        return StructureResult(False, None, None, 0.0, None, "INSUFFICIENT_CANDLES")

    try:
        validate_candles(candles_1h)
    except (TypeError, ValueError):
        return StructureResult(False, None, None, 0.0, None, "BAD_CANDLES")

    swing_levels = _find_swing_levels(candles_1h, normalized_direction)
    if not swing_levels:
        return StructureResult(False, None, None, 0.0, None, "NO_SWING_LEVEL")

    fresh, stale_exists = _find_broken_level(
        candles_1h, swing_levels, normalized_direction, atr
    )
    if fresh is None:
        reason = "STALE_BREAK" if stale_exists else "NO_BROKEN_LEVEL"
        return StructureResult(False, None, None, 0.0, None, reason)

    distance = abs(candles_1h[-1].close - fresh.price)
    distance_atr = distance / atr
    if distance > 0.30 * atr:
        return StructureResult(
            False,
            fresh.price,
            fresh.candles_ago,
            0.0,
            distance_atr,
            "NO_RETEST",
        )

    rejection_strength = _rejection_strength(
        candles_1h, fresh, normalized_direction, atr
    )
    if rejection_strength <= 0:
        return StructureResult(
            False,
            fresh.price,
            fresh.candles_ago,
            0.0,
            distance_atr,
            "NO_REJECTION",
        )

    return StructureResult(
        True,
        fresh.price,
        fresh.candles_ago,
        rejection_strength,
        distance_atr,
    )


def _find_swing_levels(candles: list[Candle], direction: str) -> list[tuple[int, float]]:
    levels: list[tuple[int, float]] = []
    for index in range(SWING_STRENGTH, len(candles) - SWING_STRENGTH):
        before = candles[index - SWING_STRENGTH : index]
        after = candles[index + 1 : index + SWING_STRENGTH + 1]
        if direction == "UP":
            price = candles[index].high
            if all(price > candle.high for candle in before + after):
                levels.append((index, price))
        else:
            price = candles[index].low
            if all(price < candle.low for candle in before + after):
                levels.append((index, price))
    return levels


def _find_broken_level(
    candles: list[Candle],
    swing_levels: list[tuple[int, float]],
    direction: str,
    atr: float,
) -> tuple[_BrokenLevel | None, bool]:
    fresh: list[_BrokenLevel] = []
    stale_exists = False
    last_breakable_index = len(candles) - 2

    for swing_index, price in swing_levels:
        threshold = price + 0.10 * atr if direction == "UP" else price - 0.10 * atr
        for index in range(swing_index + 1, last_breakable_index + 1):
            previous_close = candles[index - 1].close
            close = candles[index].close
            crossed = (
                previous_close <= threshold < close
                if direction == "UP"
                else previous_close >= threshold > close
            )
            if not crossed:
                continue

            candles_ago = len(candles) - 1 - index
            broken = _BrokenLevel(price, index, candles_ago)
            if candles_ago <= LOOKBACK:
                fresh.append(broken)
            else:
                stale_exists = True

    if not fresh:
        return None, stale_exists
    return max(fresh, key=lambda broken: broken.break_index), stale_exists


def _rejection_strength(
    candles: list[Candle],
    broken: _BrokenLevel,
    direction: str,
    atr: float,
) -> float:
    tolerance = 0.30 * atr
    start = max(broken.break_index + 1, len(candles) - 3)
    strongest = 0.0
    for candle in candles[start:]:
        if direction == "UP":
            wick = min(candle.open, candle.close) - candle.low
            rejects = candle.low <= broken.price + tolerance and candle.close > broken.price
        else:
            wick = candle.high - max(candle.open, candle.close)
            rejects = candle.high >= broken.price - tolerance and candle.close < broken.price
        if rejects and wick > 0:
            strongest = max(strongest, wick / atr)
    return strongest
