from __future__ import annotations

from typing import List


def sma(values: List[float], period: int) -> float:
    if len(values) < period:
        raise ValueError("not_enough_data")
    return sum(values[-period:]) / float(period)


def ema(values: List[float], period: int) -> float:
    if len(values) < period:
        raise ValueError("not_enough_data")
    k = 2 / (period + 1)
    e = values[0]
    for v in values[1:]:
        e = v * k + e * (1 - k)
    return e


def rsi(closes: List[float], period: int = 14) -> float:
    if len(closes) < period + 1:
        raise ValueError("not_enough_data")
    gains = 0.0
    losses = 0.0
    for i in range(-period, 0):
        diff = closes[i] - closes[i - 1]
        if diff >= 0:
            gains += diff
        else:
            losses += abs(diff)
    if losses == 0:
        return 100.0
    rs = gains / losses
    return 100 - (100 / (1 + rs))
