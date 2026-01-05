from __future__ import annotations


def sma(values: list[float], period: int) -> float:
    if len(values) < period:
        raise ValueError("Not enough data for SMA")
    return sum(values[-period:]) / period


def signal_from_closes(closes: list[float], fast: int = 10, slow: int = 30) -> str:
    """
    Returns: "BUY", "SELL", or "HOLD"
    Simple crossover: fast SMA crosses slow SMA.
    """
    if len(closes) < slow + 2:
        return "HOLD"

    fast_prev = sum(closes[-(fast + 1) : -1]) / fast
    slow_prev = sum(closes[-(slow + 1) : -1]) / slow

    fast_now = sum(closes[-fast:]) / fast
    slow_now = sum(closes[-slow:]) / slow

    if fast_prev <= slow_prev and fast_now > slow_now:
        return "BUY"
    if fast_prev >= slow_prev and fast_now < slow_now:
        return "SELL"
    return "HOLD"
