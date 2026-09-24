"""Section 22 test helpers: a deterministic SYNTHETIC OHLCV generator.

Synthetic data exercises the certification framework; it can NEVER certify
anything (its data_source classifies as SYNTHETIC_TEST).
"""
from __future__ import annotations

import math
import random
from typing import Dict, List, Sequence

BAR_15M = 900_000
#: 2026-01-01T00:00:00Z
T0 = 1_767_225_600_000
SYNTHETIC_SOURCES = ("synthetic_test",)
REAL_SOURCES = ("binance",)


def ohlcv(seed: int, n: int, *, start: int = T0, bar: int = BAR_15M, price: float = 100.0) -> List[list]:
    """Regime-switching walk: trend segments and ranges, real intrabar highs/lows."""
    rng = random.Random(seed)
    rows, drift, left = [], 0.0, 0
    for i in range(n):
        if left <= 0:
            drift = rng.choice((0.0015, -0.0015, 0.0, 0.0, 0.0006, -0.0006))
            left = rng.randint(40, 160)
        left -= 1
        o = price
        c = o * math.exp(drift + rng.gauss(0, 0.004))
        hi = max(o, c) * (1 + abs(rng.gauss(0, 0.0025)))
        lo = min(o, c) * (1 - abs(rng.gauss(0, 0.0025)))
        vol = 1000.0 * math.exp(rng.gauss(0, 0.4))
        t = start + i * bar
        rows.append([t, o, hi, lo, c, vol, t + bar - 1, vol * c, 100])
        price = c
    return rows


def series(symbols: Sequence[str] = ("BTCUSDT", "ETHUSDT"), *, n: int = 1500, seed: int = 7,
           timeframe: str = "15m") -> Dict[str, Dict[str, list]]:
    return {s: {timeframe: ohlcv(seed + i * 101, n, price=100.0 * (i + 1))} for i, s in enumerate(symbols)}


def meta(symbols: Sequence[str] = ("BTCUSDT", "ETHUSDT")) -> Dict[str, Dict[str, str]]:
    return {s: {"base": s[:-4], "quote": "USDT"} for s in symbols}
