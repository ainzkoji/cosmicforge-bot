import random

import pytest

from app.trading_intelligence.contracts.market_state import CandleSeries


def make_binance_klines(n: int, *, start_time: int = 1_700_000_000_000, interval_ms: int = 900_000,
                         trend: float = 0.0, vol: float = 1.0, base: float = 100.0, seed: int = 7):
    """Binance-style kline rows: [openTime, open, high, low, close, volume,
    closeTime, quoteVolume, numTrades, takerBuyBase, takerBuyQuote, ignore]."""
    rng = random.Random(seed)
    rows = []
    price = base
    for i in range(n):
        open_time = start_time + i * interval_ms
        close_time = open_time + interval_ms - 1
        open_price = price
        price = price + trend + rng.uniform(-vol, vol)
        close_price = price
        high = max(open_price, close_price) + abs(rng.uniform(0, vol))
        low = min(open_price, close_price) - abs(rng.uniform(0, vol))
        volume = 1000 + rng.uniform(-50, 50)
        taker_buy = volume * rng.uniform(0.3, 0.7)
        num_trades = 100 + rng.randint(-10, 10)
        rows.append(
            [
                open_time,
                str(open_price),
                str(high),
                str(low),
                str(close_price),
                str(volume),
                close_time,
                str(volume * close_price),
                num_trades,
                str(taker_buy),
                str(taker_buy * close_price),
                "0",
            ]
        )
    return rows


def make_candle_series(n: int, **kwargs) -> CandleSeries:
    from app.trading_intelligence.integration.snapshot_adapter import build_candle_series

    rows = make_binance_klines(n, **kwargs)
    return build_candle_series(rows)


@pytest.fixture
def trending_series() -> CandleSeries:
    return make_candle_series(150, trend=0.8, vol=1.0, seed=1)


@pytest.fixture
def ranging_series() -> CandleSeries:
    return make_candle_series(150, trend=0.0, vol=1.5, seed=2)


@pytest.fixture
def short_series() -> CandleSeries:
    return make_candle_series(5, seed=3)
