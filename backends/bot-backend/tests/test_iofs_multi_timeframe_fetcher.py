from __future__ import annotations

import asyncio
import inspect

import pytest

from app.data.multi_timeframe_fetcher import (
    MultiTimeframeFetcher,
    MultiTimeframeFetchError,
)
from app.strategy.iofs_components.models import Candle


def _raw_candles(count: int, start: float = 100.0) -> list[list[float]]:
    return [
        [index, start, start + 1.0, start - 1.0, start + 0.25, 10.0]
        for index in range(count)
    ]


class FakeClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, int]] = []

    def get_klines(self, symbol: str, interval: str, limit: int):
        self.calls.append((symbol, interval, limit))
        return _raw_candles(limit)


def test_fetches_required_timeframes_limits_and_uses_injected_client():
    client = FakeClient()
    result = asyncio.run(MultiTimeframeFetcher(client).fetch_all("btcusdt"))

    assert set(result) == {"4h", "1h", "15m"}
    assert {interval: len(candles) for interval, candles in result.items()} == {
        "4h": 220,
        "1h": 50,
        "15m": 30,
    }
    assert set(client.calls) == {
        ("BTCUSDT", "4h", 220),
        ("BTCUSDT", "1h", 50),
        ("BTCUSDT", "15m", 30),
    }
    assert all(isinstance(candle, Candle) for candles in result.values() for candle in candles)


def test_cache_policy_uses_15_minutes_5_minutes_and_never_caches_15m():
    now = [1_000.0]
    client = FakeClient()
    fetcher = MultiTimeframeFetcher(client, clock=lambda: now[0])

    asyncio.run(fetcher.fetch_all("BTCUSDT"))
    asyncio.run(fetcher.fetch_all("BTCUSDT"))
    assert _call_counts(client) == {"4h": 1, "1h": 1, "15m": 2}

    now[0] += 301
    asyncio.run(fetcher.fetch_all("BTCUSDT"))
    assert _call_counts(client) == {"4h": 1, "1h": 2, "15m": 3}

    now[0] += 600
    asyncio.run(fetcher.fetch_all("BTCUSDT"))
    assert _call_counts(client) == {"4h": 2, "1h": 3, "15m": 4}


def test_underfilled_timeframe_fails_without_partial_result():
    class UnderfilledClient(FakeClient):
        def get_klines(self, symbol: str, interval: str, limit: int):
            self.calls.append((symbol, interval, limit))
            return _raw_candles(limit - 1 if interval == "1h" else limit)

    with pytest.raises(MultiTimeframeFetchError, match="1h: underfilled_candles"):
        asyncio.run(MultiTimeframeFetcher(UnderfilledClient()).fetch_all("BTCUSDT"))


def test_missing_or_bad_candles_fail_safely():
    class MissingClient(FakeClient):
        def get_klines(self, symbol: str, interval: str, limit: int):
            return None if interval == "4h" else _raw_candles(limit)

    class BadClient(FakeClient):
        def get_klines(self, symbol: str, interval: str, limit: int):
            rows = _raw_candles(limit)
            rows[-1][2] = rows[-1][3] - 1
            return rows

    with pytest.raises(MultiTimeframeFetchError, match="4h: missing_candles"):
        asyncio.run(MultiTimeframeFetcher(MissingClient()).fetch_all("BTCUSDT"))
    with pytest.raises(MultiTimeframeFetchError, match="invalid_candles"):
        asyncio.run(MultiTimeframeFetcher(BadClient()).fetch_all("BTCUSDT"))


def test_supports_async_exchange_method():
    class AsyncClient:
        def __init__(self) -> None:
            self.calls = []

        async def get_klines(self, symbol: str, interval: str, limit: int):
            self.calls.append((symbol, interval, limit))
            await asyncio.sleep(0)
            return _raw_candles(limit)

    client = AsyncClient()
    result = asyncio.run(MultiTimeframeFetcher(client).fetch_all("ETHUSDT"))
    assert len(result["4h"]) == 220
    assert len(client.calls) == 3


def test_fetcher_has_no_live_binance_dependency():
    source = inspect.getsource(inspect.getmodule(MultiTimeframeFetcher))
    assert "binance" not in source.lower()


def _call_counts(client: FakeClient) -> dict[str, int]:
    return {
        interval: sum(call_interval == interval for _, call_interval, _ in client.calls)
        for interval in MultiTimeframeFetcher.REQUIRED
    }
