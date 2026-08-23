from __future__ import annotations

import asyncio
import inspect
import time
from collections.abc import Callable
from typing import Any

from app.strategy.iofs_components.models import Candle


class MultiTimeframeFetchError(RuntimeError):
    """Raised when IOFS candle data cannot be fetched or validated."""


class MultiTimeframeFetcher:
    """Fetches 4H, 1H, and 15M candles for IOFS evaluation."""

    REQUIRED = {
        "4h": 220,
        "1h": 50,
        "15m": 30,
    }
    CACHE_TTL_SECONDS = {
        "4h": 15 * 60,
        "1h": 5 * 60,
        "15m": 0,
    }

    def __init__(
        self,
        client: Any,
        *,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._client = client
        self._clock = clock
        self._cache: dict[tuple[str, str], tuple[float, tuple[Candle, ...]]] = {}

    async def fetch_all(self, symbol: str) -> dict[str, list[Candle]]:
        """
        Fetch all required timeframes concurrently through the injected client.

        Raises MultiTimeframeFetchError rather than returning partial data.
        """
        if not isinstance(symbol, str) or not symbol.strip():
            raise MultiTimeframeFetchError("invalid_symbol")

        normalized_symbol = symbol.strip().upper()
        try:
            results = await asyncio.gather(
                *(
                    self._fetch_timeframe(normalized_symbol, interval, limit)
                    for interval, limit in self.REQUIRED.items()
                )
            )
        except MultiTimeframeFetchError:
            raise
        except Exception as exc:
            raise MultiTimeframeFetchError(f"fetch_failed: {exc}") from exc

        return {
            interval: candles
            for interval, candles in zip(self.REQUIRED, results, strict=True)
        }

    async def _fetch_timeframe(
        self,
        symbol: str,
        interval: str,
        limit: int,
    ) -> list[Candle]:
        cached = self._get_cached(symbol, interval)
        if cached is not None:
            return cached

        raw_candles = await self._call_client(symbol, interval, limit)
        if raw_candles is None or isinstance(raw_candles, (str, bytes, dict)):
            raise MultiTimeframeFetchError(f"{interval}: missing_candles")

        try:
            candles = [Candle.from_raw(raw) for raw in raw_candles]
        except (TypeError, ValueError, KeyError, OverflowError) as exc:
            raise MultiTimeframeFetchError(f"{interval}: invalid_candles") from exc

        if len(candles) < limit:
            raise MultiTimeframeFetchError(
                f"{interval}: underfilled_candles ({len(candles)} < {limit})"
            )

        candles = candles[-limit:]
        self._put_cached(symbol, interval, candles)
        return list(candles)

    async def _call_client(self, symbol: str, interval: str, limit: int) -> Any:
        method = self._resolve_client_method()
        if inspect.iscoroutinefunction(method):
            return await method(symbol, interval, limit)

        result = await asyncio.to_thread(method, symbol, interval, limit)
        if inspect.isawaitable(result):
            return await result
        return result

    def _resolve_client_method(self) -> Callable[..., Any]:
        for name in ("get_klines", "klines", "fetch_candles"):
            method = getattr(self._client, name, None)
            if callable(method):
                return method
        raise MultiTimeframeFetchError("client_has_no_candle_method")

    def _get_cached(self, symbol: str, interval: str) -> list[Candle] | None:
        cached = self._cache.get((symbol, interval))
        if cached is None:
            return None
        expires_at, candles = cached
        if self._clock() >= expires_at:
            self._cache.pop((symbol, interval), None)
            return None
        return list(candles)

    def _put_cached(self, symbol: str, interval: str, candles: list[Candle]) -> None:
        ttl = self.CACHE_TTL_SECONDS[interval]
        if ttl > 0:
            self._cache[(symbol, interval)] = (
                self._clock() + ttl,
                tuple(candles),
            )
