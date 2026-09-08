"""The historical clock and the snapshot provider it drives (§13.1–§13.4).

Look-ahead is the failure mode that makes a backtest confidently wrong rather
than merely inaccurate, and it is easy to introduce by accident: one `[-1]` on
an unfiltered series, one higher-timeframe candle that has not closed yet, one
"current price" read from a bar the strategy could not have seen.

So the cut is made in one place and enforced by construction:

* :class:`HistoricalClock` owns "now". It only moves forward.
* :class:`HistoricalMarketDataProvider` slices every series at the clock and
  returns a ``MarketSnapshot`` built from the result. A caller that wants raw
  candles gets them through the same cut.

The higher-timeframe rule (§13.4) is the subtle one. A 15m decision at 14:45
may consult the 1h candle that closed at 14:00, never the one that closes at
15:00 — that candle does not exist yet at decision time. The provider drops any
HTF candle whose close is after the strategy candle's close, and
``MarketSnapshot.htf_is_timestamp_aligned()`` independently re-checks it.
"""
from __future__ import annotations

import logging
from bisect import bisect_right
from dataclasses import dataclass, field
from typing import Any, Iterator, Mapping, Sequence

from app.runner.market_snapshot import MarketSnapshot

logger = logging.getLogger(__name__)

TIMEFRAME_MS: Mapping[str, int] = {
    "1m": 60_000,
    "3m": 180_000,
    "5m": 300_000,
    "15m": 900_000,
    "30m": 1_800_000,
    "1h": 3_600_000,
    "2h": 7_200_000,
    "4h": 14_400_000,
    "6h": 21_600_000,
    "12h": 43_200_000,
    "1d": 86_400_000,
}


class ReplayDataError(RuntimeError):
    """The replay cannot proceed on the data it was given."""


def _close_time(row: Any) -> int:
    """Exchange close timestamp of one candle row, in ms."""
    if isinstance(row, Mapping):
        for key in ("closeTime", "close_time", "close_timestamp"):
            if key in row:
                return int(row[key])
        raise ReplayDataError(f"candle mapping has no close time: {sorted(row)}")
    return int(row[6])


def _open_time(row: Any) -> int:
    if isinstance(row, Mapping):
        for key in ("openTime", "open_time"):
            if key in row:
                return int(row[key])
        raise ReplayDataError(f"candle mapping has no open time: {sorted(row)}")
    return int(row[0])


def _close_price(row: Any) -> float:
    if isinstance(row, Mapping):
        for key in ("close", "c", "closePrice"):
            if key in row:
                return float(row[key])
        raise ReplayDataError("candle mapping has no close price")
    return float(row[4])


# ── The clock ───────────────────────────────────────────────────────────────


@dataclass
class HistoricalClock:
    """Replay "now". Monotonic by construction.

    ``now_ms`` is the moment the system is being asked to make a decision. A
    candle is visible only once its close time has passed, which is the same
    rule the live runtime applies -- so a bar that is still forming is invisible
    in replay exactly as it is invisible live.
    """

    now_ms: int
    _started_ms: int = field(init=False)

    def __post_init__(self) -> None:
        self.now_ms = int(self.now_ms)
        self._started_ms = self.now_ms

    def advance_to(self, timestamp_ms: int) -> None:
        timestamp_ms = int(timestamp_ms)
        if timestamp_ms < self.now_ms:
            raise ReplayDataError(
                f"the historical clock cannot go backwards: "
                f"{timestamp_ms} < {self.now_ms}"
            )
        self.now_ms = timestamp_ms

    @property
    def started_ms(self) -> int:
        return self._started_ms

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return f"HistoricalClock(now_ms={self.now_ms})"


# ── The provider ────────────────────────────────────────────────────────────


class HistoricalMarketDataProvider:
    """Serves the production ``MarketSnapshot`` contract from historical data.

    Series are supplied once, sorted ascending by close time, and never mutated.
    Every read is a slice at the clock, so there is no path by which a caller
    can obtain a candle from the future.
    """

    def __init__(
        self,
        series: Mapping[str, Mapping[str, Sequence[Any]]],
        clock: HistoricalClock,
        *,
        source: str = "REPLAY",
        source_environment: str | None = "REPLAY",
    ) -> None:
        self.clock = clock
        self.source = source
        self.source_environment = source_environment
        self._series: dict[str, dict[str, tuple[Any, ...]]] = {}
        self._closes: dict[str, dict[str, list[int]]] = {}

        for symbol, by_timeframe in series.items():
            key = symbol.upper()
            self._series[key] = {}
            self._closes[key] = {}
            for timeframe, rows in by_timeframe.items():
                ordered = self._validate(key, timeframe, rows)
                self._series[key][timeframe] = ordered
                self._closes[key][timeframe] = [_close_time(r) for r in ordered]

    # ── Loading and validation ──────────────────────────────────────────────

    @staticmethod
    def _validate(symbol: str, timeframe: str, rows: Sequence[Any]) -> tuple[Any, ...]:
        """Reject data that would silently corrupt a replay (§14.8 in miniature).

        Out-of-order or duplicated candles are not something to sort quietly:
        they mean the loader is wrong, and a replay built on them would produce
        results nobody could reproduce.
        """
        if not rows:
            raise ReplayDataError(f"{symbol} {timeframe}: no candles supplied")
        ordered = tuple(rows)
        previous = None
        for index, row in enumerate(ordered):
            close = _close_time(row)
            if previous is not None:
                if close == previous:
                    raise ReplayDataError(
                        f"{symbol} {timeframe}: duplicate candle close at index "
                        f"{index} ({close})"
                    )
                if close < previous:
                    raise ReplayDataError(
                        f"{symbol} {timeframe}: candles are not ascending at index "
                        f"{index} ({close} after {previous})"
                    )
            if _open_time(row) >= close:
                raise ReplayDataError(
                    f"{symbol} {timeframe}: candle at index {index} opens at or "
                    f"after it closes"
                )
            previous = close
        return ordered

    def timeframes(self, symbol: str) -> tuple[str, ...]:
        return tuple(self._series.get(symbol.upper(), {}))

    # ── The cut ─────────────────────────────────────────────────────────────

    def closed_candles(
        self, symbol: str, timeframe: str, *, limit: int | None = None,
        as_of_ms: int | None = None,
    ) -> tuple[Any, ...]:
        """Every candle closed at or before the clock. Never one more.

        ``as_of_ms`` is for the higher-timeframe alignment rule, which cuts at
        the *strategy candle's* close rather than at wall-clock now.
        """
        key = symbol.upper()
        try:
            rows = self._series[key][timeframe]
            closes = self._closes[key][timeframe]
        except KeyError:
            raise ReplayDataError(
                f"no historical series for {key} {timeframe}; have "
                f"{sorted(self._series.get(key, {}))}"
            ) from None

        cutoff = self.clock.now_ms if as_of_ms is None else int(as_of_ms)
        # bisect_right on close times: everything strictly before the boundary,
        # plus a candle closing exactly at it (it has closed).
        end = bisect_right(closes, cutoff)
        visible = rows[:end]
        if limit is not None and limit > 0:
            visible = visible[-limit:]
        return visible

    def latest_closed_candle(self, symbol: str, timeframe: str) -> Any | None:
        visible = self.closed_candles(symbol, timeframe, limit=1)
        return visible[-1] if visible else None

    def reference_price(self, symbol: str, timeframe: str) -> float | None:
        """The price a decision at "now" may use: the last *closed* close.

        Deliberately not a mid-bar price. In replay there is no such thing as
        the current price of a bar that has not finished, and pretending there
        is would be look-ahead by another name.
        """
        row = self.latest_closed_candle(symbol, timeframe)
        return None if row is None else _close_price(row)

    # ── The production contract ─────────────────────────────────────────────

    def build_snapshot(
        self,
        symbol: str,
        timeframe: str,
        *,
        limit: int = 250,
        higher_timeframe: str | None = None,
        higher_timeframe_limit: int = 250,
    ) -> MarketSnapshot | None:
        """One immutable snapshot, cut at the clock. ``None`` before any candle.

        The higher-timeframe series is cut at the *strategy candle's* close, not
        at the clock, so a 15m decision can never consult an hourly candle that
        closes after it (§13.4).
        """
        candles = self.closed_candles(symbol, timeframe, limit=limit)
        if not candles:
            return None

        strategy_close = _close_time(candles[-1])
        htf_rows: tuple[Any, ...] = ()
        if higher_timeframe:
            htf_rows = self.closed_candles(
                symbol, higher_timeframe,
                limit=higher_timeframe_limit, as_of_ms=strategy_close,
            )

        snapshot = MarketSnapshot.build(
            symbol=symbol.upper(),
            timeframe=timeframe,
            candles=candles,
            source=self.source,
            higher_timeframe=higher_timeframe,
            higher_timeframe_candles=htf_rows,
        )
        object.__setattr__(snapshot, "source_environment", self.source_environment)

        # Belt and braces: the contract has its own alignment check, and a
        # snapshot that fails it must never reach a strategy.
        if not snapshot.htf_is_timestamp_aligned():
            raise ReplayDataError(
                f"{symbol} {timeframe}: higher-timeframe candle closes after the "
                f"strategy candle ({snapshot.higher_timeframe_closed_candle_time} "
                f"> {snapshot.latest_closed_candle_time})"
            )
        return snapshot

    # ── Driving the replay ──────────────────────────────────────────────────

    def evaluation_times(
        self, symbol: str, timeframe: str, *, start_ms: int | None = None,
        end_ms: int | None = None,
    ) -> tuple[int, ...]:
        """Every close time in the window: one decision point per closed candle."""
        key = symbol.upper()
        try:
            closes = self._closes[key][timeframe]
        except KeyError:
            raise ReplayDataError(f"no historical series for {key} {timeframe}") from None
        return tuple(
            c for c in closes
            if (start_ms is None or c >= start_ms) and (end_ms is None or c <= end_ms)
        )

    def step(
        self, symbol: str, timeframe: str, *, start_ms: int | None = None,
        end_ms: int | None = None,
    ) -> Iterator[int]:
        """Advance the clock candle by candle, yielding each decision point."""
        for close_time in self.evaluation_times(
            symbol, timeframe, start_ms=start_ms, end_ms=end_ms
        ):
            self.clock.advance_to(close_time)
            yield close_time


class ReplayMarketClient:
    """A client that answers only from the provider, at the clock.

    The strategy stack asks a *client* for candles and prices. Giving it this
    one means no code path can reach a live exchange during a replay, and every
    answer is already cut at the historical clock.
    """

    is_replay_client = True

    def __init__(self, provider: HistoricalMarketDataProvider, *, default_timeframe: str) -> None:
        self._provider = provider
        self._default_timeframe = default_timeframe

    def klines(self, symbol: str, interval: str | None = None, limit: int = 250, **_: Any) -> list:
        rows = self._provider.closed_candles(
            symbol, interval or self._default_timeframe, limit=limit
        )
        return [list(r) if not isinstance(r, Mapping) else dict(r) for r in rows]

    def historical_klines(self, symbol: str, interval: str | None = None, **kwargs: Any) -> list:
        return self.klines(symbol, interval, limit=int(kwargs.get("limit", 250)))

    def last_price(self, symbol: str) -> float:
        price = self._provider.reference_price(symbol, self._default_timeframe)
        if price is None:
            raise ReplayDataError(f"{symbol}: no closed candle at the replay clock")
        return price

    def get_prices(self, symbols: Sequence[str]) -> dict[str, float]:
        return {s.upper(): self.last_price(s) for s in symbols}

    def mark_price(self, symbol: str) -> dict:
        return {"symbol": symbol.upper(), "markPrice": str(self.last_price(symbol))}

    def server_time(self) -> int:
        return self._provider.clock.now_ms

    def __getattr__(self, name: str) -> Any:
        raise AttributeError(
            f"ReplayMarketClient has no '{name}'. A replay may not reach a live "
            f"exchange; if the strategy stack needs this, it must be served from "
            f"historical data."
        )
