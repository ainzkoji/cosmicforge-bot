from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Sequence


def _value(row: Any, index: int, *keys: str) -> Any:
    if isinstance(row, dict):
        for key in keys:
            if key in row:
                return row[key]
        return None
    return row[index]


def closed_candles(rows: Sequence[Any], now_ms: int | None = None) -> tuple[Any, ...]:
    """Return only candles whose exchange close timestamp has passed."""
    now_ms = now_ms or int(datetime.now(timezone.utc).timestamp() * 1000)
    result = []
    for row in rows or ():
        close_ms = _value(row, 6, "closeTime", "close_time", "close_timestamp")
        if close_ms is None or int(close_ms) <= now_ms:
            result.append(row)
    return tuple(result)


@dataclass(frozen=True)
class MarketSnapshot:
    symbol: str
    timeframe: str
    candles: tuple[Any, ...]
    latest_closed_candle_time: int
    reference_price: float
    fetched_at: str
    source: str
    higher_timeframe: str | None = None
    higher_timeframe_candles: tuple[Any, ...] = ()

    @classmethod
    def build(
        cls,
        *,
        symbol: str,
        timeframe: str,
        candles: Sequence[Any],
        source: str,
        higher_timeframe: str | None = None,
        higher_timeframe_candles: Sequence[Any] | None = None,
    ) -> "MarketSnapshot":
        primary = closed_candles(candles)
        if not primary:
            raise ValueError("STALE_MARKET_DATA:no_closed_candle")
        last = primary[-1]
        close_time = _value(last, 6, "closeTime", "close_time", "close_timestamp")
        if close_time is None:
            close_time = _value(last, 0, "openTime", "open_time", "timestamp")
        return cls(
            symbol=symbol.upper(),
            timeframe=timeframe,
            candles=primary,
            latest_closed_candle_time=int(close_time),
            reference_price=float(_value(last, 4, "close") or 0.0),
            fetched_at=datetime.now(timezone.utc).isoformat(),
            source=source,
            higher_timeframe=higher_timeframe,
            higher_timeframe_candles=closed_candles(higher_timeframe_candles or ()),
        )


class SnapshotMarketClient:
    """Read-through client that pins strategy candle reads to one immutable snapshot."""

    def __init__(self, delegate: Any, snapshot: MarketSnapshot) -> None:
        self._delegate = delegate
        self._snapshot = snapshot

    def klines(self, *, symbol: str, interval: str, limit: int = 500, **kwargs: Any) -> list[Any]:
        if symbol.upper() != self._snapshot.symbol:
            raise ValueError("snapshot_symbol_mismatch")
        if interval == self._snapshot.timeframe:
            return list(self._snapshot.candles[-limit:])
        if interval == self._snapshot.higher_timeframe:
            return list(self._snapshot.higher_timeframe_candles[-limit:])
        raise ValueError(f"snapshot_timeframe_unavailable:{interval}")

    def __getattr__(self, name: str) -> Any:
        return getattr(self._delegate, name)


def claim_candle(db: Any, *, bot_instance_id: str, symbol: str, timeframe: str, close_time: int) -> bool:
    """Atomically claim a closed candle. False means it was already evaluated."""
    with db.connect() as conn:
        row = conn.execute(
            """SELECT last_closed_candle_time FROM bot_candle_evaluations
               WHERE bot_instance_id=? AND symbol=? AND timeframe=?""",
            (bot_instance_id, symbol.upper(), timeframe),
        ).fetchone()
        previous = int(row[0]) if row else -1
        if close_time <= previous:
            return False
        conn.execute(
            """INSERT INTO bot_candle_evaluations
               (bot_instance_id,symbol,timeframe,last_closed_candle_time,updated_at)
               VALUES (?,?,?,?,?)
               ON CONFLICT(bot_instance_id,symbol,timeframe) DO UPDATE SET
                 last_closed_candle_time=excluded.last_closed_candle_time,
                 updated_at=excluded.updated_at""",
            (bot_instance_id, symbol.upper(), timeframe, close_time, datetime.now(timezone.utc).isoformat()),
        )
        return True
