from __future__ import annotations

import hashlib
import uuid
from dataclasses import dataclass, field
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
    """One immutable market view, shared by every component of one decision.

    Every strategy component in a single decision must see the same latest
    candle. ``market_snapshot_id`` is the correlation key that ties the
    resulting TradingOpportunity, entry-quality decision and fills back to the
    exact market view that produced them.
    """

    symbol: str
    timeframe: str
    candles: tuple[Any, ...]
    latest_closed_candle_time: int
    reference_price: float
    fetched_at: str
    source: str
    higher_timeframe: str | None = None
    higher_timeframe_candles: tuple[Any, ...] = ()
    market_snapshot_id: str = field(default_factory=lambda: f"ms_{uuid.uuid4().hex[:16]}")
    source_environment: str | None = None
    latest_closed_candle_open_time: int | None = None
    higher_timeframe_closed_candle_time: int | None = None
    #: Further closed-candle series, keyed by timeframe, for experts that read
    #: their own timeframe (vwap_reversion runs on 5m). Every series is cut at
    #: the strategy candle's close in :meth:`build`, so none can carry a candle
    #: the decision could not have seen.
    auxiliary_candles: dict[str, tuple[Any, ...]] = field(default_factory=dict)

    @property
    def data_hash(self) -> str:
        """Deterministic fingerprint of the candle data this snapshot pins."""
        parts: tuple[Any, ...] = (
            self.symbol, self.timeframe, self.candles, self.higher_timeframe_candles,
        )
        if self.auxiliary_candles:
            # Only when present, so a snapshot without auxiliary series keeps
            # exactly the fingerprint it had before they existed.
            parts = parts + (tuple(sorted(self.auxiliary_candles.items())),)
        payload = repr(parts)
        return hashlib.sha256(payload.encode()).hexdigest()[:32]

    def htf_is_timestamp_aligned(self) -> bool:
        """True when the HTF candle closed at or before the strategy candle.

        A 15m decision at 14:45 may not consult a 1h candle that closes at
        15:00 -- that candle does not exist yet at decision time. Returning
        False here means the snapshot carries look-ahead data and must not be
        used for an entry decision.
        """
        if not self.higher_timeframe_candles:
            return True
        htf_close = self.higher_timeframe_closed_candle_time
        if htf_close is None:
            htf_close = _value(self.higher_timeframe_candles[-1], 6, "closeTime", "close_time")
        if htf_close is None:
            return False
        return int(htf_close) <= int(self.latest_closed_candle_time)

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
        source_environment: str | None = None,
        auxiliary_candles: dict[str, Sequence[Any]] | None = None,
    ) -> "MarketSnapshot":
        primary = closed_candles(candles)
        if not primary:
            raise ValueError("STALE_MARKET_DATA:no_closed_candle")
        last = primary[-1]
        close_time = _value(last, 6, "closeTime", "close_time", "close_timestamp")
        if close_time is None:
            close_time = _value(last, 0, "openTime", "open_time", "timestamp")
        # Auxiliary series are cut at the strategy candle's close: a 5m candle
        # closing after the 15m decision candle did not exist at decision time.
        # A row whose close cannot be read is dropped rather than trusted.
        auxiliary: dict[str, tuple[Any, ...]] = {}
        for aux_timeframe, aux_rows in (auxiliary_candles or {}).items():
            series = []
            for row in closed_candles(aux_rows or ()):
                aux_close = _value(row, 6, "closeTime", "close_time", "close_timestamp")
                if aux_close is not None and int(aux_close) <= int(close_time):
                    series.append(row)
            if series:
                auxiliary[str(aux_timeframe)] = tuple(series)
        htf = closed_candles(higher_timeframe_candles or ())
        htf_close_time = None
        if htf:
            htf_close_time = _value(htf[-1], 6, "closeTime", "close_time", "close_timestamp")
            htf_close_time = int(htf_close_time) if htf_close_time is not None else None
        open_time = _value(last, 0, "openTime", "open_time", "timestamp")
        return cls(
            symbol=symbol.upper(),
            timeframe=timeframe,
            candles=primary,
            latest_closed_candle_time=int(close_time),
            latest_closed_candle_open_time=int(open_time) if open_time is not None else None,
            higher_timeframe_closed_candle_time=htf_close_time,
            source_environment=source_environment,
            reference_price=float(_value(last, 4, "close") or 0.0),
            fetched_at=datetime.now(timezone.utc).isoformat(),
            source=source,
            higher_timeframe=higher_timeframe,
            higher_timeframe_candles=htf,
            auxiliary_candles=auxiliary,
        )


class SnapshotMarketClient:
    """Read-through client that pins strategy candle reads to one immutable snapshot.

    The contract is keyword-only (``klines(symbol=..., interval=..., limit=...)``)
    and serves exactly the series the snapshot pins: the strategy timeframe, the
    higher timeframe, and any auxiliary timeframes. Anything else raises
    ``snapshot_timeframe_unavailable`` -- which an expert must surface as an
    ERROR, never fold into a HOLD.
    """

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
        auxiliary = getattr(self._snapshot, "auxiliary_candles", None) or {}
        if interval in auxiliary:
            return list(auxiliary[interval][-limit:])
        raise ValueError(f"snapshot_timeframe_unavailable:{interval}")

    def __getattr__(self, name: str) -> Any:
        return getattr(self._delegate, name)


def last_evaluated_candle(db: Any, *, bot_instance_id: str, symbol: str, timeframe: str) -> int | None:
    """The persisted last-evaluated candle, or None if this pair is new.

    Used to rehydrate the runtime watchdog after a restart so an up-to-date bot
    is not mistaken for a stalled one.
    """
    try:
        with db.connect() as conn:
            row = conn.execute(
                """SELECT last_closed_candle_time FROM bot_candle_evaluations
                   WHERE bot_instance_id=? AND symbol=? AND timeframe=?""",
                (bot_instance_id, symbol.upper(), timeframe),
            ).fetchone()
        return int(row[0]) if row else None
    except Exception:
        return None


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
