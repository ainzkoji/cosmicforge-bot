"""
MarketDataSnapshotService — fetches OHLCV+ATR from an exchange client
and persists a single event_market_snapshots row.

Design constraints:
  - Works with any client that exposes .klines(symbol, interval, limit) -> list
  - Kline format: [open_time, open, high, low, close, volume, close_time, ...]
    (Binance and the Bybit adapter both emit this layout)
  - Spread / order-book depth are stored as None when unavailable (most kline feeds)
  - Errors are caught and logged; they never raise to the caller
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, List, Optional

from shared_lib.persistence.db import DB
from shared_lib.persistence.market_reactions import insert_snapshot, snapshot_exists
from app.events.reaction_metrics import compute_atr

logger = logging.getLogger(__name__)

# Number of historical candles fetched to compute ATR (14-period ATR needs ≥15)
_ATR_CANDLE_LIMIT = 18


def _parse_klines(raw: List[Any]) -> List[dict]:
    """
    Convert raw kline list to candle dicts.
    Binance / Bybit (via adapter) format:
      [open_time, open, high, low, close, volume, close_time, ...]
    """
    out = []
    for k in raw:
        try:
            out.append({
                "candle_open":  float(k[1]),
                "candle_high":  float(k[2]),
                "candle_low":   float(k[3]),
                "candle_close": float(k[4]),
                "volume":       float(k[5]),
            })
        except (IndexError, TypeError, ValueError):
            continue
    return out


class MarketDataSnapshotService:
    """
    Fetches one market snapshot for (event_id, symbol, window_label) and
    stores it in event_market_snapshots.

    Intended to be called by EventReactionWorker on a schedule.
    """

    def __init__(self, db: DB, exchange_client: Any, exchange_name: str = "binance"):
        self._db = db
        self._client = exchange_client
        self._exchange = exchange_name

    def fetch_and_store(
        self,
        event_id: str,
        symbol: str,
        window_label: str,
        *,
        allow_duplicate: bool = False,
    ) -> bool:
        """
        Fetch the most recent closed 1-minute candle for the symbol and store
        it as a snapshot.  Returns True on success, False on any error.

        allow_duplicate=False (default): skip if a row for this
        (event_id, symbol, window_label) already exists.
        """
        if not allow_duplicate and snapshot_exists(self._db, event_id, symbol, window_label):
            logger.debug(
                "[SnapshotSvc] Snapshot already exists event=%s symbol=%s window=%s — skipping",
                event_id, symbol, window_label,
            )
            return True

        try:
            raw = self._client.klines(symbol, interval="1m", limit=_ATR_CANDLE_LIMIT)
        except Exception as exc:
            logger.warning(
                "[SnapshotSvc] klines fetch failed event=%s symbol=%s: %s",
                event_id, symbol, exc,
            )
            return False

        if not raw or len(raw) < 2:
            logger.warning(
                "[SnapshotSvc] Empty klines event=%s symbol=%s", event_id, symbol
            )
            return False

        candles = _parse_klines(raw)
        if not candles:
            return False

        # Use the second-to-last candle as the closed candle
        # (the last candle may still be open)
        closed = candles[-2] if len(candles) >= 2 else candles[-1]
        price = closed["candle_close"]
        volume = closed["volume"]

        atr = compute_atr(candles[:-1])  # exclude the live candle from ATR

        now_utc = datetime.now(timezone.utc).isoformat()

        try:
            insert_snapshot(
                self._db,
                event_id=event_id,
                symbol=symbol,
                exchange=self._exchange,
                timestamp_utc=now_utc,
                window_label=window_label,
                price=price,
                volume=volume,
                candle_open=closed["candle_open"],
                candle_high=closed["candle_high"],
                candle_low=closed["candle_low"],
                candle_close=closed["candle_close"],
                atr=atr,
                spread=None,       # not available from kline feed
                bid_depth=None,
                ask_depth=None,
                source=self._exchange,
            )
        except Exception as exc:
            logger.error(
                "[SnapshotSvc] DB write failed event=%s symbol=%s: %s",
                event_id, symbol, exc,
            )
            return False

        logger.debug(
            "[SnapshotSvc] Stored snapshot event=%s symbol=%s window=%s price=%.4f",
            event_id, symbol, window_label, price,
        )
        return True
