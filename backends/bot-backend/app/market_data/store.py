"""Venue-aware market-data persistence (Phase 4A/4D/4F).

* Candles are keyed by venue + venue symbol + product + timeframe + source:
  Binance / Bybit / BingX ``BTCUSDT`` never collapse into one series.
* Feature observations (funding, open interest, mark/index, basis, spread,
  book summaries, liquidations, turnover) are AVAILABLE with a value or
  UNAVAILABLE with a reason. ``record_feature(..., value=None)`` without a
  reason is refused: missing is never silently stored, and never as 0.
* FX reference quotes are REFERENCE_MARKET_PRICE; execution-venue prices are
  stored as venue feature observations. The two are never mixed.
"""
from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

from app.research.dataset import close_time, ohlcv, open_time

FEATURES = frozenset({
    "funding_rate", "open_interest", "mark_price", "index_price", "basis_bps", "bid", "ask", "spread_bps",
    "book_bid_depth", "book_ask_depth", "liquidations_long", "liquidations_short", "turnover_24h", "volume_24h",
    "last_price", "next_funding_time",
})
REFERENCE_MARKET_PRICE = "REFERENCE_MARKET_PRICE"
EXECUTION_VENUE_PRICE = "EXECUTION_VENUE_PRICE"


class MissingReasonRequired(ValueError):
    """An unavailable observation must say why."""


@dataclass(frozen=True)
class SeriesId:
    venue: str
    venue_symbol: str
    canonical_symbol: str
    asset_class: str
    product_type: str
    source: str
    source_version: Optional[str] = None
    environment: str = "REAL"


class MarketDataStore:
    def __init__(self, db: Any):
        self.db = db

    # -- candles ------------------------------------------------------------------
    def write_candles(self, sid: SeriesId, timeframe: str, rows: Sequence[Any], *, derived_from: Optional[str] = None) -> int:
        from app.market_data.quality import check_series
        report = check_series(rows, symbol=sid.venue_symbol, timeframe=timeframe)
        if not report.is_usable:
            raise ValueError(f"CANDLE_QUALITY_REJECTED:{report.to_dict()}")
        now = int(time.time() * 1000)
        n = 0
        with self.db.connect() as conn:
            for r in rows:
                o, h, low, c, v = ohlcv(r)
                qv = float(r[7]) if len(r) > 7 and r[7] not in (None, "") else None
                trades = int(r[8]) if len(r) > 8 and r[8] not in (None, "") else None
                conn.execute(
                    """INSERT OR IGNORE INTO market_candles (venue, venue_symbol, canonical_symbol, asset_class,
                       product_type, timeframe, open_time, close_time, open, high, low, close, volume, quote_volume,
                       trades, source, source_version, environment, derived_from, ingested_at)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (sid.venue, sid.venue_symbol, sid.canonical_symbol, sid.asset_class, sid.product_type, timeframe,
                     open_time(r), close_time(r), o, h, low, c, v, qv, trades, sid.source, sid.source_version,
                     sid.environment, derived_from, now))
                n += conn.execute("SELECT changes()").fetchone()[0]
        return n

    def read_candles(self, *, venue: str, venue_symbol: str, timeframe: str, start_ms: Optional[int] = None,
                     end_ms: Optional[int] = None, source: Optional[str] = None) -> List[list]:
        """Bars in [start, end] -- never a bar closing after ``end_ms``."""
        sql = ("SELECT open_time, open, high, low, close, volume, close_time, source FROM market_candles "
               "WHERE venue=? AND venue_symbol=? AND timeframe=?")
        args: List[Any] = [venue, venue_symbol, timeframe]
        if start_ms is not None:
            sql += " AND open_time>=?"
            args.append(start_ms)
        if end_ms is not None:
            sql += " AND close_time<=?"
            args.append(end_ms)
        if source:
            sql += " AND source=?"
            args.append(source)
        with self.db.connect() as conn:
            identities = conn.execute("SELECT DISTINCT source, product_type, environment FROM market_candles "
                "WHERE venue=? AND venue_symbol=? AND timeframe=?" + (" AND source=?" if source else ""),
                [venue, venue_symbol, timeframe] + ([source] if source else [])).fetchall()
            if len(identities) > 1:
                raise ValueError("AMBIGUOUS_CANDLE_SERIES_IDENTITY")
            return [list(r) for r in conn.execute(sql + " ORDER BY open_time", args).fetchall()]

    # -- feature observations -------------------------------------------------------
    def record_feature(self, sid: SeriesId, feature: str, observed_at: int, *, value: Optional[float] = None,
                       value_json: Optional[Mapping[str, Any]] = None,
                       unavailable_reason: Optional[str] = None) -> None:
        if feature not in FEATURES:
            raise ValueError(f"unknown feature {feature!r}")
        available = value is not None or value_json is not None
        if not available and not unavailable_reason:
            raise MissingReasonRequired(f"{feature}: an unavailable observation needs a reason")
        with self.db.connect() as conn:
            conn.execute(
                """INSERT OR REPLACE INTO market_feature_observations (venue, venue_symbol, canonical_symbol,
                   asset_class, feature, observed_at, value, value_json, status, unavailable_reason, source,
                   source_version, ingested_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (sid.venue, sid.venue_symbol, sid.canonical_symbol, sid.asset_class, feature, int(observed_at),
                 None if value is None else float(value), json.dumps(value_json) if value_json is not None else None,
                 "AVAILABLE" if available else "UNAVAILABLE", None if available else unavailable_reason,
                 sid.source, sid.source_version, int(time.time() * 1000)))

    def feature_at(self, *, venue: str, venue_symbol: str, feature: str, as_of_ms: int) -> Optional[Dict[str, Any]]:
        """Latest observation at or before ``as_of_ms`` (causal), or None."""
        with self.db.connect() as conn:
            r = conn.execute(
                "SELECT observed_at, value, value_json, status, unavailable_reason, source FROM "
                "market_feature_observations WHERE venue=? AND venue_symbol=? AND feature=? AND observed_at<=? "
                "ORDER BY observed_at DESC LIMIT 1", (venue, venue_symbol, feature, int(as_of_ms))).fetchone()
        return dict(r) if r else None

    # -- FX reference -------------------------------------------------------------
    def write_fx_quotes(self, provider: str, pair: str, base: str, quote: str, timeframe: str,
                        quotes: Iterable[Mapping[str, Any]], *, source_version: Optional[str] = None,
                        conn: Any = None) -> int:
        """INSERT OR IGNORE (idempotent). ``conn``: write inside the caller's transaction (repairs)."""
        if conn is None:
            with self.db.connect() as own:
                return self.write_fx_quotes(provider, pair, base, quote, timeframe, quotes,
                                            source_version=source_version, conn=own)
        now = int(time.time() * 1000)
        n = 0
        from app.market_data.quality import STRUCTURAL_ONLY, check_fx_quotes
        previous = None
        for q in quotes:
            timestamp = int(q["open_time"])
            if previous is not None and timestamp < previous:
                raise ValueError("FX_OUT_OF_ORDER")
            previous = timestamp
            # structural rejection only: a wide (e.g. rollover) spread is stored and flagged by QA, never dropped
            if not check_fx_quotes([q], pair=pair, timeframe=timeframe, max_spread_bps=STRUCTURAL_ONLY).is_usable:
                raise ValueError("FX_QUOTE_QUALITY_REJECTED")
            bid, ask = q.get("bid_close"), q.get("ask_close")
            mid = q.get("mid_close")
            if mid is None and bid is not None and ask is not None:
                mid = (bid + ask) / 2.0
            spread = (ask - bid) if (bid is not None and ask is not None) else None
            conn.execute(
                """INSERT OR IGNORE INTO fx_reference_quotes (provider, pair, base_currency, quote_currency,
                   timeframe, open_time, bid_open, bid_high, bid_low, bid_close, ask_open, ask_high, ask_low,
                   ask_close, mid_close, spread_close, volume, session, price_kind, source_version, ingested_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (provider, pair, base, quote, timeframe, int(q["open_time"]), q.get("bid_open"), q.get("bid_high"),
                 q.get("bid_low"), bid, q.get("ask_open"), q.get("ask_high"), q.get("ask_low"), ask, mid, spread,
                 q.get("volume"), q.get("session"), REFERENCE_MARKET_PRICE, source_version, now))
            n += conn.execute("SELECT changes()").fetchone()[0]
        return n

    def fx_reference_at(self, *, pair: str, timeframe: str, as_of_ms: int, provider: Optional[str] = None
                        ) -> Optional[Dict[str, Any]]:
        """Latest CLOSED reference bar at ``as_of_ms`` (causal)."""
        from app.research.dataset import DERIVABLE, MINUTE_MS

        step = MINUTE_MS * (DERIVABLE.get(timeframe, 1) if timeframe != "1m" else 1)
        sql = ("SELECT * FROM fx_reference_quotes WHERE pair=? AND timeframe=? AND open_time + ? <= ?")
        args: List[Any] = [pair, timeframe, step, int(as_of_ms) + 1]
        if provider:
            sql += " AND provider=?"
            args.append(provider)
        with self.db.connect() as conn:
            if not provider:
                providers = conn.execute("SELECT DISTINCT provider FROM fx_reference_quotes WHERE pair=? AND timeframe=?", (pair, timeframe)).fetchall()
                if len(providers) > 1:
                    raise ValueError("FX_REFERENCE_PROVIDER_REQUIRED")
            r = conn.execute(sql + " ORDER BY open_time DESC LIMIT 1", args).fetchone()
        return dict(r) if r else None


__all__ = ["EXECUTION_VENUE_PRICE", "FEATURES", "MarketDataStore", "MissingReasonRequired",
           "REFERENCE_MARKET_PRICE", "SeriesId"]
