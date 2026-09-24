"""Turns the existing, immutable ``MarketSnapshot`` into CATI's causal inputs
(Section 9.1) and is the single entry point shadow integration calls.

No function here fetches anything. Every input CATI ever sees originates
from a ``MarketSnapshot`` that ``app/runner/runner.py`` already built with
its own closed-candle filtering and HTF/auxiliary causal cutoff -- this
module only re-shapes that same data. It never asks the exchange client for
one more candle, never reaches past ``decision_time``, and reuses
``MarketSnapshot.htf_is_timestamp_aligned()`` rather than re-deriving that
check.
"""
from __future__ import annotations

import hashlib
from typing import Any, Mapping, Optional, Sequence

from app.trading_intelligence.contracts.data_quality import DataManifest, SharedStateCacheKey
from app.trading_intelligence.contracts.instrument import (
    CRYPTO, FX, InstrumentKey, from_canonical, from_fx_pair, from_symbol_fallback, instrument_key_for,
)
from app.trading_intelligence.contracts.market_state import CandleSeries, MarketState
from app.trading_intelligence.market_state.derivatives import DerivativesSnapshotInput
from app.trading_intelligence.market_state.engine import build_market_state, get_shared_market_intelligence_service
from app.trading_intelligence.market_state.liquidity import BookSnapshotInput
from app.trading_intelligence.versions import DATA_MANIFEST_SCHEMA_VERSION, MARKET_STATE_SCHEMA_VERSION


def _row_value(row: Any, index: int, *keys: str) -> Any:
    """Mirrors app.runner.market_snapshot._value's row-access convention
    (index-based Binance kline arrays, with a dict-keyed fallback) so CATI
    reads exactly the same row shape the live snapshot already validated."""
    if isinstance(row, dict):
        for key in keys:
            if key in row:
                return row[key]
        return None
    try:
        return row[index]
    except (IndexError, TypeError):
        return None


def build_candle_series(rows: Sequence[Any]) -> CandleSeries:
    opens, highs, lows, closes, volumes, close_times = [], [], [], [], [], []
    taker_buy: list = []
    trades: list = []
    has_taker_buy = True
    has_trades = True
    for row in rows or ():
        opens.append(float(_row_value(row, 1, "open") or 0.0))
        highs.append(float(_row_value(row, 2, "high") or 0.0))
        lows.append(float(_row_value(row, 3, "low") or 0.0))
        closes.append(float(_row_value(row, 4, "close") or 0.0))
        volumes.append(float(_row_value(row, 5, "volume") or 0.0))
        close_time = _row_value(row, 6, "closeTime", "close_time", "close_timestamp")
        close_times.append(int(close_time) if close_time is not None else 0)

        tbv = _row_value(row, 9, "takerBuyBaseVolume", "taker_buy_base_volume")
        if tbv is None:
            has_taker_buy = False
        else:
            taker_buy.append(float(tbv))

        nt = _row_value(row, 8, "numTrades", "num_trades", "trade_count")
        if nt is None:
            has_trades = False
        else:
            trades.append(float(nt))

    return CandleSeries(
        open=tuple(opens),
        high=tuple(highs),
        low=tuple(lows),
        close=tuple(closes),
        volume=tuple(volumes),
        close_time=tuple(close_times),
        taker_buy_volume=tuple(taker_buy) if has_taker_buy and taker_buy else None,
        trade_count=tuple(trades) if has_trades and trades else None,
    )


def build_instrument_key(
    *,
    venue: str,
    venue_symbol: str,
    asset_class: str = CRYPTO,
    base_asset: Optional[str] = None,
    quote_asset: Optional[str] = None,
) -> InstrumentKey:
    """Prefers the venue's real base/quote metadata (via the existing
    ``app.universe.identity.canonical_instrument`` mapping) and only falls
    back to best-effort symbol parsing when that metadata is not supplied."""
    if asset_class == FX:
        # FX has no perpetual/multiplier semantics: identity is the currency pair.
        if base_asset and quote_asset:
            return from_fx_pair(venue=venue, venue_symbol=venue_symbol, base=base_asset, quote=quote_asset)
        return instrument_key_for(venue=venue, venue_symbol=venue_symbol, asset_class=asset_class)
    if base_asset and quote_asset:
        from app.universe.identity import canonical_instrument

        canonical = canonical_instrument(base_asset, quote_asset)
        return from_canonical(canonical=canonical, venue=venue, venue_symbol=venue_symbol, asset_class=asset_class)
    if asset_class != CRYPTO:
        return instrument_key_for(venue=venue, venue_symbol=venue_symbol, asset_class=asset_class)
    return from_symbol_fallback(venue=venue, venue_symbol=venue_symbol, asset_class=asset_class)


def _series_hash(series: Optional[CandleSeries]) -> Optional[str]:
    if series is None or len(series) == 0:
        return None
    payload = repr((series.open, series.high, series.low, series.close, series.volume, series.close_time))
    return hashlib.sha256(payload.encode()).hexdigest()[:32]


def build_data_manifest(
    *,
    instrument_key: InstrumentKey,
    source: str,
    timeframe: str,
    primary_last_closed_candle_time: int,
    primary_data_hash: str,
    htf_timeframe: Optional[str] = None,
    htf_series: Optional[CandleSeries] = None,
    auxiliary_series: Optional[Mapping[str, CandleSeries]] = None,
    derivatives: Optional[DerivativesSnapshotInput] = None,
    book: Optional[BookSnapshotInput] = None,
) -> DataManifest:
    aux_hash = None
    if auxiliary_series:
        parts = tuple(sorted((tf, _series_hash(s)) for tf, s in auxiliary_series.items()))
        aux_hash = hashlib.sha256(repr(parts).encode()).hexdigest()[:32]
    return DataManifest(
        source=source,
        venue=instrument_key.venue,
        canonical_symbol=instrument_key.canonical_symbol,
        primary_timeframe=timeframe,
        primary_last_closed_candle_time=primary_last_closed_candle_time,
        primary_data_hash=primary_data_hash,
        schema_version=DATA_MANIFEST_SCHEMA_VERSION,
        htf_timeframe=htf_timeframe,
        htf_last_closed_candle_time=htf_series.latest_close_time if htf_series else None,
        htf_data_hash=_series_hash(htf_series),
        auxiliary_timeframes=tuple(sorted((auxiliary_series or {}).keys())),
        auxiliary_data_hash=aux_hash,
        derivatives_as_of=derivatives.as_of if derivatives else None,
        book_as_of=book.as_of if book else None,
    )


def evaluate_market_state(
    snapshot: Any,  # app.runner.market_snapshot.MarketSnapshot
    *,
    venue: str,
    source: str,
    asset_class: str = CRYPTO,
    base_asset: Optional[str] = None,
    quote_asset: Optional[str] = None,
    book: Optional[BookSnapshotInput] = None,
    derivatives: Optional[DerivativesSnapshotInput] = None,
    use_cache: bool = True,
) -> MarketState:
    """The single, tenant-free entry point: existing MarketSnapshot -> MarketState.

    Deliberately takes no user_id/bot_instance_id/run_id/cycle_id -- those
    are attached externally, only to evidence, by the caller (Section 7.2 /
    "Evidence / Observability"). Passing one in here would let tenant
    identity leak into the shared cache key and, through it, into
    MarketState identity -- exactly what P3 forbids.
    """
    instrument_key = build_instrument_key(
        venue=venue,
        venue_symbol=snapshot.symbol,
        asset_class=asset_class,
        base_asset=base_asset,
        quote_asset=quote_asset,
    )

    primary_series = build_candle_series(snapshot.candles)
    decision_time = int(snapshot.latest_closed_candle_time)

    htf_series = None
    if snapshot.higher_timeframe_candles:
        htf_series = build_candle_series(snapshot.higher_timeframe_candles)
    htf_causally_aligned = bool(snapshot.htf_is_timestamp_aligned())

    auxiliary_series = None
    raw_auxiliary = getattr(snapshot, "auxiliary_candles", None) or {}
    if raw_auxiliary:
        auxiliary_series = {tf: build_candle_series(rows) for tf, rows in raw_auxiliary.items()}

    manifest = build_data_manifest(
        instrument_key=instrument_key,
        source=source,
        timeframe=snapshot.timeframe,
        primary_last_closed_candle_time=decision_time,
        primary_data_hash=snapshot.data_hash,
        htf_timeframe=snapshot.higher_timeframe,
        htf_series=htf_series,
        auxiliary_series=auxiliary_series,
        derivatives=derivatives,
        book=book,
    )

    def _build() -> MarketState:
        return build_market_state(
            instrument_key=instrument_key,
            timeframe=snapshot.timeframe,
            decision_time=decision_time,
            primary_series=primary_series,
            snapshot_id=snapshot.market_snapshot_id,
            data_hash=snapshot.data_hash,
            manifest=manifest,
            htf_series=htf_series,
            htf_causally_aligned=htf_causally_aligned,
            auxiliary_series=auxiliary_series,
            book=book,
            derivatives=derivatives,
        )

    if not use_cache:
        return _build()

    cache_key = SharedStateCacheKey(
        source_venue_or_provider=venue,
        canonical_instrument_id=instrument_key.canonical_symbol,
        timeframe=snapshot.timeframe,
        latest_closed_candle_time=decision_time,
        market_state_schema_version=MARKET_STATE_SCHEMA_VERSION,
        data_manifest_hash=manifest.manifest_hash,
    )
    service = get_shared_market_intelligence_service()
    return service.get_or_build(cache_key, _build)
