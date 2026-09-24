"""Section 9.15.D/E -- missing capabilities are NULL + typed reason (never
zero), and no valid MarketState contains an unhandled NaN/Infinity."""
from __future__ import annotations

import math

from app.trading_intelligence.contracts.data_quality import Capability, CapabilityState, ReasonCode
from app.trading_intelligence.contracts.instrument import CRYPTO, from_symbol_fallback
from app.trading_intelligence.integration.snapshot_adapter import build_data_manifest
from app.trading_intelligence.market_state.engine import build_market_state

INSTRUMENT = from_symbol_fallback(venue="binance", venue_symbol="BTCUSDT", asset_class=CRYPTO)


def _build(series):
    manifest = build_data_manifest(
        instrument_key=INSTRUMENT,
        source="Test",
        timeframe="15m",
        primary_last_closed_candle_time=series.latest_close_time,
        primary_data_hash="h",
    )
    return build_market_state(
        instrument_key=INSTRUMENT,
        timeframe="15m",
        decision_time=series.latest_close_time,
        primary_series=series,
        snapshot_id="s",
        data_hash="h",
        manifest=manifest,
    )


def test_missing_book_and_derivatives_are_null_not_zero(trending_series):
    ms = _build(trending_series)

    assert ms.liquidity_state.available is False
    assert ms.liquidity_state.spread_bps is None
    assert ms.liquidity_state.top_book_depth is None

    assert ms.derivatives_state.available is False
    assert ms.derivatives_state.funding_current is None
    assert ms.derivatives_state.open_interest is None

    assert ms.feature_availability.state_of(Capability.TOP_OF_BOOK) == CapabilityState.UNAVAILABLE
    assert ms.feature_availability.state_of(Capability.FUNDING) == CapabilityState.UNAVAILABLE
    assert ms.feature_availability.state_of(Capability.LIQUIDATIONS) == CapabilityState.UNSUPPORTED
    assert ms.feature_availability.reasons[Capability.FUNDING.value] == ReasonCode.FUNDING_UNAVAILABLE.value


def test_missing_taker_data_leaves_participation_fields_null(trending_series):
    ms = _build(trending_series)
    # conftest's make_candle_series/build_candle_series only populates taker
    # buy volume / trade count when every row has that field. The generator
    # in conftest does include them, so use a raw OHLCV-only series here.
    from app.trading_intelligence.contracts.market_state import CandleSeries
    from app.trading_intelligence.market_state.participation import compute_participation_state

    ohlcv_only = CandleSeries(
        open=trending_series.open,
        high=trending_series.high,
        low=trending_series.low,
        close=trending_series.close,
        volume=trending_series.volume,
        close_time=trending_series.close_time,
    )
    participation = compute_participation_state(ohlcv_only)
    assert participation.taker_imbalance is None
    assert participation.trade_intensity is None
    assert ReasonCode.AUXILIARY_DATA_MISSING.value in participation.reason_codes


def test_nonfinite_primary_candles_are_rejected_not_propagated():
    from app.trading_intelligence.contracts.market_state import CandleSeries

    n = 40
    closes = [100.0 + i * 0.1 for i in range(n)]
    closes[-1] = float("nan")
    series = CandleSeries(
        open=tuple(closes),
        high=tuple(c + 1 for c in closes),
        low=tuple(c - 1 for c in closes),
        close=tuple(closes),
        volume=tuple(1000.0 for _ in closes),
        close_time=tuple(1_700_000_000_000 + i * 900_000 for i in range(n)),
    )
    manifest = build_data_manifest(
        instrument_key=INSTRUMENT, source="Test", timeframe="15m",
        primary_last_closed_candle_time=series.latest_close_time, primary_data_hash="h",
    )
    ms = build_market_state(
        instrument_key=INSTRUMENT, timeframe="15m", decision_time=series.latest_close_time,
        primary_series=series, snapshot_id="s", data_hash="h", manifest=manifest,
    )
    assert ms.structure_state.available is False
    assert ReasonCode.NONFINITE_FEATURE.value in ms.structure_state.reason_codes
    assert ms.trend_state.available is False
    assert not ms.is_usable


def test_no_finite_field_is_ever_nan_or_inf(trending_series):
    ms = _build(trending_series)

    def _walk(obj):
        if isinstance(obj, float):
            assert math.isfinite(obj), f"non-finite float leaked into MarketState: {obj!r}"
        elif isinstance(obj, dict):
            for v in obj.values():
                _walk(v)
        elif isinstance(obj, (list, tuple)):
            for v in obj:
                _walk(v)

    _walk(ms.canonical_payload())
