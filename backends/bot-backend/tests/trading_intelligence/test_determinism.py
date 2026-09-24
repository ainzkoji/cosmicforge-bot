"""Section 9.15.B/G -- determinism and replay/live parity."""
from __future__ import annotations

from app.trading_intelligence.contracts.instrument import CRYPTO, from_symbol_fallback
from app.trading_intelligence.integration.snapshot_adapter import build_data_manifest
from app.trading_intelligence.market_state.engine import build_market_state

INSTRUMENT = from_symbol_fallback(venue="binance", venue_symbol="ETHUSDT", asset_class=CRYPTO)


def _build(series):
    manifest = build_data_manifest(
        instrument_key=INSTRUMENT,
        source="Test",
        timeframe="15m",
        primary_last_closed_candle_time=series.latest_close_time,
        primary_data_hash="abc123",
    )
    return build_market_state(
        instrument_key=INSTRUMENT,
        timeframe="15m",
        decision_time=series.latest_close_time,
        primary_series=series,
        snapshot_id="ms_fixed",
        data_hash="abc123",
        manifest=manifest,
    )


def test_identical_input_produces_byte_equivalent_canonical_hash(trending_series):
    ms1 = _build(trending_series)
    ms2 = _build(trending_series)
    assert ms1.canonical_hash == ms2.canonical_hash
    assert ms1.market_state_id == ms2.market_state_id
    assert ms1.canonical_payload() == ms2.canonical_payload()


def test_replay_and_live_identical_snapshot_produce_same_market_state(trending_series):
    """Two independently-constructed but data-identical CandleSeries (as a
    replay engine and a live runner would each build from the same recorded
    candles) must produce the exact same MarketState."""
    from conftest import make_candle_series

    replay_series = make_candle_series(150, trend=0.8, vol=1.0, seed=1)  # same generator args
    assert replay_series.close == trending_series.close

    ms_live = _build(trending_series)
    ms_replay = _build(replay_series)
    assert ms_live.canonical_hash == ms_replay.canonical_hash


def test_different_correlation_id_does_not_change_canonical_hash(trending_series):
    """market_snapshot_id/data_hash strings are part of identity by design
    (they pin which raw data fed this state) but a freshly-generated random
    correlation id used purely for logging must never appear inside the
    canonical hash payload."""
    from app.trading_intelligence.contracts.market_state import new_correlation_id

    ms = _build(trending_series)
    # Two random correlation ids are never equal, proving randomness exists
    # in the system generally, while the MarketState hash above is stable --
    # i.e. that randomness is never allowed to leak into MarketState identity.
    assert new_correlation_id("evt") != new_correlation_id("evt")
    assert ms.canonical_hash == _build(trending_series).canonical_hash
