"""Section 9.15.I -- shared cache: same key reuses computation; a change in
candle, venue, manifest, schema or engine version must never collide."""
from __future__ import annotations

from app.trading_intelligence.contracts.data_quality import SharedStateCacheKey
from app.trading_intelligence.contracts.instrument import CRYPTO, from_symbol_fallback
from app.trading_intelligence.integration.snapshot_adapter import build_data_manifest
from app.trading_intelligence.market_state.engine import (
    SharedMarketIntelligenceService,
    build_market_state,
)

INSTRUMENT = from_symbol_fallback(venue="binance", venue_symbol="BTCUSDT", asset_class=CRYPTO)


def _key(**overrides):
    base = dict(
        source_venue_or_provider="binance",
        canonical_instrument_id="BTC/USDT:PERP",
        timeframe="15m",
        latest_closed_candle_time=1_700_000_000_000,
        market_state_schema_version="1.0.0",
        data_manifest_hash="manifest_hash_a",
    )
    base.update(overrides)
    return SharedStateCacheKey(**base)


def test_same_key_hits_cache_second_time():
    service = SharedMarketIntelligenceService()
    calls = []

    def builder():
        calls.append(1)
        return object()

    key = _key()
    r1 = service.get_or_build(key, builder)
    r2 = service.get_or_build(key, builder)
    assert r1 is r2
    assert len(calls) == 1
    assert service.hits == 1
    assert service.misses == 1


def test_different_candle_time_does_not_collide():
    service = SharedMarketIntelligenceService()
    key_a = _key(latest_closed_candle_time=1_700_000_000_000)
    key_b = _key(latest_closed_candle_time=1_700_000_900_000)
    a = service.get_or_build(key_a, lambda: "A")
    b = service.get_or_build(key_b, lambda: "B")
    assert a != b
    assert service.size() == 2


def test_different_venue_does_not_collide():
    service = SharedMarketIntelligenceService()
    key_a = _key(source_venue_or_provider="binance")
    key_b = _key(source_venue_or_provider="bybit")
    a = service.get_or_build(key_a, lambda: "A")
    b = service.get_or_build(key_b, lambda: "B")
    assert a != b
    assert service.size() == 2


def test_different_manifest_hash_does_not_collide():
    service = SharedMarketIntelligenceService()
    key_a = _key(data_manifest_hash="hash_a")
    key_b = _key(data_manifest_hash="hash_b")
    a = service.get_or_build(key_a, lambda: "A")
    b = service.get_or_build(key_b, lambda: "B")
    assert a != b
    assert service.size() == 2


def test_different_schema_version_does_not_collide():
    service = SharedMarketIntelligenceService()
    key_a = _key(market_state_schema_version="1.0.0")
    key_b = _key(market_state_schema_version="1.1.0")
    a = service.get_or_build(key_a, lambda: "A")
    b = service.get_or_build(key_b, lambda: "B")
    assert a != b
    assert service.size() == 2


def test_manifest_hash_changes_when_engine_relevant_input_changes(trending_series):
    manifest_a = build_data_manifest(
        instrument_key=INSTRUMENT, source="Test", timeframe="15m",
        primary_last_closed_candle_time=trending_series.latest_close_time, primary_data_hash="hash_a",
    )
    manifest_b = build_data_manifest(
        instrument_key=INSTRUMENT, source="Test", timeframe="15m",
        primary_last_closed_candle_time=trending_series.latest_close_time, primary_data_hash="hash_b",
    )
    assert manifest_a.manifest_hash != manifest_b.manifest_hash


def test_bounded_cache_evicts_oldest():
    service = SharedMarketIntelligenceService(max_entries=2)
    service.get_or_build(_key(data_manifest_hash="a"), lambda: "A")
    service.get_or_build(_key(data_manifest_hash="b"), lambda: "B")
    service.get_or_build(_key(data_manifest_hash="c"), lambda: "C")
    assert service.size() == 2
