"""Section 9.15.H -- shared MarketState must not depend on user_id, bot_id,
capital or positions. Enforced structurally (no such parameter exists to
pass) and behaviorally (identical market input -> identical output
regardless of any external tenant context)."""
from __future__ import annotations

import dataclasses
import inspect

from app.trading_intelligence.contracts.instrument import CRYPTO, from_symbol_fallback
from app.trading_intelligence.integration.snapshot_adapter import build_data_manifest, evaluate_market_state
from app.trading_intelligence.market_state.engine import (
    SharedMarketIntelligenceService,
    build_market_state,
    reset_shared_market_intelligence_service,
)

INSTRUMENT = from_symbol_fallback(venue="binance", venue_symbol="BTCUSDT", asset_class=CRYPTO)

_FORBIDDEN_SUBSTRINGS = ("user_id", "bot_id", "bot_instance_id", "capital", "position", "credential", "risk_level", "allocation")


def test_build_market_state_signature_has_no_tenant_parameters():
    sig = inspect.signature(build_market_state)
    for name in sig.parameters:
        lowered = name.lower()
        for forbidden in _FORBIDDEN_SUBSTRINGS:
            assert forbidden not in lowered, f"build_market_state accepts tenant-shaped parameter: {name}"


def test_shared_service_signature_has_no_tenant_parameters():
    sig = inspect.signature(SharedMarketIntelligenceService.get_or_build)
    for name in sig.parameters:
        lowered = name.lower()
        for forbidden in _FORBIDDEN_SUBSTRINGS:
            assert forbidden not in lowered, f"get_or_build accepts tenant-shaped parameter: {name}"


def test_evaluate_market_state_signature_has_no_tenant_parameters():
    sig = inspect.signature(evaluate_market_state)
    for name in sig.parameters:
        lowered = name.lower()
        for forbidden in _FORBIDDEN_SUBSTRINGS:
            assert forbidden not in lowered, f"evaluate_market_state accepts tenant-shaped parameter: {name}"


def test_market_state_dataclass_fields_carry_no_tenant_identity():
    from app.trading_intelligence.contracts.market_state import MarketState

    field_names = {f.name.lower() for f in dataclasses.fields(MarketState)}
    for forbidden in _FORBIDDEN_SUBSTRINGS:
        assert not any(forbidden in name for name in field_names), f"MarketState carries a tenant-shaped field matching {forbidden!r}"


def test_identical_market_input_ignores_any_tenant_context(trending_series):
    """Two 'bots' evaluating the exact same instrument/timeframe/candle at
    the same causal boundary must get the identical MarketState -- there is
    no tenant-scoped branch anywhere in the call path that could make them
    diverge."""
    manifest = build_data_manifest(
        instrument_key=INSTRUMENT, source="Test", timeframe="15m",
        primary_last_closed_candle_time=trending_series.latest_close_time, primary_data_hash="h",
    )

    def build_for(_pretend_tenant_label: str):
        # The tenant label is deliberately never passed into build_market_state.
        return build_market_state(
            instrument_key=INSTRUMENT, timeframe="15m", decision_time=trending_series.latest_close_time,
            primary_series=trending_series, snapshot_id="s", data_hash="h", manifest=manifest,
        )

    ms_bot_a = build_for("bot_alpha_user_1")
    ms_bot_b = build_for("bot_beta_user_2")
    assert ms_bot_a.canonical_hash == ms_bot_b.canonical_hash


def test_service_cache_is_shared_across_simulated_callers(trending_series):
    reset_shared_market_intelligence_service()
    from app.trading_intelligence.contracts.data_quality import SharedStateCacheKey
    from app.trading_intelligence.market_state.engine import get_shared_market_intelligence_service

    manifest = build_data_manifest(
        instrument_key=INSTRUMENT, source="Test", timeframe="15m",
        primary_last_closed_candle_time=trending_series.latest_close_time, primary_data_hash="h",
    )
    key = SharedStateCacheKey(
        source_venue_or_provider="binance",
        canonical_instrument_id=INSTRUMENT.canonical_symbol,
        timeframe="15m",
        latest_closed_candle_time=trending_series.latest_close_time,
        market_state_schema_version="1.0.0",
        data_manifest_hash=manifest.manifest_hash,
    )

    calls = {"n": 0}

    def builder():
        calls["n"] += 1
        return build_market_state(
            instrument_key=INSTRUMENT, timeframe="15m", decision_time=trending_series.latest_close_time,
            primary_series=trending_series, snapshot_id="s", data_hash="h", manifest=manifest,
        )

    service = get_shared_market_intelligence_service()
    state_for_bot_a = service.get_or_build(key, builder)
    state_for_bot_b = service.get_or_build(key, builder)  # a different "caller"
    assert calls["n"] == 1  # computed once, reused for the second caller
    assert state_for_bot_a is state_for_bot_b
    reset_shared_market_intelligence_service()
