"""Section 9.15.C -- instrument identity: equivalent canonical mapping while
preserving venue distinction."""
from __future__ import annotations

from app.trading_intelligence.contracts.instrument import CRYPTO, PERPETUAL, from_canonical, from_symbol_fallback
from app.universe.identity import canonical_instrument


def test_same_underlying_different_venues_share_canonical_symbol_but_not_identity():
    canonical = canonical_instrument("BTC", "USDT")
    binance_key = from_canonical(canonical=canonical, venue="binance", venue_symbol="BTCUSDT")
    bybit_key = from_canonical(canonical=canonical, venue="bybit", venue_symbol="BTCUSDT")

    assert binance_key.canonical_symbol == bybit_key.canonical_symbol == "BTC/USDT:PERP"
    assert binance_key.venue != bybit_key.venue
    # Cache identity must still distinguish venues -- same economic
    # instrument, different data source.
    assert binance_key.cache_identity != bybit_key.cache_identity


def test_multiplier_prefixed_symbol_maps_to_underlying():
    canonical = canonical_instrument("1000PEPE", "USDT")
    key = from_canonical(canonical=canonical, venue="binance", venue_symbol="1000PEPEUSDT")
    assert key.base_asset == "PEPE"
    assert key.contract_multiplier == 1000.0
    assert key.canonical_symbol == "PEPE/USDT:PERP"


def test_fallback_symbol_parsing_extracts_quote_and_underlying():
    key = from_symbol_fallback(venue="binance", venue_symbol="ETHUSDT", asset_class=CRYPTO)
    assert key.base_asset == "ETH"
    assert key.quote_asset == "USDT"
    assert key.contract_type == PERPETUAL


def test_instrument_key_rejects_unknown_asset_class():
    import pytest

    with pytest.raises(ValueError):
        from app.trading_intelligence.contracts.instrument import InstrumentKey

        InstrumentKey(
            asset_class="NOT_A_REAL_CLASS",
            base_asset="BTC",
            quote_asset="USDT",
            canonical_symbol="BTC/USDT:PERP",
            venue="binance",
            venue_symbol="BTCUSDT",
        )
