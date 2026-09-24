"""End-to-end: real app.runner.market_snapshot.MarketSnapshot -> MarketState,
proving the integration adapter actually reuses the existing causal
machinery rather than re-deriving it (Section 9.1, 9.14)."""
from __future__ import annotations

from app.runner.market_snapshot import MarketSnapshot
from app.trading_intelligence.integration.snapshot_adapter import evaluate_market_state
from app.trading_intelligence.market_state.engine import reset_shared_market_intelligence_service

from conftest import make_binance_klines


def _build_snapshot(*, with_htf=False, htf_misaligned=False):
    primary = make_binance_klines(200, seed=21)  # 200 x 15m = 180,000,000ms span
    htf_rows = None
    if with_htf:
        # 35 x 1h = 126,000,000ms span (>=30 bars for the HTF trend
        # calculator's own MIN_HISTORY), starting at the same time as
        # primary, so its latest close falls safely inside the primary's
        # span (aligned).
        htf_rows = make_binance_klines(35, seed=22, interval_ms=3_600_000)
        if htf_misaligned:
            # Force the HTF candle to close after the primary decision candle.
            htf_rows[-1][6] = primary[-1][6] + 10_000_000
    return MarketSnapshot.build(
        symbol="btcusdt",
        timeframe="15m",
        candles=primary,
        source="TestClient",
        higher_timeframe="1h" if with_htf else None,
        higher_timeframe_candles=htf_rows,
    )


def test_evaluate_market_state_reuses_snapshot_identity():
    reset_shared_market_intelligence_service()
    snapshot = _build_snapshot()
    ms = evaluate_market_state(snapshot, venue="binance", source="TestClient", use_cache=False)

    assert ms.snapshot_id == snapshot.market_snapshot_id
    assert ms.data_hash == snapshot.data_hash
    assert ms.latest_closed_candle_time == snapshot.latest_closed_candle_time
    assert ms.instrument_key.venue_symbol == "BTCUSDT"


def test_evaluate_market_state_marks_htf_available_when_aligned():
    reset_shared_market_intelligence_service()
    snapshot = _build_snapshot(with_htf=True, htf_misaligned=False)
    assert snapshot.htf_is_timestamp_aligned() is True
    ms = evaluate_market_state(snapshot, venue="binance", source="TestClient", use_cache=False)
    assert ms.higher_timeframe_state.available is True


def test_evaluate_market_state_marks_htf_unavailable_when_misaligned():
    reset_shared_market_intelligence_service()
    snapshot = _build_snapshot(with_htf=True, htf_misaligned=True)
    assert snapshot.htf_is_timestamp_aligned() is False
    ms = evaluate_market_state(snapshot, venue="binance", source="TestClient", use_cache=False)
    assert ms.higher_timeframe_state.available is False
    from app.trading_intelligence.contracts.data_quality import ReasonCode

    assert ReasonCode.HTF_MISALIGNED.value in ms.higher_timeframe_state.reason_codes


def test_evaluate_market_state_uses_shared_cache_between_calls():
    reset_shared_market_intelligence_service()
    snapshot = _build_snapshot()
    ms1 = evaluate_market_state(snapshot, venue="binance", source="TestClient")
    ms2 = evaluate_market_state(snapshot, venue="binance", source="TestClient")
    assert ms1 is ms2
    reset_shared_market_intelligence_service()


def test_base_quote_metadata_improves_identity_over_fallback():
    reset_shared_market_intelligence_service()
    snapshot = _build_snapshot()
    ms_fallback = evaluate_market_state(snapshot, venue="binance", source="TestClient", use_cache=False)
    ms_with_metadata = evaluate_market_state(
        snapshot, venue="binance", source="TestClient", base_asset="BTC", quote_asset="USDT", use_cache=False
    )
    assert ms_fallback.instrument_key.canonical_symbol == ms_with_metadata.instrument_key.canonical_symbol == "BTC/USDT:PERP"
