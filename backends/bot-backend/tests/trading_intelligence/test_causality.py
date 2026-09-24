"""Section 9.15.A/F -- no-lookahead and candle-boundary tests."""
from __future__ import annotations

from app.trading_intelligence.contracts.data_quality import ReasonCode
from app.trading_intelligence.contracts.instrument import CRYPTO, from_symbol_fallback
from app.trading_intelligence.integration.snapshot_adapter import build_candle_series, build_data_manifest
from app.trading_intelligence.market_state.derivatives import DerivativesSnapshotInput, compute_derivatives_state
from app.trading_intelligence.market_state.engine import build_market_state
from app.trading_intelligence.market_state.liquidity import BookSnapshotInput, compute_liquidity_state
from app.trading_intelligence.market_state.multi_timeframe import compute_higher_timeframe_state

from conftest import make_candle_series

INSTRUMENT = from_symbol_fallback(venue="binance", venue_symbol="BTCUSDT", asset_class=CRYPTO)


def _manifest_for(series, decision_time):
    return build_data_manifest(
        instrument_key=INSTRUMENT,
        source="Test",
        timeframe="15m",
        primary_last_closed_candle_time=decision_time,
        primary_data_hash="hash",
    )


def test_primary_exact_close_boundary_is_valid(trending_series):
    decision_time = trending_series.latest_close_time
    ms = build_market_state(
        instrument_key=INSTRUMENT,
        timeframe="15m",
        decision_time=decision_time,
        primary_series=trending_series,
        snapshot_id="s1",
        data_hash="hash",
        manifest=_manifest_for(trending_series, decision_time),
    )
    assert ReasonCode.OUT_OF_CAUSAL_BOUNDARY.value not in ms.reason_codes


def test_primary_before_close_boundary_is_rejected(trending_series):
    # decision_time earlier than the last candle's close -- that candle did
    # not exist yet at decision time.
    decision_time = trending_series.latest_close_time - 1
    ms = build_market_state(
        instrument_key=INSTRUMENT,
        timeframe="15m",
        decision_time=decision_time,
        primary_series=trending_series,
        snapshot_id="s1",
        data_hash="hash",
        manifest=_manifest_for(trending_series, decision_time),
    )
    assert ReasonCode.OUT_OF_CAUSAL_BOUNDARY.value in ms.reason_codes
    assert not ms.is_usable


def test_new_closed_candle_advances_boundary(trending_series):
    from conftest import make_binance_klines
    from app.trading_intelligence.integration.snapshot_adapter import build_candle_series as bcs

    shorter = bcs(make_binance_klines(149, trend=0.8, vol=1.0, seed=1))
    longer = trending_series  # 150 candles, same generator/seed
    assert longer.latest_close_time > shorter.latest_close_time
    assert longer.close[: len(shorter)] == shorter.close


def test_htf_misaligned_is_marked_unavailable():
    htf_series = make_candle_series(60, seed=9)
    result = compute_higher_timeframe_state(
        htf_series,
        decision_time=htf_series.latest_close_time - 1,
        htf_causally_aligned=False,  # simulates MarketSnapshot.htf_is_timestamp_aligned() == False
        local_direction="UP",
    )
    assert result.available is False
    assert ReasonCode.HTF_MISALIGNED.value in result.reason_codes


def test_htf_future_close_time_rejected_even_if_flagged_aligned():
    htf_series = make_candle_series(60, seed=9)
    result = compute_higher_timeframe_state(
        htf_series,
        decision_time=htf_series.latest_close_time - 1,
        htf_causally_aligned=True,  # caller mistake: claims aligned anyway
        local_direction="UP",
    )
    assert result.available is False
    assert ReasonCode.OUT_OF_CAUSAL_BOUNDARY.value in result.reason_codes


def test_auxiliary_candle_after_primary_close_is_dropped_by_market_snapshot():
    from app.runner.market_snapshot import MarketSnapshot
    from conftest import make_binance_klines

    primary = make_binance_klines(60, seed=1)
    primary_close = primary[-1][6]
    aux = make_binance_klines(60, seed=2, interval_ms=300_000)
    # Force one auxiliary row to close after the primary decision boundary.
    aux[-1][6] = primary_close + 1_000_000

    snapshot = MarketSnapshot.build(
        symbol="BTCUSDT",
        timeframe="15m",
        candles=primary,
        source="Test",
        auxiliary_candles={"5m": aux},
    )
    aux_series = build_candle_series(snapshot.auxiliary_candles["5m"])
    assert aux_series.latest_close_time <= snapshot.latest_closed_candle_time


def test_derivatives_predicted_funding_published_after_decision_dropped():
    decision_time = 1_700_000_000_000
    derivatives = DerivativesSnapshotInput(
        as_of=decision_time,
        funding_current=0.0001,
        funding_predicted=0.0005,
        funding_predicted_as_of=decision_time + 1,  # published after decision_time
    )
    state = compute_derivatives_state(derivatives, decision_time=decision_time)
    assert state.funding_predicted is None
    assert ReasonCode.OUT_OF_CAUSAL_BOUNDARY.value in state.reason_codes
    # funding_current, published as_of decision_time, is still usable.
    assert state.funding_current == 0.0001


def test_derivatives_snapshot_itself_after_decision_time_rejected():
    decision_time = 1_700_000_000_000
    derivatives = DerivativesSnapshotInput(as_of=decision_time + 1, funding_current=0.0001)
    state = compute_derivatives_state(derivatives, decision_time=decision_time)
    assert state.available is False
    assert ReasonCode.OUT_OF_CAUSAL_BOUNDARY.value in state.reason_codes


def test_book_snapshot_after_decision_time_rejected():
    decision_time = 1_700_000_000_000
    book = BookSnapshotInput(as_of=decision_time + 1, best_bid=100.0, best_ask=100.1)
    state = compute_liquidity_state(book, decision_time=decision_time)
    assert state.available is False
    assert ReasonCode.OUT_OF_CAUSAL_BOUNDARY.value in state.reason_codes


def test_book_snapshot_stale_but_causal_is_degraded_not_invalid():
    decision_time = 1_700_000_000_000
    book = BookSnapshotInput(as_of=decision_time - 60_000, best_bid=100.0, best_ask=100.1)
    state = compute_liquidity_state(book, decision_time=decision_time)
    assert state.available is True
    assert state.stale_book is True
    assert ReasonCode.TOP_BOOK_STALE.value in state.reason_codes
    # Staleness on an optional feature must never be a critical fault.
    assert ReasonCode.PRIMARY_CANDLES_STALE.value not in state.reason_codes


def test_historical_replay_timestamp_is_pinned_not_wall_clock(trending_series):
    """A decision_time far in the past must produce the exact same result as
    the exact same decision_time computed "live" -- the engine must never
    consult wall-clock time (P1/P2)."""
    decision_time = trending_series.latest_close_time
    manifest = _manifest_for(trending_series, decision_time)
    kwargs = dict(
        instrument_key=INSTRUMENT,
        timeframe="15m",
        decision_time=decision_time,
        primary_series=trending_series,
        snapshot_id="s1",
        data_hash="hash",
        manifest=manifest,
    )
    ms_a = build_market_state(**kwargs)
    ms_b = build_market_state(**kwargs)
    assert ms_a.canonical_hash == ms_b.canonical_hash
