"""
Unit tests for the Market Reaction Layer — metrics and classifier.

Run with:
    cd backends/bot-backend
    python -m pytest tests/test_reaction_metrics.py -v
"""
from __future__ import annotations

import math
import pytest

from app.events.reaction_metrics import (
    compute_atr,
    compute_realized_vol,
    compute_price_moves,
    compute_continuation_or_reversal,
    compute_volatility_expansion,
    compute_volume_metrics,
    compute_confidence_and_quality,
)
from app.events.reaction_classifier import classify_reaction


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _candles(highs, lows, closes):
    candles = []
    for h, lo, c in zip(highs, lows, closes):
        candles.append({"candle_high": h, "candle_low": lo, "candle_close": c})
    return candles


# ---------------------------------------------------------------------------
# T1 — ATR calculation
# ---------------------------------------------------------------------------

class TestComputeATR:
    def test_basic(self):
        candles = _candles(
            highs=[101, 102, 103, 104, 105],
            lows=[99, 100, 101, 102, 103],
            closes=[100, 101, 102, 103, 104],
        )
        atr = compute_atr(candles)
        assert atr is not None
        assert atr > 0

    def test_insufficient_data_returns_none(self):
        candles = _candles(highs=[101], lows=[99], closes=[100])
        assert compute_atr(candles) is None

    def test_empty_returns_none(self):
        assert compute_atr([]) is None


# ---------------------------------------------------------------------------
# T2 — Realized volatility
# ---------------------------------------------------------------------------

class TestRealizedVol:
    def test_constant_prices_returns_zero(self):
        prices = [100.0] * 10
        rv = compute_realized_vol(prices)
        assert rv == 0.0 or rv is None or rv < 0.0001

    def test_volatile_prices_returns_positive(self):
        prices = [100, 110, 90, 115, 85, 120, 80]
        rv = compute_realized_vol(prices)
        assert rv is not None
        assert rv > 0

    def test_insufficient_prices_returns_none(self):
        assert compute_realized_vol([100.0, 101.0]) is None


# ---------------------------------------------------------------------------
# T3 — Price move metrics
# ---------------------------------------------------------------------------

class TestPriceMoves:
    def test_net_move_up(self):
        result = compute_price_moves(
            price_before=100.0,
            prices_during=[101.0],
            price_after_5m=102.0,
            price_after_15m=103.0,
            price_after_30m=104.0,
            price_after_60m=105.0,
        )
        assert result["net_move_pct"] == pytest.approx(5.0)
        assert result["direction_after_event"] == "UP"

    def test_net_move_down(self):
        result = compute_price_moves(
            price_before=100.0,
            prices_during=[99.0],
            price_after_5m=98.0,
            price_after_15m=97.0,
            price_after_30m=96.0,
            price_after_60m=95.0,
        )
        assert result["net_move_pct"] == pytest.approx(-5.0)
        assert result["direction_after_event"] == "DOWN"

    def test_max_move_captured(self):
        result = compute_price_moves(
            price_before=100.0,
            prices_during=[115.0],
            price_after_5m=110.0,
            price_after_15m=105.0,
            price_after_30m=102.0,
            price_after_60m=101.0,
        )
        assert result["max_move_pct"] == pytest.approx(15.0)

    def test_zero_price_before_returns_empty(self):
        result = compute_price_moves(0.0, [], None, None, None, None)
        assert result == {}


# ---------------------------------------------------------------------------
# T4 — Volume spike ratio
# ---------------------------------------------------------------------------

class TestVolumeMetrics:
    def test_spike_ratio(self):
        result = compute_volume_metrics(
            pre_volumes=[100.0, 100.0, 100.0],
            event_volume=300.0,
            vol_spike_threshold=3.0,
        )
        assert result["volume_spike_ratio"] == pytest.approx(3.0)
        assert result["abnormal_volume_score"] == pytest.approx(1.0)

    def test_no_event_volume(self):
        result = compute_volume_metrics(pre_volumes=[100.0, 100.0], event_volume=None)
        assert result["volume_spike_ratio"] is None

    def test_empty_pre_volumes(self):
        result = compute_volume_metrics(pre_volumes=[], event_volume=500.0)
        assert result["volume_spike_ratio"] is None


# ---------------------------------------------------------------------------
# T5 — Confidence score and data quality
# ---------------------------------------------------------------------------

class TestConfidenceAndQuality:
    def test_full_data_complete(self):
        score, quality = compute_confidence_and_quality(
            has_pre60=True, has_event=True, has_post30=True, has_post60=True,
            pre60_count=5, event_count=3, post60_count=5,
        )
        assert score == pytest.approx(1.0)
        assert quality == "COMPLETE"

    def test_missing_pre_event(self):
        score, quality = compute_confidence_and_quality(
            has_pre60=False, has_event=True, has_post30=True, has_post60=True,
            pre60_count=0, event_count=3, post60_count=5,
        )
        assert score < 1.0
        assert quality == "MISSING_PRE_EVENT_DATA"

    def test_exchange_error(self):
        score, quality = compute_confidence_and_quality(
            has_pre60=False, has_event=False, has_post30=False, has_post60=False,
            pre60_count=0, event_count=0, post60_count=0,
            exchange_error=True,
        )
        assert score == 0.0
        assert quality == "EXCHANGE_DATA_ERROR"

    def test_low_confidence(self):
        score, quality = compute_confidence_and_quality(
            has_pre60=False, has_event=False, has_post30=True, has_post60=True,
            pre60_count=0, event_count=0, post60_count=3,
        )
        assert score < 0.5
        assert quality in ("LOW_CONFIDENCE", "MISSING_PRE_EVENT_DATA")


# ---------------------------------------------------------------------------
# T6 — Reaction classifier: WHIPSAW
# ---------------------------------------------------------------------------

class TestClassifyWhipsaw:
    def test_whipsaw_detected(self):
        result = classify_reaction(
            data_quality="COMPLETE",
            volatility_expansion_ratio=3.0,
            volume_spike_ratio=2.0,
            spread_widening_ratio=None,
            net_move_pct=0.1,
            max_move_pct=2.5,
            min_move_pct=-0.2,
            direction_after_event="FLAT",
            continuation_or_reversal="NEUTRAL",
            price_before_event=100.0,
            price_after_5m=100.2,
            price_after_15m=100.3,   # returned near origin
            price_after_30m=100.2,
            price_after_60m=100.1,
            atr_before=1.0,
            atr_after=3.0,
            vol_spike_threshold=2.5,
        )
        assert result == "WHIPSAW"


# ---------------------------------------------------------------------------
# T7 — Reaction classifier: TREND_CONTINUATION
# ---------------------------------------------------------------------------

class TestClassifyTrendContinuation:
    def test_continuation_detected(self):
        result = classify_reaction(
            data_quality="COMPLETE",
            volatility_expansion_ratio=1.5,
            volume_spike_ratio=2.0,
            spread_widening_ratio=None,
            net_move_pct=1.5,
            max_move_pct=2.0,
            min_move_pct=0.3,
            direction_after_event="UP",
            continuation_or_reversal="CONTINUATION",
            price_before_event=100.0,
            price_after_5m=101.0,
            price_after_15m=101.5,
            price_after_30m=101.8,
            price_after_60m=101.5,
            atr_before=1.0,
            atr_after=1.5,
            vol_spike_threshold=2.5,
        )
        assert result == "TREND_CONTINUATION"


# ---------------------------------------------------------------------------
# T8 — Reaction classifier: REVERSAL
# ---------------------------------------------------------------------------

class TestClassifySharpReversal:
    def test_reversal_detected(self):
        result = classify_reaction(
            data_quality="COMPLETE",
            volatility_expansion_ratio=2.0,
            volume_spike_ratio=3.0,
            spread_widening_ratio=None,
            net_move_pct=-1.5,
            max_move_pct=2.0,
            min_move_pct=-2.0,
            direction_after_event="DOWN",
            continuation_or_reversal="REVERSAL",
            price_before_event=100.0,
            price_after_5m=102.0,
            price_after_15m=101.0,
            price_after_30m=98.5,
            price_after_60m=98.5,
            atr_before=1.0,
            atr_after=2.0,
            vol_spike_threshold=2.5,
        )
        assert result == "REVERSAL"


# ---------------------------------------------------------------------------
# T9 — Missing data → PARTIAL or LOW_CONFIDENCE
# ---------------------------------------------------------------------------

class TestMissingDataQuality:
    def test_missing_post_event_data(self):
        score, quality = compute_confidence_and_quality(
            has_pre60=True, has_event=True, has_post30=False, has_post60=False,
            pre60_count=4, event_count=3, post60_count=0,
        )
        assert quality in ("MISSING_POST_EVENT_DATA", "PARTIAL", "LOW_CONFIDENCE")
        assert score < 1.0


# ---------------------------------------------------------------------------
# T10 — NO_REACTION when nothing happened
# ---------------------------------------------------------------------------

class TestNoReaction:
    def test_no_reaction_flat_market(self):
        result = classify_reaction(
            data_quality="COMPLETE",
            volatility_expansion_ratio=1.1,
            volume_spike_ratio=1.1,
            spread_widening_ratio=None,
            net_move_pct=0.05,
            max_move_pct=0.1,
            min_move_pct=-0.05,
            direction_after_event="FLAT",
            continuation_or_reversal="NEUTRAL",
            price_before_event=100.0,
            price_after_5m=100.05,
            price_after_15m=100.06,
            price_after_30m=100.04,
            price_after_60m=100.05,
            atr_before=1.0,
            atr_after=1.1,
        )
        assert result == "NO_REACTION"


# ---------------------------------------------------------------------------
# T11 — EXCHANGE_DATA_ERROR keeps data_quality degraded and classifier safe
# ---------------------------------------------------------------------------

class TestExchangeDataError:
    def test_error_degrades_to_safe_no_reaction_label(self):
        result = classify_reaction(
            data_quality="EXCHANGE_DATA_ERROR",
            volatility_expansion_ratio=None,
            volume_spike_ratio=None,
            spread_widening_ratio=None,
            net_move_pct=None,
            max_move_pct=None,
            min_move_pct=None,
            direction_after_event=None,
            continuation_or_reversal=None,
            price_before_event=None,
            price_after_5m=None,
            price_after_15m=None,
            price_after_30m=None,
            price_after_60m=None,
            atr_before=None,
            atr_after=None,
        )
        assert result == "NO_REACTION"

    def test_confidence_zero_on_error(self):
        score, quality = compute_confidence_and_quality(
            has_pre60=True, has_event=True, has_post30=True, has_post60=True,
            pre60_count=5, event_count=3, post60_count=5,
            exchange_error=True,
        )
        assert score == 0.0
        assert quality == "EXCHANGE_DATA_ERROR"
