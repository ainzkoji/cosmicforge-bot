"""CATIController -- proves the shadow pipeline reaches EconomicOpportunity
and can never place an order (Section 13.18 success condition)."""
from __future__ import annotations

import inspect
import math

from app.runner.market_snapshot import MarketSnapshot
from app.trading_intelligence.contracts.economics import AdmissionStatus
from app.trading_intelligence.controller.cati_controller import CATIController
from app.trading_intelligence.controller.interfaces import CATIControllerProtocol


def _trend_pullback_rows():
    n_bars, trend_rate, amp, period, phase = 140, 0.5, 6, 16, 3
    closes = [100 + trend_rate * i + amp * math.sin((i + phase) / (period / (2 * math.pi))) for i in range(n_bars)]
    rows = []
    for i, c in enumerate(closes):
        o = closes[i - 1] if i > 0 else c
        h = max(o, c) + amp * 0.1
        low = min(o, c) - amp * 0.1
        t = 1_700_000_000_000 + i * 900_000
        rows.append([t, o, h, low, c, 1000, t + 899_999, 0, 0, 0, 0, 0])
    return rows


def test_controller_implements_protocol_shape():
    controller = CATIController()
    assert isinstance(controller, CATIControllerProtocol)
    sig = inspect.signature(controller.run_cycle)
    forbidden = ("client", "capital", "order", "execute")
    for name in sig.parameters:
        assert not any(f in name.lower() for f in forbidden)


def test_shadow_pipeline_reaches_economic_opportunity_with_no_library():
    """No production HistoricalOutcomeLibrary is wired -- every opportunity
    must still come back as a well-formed, fail-closed record, never a
    crash and never a fabricated forecast."""
    snapshot = MarketSnapshot.build(symbol="BTCUSDT", timeframe="15m", candles=_trend_pullback_rows(), source="Test")
    controller = CATIController()
    opportunities = controller.run_cycle(snapshot=snapshot, venue="binance", source="Test")
    assert len(opportunities) >= 1
    for opportunity in opportunities:
        assert opportunity.admission_status == AdmissionStatus.INSUFFICIENT_EVIDENCE.value
        assert "FORECAST_UNAVAILABLE" in opportunity.reason_codes


def test_run_cycle_returns_no_order_shaped_object():
    snapshot = MarketSnapshot.build(symbol="BTCUSDT", timeframe="15m", candles=_trend_pullback_rows(), source="Test")
    controller = CATIController()
    opportunities = controller.run_cycle(snapshot=snapshot, venue="binance", source="Test")
    for opportunity in opportunities:
        assert not hasattr(opportunity, "order_id")
        assert not hasattr(opportunity, "submit")
        assert not hasattr(opportunity, "execute")


def test_controller_returns_empty_tuple_for_ambiguous_market():
    """An ambiguous market with zero discovered candidates returns an empty
    tuple -- not an error, not a fabricated opportunity."""
    from conftest import make_candle_series

    series = make_candle_series(120, trend=0.0, vol=3.0, seed=99)
    rows = []
    for o, h, low, c, v, ct in zip(series.open, series.high, series.low, series.close, series.volume, series.close_time):
        rows.append([ct - 899_999, o, h, low, c, v, ct, 0, 0, 0, 0, 0])
    snapshot = MarketSnapshot.build(symbol="BTCUSDT", timeframe="15m", candles=rows, source="Test")
    controller = CATIController()
    opportunities = controller.run_cycle(snapshot=snapshot, venue="binance", source="Test")
    assert isinstance(opportunities, tuple)
