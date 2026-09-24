"""Section 10.8 -- calibration report is structural-only, never claims
validated probability."""
from __future__ import annotations

from app.trading_intelligence.contracts.instrument import CRYPTO, from_symbol_fallback
from app.trading_intelligence.integration.snapshot_adapter import build_data_manifest
from app.trading_intelligence.market_state.engine import build_market_state
from app.trading_intelligence.regime.calibration import build_calibration_report
from app.trading_intelligence.regime.engine import compute_regime_distribution
from app.trading_intelligence.regime.policy import default_policy

from conftest import make_candle_series

INSTRUMENT = from_symbol_fallback(venue="binance", venue_symbol="BTCUSDT", asset_class=CRYPTO)


def _distribution(seed):
    series = make_candle_series(120, trend=0.5, seed=seed)
    manifest = build_data_manifest(
        instrument_key=INSTRUMENT, source="Test", timeframe="15m",
        primary_last_closed_candle_time=series.latest_close_time, primary_data_hash="h",
    )
    ms = build_market_state(
        instrument_key=INSTRUMENT, timeframe="15m", decision_time=series.latest_close_time,
        primary_series=series, snapshot_id="s", data_hash="h", manifest=manifest,
    )
    return compute_regime_distribution(ms, default_policy())


def test_calibration_report_never_claims_calibration():
    history = [_distribution(seed) for seed in range(30, 35)]
    report = build_calibration_report(history)
    assert report.is_calibrated is False
    assert "not validated" in report.note.lower() or "structural" in report.note.lower()
    assert report.sample_count == len(history)
    assert 0.0 <= report.dominant_regime_stability <= 1.0
    assert 0.0 <= report.transition_frequency <= 1.0


def test_calibration_report_handles_empty_and_single_sample():
    assert build_calibration_report([]).sample_count == 0
    single = build_calibration_report([_distribution(40)])
    assert single.sample_count == 1
    assert single.dominant_regime_stability == 1.0
