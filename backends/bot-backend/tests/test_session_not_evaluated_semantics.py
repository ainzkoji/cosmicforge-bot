"""NOT_EVALUATED is not FALSE.

Found live: BTC evidence recorded ``session_allowed=false`` when the session gate
was never evaluated -- LOW_VOLATILITY_CHOP had already returned. A skipped gate
became indistinguishable from a closed session, the same family of misreadable
evidence as the 35 false SESSION_BLOCKED rows. A gate that did not run now
records ``session_status=NOT_EVALUATED`` and ``session_allowed=None``; the HTF
bias veto likewise records ``htf_opposed=None`` when it did not run.
"""
from __future__ import annotations

from types import SimpleNamespace

import pytest

from app.runner.market_snapshot import MarketSnapshot
from app.strategy.hold_breakdown import build_hold_breakdown
from app.strategy.master_ensemble import MasterEnsembleStrategy
from app.strategy.regime import MarketRegime

#: 2025-12-31T23:59:59.999Z, aligned to 15m boundaries.
PRIMARY_CLOSE = 1767225599999


class NoNetwork:
    def __getattr__(self, name):
        raise AssertionError(f"reached past the snapshot: {name}")


def snapshot() -> MarketSnapshot:
    rows, price = [], 100.0
    first_open = PRIMARY_CLOSE + 1 - 250 * 900_000
    for i in range(250):
        close = price * (1.0005 if i % 3 else 0.9995)
        rows.append([first_open + i * 900_000, f"{price}", f"{max(price, close) * 1.001}",
                     f"{min(price, close) * 0.999}", f"{close}", "1000",
                     first_open + (i + 1) * 900_000 - 1])
        price = close
    return MarketSnapshot.build(symbol="BTCUSDT", timeframe="15m", candles=rows, source="test")


def _breakdown(meta):
    return build_hold_breakdown(
        symbol="BTCUSDT", raw_strategy_signal="HOLD", raw_confidence=0.0,
        final_action="HOLD", reason="regime_low_vol_chop_suspended", meta=meta,
    )


@pytest.mark.parametrize("gate_result", [
    "pending", "skipped_regime_blocked", "skipped_no_active_strategies", None,
])
def test_a_gate_that_never_ran_has_no_verdict(gate_result):
    evidence = _breakdown({"session_gate_result": gate_result, "session_reason_code": None})
    assert evidence["session_allowed"] is None
    assert evidence["session_status"] == "NOT_EVALUATED"


@pytest.mark.parametrize("gate_result,allowed", [
    ("bypassed", True), ("allowed", True), ("disabled", True), ("blocked", False),
])
def test_a_gate_that_ran_keeps_its_verdict(gate_result, allowed):
    assert _breakdown({"session_gate_result": gate_result})["session_allowed"] is allowed


def test_a_blocked_session_is_still_false_not_none():
    evidence = _breakdown({"session_gate_result": "blocked",
                           "session_reason_code": "SESSION_BLOCKED"})
    assert evidence["session_allowed"] is False
    assert evidence["session_status"] == "SESSION_BLOCKED"


def test_the_ensemble_records_not_evaluated_when_the_regime_returns_first(monkeypatch):
    ensemble = MasterEnsembleStrategy(client=NoNetwork())
    monkeypatch.setattr(ensemble, "_get_classifier", lambda s: SimpleNamespace(
        classify_stable=lambda h, l, c: SimpleNamespace(
            regime=MarketRegime.LOW_VOLATILITY_CHOP, regime_confidence=0.9, adx=10.0,
            atr_percent=0.1, ma_slope=0.0, compression_ratio=0.9, breakout_pressure=0.0,
        )))
    result = ensemble.get_signal("BTCUSDT", market_snapshot=snapshot(), market_type="CRYPTO")

    assert result.meta["session_gate_result"].startswith("skipped")
    assert result.meta["session_allowed"] is None
    assert result.meta["session_reason_code"] == "NOT_EVALUATED"
    assert result.meta["htf_opposed"] is None
