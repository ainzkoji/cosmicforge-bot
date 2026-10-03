"""Synthetic forward enrollment and frozen-evaluator parity; no market outcomes."""
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import numpy as np
import pytest

from shared_lib.persistence.db import DB
from app.trading_intelligence.integration import residual_prospective as observe


@pytest.fixture
def tracker(tmp_path):
    return observe.Tracker(DB(str(tmp_path / "prospective.db")), now_ms=1000 * observe.H + 20)


def snapshot(symbol="X", score=3., side="LONG"):
    sign = 1 if side == "LONG" else -1
    return {"reason": "SELECTED_TOP1", "eligible_universe": [f"S{i}" for i in range(30)],
            "registered_universe": [symbol], "candidate": {"symbol": symbol, "side": side,
                "score": score, "entry_reference": 100., "risk": 10.,
                "stop": 100. - sign * 10, "target": 100. + sign * 25}}


def enroll(tracker, offset=0, candidate=None):
    t = tracker.state()["first_decision_time"] + offset * observe.H
    identity, _ = tracker.commit_decision(t, candidate or snapshot(), t + 100)
    return identity, t


def row(tracker, identity):
    with tracker.db.connect() as conn:
        return dict(conn.execute("SELECT * FROM cati_residual_decisions WHERE decision_id=?", (identity,)).fetchone())


def test_registry_exact_identity_and_rates():
    family, rates = observe.frozen_definition()
    assert len(family["universe"]) == 134
    assert family["family"] == observe.FAMILY
    assert rates == {"fee": .0004, "half_spread": .0001, "slippage": .0002, "funding_per_8h": .0001}


def test_no_historical_enrollment_late_skips_and_idempotency(tracker):
    t = tracker.state()["first_decision_time"]
    with pytest.raises(ValueError, match="PROSPECTIVE"):
        tracker.commit_decision(t - observe.H, snapshot(), t + 100)
    with pytest.raises(ValueError, match="PROSPECTIVE"):
        tracker.commit_decision(t, snapshot(), t)
    identity, created = tracker.commit_decision(t, snapshot(), t + 100)
    assert created
    assert tracker.commit_decision(t, snapshot("DIFFERENT"), t + 200) == (identity, False)
    assert row(tracker, identity)["selected_symbol"] == "X"
    later = t + observe.H
    missed, _ = tracker.commit_decision(later, snapshot(), later + observe.Q)
    assert row(tracker, missed)["lifecycle"] == "SKIPPED"
    assert json.loads(row(tracker, missed)["risk_state_json"])["reason"] == "MISSED_PROSPECTIVE_BOUNDARY"


def test_two_trackers_restarts_share_watermark_and_one_slot(tracker):
    identity, t = enroll(tracker)
    other = observe.Tracker(tracker.db, now_ms=t + 200)
    assert other.state()["first_decision_time"] == tracker.state()["first_decision_time"]
    overlap, _ = enroll(other, 1)
    assert row(tracker, identity)["portfolio_selected"] == 1
    assert row(tracker, overlap)["portfolio_selected"] == 0
    assert row(tracker, overlap)["lifecycle"] == "PENDING_ENTRY"
    assert json.loads(row(tracker, overlap)["risk_state_json"])["overlap_rejected"]


@pytest.mark.parametrize("side,entry", [("LONG", 89.), ("LONG",125.), ("SHORT",111.), ("SHORT",75.)])
def test_non_executable_gap_never_resizes(tracker, side, entry):
    identity, t = enroll(tracker, candidate=snapshot(side=side))
    tracker.record_entry(identity, t + 1, entry, t + 200)
    saved = row(tracker, identity)
    assert saved["lifecycle"] == "NON_EXECUTABLE_GAP"
    assert saved["stop"] == (90 if side == "LONG" else 110)
    assert saved["outcome"] is None


def test_forward_stop_priority_actual_notional_cost_and_immutable_outcome(tracker):
    identity, t = enroll(tracker)
    tracker.record_entry(identity, t + 1, 101., t + 200)
    bar = [t + 1, 101., 126., 89., 110., 1.]
    tracker.observe_bar(identity, bar, t + observe.Q + 100)
    saved = row(tracker, identity)
    assert saved["outcome"] == "STOP"
    assert saved["gross_R"] == pytest.approx(-1.1)
    # Both actual entry/exit notionals + six complete funding-reserve periods.
    expected = (.0007 * (101 + 90) + .0006 * 101) / 10
    assert saved["cost_R"] == pytest.approx(expected)
    assert saved["net_R"] == pytest.approx(-1.1 - expected)
    assert json.loads(saved["outcome_json"])["ambiguous_stop_priority"]
    tracker.observe_bar(identity, [t + 1, 101., 130., 100., 129., 1.], t + observe.Q + 500)
    assert row(tracker, identity) == saved


@pytest.mark.parametrize("side,high,low,close,expected", [("LONG",126.,99.,110.,2.5), ("SHORT",101.,74.,90.,2.5)])
def test_target_symmetric(tracker, side, high, low, close, expected):
    identity, t = enroll(tracker, candidate=snapshot(side=side))
    tracker.record_entry(identity, t + 1, 100., t + 200)
    tracker.observe_bar(identity, [t + 1, 100., high, low, close, 1.], t + observe.Q + 1)
    assert row(tracker, identity)["outcome"] == "TARGET"
    assert row(tracker, identity)["gross_R"] == expected


def test_future_gap_pre_enrollment_bar_and_entry_time_rejected(tracker):
    identity, t = enroll(tracker)
    with pytest.raises(ValueError, match="ENTRY_TIME"):
        tracker.record_entry(identity, t + 2, 100., t + 200)
    tracker.record_entry(identity, t + 1, 100., t + 200)
    with pytest.raises(ValueError, match="OUTCOME_GAP"):
        tracker.observe_bar(identity, [t + 1,100.,101.,99.,100.,1.], t + 300)
    with pytest.raises(ValueError, match="OUTCOME_GAP"):
        tracker.observe_bar(identity, [t + 1 + observe.Q,100.,101.,99.,100.,1.], t + 2 * observe.Q + 10)
    assert row(tracker, identity)["outcome"] is None


def test_timeout_after_exact_192_closed_bars_and_restart_cursor(tracker):
    identity, t = enroll(tracker)
    tracker.record_entry(identity, t + 1, 100., t + 200)
    for i in range(192):
        current = t + 1 + i * observe.Q
        tracker.observe_bar(identity, [current,100.,101.,99.,100.5,1.], current + observe.Q)
        if i == 10:
            tracker = observe.Tracker(tracker.db, now_ms=current + observe.Q)
        if i < 191:
            assert row(tracker, identity)["outcome"] is None
    saved = row(tracker, identity)
    assert saved["outcome"] == "TIMEOUT"
    assert saved["outcome_time"] == t + 48 * observe.H
    assert saved["gross_R"] == pytest.approx(.05)
    next_id, _ = enroll(tracker, 48)
    assert row(tracker, next_id)["portfolio_selected"] == 1


def synthetic_native():
    rng = np.random.default_rng(772)
    n = 673
    b,e = rng.normal(0,.005,(2,n))
    noise = rng.normal(0,.001,n)
    noise[-24:] += .002
    returns = np.column_stack((b,e,.6*b+.4*e+noise))
    closes = 100 * np.exp(np.cumsum(returns,axis=0))
    decision = n * observe.H - 1
    result = {}
    symbols = ("BTCUSDT","ETHUSDT",*[f"S{i:02}" for i in range(30)])
    for symbol in symbols:
        j = 0 if symbol == "BTCUSDT" else 1 if symbol == "ETHUSDT" else 2
        result[symbol] = [[h * observe.H + q * observe.Q, closes[h,j],closes[h,j]*1.01,
            closes[h,j]*.99,closes[h,j],1.] for h in range(n) for q in range(4)]
    return result,decision,symbols


def test_exact_frozen_evaluator_parity_and_lexical_selection():
    native,t,symbols = synthetic_native()
    result = observe.decision_snapshot(native,t,list(symbols[2:]))
    assert result["candidate"]["symbol"] == "S00"
    assert result["reason"] == "SELECTED_TOP1"
    root = observe.ROOT
    import sys
    sys.path.insert(0,str(root / "scripts"))
    import evaluate_cati_edge003 as frozen
    old_symbols = frozen.CURRENT_SYMBOLS
    frozen.CURRENT_SYMBOLS = symbols
    try:
        hourly = [observe.hourly_window(native[s],t) for s in symbols]
        close = np.column_stack([x[:,3] for x in hourly])
        high = np.column_stack([x[:,1] for x in hourly]);low = np.column_stack([x[:,2] for x in hourly])
        features = frozen.rolling_features(close,high,low,np.arange(673)*observe.H+observe.H-1)
        c = result["candidate"]
        assert c["score"] == pytest.approx(features["score"][-1,2],abs=1e-8)
        assert c["atr14"] == pytest.approx(features["atr"][-1,2])
        assert c["prior24h_swing"] == pytest.approx(features["swing_low"][-1,2])
        price = close[-1,2];atr = features["atr"][-1,2]
        risk = max(price-features["swing_low"][-1,2]+.25*atr,2*atr,.003*price)
        assert c["stop"] == pytest.approx(price-risk)
        assert c["target"] == pytest.approx(price+2.5*risk)
    finally:
        frozen.CURRENT_SYMBOLS = old_symbols


def test_future_partial_gap_factor_rank_and_minimum_assets():
    native,t,symbols = synthetic_native()
    baseline = observe.decision_snapshot(native,t,list(symbols[2:]))
    for rows in native.values():rows.append([t+1,999.,1000.,998.,999.,1.])
    assert observe.decision_snapshot(native,t,list(symbols[2:])) == baseline
    native['S00'].pop(10)
    result = observe.decision_snapshot(native,t,list(symbols[2:]))
    assert result['candidate'] is None and len(result['eligible_universe']) == 29
    native['BTCUSDT'].pop(20)
    assert observe.decision_snapshot(native,t,list(symbols[2:]))['reason'] == 'FACTOR_INPUTS_UNAVAILABLE'
    native['BTCUSDT'] = native['ETHUSDT']
    assert observe.decision_snapshot(native,t,list(symbols[2:]))['reason'] == 'FACTOR_FIT_NOT_FULL_RANK'


def test_schedule_requires_lease_test_guard_and_single_worker(monkeypatch, tracker):
    monkeypatch.delenv('COSMICFORGE_TEST_MODE',raising=False)
    monkeypatch.setattr(observe,'owner_current',lambda db:False)
    pool = Mock();monkeypatch.setattr(observe,'_pool',pool)
    observe.schedule(SimpleNamespace(db=tracker.db));pool.submit.assert_not_called()
    monkeypatch.setattr(observe,'owner_current',lambda db:True)
    monkeypatch.setenv('COSMICFORGE_TEST_MODE','1')
    observe.schedule(SimpleNamespace(db=tracker.db));pool.submit.assert_not_called()
    monkeypatch.delenv('COSMICFORGE_TEST_MODE')
    monkeypatch.setattr(observe,'_future',None);monkeypatch.setattr(observe,'_last',0.)
    observe.schedule(SimpleNamespace(db=tracker.db));pool.submit.assert_called_once()
    observe.schedule(SimpleNamespace(db=tracker.db));pool.submit.assert_called_once()


def test_public_client_get_only_and_entry_read_follows_commit(monkeypatch,tracker):
    monkeypatch.setattr(observe,'owner_current',lambda db:True)
    identity,t = enroll(tracker)
    calls=[]
    def fetch(symbol,start,end,limit=1000):
        saved = row(tracker,identity)
        assert saved['recorded_at'] < start + observe.Q - 1
        calls.append(saved['lifecycle'])
        return [[start,100.,999.,1.,999.,1.,start+observe.Q-1]]
    # No closed outcome bar exists yet, so future H/L/C must have no effect.
    observe.settle_pending(tracker,fetch,t+200)
    assert calls == ['PENDING_ENTRY']
    assert row(tracker,identity)['entry_price'] == 100.
    assert row(tracker,identity)['outcome'] is None
    import inspect
    source=inspect.getsource(observe)
    assert 'session.post' not in source and 'create_order' not in source
    assert 'selected_portfolio_labels' not in source and 'crypto_deep_binance.db' not in source
