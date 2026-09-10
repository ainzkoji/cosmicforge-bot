"""External signals take the one canonical entry-quality path.

The audited latent violation: a TradingView candidate reached the executor
through the event filter, the PolicyEngine and the execution filter, and never
through AdaptiveEntryThresholdEngine. Now it is normalised into a
TradingOpportunity, evaluated by the one threshold authority, compared by
TradingDecisionEngine, and may execute only with exactly one persisted,
passing threshold decision.
"""
from __future__ import annotations

import inspect
import random
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

import app.decision.external_signal_gate as gate_module
from app.decision.external_signal_gate import (
    REASON_CONTRACT,
    REASON_EVIDENCE_UNAVAILABLE,
    evaluate_external_candidate,
)
from app.strategy.regime import MarketRegime
from app.threshold.contracts import ThresholdStatus
from app.threshold.engine import AdaptiveEntryThresholdEngine
from app.threshold.policy import resolve_threshold_policy
from shared_lib.persistence.db import DB
from shared_lib.persistence.migrations import migrate

SYMBOL = "BTCUSDT"
BOT = "bot_external"
PRIMARY_CLOSE = 1767225599999


def klines(n: int = 120):
    rnd = random.Random(3)
    rows, price = [], 100.0
    first_open = PRIMARY_CLOSE + 1 - n * 900_000
    for i in range(n):
        o = price
        c = o * (1 + rnd.uniform(-0.003, 0.004))
        rows.append([first_open + i * 900_000, f"{o}", f"{max(o, c) * 1.001}",
                     f"{min(o, c) * 0.999}", f"{c}", "1000", first_open + (i + 1) * 900_000 - 1])
        price = c
    return rows


def classifier(regime=MarketRegime.WEAK_TREND):
    return lambda: SimpleNamespace(classify_stable=lambda h, l, c: SimpleNamespace(
        regime=regime, regime_confidence=0.9, atr_percent=0.5,
        compression_ratio=0.2, adx=22.0, ma_slope=0.01, breakout_pressure=0.1,
    ))


def policy(base: float = 0.70, low: float = 0.50):
    return lambda **_: resolve_threshold_policy(
        scopes=[("GLOBAL", {"min_threshold": low})], base_threshold=base,
    )


@pytest.fixture
def db():
    database = DB(":memory:")
    migrate(database)
    return database


def evaluate(db, *, confidence=0.40, regime=MarketRegime.WEAK_TREND, bars=120,
             base=0.70, low=0.50, persist=None):
    return evaluate_external_candidate(
        db=db, symbol=SYMBOL, side="BUY", confidence=confidence, klines=klines(bars),
        timeframe="15m", source="TRADINGVIEW", bot_instance_id=BOT,
        run_id="run_ext", cycle_id="cyc_ext", market_type="CRYPTO",
        engine=AdaptiveEntryThresholdEngine(), policy_resolver=policy(base, low),
        regime_classifier_factory=classifier(regime), persist=persist,
    )


def threshold_rows(db):
    with db.connect() as conn:
        return conn.execute(
            "SELECT threshold_decision_id, opportunity_id, status, passed, symbol, "
            "bot_instance_id FROM threshold_decisions"
        ).fetchall()


def test_a_weak_external_candidate_is_rejected_by_the_threshold_authority(db):
    result = evaluate(db, confidence=0.40)
    assert result.passed is False
    assert result.reason == "ENTRY_CONFIDENCE_BELOW_THRESHOLD"
    assert result.threshold_status == ThresholdStatus.EVALUATED
    rows = threshold_rows(db)
    assert len(rows) == 1, "exactly one threshold decision per external opportunity"
    assert rows[0][0] == result.threshold_decision_id
    assert rows[0][1] == result.opportunity_id
    assert rows[0][3] == 0


def test_a_strong_candidate_passes_with_one_persisted_decision(db):
    result = evaluate(db, confidence=0.95, base=0.20, low=0.10)
    assert result.passed is True
    assert result.persisted is True
    rows = threshold_rows(db)
    assert len(rows) == 1
    assert rows[0][3] == 1


def test_a_hard_blocked_regime_cannot_pass(db):
    result = evaluate(db, confidence=0.99, regime=MarketRegime.LOW_VOLATILITY_CHOP,
                      base=0.20, low=0.10)
    assert result.passed is False
    assert result.threshold_status == ThresholdStatus.HARD_BLOCKED


def test_an_unpersistable_decision_cannot_execute(db):
    def failing_persist(*args, **kwargs):
        raise RuntimeError("disk full")

    result = evaluate(db, confidence=0.95, base=0.20, low=0.10, persist=failing_persist)
    assert result.passed is False
    assert result.reason == REASON_EVIDENCE_UNAVAILABLE
    assert threshold_rows(db) == []


def test_too_little_market_data_violates_the_opportunity_contract(db):
    result = evaluate(db, bars=40)
    assert result.passed is False
    assert result.reason == REASON_CONTRACT
    assert threshold_rows(db) == [], "no opportunity means no threshold decision"


@pytest.mark.parametrize("side,confidence", [("CLOSE", 0.5), ("BUY", None), ("BUY", 1.5)])
def test_malformed_candidates_are_rejected_explicitly(db, side, confidence):
    result = evaluate_external_candidate(
        db=db, symbol=SYMBOL, side=side, confidence=confidence, klines=klines(),
        timeframe="15m", source="TRADINGVIEW", bot_instance_id=BOT,
    )
    assert result.passed is False
    assert result.reason == REASON_CONTRACT


def test_external_state_never_pollutes_the_ensemble_distribution(db):
    engine = AdaptiveEntryThresholdEngine()
    evaluate_external_candidate(
        db=db, symbol=SYMBOL, side="BUY", confidence=0.40, klines=klines(),
        timeframe="15m", source="TRADINGVIEW", bot_instance_id=BOT, engine=engine,
        policy_resolver=policy(), regime_classifier_factory=classifier(),
    )
    keys = list(engine.state_store._states) if hasattr(engine.state_store, "_states") else []
    for key in keys:
        assert key[3].startswith("external_tradingview"), key


# ── The runner cannot reach the executor around the gate ────────────────────


def _bare_runner(db):
    from app.runner.runner import PaperRunner

    runner = object.__new__(PaperRunner)
    runner.run_id = "run_ext"
    runner.cycle_id = "cyc_ext"
    runner.interval = "15m"
    runner.context = None
    runner.db = db
    runner.effective_policy_hash = None
    runner.state = {}
    runner.event_blackout_filter = SimpleNamespace(
        check=lambda symbol=None: SimpleNamespace(is_blocked=False, reason=None, details={}),
    )
    runner.client = SimpleNamespace(klines=lambda **kw: klines())
    runner.policy_engine = MagicMock()
    runner._execute_signal_with_evidence = MagicMock()
    return runner


def test_a_rejected_gate_never_reaches_risk_or_the_executor(db, monkeypatch):
    rejection = gate_module.ExternalGateResult(
        passed=False, reason="ENTRY_CONFIDENCE_BELOW_THRESHOLD",
        threshold_decision_id="thr_x", persisted=True,
    )
    monkeypatch.setattr(gate_module, "evaluate_external_candidate", lambda **kw: rejection)
    runner = _bare_runner(db)

    result = runner.process_external_signal_candidate({
        "source": "TRADINGVIEW", "queue_id": "q1", "symbol": SYMBOL,
        "action": "BUY", "confidence": 0.7,
    })

    assert result["final_status"] == "REJECTED_THRESHOLD_NOT_MET"
    assert result["threshold_decision_id"] == "thr_x"
    runner.policy_engine.evaluate.assert_not_called()
    runner._execute_signal_with_evidence.assert_not_called()


def test_the_gate_precedes_risk_and_execution_in_the_runner():
    from app.runner.runner import PaperRunner

    source = inspect.getsource(PaperRunner.process_external_signal_candidate)
    gate = source.index("evaluate_external_candidate(")
    risk = source.index("self.policy_engine.evaluate(")
    execute = source.index("self._execute_signal_with_evidence(")
    assert gate < risk < execute
    assert "_external_gate.persisted" in source[risk:execute]
