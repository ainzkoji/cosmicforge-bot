"""External observations cannot receive independent CATI entry authority.

Retired threshold and execution approvals are replaced by unconditional denial.
No external candidate can reach risk or the broker executor.
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


def policy(base: float = 0.30, low: float = 0.25):
    """Production scale by default (threshold policy 1.1.0)."""
    return lambda **_: resolve_threshold_policy(
        scopes=[("GLOBAL", {"min_threshold": low})], base_threshold=base,
    )


@pytest.fixture
def db():
    database = DB(":memory:")
    migrate(database)
    return database


def evaluate(db, *, confidence=0.20, regime=MarketRegime.WEAK_TREND, bars=120,
             base=0.30, low=0.25, persist=None):
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


@pytest.mark.parametrize("confidence,bars", [(0.20, 120), (0.95, 120), (0.99, 40), (None, 120), (1.5, 120)])
def test_external_candidates_have_no_entry_authority(db, confidence, bars):
    result = evaluate(db, confidence=confidence, bars=bars)
    assert result.passed is False
    assert result.reason == "CATI_ONLY_ENGINE_EXTERNAL_SIGNAL_ADVISORY_ONLY"
    assert result.persisted is False
    assert threshold_rows(db) == []


def test_external_gate_cannot_call_legacy_engine_or_persistence(db):
    engine, persist, classifier_factory = MagicMock(), MagicMock(), MagicMock()
    result = evaluate_external_candidate(
        db=db, symbol=SYMBOL, side="BUY", confidence=0.99, klines=klines(),
        timeframe="15m", source="TRADINGVIEW", bot_instance_id=BOT,
        engine=engine, persist=persist, regime_classifier_factory=classifier_factory,
    )
    assert not result.passed
    engine.assert_not_called()
    persist.assert_not_called()
    classifier_factory.assert_not_called()
    assert threshold_rows(db) == []


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

    assert result["status"] == "BLOCKED"
    assert "CATI" in result["reason"]
    runner.policy_engine.evaluate.assert_not_called()
    runner._execute_signal_with_evidence.assert_not_called()


def test_the_sole_cati_guard_precedes_historical_external_entry_path():
    from app.runner.runner import PaperRunner

    source = inspect.getsource(PaperRunner.process_external_signal_candidate)
    assert "EXTERNAL_SIGNAL_ADVISORY_ONLY_CATI_DISPATCH_REQUIRED" in source
    assert "evaluate_external_candidate(" not in source
    assert "self._execute_signal_with_evidence(" not in source
