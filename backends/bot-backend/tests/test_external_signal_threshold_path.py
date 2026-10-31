"""External observations cannot receive independent CATI entry authority.

Retired threshold and execution approvals are replaced by unconditional denial.
No external candidate can reach risk or the broker executor.

The compatibility gate ``app/decision/external_signal_gate.py`` (a TradingView
leftover that always refused) was removed in Section I Batch 7: no production
module imported it. The denial it stood for is asserted here where an external
candidate still arrives, ``PaperRunner.process_external_signal_candidate``.
"""
from __future__ import annotations

import importlib.util
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from shared_lib.persistence.db import DB
from shared_lib.persistence.migrations import migrate

SYMBOL = "BTCUSDT"


@pytest.fixture
def db():
    database = DB(":memory:")
    migrate(database)
    return database


def threshold_rows(db):
    with db.connect() as conn:
        return conn.execute("SELECT threshold_decision_id FROM threshold_decisions").fetchall()


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
    runner.client = MagicMock()
    runner.policy_engine = MagicMock()
    runner.orchestrator = MagicMock()
    runner.executor = MagicMock()
    runner._execute_signal_with_evidence = MagicMock()
    return runner


@pytest.mark.parametrize("confidence", [0.20, 0.95, 0.99, None, 1.5])
def test_an_external_candidate_never_reaches_threshold_risk_or_the_executor(db, confidence):
    runner = _bare_runner(db)

    result = runner.process_external_signal_candidate({
        "source": "TRADINGVIEW", "queue_id": "q1", "symbol": SYMBOL,
        "action": "BUY", "confidence": confidence,
    })

    assert result["status"] == "BLOCKED" and result["decision"] == "BLOCKED"
    assert result["executed"] is False
    assert result["reason"] == "EXTERNAL_SIGNAL_ADVISORY_ONLY_CATI_DISPATCH_REQUIRED"
    runner.policy_engine.evaluate.assert_not_called()
    runner.orchestrator.assert_not_called()
    assert runner.orchestrator.mock_calls == []
    runner._execute_signal_with_evidence.assert_not_called()
    runner.executor.execute_signal.assert_not_called()
    raw_client = runner.__dict__["_client_raw"]
    assert not [c for c in raw_client.mock_calls if "order" in c[0] or "position" in c[0]]
    assert threshold_rows(db) == []


def test_the_retired_external_gate_module_is_gone():
    assert importlib.util.find_spec("app.decision.external_signal_gate") is None
