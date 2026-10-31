"""Runner logging hardening, asserted on behaviour.

``PaperRunner._step_symbol_orchestrated`` no longer runs the V2 orchestrator for entries (6e0b42c: CATI is the
sole engine). What it still owes the operator is (a) a clean, observable observe-only outcome on a normal step
and (b) a caught, logged, structured error -- never a crashed cycle -- when anything inside the step raises.
"""
import logging
from unittest.mock import MagicMock, patch

import pytest

from app.runner.runner import PaperRunner


@pytest.fixture
def capture_logs(caplog):
    caplog.set_level(logging.INFO)
    return caplog


def _runner():
    client = MagicMock()
    client.account.return_value = {"totalWalletBalance": "1000.0", "totalMaintMargin": "0.0",
                                   "availableBalance": "1000.0"}
    client.last_price.return_value = 50000.0
    runner = PaperRunner(client)
    runner.state = {"BTCUSDT": MagicMock(position="NONE", position_id=None)}
    runner.executor = MagicMock()
    runner.executor.client.get_position_info.return_value = {"positionAmt": "0.0", "entryPrice": "0.0"}
    runner.position_manager = MagicMock()
    runner.position_manager.get_position.return_value = None
    runner.orchestrator = MagicMock()
    return runner


def test_a_successful_step_is_observe_only_and_never_runs_the_v2_orchestrator(capture_logs):
    runner = _runner()
    recorder = MagicMock()
    with patch("app.runner.runner.get_trace_recorder", return_value=recorder):
        res = runner._step_symbol_orchestrated("BTCUSDT", [], "trace_123")

    assert res["decision"] == "CATI_OBSERVE"
    assert res["executed"] is False and res["signal_source"] == "CATI" and res["evaluated"] is True
    assert res["reason"] == res["authority"]["reason"]          # the authority's own explanation is surfaced
    runner.orchestrator.process_trading_opportunity.assert_not_called()
    runner.executor.execute_signal.assert_not_called()
    gates = [c.kwargs for c in recorder.record_gate.call_args_list]
    assert any(g.get("reason_code") == "CATI_CONTROLLER_DISPATCH_ONLY" and g.get("allowed") is False for g in gates)
    finals = [c.kwargs.get("state_change") for c in recorder.finalize.call_args_list]
    assert "CATI_OBSERVE" in finals
    assert "CRITICAL: Runner exception" not in capture_logs.text


def test_an_exception_inside_the_step_is_caught_logged_and_returned(capture_logs, monkeypatch):
    runner = _runner()

    def boom(*_a, **_k):
        raise ValueError("Simulated Runner Error")

    monkeypatch.setattr("app.trading_intelligence.governance.runtime_authority.resolve_order_authority", boom)
    with patch("app.runner.runner.get_trace_recorder", return_value=MagicMock()):
        res = runner._step_symbol_orchestrated("BTCUSDT", [], "trace_err")

    assert res["decision"] == "error"
    assert res["reason"] == "ValueError: Simulated Runner Error"
    assert "Traceback" in res["details"]["traceback"] and "Simulated Runner Error" in res["details"]["traceback"]
    records = [r for r in capture_logs.records if "CRITICAL: Runner exception for BTCUSDT" in r.getMessage()]
    assert records and records[0].levelno == logging.ERROR and records[0].exc_info is not None
    runner.executor.execute_signal.assert_not_called()


def test_executor_exception_handling(capture_logs):
    """Verify executor propagates typed exceptions rather than swallowing them.

    Phase 5 contract: executor.execute_signal() does NOT catch all exceptions.
    Typed errors (ExchangeError, FatalIntegrationError) propagate to the runner
    which is responsible for circuit breaker logic.
    Raw unexpected errors also propagate so the runner can handle them.
    """
    from app.execution.executor import BinanceExecutor
    executor = BinanceExecutor(MagicMock(), MagicMock(), MagicMock())

    # Mock internal impl to raise a raw RuntimeError (unexpected crash)
    executor._execute_impl = MagicMock(side_effect=RuntimeError("Simulated Execution Crash"))

    # The executor should propagate the exception, not swallow it
    with pytest.raises(RuntimeError, match="Simulated Execution Crash"):
        executor.execute_signal("BTCUSDT", "BUY", 100.0)
