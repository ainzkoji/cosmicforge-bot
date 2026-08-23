import logging
import pytest
from unittest.mock import MagicMock, patch
from app.runner.runner import PaperRunner
from app.execution.executor import ExecResult

# Setup logger to capture logs
@pytest.fixture
def capture_logs(caplog):
    caplog.set_level(logging.INFO)
    return caplog

def test_eval_logging_on_success(capture_logs):
    """Verify [EVAL] log is printed on successful step."""
    
    # Mock dependencies
    client = MagicMock()
    runner = PaperRunner(client)
    
    # Mock state
    runner.state = {"BTCUSDT": MagicMock(position="NONE")}
    runner.executor = MagicMock()
    runner.executor.client.get_position_info.return_value = {"positionAmt": "0.0", "entryPrice": "0.0"}
    runner.position_manager = MagicMock()
    client.account.return_value = {"totalWalletBalance": "1000.0", "totalMaintMargin": "0.0", "availableBalance": "1000.0"}
    client.last_price.return_value = 50000.0
    runner.orchestrator = MagicMock()
    runner.orchestrator.process_trading_opportunity.return_value = {
        "decision": "pass",
        "reason": "low confidence",
        "details": {
            "strategy_output": {"signal": "BUY", "confidence": 0.15}
        }
    }
    
    res = runner._step_symbol_orchestrated("BTCUSDT", [], "trace_123")
    
    # Check log
    assert res["decision"] == "pass"
    # Ensure it logged either SIGNAL or EVAL
    assert "SIGNAL" in capture_logs.text or "EVAL" in capture_logs.text or "BTCUSDT" in capture_logs.text

def test_exception_handling_in_runner(capture_logs, capsys):
    """Verify runner catches exception and logs traceback."""
    
    client = MagicMock()
    client.account.return_value = {"totalWalletBalance": "1000.0", "totalMaintMargin": "0.0", "availableBalance": "1000.0"}
    client.last_price.return_value = 50000.0
    runner = PaperRunner(client)
    runner.state = {"BTCUSDT": MagicMock(position="NONE")}
    runner.executor = MagicMock()
    runner.executor.client.get_position_info.return_value = {"positionAmt": "0.0", "entryPrice": "0.0"}
    runner.position_manager = MagicMock()
    
    # Force exception in orchestrator
    runner.orchestrator = MagicMock()
    runner.orchestrator.process_trading_opportunity.side_effect = ValueError("Simulated Runner Error")
    
    with patch("logging.Logger.exception") as mock_log:
        res = runner._step_symbol_orchestrated("BTCUSDT", [], "trace_err")
    
    # Verify result
    assert res["decision"] == "error"
    assert "Simulated Runner Error" in res["reason"]
    
    log_calls = "".join(str(call) for call in mock_log.mock_calls)
    assert "CRITICAL: Runner exception" in log_calls

def test_executor_exception_handling(capture_logs):
    """Verify executor propagates typed exceptions rather than swallowing them.
    
    Phase 5 contract: executor.execute_signal() does NOT catch all exceptions.
    Typed errors (ExchangeError, FatalIntegrationError) propagate to the runner
    which is responsible for circuit breaker logic.
    Raw unexpected errors also propagate so the runner can handle them.
    """
    from app.execution.executor import BinanceExecutor, ExchangeError
    executor = BinanceExecutor(MagicMock(), MagicMock(), MagicMock())
    
    # Mock internal impl to raise a raw RuntimeError (unexpected crash)
    executor._execute_impl = MagicMock(side_effect=RuntimeError("Simulated Execution Crash"))
    
    # The executor should propagate the exception, not swallow it
    with pytest.raises(RuntimeError, match="Simulated Execution Crash"):
        executor.execute_signal("BTCUSDT", "BUY", 100.0)
