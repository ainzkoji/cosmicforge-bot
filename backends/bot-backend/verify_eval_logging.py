
import sys
import os
import unittest
from unittest.mock import MagicMock, patch, PropertyMock
import logging
import json
from collections import defaultdict

# Ensure app is in path
sys.path.append(os.getcwd())
sys.path.append(os.path.join(os.getcwd(), 'backends', 'bot-backend'))

# Mock DB and other persistence layers BEFORE importing runner if they execute on import
# But here they are likely imported inside classes or at top level.
# We will rely on patching runner.py imports or instance attributes.

from app.runner.runner import PaperRunner
from app.core.trading_orchestrator import TradingOrchestrator

class TestEvalLogging(unittest.TestCase):
    def setUp(self):
        # Create a runner with heavily mocked dependencies
        self.mock_client = MagicMock()
        
        # Patch init to avoid setting up real DB/Audit
        with patch('app.runner.runner.PaperRunner.__init__', return_value=None) as mock_init:
            self.runner = PaperRunner(self.mock_client)
            # Manually set attributes that __init__ would have set
            self.runner.client = self.mock_client
            self.runner.run_id = "test_run"
            self.runner.cycle_id = "test_cycle"
            self.runner.state = {}
            self.runner._symbol_locks = defaultdict(MagicMock)
            self.runner.audit = MagicMock()
            self.runner.store = MagicMock()
            self.runner.daily = MagicMock()
            self.runner.daily.kill = False
            self.runner.daily.realized_pnl = 0.0
            self.runner.orchestrator = MagicMock(spec=TradingOrchestrator)
            self.runner.orchestrator.strategy_id = "test_strat"
            self.runner.parameters = {}
            self.runner.interval = "15m"
            self.runner.policy_engine = MagicMock()
            self.runner.context = MagicMock()
            self.runner.context.broker_account_id = "BINANCE"
            self.runner._last_protection_checks = {}
            self.runner.usdt_map = {}
            self.runner.daily_max_loss = 100
            self.runner.max_trades_daily = 10
            self.runner.max_open_positions = 3
            self.runner._closed_symbols_this_cycle = set()
            self.runner.last_signal_confidence = {}
            
    @patch('app.runner.runner.logger')
    @patch('app.runner.runner.get_trace_recorder')
    @patch('app.runner.runner.get_invariant_checker')
    @patch('app.runner.runner.settings')
    @patch('app.runner.runner.calculate_atr', return_value=100.0)
    def test_eval_logging_always_fires(self, mock_atr, mock_settings, mock_checker, mock_recorder, mock_logger):
        # Setup specific mocks
        mock_recorder.return_value.start_trace.return_value = "trace_123"
        self.runner.client.klines.return_value = [] # legitimate list
        self.runner.client.last_price.return_value = 50000.0
        
        # Mock orchestrator behavior
        self.runner._step_symbol_orchestrated = MagicMock(return_value={
            "decision": "PASS",
            "reason": "low_confidence",
            "details": {
                "strategy_output": {
                    "signal": "BUY",
                    "confidence": 0.45
                }
            }
        })
        
        # Execute
        self.runner.step_symbol("BTCUSDT")
        
        # Verify [EVAL] log
        eval_log_found = False
        for call in mock_logger.info.call_args_list:
            msg = call[0][0]
            if "[EVAL]" in msg:
                eval_log_found = True
                print(f"FOUND LOG: {msg}")
                self.assertIn("bot=test_run", msg)
                self.assertIn("sym=BTCUSDT", msg)
                self.assertIn("strat=test_strat", msg)
                self.assertIn("sig=BUY", msg)
                self.assertIn("conf_raw=0.4500", msg)
                self.assertIn("decision=PASS", msg)
                self.assertIn("reason=low_confidence", msg)
                break
        
        self.assertTrue(eval_log_found, "The [EVAL] log was not found in logger calls")

    @patch('app.runner.runner.logger')
    @patch('app.runner.runner.get_trace_recorder')
    @patch('app.runner.runner.get_invariant_checker')
    @patch('app.runner.runner.settings')
    def test_eval_logging_on_exception(self, mock_settings, mock_checker, mock_recorder, mock_logger):
        # Setup specific mocks
        mock_recorder.return_value.start_trace.return_value = "trace_123"
        self.runner.client.klines.return_value = []
        
        # FORCE EXCEPTION
        self.runner._step_symbol_orchestrated = MagicMock(side_effect=ValueError("Simulated Error"))
        
        # Execute
        self.runner.step_symbol("ETHUSDT")
        
        # Verify [EVAL] log exists even after crash
        eval_log_found = False
        for call in mock_logger.info.call_args_list:
             msg = call[0][0]
             if "[EVAL]" in msg:
                 eval_log_found = True
                 print(f"FOUND LOG (Exception Path): {msg}")
                 self.assertIn("decision=CRITICAL_ERROR", msg)
                 self.assertIn("reason=Runner Top-Level Error", msg)
                 break
        
        self.assertTrue(eval_log_found, "The [EVAL] log was not preserved during exception")

if __name__ == '__main__':
    unittest.main()
