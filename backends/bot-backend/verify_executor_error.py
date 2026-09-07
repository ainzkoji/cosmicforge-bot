
import sys
import os
import unittest
from unittest.mock import MagicMock, patch

# Ensure app is in path
sys.path.append(os.getcwd())
sys.path.append(os.path.join(os.getcwd(), 'backends', 'bot-backend'))

from app.execution.executor import BinanceExecutor, ExecResult

class TestExecutorErrorHandling(unittest.TestCase):
    def setUp(self):
        self.mock_client = MagicMock()
        self.executor = BinanceExecutor(self.mock_client)
        # Mock circuit breaker inside executor
        self.executor.circuit = MagicMock()
        self.executor.circuit.is_tripped.return_value = False
        
    @patch('app.execution.executor.get_instrument_registry')
    @patch('app.execution.executor.settings')
    def test_executor_records_circuit_breaker_on_exception(self, mock_settings, mock_registry):
        # Setup dependencies for sizing
        mock_spec = MagicMock()
        mock_spec.step_size = "0.001"
        mock_spec.min_qty = "0.001"
        mock_spec.min_notional = "5.0"
        mock_spec.contract_size = "1.0"
        mock_registry.return_value.get_spec.return_value = mock_spec
        
        mock_settings.SYMBOL_LEVERAGE_MAP = ""
        mock_settings.DEFAULT_LEVERAGE = 1
        mock_settings.MIN_LEVERAGE = 1
        mock_settings.MIN_NOTIONAL_USDT = 5.0
        mock_settings.EXECUTION_MODE = "live"
        mock_settings.LIVE_SYMBOLS = "BTCUSDT"
        
        self.mock_client.get_prices.return_value = {"BTCUSDT": 50000.0}
        
        # Setup: place_order raises exception (CRITICAL)
        self.mock_client.place_order.side_effect = Exception("API Order Failed")
        
        # Execute
        res = self.executor.execute_signal("BTCUSDT", "BUY", 100.0)
        
        # Verify result is failure with details
        self.assertFalse(res.success)
        self.assertIn(res.status, ["EXECUTOR_ERROR", "CRITICAL_EXEC_ERROR"])
        self.assertIn("API Order Failed", res.error)
        # Check that we got details including traceback
        self.assertIn("traceback", res.details)
        
        if "traceback" in res.details:
            print("\n=== EXECUTOR TRACEBACK ===")
            print(res.details["traceback"])
            print("==========================\n")

        # Verify circuit breaker recorded error
        self.executor.circuit.record_error.assert_called_once()
        print("✅ Circuit breaker recorded error successfully.")

if __name__ == '__main__':
    unittest.main()
