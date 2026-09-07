
import unittest
import json
import uuid
from typing import Dict, Any

from shared_lib.persistence.db import DB
from app.core.trading_orchestrator import TradingOrchestrator
from app.risk.system_limits import SystemLimits, UserConfigurableLimits
from app.strategy.strategy_framework import StrategyOutput, Signal, BaseStrategy, StrategyFamily

# Mock Strategy
class MockStrategy(BaseStrategy):
    def __init__(self, output: StrategyOutput):
        super().__init__("mock", StrategyFamily.TREND_FOLLOWING, "Mock", "Desc")
        self.mock_output = output
        
    def analyze(self, *args, **kwargs):
        return self.mock_output
    
    def validate_output(self, output):
        return True
        
    def get_parameter_schema(self):
        return {}

class TestAuditFlow(unittest.TestCase):
    def setUp(self):
        self.db = DB() # Use default test DB
        self.config_id = f"test_config_{uuid.uuid4().hex}"
        self.user_limits = UserConfigurableLimits(
            max_leverage=10.0,
            risk_level="aggressive",
            allowed_symbols=["BTCUSDT"]
        )
        self.system_limits = SystemLimits()
        
        # Insert mock config into DB to satisfy foreign keys
        with self.db.connect() as conn:
             # Ensure users/accounts exist... skipping for unit test if FKs are enforced.
             # Sqlite default doesn't strictly enforce FK unless enabled.
             # We will insert a config.
             conn.execute("PRAGMA foreign_keys = OFF")
             conn.execute(
                 "INSERT INTO user_strategy_configs (id, user_id, broker_account_id, strategy_id, name, status, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                 (self.config_id, "u1", "b1", "s1", "Test", "active", "now", "now")
             )
             conn.execute("INSERT INTO risk_parameters (config_id, risk_profile, portfolio_risk_pct, per_trade_risk_pct, max_margin_usage_pct, max_drawdown_pct, daily_loss_limit_pct, position_sizing_method, base_position_slots, max_position_slots) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                 (self.config_id, "aggressive", 0.1, 0.05, 1.0, 0.2, 0.1, "fixed", 5, 10)
             )

    def test_decision_log_execute(self):
        # Setup Orchestrator
        orchestrator = TradingOrchestrator(
             config_id=self.config_id,
             user_config=self.user_limits,
             system_limits=self.system_limits,
             strategy=MockStrategy(StrategyOutput(
                 signal=Signal.BUY,
                 confidence=0.9,
                 suggested_stop_distance=0.01,
                 riskiness=0.2
             ))
        )
        
        run_id = "run_123"
        
        # Execute
        result = orchestrator.process_trading_opportunity(
            symbol="BTCUSDT",
            klines=[],
            current_price=50000.0,
            current_equity=10000.0,
            margin_used=0.0,
            margin_available=10000.0,
            run_id=run_id
        )
        
        self.assertEqual(result["decision"], "execute")
        
        # Verify Log
        with self.db.connect() as conn:
            row = conn.execute("SELECT * FROM decision_logs WHERE run_id = ?", (run_id,)).fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(row["symbol"], "BTCUSDT")
            self.assertEqual(row["final_action"], "execute")
            
            # Check JSON
            sizing = json.loads(row["sizing_decision_json"])
            self.assertTrue("adjusted_size" in sizing)

    def test_decision_log_blocked(self):
        # Setup Orchestrator (Blocked by symbol)
        orchestrator = TradingOrchestrator(
             config_id=self.config_id,
             user_config=self.user_limits, # Only BTCUSDT allowed
             system_limits=self.system_limits,
             strategy=MockStrategy(StrategyOutput(Signal.BUY, 0.9, 0.01))
        )
        
        run_id = "run_456"
        
        # Execute for ETHUSDT
        result = orchestrator.process_trading_opportunity(
            symbol="ETHUSDT",
            klines=[],
            current_price=3000.0,
            current_equity=10000.0,
            margin_used=0.0,
            margin_available=10000.0,
            run_id=run_id
        )
        
        self.assertEqual(result["decision"], "blocked")
        
        # Verify Log
        with self.db.connect() as conn:
            row = conn.execute("SELECT * FROM decision_logs WHERE run_id = ?", (run_id,)).fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(row["symbol"], "ETHUSDT")
            self.assertEqual(row["final_action"], "blocked")

if __name__ == "__main__":
    unittest.main()
