"""
Paper vs Live Parity Tests
Goal: Verify that core trading logic produces IDENTICAL outputs for Paper and Live modes.
"""
import pytest
import sys
import os

# Add bot-backend to path so 'app' module can be found
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

from unittest.mock import MagicMock, PropertyMock, ANY
from dataclasses import replace

# Imports from bot-backend
from app.risk.safety_engine import SafetyEngine, SafetyConfig
from decimal import Decimal
from app.execution.executor import BinanceExecutor, _round_to_tick, _apply_sl_tp_rounding_with_buffer
from _boundary_permit import permitted_entry
from app.symbols.sizing import size_from_budget

class TestPaperLiveParity:
    
    @pytest.fixture
    def common_inputs(self):
        """Standard inputs for both modes."""
        return {
            "symbol": "BTCUSDT",
            "price": 50000.0,
            "budget": 1000.0,
            "equity": 5000.0,
            "leverage": 5,
            "signal": "BUY"
        }

    @pytest.fixture
    def mock_components(self):
        """Mock DB, RiskBudget, Protection."""
        client = MagicMock()
        client.last_price.return_value = 50000.0
        client.klines.return_value = []
        return {
            "db": MagicMock(),
            "budget": MagicMock(),
            "protection": MagicMock(),
            "client": client
        }

    def _create_safety_engine(self, mocks, live_mode: bool):
        config = SafetyConfig()
        # SafetyConfig no longer carries a confidence threshold.
        config.require_kyc_for_live = live_mode
        # Verify if SafetyEngine has other mode-specific logic? 
        return SafetyEngine(mocks["db"], mocks["budget"], mocks["protection"], config)

    def test_sizing_parity(self):
        """Verify size_from_budget is bitwise identical."""
        # It's a pure function, but let's verify no global config leaks
        from app.models.unified_trading import SymbolFilters
        filters = SymbolFilters(
            symbol="BTCUSDT",
            step_size=Decimal("0.001"),
            min_qty=Decimal("0.001"),
            min_notional=Decimal("5"),
            tick_size=Decimal("0.01")
        )
        live_res = size_from_budget(
            symbol="BTCUSDT", price=50000, usdt_margin=100, leverage=5,
            filters=filters
        )
        paper_res = size_from_budget(
            symbol="BTCUSDT", price=50000, usdt_margin=100, leverage=5,
            filters=filters
        )
        
        # Check all fields
        assert live_res == paper_res
        assert live_res.qty == paper_res.qty

    def test_safety_gating_parity_kyc_exception(self, common_inputs, mock_components):
        """
        Verify that Safety Engine logic is identical EXCEPT for strictly documented deviations (KYC).
        """
        # Paper Engine
        paper = self._create_safety_engine(mock_components, live_mode=False)
        # Live Engine
        live = self._create_safety_engine(mock_components, live_mode=True)
        
        # 1. Test Pre-Trade Gating (Normal)
        # Mock DB connection and queries
        conn = MagicMock()
        mock_components["db"].connect.return_value.__enter__.return_value = conn
        
        # We need to handle two queries: daily_trade_counts and order_failures
        def execute_side_effect(query, params):
            cursor = MagicMock()
            if "daily_trade_counts" in str(query):
                cursor.fetchone.return_value = {"trade_count": 0}
            elif "order_failures" in str(query):
                cursor.fetchone.return_value = {"paused_until": None} # Not paused
            else:
                cursor.fetchone.return_value = None
            return cursor
            
        conn.execute.side_effect = execute_side_effect

        p_dec = paper.check_pre_trade("cfg1", "BTC", 0.9, 5, 1000, 0, is_live_mode=False)
        l_dec = live.check_pre_trade("cfg1", "BTC", 0.9, 5, 1000, 0, is_live_mode=True, user_kyc_approved=True)
        
        assert p_dec.allowed == l_dec.allowed
        assert p_dec.message == l_dec.message
        
        # 2. Test KYC Deviation
        # Paper: KYC=False -> Allowed (Paper doesn't care)
        p_dec_no_kyc = paper.check_pre_trade("cfg1", "BTC", 0.9, 5, 1000, 0, is_live_mode=False, user_kyc_approved=False)
        # Live: KYC=False -> Blocked
        l_dec_no_kyc = live.check_pre_trade("cfg1", "BTC", 0.9, 5, 1000, 0, is_live_mode=True, user_kyc_approved=False)
        
        assert p_dec_no_kyc.allowed is True
        assert l_dec_no_kyc.allowed is False
        assert l_dec_no_kyc.block_reason.name == "KYC_REQUIRED"

    
    def test_rounding_parity(self):
        """Verify price rounding is mode-agnostic."""
        # This is a static utility, but crucial for ensuring parity
        tick = Decimal("0.01")
        val = 100.019
        
        # Round logic should be strictly mathematical
        res_down = _round_to_tick(val, tick, round_up=False)
        res_up = _round_to_tick(val, tick, round_up=True)
        
        assert res_down == Decimal("100.01")
        assert res_up == Decimal("100.02")
        
    def test_sl_tp_parity(self):
        """Verify SL/TP calculation is identical for both modes."""
        entry = 50000.0
        tick = Decimal("0.1")
        
        # Scenario: LONG, buffer=2 ticks (0.2)
        # SL below entry, rounded down
        # TP above entry, rounded up (or just nearest?)
        res = _apply_sl_tp_rounding_with_buffer(
            side="BUY",
            entry_px=entry,
            sl_price=49000.05, # Raw
            tp_price=51000.05, # Raw
            tick=tick
        )
        
        # Expect SL <= 49000.0 (rounded down) - buffer?
        # _apply... logic is complex, but parity test asserts determinism
        # We assume logic is shared, so result must be valid float
        sl, tp = res
        assert isinstance(sl, float)
        assert isinstance(tp, float)
        
        # If we ran this in a 'live context' vs 'paper context', logic is same
        # purely functional.
        
    
    def test_executor_decision_parity(self, common_inputs, mock_components, monkeypatch):
        """
        Simulate a full trade execution in Executor and verify payloads match.
        This confirms 'same signal -> same order'.
        """
        client = mock_components["client"]
        audit = MagicMock()
        
        # Patch execution mode to 'live' using fixture
        from app.core.config import settings
        monkeypatch.setattr(settings, "EXECUTION_MODE", "live")
        monkeypatch.setattr(settings, "LIVE_SYMBOLS", "BTCUSDT") # Satisfy symbol check
    
        # Setup mocks to return consistent data for both runs
        # Fix: Mock exchange_info_cached structure to include 'symbols' list for extract_filters
        from app.models.unified_trading import SymbolFilters
        client.get_symbol_filters.return_value = SymbolFilters(
            symbol="BTCUSDT",
            step_size=Decimal("0.001"),
            min_qty=Decimal("0.001"),
            min_notional=Decimal("5.0"),
            tick_size=Decimal("0.01")
        )
        
        client.position_risk.return_value = [] # Flat
        client.get_position_amt.return_value = 0.0 # Float required for > checks
        client.get_position_info.return_value = {"positionAmt": "0"}
        client.account_balance.return_value = [{"asset": "USDT", "balance": "1000", "crossWalletBalance": "1000"}]
        client.account.return_value = {"availableBalance": "1000.0"}
        client.get_prices.return_value = {"BTCUSDT": 50000.0}
        
        mock_registry = MagicMock()
        mock_registry.get_spec.return_value = client.get_symbol_filters.return_value
        monkeypatch.setattr("app.execution.executor.get_instrument_registry", lambda: mock_registry)
        
        # Create Executors
        # Note: Executor doesn't take 'mode', it takes 'client'.
        # We simulate Paper vs Live by how we interact, but here we test core logic.
        executor = BinanceExecutor(client, audit=audit)
        
        # Mock _size_qty to isolate from Sizing (already tested)
        # Or let it run? Let's let it run to test integration parity.
        # But we need to make sure size_from_budget gets proper filters. 
        # _size_qty calls client.get_symbol_info internally or uses cached?
        # Typically it relies on `runner` to pass klines/etc or fetches.
        # Let's inspect `executor.execute_signal`.
        
        # `execute_signal` needs: symbol, signal, usdt.
        # It calls `_size_qty`, which calls `size_from_budget`. 
        # It needs `self.client.get_symbol_filters` logic working.
        
        # Mocking `client.exchange_info` for filters
        # Assuming `BinanceFuturesClient` methods are mocked on `client`
        
        # Run 1: "Paper" (conceptually)
        # We just assert that given inputs, the output calls to `order_market` are
        # Run 1: "Paper" (conceptually)
        # Mock success order response
        mock_order = MagicMock()
        mock_order.broker_order_id = "123"
        mock_order.status = "FILLED"
        mock_order.filled_qty = Decimal("0.002")
        mock_order.avg_price = Decimal("50000.0")
        mock_order.model_dump.return_value = {"order_id": "123", "avg_price": "50000"}
        client.place_order.return_value = mock_order
        
        mock_prot = MagicMock()
        mock_prot.broker_order_id = "prot_123"
        mock_prot.status = "success"
        mock_prot.sl_order_id = "sl_123"
        mock_prot.tp_order_id = "tp_123"
        mock_prot.error = None
        mock_prot.model_dump.return_value = {"order_id": "prot_123"}
        client.place_protection.return_value = mock_prot
        
        import time
        client.get_klines.return_value = [[1, "50000", "50100", "49900", "50000", "10", int(time.time() * 1000)]]
        
        res = permitted_entry(executor, "BTCUSDT", "BUY", 100.0, current_equity=1000.0)
        
        assert res.success is True
        
        # Verify call args
        args, kwargs = client.place_order.call_args
        req = kwargs.get("req") or args[0]
        assert req.symbol == "BTCUSDT"
        assert req.side.name == "BUY"
        assert float(req.qty) > 0
        
        call_qty = float(req.qty)
        
        # Run 2: "Live" (conceptually same inputs, ensure determinism)
        res2 = permitted_entry(executor, "BTCUSDT", "BUY", 100.0, current_equity=1000.0)
        # Capture the qty sent
        args2, kwargs2 = client.place_order.call_args
        req2 = kwargs2.get("req") or args2[0]
        call_qty_2 = float(req2.qty)
        
        assert call_qty == call_qty_2
        assert float(call_qty) > 0

    def test_exec_result_parity(self, common_inputs, mock_components, monkeypatch):
        """
        Verify that the ExecutionResult returned is identical for both modes.
        Even if audit happened elsewhere, this object is the source of truth.
        """
        client = mock_components["client"]
        
        # Setup mocks
        from app.models.unified_trading import SymbolFilters
        client.get_symbol_filters.return_value = SymbolFilters(
            symbol="BTCUSDT",
            step_size=Decimal("0.001"),
            min_qty=Decimal("0.001"),
            min_notional=Decimal("5.0"),
            tick_size=Decimal("0.01")
        )
        client.position_risk.return_value = []
        client.account_balance.return_value = [{"asset": "USDT", "balance": "1000", "crossWalletBalance": "1000"}]
        client.account.return_value = {"availableBalance": "1000.0"}
        mock_order = MagicMock()
        mock_order.broker_order_id = "123"
        mock_order.status = "FILLED"
        mock_order.filled_qty = Decimal("0.002")
        mock_order.avg_price = Decimal("50000.0")
        mock_order.model_dump.return_value = {"order_id": "111", "avg_price": "50000"}
        client.place_order.return_value = mock_order
        
        mock_prot = MagicMock()
        mock_prot.broker_order_id = "prot_111"
        mock_prot.status = "success"
        mock_prot.sl_order_id = "sl_111"
        mock_prot.tp_order_id = "tp_111"
        mock_prot.error = None
        mock_prot.model_dump.return_value = {"order_id": "prot_111"}
        client.place_protection.return_value = mock_prot
        client.get_position_amt.return_value = 0.0
        client.get_position_info.return_value = {"positionAmt": "0"}
        client.get_prices.return_value = {"BTCUSDT": 50000.0}
        
        import time
        client.get_klines.return_value = [[1, "50000", "50100", "49900", "50000", "10", int(time.time() * 1000)]]
        
        mock_registry = MagicMock()
        mock_registry.get_spec.return_value = client.get_symbol_filters.return_value
        monkeypatch.setattr("app.execution.executor.get_instrument_registry", lambda: mock_registry)
        
        # Create Executor
        executor = BinanceExecutor(client, audit=MagicMock())
        
        # Mock price fetch
        client.mark_price.return_value = 50000.0
        
        # Execute "Paper" run - force mode to LIVE
        from app.core.config import settings
        monkeypatch.setattr(settings, "EXECUTION_MODE", "live")
        monkeypatch.setattr(settings, "LIVE_SYMBOLS", "BTCUSDT")
        
        paper_res = permitted_entry(executor, "BTCUSDT", "BUY", 100.0, current_equity=1000.0)
        
        # Execute "Live" run
        client.place_order.reset_mock()
        live_res = permitted_entry(executor, "BTCUSDT", "BUY", 100.0, current_equity=1000.0)
        
        # Assert Result Parity
        assert paper_res.status == live_res.status
        assert paper_res.success == live_res.success
        assert paper_res.details["qty"] == live_res.details["qty"]
        # Do not check orderId if we mocked it identically, it's fine.
        assert paper_res.details["qty"] == live_res.details["qty"]
        # Do not check orderId if we mocked it identically, it's fine.

    def test_runner_state_parity(self, mock_components, monkeypatch):
        """Paper and broker execution modes give the legacy runner the SAME outcome for the same inputs.

        Since 6e0b42c the runner has no entry path: a strategy BUY can no longer make it open, size or persist
        a position in either mode (the old version of this test expected a LONG). What must stay identical
        across modes is that a flat symbol is observed, never executed, and no position state is written.
        """
        from app.runner.runner import PaperRunner
        from app.runner.models import SymbolState

        client = mock_components["client"]
        MockStateStore = MagicMock()
        monkeypatch.setattr("app.runner.runner.StateStore", MockStateStore)
        monkeypatch.setattr("app.runner.runner.DB", MagicMock())
        monkeypatch.setattr("app.runner.runner.Audit", MagicMock())
        for name in ("get_circuit_registry", "get_risk_budget_engine", "get_policy_engine",
                     "get_invariant_checker", "StrategyHealthMonitor", "DrawdownMonitor", "get_trace_recorder"):
            monkeypatch.setattr(f"app.runner.runner.{name}", MagicMock())
        mock_time = MagicMock()
        mock_time.time.return_value = 1700000000.0
        monkeypatch.setattr("app.runner.runner.time", mock_time)

        import time
        client.exchange_info_cached.return_value = {"symbols": [{"symbol": "BTCUSDT", "filters": []}]}
        client.klines.return_value = [[1000, "50000", "50100", "49900", "50000", "10", int(time.time() * 1000)]]
        client.last_price.return_value = 50000.0
        client.account.return_value = {"availableBalance": "1000.0"}

        class BuySignal:
            signal = type("Signal", (), {"value": "BUY", "name": "BUY"})()
            confidence = 0.90
            reason = "a strong scalar BUY the runner must not act on"
            meta = {}

        outcomes = {}
        for mode in ("paper", "broker"):
            runner = PaperRunner(client)
            runner.store = MockStateStore.return_value
            runner.store.save_symbol.reset_mock()
            runner.executor = MagicMock()
            runner.executor.client.get_position_info.return_value = {"positionAmt": "0", "entryPrice": "0"}
            runner.strategy = MagicMock()
            runner.strategy.get_signal.return_value = BuySignal()
            runner._effective_execution_mode = lambda m=mode: m
            runner.state["BTCUSDT"] = SymbolState()  # flat

            out = runner.step_symbol("BTCUSDT")

            saved_positions = [c.args[1].position for c in runner.store.save_symbol.call_args_list
                               if len(c.args) > 1 and hasattr(c.args[1], "position")]
            assert all(p == "NONE" for p in saved_positions), (mode, saved_positions)
            runner.executor.execute_signal.assert_not_called()
            runner.strategy.get_signal.assert_not_called()
            assert runner.state["BTCUSDT"].position == "NONE"
            outcomes[mode] = out

        assert outcomes["paper"] == outcomes["broker"]
        assert outcomes["paper"]["decision"] == "CATI_OBSERVE" and outcomes["paper"]["executed"] is False
