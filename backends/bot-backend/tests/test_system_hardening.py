"""
System Hardening Audit - Final Safety Closure Proofs
"""
import pytest
from unittest.mock import Mock, patch, MagicMock
from decimal import Decimal

from app.execution.executor import BinanceExecutor, ExecResult, ExchangeError, ProtectionRequest
from app.policy.policy_engine import PolicyEngine, PolicyContext, ReasonCode
from app.risk.circuit import ExchangeCircuitBreaker, CircuitBreakerRegistry

class TestSystemHardening:
    
    # ---------------------------------------------------------
    # 1. Stop-Loss Atomic Guarantee
    # ---------------------------------------------------------
    def test_atomic_guarantee_rollback(self):
        """
        Prove that if SL protection order fails, the entry order is rolled back safely 
        using close_position_market or close_position.
        """
        # Mock Client
        mock_client = MagicMock()
        mock_client.get_prices = Mock(return_value={"BTCUSDT": 50000.0})
        mock_client.account = Mock(return_value={"availableBalance": "10000.0", "totalWalletBalance": "10000.0", "totalMarginBalance": "10000.0", "totalMaintMargin": "100.0"})
        del mock_client.get_klines
        del mock_client.klines
        # Mock place order to succeed
        mock_order = MagicMock()
        mock_order.avg_fill_price = 50000.0
        mock_order.broker_order_id = "12345"
        mock_client.place_order = Mock(return_value=mock_order)
        
        # Mock protection to fail
        mock_client.place_protection = Mock(side_effect=Exception("API Timeout on SL"))
        
        # Executor setup
        executor = BinanceExecutor(client=mock_client)
        
        # Override sizing to bypass limits
        executor._size_qty = Mock(return_value=(0.01, {"price": 50000.0}))
        
        # Execute trade
        result = executor.execute_signal("BTCUSDT", "BUY", 500.0)
        
        # Assertions
        assert result.success is False
        assert result.status == "PROTECTION_FAILED_ENTRY_CLOSED"
        assert "Force closed orphaned entry" in result.details["action"]
        assert "Atomic transaction rolled back" in result.details["action"]
        
        # Verify rollback was called
        mock_client.close_position_market.assert_called_once_with("BTCUSDT")

    # ---------------------------------------------------------
    # 2. Volatility Spike Gate
    # ---------------------------------------------------------
    def test_volatility_spike_gate(self):
        """
        Prove that the Policy Engine suspends new entries if short_term_atr > 3 * baseline_atr.
        """
        engine = PolicyEngine()
        
        ctx = PolicyContext(
            symbol="BTCUSDT",
            signal="BUY",
            confidence=1.0,
            position="NONE",
            now_ms=1000000,
            baseline_atr=100.0,
            short_term_atr=350.0,  # > 3x baseline
            equity=10000.0,
            max_daily_loss=500.0,
            leverage=1.0,
            stop_loss_pct=0.05
        )
        
        decision = engine.evaluate(ctx)
        
        assert decision.allowed is False
        assert decision.reason_code == ReasonCode.VOLATILITY_SPIKE
        assert "Volatility spike detected" in decision.reason
        
        # And ensure it passes when below threshold
        ctx.short_term_atr = 250.0
        decision_ok = engine.evaluate(ctx)
        
        # Should not block on Volatility Spike
        assert decision_ok.reason_code != ReasonCode.VOLATILITY_SPIKE

    # ---------------------------------------------------------
    # 3. Slippage-Integrated Sizing
    # ---------------------------------------------------------
    def test_slippage_integrated_sizing(self):
        """
        Prove that high expected slippage reduces the position size by increasing the effective stop distance.
        """
        engine = PolicyEngine()
        
        # Base case: 0% slippage
        ctx_base = PolicyContext(
            symbol="DOGEUSDT",
            signal="BUY",
            confidence=1.0,
            position="NONE",
            entry_price=0.10,
            atr=0.005,
            equity=10000.0,
            trade_amount_mode="atr_risk",
            trade_amount_value=0.0,
            expected_slippage_pct=0.0,
            max_daily_loss=500.0,
            account_risk_pct=1.0, # Target 1% risk
            leverage=1.0,
            stop_loss_pct=0.02
        )
        
        decision_base = engine.evaluate(ctx_base)
        assert decision_base.allowed is True
        base_qty = decision_base.quantity
        
        # High slippage case
        ctx_slip = PolicyContext(
            symbol="DOGEUSDT",
            signal="BUY",
            confidence=1.0,
            position="NONE",
            entry_price=0.10,
            atr=0.005,
            equity=10000.0,
            trade_amount_mode="atr_risk",
            trade_amount_value=0.0,
            expected_slippage_pct=0.02, # 2% slippage
            max_daily_loss=500.0,
            account_risk_pct=1.0,
            leverage=1.0,
            stop_loss_pct=0.02
        )
        
        decision_slip = engine.evaluate(ctx_slip)
        assert decision_slip.allowed is True
        slip_qty = decision_slip.quantity
        
        # Sizing should be drastically reduced
        assert slip_qty < base_qty * 0.90 

    # ---------------------------------------------------------
    # 4. Exchange Health Safe Mode
    # ---------------------------------------------------------
    def test_exchange_safe_mode(self):
        """
        Prove that continuous routing of API timeouts trips the circuit breaker 
        and halts the system, preventing new trades.
        """
        registry = CircuitBreakerRegistry()
        
        # Clear specific registry to ensure fresh state
        registry._breakers = {}
        breaker = registry.get_breaker("BINANCE_TEST")
        breaker.error_limit = 3
        
        # Simulate errors
        for _ in range(4):
            breaker.record_error(ExchangeError("Read timeout on get_ticker"))
            
        assert breaker.is_tripped() is True
        assert breaker.state.name == "HALTED"
        
        engine = PolicyEngine(circuit_registry=registry)
        
        ctx = PolicyContext(
            symbol="BTCUSDT",
            signal="BUY",
            position="NONE",
            broker_id="BINANCE_TEST",
            equity=10000.0,
            max_daily_loss=500.0,
            leverage=1.0,
            stop_loss_pct=0.05
        )
        
        decision = engine.evaluate(ctx)
        
        assert decision.allowed is False
        assert decision.reason_code == ReasonCode.CIRCUIT_BREAKER_TRIPPED
