"""
Unit Tests for Executor Exception Handling & Circuit Breaker

Tests verify:
1. Executor raises ExchangeError on specific failures (no longer swallows errors)
2. Circuit breaker NOT triggered by code exceptions (like AttributeError)
3. Circuit breaker DOES trigger when ExchangeError is explicitly thrown
"""
import pytest
from unittest.mock import Mock, MagicMock
from app.execution.executor import BinanceExecutor, ExecResult, ExchangeError

class TestExecutorExceptionHandling:
    """Test that executor propagates exceptions with proper types"""
    
    def test_executor_exchange_error_propagates(self):
        """Executor should raise ExchangeError on position info failure"""
        # Arrange
        mock_client = Mock()
        mock_client.get_position_info = Mock(side_effect=ConnectionError("Network error"))
        
        executor = BinanceExecutor(client=mock_client)
        
        # Act & Assert
        with pytest.raises(ExchangeError) as exc_info:
            executor.execute_signal("BTCUSDT", "BUY", 100.0)
            
        assert "Failed to fetch position info" in str(exc_info.value)
        assert "Network error" in str(exc_info.value)
    
    def test_executor_timeout_after_submit_returns_submit_uncertain(self):
        """Timeout-like submit errors should fail closed as SUBMIT_UNCERTAIN, not retryable ExchangeError."""
        mock_client = Mock()
        mock_client.get_position_info = Mock(return_value={"positionAmt": "0.0"})
        mock_client.get_prices = Mock(return_value={"ETHUSDT": 5000.0})
        
        # Mock place_order to fail with an unexpected error
        mock_client.place_order = Mock(side_effect=Exception("API Timeout"))
        
        executor = BinanceExecutor(client=mock_client)
        executor._size_qty = Mock(return_value=(0.002, {}))
        
        result = executor.execute_signal("ETHUSDT", "SELL", 50.0)

        assert isinstance(result, ExecResult)
        assert result.success is False
        assert result.status == "SUBMIT_UNCERTAIN"
        assert result.details["error"] == "API Timeout"

    def test_executor_insufficient_margin_returns_failure(self):
        """Insufficient margin from Exchange should return ExecResult, not raise"""
        mock_client = Mock()
        mock_client.get_position_info = Mock(return_value={"positionAmt": "0.0"})
        mock_client.get_prices = Mock(return_value={"BTCUSDT": 50000.0})
        mock_client.account = Mock(return_value={"availableBalance": "1000.0"})
        
        # Mock place_order to fail with margin error map
        mock_client.place_order = Mock(side_effect=Exception('"code":-2019 Insufficient Margin'))
        
        executor = BinanceExecutor(client=mock_client)
        executor._size_qty = Mock(return_value=(0.002, {}))
        
        result = executor.execute_signal("BTCUSDT", "BUY", 100.0)
        
        assert isinstance(result, ExecResult)
        assert result.success == False
        assert result.status == "INSUFFICIENT_MARGIN"

class TestCircuitBreakerBehavior:
    """Test circuit breaker triggers only on real failures"""
    
    def test_circuit_breaker_not_triggered_by_code_exception(self):
        """Exchange errors from get_position_info are wrapped as ExchangeError (Phase 5 contract).
        
        The executor wraps all exchange failures in ExchangeError and propagates them.
        The circuit breaker is NOT tripped by the executor itself — the runner does that
        after catching the ExchangeError. This separates concerns correctly.
        """
        mock_client = Mock()
        # AttributeError from get_position_info is wrapped as ExchangeError by executor
        mock_client.get_position_info = Mock(side_effect=AttributeError("Object has no attribute"))
        
        executor = BinanceExecutor(client=mock_client)
        
        # Executor wraps it as ExchangeError and propagates
        with pytest.raises(ExchangeError) as exc_info:
            executor.execute_signal("BTCUSDT", "BUY", 100.0)
        
        assert "Failed to fetch position info" in str(exc_info.value)
        
        # Circuit breaker is NOT tripped by the executor — the runner handles that
        assert not executor.circuit.is_tripped(), "Executor does not trip the breaker; runner does"
        
    def test_circuit_breaker_recording_logic(self):
        """Test the explicit recording logic on the breaker directly for ExchangeError"""
        from app.risk.circuit import ExchangeCircuitBreaker
        breaker = ExchangeCircuitBreaker(error_limit=5)
        
        # Record 10 random exceptions
        for _ in range(10):
            breaker.record_error(ValueError("Bad val"))
            
        assert not breaker.is_tripped()
        
        # Record ExchangeErrors
        for _ in range(5):
            breaker.record_error(ExchangeError("Timeout"))
            
        assert breaker.is_tripped()

class TestExecResultContract:
    """Test ExecResult dataclass contract"""
    
    def test_exec_result_has_success_field(self):
        """ExecResult must have success boolean field"""
        result = ExecResult(
            status="TEST",
            details={"test": "data"},
            success=True
        )
        assert hasattr(result, "success")
        assert result.success == True
    
    def test_exec_result_has_error_field(self):
        """ExecResult must have error string field for failures"""
        result = ExecResult(
            status="FAILED",
            details={},
            success=False,
            error="Test error message"
        )
        assert hasattr(result, "error")
        assert result.error == "Test error message"
    
    def test_exec_result_has_avg_price_field(self):
        """ExecResult must have avg_price for successful trades"""
        result = ExecResult(
            status="ORDER_PLACED",
            details={},
            success=True,
            avg_price=50000.5
        )
        assert hasattr(result, "avg_price")
        assert result.avg_price == 50000.5

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
