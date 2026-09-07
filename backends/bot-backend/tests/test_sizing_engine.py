"""
Unit tests for the Universal Sizing Engine.
"""
import pytest
from app.risk.sizing_engine import get_sizing_engine, SizingInputs, SizingResult

class TestUniversalSizingEngine:
    
    @pytest.fixture
    def engine(self):
        return get_sizing_engine()
        
    def test_basic_risk_based_sizing(self, engine):
        """Test fundamental risk-to-stop distance calculation."""
        inputs = SizingInputs(
            symbol="BTCUSDT",
            equity=1000.0,
            entry_price=50000.0,
            risk_per_trade_pct=0.01, # 1% risk ($10)
            stop_distance_pct=0.02, # 2% stop distance
        )
        
        # Risk = notional * stop_distance
        # $10 = notional * 0.02  => notional = $500
        # qty = 500 / 50000 = 0.01
        res = engine.calculate(inputs)
        
        assert res.allowed is True
        assert res.notional == pytest.approx(500.0, rel=0.01)
        assert res.quantity == pytest.approx(0.01, rel=0.01)
        assert res.effective_risk_pct == pytest.approx(0.01, rel=0.01)
        assert res.details.get("sizing_method") == "risk_based"

    def test_hard_risk_ceiling(self, engine):
        """Should cap target risk at max_risk_ceiling_pct."""
        inputs = SizingInputs(
            symbol="BTCUSDT",
            equity=1000.0,
            entry_price=50000.0,
            risk_per_trade_pct=0.10, # Requesting 10% risk!
            max_risk_ceiling_pct=0.05, # Capped at 5% ($50)
            stop_distance_pct=0.02,
        )
        
        # Risk capped at $50. Notional = 50 / 0.02 = $2500
        res = engine.calculate(inputs)
        
        assert res.allowed is True
        assert res.details["target_risk_usdt"] == 50.0
        assert res.notional == pytest.approx(2500.0, rel=0.01)

    def test_fixed_usdt_amount(self, engine):
        """Should override with exact USDT amount."""
        inputs = SizingInputs(
            symbol="BTCUSDT",
            equity=1000.0,
            entry_price=50000.0,
            risk_per_trade_pct=0.01,
            stop_distance_pct=0.02,
            custom_trade_amount_mode="fixed",
            custom_trade_amount_value=123.45,
        )
        
        res = engine.calculate(inputs)
        
        assert res.allowed is True
        assert res.notional == 123.45
        assert res.details.get("sizing_method") == "fixed_usdt"

    def test_percent_equity_amount(self, engine):
        """Should override with exact % of equity notional."""
        inputs = SizingInputs(
            symbol="BTCUSDT",
            equity=2000.0,
            entry_price=50000.0,
            risk_per_trade_pct=0.01,
            stop_distance_pct=0.02,
            custom_trade_amount_mode="percent",
            custom_trade_amount_value=15.0, # 15% of $2000 = $300
        )
        
        res = engine.calculate(inputs)
        
        assert res.allowed is True
        assert res.notional == 300.0
        assert res.details.get("sizing_method") == "percent_equity"

    def test_fallback_mode_multiplier(self, engine):
        """Should apply fallback size reduction."""
        inputs = SizingInputs(
            symbol="BTCUSDT",
            equity=1000.0,
            entry_price=50000.0,
            risk_per_trade_pct=0.01,
            stop_distance_pct=0.02,
            is_fallback_mode=True,       # Enable fallback
            fallback_size_multiplier=0.25 # 25% of normal
        )
        
        # Normal notional = $500. Fallback = $125
        res = engine.calculate(inputs)
        
        assert res.allowed is True
        assert res.notional == pytest.approx(125.0, rel=0.01)
        assert "fallback" in res.details.get("sizing_method", "")

    def test_min_notional_bump(self, engine):
        """Should bump to minimum notional if account allows."""
        inputs = SizingInputs(
            symbol="BTCUSDT",
            equity=1000.0,
            entry_price=50000.0,
            risk_per_trade_pct=0.01,
            stop_distance_pct=0.10, # Very wide stop (10%), risk $10 => Notional $100
            custom_trade_amount_mode="fixed",
            custom_trade_amount_value=2.0, # Ask for $2 trade
            min_notional_usdt=5.0,
        )
        
        res = engine.calculate(inputs)
        
        assert res.allowed is True
        assert res.notional == 5.0
        assert "bumped_to_min" in res.details.get("sizing_method", "")

    def test_insufficient_equity_for_min_notional(self, engine):
        """Should block if equity is strictly less than min notional."""
        inputs = SizingInputs(
            symbol="BTCUSDT",
            equity=4.0, # Account only has $4
            entry_price=50000.0,
            risk_per_trade_pct=0.01,
            stop_distance_pct=0.02,
            min_notional_usdt=5.0,
        )
        
        res = engine.calculate(inputs)
        
        assert res.allowed is False
        assert res.notional == 0.0

    def test_maximum_notional_cap(self, engine):
        """Should cap at max_notional_usdt limits."""
        inputs = SizingInputs(
            symbol="BTCUSDT",
            equity=1_000_000.0,
            entry_price=50000.0,
            risk_per_trade_pct=0.01,
            stop_distance_pct=0.01, # 1% stop on $10k risk => $1,000,000 notional
            max_notional_usdt=100_000.0, # System max
        )
        
        res = engine.calculate(inputs)
        
        assert res.allowed is True
        assert res.notional == 100_000.0
        assert "capped_at_max" in res.details.get("sizing_method", "")

    def test_zero_equity_edge_case(self, engine):
        inputs = SizingInputs(
            symbol="BTCUSDT",
            equity=0.0,
            entry_price=50000.0,
            risk_per_trade_pct=0.01,
            stop_distance_pct=0.02,
        )
        res = engine.calculate(inputs)
        assert res.allowed is False

    def test_zero_price_edge_case(self, engine):
        inputs = SizingInputs(
            symbol="BTCUSDT",
            equity=1000.0,
            entry_price=0.0,
            risk_per_trade_pct=0.01,
            stop_distance_pct=0.02,
        )
        res = engine.calculate(inputs)
        assert res.allowed is False
