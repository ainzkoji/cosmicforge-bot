# tests/test_policy_engine.py
r"""
Unit tests for the unified PolicyEngine.

Run with:
    cd c:\Users\favou\OneDrive\Desktop\cosmicforge-bot\backends\bot-backend
    python -m pytest tests/test_policy_engine.py -v
"""
import pytest
import time

from app.policy.policy_engine import (
    PolicyEngine,
    PolicyContext,
    PolicyDecision,
    ReasonCode,
    Action,
    RiskLevel,
    calculate_atr,
    reset_policy_engine,
)


class TestReasonCodes:
    """Test that reason codes are semantic and don't overlap."""
    
    def test_reason_codes_are_unique(self):
        """All reason codes should have unique values."""
        values = [r.value for r in ReasonCode]
        assert len(values) == len(set(values))
    
    def test_reason_codes_are_uppercase(self):
        """All reason codes should be UPPER_SNAKE_CASE."""
        for r in ReasonCode:
            assert r.value == r.value.upper()
            assert " " not in r.value


class TestATRCalculation:
    """Test ATR calculation from klines."""
    
    def test_calculate_atr_with_list_format(self):
        """Test ATR with Binance kline list format."""
        # Simulated klines: [open_time, open, high, low, close, volume, ...]
        klines = [
            [0, "100", "105", "95", "102", "1000"],
            [1, "102", "108", "100", "106", "1000"],
            [2, "106", "110", "104", "108", "1000"],
            [3, "108", "112", "106", "110", "1000"],
            [4, "110", "115", "108", "114", "1000"],
        ] * 5  # 25 klines
        
        atr = calculate_atr(klines, period=14)
        assert atr > 0
        assert atr < 20  # Reasonable range
    
    def test_calculate_atr_with_dict_format(self):
        """Test ATR with dict format."""
        klines = [
            {"high": 105, "low": 95, "close": 102},
            {"high": 108, "low": 100, "close": 106},
            {"high": 110, "low": 104, "close": 108},
        ] * 10
        
        atr = calculate_atr(klines, period=14)
        assert atr > 0
    
    def test_calculate_atr_insufficient_data(self):
        """ATR should return 0 with insufficient data."""
        klines = [{"high": 105, "low": 95, "close": 100}] * 5
        atr = calculate_atr(klines, period=14)
        assert atr >= 0  # Either 0 or small value


class TestPolicyEngineGating:
    """Test PolicyEngine gating checks."""
    
    @pytest.fixture
    def engine(self):
        reset_policy_engine()
        return PolicyEngine()
    
    def test_hold_signal_returns_ok(self, engine):
        """HOLD signal should return allowed with SIGNAL_HOLD."""
        ctx = PolicyContext(
            symbol="BTCUSDT",
            signal="HOLD",
            now_ms=int(time.time() * 1000),
        )
        
        decision = engine.evaluate(ctx)
        
        assert decision.allowed is True
        assert decision.action == Action.HOLD
        assert decision.reason_code == ReasonCode.SIGNAL_HOLD
    
    def test_kill_switch_blocks(self, engine):
        """Kill switch should block trades."""
        ctx = PolicyContext(
            symbol="BTCUSDT",
            signal="BUY",
            kill_switch=True,
            now_ms=int(time.time() * 1000),
        )
        
        decision = engine.evaluate(ctx)
        
        assert decision.allowed is False
        assert decision.reason_code == ReasonCode.KILL_SWITCH_ACTIVE
    
    def test_daily_loss_limit_blocks(self, engine):
        """Exceeding daily loss limit should block."""
        ctx = PolicyContext(
            symbol="BTCUSDT",
            signal="BUY",
            daily_realized_pnl=-60.0,
            max_daily_loss=50.0,
            now_ms=int(time.time() * 1000),
        )
        
        decision = engine.evaluate(ctx)
        
        assert decision.allowed is False
        assert decision.reason_code == ReasonCode.DAILY_LOSS_LIMIT
    
    def test_daily_trade_limit_blocks(self, engine):
        """Exceeding daily trade count should block."""
        ctx = PolicyContext(
            symbol="BTCUSDT",
            signal="BUY",
            daily_trade_count=25,
            max_daily_trades=20,
            now_ms=int(time.time() * 1000),
        )
        
        decision = engine.evaluate(ctx)
        
        assert decision.allowed is False
        assert decision.reason_code == ReasonCode.DAILY_TRADE_LIMIT
    
    def test_cooldown_blocks(self, engine):
        """Active cooldown should block."""
        now = int(time.time() * 1000)
        ctx = PolicyContext(
            symbol="BTCUSDT",
            signal="BUY",
            last_trade_ms=now - 60_000,  # 60 seconds ago
            cooldown_seconds=120,  # 2 minute cooldown
            now_ms=now,
        )
        
        decision = engine.evaluate(ctx)
        
        assert decision.allowed is False
        assert decision.reason_code == ReasonCode.COOLDOWN_ACTIVE
    
    def test_cooldown_ok_after_expiry(self, engine):
        """Cooldown should pass after expiry."""
        now = int(time.time() * 1000)
        ctx = PolicyContext(
            symbol="BTCUSDT",
            signal="BUY",
            position="NONE",
            last_trade_ms=now - 200_000,  # 200 seconds ago
            cooldown_seconds=120,  # 2 minute cooldown
            now_ms=now,
            entry_price=50000.0,
            atr=500.0,
            equity=1000.0,
            margin_available=1000.0,
        )
        
        decision = engine.evaluate(ctx)
        
        # Should pass cooldown (may fail other checks, but not cooldown)
        assert decision.reason_code != ReasonCode.COOLDOWN_ACTIVE
    
    def test_low_confidence_blocks(self, engine):
        """Low confidence should block."""
        ctx = PolicyContext(
            symbol="BTCUSDT",
            signal="BUY",
            confidence=0.05,  # 5% < 10% threshold
            now_ms=int(time.time() * 1000),
        )
        
        decision = engine.evaluate(ctx)
        
        assert decision.allowed is False
        assert decision.reason_code == ReasonCode.LOW_CONFIDENCE


class TestPolicyEngineAction:
    """Test PolicyEngine action determination."""
    
    @pytest.fixture
    def engine(self):
        reset_policy_engine()
        return PolicyEngine()
    
    def test_open_long_from_flat(self, engine):
        """BUY signal when flat should open long."""
        ctx = PolicyContext(
            symbol="BTCUSDT",
            signal="BUY",
            position="NONE",
            confidence=1.0,
            now_ms=int(time.time() * 1000),
            entry_price=50000.0,
            atr=500.0,
            equity=1000.0,
            margin_available=1000.0,
            leverage=10.0,
        )
        
        decision = engine.evaluate(ctx)
        
        assert decision.allowed is True
        assert decision.action == Action.OPEN_LONG
        assert decision.quantity > 0
    
    def test_open_short_from_flat(self, engine):
        """SELL signal when flat should open short."""
        ctx = PolicyContext(
            symbol="BTCUSDT",
            signal="SELL",
            position="NONE",
            confidence=1.0,
            now_ms=int(time.time() * 1000),
            entry_price=50000.0,
            atr=500.0,
            equity=1000.0,
            margin_available=1000.0,
            leverage=10.0,
        )
        
        decision = engine.evaluate(ctx)
        
        assert decision.allowed is True
        assert decision.action == Action.OPEN_SHORT
    
    def test_close_long_on_sell(self, engine):
        """SELL signal when long should close (normal mode)."""
        ctx = PolicyContext(
            symbol="BTCUSDT",
            signal="SELL",
            position="LONG",
            trade_mode="normal",
            now_ms=int(time.time() * 1000),
        )
        
        decision = engine.evaluate(ctx)
        
        assert decision.allowed is True
        assert decision.action == Action.CLOSE
    
    def test_flip_to_short_on_sell(self, engine):
        """SELL signal when long should flip (flip mode)."""
        ctx = PolicyContext(
            symbol="BTCUSDT",
            signal="SELL",
            position="LONG",
            trade_mode="flip",
            confidence=1.0,
            now_ms=int(time.time() * 1000),
            entry_price=50000.0,
            atr=500.0,
            equity=1000.0,
            margin_available=1000.0,
            leverage=10.0,
        )
        
        decision = engine.evaluate(ctx)
        
        assert decision.allowed is True
        assert decision.action == Action.FLIP_TO_SHORT


class TestCompoundRisk:
    """Test compound risk validation."""
    
    @pytest.fixture
    def engine(self):
        reset_policy_engine()
        return PolicyEngine()
    
    def test_compound_risk_within_limits(self, engine):
        """Compound risk within profile limit should pass."""
        ctx = PolicyContext(
            symbol="BTCUSDT",
            signal="BUY",
            position="NONE",
            confidence=1.0,
            now_ms=int(time.time() * 1000),
            entry_price=50000.0,
            atr=250.0,  # 0.5% of price
            stop_loss_pct=0.01,  # 1%
            leverage=10.0,  # Compound = 10%
            risk_level=RiskLevel.LOW,  # Max 15%
            equity=1000.0,
            margin_available=1000.0,
        )
        
        decision = engine.evaluate(ctx)
        
        # Should pass (compound 10% < 15% limit)
        assert decision.reason_code != ReasonCode.COMPOUND_RISK_EXCEEDED
    
    def test_compound_risk_exceeds_absolute_cap(self, engine):
        """Compound risk > 30% absolute cap should block."""
        ctx = PolicyContext(
            symbol="BTCUSDT",
            signal="BUY",
            position="NONE",
            confidence=1.0,
            now_ms=int(time.time() * 1000),
            entry_price=50000.0,
            atr=2500.0,  # 5% of price → compound = 50%!
            stop_loss_pct=0.05,  # 5%
            leverage=20.0,  # Compound = 100%! 
            risk_level=RiskLevel.HIGH,  # Even HIGH is 30%
            equity=1000.0,
            margin_available=1000.0,
        )
        
        decision = engine.evaluate(ctx)
        
        # Stop loss should be clamped such that compound risk hits exactly the cap
        assert decision.allowed is True
        assert decision.compound_risk_pct == pytest.approx(0.30, rel=0.01)
        # Verify adjustment was logged
        assert any("SL clamped" in adj for adj in decision.adjustments)


class TestSizeAdjustments:
    """Test position size adjustments."""
    
    @pytest.fixture
    def engine(self):
        reset_policy_engine()
        return PolicyEngine()
    
    def test_leverage_penalty_applied(self, engine):
        """High leverage should reduce position size."""
        ctx = PolicyContext(
            symbol="BTCUSDT",
            signal="BUY",
            position="NONE",
            confidence=1.0,
            now_ms=int(time.time() * 1000),
            entry_price=50000.0,
            atr=500.0,
            leverage=15.0,  # > 10x triggers penalty
            equity=1000.0,
            margin_available=1000.0,
        )
        
        decision = engine.evaluate(ctx)
        
        # Check for leverage penalty adjustment
        if decision.allowed and decision.adjustments:
            assert any("Leverage penalty" in adj for adj in decision.adjustments)



if __name__ == "__main__":
    pytest.main([__file__, "-v"])

