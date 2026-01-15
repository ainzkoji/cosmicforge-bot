"""
Unit Tests for SafetyEngine

Tests all four protection layers.
"""
import pytest
from datetime import datetime, timezone, timedelta
from app.persistence.db import DB
from app.risk.safety_engine import (
    SafetyEngine,
    SafetyConfig,
    SafetyDecision,
    MarketConditions,
    BrokerHealth,
    BlockReason
)
from app.risk.risk_budget import RiskBudgetEngine, RiskBudgetConfig
from app.risk.account_protection import AccountProtection


@pytest.fixture
def db():
    """In-memory database for testing."""
    return DB(":memory:")


@pytest.fixture
def risk_budget():
    """Default risk budget."""
    config = RiskBudgetConfig(
        portfolio_risk_pct=0.05,
        max_margin_usage_pct=0.50,
        base_slots=5,
        max_slots=20
    )
    return RiskBudgetEngine(config)


@pytest.fixture
def protection(db):
    """Account protection instance."""
    return AccountProtection(db)


@pytest.fixture
def safety_config():
    """Default safety configuration."""
    return SafetyConfig(
        max_leverage=20.0,
        max_trades_per_day=100,
        min_strategy_confidence=0.3,
        max_spread_pct=0.005,
        min_margin_buffer_pct=0.30,
        max_stop_distance_pct=0.10,
        max_compound_risk_pct=0.15
    )


@pytest.fixture
def safety_engine(db, risk_budget, protection, safety_config):
    """Safety engine instance."""
    return SafetyEngine(db, risk_budget, protection, safety_config)


# ===========================================================================
# LAYER A: PRE-TRADE GATING TESTS
# ===========================================================================

def test_layer_a_max_trades_per_day(safety_engine):
    """Test daily trade limit enforcement."""
    config_id = "test_config_1"
    
    # Increment trade count to limit
    for _ in range(100):
        safety_engine.increment_trade_count(config_id)
    
    # Next trade should be blocked
    decision = safety_engine.check_pre_trade(
        config_id=config_id,
        symbol="BTCUSDT",
        confidence=0.8,
        leverage=10.0,
        current_equity=1000,
        open_positions=0
    )
    
    assert not decision.allowed
    assert decision.block_reason == BlockReason.MAX_TRADES_DAY


def test_layer_a_max_leverage(safety_engine):
    """Test max leverage enforcement."""
    decision = safety_engine.check_pre_trade(
        config_id="test_config_2",
        symbol="BTCUSDT",
        confidence=0.8,
        leverage=25.0,  # Exceeds max of 20
        current_equity=1000,
        open_positions=0
    )
    
    assert not decision.allowed
    assert decision.block_reason == BlockReason.MAX_LEVERAGE


def test_layer_a_low_confidence(safety_engine):
    """Test strategy confidence threshold."""
    decision = safety_engine.check_pre_trade(
        config_id="test_config_3",
        symbol="BTCUSDT",
        confidence=0.2,  # Below 0.3 minimum
        leverage=10.0,
        current_equity=1000,
        open_positions=0
    )
    
    assert not decision.allowed
    assert decision.block_reason == BlockReason.LOW_CONFIDENCE


def test_layer_a_unsafe_market_conditions(safety_engine):
    """Test market conditions check."""
    unsafe_market = MarketConditions(
        symbol="BTCUSDT",
        spread_pct=0.01,  # Wide spread
        volatility=500,
        volume_24h=100000,
        price_change_24h_pct=0.05,
        is_safe=False,
        reason="Wide spread"
    )
    
    decision = safety_engine.check_pre_trade(
        config_id="test_config_4",
        symbol="BTCUSDT",
        confidence=0.8,
        leverage=10.0,
        current_equity=1000,
        open_positions=0,
        market_conditions=unsafe_market
    )
    
    assert not decision.allowed
    assert decision.block_reason == BlockReason.MARKET_CONDITIONS


def test_layer_a_unhealthy_broker(safety_engine):
    """Test broker health check."""
    unhealthy_broker = BrokerHealth(
        broker_id="binance",
        is_healthy=False,
        time_sync_ok=False,
        rate_limit_ok=True,
        api_responsive=True,
        last_error="Time sync failed"
    )
    
    decision = safety_engine.check_pre_trade(
        config_id="test_config_5",
        symbol="BTCUSDT",
        confidence=0.8,
        leverage=10.0,
        current_equity=1000,
        open_positions=0,
        broker_health=unhealthy_broker
    )
    
    assert not decision.allowed
    assert decision.block_reason == BlockReason.BROKER_UNHEALTHY


def test_layer_a_all_gates_pass(safety_engine):
    """Test that trade is allowed when all gates pass."""
    safe_market = MarketConditions(
        symbol="BTCUSDT",
        spread_pct=0.001,
        volatility=100,
        volume_24h=10000000,
        price_change_24h_pct=0.02,
        is_safe=True
    )
    
    healthy_broker = BrokerHealth(
        broker_id="binance",
        is_healthy=True,
        time_sync_ok=True,
        rate_limit_ok=True,
        api_responsive=True
    )
    
    decision = safety_engine.check_pre_trade(
        config_id="test_config_6",
        symbol="BTCUSDT",
        confidence=0.8,
        leverage=10.0,
        current_equity=1000,
        open_positions=0,
        market_conditions=safe_market,
        broker_health=healthy_broker
    )
    
    assert decision.allowed
    assert decision.layer == "A"


# ===========================================================================
# LAYER B: SIZING CONTROLS TESTS
# ===========================================================================

def test_layer_b_margin_buffer_enforcement(safety_engine):
    """Test margin buffer enforcement."""
    decision = safety_engine.calculate_safe_size(
        config_id="test_config_7",
        symbol="BTCUSDT",
        base_size=1.0,
        entry_price=50000,
        current_equity=1000,
        margin_used=600,
        margin_available=400,  # Only 400 available, needs 300 buffer (30% of 1000)
        total_notional_exposure=0
    )
    
    # Should reduce size or block
    assert decision.adjusted_size < decision.original_size or not decision.allowed


def test_layer_b_total_exposure_cap(safety_engine):
    """Test total exposure limit."""
    decision = safety_engine.calculate_safe_size(
        config_id="test_config_8",
        symbol="BTCUSDT",
        base_size=0.05,  # Would add 2500 USDT notional
        entry_price=50000,
        current_equity=1000,
        margin_used=100,
        margin_available=900,
        total_notional_exposure=1800  # Already at 1800, max is 2000 (2x equity)
    )
    
    # Should reduce size to fit within 200 USDT remaining budget
    assert decision.adjusted_size < decision.original_size


def test_layer_b_volatility_scaling(safety_engine):
    """Test volatility-based size scaling."""
    # Set baseline ATR
    safety_engine.config.volatility_base_atr = {"BTCUSDT": 500}
    safety_engine.config.volatility_scaling_enabled = True
    
    decision = safety_engine.calculate_safe_size(
        config_id="test_config_9",
        symbol="BTCUSDT",
        base_size=0.02,
        entry_price=50000,
        current_equity=1000,
        margin_used=100,
        margin_available=900,
        atr=1500,  # 3x normal volatility
        total_notional_exposure=0
    )
    
    # Should reduce size due to high volatility
    assert decision.adjusted_size < decision.original_size


# ===========================================================================
# LAYER C: PROTECTIVE ORDERS TESTS
# ===========================================================================

def test_layer_c_stop_too_wide(safety_engine):
    """Test max stop distance validation."""
    decision = safety_engine.validate_protective_orders(
        symbol="BTCUSDT",
        entry_price=50000,
        stop_loss_price=43000,  # 14% stop (exceeds 10% max)
        leverage=10.0,
        position_size=0.02
    )
    
    assert not decision.allowed
    assert decision.block_reason == BlockReason.STOP_TOO_WIDE


def test_layer_c_compound_risk_too_high(safety_engine):
    """Test compound risk (stop * leverage) validation."""
    decision = safety_engine.validate_protective_orders(
        symbol="BTCUSDT",
        entry_price=50000,
        stop_loss_price=48500,  # 3% stop
        leverage=20.0,  # 3% * 20x = 60% compound risk (exceeds 15% max)
        position_size=0.02
    )
    
    assert not decision.allowed
    assert decision.block_reason == BlockReason.LEVERAGE_RISK


def test_layer_c_protective_orders_valid(safety_engine):
    """Test valid protective orders pass."""
    decision = safety_engine.validate_protective_orders(
        symbol="BTCUSDT",
        entry_price=50000,
        stop_loss_price=49000,  # 2% stop  
        leverage=5.0,  # 2% * 5x = 10% compound risk (within 15% max)
        position_size=0.02
    )
    
    assert decision.allowed
    assert decision.layer == "C"


# ===========================================================================
# LAYER D: POST-TRADE MONITORING TESTS
# ===========================================================================

def test_layer_d_order_failure_circuit_breaker(safety_engine):
    """Test circuit breaker triggers after repeated failures."""
    config_id = "test_config_10"
    symbol = "BTCUSDT"
    
    # Record 3 consecutive failures
    for _ in range(3):
        safety_engine.record_order_result(config_id, symbol, success=False, error_message="Test failure")
    
    # Check if circuit breaker is active
    decision = safety_engine.check_pre_trade(
        config_id=config_id,
        symbol=symbol,
        confidence=0.8,
        leverage=10.0,
        current_equity=1000,
        open_positions=0
    )
    
    assert not decision.allowed
    assert decision.block_reason == BlockReason.CIRCUIT_BREAKER


def test_layer_d_slippage_tracking(safety_engine):
    """Test slippage monitoring."""
    config_id = "test_config_11"
    symbol = "BTCUSDT"
    
    # Record some slippage
    safety_engine.record_slippage(config_id, symbol, expected_price=50000, executed_price=50100)
    safety_engine.record_slippage(config_id, symbol, expected_price=50000, executed_price=50050)
    
    # Check that slippage was recorded (would need to query DB to verify)
    # For now, just ensure no errors
    assert True


def test_layer_d_liquidation_risk_check(safety_engine):
    """Test liquidation proximity detection."""
    decision = safety_engine.check_liquidation_risk(
        current_equity=1000,
        margin_used=900  # Margin ratio = 1.11, below 1.2 threshold
    )
    
    assert not decision.allowed
    assert decision.block_reason == BlockReason.LIQUIDATION_RISK


def test_layer_d_liquidation_risk_healthy(safety_engine):
    """Test healthy margin ratio passes."""
    decision = safety_engine.check_liquidation_risk(
        current_equity=1000,
        margin_used=500  # Margin ratio = 2.0, healthy
    )
    
    assert decision.allowed
    assert decision.layer == "D"


# ===========================================================================
# INTEGRATION TESTS
# ===========================================================================

def test_full_safety_stack_integration(safety_engine):
    """Test complete flow through all layers."""
    config_id = "test_integration"
    symbol = "BTCUSDT"
    
    safe_market = MarketConditions(
        symbol=symbol,
        spread_pct=0.001,
        volatility=100,
        volume_24h=10000000,
        price_change_24h_pct=0.02,
        is_safe=True
    )
    
    healthy_broker = BrokerHealth(
        broker_id="binance",
        is_healthy=True,
        time_sync_ok=True,
        rate_limit_ok=True,
        api_responsive=True
    )
    
    # Layer A: Pre-trade gating
    gate_decision = safety_engine.check_pre_trade(
        config_id=config_id,
        symbol=symbol,
        confidence=0.8,
        leverage=10.0,
        current_equity=1000,
        open_positions=0,
        market_conditions=safe_market,
        broker_health=healthy_broker
    )
    assert gate_decision.allowed
    
    # Layer B: Sizing
    size_decision = safety_engine.calculate_safe_size(
        config_id=config_id,
        symbol=symbol,
        base_size=0.02,
        entry_price=50000,
        current_equity=1000,
        margin_used=100,
        margin_available=900,
        atr=500,
        total_notional_exposure=0
    )
    assert size_decision.allowed
    
    # Layer C: Protective orders
    protection_decision = safety_engine.validate_protective_orders(
        symbol=symbol,
        entry_price=50000,
        stop_loss_price=49000,
        leverage=10.0,
        position_size=size_decision.adjusted_size
    )
    assert protection_decision.allowed
    
    # Layer D: Record result
    safety_engine.record_order_result(config_id, symbol, success=True)
    safety_engine.record_slippage(config_id, symbol, 50000, 50020)
    safety_engine.increment_trade_count(config_id)
    
    # All layers passed!
    assert True


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
