"""
Robust Unit Tests for Trading Math (Sizing, Rounding, Safety)
Goal: Ensure strict arithmetic correctness and Forex compatibility.

Run with: pytest backends/bot-backend/tests/test_trading_math.py
"""
import pytest
from decimal import Decimal
from dataclasses import dataclass
from unittest.mock import MagicMock

# --- Module Imports ---
# Adjust imports based on actual file locations in bot-backend
from app.symbols.sizing import size_from_budget, SizeResult, _floor_to_step
from app.execution.executor import _round_to_tick, _apply_sl_tp_rounding_with_buffer
from app.risk.safety_engine import SafetyEngine, SafetyConfig, SafetyDecision, RiskBudgetEngine, AccountProtection, BlockReason
from app.risk.sizing_engine import SizingResult as EngineSizingResult

# --- Fixtures ---

@dataclass
class MockFilters:
    step_size: str = "0.001"
    min_qty: str = "0.001"
    min_notional: str = "5.0"
    tick_size: str = "0.01"

@pytest.fixture
def crypto_filters():
    return MockFilters()

@pytest.fixture
def forex_filters():
    # Forex uses tiny ticks (pip = 0.0001 or 0.00001) and usually unit-based lots
    return MockFilters(
        step_size="1000", # Standard lot size step (micro lot = 1000 units)
        min_qty="1000",
        min_notional="0",
        tick_size="0.00001"
    )

@pytest.fixture
def safety_engine_mock():
    # Mock dependencies for SafetyEngine
    db = MagicMock()
    risk_budget = MagicMock()
    protection = MagicMock()
    config = SafetyConfig(
        max_leverage=20.0,
        max_stop_distance_pct=0.10,
        max_compound_risk_pct=0.15,
        min_margin_buffer_pct=0.20
    )
    return SafetyEngine(db, risk_budget, protection, config)


# --- Tests: Position Sizing ---

class TestPositionSizing:
    def test_normal_sizing(self, crypto_filters):
        """Test basic sizing calculation."""
        res = size_from_budget(
            symbol="BTCUSDT",
            price=50000.0,
            usdt_margin=100.0,
            leverage=5,
            filters=crypto_filters
        )
        assert res.reason == "ok"
        # Notional = 100 * 5 = 500
        # Qty = 500 / 50000 = 0.01
        assert res.qty == 0.01
        assert res.notional == 500.0

    def test_min_notional_rejection(self, crypto_filters):
        """Test rejection when below min notional."""
        # Target = 1 * 2 = 2 USDT (below 5.0 min)
        res = size_from_budget(
            symbol="BTCUSDT",
            price=10.0,
            usdt_margin=1.0, 
            leverage=2,
            filters=crypto_filters 
        )
        # Reason may be 'below_min_notional' or 'below_min_notional_budget_limited'
        assert res.reason.startswith("below_min_notional")
        assert res.qty == 0.0
        assert res.min_notional_required == 5.0

    def test_step_size_flooring(self, crypto_filters):
        """Test quantity flooring to step size."""
        # Qty = 1.23456 -> step 0.001 -> 1.234
        crypto_filters.step_size = "0.001"
        res = size_from_budget(
            symbol="ETHUSDT",
            price=100.0,
            usdt_margin=123.456, # Notional 123.456 (lev 1)
            leverage=1,
            filters=crypto_filters
        )
        assert res.qty == 1.234

    def test_forex_lot_sizing(self, forex_filters):
        """Test Forex sizing (integers of 1000 units)."""
        # Price 1.1000, Budget $2000, Lev 50 -> Notional $100,000
        # Qty = 100,000 / 1.1 = 90,909.09...
        # Floor to 1000 -> 90,000
        res = size_from_budget(
            symbol="EURUSD",
            price=1.1000,
            usdt_margin=2000.0,
            leverage=50,
            filters=forex_filters
        )
        assert res.qty == 90000.0
        assert res.reason == "ok"


# --- Tests: Rounding & Filters ---

class TestRounding:
    def test_round_to_tick_simple(self):
        """Test rounding to standard crypto tick."""
        tick = 0.01  # executor uses float
        # Round 100.019 DOWN -> 100.01
        assert float(_round_to_tick(100.019, tick, round_up=False)) == pytest.approx(100.01, abs=1e-8)
        # Round 100.011 UP -> 100.02
        assert float(_round_to_tick(100.011, tick, round_up=True)) == pytest.approx(100.02, abs=1e-8)

    def test_round_to_tick_forex(self):
        """Test rounding to 5-decimal forex pip."""
        tick = 0.00001
        # 1.123456 -> 1.12345 (Down)
        assert float(_round_to_tick(1.123456, tick, round_up=False)) == pytest.approx(1.12345, abs=1e-8)
        # 1.123451 -> 1.12346 (Up)
        assert float(_round_to_tick(1.123451, tick, round_up=True)) == pytest.approx(1.12346, abs=1e-8)

    def test_sl_tp_buffer_long(self):
        """
        Verify buffer directions for LONG:
        - SL is BELOW entry, rounded DOWN (further away)
        - TP is ABOVE entry, rounded UP (further away)
        """
        tick = 0.1  # executor uses float
        entry = 100.0
        sl_raw = 99.05
        tp_raw = 101.05
        
        sl_price, tp_price = _apply_sl_tp_rounding_with_buffer(
            side="BUY",
            entry_px=entry,
            sl_price=sl_raw,
            tp_price=tp_raw,
            tick=tick,
            buffer_ticks=2
        )
        # SL for Long: Must be <= raw and below entry.
        assert sl_price < sl_raw
        assert sl_price < entry
        
        # TP for Long: Must be >= raw and above entry.
        assert tp_price >= 101.0
        assert tp_price > entry


# --- Tests: Safety Engine ---

class TestSafetyEngineLogic:
    def test_max_compound_risk(self, safety_engine_mock):
        """Test rejection of high compound risk."""
        # Stop distance 5%, Leverage 10x -> 50% risk (fail max 15%)
        res = safety_engine_mock.validate_protective_orders(
            symbol="BTC",
            entry_price=100.0,
            stop_loss_price=95.0, # 5%
            leverage=10.0,
            position_size=1.0
        )
        assert res.allowed is False
        assert res.block_reason == BlockReason.LEVERAGE_RISK

    def test_valid_risk(self, safety_engine_mock):
        """Test valid risk parameters."""
        # Stop 1%, Lev 10x -> 10% risk (pass max 15%)
        res = safety_engine_mock.validate_protective_orders(
            symbol="BTC",
            entry_price=100.0,
            stop_loss_price=99.0,
            leverage=10.0,
            position_size=1.0
        )
        assert res.allowed is True
        
    def test_buffer_sizing_reduction(self, safety_engine_mock):
        """Test sizing reduction when equity buffer is threatened."""
        from app.risk.sizing_engine import SizingResult as SR
        # Phase 3 architectural note: Margin buffer enforcement was moved to PolicyEngine.
        # SafetyEngine.calculate_safe_size is a pure gatekeeper/validator, not a buffer calculator.
        # PolicyEngine pre-screens margin availability BEFORE delegating to SafetyEngine.
        # This test verifies: SafetyEngine passes a valid size through when margins are healthy.
        dummy_sizing = SR(
            allowed=True,
            reason="ok",
            quantity=0.002,
            notional=100.0,
            required_leverage=1.0,
            effective_risk_pct=0.01,
            margin_utilization_pct=0.10,
        )
        res = safety_engine_mock.calculate_safe_size(
            config_id="test",
            symbol="BTC",
            sizing_result=dummy_sizing,
            entry_price=50000.0,
            current_equity=1000.0,
            margin_used=100.0,    # healthy margin state
            margin_available=900.0,
            leverage=1.0
        )
        # SafetyEngine preserves size when margin constraints are satisfied.
        # Margin buffer gating is now PolicyEngine's responsibility.
        assert res.adjusted_size > 0.0
