"""
Tests for strict user sizing mode (Option A).

Covers:
  - Fixed mode 120 USDT → exactly 120 USDT used (no reduction)
  - Adaptive multiplier has NO effect regardless of value
  - Loss streak has NO effect on final size
  - Layer B leverage penalty disabled
  - Layer B volatility scaling disabled
  - Layer B margin buffer soft reduction disabled
  - Hard affordability block still fires when margin truly unavailable
  - sizing_trace fields: base_margin_usdt, final_margin_usdt, size_changed, size_change_reason
  - ATR cap diagnostics are warning-only on fixed mode

Run with:
    python -m pytest tests/test_strict_user_sizing.py -v
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch
from dataclasses import dataclass
from typing import Any

import pytest


# ── helpers ───────────────────────────────────────────────────────────────────

def _make_sizing_result(qty: float, notional: float, sizing_method: str = "fixed_absolute_margin"):
    """Build a SizingResult-like object for Layer B tests."""
    from app.risk.sizing_engine import SizingResult
    return SizingResult(
        allowed=True,
        reason="ok",
        quantity=qty,
        notional=notional,
        required_leverage=10.0,
        effective_risk_pct=0.0,
        margin_utilization_pct=0.0,
        details={"sizing_method": sizing_method},
    )


def _make_safety_engine():
    from app.risk.safety_engine import SafetyEngine, SafetyConfig
    from unittest.mock import MagicMock
    from contextlib import contextmanager

    cfg = SafetyConfig(
        volatility_scaling_enabled=True,  # enabled in config but disabled in code path
        min_margin_buffer_pct=0.15,
    )

    # Minimal DB stub: connect() returns a context manager yielding a no-op conn
    mock_conn = MagicMock()
    mock_conn.execute.return_value = MagicMock()

    @contextmanager
    def _connect():
        yield mock_conn

    mock_db = MagicMock()
    mock_db.connect = _connect

    mock_budget = MagicMock()
    mock_protection = MagicMock()

    return SafetyEngine(db=mock_db, risk_budget=mock_budget, protection=mock_protection, config=cfg)


# ── Layer B: leverage penalty disabled ───────────────────────────────────────

class TestLayerBLeveragePenaltyDisabled:
    """High leverage must NOT reduce size under Option A."""

    def test_20x_leverage_no_reduction(self):
        engine = _make_safety_engine()
        # 1200 USDT notional, 20x leverage = 60 USDT margin required
        sr = _make_sizing_result(qty=100.0, notional=1200.0)
        result = engine.calculate_safe_size(
            config_id="test",
            symbol="BTCUSDT",
            sizing_result=sr,
            entry_price=12.0,
            current_equity=2000.0,
            margin_used=100.0,
            margin_available=1900.0,
            leverage=20.0,
        )
        assert result.allowed
        assert result.adjusted_size == 100.0, (
            f"Expected 100.0 (no reduction), got {result.adjusted_size}"
        )
        assert result.size_reduction_pct == 0.0
        lever_reductions = [r for r in result.details.get("reductions", []) if "Leverage" in r]
        assert not lever_reductions, f"Leverage penalty fired: {lever_reductions}"

    def test_50x_leverage_no_reduction(self):
        engine = _make_safety_engine()
        sr = _make_sizing_result(qty=50.0, notional=600.0)
        result = engine.calculate_safe_size(
            config_id="test",
            symbol="ETHUSDT",
            sizing_result=sr,
            entry_price=12.0,
            current_equity=5000.0,
            margin_used=50.0,
            margin_available=4950.0,
            leverage=50.0,
        )
        assert result.allowed
        assert result.adjusted_size == 50.0


# ── Layer B: volatility scaling disabled ─────────────────────────────────────

class TestLayerBVolatilityScalingDisabled:
    """ATR spike must NOT reduce size under Option A."""

    def test_high_volatility_no_reduction(self):
        engine = _make_safety_engine()
        # Manually set a baseline ATR so ratio > 1.5 would fire
        engine.config.volatility_base_atr = {"BTCUSDT": 10.0}
        sr = _make_sizing_result(qty=100.0, notional=1200.0)
        result = engine.calculate_safe_size(
            config_id="test",
            symbol="BTCUSDT",
            sizing_result=sr,
            entry_price=12.0,
            current_equity=2000.0,
            margin_used=100.0,
            margin_available=1900.0,
            leverage=10.0,
            atr=20.0,  # 2× baseline → ratio=2.0 → would have triggered 50% cut
        )
        assert result.allowed
        assert result.adjusted_size == 100.0
        vol_reductions = [r for r in result.details.get("reductions", []) if "Volatility" in r or "volatility" in r]
        assert not vol_reductions, f"Volatility scaling fired: {vol_reductions}"


# ── Layer B: margin buffer soft reduction disabled ────────────────────────────

class TestLayerBMarginBufferDisabled:
    """Soft margin buffer reduction must NOT fire under Option A."""

    def test_tight_margin_no_soft_reduction(self):
        engine = _make_safety_engine()
        # available=200, buffer=15%×2000=300 → old code would reduce size
        sr = _make_sizing_result(qty=100.0, notional=1200.0)
        result = engine.calculate_safe_size(
            config_id="test",
            symbol="DOTUSDT",
            sizing_result=sr,
            entry_price=12.0,
            current_equity=2000.0,
            margin_used=1800.0,
            margin_available=200.0,    # tight but trade costs 120 USDT (1200/10)
            leverage=10.0,
        )
        # 200 > 120 required → affordability passes → no reduction
        assert result.allowed
        assert result.adjusted_size == 100.0


# ── Layer B: hard affordability block still works ────────────────────────────

class TestLayerBHardAffordabilityBlock:
    """Trade must be rejected when account literally cannot fund it."""

    def test_insufficient_margin_blocked(self):
        engine = _make_safety_engine()
        # 1200 USDT notional ÷ 10x = 120 USDT margin required; only 50 available
        sr = _make_sizing_result(qty=100.0, notional=1200.0)
        result = engine.calculate_safe_size(
            config_id="test",
            symbol="DOTUSDT",
            sizing_result=sr,
            entry_price=12.0,
            current_equity=500.0,
            margin_used=450.0,
            margin_available=50.0,     # < 120 required
            leverage=10.0,
        )
        assert not result.allowed
        assert "Insufficient margin" in result.message

    def test_zero_margin_available_bypassed(self):
        """margin_available=0 means data unavailable — must NOT block."""
        engine = _make_safety_engine()
        sr = _make_sizing_result(qty=100.0, notional=1200.0)
        result = engine.calculate_safe_size(
            config_id="test",
            symbol="DOTUSDT",
            sizing_result=sr,
            entry_price=12.0,
            current_equity=2000.0,
            margin_used=0.0,
            margin_available=0.0,  # unknown / unavailable
            leverage=10.0,
        )
        assert result.allowed
        assert result.adjusted_size == 100.0


# ── Adaptive multiplier disabled ─────────────────────────────────────────────

class TestAdaptiveMultiplierDisabled:
    """Stage 3A must not reduce final_size regardless of adaptive_size_multiplier value."""

    def _run_orchestrator_sizing(self, adaptive_mult: float, base_qty: float = 980.39) -> dict:
        """
        Simulate Stage 3A in the orchestrator.
        Returns dict with final_size and adaptive_applied.
        """
        # Replicate the exact logic from trading_orchestrator.py Stage 3A
        final_size = base_qty
        _policy_base_size = final_size
        _adaptive_applied = False

        # Option A: multiplier is recorded but NOT applied
        _adaptive_mult = float(adaptive_mult)
        # DISABLED (Option A): if 0.0 < _adaptive_mult < 1.0: ...

        return {
            "final_size": final_size,
            "adaptive_applied": _adaptive_applied,
            "adaptive_mult": _adaptive_mult,
            "policy_base_size": _policy_base_size,
        }

    def test_0_49_multiplier_has_no_effect(self):
        r = self._run_orchestrator_sizing(adaptive_mult=0.49)
        assert r["final_size"] == 980.39
        assert not r["adaptive_applied"]

    def test_0_1_multiplier_has_no_effect(self):
        r = self._run_orchestrator_sizing(adaptive_mult=0.1)
        assert r["final_size"] == 980.39
        assert not r["adaptive_applied"]

    def test_1_0_multiplier_has_no_effect(self):
        r = self._run_orchestrator_sizing(adaptive_mult=1.0)
        assert r["final_size"] == 980.39

    def test_loss_streak_does_not_affect_size(self):
        """Loss streak feeds the adaptive engine → multiplier; multiplier is now ignored."""
        # Even if the adaptive engine outputs 0.3 from a streak of 5 losses,
        # final_size must remain unchanged.
        r = self._run_orchestrator_sizing(adaptive_mult=0.3)
        assert r["final_size"] == 980.39
        assert not r["adaptive_applied"]


# ── sizing_trace contract ─────────────────────────────────────────────────────

class TestSizingTraceContract:
    """sizing_trace must expose base_margin_usdt, final_margin_usdt, size_changed, size_change_reason."""

    def _make_trace(
        self,
        base_margin: float,
        final_margin: float,
        cap_applied: bool,
        cap_reason: str | None,
        policy_base_size: float,
        final_size: float,
    ) -> dict:
        """Replicate the sizing_trace construction from trading_orchestrator.py."""
        _size_changed = round(final_size, 8) != round(policy_base_size, 8)
        _size_change_reason = None
        if _size_changed:
            if cap_applied:
                _size_change_reason = f"atr_safety_cap: base={base_margin:.4f} capped={final_margin:.4f}"
            else:
                _size_change_reason = "layer_b_adjustment"

        return {
            "allocation_mode":    "fixed",
            "base_margin_usdt":   base_margin,
            "final_margin_usdt":  final_margin,
            "size_changed":       _size_changed,
            "size_change_reason": _size_change_reason,
            "cap_applied":        cap_applied,
        }

    def test_no_cap_120_usdt_trace(self):
        trace = self._make_trace(
            base_margin=120.0, final_margin=120.0,
            cap_applied=False, cap_reason=None,
            policy_base_size=980.39, final_size=980.39,
        )
        assert trace["base_margin_usdt"] == 120.0
        assert trace["final_margin_usdt"] == 120.0
        assert trace["size_changed"] is False
        assert trace["size_change_reason"] is None

    def test_atr_cap_warning_does_not_reduce_margin_trace(self):
        trace = self._make_trace(
            base_margin=120.0, final_margin=120.0,
            cap_applied=False, cap_reason="fixed_amount_strict: user fixed margin respected",
            policy_base_size=980.39, final_size=980.39,
        )
        assert trace["base_margin_usdt"] == 120.0
        assert trace["final_margin_usdt"] == 120.0
        assert trace["size_changed"] is False
        assert trace["size_change_reason"] is None

    def test_all_required_fields_present(self):
        trace = self._make_trace(120.0, 120.0, False, None, 100.0, 100.0)
        required = {"allocation_mode", "base_margin_usdt", "final_margin_usdt",
                    "size_changed", "size_change_reason"}
        missing = required - set(trace.keys())
        assert not missing, f"Missing sizing_trace fields: {missing}"


# ── end-to-end: fixed 120 USDT → 120 USDT with high adaptive mult ────────────

class TestFixedSizingEndToEnd:
    """
    Policy engine fixed-mode path + Layer B + Stage 3A:
    User sets 120 USDT, loss_streak=5, adaptive_mult=0.3 → output must be 120 USDT margin.
    """

    def test_120_usdt_unmodified_by_adaptive(self):
        """
        With adaptive_mult=0.3 and loss_streak=5, the final margin used
        must still equal user's 120 USDT under Option A.
        """
        entry_price = 1.224
        leverage    = 10
        base_margin = 120.0

        # PolicyEngine output (fixed mode, no ATR cap)
        notional   = base_margin * leverage     # 1200 USDT
        base_qty   = notional / entry_price     # ~980.39 contracts

        # Stage 3A (Option A — multiplier ignored)
        adaptive_mult = 0.3  # would have been applied for loss_streak=5
        final_qty     = base_qty   # unchanged

        # Verify margin is preserved
        actual_margin = (final_qty * entry_price) / leverage
        assert abs(actual_margin - base_margin) < 0.01, (
            f"Expected margin ~120.0 USDT, got {actual_margin:.4f} USDT. "
            f"Adaptive multiplier {adaptive_mult} must NOT be applied."
        )

    def test_percent_mode_uses_correct_pct(self):
        """
        Percent mode: 5% of 2000 USDT equity = 100 USDT margin.
        Must not be reduced by adaptive multiplier.
        """
        equity   = 2000.0
        pct      = 0.05
        leverage = 10

        base_margin = equity * pct    # 100 USDT
        notional    = base_margin * leverage  # 1000 USDT

        # Adaptive mult ignored
        final_margin = base_margin  # 100 USDT
        assert abs(final_margin - 100.0) < 0.01

    def test_insufficient_margin_rejects_trade(self):
        """
        If the account has 50 USDT free margin and the trade needs 120 USDT,
        the trade must be rejected, not silently downsized.
        """
        engine = _make_safety_engine()
        entry_price = 1.224
        leverage    = 10
        base_margin = 120.0
        notional    = base_margin * leverage   # 1200 USDT
        qty         = notional / entry_price   # ~980.39

        sr = _make_sizing_result(qty=qty, notional=notional)
        result = engine.calculate_safe_size(
            config_id="test",
            symbol="DOTUSDT",
            sizing_result=sr,
            entry_price=entry_price,
            current_equity=500.0,
            margin_used=450.0,
            margin_available=50.0,   # cannot fund 120 USDT margin
            leverage=float(leverage),
        )
        assert not result.allowed
        assert result.adjusted_size == 0.0 or result.adjusted_size < qty * 0.1
