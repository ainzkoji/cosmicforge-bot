"""
Stage 3 / SEV-1 fixed amount sizing contract tests.

fixed_amount/fixed/fixed_usdt now means USER_FIXED_MARGIN_STRICT:
the user's configured amount is margin and cannot be reduced by ATR,
policy risk caps, volatility caps, or internal risk percentages.
"""

from __future__ import annotations

import os
from typing import Dict, Optional
from unittest.mock import patch

import pytest

from app.policy.policy_engine import PolicyContext, PolicyEngine


def _make_ctx(
    *,
    mode: str = "fixed",
    amount: float = 120.0,
    equity: float = 1_580.81315255,
    leverage: float = 7.0,
    atr: float = 0.0,
    entry_price: float = 1.0,
    account_risk_pct: float = 0.4,
    symbol: str = "BTCUSDT",
) -> PolicyContext:
    return PolicyContext(
        symbol=symbol,
        signal="BUY",
        confidence=0.75,
        position="NONE",
        entry_price=entry_price,
        atr=atr,
        equity=equity,
        margin_available=equity,
        margin_used=0.0,
        open_positions_count=0,
        leverage=leverage,
        account_risk_pct=account_risk_pct,
        max_leverage=20.0,
        min_notional=5.0,
        max_notional=equity * 20.0,
        trade_amount_mode=mode,
        trade_amount_value=amount,
        now_ms=0,
        max_daily_loss=equity,
        max_daily_trades=9999,
        max_open_positions=10,
    )


def _eval(ctx: PolicyContext, env: Optional[Dict[str, str]] = None):
    engine = PolicyEngine(sl_multiplier=2.0)
    with patch.dict(os.environ, env or {}):
        decision = engine.evaluate(ctx)
    return decision, decision.details


def _atr_for_stop(entry_price: float, stop_distance_pct: float) -> float:
    return entry_price * (stop_distance_pct / 100.0) / 2.0


class TestFixedAmountStrict:
    def test_ena_fixed_amount_strict_uses_user_margin(self):
        ctx = _make_ctx(
            symbol="ENAUSDT",
            mode="fixed_amount",
            amount=120.0,
            leverage=7.0,
            entry_price=0.1120,
            atr=_atr_for_stop(0.1120, 1.9677),
            account_risk_pct=0.4,
        )
        decision, details = _eval(ctx, {"ATR_SAFETY_CAP_ENABLED": "true"})

        assert decision.allowed, decision.reason
        assert details["sizing_method"] == "fixed_amount_strict"
        assert details["allocation_type"] == "fixed_amount"
        assert details["user_fixed_margin_usdt"] == pytest.approx(120.0)
        assert details["final_margin_usdt"] == pytest.approx(120.0)
        assert details["final_notional_usdt"] == pytest.approx(840.0)
        assert decision.quantity == pytest.approx(7_500.0, rel=1e-6)
        assert details["calculated_qty"] == pytest.approx(7_500.0, rel=1e-6)
        assert details["cap_applied"] is False
        assert details["cap_reason"] == "fixed_amount_strict: user fixed margin respected"

    def test_raysol_fixed_amount_strict_uses_user_margin(self):
        ctx = _make_ctx(
            symbol="RAYSOLUSDT",
            mode="fixed_amount",
            amount=120.0,
            leverage=7.0,
            entry_price=0.8133,
            atr=_atr_for_stop(0.8133, 1.0605),
            account_risk_pct=0.4,
        )
        decision, details = _eval(ctx, {"ATR_SAFETY_CAP_ENABLED": "true"})

        assert decision.allowed, decision.reason
        assert details["final_margin_usdt"] == pytest.approx(120.0)
        assert details["final_notional_usdt"] == pytest.approx(840.0)
        assert decision.quantity == pytest.approx(840.0 / 0.8133, rel=1e-6)
        assert details["cap_applied"] is False

    def test_risk_diagnostics_are_warning_only(self):
        ctx = _make_ctx(
            symbol="ENAUSDT",
            mode="fixed_amount",
            amount=120.0,
            leverage=7.0,
            entry_price=0.1120,
            atr=_atr_for_stop(0.1120, 1.9677),
            account_risk_pct=0.4,
        )
        decision, details = _eval(ctx, {"ATR_SAFETY_CAP_ENABLED": "true"})

        assert decision.allowed
        assert details["atr_cap_margin_usdt"] is not None
        assert details["atr_cap_margin_usdt"] < 120.0
        assert details["theoretical_risk_usdt"] == pytest.approx(840.0 * 0.019677, rel=1e-4)
        assert details["theoretical_risk_pct"] > details["account_risk_pct"]
        assert details["risk_warning"] is True
        assert details["final_margin_usdt"] == pytest.approx(120.0)
        assert "Risk cap was NOT applied" in details["admin_message"]

    def test_fixed_aliases_are_also_strict(self):
        for mode in ("fixed", "fixed_usdt", "fixed_amount"):
            ctx = _make_ctx(
                mode=mode,
                amount=120.0,
                leverage=7.0,
                entry_price=0.8133,
                atr=_atr_for_stop(0.8133, 10.0),
            )
            decision, details = _eval(ctx, {"ATR_SAFETY_CAP_ENABLED": "true"})

            assert decision.allowed
            assert details["sizing_method"] == "fixed_amount_strict"
            assert details["final_margin_usdt"] == pytest.approx(120.0)
            assert details["final_notional_usdt"] == pytest.approx(840.0)
            assert details["cap_applied"] is False


class TestNonFixedModesUnchanged:
    def test_percent_mode_does_not_use_fixed_amount_strict_path(self):
        ctx = _make_ctx(
            mode="percent",
            amount=5.0,
            equity=1000.0,
            leverage=5.0,
            entry_price=50_000.0,
            atr=5_000.0,
        )
        decision, details = _eval(ctx, {"ATR_SAFETY_CAP_ENABLED": "true"})

        assert decision.allowed
        assert details.get("sizing_method") != "fixed_amount_strict"
        assert details.get("cap_applied") is not True

    def test_atr_risk_mode_does_not_use_fixed_amount_strict_path(self):
        ctx = _make_ctx(
            mode="atr_risk",
            amount=0.0,
            equity=1000.0,
            leverage=5.0,
            entry_price=50_000.0,
            atr=500.0,
        )
        decision, details = _eval(ctx, {"ATR_SAFETY_CAP_ENABLED": "true"})

        assert decision.allowed
        assert details.get("sizing_method") != "fixed_amount_strict"
        assert details.get("cap_applied") is not True


class TestSizingTraceFields:
    def test_fixed_amount_strict_details_include_required_fields(self):
        ctx = _make_ctx(
            mode="fixed_amount",
            amount=120.0,
            leverage=7.0,
            entry_price=0.1120,
            atr=_atr_for_stop(0.1120, 1.9677),
        )
        decision, details = _eval(ctx)

        assert decision.allowed
        required_fields = {
            "allocation_type",
            "sizing_method",
            "user_fixed_margin_usdt",
            "final_margin_usdt",
            "leverage",
            "final_notional_usdt",
            "entry_price",
            "calculated_qty",
            "rounded_qty",
            "cap_applied",
            "cap_reason",
            "atr_cap_margin_usdt",
            "stop_distance_pct",
            "theoretical_risk_usdt",
            "theoretical_risk_pct",
            "account_risk_pct",
            "risk_warning",
        }
        missing = required_fields - set(details)
        assert not missing, f"Missing sizing fields: {sorted(missing)}"
