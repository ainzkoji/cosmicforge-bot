"""
CosmicForge Section D Risk Management Tests

D-1: Consecutive loss pause (soft 2h + hard rest-of-day)
D-2: Minimum risk/reward enforcement before every entry
D-3: ATR fixed-sizing noise floor + risk cap
"""
from __future__ import annotations

import time
import pytest
from datetime import date, timedelta
from unittest.mock import MagicMock


# ── helpers ─────────────────────────────────────────────────────────────────

def _now_ms() -> int:
    return int(time.time() * 1000)


def _ctx(**kw):
    from app.policy.policy_engine import PolicyContext
    defaults = dict(
        symbol="BTCUSDT", signal="BUY", confidence=0.80,
        position="NONE", adds=0, last_trade_ms=0, last_stop_ms=0,
        equity=10000.0, daily_realized_pnl=0.0, daily_trade_count=0,
        open_positions_count=0, leverage=3.0, stop_loss_pct=0.02,
        take_profit_pct=0.03, cooldown_seconds=0, sl_cooldown_seconds=0,
        max_adds=0, trade_mode="normal", max_daily_loss=50.0,
        max_daily_trades=6, max_open_positions=3, kill_switch=False,
        execution_mode="paper", now_ms=_now_ms(), entry_price=50000.0, atr=500.0,
        weekly_drawdown_pct=0.0, monthly_drawdown_pct=0.0,
        max_weekly_drawdown_pct=5.0, max_monthly_drawdown_pct=10.0,
        consecutive_losses=0, max_consecutive_losses=3,
        consec_loss_cooldown_until_ms=0, consec_loss_day_paused=False,
        stop_loss_price=0.0, take_profit_price=0.0, min_risk_reward=0.0,
        min_stop_atr_multiplier=0.5, max_risk_per_trade_pct=1.0,
    )
    defaults.update(kw)
    return PolicyContext(**defaults)


# ═══════════════════════════════════════════════════════════════════════════
# D-1: Consecutive Loss Pause
# ═══════════════════════════════════════════════════════════════════════════

class TestDailyLossState:
    """Tests for the DailyLossState consecutive loss tracking."""

    def _fresh_state(self):
        from app.risk.daily_loss import DailyLossState
        return DailyLossState(day=date.today())

    def test_loss_increments_counter(self):
        state = self._fresh_state()
        state.record_trade_result(is_win=False, soft_limit=3, hard_limit=5,
                                   cooldown_minutes=120, now_ms=_now_ms())
        assert state.consecutive_losses == 1

    def test_win_resets_counter(self):
        state = self._fresh_state()
        state.consecutive_losses = 2
        state.record_trade_result(is_win=True, soft_limit=3, hard_limit=5,
                                   cooldown_minutes=120, now_ms=_now_ms())
        assert state.consecutive_losses == 0

    def test_daily_reset_clears_counter(self):
        from app.risk.daily_loss import DailyLossState
        state = DailyLossState(day=date.today() - timedelta(days=1))
        state.consecutive_losses = 4
        state.consec_loss_cooldown_until_ms = _now_ms() + 999999
        state.consec_loss_day_paused = True
        state.reset_if_new_day()
        assert state.consecutive_losses == 0
        assert state.consec_loss_cooldown_until_ms == 0
        assert state.consec_loss_day_paused is False

    def test_three_losses_trigger_soft_pause(self):
        state = self._fresh_state()
        now = _now_ms()
        for _ in range(3):
            state.record_trade_result(is_win=False, soft_limit=3, hard_limit=5,
                                       cooldown_minutes=120, now_ms=now)
        assert state.consec_loss_cooldown_until_ms > now
        assert state.consec_loss_day_paused is False

    def test_five_losses_trigger_hard_day_pause(self):
        state = self._fresh_state()
        now = _now_ms()
        for _ in range(5):
            state.record_trade_result(is_win=False, soft_limit=3, hard_limit=5,
                                       cooldown_minutes=120, now_ms=now)
        assert state.consec_loss_day_paused is True

    def test_is_entry_paused_during_soft_cooldown(self):
        state = self._fresh_state()
        state.consec_loss_cooldown_until_ms = _now_ms() + 7_200_000  # 2h from now
        paused, reason = state.is_entry_paused()
        assert paused is True
        assert reason == "TRADE_REJECTED_CONSECUTIVE_LOSS_COOLDOWN"

    def test_is_entry_paused_day_pause(self):
        state = self._fresh_state()
        state.consec_loss_day_paused = True
        paused, reason = state.is_entry_paused()
        assert paused is True
        assert reason == "TRADE_REJECTED_CONSECUTIVE_LOSS_DAY_PAUSE"

    def test_cooldown_expires_entry_resumes(self):
        state = self._fresh_state()
        # Cooldown already expired (in the past)
        state.consec_loss_cooldown_until_ms = _now_ms() - 1000
        paused, reason = state.is_entry_paused()
        assert paused is False

    def test_hard_day_pause_not_reset_by_win(self):
        """A win during a day pause does NOT lift the hard pause — only daily reset does."""
        state = self._fresh_state()
        state.consec_loss_day_paused = True
        state.record_trade_result(is_win=True, soft_limit=3, hard_limit=5,
                                   cooldown_minutes=120, now_ms=_now_ms())
        # Day pause must persist even after a win
        assert state.consec_loss_day_paused is True


class TestConsecutiveLossPolicyEngine:
    """Tests for policy engine enforcement of consecutive loss pause."""

    def test_day_pause_blocks_new_entry(self):
        from app.policy.policy_engine import PolicyEngine, ReasonCode
        engine = PolicyEngine(min_confidence=0.10)
        ctx = _ctx(consec_loss_day_paused=True)
        d = engine.evaluate(ctx)
        assert not d.allowed
        assert d.reason_code == ReasonCode.CONSECUTIVE_LOSS_DAY_PAUSE
        assert "TRADE_REJECTED_CONSECUTIVE_LOSS_DAY_PAUSE" in d.reason

    def test_soft_cooldown_blocks_new_entry(self):
        from app.policy.policy_engine import PolicyEngine, ReasonCode
        engine = PolicyEngine(min_confidence=0.10)
        future_ms = _now_ms() + 7_200_000
        ctx = _ctx(
            consec_loss_cooldown_until_ms=future_ms,
            consec_loss_day_paused=False,
            now_ms=_now_ms(),
        )
        d = engine.evaluate(ctx)
        assert not d.allowed
        assert d.reason_code == ReasonCode.CONSECUTIVE_LOSS_COOLDOWN
        assert "TRADE_REJECTED_CONSECUTIVE_LOSS_COOLDOWN" in d.reason

    def test_expired_cooldown_allows_trade(self):
        from app.policy.policy_engine import PolicyEngine, Action
        engine = PolicyEngine(min_confidence=0.10)
        ctx = _ctx(
            consec_loss_cooldown_until_ms=_now_ms() - 1000,  # expired
            consec_loss_day_paused=False,
            now_ms=_now_ms(),
            equity=10000.0,
        )
        d = engine.evaluate(ctx)
        assert d.action == Action.OPEN_LONG

    def test_correct_rejection_reasons(self):
        from app.policy.policy_engine import PolicyEngine, ReasonCode
        engine = PolicyEngine(min_confidence=0.10)
        # Day pause
        d1 = engine.evaluate(_ctx(consec_loss_day_paused=True))
        assert d1.reason_code == ReasonCode.CONSECUTIVE_LOSS_DAY_PAUSE
        # Soft cooldown
        d2 = engine.evaluate(_ctx(consec_loss_cooldown_until_ms=_now_ms() + 9999999))
        assert d2.reason_code == ReasonCode.CONSECUTIVE_LOSS_COOLDOWN


# ═══════════════════════════════════════════════════════════════════════════
# D-2: Risk/Reward Enforcement
# ═══════════════════════════════════════════════════════════════════════════

class TestRiskRewardEnforcement:

    def test_rr_below_minimum_rejected(self):
        from app.policy.policy_engine import PolicyEngine, ReasonCode
        engine = PolicyEngine(min_confidence=0.10)
        # LONG: entry=50000, sl=49000, tp=50500 → risk=1000, reward=500 → RR=0.5
        ctx = _ctx(
            entry_price=50000.0, stop_loss_price=49000.0, take_profit_price=50500.0,
            min_risk_reward=1.5,
        )
        d = engine.evaluate(ctx)
        assert not d.allowed
        assert d.reason_code == ReasonCode.RISK_REWARD_TOO_LOW
        assert "TRADE_REJECTED_RISK_REWARD_TOO_LOW" in d.reason

    def test_rr_at_minimum_allowed(self):
        from app.policy.policy_engine import PolicyEngine, Action
        engine = PolicyEngine(min_confidence=0.10)
        # LONG: entry=50000, sl=49000, tp=51500 → risk=1000, reward=1500 → RR=1.5
        ctx = _ctx(
            entry_price=50000.0, stop_loss_price=49000.0, take_profit_price=51500.0,
            min_risk_reward=1.5, equity=10000.0,
        )
        d = engine.evaluate(ctx)
        # Should not be blocked by R:R
        assert d.reason_code.value != "RISK_REWARD_TOO_LOW"

    def test_rr_above_minimum_allowed(self):
        from app.policy.policy_engine import PolicyEngine, Action
        engine = PolicyEngine(min_confidence=0.10)
        # LONG: RR = 2000/1000 = 2.0 > 1.5
        ctx = _ctx(
            entry_price=50000.0, stop_loss_price=49000.0, take_profit_price=52000.0,
            min_risk_reward=1.5, equity=10000.0,
        )
        d = engine.evaluate(ctx)
        assert d.reason_code.value != "RISK_REWARD_TOO_LOW"

    def test_invalid_risk_rejected(self):
        from app.policy.policy_engine import PolicyEngine, ReasonCode
        engine = PolicyEngine(min_confidence=0.10)
        # LONG with SL above entry → risk < 0
        ctx = _ctx(
            entry_price=50000.0, stop_loss_price=51000.0, take_profit_price=52000.0,
            min_risk_reward=1.5,
        )
        d = engine.evaluate(ctx)
        assert not d.allowed
        assert d.reason_code == ReasonCode.INVALID_RISK

    def test_invalid_reward_rejected(self):
        from app.policy.policy_engine import PolicyEngine, ReasonCode
        engine = PolicyEngine(min_confidence=0.10)
        # LONG with TP below entry → reward < 0
        ctx = _ctx(
            entry_price=50000.0, stop_loss_price=49000.0, take_profit_price=48000.0,
            min_risk_reward=1.5,
        )
        d = engine.evaluate(ctx)
        assert not d.allowed
        assert d.reason_code == ReasonCode.INVALID_REWARD

    def test_rr_check_disabled_when_zero(self):
        """min_risk_reward=0 disables the check entirely."""
        from app.policy.policy_engine import PolicyEngine
        engine = PolicyEngine(min_confidence=0.10)
        # Very bad R:R but check is disabled
        ctx = _ctx(
            entry_price=50000.0, stop_loss_price=49000.0, take_profit_price=50100.0,
            min_risk_reward=0.0, equity=10000.0,
        )
        d = engine.evaluate(ctx)
        assert d.reason_code.value != "RISK_REWARD_TOO_LOW"

    def test_rr_not_checked_for_existing_position_close(self):
        """R:R check only applies to new opens (position=NONE)."""
        from app.policy.policy_engine import PolicyEngine
        engine = PolicyEngine(min_confidence=0.10)
        ctx = _ctx(
            position="LONG", signal="SELL",  # closing
            entry_price=50000.0, stop_loss_price=51000.0, take_profit_price=48000.0,
            min_risk_reward=1.5,
        )
        d = engine.evaluate(ctx)
        assert d.reason_code.value != "RISK_REWARD_TOO_LOW"

    def test_short_side_rr_calculation(self):
        from app.policy.policy_engine import PolicyEngine, ReasonCode
        engine = PolicyEngine(min_confidence=0.10)
        # SHORT: entry=50000, sl=51000, tp=49500 → risk=1000, reward=500 → RR=0.5
        ctx = _ctx(
            signal="SELL",
            entry_price=50000.0, stop_loss_price=51000.0, take_profit_price=49500.0,
            min_risk_reward=1.5,
        )
        d = engine.evaluate(ctx)
        assert not d.allowed
        assert d.reason_code == ReasonCode.RISK_REWARD_TOO_LOW


# ═══════════════════════════════════════════════════════════════════════════
# D-3: ATR Fixed Sizing Protection
# ═══════════════════════════════════════════════════════════════════════════

class TestATRFixedSizingProtection:

    def test_stop_below_atr_noise_floor_rejected(self):
        from app.policy.policy_engine import PolicyEngine, ReasonCode
        engine = PolicyEngine(min_confidence=0.10)
        # ATR=500, stop_distance=100, floor=0.5*500=250 → 100 < 250 → reject
        ctx = _ctx(
            trade_amount_mode="fixed", trade_amount_value=50.0,
            entry_price=50000.0, stop_loss_price=49900.0,  # distance=100
            atr=500.0, min_stop_atr_multiplier=0.5,
        )
        d = engine.evaluate(ctx)
        assert not d.allowed
        assert d.reason_code == ReasonCode.STOP_BELOW_ATR_NOISE_FLOOR
        assert "TRADE_REJECTED_STOP_DISTANCE_BELOW_ATR_NOISE_FLOOR" in d.reason

    def test_missing_atr_rejected_for_fixed_sizing(self):
        from app.policy.policy_engine import PolicyEngine, ReasonCode
        engine = PolicyEngine(min_confidence=0.10)
        ctx = _ctx(
            trade_amount_mode="fixed", trade_amount_value=50.0,
            entry_price=50000.0, stop_loss_price=49000.0,
            atr=0.0,  # missing ATR
        )
        d = engine.evaluate(ctx)
        assert not d.allowed
        assert d.reason_code == ReasonCode.MISSING_ATR_FOR_FIXED_SIZING

    def test_stop_above_atr_noise_floor_passes(self):
        from app.policy.policy_engine import PolicyEngine
        engine = PolicyEngine(min_confidence=0.10)
        # ATR=500, stop_distance=600, floor=250 → 600 > 250 → pass
        ctx = _ctx(
            trade_amount_mode="fixed", trade_amount_value=50.0,
            entry_price=50000.0, stop_loss_price=49400.0,  # distance=600
            atr=500.0, min_stop_atr_multiplier=0.5, equity=10000.0,
            max_risk_per_trade_pct=1.0,
        )
        d = engine.evaluate(ctx)
        assert d.reason_code.value not in (
            "STOP_BELOW_ATR_NOISE_FLOOR", "MISSING_ATR_FOR_FIXED_SIZING"
        )

    def test_estimated_loss_above_risk_cap_rejected(self):
        from app.policy.policy_engine import PolicyEngine, ReasonCode
        engine = PolicyEngine(min_confidence=0.10)
        # fixed_amount=50, leverage=3 → qty=50*3/50000=0.003
        # stop_distance=5000 → loss=0.003*5000=15 > 1% of 100 equity=1.0
        ctx = _ctx(
            trade_amount_mode="fixed", trade_amount_value=50.0, leverage=3.0,
            entry_price=50000.0, stop_loss_price=45000.0,  # huge stop
            atr=500.0, min_stop_atr_multiplier=0.5,
            equity=100.0,  # tiny equity
            max_risk_per_trade_pct=1.0,  # 1% of 100 = $1 max loss
        )
        d = engine.evaluate(ctx)
        assert not d.allowed
        assert d.reason_code == ReasonCode.ATR_RISK_CAP_EXCEEDED

    def test_atr_check_applies_to_atr_risk_mode(self):
        """F-5 fix: ATR noise-floor check must also apply to ATR_RISK mode (not just fixed).
        Previously the gate was bypassed for atr_risk — that was the bug F-5 corrected."""
        from app.policy.policy_engine import PolicyEngine, ReasonCode
        engine = PolicyEngine(min_confidence=0.10)
        ctx = _ctx(
            trade_amount_mode="atr_risk",  # was previously excluded — F-5 removed that guard
            entry_price=50000.0, stop_loss_price=49900.0,  # tiny stop (100 < 0.5*500=250)
            atr=500.0, equity=10000.0,
        )
        d = engine.evaluate(ctx)
        assert d.reason_code == ReasonCode.STOP_BELOW_ATR_NOISE_FLOOR, (
            f"D-3 ATR floor gate must fire for atr_risk mode after F-5 fix. Got: {d.reason_code}"
        )

    def test_rejection_reason_is_correct(self):
        from app.policy.policy_engine import PolicyEngine, ReasonCode
        engine = PolicyEngine(min_confidence=0.10)
        ctx = _ctx(
            trade_amount_mode="fixed", trade_amount_value=50.0,
            entry_price=50000.0, stop_loss_price=49900.0,
            atr=500.0,
        )
        d = engine.evaluate(ctx)
        assert d.reason_code == ReasonCode.STOP_BELOW_ATR_NOISE_FLOOR

    def test_details_contain_atr_fields(self):
        from app.policy.policy_engine import PolicyEngine
        engine = PolicyEngine(min_confidence=0.10)
        ctx = _ctx(
            trade_amount_mode="fixed", trade_amount_value=50.0,
            entry_price=50000.0, stop_loss_price=49900.0,
            atr=500.0,
        )
        d = engine.evaluate(ctx)
        assert d.details is not None
        assert "atr_value" in d.details or "stop_distance" in d.details


# ═══════════════════════════════════════════════════════════════════════════
# D-1/D-2/D-3 config checks
# ═══════════════════════════════════════════════════════════════════════════

class TestSectionDConfig:
    def test_soft_limit_in_config(self):
        from app.core.config import settings
        assert getattr(settings, "MAX_CONSECUTIVE_LOSSES_SOFT", 0) == 3

    def test_cooldown_minutes_in_config(self):
        from app.core.config import settings
        assert getattr(settings, "CONSECUTIVE_LOSS_COOLDOWN_MINUTES", 0) == 120

    def test_hard_limit_in_config(self):
        from app.core.config import settings
        assert getattr(settings, "MAX_CONSECUTIVE_LOSSES_HARD", 0) == 5

    def test_min_risk_reward_in_config(self):
        from app.core.config import settings
        assert settings.MIN_RISK_REWARD >= 1.5

    def test_min_stop_atr_multiplier_in_config(self):
        from app.core.config import settings
        assert getattr(settings, "MIN_STOP_ATR_MULTIPLIER", 0.0) == 0.5

    def test_max_risk_per_trade_pct_in_config(self):
        from app.core.config import settings
        assert getattr(settings, "MAX_RISK_PER_TRADE_PCT", 0.0) == 1.0
