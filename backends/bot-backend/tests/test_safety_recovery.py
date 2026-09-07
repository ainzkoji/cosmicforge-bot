"""
CosmicForge Safety Recovery Tests

Verifies all critical safety configuration and enforcement logic introduced
in the strategy recovery phase. Tests are grouped by area.
"""
from __future__ import annotations

import pytest
from unittest.mock import MagicMock, patch
from dataclasses import replace


# ── helpers ─────────────────────────────────────────────────────────────────

def _make_settings(**overrides):
    """Return a minimal settings-like object for safety tests."""
    from types import SimpleNamespace
    defaults = dict(
        DEFAULT_INTERVAL="15m",
        SIGNAL_INTERVAL="15m",
        MAX_ADDS_PER_POSITION=0,
        MAX_WEEKLY_DRAWDOWN_PCT=5.0,
        MAX_MONTHLY_DRAWDOWN_PCT=10.0,
        MAX_CONSECUTIVE_LOSSES=3,
        DAILY_MAX_LOSS_USDT=50.0,
        MAX_TRADES_DAILY=6,
        MAX_OPEN_POSITIONS=3,
        MIN_CONFIDENCE_THRESHOLD=0.70,
        MIN_RISK_REWARD=1.8,
        EXECUTION_MODE="paper",
        STRATEGY_NAME="master_ensemble",
        TRADINGVIEW_EXTERNAL_SIGNALS_ENABLED=False,
        TRADINGVIEW_TESTNET_ONLY=True,
        # A-4, A-7, A-8
        KILL_SWITCH_CLOSE_POSITIONS=True,
        EVENT_FILTER_ENABLED=True,
        NEWS_TRADING_ENABLED=False,
        HIGH_IMPACT_BLACKOUT_MINUTES_BEFORE=30,
        HIGH_IMPACT_BLACKOUT_MINUTES_AFTER=15,
        TRADE_USDT_PER_ORDER=50.0,
    )
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def _make_policy_context(**overrides):
    from app.policy.policy_engine import PolicyContext
    defaults = dict(
        symbol="BTCUSDT",
        signal="BUY",
        confidence=0.80,
        position="NONE",
        adds=0,
        last_trade_ms=0,
        last_stop_ms=0,
        equity=1000.0,
        daily_realized_pnl=0.0,
        daily_trade_count=0,
        open_positions_count=0,
        leverage=3.0,
        stop_loss_pct=0.02,
        take_profit_pct=0.03,
        cooldown_seconds=0,
        sl_cooldown_seconds=0,
        max_adds=0,
        trade_mode="normal",
        max_daily_loss=50.0,
        max_daily_trades=5,
        max_open_positions=2,
        kill_switch=False,
        execution_mode="paper",
        now_ms=int(1e12),
        entry_price=50000.0,
        atr=500.0,
        weekly_drawdown_pct=0.0,
        monthly_drawdown_pct=0.0,
        max_weekly_drawdown_pct=5.0,
        max_monthly_drawdown_pct=10.0,
        consecutive_losses=0,
        max_consecutive_losses=3,
    )
    defaults.update(overrides)
    return PolicyContext(**defaults)


# ═══════════════════════════════════════════════════════════════════════════
# 1. CONFIG SAFETY TESTS
# ═══════════════════════════════════════════════════════════════════════════

class TestConfigSafety:
    def test_default_interval_is_15m(self):
        from app.core.config import settings
        assert settings.DEFAULT_INTERVAL == "15m", (
            f"DEFAULT_INTERVAL must be 15m, got {settings.DEFAULT_INTERVAL}"
        )

    def test_max_adds_is_zero(self):
        from app.core.config import settings
        assert settings.MAX_ADDS_PER_POSITION == 0, (
            f"MAX_ADDS_PER_POSITION must be 0, got {settings.MAX_ADDS_PER_POSITION}"
        )

    def test_weekly_drawdown_active(self):
        from app.core.config import settings
        assert settings.MAX_WEEKLY_DRAWDOWN_PCT > 0, (
            f"MAX_WEEKLY_DRAWDOWN_PCT must be > 0, got {settings.MAX_WEEKLY_DRAWDOWN_PCT}"
        )

    def test_monthly_drawdown_active(self):
        from app.core.config import settings
        assert settings.MAX_MONTHLY_DRAWDOWN_PCT > 0, (
            f"MAX_MONTHLY_DRAWDOWN_PCT must be > 0, got {settings.MAX_MONTHLY_DRAWDOWN_PCT}"
        )

    def test_daily_loss_active(self):
        from app.core.config import settings
        assert settings.DAILY_MAX_LOSS_USDT > 0

    def test_validate_safety_passes_with_correct_config(self):
        from app.core.config import settings
        ok, failures, _ = settings.validate_safety()
        assert ok, f"Safety check failed: {failures}"

    def test_validate_safety_fails_on_1m_interval(self):
        from app.core.config import Settings
        s = Settings(_env_file=None, DEFAULT_INTERVAL="1m", MAX_WEEKLY_DRAWDOWN_PCT=5.0,
                     MAX_MONTHLY_DRAWDOWN_PCT=10.0, DAILY_MAX_LOSS_USDT=50.0)
        ok, failures, _ = s.validate_safety()
        assert not ok
        assert any("1m" in f for f in failures)

    def test_validate_safety_fails_on_adds_enabled(self):
        from app.core.config import Settings
        s = Settings(_env_file=None, MAX_ADDS_PER_POSITION=5,
                     MAX_WEEKLY_DRAWDOWN_PCT=5.0, MAX_MONTHLY_DRAWDOWN_PCT=10.0,
                     DAILY_MAX_LOSS_USDT=50.0)
        ok, failures, _ = s.validate_safety()
        assert not ok
        assert any("MAX_ADDS" in f for f in failures)

    def test_validate_safety_fails_when_weekly_drawdown_zero(self):
        from app.core.config import Settings
        s = Settings(_env_file=None, MAX_WEEKLY_DRAWDOWN_PCT=0.0,
                     MAX_MONTHLY_DRAWDOWN_PCT=10.0, DAILY_MAX_LOSS_USDT=50.0)
        ok, failures, _ = s.validate_safety()
        assert not ok
        assert any("WEEKLY" in f for f in failures)

    def test_validate_safety_fails_when_monthly_drawdown_zero(self):
        from app.core.config import Settings
        s = Settings(_env_file=None, MAX_WEEKLY_DRAWDOWN_PCT=5.0,
                     MAX_MONTHLY_DRAWDOWN_PCT=0.0, DAILY_MAX_LOSS_USDT=50.0)
        ok, failures, _ = s.validate_safety()
        assert not ok
        assert any("MONTHLY" in f for f in failures)


# ═══════════════════════════════════════════════════════════════════════════
# 2. POSITION ADD TESTS
# ═══════════════════════════════════════════════════════════════════════════

class TestPositionAddProtection:
    def test_add_blocked_when_max_adds_zero_long(self):
        from app.policy.policy_engine import PolicyEngine, Action
        engine = PolicyEngine(min_confidence=0.10)
        ctx = _make_policy_context(position="LONG", signal="BUY", max_adds=0, adds=0)
        decision = engine.evaluate(ctx)
        # Should HOLD, not ADD_LONG
        assert decision.action == Action.HOLD
        assert "POSITION_ADD_REJECTED_MAX_ADDS_DISABLED" in decision.reason

    def test_add_blocked_when_max_adds_zero_short(self):
        from app.policy.policy_engine import PolicyEngine, Action
        engine = PolicyEngine(min_confidence=0.10)
        ctx = _make_policy_context(position="SHORT", signal="SELL", max_adds=0, adds=0)
        decision = engine.evaluate(ctx)
        assert decision.action == Action.HOLD
        assert "POSITION_ADD_REJECTED_MAX_ADDS_DISABLED" in decision.reason

    def test_add_allowed_when_max_adds_nonzero(self):
        from app.policy.policy_engine import PolicyEngine, Action
        engine = PolicyEngine(min_confidence=0.10)
        ctx = _make_policy_context(
            position="LONG", signal="BUY", max_adds=2, adds=0,
            # Give enough margin for sizing
            equity=10000.0,
        )
        decision = engine.evaluate(ctx)
        assert decision.action == Action.ADD_LONG

    def test_new_position_still_opens_when_adds_zero(self):
        """max_adds=0 must not prevent new positions, only adds."""
        from app.policy.policy_engine import PolicyEngine, Action
        engine = PolicyEngine(min_confidence=0.10)
        ctx = _make_policy_context(position="NONE", signal="BUY", max_adds=0, equity=10000.0)
        decision = engine.evaluate(ctx)
        assert decision.action == Action.OPEN_LONG


# ═══════════════════════════════════════════════════════════════════════════
# 3. DRAWDOWN TESTS
# ═══════════════════════════════════════════════════════════════════════════

class TestDrawdownProtection:
    def test_weekly_drawdown_at_limit_blocks_trade(self):
        from app.policy.policy_engine import PolicyEngine, Action, ReasonCode
        engine = PolicyEngine(min_confidence=0.10)
        ctx = _make_policy_context(
            weekly_drawdown_pct=5.0,
            max_weekly_drawdown_pct=5.0,
        )
        decision = engine.evaluate(ctx)
        assert not decision.allowed
        assert decision.reason_code == ReasonCode.WEEKLY_DRAWDOWN_LIMIT

    def test_weekly_drawdown_below_limit_allows_trade(self):
        from app.policy.policy_engine import PolicyEngine, Action
        engine = PolicyEngine(min_confidence=0.10)
        ctx = _make_policy_context(
            weekly_drawdown_pct=4.9,
            max_weekly_drawdown_pct=5.0,
            equity=10000.0,
        )
        decision = engine.evaluate(ctx)
        # Should not be blocked by drawdown
        assert decision.reason_code.value != "WEEKLY_DRAWDOWN_LIMIT"

    def test_monthly_drawdown_at_limit_blocks_trade(self):
        from app.policy.policy_engine import PolicyEngine, ReasonCode
        engine = PolicyEngine(min_confidence=0.10)
        ctx = _make_policy_context(
            monthly_drawdown_pct=10.0,
            max_monthly_drawdown_pct=10.0,
        )
        decision = engine.evaluate(ctx)
        assert not decision.allowed
        assert decision.reason_code == ReasonCode.MONTHLY_DRAWDOWN_LIMIT

    def test_drawdown_zero_limit_does_not_block(self):
        """Drawdown limit of 0 means disabled — must not block."""
        from app.policy.policy_engine import PolicyEngine
        engine = PolicyEngine(min_confidence=0.10)
        ctx = _make_policy_context(
            weekly_drawdown_pct=99.0,
            max_weekly_drawdown_pct=0.0,  # disabled
            monthly_drawdown_pct=99.0,
            max_monthly_drawdown_pct=0.0,  # disabled
            equity=10000.0,
        )
        decision = engine.evaluate(ctx)
        assert "DRAWDOWN" not in (decision.reason_code.value if decision.reason_code else "")


# ═══════════════════════════════════════════════════════════════════════════
# 4. CONSECUTIVE LOSS PAUSE TESTS
# ═══════════════════════════════════════════════════════════════════════════

class TestConsecutiveLossPause:
    def test_three_consecutive_losses_blocks_trade(self):
        """D-1 redesign: soft cooldown uses consec_loss_cooldown_until_ms."""
        import time
        from app.policy.policy_engine import PolicyEngine, ReasonCode
        engine = PolicyEngine(min_confidence=0.10)
        # Simulate soft cooldown active (2h from now)
        ctx = _make_policy_context(
            consecutive_losses=3,
            max_consecutive_losses=3,
            consec_loss_cooldown_until_ms=int(time.time() * 1000) + 7_200_000,
            now_ms=int(time.time() * 1000),
        )
        decision = engine.evaluate(ctx)
        assert not decision.allowed
        assert decision.reason_code == ReasonCode.CONSECUTIVE_LOSS_COOLDOWN

    def test_two_consecutive_losses_still_allows_trade(self):
        from app.policy.policy_engine import PolicyEngine
        engine = PolicyEngine(min_confidence=0.10)
        ctx = _make_policy_context(
            consecutive_losses=2, max_consecutive_losses=3, equity=10000.0
        )
        decision = engine.evaluate(ctx)
        assert "CONSECUTIVE_LOSS_PAUSE" not in (decision.reason or "")

    def test_max_consecutive_zero_disables_check(self):
        from app.policy.policy_engine import PolicyEngine
        engine = PolicyEngine(min_confidence=0.10)
        ctx = _make_policy_context(
            consecutive_losses=100, max_consecutive_losses=0, equity=10000.0
        )
        decision = engine.evaluate(ctx)
        assert "CONSECUTIVE_LOSS_PAUSE" not in (decision.reason or "")


# ═══════════════════════════════════════════════════════════════════════════
# 5. STARTUP SAFETY CHECK TESTS
# ═══════════════════════════════════════════════════════════════════════════

class TestStartupSafetyCheck:
    def test_all_pass_with_safe_config(self):
        from app.core.safety_startup import run_startup_safety_check
        s = _make_settings()
        report = run_startup_safety_check(s, mode="paper")
        assert report.all_pass, [c for c in report.checks if not c.passed]

    def test_fails_with_1m_interval(self):
        from app.core.safety_startup import run_startup_safety_check
        s = _make_settings(DEFAULT_INTERVAL="1m")
        report = run_startup_safety_check(s, mode="paper")
        assert not report.all_pass

    def test_fails_with_adds_enabled(self):
        from app.core.safety_startup import run_startup_safety_check
        s = _make_settings(MAX_ADDS_PER_POSITION=10)
        report = run_startup_safety_check(s, mode="paper")
        assert not report.all_pass

    def test_fails_with_weekly_drawdown_zero(self):
        from app.core.safety_startup import run_startup_safety_check
        s = _make_settings(MAX_WEEKLY_DRAWDOWN_PCT=0.0)
        report = run_startup_safety_check(s, mode="paper")
        assert not report.all_pass

    def test_fails_with_monthly_drawdown_zero(self):
        from app.core.safety_startup import run_startup_safety_check
        s = _make_settings(MAX_MONTHLY_DRAWDOWN_PCT=0.0)
        report = run_startup_safety_check(s, mode="paper")
        assert not report.all_pass

    def test_fails_with_daily_loss_zero(self):
        from app.core.safety_startup import run_startup_safety_check
        s = _make_settings(DAILY_MAX_LOSS_USDT=0.0)
        report = run_startup_safety_check(s, mode="paper")
        assert not report.all_pass

    def test_sma_cross_fails_in_live_mode(self):
        from app.core.safety_startup import run_startup_safety_check
        s = _make_settings(STRATEGY_NAME="sma_cross", EXECUTION_MODE="live")
        report = run_startup_safety_check(s, mode="live")
        assert not report.all_pass

    def test_sma_cross_ok_in_paper_mode(self):
        from app.core.safety_startup import run_startup_safety_check
        s = _make_settings(STRATEGY_NAME="sma_cross", EXECUTION_MODE="paper")
        report = run_startup_safety_check(s, mode="paper")
        # sma_cross only fails in live mode
        strategy_check = next(c for c in report.checks if c.name == "STRATEGY_NAME")
        assert strategy_check.passed


# ═══════════════════════════════════════════════════════════════════════════
# 6. DRAWDOWN MONITOR UNIT TESTS
# ═══════════════════════════════════════════════════════════════════════════

class TestDrawdownMonitor:
    def test_check_weekly_triggers_at_limit(self):
        from app.risk.drawdown import DrawdownMonitor
        from app.risk.state import PeriodSnapshot
        from datetime import date
        monitor = DrawdownMonitor(store=MagicMock())
        snap = PeriodSnapshot(
            start_date=date.today(),
            start_equity=1000.0,
            peak_equity=1000.0,
            low_equity=950.0,
        )
        # 5% drawdown from peak of 1000 → current = 950
        assert monitor.check_weekly_drawdown(5.0, 950.0, snap) is True

    def test_check_weekly_below_limit(self):
        from app.risk.drawdown import DrawdownMonitor
        from app.risk.state import PeriodSnapshot
        from datetime import date
        monitor = DrawdownMonitor(store=MagicMock())
        snap = PeriodSnapshot(
            start_date=date.today(),
            start_equity=1000.0,
            peak_equity=1000.0,
            low_equity=960.0,
        )
        # 4% drawdown — below 5% limit
        assert monitor.check_weekly_drawdown(5.0, 960.0, snap) is False

    def test_check_monthly_triggers_at_limit(self):
        from app.risk.drawdown import DrawdownMonitor
        from app.risk.state import PeriodSnapshot
        from datetime import date
        monitor = DrawdownMonitor(store=MagicMock())
        snap = PeriodSnapshot(
            start_date=date.today(),
            start_equity=1000.0,
            peak_equity=1000.0,
            low_equity=900.0,
        )
        assert monitor.check_monthly_drawdown(10.0, 900.0, snap) is True

    def test_zero_limit_never_triggers(self):
        from app.risk.drawdown import DrawdownMonitor
        from app.risk.state import PeriodSnapshot
        from datetime import date
        monitor = DrawdownMonitor(store=MagicMock())
        snap = PeriodSnapshot(
            start_date=date.today(),
            start_equity=1000.0,
            peak_equity=1000.0,
            low_equity=1.0,
        )
        # 0 limit = disabled
        assert monitor.check_weekly_drawdown(0.0, 1.0, snap) is False
        assert monitor.check_monthly_drawdown(0.0, 1.0, snap) is False


# ═══════════════════════════════════════════════════════════════════════════
# 7. EXTERNAL SIGNAL SAFETY TESTS (validation layer)
# ═══════════════════════════════════════════════════════════════════════════

class TestExternalSignalSafety:
    """Check that external signal safety flags are correctly configured."""

    def test_tv_testnet_only_by_default_in_config(self):
        from app.core.config import settings
        # External signals must require testnet_only when enabled
        if settings.TRADINGVIEW_EXTERNAL_SIGNALS_ENABLED:
            assert settings.TRADINGVIEW_TESTNET_ONLY, (
                "External signals enabled without TRADINGVIEW_TESTNET_ONLY=true"
            )

    def test_tv_risk_overrides_disabled(self):
        from app.core.config import settings
        assert not settings.TRADINGVIEW_ALLOW_RISK_OVERRIDE, (
            "External signals must not be allowed to override risk settings"
        )

    def test_tv_external_sltp_disabled(self):
        from app.core.config import settings
        assert not settings.TRADINGVIEW_ALLOW_EXTERNAL_SLTP, (
            "External signals must not be allowed to set SL/TP"
        )

    def test_tv_external_size_disabled(self):
        from app.core.config import settings
        assert not settings.TRADINGVIEW_ALLOW_EXTERNAL_SIZE, (
            "External signals must not be allowed to set position size"
        )
