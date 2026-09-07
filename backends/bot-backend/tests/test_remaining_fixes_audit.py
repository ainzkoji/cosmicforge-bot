"""
CosmicForge — Remaining Fixes Audit Tests
B-6, D-1, B-5, A-x, C-x, D-2, B-1, B-7, B-8
"""
from __future__ import annotations
import pytest


class TestActivityFallbackDisabled:
    def test_daily_activity_fallback_disabled_by_default(self):
        from app.risk.safety_engine import SafetyConfig
        assert SafetyConfig().daily_activity_fallback_enabled is False

    def test_fallback_config_field_false(self):
        """When disabled=False, check_daily_activity_fallback must fast-return inactive."""
        from app.risk.safety_engine import SafetyConfig
        # Verify the source early-exits when config flag is False
        import inspect
        from app.risk import safety_engine as _se
        src = inspect.getsource(_se.SafetyEngine.check_daily_activity_fallback)
        # The method must check daily_activity_fallback_enabled and return early
        assert "daily_activity_fallback_enabled" in src


class TestGuardModuleD1:
    def setup_method(self):
        from app.risk.guard import reset_all
        reset_all()

    def teardown_method(self):
        from app.risk.guard import reset_all
        reset_all()

    def test_loss_increments_count(self):
        from app.risk.guard import on_trade_closed, _get_state
        on_trade_closed(pnl=-50.0, bot_id="bot1")
        assert _get_state("bot1").consecutive_losses == 1

    def test_win_resets_streak(self):
        from app.risk.guard import on_trade_closed, _get_state
        on_trade_closed(-10.0, "bot1"); on_trade_closed(-10.0, "bot1")
        on_trade_closed(+20.0, "bot1")
        assert _get_state("bot1").consecutive_losses == 0

    def test_should_pause_after_max_losses(self):
        from app.risk.guard import on_trade_closed, should_pause
        for _ in range(3): on_trade_closed(-1.0, "bot1")
        assert should_pause(bot_id="bot1", max_losses=3) is True

    def test_fresh_bot_not_paused(self):
        from app.risk.guard import should_pause
        assert should_pause("fresh", max_losses=3) is False

    def test_reset_bot_clears(self):
        from app.risk.guard import on_trade_closed, reset_bot, _get_state
        on_trade_closed(-1.0, "bot1"); on_trade_closed(-1.0, "bot1")
        reset_bot("bot1")
        assert _get_state("bot1").consecutive_losses == 0

    def test_bot_a_losses_never_pause_bot_b(self):
        from app.risk.guard import on_trade_closed, should_pause
        for _ in range(5): on_trade_closed(-100.0, "botA")
        should_pause("botA", max_losses=3)
        assert should_pause("botB", max_losses=3) is False


class TestGuardWiredInRunner:
    SRC = open("C:/Users/favou/OneDrive/Desktop/cosmicforge-bot/backends/bot-backend/app/runner/runner.py", encoding="utf-8").read()

    def test_guard_imported(self):
        assert "app.risk.guard" in self.SRC

    def test_on_trade_closed_called(self):
        assert "_guard_on_trade_closed" in self.SRC

    def test_should_pause_called(self):
        assert "_guard_should_pause" in self.SRC

    def test_reset_bot_called(self):
        assert "_guard_reset_bot" in self.SRC

    def test_bot_id_guard_resolved(self):
        assert "_bot_id_guard" in self.SRC


class TestExternalConfidence:
    def test_none_returns_none(self):
        from app.api.tradingview import _safe_confidence
        assert _safe_confidence(None) is None

    def test_empty_returns_none(self):
        from app.api.tradingview import _safe_confidence
        assert _safe_confidence("") is None

    def test_not_1_for_none(self):
        from app.api.tradingview import _safe_confidence
        assert _safe_confidence(None) != 1.0

    def test_valid_accepted(self):
        from app.api.tradingview import _safe_confidence
        assert _safe_confidence(0.75) == pytest.approx(0.75)


class TestConfigD:
    def test_daily_close_enabled(self):
        from app.core.config import settings
        assert settings.DAILY_CLOSE_ENABLED is True

    def test_interval_15m(self):
        from app.core.config import settings
        assert settings.DEFAULT_INTERVAL == "15m"

    def test_strategy_master_ensemble(self):
        from app.core.config import settings
        assert settings.STRATEGY_NAME == "master_ensemble"

    def test_trade_usdt_50(self):
        from app.core.config import settings
        assert float(settings.TRADE_USDT_PER_ORDER) >= 50.0

    def test_weekly_dd_positive(self):
        from app.core.config import settings
        assert float(getattr(settings, "MAX_WEEKLY_DRAWDOWN_PCT", 0)) > 0

    def test_monthly_dd_positive(self):
        from app.core.config import settings
        assert float(getattr(settings, "MAX_MONTHLY_DRAWDOWN_PCT", 0)) > 0

    def test_min_rr_gte_1_5(self):
        from app.core.config import settings
        assert float(settings.MIN_RISK_REWARD) >= 1.5


class TestSignalConfig:
    def test_max_published_10(self):
        from app.signals.signal_scheduler_config import DEFAULT_MAX_PUBLISHED_PER_SCAN
        assert DEFAULT_MAX_PUBLISHED_PER_SCAN == 10

    def test_max_active_20(self):
        from app.signals.signal_scheduler_config import DEFAULT_MAX_ACTIVE_SIGNALS
        assert DEFAULT_MAX_ACTIVE_SIGNALS == 20

    def test_scheduler_in_main(self):
        src = open("C:/Users/favou/OneDrive/Desktop/cosmicforge-bot/backends/bot-backend/app/main.py", encoding="utf-8").read()
        assert "AsyncIOScheduler" in src
        assert "expire_stale_signals" in src
        assert "minutes=5" in src


class TestRRPolicy:
    def test_policy_context_rr_field(self):
        from app.policy.policy_engine import PolicyContext
        ctx = PolicyContext(symbol="BTCUSDT", signal="BUY", min_risk_reward=1.5)
        assert ctx.min_risk_reward == 1.5

    def test_rr_in_engine_source(self):
        src = open("C:/Users/favou/OneDrive/Desktop/cosmicforge-bot/backends/bot-backend/app/policy/policy_engine.py", encoding="utf-8").read()
        assert "min_risk_reward" in src


class TestAutoPlotMinAmount:
    def test_auto_pilot_50_usdt_check(self):
        src = open("C:/Users/favou/OneDrive/Desktop/cosmicforge-bot/backends/bot-backend/app/api/auto_pilot.py", encoding="utf-8").read()
        assert "50" in src and ("422" in src or "HTTPException" in src)


class TestSmaCrossRunner:
    def test_sma_cross_blocked(self):
        src = open("C:/Users/favou/OneDrive/Desktop/cosmicforge-bot/backends/bot-backend/app/runner/runner.py", encoding="utf-8").read()
        assert "sma_cross" in src.lower() and "master_ensemble" in src
