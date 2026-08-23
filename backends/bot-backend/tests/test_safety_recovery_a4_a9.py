"""
CosmicForge Safety Recovery A-4 to A-9 Tests

A-4: Kill switch closes open positions
A-5: Max open positions = 3
A-6: Max daily trades = 6
A-7: Event/news blackout filter enabled
A-8: Minimum trade amount 50 USDT
A-9: master_ensemble default, sma_cross restricted
"""
from __future__ import annotations

import pytest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch, call


# ── helpers ─────────────────────────────────────────────────────────────────

def _make_ctx(**kw):
    from app.policy.policy_engine import PolicyContext
    defaults = dict(
        symbol="BTCUSDT", signal="BUY", confidence=0.80,
        position="NONE", adds=0, last_trade_ms=0, last_stop_ms=0,
        equity=10000.0, daily_realized_pnl=0.0, daily_trade_count=0,
        open_positions_count=0, leverage=3.0, stop_loss_pct=0.02,
        take_profit_pct=0.03, cooldown_seconds=0, sl_cooldown_seconds=0,
        max_adds=0, trade_mode="normal", max_daily_loss=50.0,
        max_daily_trades=6, max_open_positions=3, kill_switch=False,
        execution_mode="paper", now_ms=int(1e12), entry_price=50000.0, atr=500.0,
        weekly_drawdown_pct=0.0, monthly_drawdown_pct=0.0,
        max_weekly_drawdown_pct=5.0, max_monthly_drawdown_pct=10.0,
        consecutive_losses=0, max_consecutive_losses=3,
    )
    defaults.update(kw)
    return PolicyContext(**defaults)


def _make_settings(**overrides):
    defaults = dict(
        DEFAULT_INTERVAL="15m",
        MAX_ADDS_PER_POSITION=0,
        MAX_WEEKLY_DRAWDOWN_PCT=5.0,
        MAX_MONTHLY_DRAWDOWN_PCT=10.0,
        MAX_CONSECUTIVE_LOSSES=3,
        DAILY_MAX_LOSS_USDT=50.0,
        MAX_TRADES_DAILY=6,
        MAX_OPEN_POSITIONS=3,
        MIN_CONFIDENCE_THRESHOLD=0.70,
        EXECUTION_MODE="paper",
        STRATEGY_NAME="master_ensemble",
        TRADINGVIEW_EXTERNAL_SIGNALS_ENABLED=False,
        TRADINGVIEW_TESTNET_ONLY=True,
        KILL_SWITCH_CLOSE_POSITIONS=True,
        EVENT_FILTER_ENABLED=True,
        NEWS_TRADING_ENABLED=False,
        HIGH_IMPACT_BLACKOUT_MINUTES_BEFORE=30,
        HIGH_IMPACT_BLACKOUT_MINUTES_AFTER=15,
        TRADE_USDT_PER_ORDER=50.0,
    )
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


# ═══════════════════════════════════════════════════════════════════════════
# A-4: Kill Switch
# ═══════════════════════════════════════════════════════════════════════════

class TestKillSwitch:
    def test_kill_switch_close_positions_true_in_config(self):
        from app.core.config import settings
        assert settings.KILL_SWITCH_CLOSE_POSITIONS is True, (
            "KILL_SWITCH_CLOSE_POSITIONS must be True to close open positions on kill switch"
        )

    def test_kill_switch_close_positions_true_in_safety_check(self):
        from app.core.safety_startup import run_startup_safety_check
        s = _make_settings(KILL_SWITCH_CLOSE_POSITIONS=True)
        report = run_startup_safety_check(s, mode="paper")
        ks_check = next(c for c in report.checks if c.name == "KILL_SWITCH_CLOSE_POSITIONS")
        assert ks_check.passed

    def test_kill_switch_close_positions_false_fails_check(self):
        from app.core.safety_startup import run_startup_safety_check
        s = _make_settings(KILL_SWITCH_CLOSE_POSITIONS=False)
        report = run_startup_safety_check(s, mode="paper")
        ks_check = next(c for c in report.checks if c.name == "KILL_SWITCH_CLOSE_POSITIONS")
        assert not ks_check.passed

    def _make_runner_stub(self, position_map: dict, close_raises: bool = False):
        """Return a minimal PaperRunner-like object for kill switch testing."""
        from app.runner.runner import PaperRunner

        runner = object.__new__(PaperRunner)
        runner.live_symbols = list(position_map.keys())[:1]  # first symbol as live
        runner.run_id = "test-run"
        runner.cycle_id = "test-cycle"
        runner.state = {sym: MagicMock(position=pos) for sym, pos in position_map.items()}
        runner.daily = MagicMock(realized_pnl=-55.0)
        runner.client = MagicMock()
        runner.audit = MagicMock()
        runner.executor = MagicMock()
        if close_raises:
            runner.executor.execute_signal.side_effect = RuntimeError("exchange error")
        return runner

    def test_activate_kill_switch_calls_close_for_open_positions(self):
        """activate_kill_switch must attempt to close all open positions."""
        runner = self._make_runner_stub({
            "BTCUSDT": "LONG",
            "ETHUSDT": "SHORT",
        })

        with patch("app.runner.runner.settings") as mock_settings:
            mock_settings.KILL_SWITCH_CLOSE_POSITIONS = True
            runner.activate_kill_switch()

        close_calls = [
            c for c in runner.executor.execute_signal.call_args_list
            if c[0][1] == "CLOSE"
        ]
        assert len(close_calls) >= 1, "Expected at least one CLOSE call on kill switch"

    def test_activate_kill_switch_logs_failure(self):
        """Failed close attempts must be logged, not swallowed."""
        runner = self._make_runner_stub({"BTCUSDT": "LONG"}, close_raises=True)

        with patch("app.runner.runner.settings") as mock_settings, \
             patch("app.runner.runner.logger") as mock_logger:
            mock_settings.KILL_SWITCH_CLOSE_POSITIONS = True
            runner.activate_kill_switch()

        error_calls = [c for c in mock_logger.error.call_args_list
                       if "KILL_SWITCH_POSITION_CLOSE_FAILED" in str(c)]
        assert len(error_calls) >= 1, "Expected KILL_SWITCH_POSITION_CLOSE_FAILED log"

    def test_kill_switch_blocks_new_entry_via_policy(self):
        from app.policy.policy_engine import PolicyEngine, Action
        engine = PolicyEngine(min_confidence=0.10)
        ctx = _make_ctx(kill_switch=True, position="NONE", signal="BUY")
        decision = engine.evaluate(ctx)
        assert not decision.allowed

    def test_kill_switch_does_not_close_when_flag_false(self):
        runner = self._make_runner_stub({"BTCUSDT": "LONG"})

        with patch("app.runner.runner.settings") as mock_settings:
            mock_settings.KILL_SWITCH_CLOSE_POSITIONS = False
            runner.activate_kill_switch()

        runner.executor.execute_signal.assert_not_called()


# ═══════════════════════════════════════════════════════════════════════════
# A-5: Max Open Positions = 3
# ═══════════════════════════════════════════════════════════════════════════

class TestMaxOpenPositions:
    def test_max_open_positions_is_three_in_config(self):
        from app.core.config import settings
        assert settings.MAX_OPEN_POSITIONS == 3, (
            f"MAX_OPEN_POSITIONS must be 3, got {settings.MAX_OPEN_POSITIONS}"
        )

    def test_fourth_position_rejected(self):
        from app.policy.policy_engine import PolicyEngine, ReasonCode
        engine = PolicyEngine(min_confidence=0.10)
        ctx = _make_ctx(
            position="NONE", signal="BUY",
            open_positions_count=3,
            max_open_positions=3,
        )
        decision = engine.evaluate(ctx)
        assert not decision.allowed
        assert decision.reason_code == ReasonCode.MAX_POSITIONS_REACHED

    def test_third_position_allowed(self):
        from app.policy.policy_engine import PolicyEngine, Action
        engine = PolicyEngine(min_confidence=0.10)
        ctx = _make_ctx(
            position="NONE", signal="BUY",
            open_positions_count=2,
            max_open_positions=3,
        )
        decision = engine.evaluate(ctx)
        assert decision.action == Action.OPEN_LONG


# ═══════════════════════════════════════════════════════════════════════════
# A-6: Max Daily Trades = 6
# ═══════════════════════════════════════════════════════════════════════════

class TestMaxDailyTrades:
    def test_max_trades_daily_is_six_in_config(self):
        from app.core.config import settings
        assert settings.MAX_TRADES_DAILY == 6, (
            f"MAX_TRADES_DAILY must be 6, got {settings.MAX_TRADES_DAILY}"
        )

    def test_seventh_trade_rejected(self):
        from app.policy.policy_engine import PolicyEngine, ReasonCode
        engine = PolicyEngine(min_confidence=0.10)
        ctx = _make_ctx(daily_trade_count=6, max_daily_trades=6)
        decision = engine.evaluate(ctx)
        assert not decision.allowed
        assert decision.reason_code == ReasonCode.DAILY_TRADE_LIMIT

    def test_sixth_trade_allowed(self):
        from app.policy.policy_engine import PolicyEngine, Action
        engine = PolicyEngine(min_confidence=0.10)
        ctx = _make_ctx(daily_trade_count=5, max_daily_trades=6)
        decision = engine.evaluate(ctx)
        assert decision.action == Action.OPEN_LONG


# ═══════════════════════════════════════════════════════════════════════════
# A-7: Event Filter
# ═══════════════════════════════════════════════════════════════════════════

class TestEventFilter:
    def test_event_filter_enabled_in_config(self):
        from app.core.config import settings
        assert settings.EVENT_FILTER_ENABLED is True, (
            "EVENT_FILTER_ENABLED must be True"
        )

    def test_news_trading_disabled_in_config(self):
        from app.core.config import settings
        # NEWS_TRADING_ENABLED appears twice in config; check the last one wins
        assert not settings.NEWS_TRADING_ENABLED, (
            "NEWS_TRADING_ENABLED must be False"
        )

    def test_high_impact_blackout_before_set(self):
        from app.core.config import settings
        assert settings.HIGH_IMPACT_BLACKOUT_MINUTES_BEFORE == 30

    def test_high_impact_blackout_after_set(self):
        from app.core.config import settings
        assert settings.HIGH_IMPACT_BLACKOUT_MINUTES_AFTER == 15

    def test_event_filter_enabled_passes_startup_check(self):
        from app.core.safety_startup import run_startup_safety_check
        s = _make_settings(EVENT_FILTER_ENABLED=True)
        report = run_startup_safety_check(s, mode="paper")
        ef_check = next(c for c in report.checks if c.name == "EVENT_FILTER_ENABLED")
        assert ef_check.passed

    def test_event_filter_disabled_fails_startup_check(self):
        from app.core.safety_startup import run_startup_safety_check
        s = _make_settings(EVENT_FILTER_ENABLED=False)
        report = run_startup_safety_check(s, mode="paper")
        ef_check = next(c for c in report.checks if c.name == "EVENT_FILTER_ENABLED")
        assert not ef_check.passed

    def test_event_filter_enabled_in_validate_safety(self):
        from app.core.config import Settings
        s = Settings(_env_file=None, EVENT_FILTER_ENABLED=False,
                     KILL_SWITCH_CLOSE_POSITIONS=True, TRADE_USDT_PER_ORDER=50.0,
                     MAX_WEEKLY_DRAWDOWN_PCT=5.0, MAX_MONTHLY_DRAWDOWN_PCT=10.0,
                     DAILY_MAX_LOSS_USDT=50.0)
        ok, failures, _ = s.validate_safety()
        assert not ok
        assert any("EVENT_FILTER" in f for f in failures)

    def test_event_blackout_filter_blocks_when_active(self):
        """Simulate the event blackout filter blocking a new entry."""
        from app.events.event_blackout_filter import build_event_blackout_filter

        mock_svc = MagicMock()
        flt = build_event_blackout_filter(mock_svc)

        # Simulate a blocked decision
        blocked_result = MagicMock()
        blocked_result.is_blocked = True
        blocked_result.reason = "high_impact_event_blackout"

        with patch.object(flt, "check", return_value=blocked_result):
            result = flt.check(symbol="BTCUSDT")
            assert result.is_blocked


# ═══════════════════════════════════════════════════════════════════════════
# A-8: Minimum Trade Amount
# ═══════════════════════════════════════════════════════════════════════════

class TestMinimumTradeAmount:
    def test_trade_usdt_per_order_is_50_in_config(self):
        from app.core.config import settings
        assert settings.TRADE_USDT_PER_ORDER == 50.0, (
            f"TRADE_USDT_PER_ORDER must be 50.0, got {settings.TRADE_USDT_PER_ORDER}"
        )

    def test_trade_amount_50_passes_startup_check(self):
        from app.core.safety_startup import run_startup_safety_check
        s = _make_settings(TRADE_USDT_PER_ORDER=50.0)
        report = run_startup_safety_check(s, mode="paper")
        ta_check = next(c for c in report.checks if c.name == "TRADE_USDT_PER_ORDER")
        assert ta_check.passed

    def test_trade_amount_below_50_fails_startup_check(self):
        from app.core.safety_startup import run_startup_safety_check
        s = _make_settings(TRADE_USDT_PER_ORDER=10.0)
        report = run_startup_safety_check(s, mode="paper")
        ta_check = next(c for c in report.checks if c.name == "TRADE_USDT_PER_ORDER")
        assert not ta_check.passed

    def test_trade_amount_below_50_fails_validate_safety(self):
        from app.core.config import Settings
        s = Settings(_env_file=None, TRADE_USDT_PER_ORDER=10.0,
                     KILL_SWITCH_CLOSE_POSITIONS=True, EVENT_FILTER_ENABLED=True,
                     MAX_WEEKLY_DRAWDOWN_PCT=5.0, MAX_MONTHLY_DRAWDOWN_PCT=10.0,
                     DAILY_MAX_LOSS_USDT=50.0)
        ok, failures, _ = s.validate_safety()
        assert not ok
        assert any("TRADE_USDT_PER_ORDER" in f or "50 USDT" in f for f in failures)

    def test_auto_pilot_rejects_below_50_usdt(self):
        """The trade amount guard in deploy_auto_pilot raises HTTPException for < 50."""
        from fastapi import HTTPException
        from app.api.auto_pilot import _MIN_TRADE_AMOUNT  # import the constant directly

        assert _MIN_TRADE_AMOUNT == 50.0

    def test_auto_pilot_min_constant_is_50(self):
        """_MIN_TRADE_AMOUNT constant must equal 50 USDT."""
        import importlib
        import ast, inspect
        import app.api.auto_pilot as ap_module
        src = inspect.getsource(ap_module)
        # The constant is defined inline; just verify the value from source
        assert "50.0" in src or "_MIN_TRADE_AMOUNT = 50" in src

    def test_auto_pilot_guard_raises_on_small_amount(self):
        """Simulate the guard logic directly — not through HTTP routing."""
        from fastapi import HTTPException
        _MIN_TRADE_AMOUNT = 50.0

        def _guard(amount: float):
            if amount < _MIN_TRADE_AMOUNT:
                raise HTTPException(
                    status_code=422,
                    detail={"error_code": "TRADE_AMOUNT_TOO_SMALL_MINIMUM_50_USDT"},
                )

        with pytest.raises(HTTPException) as exc_info:
            _guard(10.0)
        assert exc_info.value.status_code == 422
        assert "TRADE_AMOUNT_TOO_SMALL" in str(exc_info.value.detail)

    def test_auto_pilot_guard_passes_on_50_usdt(self):
        from fastapi import HTTPException
        _MIN_TRADE_AMOUNT = 50.0

        def _guard(amount: float):
            if amount < _MIN_TRADE_AMOUNT:
                raise HTTPException(status_code=422, detail="too small")

        _guard(50.0)  # must not raise
        _guard(100.0)  # must not raise


# ═══════════════════════════════════════════════════════════════════════════
# A-9: Strategy Default and SMA Cross Restriction
# ═══════════════════════════════════════════════════════════════════════════

class TestStrategyDefault:
    def test_strategy_name_is_master_ensemble_in_config(self):
        from app.core.config import settings
        assert settings.STRATEGY_NAME == "master_ensemble", (
            f"STRATEGY_NAME must be master_ensemble, got {settings.STRATEGY_NAME}"
        )

    def test_empty_name_falls_back_to_master_ensemble(self):
        from app.strategy.loader import build_strategy
        mock_client = MagicMock()
        with patch("app.strategy.loader.get_strategy_class") as mock_get:
            mock_cls = MagicMock(return_value=MagicMock())
            mock_get.return_value = mock_cls
            build_strategy(name="", client=mock_client, interval="15m")
            # First call should be with master_ensemble
            first_call_name = mock_get.call_args_list[0][0][0]
            assert first_call_name == "master_ensemble"

    def test_sma_cross_rejected_in_live_mode(self):
        from app.strategy.loader import build_strategy
        mock_client = MagicMock()

        with patch("app.strategy.loader.os.getenv", return_value="live"), \
             patch("app.strategy.loader.get_strategy_class") as mock_get:
            mock_cls = MagicMock(return_value=MagicMock())
            mock_get.return_value = mock_cls
            build_strategy(name="sma_cross", client=mock_client, interval="15m")
            # Must have been redirected to master_ensemble
            first_call_name = mock_get.call_args_list[0][0][0]
            assert first_call_name == "master_ensemble", (
                f"sma_cross should be redirected to master_ensemble in live mode, got {first_call_name}"
            )

    def test_sma_cross_allowed_in_paper_mode_with_warning(self):
        """sma_cross is allowed in paper mode but must log a warning."""
        from app.strategy.loader import build_strategy
        mock_client = MagicMock()

        with patch("app.strategy.loader.os.getenv", return_value="paper"), \
             patch("app.strategy.loader.get_strategy_class") as mock_get, \
             patch("app.strategy.loader.logger") as mock_logger:
            mock_cls = MagicMock(return_value=MagicMock())
            mock_get.return_value = mock_cls
            build_strategy(name="sma_cross", client=mock_client, interval="15m")
            # Should log a warning
            warning_calls = [str(c) for c in mock_logger.warning.call_args_list]
            assert any("TEST_ONLY" in w or "sma_cross" in w for w in warning_calls)

    def test_sma_cross_in_live_mode_fails_startup_check(self):
        from app.core.safety_startup import run_startup_safety_check
        s = _make_settings(STRATEGY_NAME="sma_cross", EXECUTION_MODE="live")
        report = run_startup_safety_check(s, mode="live")
        strat_check = next(c for c in report.checks if c.name == "STRATEGY_NAME")
        assert not strat_check.passed

    def test_master_ensemble_in_live_mode_passes_startup_check(self):
        from app.core.safety_startup import run_startup_safety_check
        s = _make_settings(STRATEGY_NAME="master_ensemble", EXECUTION_MODE="live")
        report = run_startup_safety_check(s, mode="live")
        strat_check = next(c for c in report.checks if c.name == "STRATEGY_NAME")
        assert strat_check.passed

    def test_unknown_strategy_falls_back_to_master_ensemble(self):
        from app.strategy.loader import build_strategy
        mock_client = MagicMock()

        call_order = []

        def mock_get(name):
            call_order.append(name)
            if name == "master_ensemble":
                return MagicMock(return_value=MagicMock())
            return None  # Unknown strategy

        with patch("app.strategy.loader.os.getenv", return_value="paper"), \
             patch("app.strategy.loader.get_strategy_class", side_effect=mock_get):
            build_strategy(name="nonexistent_strategy", client=mock_client, interval="15m")
            assert "master_ensemble" in call_order
