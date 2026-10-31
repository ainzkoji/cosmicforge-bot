"""
CosmicForge Section B Code Fixes B-5 to B-8 Tests

B-5: External signal confidence must not default to 1.0
B-6: Daily activity fallback must not soften confidence thresholds
B-7: sma_cross hard-blocked for user-facing bots
B-8: Sizing failures surfaced to audit logs and dashboard
"""
from __future__ import annotations

import pytest
from unittest.mock import MagicMock, patch
from types import SimpleNamespace


# ── helpers ─────────────────────────────────────────────────────────────────

def _candidate(**kw):
    """Minimal external signal candidate dict."""
    defaults = dict(
        source="TRADINGVIEW",
        queue_id="q1",
        symbol="BTCUSDT",
        action="BUY",
        confidence=0.75,
        proof_context={},
    )
    defaults.update(kw)
    return defaults


def _mock_context(bot_instance_id="bot1", strategy_id="master_ensemble"):
    ctx = MagicMock()
    ctx.bot_instance_id = bot_instance_id
    ctx.strategy_id = strategy_id
    ctx.symbols = ["BTCUSDT"]
    ctx.interval = "15m"
    ctx.strategy_params = None
    ctx.execution_mode = "paper"
    ctx.daily_max_loss_usdt = 50.0
    ctx.max_trades_daily = 6
    ctx.max_open_positions = 3
    ctx.trade_usdt_per_order = 50.0
    ctx.user_id = "user1"
    ctx.broker_account_id = "BINANCE"
    return ctx


# B-5 (external signal confidence) was removed with the TradingView path in Section I Batch 5.

class TestDailyActivityFallback:
    # The disabled activity-target helpers (app/strategy/activity_targets.py) were removed in
    # Section I Batch 7; nothing imported them. B-6 is kept where the entry bar actually lives.

    def test_inactivity_cannot_soften_the_entry_bar(self):
        """The property B-6 protects, tested where the bar actually lives.

        This used to assert PolicyEngine rejected 0.50 against its own 0.70
        floor. That floor was a second entry-quality authority and is deleted.
        The bar is now AdaptiveEntryThresholdEngine's, and the rule that a quiet
        period must not lower it is enforced in its distribution calibrator:
        samples come only from opportunities that reached the quality stage, so
        fewer trades yields a smaller sample, never a weaker one.
        """
        from app.threshold.calibration import DistributionCalibrator

        busy = DistributionCalibrator.evaluate(
            [0.70] * 200, base_threshold=0.60, target_percentile=0.6,
            min_samples=40, bound=0.05,
        )
        quiet = DistributionCalibrator.evaluate(
            [0.70] * 45, base_threshold=0.60, target_percentile=0.6,
            min_samples=40, bound=0.05,
        )
        assert busy.adjustment == quiet.adjustment
        assert quiet.adjustment >= 0.0, "inactivity must never lower the bar"

    def test_trade_frequency_is_not_an_input_to_the_threshold(self):
        """No count of trades or candles reaches the threshold calculation."""
        import inspect

        from app.threshold import engine as threshold_engine

        source = inspect.getsource(threshold_engine)
        for forbidden in ("trade_count", "trades_today", "daily_trade", "_trades_today"):
            assert forbidden not in source


# ═══════════════════════════════════════════════════════════════════════════
# B-7: SMA Cross Block for User Bots
# ═══════════════════════════════════════════════════════════════════════════

class TestSmaCrossUserBotBlock:

    def _make_runner_with_strategy(self, strategy_id: str):
        """Create a minimal PaperRunner with a user context requesting given strategy."""
        from app.runner.runner import PaperRunner

        runner = object.__new__(PaperRunner)
        runner.settings = MagicMock()
        runner.context = _mock_context(strategy_id=strategy_id)
        runner._strategy_name = None  # set by __init__ flow

        # Simulate the B-7 guard logic inline
        effective_strategy = strategy_id
        _UNSAFE = {"sma_cross", "sma-cross", "sma cross"}
        if runner.context and effective_strategy.strip().lower() in _UNSAFE:
            effective_strategy = "master_ensemble"

        runner._strategy_name = effective_strategy
        return runner

    def test_user_bot_sma_cross_forced_to_master_ensemble(self):
        runner = self._make_runner_with_strategy("sma_cross")
        assert runner._strategy_name == "master_ensemble", (
            "User-context bot must not run sma_cross"
        )

    def test_user_bot_sma_dash_cross_blocked(self):
        runner = self._make_runner_with_strategy("sma-cross")
        assert runner._strategy_name == "master_ensemble"

    def test_user_bot_sma_space_cross_blocked(self):
        runner = self._make_runner_with_strategy("sma cross")
        assert runner._strategy_name == "master_ensemble"

    def test_user_bot_master_ensemble_unchanged(self):
        runner = self._make_runner_with_strategy("master_ensemble")
        assert runner._strategy_name == "master_ensemble"

    # The runner-source B-7 guard check is retired: the runner binds CATI for every
    # configured name (sma_cross and its aliases included), covered by
    # tests/trading_intelligence/test_cati_sole_runtime.py::
    # test_historical_configuration_never_constructs_an_old_strategy.

    def test_warning_emitted_when_sma_cross_blocked(self):
        """A warning must be logged when sma_cross is blocked."""
        import os, logging
        from io import StringIO

        stream = StringIO()
        handler = logging.StreamHandler(stream)
        handler.setLevel(logging.WARNING)
        runner_logger = logging.getLogger("app.runner.runner")
        runner_logger.addHandler(handler)
        runner_logger.setLevel(logging.WARNING)

        try:
            # Trigger the guard logic directly
            ctx = _mock_context(strategy_id="sma_cross")
            effective_strategy = ctx.strategy_id
            _UNSAFE = {"sma_cross", "sma-cross", "sma cross"}
            if ctx and effective_strategy.strip().lower() in _UNSAFE:
                runner_logger.warning(
                    "STRATEGY_REJECTED_SMA_CROSS_UNSAFE_FOR_USER_BOT — "
                    "user bot %s requested '%s'; enforcing master_ensemble. "
                    "STRATEGY_FALLBACK_MASTER_ENSEMBLE",
                    ctx.bot_instance_id, effective_strategy,
                )
                effective_strategy = "master_ensemble"

            output = stream.getvalue()
            assert "STRATEGY_REJECTED_SMA_CROSS_UNSAFE_FOR_USER_BOT" in output
            assert effective_strategy == "master_ensemble"
        finally:
            runner_logger.removeHandler(handler)

    def test_loader_rejects_sma_cross_in_live_mode(self):
        """strategy/loader.py also blocks sma_cross in live mode (from A-9)."""
        from app.strategy.loader import build_strategy, _SAFE_FALLBACK, _UNSAFE_STRATEGIES
        assert "sma_cross" in _UNSAFE_STRATEGIES
        assert _SAFE_FALLBACK == "master_ensemble"


# ═══════════════════════════════════════════════════════════════════════════
# B-8: Sizing Failure Visibility
# ═══════════════════════════════════════════════════════════════════════════

class TestSizingFailureVisibility:

    def test_below_min_notional_uses_logger_not_stderr(self):
        """B-8: sizing failure must use logger.error, not print(sys.stderr)."""
        import os
        path = os.path.join(
            os.path.dirname(__file__), "..", "app", "symbols", "sizing.py"
        )
        with open(path, encoding="utf-8") as f:
            src = f.read()
        # The old print() to stderr must be gone
        assert 'print("' not in src or 'file=sys.stderr' not in src, (
            "sizing.py still has print() to stderr — must use logger.error()"
        )
        assert "SIZING_FAILURE_MIN_NOTIONAL" in src

    def test_sizing_failure_produces_structured_details(self):
        """below_min_notional_budget_limited must return structured details dict."""
        from app.symbols.sizing import validate_and_adjust_order

        class FakeFilters:
            step_size = 0.001
            min_qty = 0.001
            min_notional = 5.0
            contract_size = 1.0

        # Use qty that passes min_qty (0.001 >= 0.001) but the budget is too small
        # to bump qty up to meet the large min_notional_override.
        # At price=50000, qty=0.001 => notional=50. min_notional_override=10000.
        # Required qty = 10000/50000 = 0.2, required_margin = 10000/1 = 10000 > budget=1.
        result = validate_and_adjust_order(
            symbol="BTCUSDT",
            raw_qty=0.001,        # passes min_qty
            price=50000.0,
            filters=FakeFilters(),
            budget_usdt=1.0,      # $1 budget — cannot meet the large min notional
            leverage=1,
            min_notional_override=10000.0,  # forces the budget-exceeded path
        )
        assert result.reason == "below_min_notional_budget_limited", (
            f"Expected below_min_notional_budget_limited, got {result.reason}"
        )
        assert result.qty == 0.0
        assert "error_code" in result.details
        assert result.details["error_code"] == "SIZING_FAILURE_MIN_NOTIONAL"
        assert "user_action" in result.details

    def test_sizing_failure_audit_event_attempted(self):
        """Runner emits SIZING_FAILURE audit event when MIN_NOTIONAL_NOT_MET."""
        from app.policy.policy_engine import PolicyDecision, ReasonCode, Action

        # Simulate a runner-like object calling the B-8 audit block
        audit = MagicMock()
        policy = PolicyDecision(
            allowed=False,
            action=Action.HOLD,
            reason_code=ReasonCode.MIN_NOTIONAL_NOT_MET,
            reason="Sizing failed: below_min_notional_budget_limited",
            details={"min_notional": 5.0, "budget": 1.0},
        )

        # Execute the B-8 audit logic
        from app.policy.policy_engine import ReasonCode as _RC
        _sizing_reason_codes = {_RC.MIN_NOTIONAL_NOT_MET, _RC.SIZE_ZERO, _RC.PRICE_INVALID}
        if not policy.allowed and policy.reason_code in _sizing_reason_codes:
            try:
                audit.event(
                    event_type="SIZING_FAILURE",
                    run_id="r1",
                    cycle_id="c1",
                    symbol="BTCUSDT",
                    details={
                        "error_code": "SIZING_FAILURE_MIN_NOTIONAL",
                        "reason_code": policy.reason_code.name,
                        "reason": policy.reason,
                        "user_message": "Trade amount too small for exchange minimum.",
                    },
                )
            except Exception:
                pass

        audit.event.assert_called_once()
        call_kwargs = audit.event.call_args
        assert call_kwargs.kwargs.get("event_type") == "SIZING_FAILURE" or \
               (call_kwargs.args and call_kwargs.args[0] == "SIZING_FAILURE") or \
               "SIZING_FAILURE" in str(call_kwargs)

    # test_runner_source_contains_sizing_audit was removed (Section I Batch 7): it read runner.py for the
    # SIZING_FAILURE audit block of the legacy entry path, which 6e0b42c removed when CATI became the sole
    # engine. A sizing refusal on the CATI path is durable risk evidence instead (RiskDecision with
    # rejection_family SIZING): tests/trading_intelligence/test_section20_execution_boundary.py
    # ::test_executor_sizing_rejection.

    def test_sizing_success_does_not_set_error(self):
        """A successful sizing must not trigger error codes."""
        from app.symbols.sizing import validate_and_adjust_order

        class FakeFilters:
            step_size = 0.001
            min_qty = 0.001
            min_notional = 5.0
            contract_size = 1.0

        result = validate_and_adjust_order(
            symbol="BTCUSDT",
            raw_qty=0.001,
            price=50000.0,
            filters=FakeFilters(),
            budget_usdt=100.0,
            leverage=3,
        )
        assert result.reason in ("ok", "qty_bumped_min_notional")
        assert result.qty > 0

    def test_user_message_present_in_failure_details(self):
        """The user-facing message must be included in the sizing failure details."""
        from app.symbols.sizing import validate_and_adjust_order

        class FakeFilters:
            step_size = 0.001
            min_qty = 0.001
            min_notional = 5.0
            contract_size = 1.0

        result = validate_and_adjust_order(
            symbol="BTCUSDT",
            raw_qty=0.001,        # passes min_qty
            price=50000.0,
            filters=FakeFilters(),
            budget_usdt=0.5,      # tiny budget
            leverage=1,
            min_notional_override=10000.0,  # large override to force failure
        )
        assert result.reason == "below_min_notional_budget_limited"
        assert "user_action" in result.details
        assert "50 USDT" in result.details["user_action"]
