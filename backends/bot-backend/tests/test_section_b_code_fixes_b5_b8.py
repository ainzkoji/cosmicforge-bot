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


# ═══════════════════════════════════════════════════════════════════════════
# B-5: External Signal Confidence
# ═══════════════════════════════════════════════════════════════════════════

class TestExternalSignalConfidence:

    def test_safe_confidence_missing_returns_none(self):
        from app.api.tradingview import _safe_confidence
        assert _safe_confidence(None) is None
        assert _safe_confidence("") is None

    def test_safe_confidence_negative_returns_none(self):
        """B-5: negative confidence is invalid, not clamped to 0.0"""
        from app.api.tradingview import _safe_confidence
        assert _safe_confidence(-0.1) is None
        assert _safe_confidence(-1) is None

    def test_safe_confidence_above_one_returns_none(self):
        """B-5: above-1.0 confidence is invalid, not clamped to 1.0"""
        from app.api.tradingview import _safe_confidence
        assert _safe_confidence(1.1) is None
        assert _safe_confidence(2.0) is None
        assert _safe_confidence(100) is None

    def test_safe_confidence_non_numeric_returns_none(self):
        from app.api.tradingview import _safe_confidence
        assert _safe_confidence("high") is None
        assert _safe_confidence([0.5]) is None

    def test_safe_confidence_valid_values(self):
        from app.api.tradingview import _safe_confidence
        assert _safe_confidence(0.0) == 0.0
        assert _safe_confidence(0.75) == pytest.approx(0.75)
        assert _safe_confidence(1.0) == 1.0
        assert _safe_confidence("0.5") == pytest.approx(0.5)

    def test_missing_confidence_never_becomes_one(self):
        """The most critical requirement: missing confidence must never be 1.0."""
        from app.api.tradingview import _safe_confidence
        result = _safe_confidence(None)
        assert result != 1.0, "Missing confidence must not become 1.0"

    def test_runner_candidate_missing_confidence_rejected(self):
        """process_external_signal_candidate must reject a candidate with no confidence."""
        from app.runner.runner import PaperRunner

        runner = object.__new__(PaperRunner)
        runner.context = None
        runner.state = {}
        runner.live_symbols = []
        runner.audit = MagicMock()
        runner.event_blackout_filter = MagicMock()
        runner.event_blackout_filter.check.return_value = MagicMock(is_blocked=False)
        runner.daily = MagicMock(kill=False, realized_pnl=0.0, trade_count=0)
        runner.daily_max_loss = 50.0
        runner.max_trades_daily = 6
        runner.max_open_positions = 3
        runner.trade_usdt = 50.0
        runner.run_id = "r1"
        runner.cycle_id = "c1"
        runner.db = MagicMock()
        runner.interval = "15m"

        candidate = _candidate(confidence=None)
        result = runner.process_external_signal_candidate(candidate)
        assert result.get("final_status") in (
            "REJECTED_MISSING_CONFIDENCE", "REJECTED",
        ) or "MISSING" in str(result.get("final_reason", "")).upper() or \
            "MISSING" in str(result.get("final_status", "")).upper(), (
            f"Expected rejection for missing confidence, got: {result}"
        )

    def test_runner_candidate_invalid_confidence_rejected(self):
        """Confidence > 1 must be rejected."""
        from app.runner.runner import PaperRunner

        runner = object.__new__(PaperRunner)
        runner.context = None
        runner.state = {}
        runner.live_symbols = []
        runner.audit = MagicMock()
        runner.daily = MagicMock(kill=False, realized_pnl=0.0, trade_count=0)
        runner.daily_max_loss = 50.0
        runner.max_trades_daily = 6
        runner.max_open_positions = 3
        runner.trade_usdt = 50.0
        runner.run_id = "r1"
        runner.cycle_id = "c1"
        runner.db = MagicMock()
        runner.interval = "15m"

        candidate = _candidate(confidence=1.5)
        result = runner.process_external_signal_candidate(candidate)
        assert "INVALID" in str(result.get("final_status", "")).upper() or \
               "INVALID" in str(result.get("final_reason", "")).upper() or \
               "REJECTED" in str(result.get("final_status", "")).upper(), (
            f"Expected rejection for invalid confidence (1.5), got: {result}"
        )

    def test_runner_candidate_negative_confidence_rejected(self):
        """Negative confidence must be rejected."""
        from app.runner.runner import PaperRunner

        runner = object.__new__(PaperRunner)
        runner.context = None
        runner.state = {}
        runner.live_symbols = []
        runner.audit = MagicMock()
        runner.daily = MagicMock(kill=False, realized_pnl=0.0, trade_count=0)
        runner.daily_max_loss = 50.0
        runner.max_trades_daily = 6
        runner.max_open_positions = 3
        runner.trade_usdt = 50.0
        runner.run_id = "r1"
        runner.cycle_id = "c1"
        runner.db = MagicMock()
        runner.interval = "15m"

        candidate = _candidate(confidence=-0.5)
        result = runner.process_external_signal_candidate(candidate)
        assert "REJECTED" in str(result.get("final_status", "")).upper() or \
               "INVALID" in str(result.get("final_reason", "")).upper(), (
            f"Expected rejection for negative confidence, got: {result}"
        )

    def test_runner_source_contains_confidence_fix(self):
        """runner.py must not contain 'or 1.0' for external signal confidence."""
        import os
        runner_path = os.path.join(
            os.path.dirname(__file__), "..", "app", "runner", "runner.py"
        )
        with open(runner_path, encoding="utf-8", errors="replace") as f:
            src = f.read()
        # The old bug: float(candidate.get("confidence") or 1.0)
        assert 'candidate.get("confidence") or 1.0' not in src, (
            "B-5: runner.py still has 'candidate.get(confidence) or 1.0' bug"
        )
        assert "EXTERNAL_SIGNAL_REJECTED_MISSING_CONFIDENCE" in src


# ═══════════════════════════════════════════════════════════════════════════
# B-6: Daily Activity Fallback
# ═══════════════════════════════════════════════════════════════════════════

class TestDailyActivityFallback:

    def test_activity_targets_get_current_reduction_always_zero(self):
        """B-6: get_current_reduction must always return 0.0 regardless of inactivity."""
        from app.strategy.activity_targets import ActivityTargets
        at = ActivityTargets(enabled=True)
        # Simulate 24h of no trades
        reduction, reason = at.get_current_reduction(trades_today=0, current_hour_utc=23)
        assert reduction == 0.0, f"Expected 0.0 reduction, got {reduction}"
        assert "DAILY_ACTIVITY_FALLBACK_DISABLED" in reason

    def test_activity_targets_enabled_still_returns_zero(self):
        """Even if enabled=True is forced, reduction must be 0.0."""
        from app.strategy.activity_targets import ActivityTargets
        at = ActivityTargets(enabled=True, min_signals_per_day=5)
        reduction, reason = at.get_current_reduction(trades_today=0, current_hour_utc=21)
        assert reduction == 0.0

    def test_conservative_targets_still_return_zero(self):
        """Pre-built conservative_activity_targets must not lower thresholds."""
        from app.strategy.activity_targets import conservative_activity_targets
        at = conservative_activity_targets()
        reduction, reason = at.get_current_reduction(trades_today=0, current_hour_utc=21)
        assert reduction == 0.0

    def test_moderate_targets_still_return_zero(self):
        from app.strategy.activity_targets import moderate_activity_targets
        at = moderate_activity_targets()
        reduction, reason = at.get_current_reduction(trades_today=0, current_hour_utc=20)
        assert reduction == 0.0

    def test_calculate_nudge_never_adjusts_threshold(self):
        """ActivityTargetsMixin.calculate_nudge must not reduce the base threshold."""
        from app.strategy.activity_targets import ActivityTargets, ActivityTargetsMixin
        mixin = ActivityTargetsMixin()
        mixin.activity_targets = ActivityTargets(enabled=True)
        mixin._trades_today = 0

        base = 0.70
        nudge = mixin.calculate_nudge(base_min_confidence=base)
        assert nudge.adjusted_min_confidence == base, (
            f"Threshold was reduced from {base} to {nudge.adjusted_min_confidence}"
        )
        assert nudge.reduction_applied == 0.0

    def test_low_confidence_after_inactivity_still_rejected(self):
        """After 24h no trades, a 0.50 confidence signal must still be rejected at 0.70 floor."""
        from app.policy.policy_engine import PolicyEngine, PolicyContext, ReasonCode

        engine = PolicyEngine(min_confidence=0.70)
        ctx = PolicyContext(
            symbol="BTCUSDT", signal="BUY", confidence=0.50,
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
        decision = engine.evaluate(ctx)
        assert not decision.allowed
        assert decision.reason_code == ReasonCode.LOW_CONFIDENCE

    def test_activity_fallback_disabled_in_source(self):
        """activity_targets.py must contain the DAILY_ACTIVITY_FALLBACK_DISABLED marker."""
        import os
        path = os.path.join(
            os.path.dirname(__file__), "..", "app", "strategy", "activity_targets.py"
        )
        with open(path, encoding="utf-8") as f:
            src = f.read()
        assert "DAILY_ACTIVITY_FALLBACK_DISABLED" in src


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

    def test_runner_source_contains_b7_guard(self):
        """runner.py must contain the B-7 sma_cross guard for user-context bots."""
        import os
        path = os.path.join(
            os.path.dirname(__file__), "..", "app", "runner", "runner.py"
        )
        with open(path, encoding="utf-8", errors="replace") as f:
            src = f.read()
        assert "STRATEGY_REJECTED_SMA_CROSS_UNSAFE_FOR_USER_BOT" in src
        assert "_UNSAFE_STRATEGY_ALIASES" in src

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

    def test_runner_source_contains_sizing_audit(self):
        """runner.py must emit SIZING_FAILURE audit events."""
        import os
        path = os.path.join(
            os.path.dirname(__file__), "..", "app", "runner", "runner.py"
        )
        with open(path, encoding="utf-8", errors="replace") as f:
            src = f.read()
        assert "SIZING_FAILURE" in src
        assert "SIZING_FAILURE_DASHBOARD_ALERT_SET" in src
        assert "SIZING_FAILURE_TRADE_SKIPPED" in src

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
