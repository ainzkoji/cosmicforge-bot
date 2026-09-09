"""Tests for the AdaptiveEntryThresholdEngine rebuild.

The failure these tests exist to prevent is specific and has already happened
once: a threshold band configured so that no computed value could survive, with
two operator-tunable settings that could not affect anything and no signal that
either fact was true. So the assertions here are less about arithmetic than
about *authority* -- who is allowed to decide the entry bar, and whether the
system can still lie about it.
"""
from __future__ import annotations

import ast
import inspect
import sqlite3
import tempfile
import textwrap
from pathlib import Path
from types import SimpleNamespace

import pytest

from app.decision.decision_engine import EntryQualityDecision, TradingDecisionEngine
from app.decision.opportunity import NoOpportunity, build_opportunity
from app.decision.reasons import QualityReason
from app.threshold.calibration import (
    DistributionCalibrator,
    PerformanceCalibrator,
)
from app.threshold.contracts import (
    AdaptiveThresholdDecision,
    AdaptiveThresholdInput,
    CalibrationStatus,
    ExpertEvidence,
    HTFContext,
    MarketQualityContext,
    RegimeContext,
    ThresholdMode,
    ThresholdStatus,
    VolatilityContext,
    experts_from_votes,
)
from app.threshold.diagnostics import (
    ADAPTIVE_THRESHOLD_EFFECTIVELY_STATIC,
    THRESHOLD_COMPONENTS_DO_NOT_RECONCILE,
    detect_inert_engine,
    health_check,
    threshold_stats,
)
from app.threshold.engine import (
    AdaptiveEntryThresholdEngine,
    REASON_MARKET_DATA_STALE,
    REASON_REGIME_HARD_BLOCK,
    agreement_component,
    htf_component,
)
from app.threshold.migration import (
    DISPOSITIONS,
    LEGACY_SETTINGS,
    deprecation_warnings,
    global_scope_from_settings,
    legacy_inventory,
    startup_report,
)
from app.threshold.policy import (
    DEFAULTS,
    EffectiveThresholdPolicy,
    ThresholdPolicyError,
    resolve_threshold_policy,
    validate_policy,
)
from app.threshold.state import ThresholdState, ThresholdStateStore, percentile

BOT = "bot_threshold_test"


# ── Helpers ──────────────────────────────────────────────────────────────────


def make_policy(**overrides) -> EffectiveThresholdPolicy:
    return resolve_threshold_policy(
        scopes=[("GLOBAL", overrides)] if overrides else (),
        base_threshold=overrides.pop("base_threshold", 0.60)
        if "base_threshold" in overrides
        else 0.60,
    )


def make_request(**overrides) -> AdaptiveThresholdInput:
    payload: dict = dict(
        bot_instance_id=BOT,
        symbol="BTCUSDT",
        timeframe="15m",
        strategy_version="2.0.0",
        side="BUY",
        opportunity_confidence=0.62,
        buy_score=0.62,
        sell_score=0.10,
        consensus=0.62,
        closed_candle_time=1_700_000_000_000,
        evaluated_at="2026-09-09T00:00:00+00:00",
        regime=RegimeContext(regime="RANGE", regime_confidence=0.8),
    )
    payload.update(overrides)
    return AdaptiveThresholdInput(**payload)


def expert(name: str, signal: str, confidence: float, *, eligible=True, executed=True, weight=1.0):
    return ExpertEvidence(
        strategy=name,
        eligible=eligible,
        executed=executed,
        signal=signal,
        confidence=confidence,
        raw_score=confidence,
        weight=weight,
        weighted_contribution=weight * confidence if signal in {"BUY", "SELL"} else 0.0,
    )


def _code_only(source: str) -> str:
    """Strip comments and docstrings so assertions cannot match prose.

    Without this, a test for "the runner does not apply a threshold floor" can
    be satisfied or broken by a comment that merely mentions one.
    """
    tree = ast.parse(textwrap.dedent(source))
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef, ast.Module)):
            if (
                node.body
                and isinstance(node.body[0], ast.Expr)
                and isinstance(node.body[0].value, ast.Constant)
                and isinstance(node.body[0].value.value, str)
            ):
                node.body.pop(0)
    return ast.unparse(tree)


# ═════════════════════════════════════════════════════════════════════════════
# AUTHORITY — exactly one component may decide the entry bar
# ═════════════════════════════════════════════════════════════════════════════


class TestSingleAuthority:
    def test_decision_engine_cannot_resolve_a_threshold(self):
        """The old resolve_threshold() was one of the competing authorities."""
        assert not hasattr(TradingDecisionEngine, "resolve_threshold")

    def test_decision_engine_takes_no_threshold_configuration(self):
        """Every constructor argument it used to take was a threshold control."""
        params = set(inspect.signature(TradingDecisionEngine.__init__).parameters)
        assert params == {"self"}, f"unexpected threshold configuration: {params}"

    def test_decision_engine_performs_exactly_one_comparison(self):
        source = _code_only(inspect.getsource(TradingDecisionEngine))
        comparisons = source.count(">= threshold")
        assert comparisons == 1, f"expected one confidence comparison, found {comparisons}"

    def test_runner_no_longer_raises_the_threshold(self):
        """runner.py:3883 applied max(adaptive_gate, 0.70). That was THE bug."""
        source = _code_only(
            Path(inspect.getsourcefile(__import__("app.runner.runner", fromlist=["x"]))).read_text(
                encoding="utf-8"
            )
        )
        assert "min_confidence_gate=max(" not in source
        assert "min_confidence_gate = max(" not in source

    def test_master_ensemble_applies_no_threshold_floor(self):
        import app.strategy.master_ensemble as me

        source = _code_only(Path(me.__file__).read_text(encoding="utf-8"))
        assert "ENSEMBLE_MIN_THRESHOLD_FLOOR" not in source
        assert "max(_threshold_val" not in source

    def test_effective_bot_policy_carries_no_threshold_floor(self):
        from app.runner.effective_policy import EffectiveBotPolicy

        fields = set(EffectiveBotPolicy.__dataclass_fields__)
        assert "confidence_absolute_floor" not in fields
        # It records which policy governed the run, not a number of its own.
        assert "threshold_policy_hash" in fields

    def test_only_the_engine_produces_an_evaluated_decision(self):
        """A threshold decision with a number can only come from the engine."""
        from app.threshold.contracts import not_evaluated

        with pytest.raises(ValueError):
            not_evaluated(
                bot_instance_id=BOT,
                symbol="BTCUSDT",
                timeframe="15m",
                engine_version="1.0.0",
                mode=ThresholdMode.ADAPTIVE,
                reason="x",
                status=ThresholdStatus.EVALUATED,
            )


# ═════════════════════════════════════════════════════════════════════════════
# BOUNDS — one band, and no hidden floor above the max
# ═════════════════════════════════════════════════════════════════════════════


class TestBounds:
    def test_final_threshold_respects_the_band(self):
        policy = make_policy(min_threshold=0.55, max_threshold=0.60, base_threshold=0.60)
        engine = AdaptiveEntryThresholdEngine()
        decision = engine.evaluate(
            make_request(regime=RegimeContext(regime="STRONG_TREND", regime_confidence=1.0)),
            policy,
        )
        assert 0.55 <= decision.final_threshold <= 0.60
        assert decision.in_bounds()

    def test_min_above_max_is_rejected(self):
        with pytest.raises(ThresholdPolicyError) as exc:
            resolve_threshold_policy(
                scopes=[("GLOBAL", {"min_threshold": 0.80, "max_threshold": 0.60})],
                base_threshold=0.70,
            )
        assert exc.value.code == "THRESHOLD_MIN_ABOVE_MAX"

    def test_base_outside_band_is_rejected(self):
        with pytest.raises(ThresholdPolicyError) as exc:
            resolve_threshold_policy(
                scopes=[("GLOBAL", {"min_threshold": 0.50, "max_threshold": 0.60})],
                base_threshold=0.90,
            )
        assert exc.value.code == "THRESHOLD_BASE_OUTSIDE_BAND"

    def test_the_original_defect_is_now_unconfigurable(self):
        """A floor above the band's ceiling is exactly the 0.70 failure.

        Under the old stack MIN_CONFIDENCE_THRESHOLD=0.70 sat above a dynamic
        cap of 0.65 and silently won. There is no separate floor to configure
        any more, so the same intent can only be expressed as a base outside the
        band -- and that is refused at startup.
        """
        with pytest.raises(ThresholdPolicyError):
            resolve_threshold_policy(
                scopes=[("GLOBAL", {"min_threshold": 0.40, "max_threshold": 0.65})],
                base_threshold=0.70,
            )

    def test_unknown_policy_field_is_rejected_not_ignored(self):
        """A typo that is silently ignored is how a setting becomes inert."""
        with pytest.raises(ThresholdPolicyError) as exc:
            resolve_threshold_policy(
                scopes=[("GLOBAL", {"ensemble_min_threshold_floor": 0.55})],
                base_threshold=0.60,
            )
        assert exc.value.code == "UNKNOWN_POLICY_FIELD"

    def test_negative_rate_limits_are_rejected(self):
        with pytest.raises(ThresholdPolicyError) as exc:
            resolve_threshold_policy(
                scopes=[("GLOBAL", {"max_step_up": -0.1})], base_threshold=0.60
            )
        assert exc.value.code == "RATE_LIMIT_INVALID"

    def test_invalid_smoothing_coefficient_is_rejected(self):
        with pytest.raises(ThresholdPolicyError) as exc:
            resolve_threshold_policy(
                scopes=[("GLOBAL", {"smoothing_alpha": 1.5})], base_threshold=0.60
            )
        assert exc.value.code == "SMOOTHING_ALPHA_INVALID"

    def test_model_mode_is_rejected_not_silently_downgraded(self):
        with pytest.raises(ThresholdPolicyError) as exc:
            resolve_threshold_policy(
                scopes=[("GLOBAL", {"mode": ThresholdMode.MODEL})], base_threshold=0.60
            )
        assert exc.value.code == "THRESHOLD_MODE_NOT_IMPLEMENTED"

    def test_all_zero_adjustment_bounds_rejected_in_adaptive_mode(self):
        """An adaptive engine that cannot adapt is a static one wearing a label."""
        zeros = {
            name: 0.0
            for name in DEFAULTS
            if name.endswith("_bound")
        }
        with pytest.raises(ThresholdPolicyError) as exc:
            resolve_threshold_policy(scopes=[("GLOBAL", zeros)], base_threshold=0.60)
        assert exc.value.code == "THRESHOLD_ADJUSTMENTS_ALL_ZERO"


# ═════════════════════════════════════════════════════════════════════════════
# REGIME
# ═════════════════════════════════════════════════════════════════════════════


class TestRegime:
    def test_hard_block_regime_returns_not_evaluated_with_null(self):
        policy = make_policy()
        engine = AdaptiveEntryThresholdEngine()
        decision = engine.evaluate(
            make_request(regime=RegimeContext(regime="LOW_VOLATILITY_CHOP")), policy
        )
        assert decision.status == ThresholdStatus.HARD_BLOCKED
        assert decision.final_threshold is None
        assert decision.reason == REASON_REGIME_HARD_BLOCK
        assert decision.passed is None

    def test_hard_block_is_not_expressed_as_an_unreachable_threshold(self):
        policy = make_policy()
        engine = AdaptiveEntryThresholdEngine()
        decision = engine.evaluate(
            make_request(regime=RegimeContext(regime="LOW_VOLATILITY_CHOP")), policy
        )
        assert decision.final_threshold != 0.99
        assert decision.final_threshold is None

    def test_threshold_eligible_regimes_calculate_normally(self):
        policy = make_policy()
        engine = AdaptiveEntryThresholdEngine()
        for regime in ("STRONG_TREND", "WEAK_TREND", "RANGE", "HIGH_VOLATILITY"):
            decision = engine.evaluate(
                make_request(regime=RegimeContext(regime=regime, regime_confidence=0.9)),
                policy,
            )
            assert decision.status == ThresholdStatus.EVALUATED, regime
            assert decision.final_threshold is not None, regime

    def test_loss_driving_regime_demands_more_evidence_than_the_best_one(self):
        policy = make_policy()
        engine = AdaptiveEntryThresholdEngine()
        strong = engine.evaluate(
            make_request(regime=RegimeContext(regime="STRONG_TREND", regime_confidence=1.0)),
            policy,
        )
        engine.state_store.clear()
        ranging = engine.evaluate(
            make_request(regime=RegimeContext(regime="RANGE", regime_confidence=1.0)),
            policy,
        )
        assert strong.regime_adjustment > ranging.regime_adjustment

    def test_low_regime_confidence_reduces_the_regime_influence(self):
        policy = make_policy()
        engine = AdaptiveEntryThresholdEngine()
        confident = engine.evaluate(
            make_request(regime=RegimeContext(regime="STRONG_TREND", regime_confidence=1.0)),
            policy,
        )
        engine.state_store.clear()
        unsure = engine.evaluate(
            make_request(regime=RegimeContext(regime="STRONG_TREND", regime_confidence=0.0)),
            policy,
        )
        assert unsure.regime_adjustment < confident.regime_adjustment


# ═════════════════════════════════════════════════════════════════════════════
# VOLATILITY
# ═════════════════════════════════════════════════════════════════════════════


class TestVolatility:
    @pytest.mark.parametrize(
        "atr_percentile,expect_positive",
        [(0.98, True), (0.50, False), (0.02, True)],
    )
    def test_both_tails_demand_more_evidence(self, atr_percentile, expect_positive):
        policy = make_policy()
        engine = AdaptiveEntryThresholdEngine()
        decision = engine.evaluate(
            make_request(volatility=VolatilityContext(atr_percentile=atr_percentile)), policy
        )
        if expect_positive:
            assert decision.volatility_adjustment > 0
        else:
            assert decision.volatility_adjustment == pytest.approx(0.0, abs=1e-9)

    def test_volatility_adjustment_is_bounded(self):
        policy = make_policy(volatility_bound=0.02)
        engine = AdaptiveEntryThresholdEngine()
        decision = engine.evaluate(
            make_request(volatility=VolatilityContext(atr_percentile=1.0, gap_detected=True)),
            policy,
        )
        assert abs(decision.volatility_adjustment) <= 0.02 + 1e-9

    def test_missing_volatility_contributes_nothing(self):
        policy = make_policy()
        engine = AdaptiveEntryThresholdEngine()
        decision = engine.evaluate(make_request(volatility=VolatilityContext()), policy)
        assert decision.volatility_score is None
        assert decision.volatility_adjustment == 0.0


# ═════════════════════════════════════════════════════════════════════════════
# EXPERT AGREEMENT
# ═════════════════════════════════════════════════════════════════════════════


class TestExpertAgreement:
    def test_strong_agreement_lowers_the_required_evidence(self):
        strong = [expert(f"s{i}", "BUY", 0.9) for i in range(4)]
        score, adjustment, _ = agreement_component(strong, 0.08)
        assert score > 0.8
        assert adjustment < 0

    def test_mixed_agreement_raises_it(self):
        mixed = [expert("a", "BUY", 0.6), expert("b", "SELL", 0.6), expert("c", "HOLD", 0.0)]
        score, adjustment, _ = agreement_component(mixed, 0.08)
        assert score == pytest.approx(0.0, abs=1e-9)
        assert adjustment > 0

    def test_not_run_expert_is_not_counted_as_hold(self):
        """A strategy that failed to run did not vote. Counting its silence as a
        HOLD would fabricate a vote that nobody cast."""
        ran = [expert("a", "BUY", 0.9), expert("b", "BUY", 0.9)]
        with_hold = ran + [expert("c", "HOLD", 0.0)]
        with_not_run = ran + [expert("c", "NOT_RUN", 0.0, executed=False)]

        _, hold_adj, hold_break = agreement_component(with_hold, 0.08)
        _, notrun_adj, notrun_break = agreement_component(with_not_run, 0.08)

        assert hold_break["neutral"] == 1
        assert notrun_break["neutral"] == 0
        assert notrun_break["not_run"] == 1

    def test_disabled_expert_is_in_neither_numerator_nor_denominator(self):
        """The regime deactivated it; it was never asked for an opinion."""
        base = [expert("a", "BUY", 0.9), expert("b", "BUY", 0.9)]
        with_disabled = base + [
            expert("c", "DISABLED", 0.0, eligible=False, executed=False, weight=1.0)
        ]
        base_score, _, _ = agreement_component(base, 0.08)
        disabled_score, _, breakdown = agreement_component(with_disabled, 0.08)
        assert disabled_score == pytest.approx(base_score)
        assert breakdown["eligible"] == 2

    def test_opposition_reduces_the_agreement_score(self):
        supporting_only, _, _ = agreement_component(
            [expert("a", "BUY", 0.9), expert("b", "BUY", 0.9)], 0.08
        )
        with_opposition, _, _ = agreement_component(
            [expert("a", "BUY", 0.9), expert("b", "BUY", 0.9), expert("c", "SELL", 0.9)], 0.08
        )
        assert with_opposition < supporting_only

    def test_experts_from_votes_preserves_the_seven(self):
        from app.strategy.master_ensemble import _BASE_WEIGHTS

        all_names = sorted(_BASE_WEIGHTS)
        evidence = experts_from_votes(
            [("supertrend", "BUY", 0.9), ("sma_cross", "HOLD", 0.0)],
            eligible=["supertrend", "sma_cross", "trend_pullback"],
            all_strategies=all_names,
            weights=_BASE_WEIGHTS,
        )
        assert len(evidence) == 7
        by_name = {e.strategy: e for e in evidence}
        assert by_name["supertrend"].executed and by_name["supertrend"].signal == "BUY"
        assert by_name["sma_cross"].executed and by_name["sma_cross"].signal == "HOLD"
        # Eligible but did not produce a vote.
        assert by_name["trend_pullback"].eligible
        assert by_name["trend_pullback"].signal == "NOT_RUN"
        # Not eligible for this regime at all.
        assert not by_name["vwap_reversion"].eligible
        assert by_name["vwap_reversion"].signal == "DISABLED"

    def test_no_experts_contributes_nothing(self):
        score, adjustment, _ = agreement_component([], 0.08)
        assert score is None and adjustment == 0.0


# ═════════════════════════════════════════════════════════════════════════════
# HTF
# ═════════════════════════════════════════════════════════════════════════════


class TestHTF:
    def test_aligned_htf_lowers_and_opposed_raises(self):
        aligned = htf_component(
            HTFContext(timeframe="4h", direction="BUY", strength=0.8), "BUY", 0.05
        )
        opposed = htf_component(
            HTFContext(timeframe="4h", direction="SELL", strength=0.8), "BUY", 0.05
        )
        assert aligned[1] < 0 < opposed[1]
        assert aligned[0] > 0 > opposed[0]

    def test_neutral_htf_contributes_nothing(self):
        score, adjustment = htf_component(
            HTFContext(timeframe="4h", direction="NEUTRAL"), "BUY", 0.05
        )
        assert score == 0.0 and adjustment == 0.0

    def test_stale_htf_is_unavailable_not_neutral(self):
        """Reporting a stale read as neutral would assert something unverified."""
        score, adjustment = htf_component(
            HTFContext(timeframe="4h", direction="BUY", strength=0.9, is_fresh=False),
            "BUY",
            0.05,
        )
        assert score is None and adjustment == 0.0

    def test_htf_adjustment_is_bounded(self):
        _, adjustment = htf_component(
            HTFContext(timeframe="4h", direction="SELL", strength=1.0), "BUY", 0.03
        )
        assert abs(adjustment) <= 0.03 + 1e-9


# ═════════════════════════════════════════════════════════════════════════════
# MARKET QUALITY
# ═════════════════════════════════════════════════════════════════════════════


class TestMarketQuality:
    def test_stale_data_hard_blocks_rather_than_raising_the_bar(self):
        policy = make_policy()
        engine = AdaptiveEntryThresholdEngine()
        decision = engine.evaluate(
            make_request(market_quality=MarketQualityContext(data_stale=True)), policy
        )
        assert decision.status == ThresholdStatus.HARD_BLOCKED
        assert decision.reason == REASON_MARKET_DATA_STALE
        assert decision.final_threshold is None

    def test_poor_but_valid_quality_raises_the_bar(self):
        policy = make_policy()
        engine = AdaptiveEntryThresholdEngine()
        decision = engine.evaluate(
            make_request(
                market_quality=MarketQualityContext(
                    spread_percentile=0.95, liquidity_score=0.05, volume_percentile=0.05
                )
            ),
            policy,
        )
        assert decision.market_quality_adjustment > 0

    def test_good_quality_lowers_it_modestly_and_within_bounds(self):
        policy = make_policy(market_quality_bound=0.04)
        engine = AdaptiveEntryThresholdEngine()
        decision = engine.evaluate(
            make_request(
                market_quality=MarketQualityContext(
                    spread_percentile=0.02, liquidity_score=0.98, volume_percentile=0.95
                )
            ),
            policy,
        )
        assert -0.04 - 1e-9 <= decision.market_quality_adjustment < 0


# ═════════════════════════════════════════════════════════════════════════════
# SLOW CALIBRATION
# ═════════════════════════════════════════════════════════════════════════════


class TestPerformanceCalibration:
    def test_insufficient_sample_is_neutral_and_says_so(self):
        result = PerformanceCalibrator.score([-1.0, -1.0], min_samples=30, bound=0.05)
        assert result.adjustment == 0.0
        assert result.status == CalibrationStatus.INSUFFICIENT_SAMPLE

    def test_two_losing_trades_cannot_move_the_bar(self):
        """The prohibition, tested directly."""
        result = PerformanceCalibrator.score([-1.0, -1.2], min_samples=30, bound=0.05)
        assert result.adjustment == 0.0

    def test_poor_performance_raises_and_good_performance_lowers(self):
        poor = PerformanceCalibrator.score([-1.0] * 40, min_samples=30, bound=0.05)
        good = PerformanceCalibrator.score([1.0] * 40, min_samples=30, bound=0.05)
        assert poor.adjustment > 0
        assert good.adjustment < 0

    def test_a_single_outlier_cannot_produce_an_extreme_shift(self):
        samples = [0.0] * 39 + [500.0]
        result = PerformanceCalibrator.score(samples, min_samples=30, bound=0.05)
        assert abs(result.adjustment) <= 0.05 + 1e-9

    def test_small_scores_fall_in_the_dead_band(self):
        samples = [0.01, -0.01] * 20
        result = PerformanceCalibrator.score(samples, min_samples=30, bound=0.05)
        assert result.adjustment == 0.0
        assert result.status == CalibrationStatus.OK

    def test_no_source_is_unavailable_not_zero_performance(self):
        result = PerformanceCalibrator(None).evaluate(
            bot_instance_id=BOT,
            symbol="BTCUSDT",
            timeframe="15m",
            min_samples=30,
            lookback=100,
            bound=0.05,
        )
        assert result.status == CalibrationStatus.UNAVAILABLE
        assert result.score is None


class TestDistributionCalibration:
    def test_insufficient_sample_is_neutral(self):
        result = DistributionCalibrator.evaluate(
            [0.5, 0.6], base_threshold=0.60, target_percentile=0.6, min_samples=40, bound=0.05
        )
        assert result.adjustment == 0.0
        assert result.status == CalibrationStatus.INSUFFICIENT_SAMPLE

    def test_the_bar_follows_signal_quality_not_trade_frequency(self):
        """Fewer opportunities must never argue for a lower bar.

        Both sets describe the same signal quality; one simply contains fewer
        observations. The adjustment must be identical in direction and must not
        soften because the bot has been quiet.
        """
        busy = [0.70] * 200
        quiet = [0.70] * 45
        busy_result = DistributionCalibrator.evaluate(
            busy, base_threshold=0.60, target_percentile=0.6, min_samples=40, bound=0.05
        )
        quiet_result = DistributionCalibrator.evaluate(
            quiet, base_threshold=0.60, target_percentile=0.6, min_samples=40, bound=0.05
        )
        assert busy_result.adjustment == quiet_result.adjustment
        assert busy_result.adjustment > 0  # quality improved, so the bar rises

    def test_weaker_signal_quality_lowers_the_bar_within_bounds(self):
        result = DistributionCalibrator.evaluate(
            [0.40] * 60, base_threshold=0.60, target_percentile=0.6, min_samples=40, bound=0.05
        )
        assert -0.05 - 1e-9 <= result.adjustment < 0

    def test_only_quality_stage_samples_are_recorded(self):
        """NoOpportunity candles must not enter the distribution at all."""
        policy = make_policy(distribution_window=50)
        engine = AdaptiveEntryThresholdEngine()
        engine.evaluate(make_request(side=None, opportunity_confidence=None), policy)
        state = engine.state_store.get(make_request().state_key())
        assert state.distribution_samples == []


# ═════════════════════════════════════════════════════════════════════════════
# SMOOTHING / HYSTERESIS
# ═════════════════════════════════════════════════════════════════════════════


class TestSmoothing:
    def test_upward_movement_is_rate_limited(self):
        policy = make_policy(max_step_up=0.01, base_threshold=0.60)
        engine = AdaptiveEntryThresholdEngine()
        key = make_request().state_key()
        state = engine.state_store.get(key)
        state.previous_threshold = 0.55
        engine.state_store.put(state)

        decision = engine.evaluate(
            make_request(regime=RegimeContext(regime="STRONG_TREND", regime_confidence=1.0)),
            policy,
        )
        assert decision.final_threshold <= 0.55 + 0.01 + 1e-9
        assert decision.rate_limit_applied

    def test_downward_movement_is_rate_limited(self):
        policy = make_policy(max_step_down=0.005, base_threshold=0.60)
        engine = AdaptiveEntryThresholdEngine()
        key = make_request().state_key()
        state = engine.state_store.get(key)
        state.previous_threshold = 0.80
        engine.state_store.put(state)

        decision = engine.evaluate(make_request(), policy)
        assert decision.final_threshold >= 0.80 - 0.005 - 1e-9

    def test_tightening_is_allowed_to_be_faster_than_loosening(self):
        policy = make_policy()
        assert policy.max_step_up > policy.max_step_down

    def test_first_evaluation_has_no_previous_and_no_smoothing(self):
        policy = make_policy()
        engine = AdaptiveEntryThresholdEngine()
        decision = engine.evaluate(make_request(), policy)
        assert decision.previous_threshold is None
        assert decision.smoothing_applied is False
        assert decision.rate_limit_applied is False


# ═════════════════════════════════════════════════════════════════════════════
# NOT_EVALUATED SEMANTICS
# ═════════════════════════════════════════════════════════════════════════════


class TestNotEvaluatedSemantics:
    def test_no_opportunity_carries_null_not_zero(self):
        policy = make_policy()
        engine = AdaptiveEntryThresholdEngine()
        decision = engine.evaluate(
            make_request(side=None, opportunity_confidence=None), policy
        )
        assert decision.status == ThresholdStatus.NOT_EVALUATED
        assert decision.final_threshold is None
        assert decision.opportunity_confidence is None
        assert decision.passed is None

    def test_a_zero_threshold_cannot_be_stored_as_not_evaluated(self):
        with pytest.raises(ValueError):
            AdaptiveThresholdDecision(
                threshold_decision_id="t1",
                bot_instance_id=BOT,
                symbol="BTCUSDT",
                timeframe="15m",
                status=ThresholdStatus.NOT_EVALUATED,
                threshold_engine_version="1.0.0",
                threshold_mode=ThresholdMode.ADAPTIVE,
                final_threshold=0.0,
            )

    def test_evaluated_must_carry_a_number(self):
        with pytest.raises(ValueError):
            AdaptiveThresholdDecision(
                threshold_decision_id="t1",
                bot_instance_id=BOT,
                symbol="BTCUSDT",
                timeframe="15m",
                status=ThresholdStatus.EVALUATED,
                threshold_engine_version="1.0.0",
                threshold_mode=ThresholdMode.ADAPTIVE,
                final_threshold=None,
            )

    def test_quality_decision_never_compares_against_a_substituted_zero(self):
        """0.0 >= 0.0 approving everything is the failure this replaces."""
        opportunity = build_opportunity(
            symbol="BTCUSDT",
            timeframe="15m",
            market_snapshot_id="snap",
            side="BUY",
            raw_confidence=0.0,
            consensus=0.0,
            buy_score=0.0,
            sell_score=0.0,
        )
        verdict = TradingDecisionEngine().evaluate(opportunity, threshold_decision=None)
        assert verdict.approved is False
        assert verdict.effective_entry_threshold is None

    def test_no_opportunity_is_not_reported_as_a_confidence_failure(self):
        nothing = NoOpportunity(
            symbol="BTCUSDT", timeframe="15m", market_snapshot_id="snap"
        )
        verdict = TradingDecisionEngine().evaluate(nothing)
        assert verdict.primary_reason == QualityReason.NO_OPPORTUNITY
        assert verdict.primary_reason != QualityReason.ENTRY_CONFIDENCE_BELOW_THRESHOLD
        assert verdict.effective_entry_threshold is None
        assert verdict.raw_confidence is None


# ═════════════════════════════════════════════════════════════════════════════
# EVIDENCE — components must reconcile with the value
# ═════════════════════════════════════════════════════════════════════════════


class TestEvidence:
    def test_every_component_is_persisted_on_the_decision(self):
        policy = make_policy()
        engine = AdaptiveEntryThresholdEngine()
        decision = engine.evaluate(
            make_request(
                experts=(expert("a", "BUY", 0.9), expert("b", "BUY", 0.8)),
                volatility=VolatilityContext(atr_percentile=0.6),
                htf=HTFContext(timeframe="4h", direction="BUY", strength=0.5),
                market_quality=MarketQualityContext(volume_percentile=0.5),
            ),
            policy,
        )
        for field in (
            "base_threshold",
            "regime_adjustment",
            "volatility_adjustment",
            "agreement_adjustment",
            "htf_adjustment",
            "market_quality_adjustment",
            "performance_adjustment",
            "distribution_adjustment",
            "market_threshold",
            "raw_unclamped_threshold",
            "smoothed_threshold",
            "rate_limited_threshold",
            "final_threshold",
            "min_threshold",
            "max_threshold",
        ):
            assert getattr(decision, field) is not None, field

    def test_the_arithmetic_reconciles(self):
        policy = make_policy()
        engine = AdaptiveEntryThresholdEngine()
        decision = engine.evaluate(
            make_request(
                experts=(expert("a", "BUY", 0.9),),
                volatility=VolatilityContext(atr_percentile=0.95),
                htf=HTFContext(timeframe="4h", direction="SELL", strength=0.7),
                market_quality=MarketQualityContext(spread_percentile=0.8),
            ),
            policy,
        )
        assert decision.reconcile(), decision.observability()
        assert decision.reconciliation_error() < 1e-6

    def test_a_tampered_decision_fails_reconciliation(self):
        policy = make_policy()
        engine = AdaptiveEntryThresholdEngine()
        decision = engine.evaluate(make_request(), policy)
        tampered = AdaptiveThresholdDecision(
            **{**decision.to_dict(), "regime_adjustment": 0.5, "expert_evidence": ()}
        )
        assert not tampered.reconcile()
        findings = health_check([tampered])
        assert any(f["reason"] == THRESHOLD_COMPONENTS_DO_NOT_RECONCILE for f in findings)

    def test_expert_evidence_travels_with_the_decision(self):
        policy = make_policy()
        engine = AdaptiveEntryThresholdEngine()
        experts = (expert("a", "BUY", 0.9), expert("b", "NOT_RUN", 0.0, executed=False))
        decision = engine.evaluate(make_request(experts=experts), policy)
        assert len(decision.expert_evidence) == 2
        assert {e.signal for e in decision.expert_evidence} == {"BUY", "NOT_RUN"}


# ═════════════════════════════════════════════════════════════════════════════
# ISOLATION — no leak between symbols, candles or bots
# ═════════════════════════════════════════════════════════════════════════════


class TestIsolation:
    def test_btc_state_cannot_leak_into_eth(self):
        policy = make_policy()
        engine = AdaptiveEntryThresholdEngine()
        for _ in range(5):
            engine.evaluate(
                make_request(
                    symbol="BTCUSDT",
                    regime=RegimeContext(regime="STRONG_TREND", regime_confidence=1.0),
                    closed_candle_time=None,
                ),
                policy,
            )
        eth = engine.evaluate(make_request(symbol="ETHUSDT"), policy)
        assert eth.previous_threshold is None

    def test_bot_a_state_cannot_leak_into_bot_b(self):
        policy = make_policy()
        engine = AdaptiveEntryThresholdEngine()
        engine.evaluate(make_request(bot_instance_id="bot_a"), policy)
        other = engine.evaluate(make_request(bot_instance_id="bot_b"), policy)
        assert other.previous_threshold is None

    def test_timeframe_partitions_state(self):
        policy = make_policy()
        engine = AdaptiveEntryThresholdEngine()
        engine.evaluate(make_request(timeframe="15m"), policy)
        other = engine.evaluate(make_request(timeframe="1h"), policy)
        assert other.previous_threshold is None

    def test_candle_n_does_not_double_count_into_candle_n_plus_one(self):
        policy = make_policy()
        engine = AdaptiveEntryThresholdEngine()
        request = make_request(closed_candle_time=1000)
        engine.evaluate(request, policy)
        engine.evaluate(request, policy)  # same candle re-evaluated after a restart
        state = engine.state_store.get(request.state_key())
        assert len(state.distribution_samples) == 1

    def test_master_ensemble_resets_evidence_every_evaluation(self):
        """The stale-evidence defect: last_opportunity was set once in __init__
        and only written at Step 7, so every early return published the previous
        symbol's opportunity as this candle's evidence."""
        from app.strategy.master_ensemble import MasterEnsembleStrategy

        source = _code_only(inspect.getsource(MasterEnsembleStrategy.get_signal))
        assert "self._reset_evaluation_state()" in source

        strategy = MasterEnsembleStrategy.__new__(MasterEnsembleStrategy)
        strategy.last_opportunity = "STALE"
        strategy.last_entry_quality = "STALE"
        strategy.last_threshold_decision = "STALE"
        strategy.last_expert_evidence = ("STALE",)
        MasterEnsembleStrategy._reset_evaluation_state(strategy)
        assert strategy.last_opportunity is None
        assert strategy.last_entry_quality is None
        assert strategy.last_threshold_decision is None
        assert strategy.last_expert_evidence == ()


# ═════════════════════════════════════════════════════════════════════════════
# DETERMINISM AND RESTART SAFETY
# ═════════════════════════════════════════════════════════════════════════════


class TestDeterminism:
    def test_identical_inputs_produce_an_identical_threshold(self):
        policy = make_policy()
        request = make_request(
            experts=(expert("a", "BUY", 0.9), expert("b", "SELL", 0.3)),
            volatility=VolatilityContext(atr_percentile=0.77),
            htf=HTFContext(timeframe="4h", direction="BUY", strength=0.4),
            market_quality=MarketQualityContext(spread_percentile=0.3),
        )
        first = AdaptiveEntryThresholdEngine().evaluate(request, policy)
        second = AdaptiveEntryThresholdEngine().evaluate(request, policy)
        assert first.final_threshold == second.final_threshold
        assert first.raw_unclamped_threshold == second.raw_unclamped_threshold
        assert first.to_dict()["adjustments" if False else "regime_adjustment"] == (
            second.regime_adjustment
        )

    def test_the_engine_reads_no_wall_clock(self):
        """decided_at comes from the injected input, not from now()."""
        policy = make_policy()
        request = make_request(evaluated_at="2020-01-01T00:00:00+00:00")
        decision = AdaptiveEntryThresholdEngine().evaluate(request, policy)
        assert decision.decided_at == "2020-01-01T00:00:00+00:00"

    def test_restart_reproduces_the_same_next_result(self):
        policy = make_policy()
        store = ThresholdStateStore()
        engine = AdaptiveEntryThresholdEngine(state_store=store)
        engine.evaluate(make_request(closed_candle_time=1000), policy)

        next_request = make_request(closed_candle_time=2000, opportunity_confidence=0.71)
        before = AdaptiveEntryThresholdEngine(state_store=store).evaluate(next_request, policy)

        # Simulate a restart: a brand new engine reading the same persisted state.
        restored = ThresholdStateStore()
        for key in store.keys():
            restored.put(store.get(key))
        after = AdaptiveEntryThresholdEngine(state_store=restored).evaluate(
            next_request, policy
        )
        assert before.final_threshold == after.final_threshold

    def test_state_survives_a_real_sqlite_round_trip(self):
        from app.threshold.state import SqliteThresholdStateStore

        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            path = Path(tmp) / "threshold.db"

            class _Conn:
                """Closes on exit. sqlite3's own context manager only commits,
                which on Windows leaves the file locked against cleanup."""

                def __init__(self, conn):
                    self._conn = conn

                def __enter__(self):
                    return self._conn

                def __exit__(self, *exc):
                    self._conn.commit()
                    self._conn.close()
                    return False

            class _DB:
                def connect(self):
                    conn = sqlite3.connect(path)
                    conn.isolation_level = None
                    return _Conn(conn)

            db = _DB()
            with db.connect() as conn:
                conn.execute(
                    """
                    CREATE TABLE adaptive_threshold_state (
                        bot_instance_id TEXT NOT NULL, symbol TEXT NOT NULL,
                        timeframe TEXT NOT NULL, strategy_version TEXT NOT NULL,
                        previous_threshold REAL, last_candle_time INTEGER,
                        distribution_samples_json TEXT, updated_at TEXT,
                        engine_version TEXT, policy_hash TEXT,
                        PRIMARY KEY (bot_instance_id, symbol, timeframe, strategy_version)
                    )
                    """
                )

            store = SqliteThresholdStateStore(db)
            state = ThresholdState(BOT, "BTCUSDT", "15m", "2.0.0")
            state.previous_threshold = 0.63
            state.distribution_samples = [0.5, 0.6, 0.7]
            store.put(state)
            assert store.write_failures == 0

            reloaded = SqliteThresholdStateStore(db).get(
                ThresholdStateStore.make_key(BOT, "BTCUSDT", "15m", "2.0.0")
            )
            assert reloaded.previous_threshold == pytest.approx(0.63)
            assert reloaded.distribution_samples == [0.5, 0.6, 0.7]


# ═════════════════════════════════════════════════════════════════════════════
# MODES
# ═════════════════════════════════════════════════════════════════════════════


class TestModes:
    def test_static_mode_applies_no_adaptation(self):
        policy = resolve_threshold_policy(
            scopes=[("GLOBAL", {"mode": ThresholdMode.STATIC, "static_threshold": 0.65})],
            base_threshold=0.60,
        )
        engine = AdaptiveEntryThresholdEngine()
        decision = engine.evaluate(
            make_request(regime=RegimeContext(regime="STRONG_TREND", regime_confidence=1.0)),
            policy,
        )
        assert decision.final_threshold == pytest.approx(0.65)
        assert decision.regime_adjustment == 0.0
        assert decision.reconcile()

    def test_static_mode_requires_a_static_threshold(self):
        with pytest.raises(ThresholdPolicyError) as exc:
            resolve_threshold_policy(
                scopes=[("GLOBAL", {"mode": ThresholdMode.STATIC})], base_threshold=0.60
            )
        assert exc.value.code == "STATIC_THRESHOLD_MISSING"

    def test_research_mode_uses_the_same_engine(self):
        """Replay must not get a threshold engine of its own."""
        policy = resolve_threshold_policy(
            scopes=[("GLOBAL", {"mode": ThresholdMode.RESEARCH})], base_threshold=0.60
        )
        engine = AdaptiveEntryThresholdEngine()
        production = AdaptiveEntryThresholdEngine().evaluate(make_request(), make_policy())
        research = engine.evaluate(make_request(), policy)
        assert research.final_threshold == production.final_threshold
        assert research.threshold_mode == ThresholdMode.RESEARCH


# ═════════════════════════════════════════════════════════════════════════════
# POLICY PRECEDENCE
# ═════════════════════════════════════════════════════════════════════════════


class TestPolicyPrecedence:
    def test_more_specific_scope_wins(self):
        policy = resolve_threshold_policy(
            scopes=[
                ("GLOBAL", {"max_threshold": 0.90}),
                ("ASSET_CLASS", {"max_threshold": 0.85}),
                ("SYMBOL", {"max_threshold": 0.80}),
            ],
            base_threshold=0.60,
        )
        assert policy.max_threshold == 0.80
        assert policy.source_scopes == ("GLOBAL", "ASSET_CLASS", "SYMBOL")

    def test_identical_values_from_different_scopes_hash_the_same(self):
        """A scope change that changes nothing must not look like a policy change."""
        a = resolve_threshold_policy(
            scopes=[("GLOBAL", {"max_threshold": 0.80})], base_threshold=0.60
        )
        b = resolve_threshold_policy(
            scopes=[("GLOBAL", {}), ("SYMBOL", {"max_threshold": 0.80})], base_threshold=0.60
        )
        assert a.policy_hash == b.policy_hash

    def test_different_values_hash_differently(self):
        a = resolve_threshold_policy(base_threshold=0.60)
        b = resolve_threshold_policy(base_threshold=0.61)
        assert a.policy_hash != b.policy_hash

    def test_unknown_scope_is_rejected(self):
        with pytest.raises(ThresholdPolicyError) as exc:
            resolve_threshold_policy(
                scopes=[("REGION", {"max_threshold": 0.8})], base_threshold=0.60
            )
        assert exc.value.code == "UNKNOWN_POLICY_SCOPE"


# ═════════════════════════════════════════════════════════════════════════════
# INERT-ENGINE DETECTION — the check that would have caught 0.70
# ═════════════════════════════════════════════════════════════════════════════


def _decision(final: float, raw: float, *, minimum=0.50, maximum=0.90) -> AdaptiveThresholdDecision:
    return AdaptiveThresholdDecision(
        threshold_decision_id=f"t{raw}",
        bot_instance_id=BOT,
        symbol="BTCUSDT",
        timeframe="15m",
        status=ThresholdStatus.EVALUATED,
        threshold_engine_version="1.0.0",
        threshold_mode=ThresholdMode.ADAPTIVE,
        base_threshold=raw,
        raw_unclamped_threshold=raw,
        final_threshold=final,
        min_threshold=minimum,
        max_threshold=maximum,
    )


class TestInertDetection:
    def test_it_catches_the_0_70_shape(self):
        """Components varying, output pinned at a bound. The original failure."""
        decisions = [
            _decision(0.90, 0.40 + i * 0.01, maximum=0.90) for i in range(30)
        ]
        report = detect_inert_engine(decisions)
        assert report.inert
        assert report.reason == ADAPTIVE_THRESHOLD_EFFECTIVELY_STATIC
        assert report.saturated_at == "max_threshold"
        assert report.distinct_raw_thresholds == 30
        assert report.distinct_thresholds == 1

    def test_a_stable_market_is_not_reported_as_a_failure(self):
        """Stable components producing a stable threshold is the engine working."""
        decisions = [_decision(0.60, 0.60) for _ in range(30)]
        report = detect_inert_engine(decisions)
        assert not report.inert
        assert "components are stable" in report.detail

    def test_a_varying_threshold_is_not_flagged(self):
        decisions = [_decision(0.60 + i * 0.001, 0.60 + i * 0.001) for i in range(30)]
        assert not detect_inert_engine(decisions).inert

    def test_a_small_sample_is_not_assessed(self):
        decisions = [_decision(0.90, 0.40 + i * 0.01) for i in range(5)]
        report = detect_inert_engine(decisions)
        assert not report.inert
        assert "not assessed" in report.detail

    def test_stats_report_the_distribution(self):
        decisions = [_decision(0.60 + i * 0.01, 0.60 + i * 0.01) for i in range(21)]
        stats = threshold_stats(decisions, total_bound=0.38)
        assert stats.sample == 21
        assert stats.distinct == 21
        assert stats.minimum == pytest.approx(0.60)
        assert stats.maximum == pytest.approx(0.80)
        assert stats.median is not None and stats.stdev > 0


# ═════════════════════════════════════════════════════════════════════════════
# MIGRATION — every legacy control has a stated disposition
# ═════════════════════════════════════════════════════════════════════════════


class TestMigration:
    def test_every_legacy_setting_has_a_valid_disposition(self):
        for entry in legacy_inventory():
            assert entry["disposition"] in DISPOSITIONS, entry

    def test_the_settings_that_caused_the_failure_are_accounted_for(self):
        by_name = {e["setting"]: e for e in legacy_inventory()}
        assert by_name["MIN_CONFIDENCE_THRESHOLD"]["disposition"] == "MIGRATED"
        assert by_name["ENSEMBLE_MIN_THRESHOLD_FLOOR"]["disposition"] == "REMOVED"
        assert by_name["confidence_absolute_floor"]["disposition"] == "REMOVED"
        assert by_name["runner.min_confidence_gate_max"]["disposition"] == "REMOVED"
        assert by_name["consensus_threshold"]["disposition"] == "REMOVED"
        for key in ("DYNAMIC_THRESHOLD_MIN", "DYNAMIC_THRESHOLD_MAX", "DYNAMIC_THRESHOLD_FALLBACK"):
            assert by_name[key]["disposition"] == "RESEARCH_ONLY"

    def test_min_confidence_threshold_is_migrated_to_the_base(self):
        settings = SimpleNamespace(MIN_CONFIDENCE_THRESHOLD=0.70, THRESHOLD_BASE=0.0)
        _, base = global_scope_from_settings(settings)
        assert base == pytest.approx(0.70)

    def test_an_explicit_base_supersedes_the_legacy_setting(self):
        settings = SimpleNamespace(MIN_CONFIDENCE_THRESHOLD=0.70, THRESHOLD_BASE=0.62)
        _, base = global_scope_from_settings(settings)
        assert base == pytest.approx(0.62)

    def test_the_dead_setting_warns_instead_of_being_silently_ignored(self):
        settings = SimpleNamespace(
            MIN_CONFIDENCE_THRESHOLD=0.70,
            ENSEMBLE_MIN_THRESHOLD_FLOOR=0.55,
            THRESHOLD_BASE=0.0,
        )
        warnings = deprecation_warnings(settings)
        assert any("ENSEMBLE_MIN_THRESHOLD_FLOOR" in w for w in warnings)
        assert any("no longer affects" in w for w in warnings)

    def test_the_startup_report_names_one_authority(self):
        policy = make_policy()
        report = startup_report(policy, SimpleNamespace(MIN_CONFIDENCE_THRESHOLD=0.70))
        assert "AdaptiveEntryThresholdEngine" in report
        assert "active final threshold authorities = 1" in report
        assert policy.policy_hash in report

    def test_no_inert_user_visible_threshold_config_remains(self):
        """Every THRESHOLD_* setting must reach the resolved policy.

        A setting an operator can see and set, which cannot affect behaviour, is
        the exact defect this rebuild removes.
        """
        from app.core.config import Settings
        from app.threshold.migration import _SETTING_MAP

        exposed = {
            name
            for name in Settings.model_fields
            if name.startswith("THRESHOLD_")
        }
        accounted = set(_SETTING_MAP) | {"THRESHOLD_SCOPED_OVERRIDES"}
        assert exposed <= accounted, f"inert threshold settings: {exposed - accounted}"


# ═════════════════════════════════════════════════════════════════════════════
# HARD GATES STAY OUTSIDE THE THRESHOLD
# ═════════════════════════════════════════════════════════════════════════════


class TestHardGatesAreSeparate:
    def test_high_confidence_still_fails_a_hard_gate(self):
        """confidence 0.95 must not be able to buy its way past a hard block."""
        policy = make_policy()
        engine = AdaptiveEntryThresholdEngine()
        decision = engine.evaluate(
            make_request(
                opportunity_confidence=0.95,
                regime=RegimeContext(regime="LOW_VOLATILITY_CHOP"),
            ),
            policy,
        )
        assert decision.status == ThresholdStatus.HARD_BLOCKED
        assert decision.passed is None

    def test_the_engine_contains_no_risk_or_execution_gates(self):
        import app.threshold.engine as engine_module

        source = _code_only(Path(engine_module.__file__).read_text(encoding="utf-8"))
        for forbidden in (
            "daily_loss",
            "kill_switch",
            "max_open_positions",
            "min_notional",
            "position_size",
            "leverage",
        ):
            assert forbidden not in source, f"threshold engine references {forbidden}"


# ═════════════════════════════════════════════════════════════════════════════
# CONTRACT INTEGRATION — the comparison uses the engine's number
# ═════════════════════════════════════════════════════════════════════════════


class TestQualityComparison:
    def _opportunity(self, confidence: float):
        return build_opportunity(
            symbol="BTCUSDT",
            timeframe="15m",
            market_snapshot_id="snap",
            side="BUY",
            raw_confidence=confidence,
            consensus=confidence,
            buy_score=confidence,
            sell_score=0.0,
            regime="RANGE",
        )

    def test_confidence_at_the_threshold_is_approved(self):
        policy = make_policy()
        engine = AdaptiveEntryThresholdEngine()
        threshold_decision = engine.evaluate(make_request(), policy)
        opportunity = self._opportunity(threshold_decision.final_threshold)
        verdict = TradingDecisionEngine().evaluate(
            opportunity, threshold_decision=threshold_decision
        )
        assert verdict.approved
        assert verdict.primary_reason == QualityReason.APPROVED_FOR_EXECUTION

    def test_confidence_below_the_threshold_is_rejected_with_the_right_reason(self):
        policy = make_policy()
        engine = AdaptiveEntryThresholdEngine()
        threshold_decision = engine.evaluate(make_request(), policy)
        opportunity = self._opportunity(threshold_decision.final_threshold - 0.01)
        verdict = TradingDecisionEngine().evaluate(
            opportunity, threshold_decision=threshold_decision
        )
        assert not verdict.approved
        assert verdict.primary_reason == QualityReason.ENTRY_CONFIDENCE_BELOW_THRESHOLD
        assert verdict.effective_entry_threshold == threshold_decision.final_threshold

    def test_the_verdict_records_which_threshold_decision_governed_it(self):
        policy = make_policy()
        engine = AdaptiveEntryThresholdEngine()
        threshold_decision = engine.evaluate(make_request(), policy)
        verdict = TradingDecisionEngine().evaluate(
            self._opportunity(0.8), threshold_decision=threshold_decision
        )
        assert verdict.threshold_decision_id == threshold_decision.threshold_decision_id
        assert verdict.threshold_status == ThresholdStatus.EVALUATED

    def test_no_consensus_requirement_is_applied_anywhere(self):
        assert "consensus_required" not in EntryQualityDecision.__dataclass_fields__
        params = set(inspect.signature(TradingDecisionEngine.evaluate).parameters)
        assert "consensus_required" not in params


# ═════════════════════════════════════════════════════════════════════════════
# UTILITIES
# ═════════════════════════════════════════════════════════════════════════════


class TestTestIsolation:
    def test_the_state_store_never_touches_the_production_database_under_test(self):
        """Threshold state is written on every evaluated candle.

        The replay and parity suites drive the real strategy, so without this
        isolation a test run deposits adaptive state into the production
        database under test bot ids -- which is exactly what happened once, and
        would silently pollute the live distribution calibration.
        """
        import os

        from app.threshold import runtime as threshold_runtime

        assert os.environ.get("COSMICFORGE_TEST_MODE") == "1", "conftest sets test mode"
        assert threshold_runtime._db() is None

        threshold_runtime.reset_for_tests()
        try:
            store = threshold_runtime.get_threshold_state_store()
            assert type(store) is ThresholdStateStore, (
                f"expected an in-memory store under test, got {type(store).__name__}"
            )
        finally:
            threshold_runtime.reset_for_tests()

    def test_the_performance_source_is_also_isolated(self):
        from app.threshold import runtime as threshold_runtime

        threshold_runtime.reset_for_tests()
        try:
            calibrator = threshold_runtime.get_performance_calibrator()
            assert calibrator._source is None
        finally:
            threshold_runtime.reset_for_tests()


class TestPercentile:
    def test_interpolates_between_samples(self):
        assert percentile([0.0, 1.0], 0.5) == pytest.approx(0.5)

    def test_handles_degenerate_inputs(self):
        assert percentile([], 0.5) is None
        assert percentile([0.4], 0.9) == pytest.approx(0.4)
