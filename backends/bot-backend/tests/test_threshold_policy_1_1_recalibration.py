"""Threshold policy 1.1.0 -- the engine recalibrated to the ensemble's own confidence scale.

Policy 1.0.0 centred the band on 0.70 in a system whose opportunities have a median
confidence of 0.325 and a P95 of 0.48: 0 of 23 clean live opportunities, and 24 of 4,644
replayed ones, could pass. These tests pin the recalibration and the three defects found
on the way (docs/adaptive_threshold_recalibration_report.md):

* agreement weighted by confidence a second time, so it raised the bar on every candle;
* a volatility "percentile" that ranked a 14-candle ATR% against single-candle ranges;
* an adaptive state from the 0.70 era that would have anchored smoothing and rate
  limiting under the new policy.

Numbered comments map to the Phase 16 test list.
"""
from __future__ import annotations

import ast
import inspect
import random
import textwrap
from pathlib import Path
from types import SimpleNamespace

import pytest

from app.decision import external_signal_gate as gate_module
from app.strategy import master_ensemble as ensemble_module
from app.strategy.master_ensemble import HTF_STRENGTH_SATURATION, MasterEnsembleStrategy
from app.strategy.regime import MarketRegime, calculate_atr_percent
from app.threshold.calibration import DistributionCalibrator, PerformanceCalibrator
from app.threshold.contracts import (
    AdaptiveThresholdInput,
    CalibrationStatus,
    ExpertEvidence,
    HTFContext,
    MarketQualityContext,
    RegimeContext,
    ThresholdStatus,
    VolatilityContext,
)
from app.threshold.engine import (
    ENGINE_VERSION,
    REASON_EXPERT_ERROR,
    REASON_MARKET_DATA_STALE,
    REASON_REGIME_HARD_BLOCK,
    AdaptiveEntryThresholdEngine,
    agreement_component,
    htf_component,
    market_quality_component,
)
from app.threshold.persistence import load_threshold_decisions, record_threshold_decision
from app.threshold.policy import (
    DEFAULTS,
    POLICY_VERSION,
    SETTING_MAP,
    ThresholdPolicyError,
    policy_from_settings,
    resolve_threshold_policy,
)
from app.threshold.state import SqliteThresholdStateStore, ThresholdStateStore
from shared_lib.persistence.db import DB
from shared_lib.persistence.migrations import migrate

APP = Path(__file__).resolve().parents[1] / "app"

#: The policy every live threshold decision carried before 1.1.0.
POLICY_1_0_0_HASH = "afd2b4636b9d89e9d99dabc46ea08338386e717e66d7b8cb226911c601f17802"

#: Empirical opportunity confidence, 4,644 organic opportunities in production-parity
#: replay (BTCUSDT + ETHUSDT, 2026-01-16 -> 2026-06-10). Phase 2 of the report.
EMPIRICAL = {
    "p50": 0.325,
    "p90": 0.400,
    "p95": 0.4838,
    "p99": 0.6426,
    "max": 0.8965,
    "lone_sma_cluster": 0.195,
    "multi_expert_p50": 0.5932,
}

WEAK_TREND_WEIGHTS = {"supertrend": 1.5, "trend_pullback": 1.3, "donchian_breakout": 1.0, "sma_cross": 0.9}
DISABLED_IN_WEAK_TREND = {"vwap_reversion": 1.2, "bollinger_reversion": 1.0, "squeeze_breakout": 1.1}


# ── Helpers ─────────────────────────────────────────────────────────────────


def setting_default(name: str):
    from app.core.config import Settings

    fields = getattr(Settings, "model_fields", None) or getattr(Settings, "__fields__")
    return fields[name].default


def production_policy():
    """The policy the runtime resolves from configuration defaults."""
    values = {name: setting_default(name) for name in SETTING_MAP}
    return policy_from_settings(SimpleNamespace(**values, THRESHOLD_SCOPED_OVERRIDES=""))


def experts(votes: dict | None = None) -> tuple[ExpertEvidence, ...]:
    """WEAK_TREND expert evidence: four eligible experts, three the regime disabled."""
    votes = votes or {}
    out = [
        ExpertEvidence(name, True, True, *votes.get(name, ("HOLD", 0.0)), weight=weight)
        for name, weight in WEAK_TREND_WEIGHTS.items()
    ]
    out += [
        ExpertEvidence(name, False, False, "DISABLED", 0.0, weight=weight)
        for name, weight in DISABLED_IN_WEAK_TREND.items()
    ]
    return tuple(out)


LONE_SUPERTREND = {"supertrend": ("BUY", 0.65)}


def request(**overrides) -> AdaptiveThresholdInput:
    payload: dict = dict(
        bot_instance_id="bot_recalibration",
        symbol="BTCUSDT",
        timeframe="15m",
        strategy_version="2.0.0",
        side="BUY",
        opportunity_confidence=0.325,
        closed_candle_time=1_789_000_000_000,
        evaluated_at="2026-09-11T00:00:00+00:00",
        experts=experts(LONE_SUPERTREND),
        regime=RegimeContext(regime="WEAK_TREND", regime_confidence=0.7),
    )
    payload.update(overrides)
    return AdaptiveThresholdInput(**payload)


class NoTrades:
    def recent_r_multiples(self, **_):
        return []


def engine(store=None) -> AdaptiveEntryThresholdEngine:
    return AdaptiveEntryThresholdEngine(
        state_store=store or ThresholdStateStore(),
        performance_calibrator=PerformanceCalibrator(NoTrades()),
    )


def seed(store, policy, *, previous, version=ENGINE_VERSION, policy_hash=None, samples=()):
    state = store.get(request().state_key())
    state.previous_threshold = previous
    state.last_candle_time = 1_788_999_100_000
    state.distribution_samples = list(samples)
    state.engine_version = version
    state.policy_hash = policy.policy_hash if policy_hash is None else policy_hash
    store.put(state)


def code_only(source: str) -> str:
    tree = ast.parse(textwrap.dedent(source))
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef, ast.Module)):
            if node.body and isinstance(node.body[0], ast.Expr) and isinstance(
                getattr(node.body[0], "value", None), ast.Constant
            ) and isinstance(node.body[0].value.value, str):
                node.body.pop(0)
    return ast.unparse(tree)


# ═════════════════════════════════════════════════════════════════════════════
# 1-4  BASE AND BAND
# ═════════════════════════════════════════════════════════════════════════════


class TestBaseAndBand:
    def test_1_default_production_base_is_0_30(self):
        assert setting_default("THRESHOLD_BASE") == pytest.approx(0.30)
        policy = production_policy()
        assert policy.base_threshold == pytest.approx(0.30)
        assert policy.policy_version == POLICY_VERSION == "1.1.0"
        assert ENGINE_VERSION == "1.1.0"
        assert policy.policy_hash != POLICY_1_0_0_HASH

    def test_2_no_production_0_70_fallback_remains(self):
        # Configuration: none of the three band values survives anywhere.
        for name in ("THRESHOLD_BASE", "THRESHOLD_MIN", "THRESHOLD_MAX"):
            assert setting_default(name) not in (0.70, 0.50, 0.90), name
        # The policy module still invents no base at all.
        assert DEFAULTS["base_threshold"] is None
        with pytest.raises(ThresholdPolicyError) as exc:
            policy_from_settings(SimpleNamespace(MIN_CONFIDENCE_THRESHOLD=0.70))
        assert exc.value.code == "THRESHOLD_BASE_UNRESOLVED"
        # No 0.70 literal in executable threshold code.
        for path in sorted((APP / "threshold").glob("*.py")):
            constants = [
                node.value for node in ast.walk(ast.parse(path.read_text(encoding="utf-8")))
                if isinstance(node, ast.Constant) and isinstance(node.value, float)
            ]
            assert 0.7 not in constants, f"{path.name} carries a 0.70 constant"
        # No admin preset re-introduces it.
        from app.core.bot_instance_service import BotInstanceService

        for profile in ("conservative", "balanced", "aggressive"):
            preset = BotInstanceService.get_risk_profile_preset(profile)
            assert "min_confidence_score" not in preset.get("additional_params", {}), profile

    def test_3_min_threshold_is_below_the_base_and_above_the_weakest_lone_expert(self):
        minimum = setting_default("THRESHOLD_MIN")
        assert minimum == DEFAULTS["min_threshold"]
        assert minimum < 0.30
        assert production_policy().min_threshold < production_policy().base_threshold
        # The most permissive state still rejects sma_cross voting alone.
        assert minimum > EMPIRICAL["lone_sma_cluster"]

    def test_4_max_threshold_is_attainable_on_the_observed_distribution(self):
        policy = production_policy()
        assert EMPIRICAL["p95"] < policy.max_threshold <= EMPIRICAL["p99"]
        assert policy.max_threshold < EMPIRICAL["max"]
        # A typical two-expert WEAK_TREND consensus clears even the ceiling.
        two_expert = (1.5 * 0.80 + 1.3 * 0.75) / 3.0
        assert two_expert >= policy.max_threshold
        assert abs(policy.max_threshold - EMPIRICAL["multi_expert_p50"]) < 0.01

    def test_ordinary_penalties_stop_at_p90_and_every_penalty_below_p95(self):
        policy = production_policy()
        market = sum(
            getattr(policy, f"{name}_bound")
            for name in ("regime", "volatility", "agreement", "htf", "market_quality")
        )
        assert policy.base_threshold + market == pytest.approx(EMPIRICAL["p90"], abs=0.005)
        assert policy.base_threshold + policy.total_adjustment_bound < EMPIRICAL["p95"]
        # No single term dominates the base.
        for name in ("regime", "volatility", "agreement", "htf", "market_quality",
                     "performance", "distribution"):
            assert getattr(policy, f"{name}_bound") < 0.1 * policy.base_threshold, name

    def test_configuration_and_policy_defaults_are_one_set_of_numbers(self):
        for setting, field in SETTING_MAP.items():
            if field in {"mode", "base_threshold", "static_threshold", "hard_block_regimes"}:
                continue
            assert setting_default(setting) == pytest.approx(DEFAULTS[field]), setting


# ═════════════════════════════════════════════════════════════════════════════
# 5-6  AGREEMENT -- breadth among the eligible experts
# ═════════════════════════════════════════════════════════════════════════════


class TestAgreement:
    def test_5_favourable_agreement_lowers_or_holds_the_threshold(self):
        unanimous = experts({n: ("BUY", 0.7) for n in WEAK_TREND_WEIGHTS})
        score, adjustment, _ = agreement_component(unanimous, 0.029, "BUY")
        assert score == pytest.approx(1.0)
        assert adjustment == pytest.approx(-0.029)

        majority = experts({"supertrend": ("BUY", 0.65), "trend_pullback": ("BUY", 0.65)})
        _, adjustment, _ = agreement_component(majority, 0.029, "BUY")
        assert adjustment <= 0.0

    def test_6_poor_agreement_raises_the_threshold(self):
        lone = experts(LONE_SUPERTREND)
        _, lone_adj, _ = agreement_component(lone, 0.029, "BUY")
        assert lone_adj > 0

        opposed = experts({"supertrend": ("BUY", 0.65), "sma_cross": ("SELL", 0.65)})
        _, opposed_adj, _ = agreement_component(opposed, 0.029, "BUY")
        assert opposed_adj > lone_adj

    def test_confidence_is_not_counted_a_second_time(self):
        """Confidence already is the opportunity's confidence -- the number the
        threshold is compared with. 1.0.0 weighted agreement by it again."""
        cool = agreement_component(experts({"supertrend": ("BUY", 0.51)}), 0.029, "BUY")
        hot = agreement_component(experts({"supertrend": ("BUY", 0.99)}), 0.029, "BUY")
        assert cool[0] == hot[0]
        assert cool[1] == hot[1]

    def test_denominator_is_the_eligible_experts_only(self):
        with_disabled = experts({n: ("BUY", 0.7) for n in WEAK_TREND_WEIGHTS})
        only_eligible = tuple(e for e in with_disabled if e.eligible)
        a = agreement_component(with_disabled, 0.029, "BUY")
        b = agreement_component(only_eligible, 0.029, "BUY")
        assert a[0] == b[0] == pytest.approx(1.0)
        assert a[2]["eligible_weight"] == pytest.approx(sum(WEAK_TREND_WEIGHTS.values()))

    def test_a_regime_is_not_penalised_for_running_only_its_intended_experts(self):
        """RANGE runs three experts. Two of three agreeing is a majority there."""
        range_experts = (
            ExpertEvidence("bollinger_reversion", True, True, "BUY", 0.6, weight=1.3),
            ExpertEvidence("vwap_reversion", True, True, "BUY", 0.6, weight=1.56),
            ExpertEvidence("squeeze_breakout", True, True, "HOLD", 0.0, weight=1.43),
        ) + tuple(
            ExpertEvidence(n, False, False, "DISABLED", 0.0, weight=w)
            for n, w in WEAK_TREND_WEIGHTS.items()
        )
        _, adjustment, breakdown = agreement_component(range_experts, 0.029, "BUY")
        assert breakdown["eligible"] == 3
        assert adjustment < 0

    def test_the_1_0_0_lock_is_gone(self):
        """The shape that raised the bar on 4,643 of 4,644 replayed opportunities:
        a lone expert used to earn a near-maximal penalty; now a modest one."""
        _, adjustment, _ = agreement_component(experts(LONE_SUPERTREND), 0.029, "BUY")
        assert 0 < adjustment < 0.5 * 0.029


# ═════════════════════════════════════════════════════════════════════════════
# 7-10  HTF AND MARKET QUALITY
# ═════════════════════════════════════════════════════════════════════════════


def _htf_snapshot(last_close: float):
    from app.runner.market_snapshot import MarketSnapshot

    four_h, fifteen = 14_400_000, 900_000
    start = 1_767_225_600_000
    closes = [100.0] * 219 + [last_close]
    htf = [[start + i * four_h, c, c * 1.001, c * 0.999, c, 1.0, start + (i + 1) * four_h - 1]
           for i, c in enumerate(closes)]
    primary_open = start + len(closes) * four_h
    primary = [[primary_open, 100.0, 100.1, 99.9, 100.0, 1.0, primary_open + fifteen - 1]]
    return MarketSnapshot.build(symbol="BTCUSDT", timeframe="15m", candles=primary,
                                source="test", higher_timeframe="4h",
                                higher_timeframe_candles=htf)


class TestHTFAndMarketQuality:
    def test_7_favourable_htf_lowers_the_threshold_and_more_so_when_stronger(self):
        weak = htf_component(HTFContext(timeframe="4h", direction="BUY", strength=0.3), "BUY", 0.018)[1]
        strong = htf_component(HTFContext(timeframe="4h", direction="BUY", strength=1.0), "BUY", 0.018)[1]
        assert strong < weak < 0

    def test_8_opposed_htf_raises_the_threshold(self):
        adj = htf_component(HTFContext(timeframe="4h", direction="SELL", strength=0.6), "BUY", 0.018)[1]
        assert adj > 0

    def test_htf_strength_is_graded_on_the_observed_distance_scale(self):
        """A 6% distance from the 4h EMA200 is below the observed median. 1.0.0
        read it as full strength; 1.1.0 grades it."""
        assert HTF_STRENGTH_SATURATION == pytest.approx(0.13)
        ctx = MasterEnsembleStrategy._htf_context(_htf_snapshot(106.0), {})
        assert ctx.direction == "BUY"
        assert 0.40 < ctx.strength < 0.50

    def test_9_better_market_quality_lowers_the_threshold(self):
        assert market_quality_component(MarketQualityContext(volume_percentile=0.9), 0.014)[1] < 0

    def test_10_worse_market_quality_raises_the_threshold(self):
        assert market_quality_component(MarketQualityContext(volume_percentile=0.1), 0.014)[1] > 0

    def test_the_engine_orders_contexts_monotonically(self):
        policy = production_policy()
        good = engine().evaluate(request(
            htf=HTFContext(timeframe="4h", direction="BUY", strength=0.8),
            market_quality=MarketQualityContext(volume_percentile=0.9),
        ), policy)
        bad = engine().evaluate(request(
            htf=HTFContext(timeframe="4h", direction="SELL", strength=0.8),
            market_quality=MarketQualityContext(volume_percentile=0.1),
        ), policy)
        assert good.final_threshold < policy.base_threshold + 0.03 < bad.final_threshold


# ═════════════════════════════════════════════════════════════════════════════
# VOLATILITY -- like compared with like
# ═════════════════════════════════════════════════════════════════════════════


def _calming_market(n: int = 250):
    """Every candle gaps up, and the ranges shrink: volatility is falling all the time."""
    rows, price = [], 100.0
    for i in range(n):
        rng = 2.0 - i * (1.0 / n)
        o = price + 1.0
        c = o + rng
        rows.append([i * 900_000, o, c, o, c, 1.0, (i + 1) * 900_000 - 1])
        price = c
    return rows


class TestVolatilityPercentile:
    def test_rolling_series_ends_on_the_classifiers_own_value(self):
        rows = _calming_market()
        h, l, c = ([float(k[j]) for k in rows] for j in (2, 3, 4))
        series = MasterEnsembleStrategy._rolling_atr_percent(h, l, c)
        assert series[-1] == calculate_atr_percent(h, l, c)
        assert series[-20] == calculate_atr_percent(h[:-19], l[:-19], c[:-19])

    def test_a_calming_market_reads_calm(self):
        rows = _calming_market()
        h, l, c = ([float(k[j]) for k in rows] for j in (2, 3, 4))
        regime = SimpleNamespace(atr_percent=calculate_atr_percent(h, l, c), compression_ratio=0.5)
        ctx = MasterEnsembleStrategy._volatility_context(regime, rows)
        assert ctx.atr_percentile < 0.05

        # The 1.0.0 comparison -- ATR% against single-candle ranges -- put the
        # same market in the upper, bar-raising tail, because an average of true
        # ranges (gaps included) sits above most individual ranges.
        ranges = [(k[2] - k[3]) / k[4] * 100.0 for k in rows[-120:]]
        old = sum(1 for r in ranges if r <= regime.atr_percent) / len(ranges)
        assert old >= 0.75


# ═════════════════════════════════════════════════════════════════════════════
# 11-12  COLD START
# ═════════════════════════════════════════════════════════════════════════════


class TestColdStart:
    def test_11_distribution_cold_start_is_neutral(self):
        result = DistributionCalibrator.evaluate(
            [0.9] * 39, base_threshold=0.30, target_percentile=0.60, min_samples=40, bound=0.018,
        )
        assert result.adjustment == 0.0
        assert result.status == CalibrationStatus.INSUFFICIENT_SAMPLE
        decision = engine().evaluate(request(), production_policy())
        assert decision.distribution_adjustment == 0.0
        assert decision.distribution_status == CalibrationStatus.INSUFFICIENT_SAMPLE

    def test_distribution_calibration_cannot_recreate_a_0_70_bar(self):
        result = DistributionCalibrator.evaluate(
            [0.95] * 200, base_threshold=0.30, target_percentile=0.60, min_samples=40, bound=0.018,
        )
        assert result.adjustment == pytest.approx(0.018)
        policy = production_policy()
        assert policy.base_threshold + policy.total_adjustment_bound < 0.45

    def test_12_performance_cold_start_is_neutral(self):
        result = PerformanceCalibrator(NoTrades()).evaluate(
            bot_instance_id="b", symbol="BTCUSDT", timeframe="15m",
            min_samples=30, lookback=100, bound=0.018,
        )
        assert result.adjustment == 0.0
        assert result.status == CalibrationStatus.INSUFFICIENT_SAMPLE
        decision = engine().evaluate(request(), production_policy())
        assert decision.performance_adjustment == 0.0


# ═════════════════════════════════════════════════════════════════════════════
# 13-15  CALIBRATION EPOCH, SMOOTHING, RATE LIMITING
# ═════════════════════════════════════════════════════════════════════════════


class TestCalibrationEpoch:
    def test_13_a_0_70_era_state_cannot_drag_the_new_policy(self):
        policy = production_policy()
        store = ThresholdStateStore()
        seed(store, policy, previous=0.78, version="1.0.0", policy_hash=POLICY_1_0_0_HASH,
             samples=[0.325] * 21)

        decision = engine(store).evaluate(request(), policy)

        assert decision.previous_threshold is None
        assert decision.smoothing_applied is False
        assert decision.rate_limit_applied is False
        assert decision.distribution_sample_size == 0, "old-epoch samples are discarded"
        assert decision.final_threshold == pytest.approx(
            min(max(decision.raw_unclamped_threshold, policy.min_threshold), policy.max_threshold)
        )
        assert decision.final_threshold < 0.40
        assert "CALIBRATION_EPOCH_RESET" in decision.state_reset_reason
        assert "1.0.0->1.1.0" in decision.state_reset_reason
        assert decision.detail.startswith("CALIBRATION_EPOCH_RESET")

        # The next candle smooths against the new epoch's own anchor.
        follow = engine(store).evaluate(request(closed_candle_time=1_789_000_900_000), policy)
        assert follow.state_reset_reason is None
        assert follow.previous_threshold == pytest.approx(decision.final_threshold)

    def test_14_policy_version_reset_is_persisted(self, tmp_path):
        db = DB(str(tmp_path / "epoch.db"))
        migrate(db)
        policy = production_policy()
        store = SqliteThresholdStateStore(db)
        seed(store, policy, previous=0.78, version="1.0.0", policy_hash=POLICY_1_0_0_HASH,
             samples=[0.30] * 15)

        decision = engine(SqliteThresholdStateStore(db)).evaluate(request(), policy)
        assert decision.state_reset_reason

        with db.connect() as conn:
            row = conn.execute(
                "SELECT previous_threshold, engine_version, policy_hash, distribution_samples_json "
                "FROM adaptive_threshold_state WHERE bot_instance_id=? AND symbol=?",
                ("bot_recalibration", "BTCUSDT"),
            ).fetchone()
        assert row[0] == pytest.approx(decision.final_threshold)
        assert row[1] == ENGINE_VERSION
        assert row[2] == policy.policy_hash
        assert row[3] == "[0.325]"

    def test_a_policy_change_under_the_same_engine_also_opens_an_epoch(self):
        store = ThresholdStateStore()
        old = production_policy()
        seed(store, old, previous=0.33)
        changed = resolve_threshold_policy(
            scopes=[("GLOBAL", {"min_threshold": 0.25, "max_threshold": 0.60})], base_threshold=0.31,
        )
        assert changed.policy_hash != old.policy_hash
        assert engine(store).evaluate(request(), changed).state_reset_reason

    def test_a_state_from_the_current_epoch_is_kept(self):
        store = ThresholdStateStore()
        policy = production_policy()
        seed(store, policy, previous=0.33)
        decision = engine(store).evaluate(request(), policy)
        assert decision.state_reset_reason is None
        assert decision.previous_threshold == pytest.approx(0.33)

    def test_15_rate_limiting_holds_in_both_directions(self):
        policy = production_policy()

        up_store = ThresholdStateStore()
        seed(up_store, policy, previous=0.30)
        adverse = request(
            regime=RegimeContext(regime="STRONG_TREND", regime_confidence=1.0),
            experts=experts({"sma_cross": ("BUY", 0.65)}),
            htf=HTFContext(timeframe="4h", direction="SELL", strength=1.0),
            market_quality=MarketQualityContext(volume_percentile=0.0),
        )
        up = engine(up_store).evaluate(adverse, policy)
        assert up.rate_limit_applied
        assert up.final_threshold == pytest.approx(0.30 + policy.max_step_up)

        down_store = ThresholdStateStore()
        seed(down_store, policy, previous=0.45)
        favourable = request(
            regime=RegimeContext(regime="RANGE", regime_confidence=1.0),
            experts=experts({n: ("BUY", 0.7) for n in WEAK_TREND_WEIGHTS}),
            htf=HTFContext(timeframe="4h", direction="BUY", strength=1.0),
            market_quality=MarketQualityContext(volume_percentile=1.0),
        )
        down = engine(down_store).evaluate(favourable, policy)
        assert down.rate_limit_applied
        assert down.final_threshold == pytest.approx(0.45 - policy.max_step_down)


# ═════════════════════════════════════════════════════════════════════════════
# 16-18  HARD GATES STAY HARD
# ═════════════════════════════════════════════════════════════════════════════


class TestHardGates:
    def test_16_hard_gates_are_not_expressed_as_thresholds(self):
        policy = production_policy()
        stale = engine().evaluate(
            request(opportunity_confidence=0.99,
                    market_quality=MarketQualityContext(data_stale=True)), policy,
        )
        assert stale.status == ThresholdStatus.HARD_BLOCKED
        assert stale.reason == REASON_MARKET_DATA_STALE
        assert stale.final_threshold is None and stale.passed is None

    def test_17_expert_error_still_fails_closed(self):
        errored = experts(LONE_SUPERTREND)[:3] + (
            ExpertEvidence("sma_cross", True, True, "ERROR", 0.0, weight=0.9, reason="data_error:x"),
        )
        decision = engine().evaluate(
            request(opportunity_confidence=0.99, experts=errored), production_policy()
        )
        assert decision.status == ThresholdStatus.ERROR
        assert decision.reason == REASON_EXPERT_ERROR
        assert decision.final_threshold is None

    def test_18_low_volatility_chop_still_hard_blocks(self):
        policy = production_policy()
        assert policy.hard_block_regimes == ("LOW_VOLATILITY_CHOP",)
        decision = engine().evaluate(
            request(opportunity_confidence=0.99,
                    regime=RegimeContext(regime="LOW_VOLATILITY_CHOP", regime_confidence=1.0)),
            policy,
        )
        assert decision.status == ThresholdStatus.HARD_BLOCKED
        assert decision.reason == REASON_REGIME_HARD_BLOCK
        assert decision.final_threshold is None


# ═════════════════════════════════════════════════════════════════════════════
# 19-21  ONE AUTHORITY, EVERY PATH
# ═════════════════════════════════════════════════════════════════════════════


def _klines(n: int = 120):
    rnd = random.Random(3)
    rows, price, close = [], 100.0, 1767225599999
    first_open = close + 1 - n * 900_000
    for i in range(n):
        o = price
        c = o * (1 + rnd.uniform(-0.003, 0.004))
        rows.append([first_open + i * 900_000, o, max(o, c) * 1.001, min(o, c) * 0.999, c,
                     1000.0, first_open + (i + 1) * 900_000 - 1])
        price = c
    return rows


class TestOneAuthority:
    def test_19_external_signals_use_the_same_engine_and_policy(self, tmp_path):
        assert isinstance(gate_module._default_engine(), AdaptiveEntryThresholdEngine)
        db = DB(str(tmp_path / "external.db"))
        migrate(db)
        result = gate_module.evaluate_external_candidate(
            db=db, symbol="BTCUSDT", side="BUY", confidence=0.70, klines=_klines(),
            timeframe="15m", source="TRADINGVIEW", bot_instance_id="bot_external_1_1",
            policy_resolver=lambda **_: production_policy(),
            regime_classifier_factory=lambda: SimpleNamespace(classify_stable=lambda h, l, c: SimpleNamespace(
                regime=MarketRegime.WEAK_TREND, regime_confidence=0.9, atr_percent=0.5,
                compression_ratio=0.2)),
        )
        decision = result.threshold_decision
        assert decision.threshold_engine_version == ENGINE_VERSION
        assert decision.base_threshold == pytest.approx(0.30)
        assert decision.policy_hash == production_policy().policy_hash
        assert result.passed is True
        rows = load_threshold_decisions(db, bot_instance_id="bot_external_1_1")
        assert [r["threshold_engine_version"] for r in rows] == [ENGINE_VERSION]

    def test_20_exactly_one_threshold_engine_and_no_second_ensemble_gate(self):
        engines = set()
        for path in APP.rglob("*.py"):
            for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
                if isinstance(node, ast.ClassDef) and "Threshold" in node.name and node.name.endswith(
                    ("Engine", "Calculator", "Resolver")
                ):
                    engines.add(node.name)
        assert engines == {"AdaptiveEntryThresholdEngine"}

        assert "min_confidence" not in inspect.signature(MasterEnsembleStrategy.__init__).parameters
        source = code_only(inspect.getsource(MasterEnsembleStrategy))
        assert "legacy_secondary_confidence_gate" not in source
        assert "self.min_confidence" not in source
        schema = ensemble_module.MasterEnsembleStrategy.__dict__.get("params_schema") or {}
        assert "min_confidence" not in str(schema)

    def test_21_the_ensemble_emits_an_entry_only_on_the_quality_verdict(self):
        tree = ast.parse(textwrap.dedent(inspect.getsource(MasterEnsembleStrategy.get_signal)))
        parents = {child: node for node in ast.walk(tree) for child in ast.iter_child_nodes(node)}
        entries = [
            node for node in ast.walk(tree)
            if isinstance(node, ast.Assign)
            and any(isinstance(t, ast.Name) and t.id == "final_signal" for t in node.targets)
            and any(isinstance(n, ast.Attribute) and n.attr in {"BUY", "SELL"}
                    and isinstance(n.value, ast.Name) and n.value.id == "Signal"
                    for n in ast.walk(node.value))
        ]
        assert entries, "the ensemble must still be able to emit an entry"
        for node in entries:
            ancestor, guarded = parents.get(node), False
            while ancestor is not None:
                if isinstance(ancestor, ast.If) and ast.unparse(ancestor.test) == "entry_quality.approved":
                    guarded = True
                    break
                ancestor = parents.get(ancestor)
            assert guarded, f"entry emitted outside the quality verdict: {ast.unparse(node)}"


# ═════════════════════════════════════════════════════════════════════════════
# 22  PERSISTED EVIDENCE RECONCILES
# ═════════════════════════════════════════════════════════════════════════════


class TestPersistedEvidence:
    def test_22_persisted_rows_reproduce_the_threshold(self, tmp_path):
        db = DB(str(tmp_path / "evidence.db"))
        migrate(db)
        policy = production_policy()
        store = ThresholdStateStore()
        eng = engine(store)
        first = eng.evaluate(request(), policy)
        second = eng.evaluate(request(
            closed_candle_time=1_789_000_900_000,
            htf=HTFContext(timeframe="4h", direction="SELL", strength=1.0),
            market_quality=MarketQualityContext(volume_percentile=0.05),
        ), policy)
        for decision in (first, second):
            record_threshold_decision(db, decision)

        rows = {r["threshold_decision_id"]: r for r in
                load_threshold_decisions(db, bot_instance_id="bot_recalibration")}
        for decision in (first, second):
            row = rows[decision.threshold_decision_id]
            assert row["reconciles"] == 1
            components = sum(row[k] for k in (
                "base_threshold", "regime_adjustment", "volatility_adjustment",
                "agreement_adjustment", "htf_adjustment", "market_quality_adjustment",
                "performance_adjustment", "distribution_adjustment",
            ))
            assert components == pytest.approx(row["raw_unclamped_threshold"], abs=1e-6)
            previous = row["previous_threshold"]
            raw = row["raw_unclamped_threshold"]
            smoothed = raw if previous is None else (
                policy.smoothing_alpha * raw + (1 - policy.smoothing_alpha) * previous
            )
            assert row["smoothed_threshold"] == pytest.approx(smoothed, abs=1e-6)
            limited = smoothed if previous is None else min(
                max(smoothed, previous - policy.max_step_down), previous + policy.max_step_up
            )
            assert row["rate_limited_threshold"] == pytest.approx(limited, abs=1e-6)
            final = min(max(limited, row["min_threshold"]), row["max_threshold"])
            assert row["final_threshold"] == pytest.approx(final, abs=1e-6)
            assert row["threshold_engine_version"] == ENGINE_VERSION
            assert row["policy_hash"] == policy.policy_hash
            assert bool(row["passed"]) == (row["opportunity_confidence"] >= row["final_threshold"])
        assert rows[second.threshold_decision_id]["previous_threshold"] == pytest.approx(
            first.final_threshold
        )
