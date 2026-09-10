"""Phase 7 — TradingOpportunity and the single entry-quality authority.

The architectural claim under test: there is exactly ONE top-level comparison of
confidence against a threshold in the active path, it lives in
TradingDecisionEngine, and nothing downstream re-litigates it.

Updated for the threshold rebuild. This engine no longer *resolves* a threshold
— :class:`~app.threshold.engine.AdaptiveEntryThresholdEngine` does, and is the
only component allowed to. What is tested here is the comparison and the reason
codes, not where the number came from.
"""
from __future__ import annotations

import inspect

import pytest

from app.decision import (
    EntryQualityDecision,
    NoOpportunity,
    QualityReason,
    TradingDecisionEngine,
    TradingOpportunity,
)
from app.decision.opportunity import build_opportunity
from app.decision.reason_mapping import (
    DUPLICATE_CONFIDENCE_CODES,
    assert_no_duplicate_confidence_authority,
    is_execution_reason,
    is_risk_reason,
    to_canonical,
)
from app.decision.reasons import DISTINCT_QUALITY_FAILURES, CycleReason, ExecutionReason, RiskReason
from app.threshold.contracts import (
    AdaptiveThresholdDecision,
    ThresholdMode,
    ThresholdStatus,
)

VOTES = [("sma", "BUY", 0.8), ("donchian", "BUY", 0.6), ("vwap", "SELL", 0.3)]


@pytest.fixture
def engine():
    return TradingDecisionEngine()


def threshold_at(value: float) -> AdaptiveThresholdDecision:
    """An EVALUATED threshold decision fixed at ``value``.

    Built through the real contract rather than a stub, so a test cannot
    accidentally hand the comparison a threshold shape the engine could never
    produce.
    """
    return AdaptiveThresholdDecision(
        threshold_decision_id="thr_fixture",
        bot_instance_id="bot_test",
        symbol="BTCUSDT",
        timeframe="15m",
        status=ThresholdStatus.EVALUATED,
        threshold_engine_version="1.0.0",
        threshold_mode=ThresholdMode.ADAPTIVE,
        base_threshold=value,
        raw_unclamped_threshold=value,
        final_threshold=value,
        min_threshold=0.0,
        max_threshold=1.0,
    )


def make_opportunity(**overrides) -> TradingOpportunity:
    params = dict(
        symbol="BTCUSDT",
        timeframe="15m",
        market_snapshot_id="ms_test",
        side="BUY",
        raw_confidence=0.72,
        consensus=0.72,
        buy_score=0.72,
        sell_score=0.10,
        votes=VOTES,
        regime="TREND",
        regime_confidence=0.8,
    )
    params.update(overrides)
    return build_opportunity(**params)


# ── 1/2. Master Ensemble produces a TradingOpportunity with its evidence ─────


def test_opportunity_is_immutable():
    opp = make_opportunity()
    with pytest.raises(Exception):
        opp.raw_confidence = 0.99  # type: ignore[misc]


def test_opportunity_preserves_component_breakdown():
    breakdown = [{"name": "sma", "signal": "BUY", "confidence": 0.8}]
    opp = make_opportunity(component_breakdown=breakdown)
    assert opp.component_breakdown == breakdown


def test_opportunity_derives_supporting_and_opposing_strategies():
    opp = make_opportunity()
    assert opp.supporting_strategies == ("sma", "donchian")
    assert opp.opposing_strategies == ("vwap",)


def test_opportunity_normalizes_symbol_and_side():
    opp = make_opportunity(symbol="btcusdt", side="buy")
    assert opp.symbol == "BTCUSDT"
    assert opp.side == "BUY"


def test_opportunity_rejects_a_non_directional_side():
    with pytest.raises(ValueError):
        make_opportunity(side="HOLD")


def test_opportunity_carries_correlation_identifiers():
    """Phase 10 §10 — later evidence layers must not reverse-engineer strings."""
    opp = make_opportunity(bot_instance_id="bot-1", closed_candle_time=1700)
    summary = opp.evidence_summary()
    assert opp.opportunity_id.startswith("opp_")
    assert summary["market_snapshot_id"] == "ms_test"
    assert summary["closed_candle_time"] == 1700
    assert opp.bot_instance_id == "bot-1"


def test_opportunity_excludes_risk_and_execution_permission():
    """§7.3 — the opportunity must not carry sizing or approval."""
    fields = set(make_opportunity().to_dict())
    for forbidden in ("quantity", "approved", "position_size", "approved_notional", "leverage"):
        assert forbidden not in fields


# ── 3. NO_OPPORTUNITY is a real outcome, not a confidence failure ────────────


def test_no_opportunity_is_distinct_from_a_confidence_failure(engine):
    none = NoOpportunity(symbol="BTCUSDT", timeframe="15m", market_snapshot_id="ms_test")
    decision = engine.evaluate(none)

    assert decision.approved is False
    assert decision.primary_reason == QualityReason.NO_OPPORTUNITY
    assert decision.primary_reason != QualityReason.ENTRY_CONFIDENCE_BELOW_THRESHOLD


def test_no_opportunity_rejects_an_unrelated_reason_code():
    with pytest.raises(ValueError):
        NoOpportunity(
            symbol="BTCUSDT",
            timeframe="15m",
            market_snapshot_id="ms",
            reason_code=QualityReason.ENTRY_CONFIDENCE_BELOW_THRESHOLD,
        )


# ── 4. Consensus is evidence, not a second gate ─────────────────────────────


def test_consensus_is_recorded_but_never_gates(engine):
    """The consensus gate was removed with the rest of the multi-authority stack.

    Expert agreement is now one bounded input to the threshold. Gating on it
    separately as well would be two authorities answering one question -- and
    the old gate never fired anyway, because consensus_required was always 0.0.
    """
    opp = make_opportunity(raw_confidence=0.95, consensus=0.10)
    decision = engine.evaluate(opp, threshold_decision=threshold_at(0.50))

    assert decision.approved is True
    assert decision.consensus_observed == pytest.approx(0.10)
    assert not hasattr(decision, "consensus_required")


def test_confidence_failure_when_direction_agrees_but_quality_is_low(engine):
    opp = make_opportunity(raw_confidence=0.30, consensus=0.90)
    decision = engine.evaluate(opp, threshold_decision=threshold_at(0.65))

    assert decision.approved is False
    assert decision.primary_reason == QualityReason.ENTRY_CONFIDENCE_BELOW_THRESHOLD


def test_the_three_quality_failures_never_collapse():
    assert len(DISTINCT_QUALITY_FAILURES) == 3
    assert QualityReason.NO_OPPORTUNITY != QualityReason.CONSENSUS_INSUFFICIENT
    assert QualityReason.CONSENSUS_INSUFFICIENT != QualityReason.ENTRY_CONFIDENCE_BELOW_THRESHOLD


def test_no_new_candle_is_not_a_quality_verdict():
    """§6.9 — the strategy not running is not the strategy finding nothing."""
    assert CycleReason.NO_NEW_CANDLE not in DISTINCT_QUALITY_FAILURES
    assert CycleReason.NO_NEW_CANDLE != QualityReason.NO_OPPORTUNITY


# ── 5. Exactly one top-level confidence comparison ──────────────────────────


def test_engine_approves_at_or_above_threshold(engine):
    opp = make_opportunity(raw_confidence=0.65)
    assert engine.evaluate(opp, threshold_decision=threshold_at(0.65)).approved is True
    assert engine.evaluate(opp, threshold_decision=threshold_at(0.6499)).approved is True


def test_engine_rejects_below_threshold(engine):
    opp = make_opportunity(raw_confidence=0.6499)
    decision = engine.evaluate(opp, threshold_decision=threshold_at(0.65))
    assert decision.approved is False
    assert decision.primary_reason == QualityReason.ENTRY_CONFIDENCE_BELOW_THRESHOLD


def test_master_ensemble_delegates_the_comparison_and_keeps_none_of_its_own():
    """Source-level proof that the ensemble no longer gates on the threshold."""
    from app.strategy import master_ensemble

    source = inspect.getsource(master_ensemble.MasterEnsembleStrategy.get_signal)

    assert "self._decision_engine.evaluate(" in source
    # The old two-branch threshold comparison must be gone.
    assert "buy_pct >= effective_threshold" not in source
    assert "sell_pct >= effective_threshold" not in source


def test_exactly_one_confidence_comparison_exists_in_the_decision_engine():
    source = inspect.getsource(TradingDecisionEngine.evaluate)
    comparisons = [
        line for line in source.splitlines()
        if "raw_confidence >=" in line or "raw_confidence <" in line
    ]
    assert len(comparisons) == 1, f"expected one comparison, found {comparisons}"


# ── 6/7. Safety and Policy have NO confidence gate left to reapply ────────


def test_policy_engine_has_no_confidence_gate_at_all():
    """Previously this asserted the gate was *bypassed* when quality approved.

    Bypassing left the gate in place, one flag away from re-litigating a
    decision it does not own. It is now deleted outright, which is the stronger
    guarantee: there is nothing to bypass.
    """
    from app.policy import policy_engine
    from app.policy.policy_engine import PolicyEngine

    source = inspect.getsource(policy_engine)
    assert "self.min_confidence" not in source
    assert "min_confidence" not in inspect.signature(PolicyEngine.__init__).parameters


def test_safety_engine_has_no_confidence_gate_at_all():
    """Gate 3 resolved its own threshold and returned LOW_CONFIDENCE. Deleted."""
    from app.risk import safety_engine
    from app.risk.safety_engine import SafetyConfig

    source = inspect.getsource(safety_engine)
    assert "confidence < threshold" not in source
    fields = set(SafetyConfig.__dataclass_fields__)
    assert "min_confidence_hard" not in fields
    assert "min_confidence_soft" not in fields


def test_orchestrator_declares_quality_already_decided_to_both_engines():
    from app.core import trading_orchestrator

    source = inspect.getsource(trading_orchestrator)
    assert source.count("confidence_already_approved=True") >= 2


def test_duplicate_confidence_authority_is_detected():
    with pytest.raises(AssertionError):
        assert_no_duplicate_confidence_authority("LOW_CONFIDENCE")
    assert_no_duplicate_confidence_authority(RiskReason.MAX_OPEN_POSITIONS)  # no raise


def test_low_confidence_has_no_canonical_equivalent():
    """It must not be translatable — it is a duplicate authority, not a reason."""
    assert "LOW_CONFIDENCE" in DUPLICATE_CONFIDENCE_CODES
    from app.decision.reason_mapping import LEGACY_REASON_MAP

    assert "LOW_CONFIDENCE" not in LEGACY_REASON_MAP


# ── 8/9. Risk and execution can still reject an approved opportunity ────────


def test_risk_reasons_are_distinct_from_execution_reasons():
    assert is_risk_reason(RiskReason.MAX_OPEN_POSITIONS)
    assert not is_execution_reason(RiskReason.MAX_OPEN_POSITIONS)

    assert is_execution_reason(ExecutionReason.DATA_STALE)
    assert not is_risk_reason(ExecutionReason.DATA_STALE)


@pytest.mark.parametrize(
    "legacy,canonical",
    [
        ("MAX_POSITIONS_REACHED", RiskReason.MAX_OPEN_POSITIONS),
        ("DAILY_LOSS_LIMIT", RiskReason.DAILY_LOSS_LIMIT),
        ("RISK_REWARD_TOO_LOW", RiskReason.RR_BELOW_MINIMUM),
        ("LEVERAGE_TOO_HIGH", RiskReason.LEVERAGE_LIMIT),
        ("MIN_NOTIONAL_NOT_MET", ExecutionReason.MIN_NOTIONAL),
        ("PRICE_INVALID", ExecutionReason.DATA_STALE),
        ("MARKET_CLOSED", ExecutionReason.SYMBOL_UNAVAILABLE),
    ],
)
def test_legacy_reason_codes_map_to_the_canonical_taxonomy(legacy, canonical):
    assert to_canonical(legacy) == canonical


def test_min_notional_is_execution_owned_not_risk_owned():
    """§8.8 — an exchange filter is a placement problem, not an affordability one."""
    assert is_execution_reason(to_canonical("MIN_NOTIONAL_NOT_MET"))


# ── Vetoes are applied after quality without re-comparing confidence ────────


def test_a_hard_veto_overrides_an_approved_decision_and_keeps_the_evidence(engine):
    approved = engine.evaluate(
        make_opportunity(raw_confidence=0.9), threshold_decision=threshold_at(0.5)
    )
    assert approved.approved is True

    vetoed = engine.veto(approved, QualityReason.HTF_NOT_ALIGNED)

    assert vetoed.approved is False
    assert vetoed.primary_reason == QualityReason.HTF_NOT_ALIGNED
    assert QualityReason.APPROVED_FOR_EXECUTION in vetoed.secondary_reasons
    assert vetoed.raw_confidence == approved.raw_confidence


def test_a_veto_on_an_already_rejected_decision_changes_nothing(engine):
    rejected = engine.evaluate(
        make_opportunity(raw_confidence=0.1), threshold_decision=threshold_at(0.9)
    )
    assert engine.veto(rejected, QualityReason.HTF_NOT_ALIGNED) is rejected


# ── Threshold resolution belongs elsewhere entirely ─────────────────────────


def test_this_engine_cannot_resolve_a_threshold():
    """resolve_threshold() was one of the four competing authorities.

    It took a base, an adaptive gate and a policy floor and applied max() to
    them, which is the operation that let MIN_CONFIDENCE_THRESHOLD=0.70
    dominate a dynamic value capped at 0.65. There is now exactly one component
    allowed to produce a threshold, and this is not it.
    """
    assert not hasattr(TradingDecisionEngine, "resolve_threshold")
    params = set(inspect.signature(TradingDecisionEngine.evaluate).parameters)
    for removed in ("base_threshold", "policy_floor", "adaptive_gate", "consensus_required"):
        assert removed not in params


def test_it_uses_the_engine_threshold_verbatim_without_raising_it():
    """No floor, no ceiling, no max(). The number arrives already decided."""
    decision = TradingDecisionEngine().evaluate(
        make_opportunity(raw_confidence=0.55), threshold_decision=threshold_at(0.5123)
    )
    assert decision.effective_entry_threshold == pytest.approx(0.5123)


def test_an_unevaluated_threshold_is_never_substituted_with_zero(engine):
    """0.0 >= 0.0 approving everything is the failure this replaces."""
    decision = engine.evaluate(make_opportunity(raw_confidence=0.0), threshold_decision=None)
    assert decision.approved is False
    assert decision.effective_entry_threshold is None


# ── 10/11. Determinism and no silent threshold change ───────────────────────


def test_the_same_inputs_always_produce_the_same_verdict(engine):
    opp = make_opportunity(raw_confidence=0.7)
    verdicts = {
        (d.approved, d.primary_reason, d.effective_entry_threshold)
        for d in (engine.evaluate(opp, threshold_decision=threshold_at(0.65)) for _ in range(10))
    }
    assert len(verdicts) == 1


def test_master_ensemble_does_not_introduce_a_new_consensus_gate():
    """No component applies a consensus requirement any more.

    The old assertion checked that the ensemble passed consensus_required=0.0.
    A parameter whose only correct value is "off" is a gate waiting to be turned
    on by accident, so it was removed rather than pinned.
    """
    from app.strategy import master_ensemble

    source = inspect.getsource(master_ensemble.MasterEnsembleStrategy.get_signal)
    assert "consensus_required" not in source


def test_decision_result_carries_no_position_sizing():
    """§7.11 — sizing belongs to the risk layer, not the quality verdict."""
    fields = set(EntryQualityDecision(approved=True, primary_reason="x").to_dict())
    for forbidden in ("quantity", "notional", "leverage", "margin", "position_size"):
        assert forbidden not in fields


def test_decision_observability_answers_every_operator_question(engine):
    """§7.12 — no component-log reading required."""
    decision = engine.evaluate(make_opportunity(), threshold_decision=threshold_at(0.65))
    obs = decision.observability()
    for key in (
        "approved", "primary_reason", "side", "raw_confidence",
        "effective_entry_threshold", "threshold_source", "consensus_observed",
        "regime", "opportunity_id", "market_snapshot_id",
    ):
        assert key in obs
