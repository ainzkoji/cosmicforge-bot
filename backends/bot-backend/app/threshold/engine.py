"""``AdaptiveEntryThresholdEngine`` -- the one entry-threshold authority.

Nothing else in the system may compute, raise, lower or replace the minimum
confidence required for an opportunity to pass entry quality.
:class:`~app.decision.decision_engine.TradingDecisionEngine` compares against
the number this engine produces; it does not produce one of its own.

The shape of a computation::

    base                              (policy)
      + regime_adjustment             \\
      + volatility_adjustment          |
      + agreement_adjustment           |  fast market context
      + htf_adjustment                 |
      + market_quality_adjustment     /
      = market_threshold

      + performance_adjustment        \\  slow calibration
      + distribution_adjustment       /
      = raw_unclamped_threshold

      -> smoothing (EMA against the previous threshold)
      -> rate limiting (max step up / down per candle)
      -> clamp to the single policy band
      = final_threshold

There is no step after the clamp. That is the whole point: the previous stack's
final act was ``max(dynamic, 0.70)``, applied by a component that did not know a
threshold had already been resolved.

Determinism is structural, not incidental. The engine reads its
:class:`~app.threshold.contracts.AdaptiveThresholdInput`, the policy, and the
persisted previous state -- and nothing else. No clock, no network, no
randomness, no module-level mutable state.
"""
from __future__ import annotations

import logging
import math
from typing import Any, Sequence

from app.threshold.calibration import DistributionCalibrator, PerformanceCalibrator
from app.threshold.contracts import (
    AdaptiveThresholdDecision,
    AdaptiveThresholdInput,
    CalibrationStatus,
    ExpertEvidence,
    ThresholdMode,
    ThresholdStatus,
    new_threshold_decision_id,
    not_evaluated,
)
from app.threshold.policy import EffectiveThresholdPolicy
from app.threshold.state import ThresholdState, ThresholdStateStore

logger = logging.getLogger(__name__)

ENGINE_VERSION = "1.0.0"

#: Reason codes emitted by the engine itself. Hard gates owned by other layers
#: (kill switch, daily loss, capital, execution feasibility) never appear here.
REASON_APPROVED = "THRESHOLD_PASSED"
REASON_BELOW = "THRESHOLD_NOT_MET"
REASON_NO_OPPORTUNITY = "NO_OPPORTUNITY"
REASON_REGIME_HARD_BLOCK = "REGIME_HARD_BLOCK"
REASON_MARKET_DATA_STALE = "MARKET_DATA_STALE"
REASON_NON_FINITE = "THRESHOLD_INPUT_NOT_FINITE"

#: How much each regime argues for more evidence. Positive raises the bar.
#: Grounded in the regime win-rate analysis already recorded in config.py:
#: STRONG_TREND was the primary loss driver and RANGE the best performer.
REGIME_FACTORS: dict[str, float] = {
    "STRONG_TREND": 1.0,
    "WEAK_TREND": 0.3,
    "RANGE": -0.5,
    "HIGH_VOLATILITY": 0.8,
    "TRANSITION": 0.6,
    "UNCERTAIN": 0.6,
    "LOW_VOLATILITY_CHOP": 1.0,
}


def _clamp(value: float, low: float, high: float) -> float:
    return max(float(low), min(float(high), float(value)))


def _bounded(factor: float, bound: float) -> float:
    """Scale a factor in ``[-1, 1]`` by a policy bound."""
    return round(_clamp(factor, -1.0, 1.0) * abs(float(bound)), 6)


def _finite(value: Any) -> bool:
    try:
        f = float(value)
    except (TypeError, ValueError):
        return False
    return math.isfinite(f)


# -- Fast market-context components -------------------------------------------


def regime_component(ctx: Any, bound: float) -> tuple[float | None, float]:
    """``(regime_confidence, adjustment)``.

    A regime the classifier is unsure about gets less influence, not more: the
    factor is weighted by classification confidence rather than applied at full
    strength on a coin flip.
    """
    regime = str(getattr(ctx, "regime", "") or "").upper()
    factor = REGIME_FACTORS.get(regime)
    if factor is None:
        return (None, 0.0)

    confidence = float(getattr(ctx, "regime_confidence", 0.0) or 0.0)
    trust = _clamp(0.5 + 0.5 * _clamp(confidence, 0.0, 1.0), 0.5, 1.0)
    factor *= trust
    if bool(getattr(ctx, "transition", False)):
        # Mid-transition the regime label is the least reliable input there is.
        factor += 0.2
    return (round(confidence, 6), _bounded(factor, bound))


def volatility_component(ctx: Any, bound: float) -> tuple[float | None, float]:
    """``(volatility_score, adjustment)``.

    Both tails argue for more evidence. Extreme volatility widens the range of
    outcomes around any entry; very low volatility means the setup has no room
    to pay for its costs. The healthy middle is neutral.
    """
    score = None
    for name in ("atr_percentile", "realized_vol_percentile", "range_percentile"):
        candidate = getattr(ctx, name, None)
        if candidate is not None and _finite(candidate):
            score = _clamp(float(candidate), 0.0, 1.0)
            break
    if score is None:
        return (None, 0.0)

    if bool(getattr(ctx, "gap_detected", False)):
        return (round(score, 6), _bounded(1.0, bound))

    if score >= 0.90:
        factor = 1.0
    elif score >= 0.75:
        factor = 0.5 + (score - 0.75) / 0.15 * 0.5
    elif score >= 0.35:
        factor = 0.0
    elif score >= 0.15:
        factor = (0.35 - score) / 0.20 * 0.5
    else:
        factor = 0.75

    acceleration = getattr(ctx, "acceleration", None)
    if acceleration is not None and _finite(acceleration):
        factor += _clamp(float(acceleration), -1.0, 1.0) * 0.2
    compression = getattr(ctx, "compression", None)
    if compression is not None and _finite(compression) and float(compression) > 0:
        # Compression before expansion is a setup, not a hazard.
        factor -= _clamp(float(compression), 0.0, 1.0) * 0.1

    return (round(score, 6), _bounded(factor, bound))


def agreement_component(
    experts: Sequence[ExpertEvidence], bound: float
) -> tuple[float | None, float, dict[str, Any]]:
    """``(agreement_score, adjustment, breakdown)``.

    ``agreement_score`` is weighted net support as a fraction of the weight that
    *could* have supported the side. An eligible expert that failed to run
    counts in the denominator and not the numerator, so missing evidence raises
    the bar. An expert the regime deactivated counts in neither -- it was never
    asked, and treating its silence as a HOLD would be a fabricated vote.
    """
    if not experts:
        return (None, 0.0, {})

    eligible = [e for e in experts if e.eligible]
    if not eligible:
        return (None, 0.0, {"eligible": 0})

    eligible_weight = sum(abs(float(e.weight)) for e in eligible)
    if eligible_weight <= 0:
        return (None, 0.0, {"eligible": len(eligible), "eligible_weight": 0.0})

    directional = [e for e in eligible if e.is_directional]
    buy_weight = sum(
        abs(float(e.weight)) * float(e.confidence) for e in directional if e.signal == "BUY"
    )
    sell_weight = sum(
        abs(float(e.weight)) * float(e.confidence) for e in directional if e.signal == "SELL"
    )
    support = max(buy_weight, sell_weight)
    oppose = min(buy_weight, sell_weight)

    score = _clamp((support - oppose) / eligible_weight, 0.0, 1.0)

    # Dispersion among the agreeing experts: unanimous-but-lukewarm is weaker
    # evidence than unanimous-and-convinced.
    confidences = [float(e.confidence) for e in directional if e.confidence > 0]
    dispersion = 0.0
    if len(confidences) > 1:
        mean = sum(confidences) / len(confidences)
        dispersion = (sum((c - mean) ** 2 for c in confidences) / len(confidences)) ** 0.5

    factor = 1.0 - 2.0 * score + _clamp(dispersion, 0.0, 0.5)
    leading = "BUY" if buy_weight >= sell_weight else "SELL"
    trailing = "SELL" if leading == "BUY" else "BUY"
    breakdown = {
        "eligible": len(eligible),
        "executed": sum(1 for e in eligible if e.executed),
        "not_run": sum(1 for e in eligible if not e.executed),
        "supporting": sum(1 for e in directional if e.signal == leading),
        "opposing": sum(1 for e in directional if e.signal == trailing),
        "neutral": sum(1 for e in eligible if e.executed and e.signal == "HOLD"),
        "weighted_support": round(support, 6),
        "weighted_opposition": round(oppose, 6),
        "eligible_weight": round(eligible_weight, 6),
        "dispersion": round(dispersion, 6),
    }
    return (round(score, 6), _bounded(factor, bound), breakdown)


def htf_component(ctx: Any, side: str | None, bound: float) -> tuple[float | None, float]:
    """``(htf_alignment_score, adjustment)``.

    Only closed, fresh higher-timeframe candles are used. A stale HTF read is
    treated as no information rather than as neutrality, because neutrality is
    itself a claim.
    """
    direction = getattr(ctx, "direction", None)
    if direction is None or not bool(getattr(ctx, "is_fresh", True)):
        return (None, 0.0)

    direction = str(direction).upper()
    strength = _clamp(float(getattr(ctx, "strength", 0.0) or 0.0), 0.0, 1.0)
    if direction == "NEUTRAL" or not side:
        return (0.0, 0.0)

    aligned = direction == str(side).upper()
    score = strength if aligned else -strength
    factor = -strength if aligned else strength
    return (round(score, 6), _bounded(factor, bound))


def market_quality_component(ctx: Any, bound: float) -> tuple[float | None, float]:
    """``(market_quality_score, adjustment)``.

    Poor-but-valid conditions raise the bar. Genuinely stale or unavailable data
    is a hard gate handled before the engine and never expressed here as a very
    high threshold.
    """
    parts: list[float] = []

    spread = getattr(ctx, "spread_percentile", None)
    if spread is not None and _finite(spread):
        parts.append(1.0 - _clamp(float(spread), 0.0, 1.0))
    liquidity = getattr(ctx, "liquidity_score", None)
    if liquidity is not None and _finite(liquidity):
        parts.append(_clamp(float(liquidity), 0.0, 1.0))
    volume = getattr(ctx, "volume_percentile", None)
    if volume is not None and _finite(volume):
        parts.append(_clamp(float(volume), 0.0, 1.0))
    slippage = getattr(ctx, "estimated_slippage_bps", None)
    if slippage is not None and _finite(slippage):
        # 0 bps is perfect, 20 bps or worse is as bad as this input gets.
        parts.append(1.0 - _clamp(float(slippage) / 20.0, 0.0, 1.0))

    if not parts:
        return (None, 0.0)

    score = sum(parts) / len(parts)
    factor = 1.0 - 2.0 * score
    if bool(getattr(ctx, "price_discontinuity", False)):
        factor += 0.5
    return (round(score, 6), _bounded(factor, bound))


# -- The engine ---------------------------------------------------------------


class AdaptiveEntryThresholdEngine:
    """Resolves the final entry threshold. The only component permitted to."""

    version = ENGINE_VERSION

    def __init__(
        self,
        *,
        state_store: ThresholdStateStore | None = None,
        performance_calibrator: PerformanceCalibrator | None = None,
    ) -> None:
        self.state_store = state_store or ThresholdStateStore()
        self.performance = performance_calibrator or PerformanceCalibrator()
        self.distribution = DistributionCalibrator()

    # -- Public API -------------------------------------------------------

    def evaluate(
        self,
        request: AdaptiveThresholdInput,
        policy: EffectiveThresholdPolicy,
    ) -> AdaptiveThresholdDecision:
        """Return the threshold decision for one opportunity on one candle."""
        identity = self._identity(request, policy)

        if policy.is_hard_blocked(request.regime.regime):
            return not_evaluated(
                engine_version=self.version,
                mode=policy.mode,
                status=ThresholdStatus.HARD_BLOCKED,
                reason=REASON_REGIME_HARD_BLOCK,
                detail=f"regime {request.regime.regime} is classified HARD_BLOCK by policy",
                regime=request.regime.regime,
                min_threshold=policy.min_threshold,
                max_threshold=policy.max_threshold,
                experts=request.experts,
                **identity,
            )

        if request.market_quality.data_stale:
            return not_evaluated(
                engine_version=self.version,
                mode=policy.mode,
                status=ThresholdStatus.HARD_BLOCKED,
                reason=REASON_MARKET_DATA_STALE,
                detail="market data is stale; quality is not evaluated on stale data",
                regime=request.regime.regime,
                min_threshold=policy.min_threshold,
                max_threshold=policy.max_threshold,
                experts=request.experts,
                **identity,
            )

        if request.side is None or request.opportunity_confidence is None:
            return not_evaluated(
                engine_version=self.version,
                mode=policy.mode,
                status=ThresholdStatus.NOT_EVALUATED,
                reason=REASON_NO_OPPORTUNITY,
                detail="no directional candidate reached the quality stage",
                regime=request.regime.regime,
                min_threshold=policy.min_threshold,
                max_threshold=policy.max_threshold,
                experts=request.experts,
                **identity,
            )

        if not _finite(request.opportunity_confidence):
            return not_evaluated(
                engine_version=self.version,
                mode=policy.mode,
                status=ThresholdStatus.INSUFFICIENT_DATA,
                reason=REASON_NON_FINITE,
                detail=f"opportunity_confidence={request.opportunity_confidence!r}",
                regime=request.regime.regime,
                min_threshold=policy.min_threshold,
                max_threshold=policy.max_threshold,
                experts=request.experts,
                **identity,
            )

        state = self.state_store.get(request.state_key())
        decision = self._compute(request, policy, state, identity)
        self._persist(request, policy, state, decision)
        return decision

    def evaluate_no_opportunity(
        self,
        request: AdaptiveThresholdInput,
        policy: EffectiveThresholdPolicy,
        *,
        reason: str = REASON_NO_OPPORTUNITY,
        detail: str = "",
    ) -> AdaptiveThresholdDecision:
        """Record that no threshold was evaluated, carrying ``final_threshold=None``."""
        return not_evaluated(
            engine_version=self.version,
            mode=policy.mode,
            status=ThresholdStatus.NOT_EVALUATED,
            reason=reason,
            detail=detail,
            regime=request.regime.regime,
            min_threshold=policy.min_threshold,
            max_threshold=policy.max_threshold,
            experts=request.experts,
            **self._identity(request, policy),
        )

    def evaluate_hard_block(
        self,
        request: AdaptiveThresholdInput,
        policy: EffectiveThresholdPolicy,
        *,
        reason: str,
        detail: str = "",
    ) -> AdaptiveThresholdDecision:
        """Record a hard gate. Never expressed as an unreachable threshold."""
        return not_evaluated(
            engine_version=self.version,
            mode=policy.mode,
            status=ThresholdStatus.HARD_BLOCKED,
            reason=reason,
            detail=detail,
            regime=request.regime.regime,
            min_threshold=policy.min_threshold,
            max_threshold=policy.max_threshold,
            experts=request.experts,
            **self._identity(request, policy),
        )

    # -- Internals --------------------------------------------------------

    @staticmethod
    def _identity(
        request: AdaptiveThresholdInput, policy: EffectiveThresholdPolicy
    ) -> dict[str, Any]:
        return {
            "bot_instance_id": request.bot_instance_id,
            "symbol": request.symbol,
            "timeframe": request.timeframe,
            "run_id": request.run_id,
            "cycle_id": request.cycle_id,
            "opportunity_id": request.opportunity_id,
            "market_snapshot_id": request.market_snapshot_id,
            "venue": request.venue,
            "market_type": request.market_type,
            "closed_candle_time": request.closed_candle_time,
            "policy_hash": policy.policy_hash,
        }

    def _compute(
        self,
        request: AdaptiveThresholdInput,
        policy: EffectiveThresholdPolicy,
        state: ThresholdState,
        identity: dict[str, Any],
    ) -> AdaptiveThresholdDecision:
        confidence = float(request.opportunity_confidence)

        if policy.mode == ThresholdMode.STATIC:
            return self._static_decision(request, policy, confidence, identity)

        base = float(policy.base_threshold)

        regime_score, regime_adj = regime_component(request.regime, policy.regime_bound)
        vol_score, vol_adj = volatility_component(request.volatility, policy.volatility_bound)
        agree_score, agree_adj, agree_breakdown = agreement_component(
            request.experts, policy.agreement_bound
        )
        htf_score, htf_adj = htf_component(request.htf, request.side, policy.htf_bound)
        mq_score, mq_adj = market_quality_component(
            request.market_quality, policy.market_quality_bound
        )

        market_threshold = base + regime_adj + vol_adj + agree_adj + htf_adj + mq_adj

        performance = self.performance.evaluate(
            bot_instance_id=request.bot_instance_id,
            symbol=request.symbol,
            timeframe=request.timeframe,
            min_samples=policy.performance_min_samples,
            lookback=policy.performance_lookback,
            bound=policy.performance_bound,
        )
        distribution = self.distribution.evaluate(
            state.distribution_samples,
            base_threshold=base,
            target_percentile=policy.distribution_target_percentile,
            min_samples=policy.distribution_min_samples,
            bound=policy.distribution_bound,
        )

        calibration_adjustment = performance.adjustment + distribution.adjustment
        raw = market_threshold + calibration_adjustment

        previous = state.previous_threshold
        smoothed, smoothing_applied = self._smooth(raw, previous, policy)
        rate_limited, rate_limit_applied = self._rate_limit(smoothed, previous, policy)
        final = _clamp(rate_limited, policy.min_threshold, policy.max_threshold)
        clamp_applied = abs(final - rate_limited) > 1e-12

        passed = confidence >= final

        return AdaptiveThresholdDecision(
            threshold_decision_id=new_threshold_decision_id(),
            status=ThresholdStatus.EVALUATED,
            threshold_engine_version=self.version,
            threshold_mode=policy.mode,
            opportunity_confidence=round(confidence, 6),
            base_threshold=round(base, 6),
            regime=request.regime.regime,
            regime_adjustment=regime_adj,
            volatility_score=vol_score,
            volatility_adjustment=vol_adj,
            expert_agreement_score=agree_score,
            agreement_adjustment=agree_adj,
            htf_alignment_score=htf_score,
            htf_adjustment=htf_adj,
            market_quality_score=mq_score,
            market_quality_adjustment=mq_adj,
            performance_score=performance.score,
            performance_adjustment=performance.adjustment,
            performance_sample_size=performance.sample_size,
            performance_status=performance.status,
            distribution_percentile=distribution.score,
            distribution_adjustment=distribution.adjustment,
            distribution_sample_size=distribution.sample_size,
            distribution_status=distribution.status,
            market_threshold=round(market_threshold, 6),
            calibration_adjustment=round(calibration_adjustment, 6),
            raw_unclamped_threshold=round(raw, 6),
            smoothed_threshold=round(smoothed, 6),
            rate_limited_threshold=round(rate_limited, 6),
            final_threshold=round(final, 6),
            min_threshold=policy.min_threshold,
            max_threshold=policy.max_threshold,
            previous_threshold=previous,
            smoothing_applied=smoothing_applied,
            rate_limit_applied=rate_limit_applied,
            clamp_applied=clamp_applied,
            passed=passed,
            reason=REASON_APPROVED if passed else REASON_BELOW,
            detail=str(agree_breakdown) if agree_breakdown else "",
            expert_evidence=tuple(request.experts),
            decided_at=request.evaluated_at,
            **identity,
        )

    def _static_decision(
        self,
        request: AdaptiveThresholdInput,
        policy: EffectiveThresholdPolicy,
        confidence: float,
        identity: dict[str, Any],
    ) -> AdaptiveThresholdDecision:
        static = float(policy.static_threshold)
        passed = confidence >= static
        return AdaptiveThresholdDecision(
            threshold_decision_id=new_threshold_decision_id(),
            status=ThresholdStatus.EVALUATED,
            threshold_engine_version=self.version,
            threshold_mode=policy.mode,
            opportunity_confidence=round(confidence, 6),
            base_threshold=round(static, 6),
            regime=request.regime.regime,
            market_threshold=round(static, 6),
            raw_unclamped_threshold=round(static, 6),
            smoothed_threshold=round(static, 6),
            rate_limited_threshold=round(static, 6),
            final_threshold=round(static, 6),
            min_threshold=policy.min_threshold,
            max_threshold=policy.max_threshold,
            performance_status=CalibrationStatus.DISABLED,
            distribution_status=CalibrationStatus.DISABLED,
            passed=passed,
            reason=REASON_APPROVED if passed else REASON_BELOW,
            detail="STATIC mode: no adaptation is applied",
            expert_evidence=tuple(request.experts),
            decided_at=request.evaluated_at,
            **identity,
        )

    @staticmethod
    def _smooth(
        proposed: float, previous: float | None, policy: EffectiveThresholdPolicy
    ) -> tuple[float, bool]:
        if previous is None:
            return (float(proposed), False)
        alpha = float(policy.smoothing_alpha)
        smoothed = alpha * float(proposed) + (1.0 - alpha) * float(previous)
        return (smoothed, abs(smoothed - float(proposed)) > 1e-12)

    @staticmethod
    def _rate_limit(
        proposed: float, previous: float | None, policy: EffectiveThresholdPolicy
    ) -> tuple[float, bool]:
        if previous is None:
            return (float(proposed), False)
        previous = float(previous)
        limited = _clamp(
            float(proposed),
            previous - abs(float(policy.max_step_down)),
            previous + abs(float(policy.max_step_up)),
        )
        return (limited, abs(limited - float(proposed)) > 1e-12)

    def _persist(
        self,
        request: AdaptiveThresholdInput,
        policy: EffectiveThresholdPolicy,
        state: ThresholdState,
        decision: AdaptiveThresholdDecision,
    ) -> None:
        """Carry the smoothing anchor and the distribution sample forward.

        Only opportunities that actually reached the quality stage contribute a
        sample, and a candle already recorded contributes nothing -- so a
        re-evaluation of the same candle after a restart cannot double-count.
        """
        if not decision.evaluated:
            return
        if (
            state.last_candle_time is not None
            and request.closed_candle_time is not None
            and int(state.last_candle_time) == int(request.closed_candle_time)
        ):
            updated = state.copy()
        else:
            updated = state.with_sample(
                float(request.opportunity_confidence),
                window=policy.distribution_window,
            )
        updated.previous_threshold = decision.final_threshold
        updated.last_candle_time = request.closed_candle_time
        updated.updated_at = request.evaluated_at
        updated.engine_version = self.version
        updated.policy_hash = policy.policy_hash
        self.state_store.put(updated)


__all__ = [
    "AdaptiveEntryThresholdEngine",
    "ENGINE_VERSION",
    "REASON_APPROVED",
    "REASON_BELOW",
    "REASON_MARKET_DATA_STALE",
    "REASON_NO_OPPORTUNITY",
    "REASON_NON_FINITE",
    "REASON_REGIME_HARD_BLOCK",
    "REGIME_FACTORS",
    "agreement_component",
    "htf_component",
    "market_quality_component",
    "regime_component",
    "volatility_component",
]
