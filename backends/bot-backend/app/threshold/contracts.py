"""Immutable input/output contracts for :mod:`app.threshold`.

The rebuild exists because the old stack had no contract at all: a threshold was
a bare float that four different components felt entitled to raise. Here the
threshold is a *decision* -- a value plus every input that produced it, plus the
arithmetic to prove the two agree.

Two rules carry most of the weight:

* ``final_threshold`` is ``None``, never ``0.0``, when no threshold was
  evaluated. ``0.0 >= 0.0`` approving everything is the failure mode this
  replaces.
* :meth:`AdaptiveThresholdDecision.reconcile` must hold for every EVALUATED
  decision. If the components do not sum to the value, the decision is a lie and
  the caller is entitled to reject it.
"""
from __future__ import annotations

import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Mapping, Sequence

#: Tolerance for the reconciliation identity. Every stored component is rounded
#: to 6 decimal places, so accumulated float error stays far below this.
RECONCILE_TOLERANCE = 1e-6


class ThresholdMode:
    """How the engine resolves a threshold.

    Not an ``Enum``: these values are persisted as text and compared against
    configuration strings.
    """

    STATIC = "STATIC"
    ADAPTIVE = "ADAPTIVE"
    RESEARCH = "RESEARCH"
    MODEL = "MODEL"

    #: MODEL is reserved for validated expected-value calibration and is not
    #: implemented. Selecting it is a configuration error, not a silent
    #: fallback -- a fallback would hide that the operator asked for something
    #: this build cannot do.
    IMPLEMENTED = frozenset({STATIC, ADAPTIVE, RESEARCH})
    ALL = frozenset({STATIC, ADAPTIVE, RESEARCH, MODEL})


class ThresholdStatus:
    """Why a decision does or does not carry a number."""

    EVALUATED = "EVALUATED"
    #: No opportunity reached the quality stage. Nothing to be confident about.
    NOT_EVALUATED = "NOT_EVALUATED"
    #: A hard gate (regime policy, stale data, kill switch) stopped the candle
    #: before quality. Recorded as a block, never as an unreachable threshold.
    HARD_BLOCKED = "HARD_BLOCKED"
    #: The engine ran but a required input was missing or non-finite.
    INSUFFICIENT_DATA = "INSUFFICIENT_DATA"
    ERROR = "ERROR"

    ALL = frozenset({EVALUATED, NOT_EVALUATED, HARD_BLOCKED, INSUFFICIENT_DATA, ERROR})
    #: Statuses that must carry ``final_threshold is None``.
    WITHOUT_THRESHOLD = frozenset({NOT_EVALUATED, HARD_BLOCKED, INSUFFICIENT_DATA, ERROR})


class CalibrationStatus:
    """Why a slow-calibration term has the value it has.

    ``INSUFFICIENT_SAMPLE`` means the data has not accumulated yet.
    ``UNAVAILABLE`` means the source could not be read (a query or database
    failure). ``ERROR`` means the data was read but could not be scored. The
    last two are broken subsystems and must never be reported as the first.
    Every non-OK status leaves the adjustment at exactly 0.0.
    """

    OK = "OK"
    INSUFFICIENT_SAMPLE = "INSUFFICIENT_SAMPLE"
    DISABLED = "DISABLED"
    UNAVAILABLE = "UNAVAILABLE"
    ERROR = "ERROR"


def new_threshold_decision_id() -> str:
    return f"thr_{uuid.uuid4().hex[:16]}"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


# -- Expert evidence ---------------------------------------------------------


@dataclass(frozen=True)
class ExpertEvidence:
    """One expert's contribution to one evaluation.

    ``eligible`` and ``executed`` are separate on purpose. A strategy the regime
    deactivated did not vote HOLD -- it did not vote at all, and counting it as
    a HOLD would understate agreement and silently raise the threshold.
    """

    strategy: str
    eligible: bool
    executed: bool
    signal: str  # BUY | SELL | HOLD | NOT_RUN | DISABLED | ERROR
    confidence: float = 0.0
    raw_score: float = 0.0
    weight: float = 0.0
    weighted_contribution: float = 0.0
    reason: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(self, "signal", str(self.signal).upper())

    @property
    def is_directional(self) -> bool:
        return self.executed and self.signal in {"BUY", "SELL"} and self.confidence > 0

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class RegimeContext:
    regime: str = "UNKNOWN"
    regime_confidence: float = 0.0
    transition: bool = False
    duration_candles: int = 0


@dataclass(frozen=True)
class VolatilityContext:
    """Normalised volatility.

    Percentiles, never raw price-scale values -- a threshold engine that reads
    an absolute ATR cannot be shared between BTC and an equity future.
    """

    atr_percentile: float | None = None
    realized_vol_percentile: float | None = None
    range_percentile: float | None = None
    expansion: float | None = None
    compression: float | None = None
    acceleration: float | None = None
    gap_detected: bool = False

    @property
    def available(self) -> bool:
        return any(
            v is not None
            for v in (self.atr_percentile, self.realized_vol_percentile, self.range_percentile)
        )


@dataclass(frozen=True)
class HTFContext:
    """Higher-timeframe context from *closed* candles only."""

    timeframe: str | None = None
    direction: str | None = None  # BUY | SELL | NEUTRAL
    strength: float = 0.0
    candle_close_time: int | None = None
    is_fresh: bool = True
    aligned: bool | None = None
    opposed: bool | None = None

    @property
    def available(self) -> bool:
        return self.direction is not None and self.is_fresh


@dataclass(frozen=True)
class MarketQualityContext:
    spread_percentile: float | None = None
    estimated_slippage_bps: float | None = None
    liquidity_score: float | None = None
    volume_percentile: float | None = None
    price_discontinuity: bool = False
    data_age_seconds: float | None = None
    data_stale: bool = False

    @property
    def available(self) -> bool:
        return any(
            v is not None
            for v in (
                self.spread_percentile,
                self.estimated_slippage_bps,
                self.liquidity_score,
                self.volume_percentile,
            )
        )


# -- Engine input ------------------------------------------------------------


@dataclass(frozen=True)
class AdaptiveThresholdInput:
    """Everything the engine is allowed to read.

    The engine takes no other input: no clock, no network, no module-level
    state. That is what makes the determinism requirement testable rather than
    aspirational.
    """

    # Identity
    bot_instance_id: str
    symbol: str
    timeframe: str
    strategy_version: str = "unknown"
    venue: str = "unknown"
    market_type: str = "UNKNOWN"
    run_id: str | None = None
    cycle_id: str | None = None
    market_snapshot_id: str | None = None
    opportunity_id: str | None = None
    closed_candle_time: int | None = None
    policy_hash: str | None = None

    # Opportunity evidence
    side: str | None = None
    opportunity_confidence: float | None = None
    buy_score: float = 0.0
    sell_score: float = 0.0
    consensus: float = 0.0

    # Expert evidence
    experts: tuple[ExpertEvidence, ...] = ()

    # Market context
    regime: RegimeContext = field(default_factory=RegimeContext)
    volatility: VolatilityContext = field(default_factory=VolatilityContext)
    htf: HTFContext = field(default_factory=HTFContext)
    market_quality: MarketQualityContext = field(default_factory=MarketQualityContext)

    #: Injected -- never ``datetime.now()`` inside the engine.
    evaluated_at: str = field(default_factory=_now)

    def state_key(self) -> tuple[str, str, str, str]:
        """The adaptive-state partition. BTC must not move ETH."""
        return (
            str(self.bot_instance_id),
            str(self.symbol).upper(),
            str(self.timeframe),
            str(self.strategy_version),
        )


# -- Engine output -----------------------------------------------------------


@dataclass(frozen=True)
class AdaptiveThresholdDecision:
    """The one threshold verdict for one opportunity on one candle."""

    # Identity
    threshold_decision_id: str
    bot_instance_id: str
    symbol: str
    timeframe: str
    status: str
    threshold_engine_version: str
    threshold_mode: str

    run_id: str | None = None
    cycle_id: str | None = None
    opportunity_id: str | None = None
    market_snapshot_id: str | None = None
    venue: str | None = None
    market_type: str | None = None
    closed_candle_time: int | None = None
    policy_hash: str | None = None

    # Values. ``None`` means "not evaluated", never 0.0.
    opportunity_confidence: float | None = None
    base_threshold: float | None = None

    # Fast market context
    regime: str | None = None
    regime_adjustment: float = 0.0
    volatility_score: float | None = None
    volatility_adjustment: float = 0.0
    expert_agreement_score: float | None = None
    agreement_adjustment: float = 0.0
    htf_alignment_score: float | None = None
    htf_adjustment: float = 0.0
    market_quality_score: float | None = None
    market_quality_adjustment: float = 0.0

    # Slow calibration
    performance_score: float | None = None
    performance_adjustment: float = 0.0
    performance_sample_size: int = 0
    performance_status: str = CalibrationStatus.INSUFFICIENT_SAMPLE
    distribution_percentile: float | None = None
    distribution_adjustment: float = 0.0
    distribution_sample_size: int = 0
    distribution_status: str = CalibrationStatus.INSUFFICIENT_SAMPLE

    # Computation trail
    market_threshold: float | None = None
    calibration_adjustment: float = 0.0
    raw_unclamped_threshold: float | None = None
    smoothed_threshold: float | None = None
    rate_limited_threshold: float | None = None
    final_threshold: float | None = None

    # Bounds and history
    min_threshold: float | None = None
    max_threshold: float | None = None
    previous_threshold: float | None = None
    smoothing_applied: bool = False
    rate_limit_applied: bool = False
    clamp_applied: bool = False

    # Verdict
    passed: bool | None = None
    reason: str = ""
    detail: str = ""
    #: Set on the first decision of a new calibration epoch: the stored adaptive
    #: state came from another engine version or policy and was discarded, not
    #: carried into this one.
    state_reset_reason: str | None = None
    expert_evidence: tuple[ExpertEvidence, ...] = ()
    decided_at: str = field(default_factory=_now)

    @property
    def evaluated(self) -> bool:
        return self.status == ThresholdStatus.EVALUATED

    def __post_init__(self) -> None:
        if self.status not in ThresholdStatus.ALL:
            raise ValueError(f"unknown threshold status {self.status!r}")
        if self.status in ThresholdStatus.WITHOUT_THRESHOLD and self.final_threshold is not None:
            raise ValueError(
                f"status {self.status} must carry final_threshold=None, "
                f"got {self.final_threshold!r}"
            )
        if self.status == ThresholdStatus.EVALUATED and self.final_threshold is None:
            raise ValueError("EVALUATED decision must carry a final_threshold")

    # -- The components must reconcile with the value --------------------

    def component_sum(self) -> float | None:
        if not self.evaluated or self.base_threshold is None:
            return None
        return (
            float(self.base_threshold)
            + float(self.regime_adjustment)
            + float(self.volatility_adjustment)
            + float(self.agreement_adjustment)
            + float(self.htf_adjustment)
            + float(self.market_quality_adjustment)
            + float(self.performance_adjustment)
            + float(self.distribution_adjustment)
        )

    def reconcile(self) -> bool:
        """True when the recorded components produce the recorded raw value.

        Deliberately checks ``raw_unclamped_threshold`` and not
        ``final_threshold``: smoothing, rate limiting and clamping are allowed
        to move the value, and each records its own flag saying it did.
        """
        if not self.evaluated:
            return True
        total = self.component_sum()
        if total is None or self.raw_unclamped_threshold is None:
            return False
        return abs(total - float(self.raw_unclamped_threshold)) <= RECONCILE_TOLERANCE

    def reconciliation_error(self) -> float | None:
        total = self.component_sum()
        if total is None or self.raw_unclamped_threshold is None:
            return None
        return abs(total - float(self.raw_unclamped_threshold))

    def in_bounds(self) -> bool:
        if not self.evaluated:
            return True
        if self.min_threshold is None or self.max_threshold is None:
            return False
        return float(self.min_threshold) <= float(self.final_threshold) <= float(self.max_threshold)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["expert_evidence"] = [e.to_dict() for e in self.expert_evidence]
        return payload

    def observability(self) -> dict[str, Any]:
        """Operator view: the number, and every term that made it."""
        return {
            "threshold_decision_id": self.threshold_decision_id,
            "status": self.status,
            "mode": self.threshold_mode,
            "engine_version": self.threshold_engine_version,
            "policy_hash": self.policy_hash,
            "symbol": self.symbol,
            "timeframe": self.timeframe,
            "regime": self.regime,
            "opportunity_confidence": self.opportunity_confidence,
            "base_threshold": self.base_threshold,
            "adjustments": {
                "regime": self.regime_adjustment,
                "volatility": self.volatility_adjustment,
                "agreement": self.agreement_adjustment,
                "htf": self.htf_adjustment,
                "market_quality": self.market_quality_adjustment,
                "performance": self.performance_adjustment,
                "distribution": self.distribution_adjustment,
            },
            "market_threshold": self.market_threshold,
            "calibration_adjustment": self.calibration_adjustment,
            "raw_unclamped_threshold": self.raw_unclamped_threshold,
            "smoothed_threshold": self.smoothed_threshold,
            "rate_limited_threshold": self.rate_limited_threshold,
            "final_threshold": self.final_threshold,
            "bounds": [self.min_threshold, self.max_threshold],
            "previous_threshold": self.previous_threshold,
            "smoothing_applied": self.smoothing_applied,
            "rate_limit_applied": self.rate_limit_applied,
            "clamp_applied": self.clamp_applied,
            "passed": self.passed,
            "reason": self.reason,
            "state_reset_reason": self.state_reset_reason,
            "reconciles": self.reconcile(),
        }


def not_evaluated(
    *,
    bot_instance_id: str,
    symbol: str,
    timeframe: str,
    engine_version: str,
    mode: str,
    reason: str,
    status: str = ThresholdStatus.NOT_EVALUATED,
    detail: str = "",
    regime: str | None = None,
    policy_hash: str | None = None,
    min_threshold: float | None = None,
    max_threshold: float | None = None,
    experts: Sequence[ExpertEvidence] = (),
    **identity: Any,
) -> AdaptiveThresholdDecision:
    """Build a decision that deliberately carries no number.

    Used for NO_OPPORTUNITY and for hard-blocked regimes. The absence of a
    threshold is recorded as an absence, not as a zero.
    """
    if status not in ThresholdStatus.WITHOUT_THRESHOLD:
        raise ValueError(f"{status} carries a threshold; use the engine instead")
    return AdaptiveThresholdDecision(
        threshold_decision_id=new_threshold_decision_id(),
        bot_instance_id=str(bot_instance_id),
        symbol=str(symbol).upper(),
        timeframe=str(timeframe),
        status=status,
        threshold_engine_version=engine_version,
        threshold_mode=mode,
        regime=regime,
        policy_hash=policy_hash,
        min_threshold=min_threshold,
        max_threshold=max_threshold,
        reason=reason,
        detail=detail,
        passed=None,
        expert_evidence=tuple(experts),
        **{k: v for k, v in identity.items() if v is not None},
    )


def experts_from_votes(
    votes: Sequence[tuple[str, Any, float]],
    *,
    eligible: Sequence[str],
    all_strategies: Sequence[str],
    weights: Mapping[str, float] | None = None,
    reasons: Mapping[str, str] | None = None,
    scores: Mapping[str, float] | None = None,
    errors: Mapping[str, str] | None = None,
) -> tuple[ExpertEvidence, ...]:
    """Build expert evidence from the ensemble's own vote list.

    The strategies are not re-executed. Every field here comes from the single
    production evaluation that already happened.

    ``errors`` names experts that ran and failed (an exception, or data they
    needed was unavailable), with the failure reason. They are recorded as
    ``ERROR`` -- never HOLD, never NOT_RUN -- with their eligibility kept, so a
    system failure cannot pass for a neutral opinion.
    """
    weights = weights or {}
    reasons = reasons or {}
    scores = scores or {}
    errors = {str(k): str(v) for k, v in (errors or {}).items()}
    eligible_set = {str(n) for n in eligible}
    executed: dict[str, tuple[str, float]] = {}
    for name, signal, conf in votes:
        sig = getattr(signal, "value", signal)
        executed[str(name)] = (str(sig).upper(), float(conf))

    out: list[ExpertEvidence] = []
    for name in sorted({*all_strategies, *eligible_set, *executed, *errors}):
        weight = float(weights.get(name, 0.0))
        if name in errors:
            out.append(
                ExpertEvidence(
                    strategy=name,
                    eligible=name in eligible_set,
                    executed=True,
                    signal="ERROR",
                    confidence=0.0,
                    raw_score=0.0,
                    weight=weight,
                    weighted_contribution=0.0,
                    # One line, bounded: a traceback in a reason column is not
                    # evidence, it is noise that breaks every consumer.
                    reason=" ".join(str(errors[name] or "error").split())[:500],
                )
            )
            continue
        if name in executed:
            sig, conf = executed[name]
            out.append(
                ExpertEvidence(
                    strategy=name,
                    eligible=name in eligible_set,
                    executed=True,
                    signal=sig,
                    confidence=conf,
                    raw_score=float(scores.get(name, conf)),
                    weight=weight,
                    weighted_contribution=(weight * conf if sig in {"BUY", "SELL"} else 0.0),
                    reason=str(reasons.get(name, "")),
                )
            )
        else:
            out.append(
                ExpertEvidence(
                    strategy=name,
                    eligible=name in eligible_set,
                    executed=False,
                    # NOT_RUN, never HOLD. See ExpertEvidence.
                    signal="NOT_RUN" if name in eligible_set else "DISABLED",
                    confidence=0.0,
                    raw_score=0.0,
                    weight=weight,
                    weighted_contribution=0.0,
                    reason=str(reasons.get(name, "")),
                )
            )
    return tuple(out)
