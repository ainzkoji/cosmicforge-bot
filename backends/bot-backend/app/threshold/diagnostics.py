"""Threshold diagnostics, including the check that would have caught 0.70.

The previous stack failed silently for months. Every component was healthy, the
dynamic calculator produced values on every candle, the adaptive engine produced
a gate, and the final number was 0.70 every single time -- because a floor
configured above the dynamic hard cap discarded all of it.

:func:`detect_inert_engine` looks for exactly that shape: **the inputs are
varying and the output is not**. It deliberately does not flag a stable
threshold on its own. A quiet market that produces a steady bar is the engine
working, and an alert that fires on correct behaviour is an alert nobody reads.
What it flags is a threshold pinned at a bound while the components underneath
it move.
"""
from __future__ import annotations

import statistics
from dataclasses import dataclass
from typing import Any, Iterable, Sequence

from app.threshold.contracts import AdaptiveThresholdDecision, ThresholdStatus
from app.threshold.state import percentile

# Health reason codes.
THRESHOLD_ENGINE_INITIALIZATION_FAILED = "THRESHOLD_ENGINE_INITIALIZATION_FAILED"
THRESHOLD_POLICY_INVALID = "THRESHOLD_POLICY_INVALID"
THRESHOLD_STATE_INVALID = "THRESHOLD_STATE_INVALID"
THRESHOLD_OUT_OF_BOUNDS = "THRESHOLD_OUT_OF_BOUNDS"
THRESHOLD_NOT_FINITE = "THRESHOLD_NOT_FINITE"
THRESHOLD_COMPONENTS_DO_NOT_RECONCILE = "THRESHOLD_COMPONENTS_DO_NOT_RECONCILE"
THRESHOLD_ZERO_WHERE_NOT_EVALUATED = "THRESHOLD_ZERO_WHERE_NOT_EVALUATED"
ADAPTIVE_THRESHOLD_EFFECTIVELY_STATIC = "ADAPTIVE_THRESHOLD_EFFECTIVELY_STATIC"

HEALTH_REASONS = frozenset(
    {
        THRESHOLD_ENGINE_INITIALIZATION_FAILED,
        THRESHOLD_POLICY_INVALID,
        THRESHOLD_STATE_INVALID,
        THRESHOLD_OUT_OF_BOUNDS,
        THRESHOLD_NOT_FINITE,
        THRESHOLD_COMPONENTS_DO_NOT_RECONCILE,
        THRESHOLD_ZERO_WHERE_NOT_EVALUATED,
        ADAPTIVE_THRESHOLD_EFFECTIVELY_STATIC,
    }
)

#: Below this many EVALUATED decisions, "is it adapting?" has no answer worth
#: reporting. Saying nothing beats guessing.
MIN_SAMPLE_FOR_INERT_CHECK = 20

#: Final thresholds within this distance of each other count as the same value.
STATIC_EPSILON = 1e-4

#: How close to a bound counts as pinned to it.
SATURATION_EPSILON = 1e-3


@dataclass(frozen=True)
class ThresholdStats:
    sample: int
    distinct: int
    minimum: float | None
    p10: float | None
    median: float | None
    p90: float | None
    maximum: float | None
    stdev: float | None
    adjustment_utilization: float | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "sample": self.sample,
            "distinct": self.distinct,
            "min": self.minimum,
            "p10": self.p10,
            "median": self.median,
            "p90": self.p90,
            "max": self.maximum,
            "stdev": self.stdev,
            "adjustment_utilization": self.adjustment_utilization,
        }


def threshold_stats(
    decisions: Iterable[AdaptiveThresholdDecision], *, total_bound: float | None = None
) -> ThresholdStats:
    """Distribution of the final threshold over a set of EVALUATED decisions."""
    evaluated = [d for d in decisions if d.status == ThresholdStatus.EVALUATED]
    values = [float(d.final_threshold) for d in evaluated if d.final_threshold is not None]
    if not values:
        return ThresholdStats(0, 0, None, None, None, None, None, None, None)

    distinct = len({round(v, 6) for v in values})
    utilization = None
    if total_bound:
        magnitudes = [abs(_total_adjustment(d)) for d in evaluated]
        if magnitudes:
            utilization = round(statistics.fmean(magnitudes) / abs(float(total_bound)), 6)

    return ThresholdStats(
        sample=len(values),
        distinct=distinct,
        minimum=round(min(values), 6),
        p10=_round(percentile(values, 0.10)),
        median=_round(percentile(values, 0.50)),
        p90=_round(percentile(values, 0.90)),
        maximum=round(max(values), 6),
        stdev=round(statistics.pstdev(values), 6) if len(values) > 1 else 0.0,
        adjustment_utilization=utilization,
    )


def _total_adjustment(decision: AdaptiveThresholdDecision) -> float:
    return (
        float(decision.regime_adjustment)
        + float(decision.volatility_adjustment)
        + float(decision.agreement_adjustment)
        + float(decision.htf_adjustment)
        + float(decision.market_quality_adjustment)
        + float(decision.performance_adjustment)
        + float(decision.distribution_adjustment)
    )


def _round(value: float | None) -> float | None:
    return None if value is None else round(float(value), 6)


@dataclass(frozen=True)
class InertReport:
    inert: bool
    reason: str | None
    sample: int
    distinct_thresholds: int
    distinct_raw_thresholds: int
    saturated_at: str | None
    detail: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "inert": self.inert,
            "reason": self.reason,
            "sample": self.sample,
            "distinct_thresholds": self.distinct_thresholds,
            "distinct_raw_thresholds": self.distinct_raw_thresholds,
            "saturated_at": self.saturated_at,
            "detail": self.detail,
        }


def detect_inert_engine(
    decisions: Sequence[AdaptiveThresholdDecision],
) -> InertReport:
    """Flag an adaptive engine whose output is pinned while its inputs move.

    The test that matters is the *disagreement* between the raw proposal and the
    final value. If the components produced many different raw thresholds and
    the final threshold is one value sitting on a bound, a configuration
    artifact is eating the adaptation -- which is precisely what
    ``max(dynamic <= 0.65, 0.70)`` did.

    A stable threshold produced by stable components is reported as not inert.
    """
    evaluated = [
        d
        for d in decisions
        if d.status == ThresholdStatus.EVALUATED and d.final_threshold is not None
    ]
    sample = len(evaluated)
    if sample < MIN_SAMPLE_FOR_INERT_CHECK:
        return InertReport(
            inert=False,
            reason=None,
            sample=sample,
            distinct_thresholds=len({round(float(d.final_threshold), 6) for d in evaluated}),
            distinct_raw_thresholds=0,
            saturated_at=None,
            detail=f"sample {sample} < {MIN_SAMPLE_FOR_INERT_CHECK}; not assessed",
        )

    finals = [float(d.final_threshold) for d in evaluated]
    raws = [
        float(d.raw_unclamped_threshold)
        for d in evaluated
        if d.raw_unclamped_threshold is not None
    ]
    distinct_final = len({round(v, 6) for v in finals})
    distinct_raw = len({round(v, 6) for v in raws})

    final_is_static = (max(finals) - min(finals)) <= STATIC_EPSILON
    inputs_vary = distinct_raw > 1 and (max(raws) - min(raws)) > STATIC_EPSILON if raws else False

    if not final_is_static:
        return InertReport(
            inert=False,
            reason=None,
            sample=sample,
            distinct_thresholds=distinct_final,
            distinct_raw_thresholds=distinct_raw,
            saturated_at=None,
            detail="final threshold varies",
        )

    if not inputs_vary:
        # Stable market, stable components, stable threshold. Working.
        return InertReport(
            inert=False,
            reason=None,
            sample=sample,
            distinct_thresholds=distinct_final,
            distinct_raw_thresholds=distinct_raw,
            saturated_at=None,
            detail="threshold is stable because its components are stable",
        )

    pinned = finals[0]
    saturated_at = None
    first = evaluated[0]
    if first.min_threshold is not None and abs(pinned - float(first.min_threshold)) <= SATURATION_EPSILON:
        saturated_at = "min_threshold"
    elif first.max_threshold is not None and abs(pinned - float(first.max_threshold)) <= SATURATION_EPSILON:
        saturated_at = "max_threshold"

    return InertReport(
        inert=True,
        reason=ADAPTIVE_THRESHOLD_EFFECTIVELY_STATIC,
        sample=sample,
        distinct_thresholds=distinct_final,
        distinct_raw_thresholds=distinct_raw,
        saturated_at=saturated_at,
        detail=(
            f"{distinct_raw} distinct raw thresholds collapsed to a single final "
            f"value {pinned}"
            + (f", pinned at {saturated_at}" if saturated_at else "")
        ),
    )


def health_check(
    decisions: Sequence[AdaptiveThresholdDecision],
    *,
    policy: Any = None,
    state_store: Any = None,
) -> list[dict[str, Any]]:
    """Return health findings for a window of threshold decisions."""
    findings: list[dict[str, Any]] = []

    for decision in decisions:
        if decision.status in ThresholdStatus.WITHOUT_THRESHOLD:
            if decision.final_threshold is not None:
                findings.append(
                    _finding(
                        THRESHOLD_ZERO_WHERE_NOT_EVALUATED,
                        decision,
                        f"status {decision.status} carries {decision.final_threshold}",
                    )
                )
            continue
        if decision.final_threshold is None or not _is_finite(decision.final_threshold):
            findings.append(
                _finding(THRESHOLD_NOT_FINITE, decision, f"final={decision.final_threshold!r}")
            )
            continue
        if not decision.in_bounds():
            findings.append(
                _finding(
                    THRESHOLD_OUT_OF_BOUNDS,
                    decision,
                    f"{decision.final_threshold} outside "
                    f"[{decision.min_threshold}, {decision.max_threshold}]",
                )
            )
        if not decision.reconcile():
            findings.append(
                _finding(
                    THRESHOLD_COMPONENTS_DO_NOT_RECONCILE,
                    decision,
                    f"error={decision.reconciliation_error()}",
                )
            )

    inert = detect_inert_engine(decisions)
    if inert.inert:
        findings.append(
            {
                "reason": inert.reason,
                "symbol": decisions[0].symbol if decisions else None,
                "detail": inert.detail,
            }
        )

    if state_store is not None:
        failures = int(getattr(state_store, "write_failures", 0) or 0) + int(
            getattr(state_store, "read_failures", 0) or 0
        )
        if failures:
            findings.append(
                {
                    "reason": THRESHOLD_STATE_INVALID,
                    "symbol": None,
                    "detail": f"{failures} threshold-state persistence failures",
                }
            )

    return findings


def _finding(reason: str, decision: AdaptiveThresholdDecision, detail: str) -> dict[str, Any]:
    return {
        "reason": reason,
        "threshold_decision_id": decision.threshold_decision_id,
        "symbol": decision.symbol,
        "timeframe": decision.timeframe,
        "detail": detail,
    }


def _is_finite(value: Any) -> bool:
    try:
        f = float(value)
    except (TypeError, ValueError):
        return False
    return f == f and f not in (float("inf"), float("-inf"))
