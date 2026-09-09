"""``TradingDecisionEngine`` — the single entry-quality authority.

Before this module the active path could compare confidence against a threshold
in up to four places: Master Ensemble's own threshold step, the runner's
adaptive/dynamic gate, SafetyEngine's ``check_pre_trade``, and PolicyEngine.
The same opportunity could be approved and then rejected again for the same
underlying reason, with different numbers and different messages.

Now there is exactly one comparison, and it lives in
:meth:`TradingDecisionEngine.evaluate`:

    opportunity.raw_confidence  >=  threshold_decision.final_threshold

**This engine no longer resolves a threshold.** It used to, via a
``resolve_threshold`` that took a base, a policy floor and an adaptive gate and
applied ``max()`` to them — which made it one of the several authorities the
0.70 saturation was built from. Threshold computation now belongs entirely to
:class:`~app.threshold.engine.AdaptiveEntryThresholdEngine`, and this engine
consumes its :class:`~app.threshold.contracts.AdaptiveThresholdDecision`.

The consensus gate is gone with it. Expert agreement is one bounded input to the
threshold; gating on it separately would be two authorities answering one
question. ``consensus_observed`` survives as evidence, ``consensus_required``
does not.

Everything downstream — SafetyEngine, PolicyEngine, risk, execution — keeps its
*hard vetoes* but must not re-litigate entry quality. They are told the quality
verdict via ``confidence_already_approved=True``.

This engine deliberately contains no position sizing (that is the risk layer's
job) and no duplicate/idempotency logic (that stays in EntryProtection).
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Mapping

from app.decision.opportunity import NoOpportunity, TradingOpportunity
from app.decision.reasons import QualityReason
from app.threshold.contracts import AdaptiveThresholdDecision, ThresholdStatus


def _round(value: float | None) -> float | None:
    """Round for display, preserving ``None`` rather than coercing it to 0.0."""
    return None if value is None else round(float(value), 4)


@dataclass(frozen=True)
class EntryQualityDecision:
    """The one entry-quality verdict for one opportunity on one candle."""

    approved: bool
    primary_reason: str
    opportunity_id: str | None = None
    market_snapshot_id: str | None = None
    symbol: str | None = None
    timeframe: str | None = None
    side: str | None = None

    #: ``None`` when no threshold was evaluated. Never 0.0 — a stored zero
    #: turns "we never asked" into "everything passes".
    effective_entry_threshold: float | None = None
    threshold_source: str = "unresolved"
    threshold_decision_id: str | None = None
    threshold_status: str = ThresholdStatus.NOT_EVALUATED
    raw_confidence: float | None = None
    effective_confidence: float | None = None

    #: Evidence only. No consensus requirement is applied anywhere.
    consensus_observed: float = 0.0
    regime: str = "UNKNOWN"

    secondary_reasons: tuple[str, ...] = ()
    modifiers: Mapping[str, Any] = field(default_factory=dict)
    threshold_inputs: Mapping[str, Any] = field(default_factory=dict)
    decided_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def observability(self) -> dict[str, Any]:
        """Phase 7 §7.12 — everything an operator needs, without reading logs."""
        return {
            "approved": self.approved,
            "primary_reason": self.primary_reason,
            "secondary_reasons": list(self.secondary_reasons),
            "opportunity_id": self.opportunity_id,
            "market_snapshot_id": self.market_snapshot_id,
            "symbol": self.symbol,
            "timeframe": self.timeframe,
            "side": self.side,
            "raw_confidence": _round(self.raw_confidence),
            "effective_confidence": _round(self.effective_confidence),
            "effective_entry_threshold": _round(self.effective_entry_threshold),
            "threshold_source": self.threshold_source,
            "threshold_status": self.threshold_status,
            "threshold_decision_id": self.threshold_decision_id,
            "consensus_observed": round(float(self.consensus_observed), 4),
            "regime": self.regime,
            "modifiers": dict(self.modifiers),
        }


class TradingDecisionEngine:
    """Performs the one quality comparison. It does not compute a threshold."""

    def __init__(self) -> None:
        # Deliberately stateless and parameterless. Every constructor argument
        # this class used to take (threshold_floor, threshold_ceiling,
        # consensus_threshold) was a threshold authority, and there is now
        # exactly one of those: AdaptiveEntryThresholdEngine.
        pass

    # ── The single entry-quality comparison ─────────────────────────────────

    def evaluate(
        self,
        opportunity: TradingOpportunity | NoOpportunity,
        *,
        threshold_decision: AdaptiveThresholdDecision | None = None,
        modifiers: Mapping[str, Any] | None = None,
        secondary_reasons: tuple[str, ...] = (),
    ) -> EntryQualityDecision:
        """Return the one entry-quality verdict for this opportunity.

        A ``NoOpportunity`` is passed straight through with its own reason code:
        there is nothing to compare, and calling it a confidence failure would
        be a lie. So is an opportunity whose threshold was never evaluated —
        comparing against a substituted zero is how ``0.0 >= 0.0`` came to
        approve everything.
        """
        # No candidate: report why, and never invent a confidence verdict.
        if not opportunity.is_opportunity:
            return EntryQualityDecision(
                approved=False,
                primary_reason=opportunity.reason_code,
                market_snapshot_id=opportunity.market_snapshot_id,
                symbol=opportunity.symbol,
                timeframe=opportunity.timeframe,
                raw_confidence=None,
                effective_confidence=None,
                effective_entry_threshold=None,
                threshold_status=(
                    threshold_decision.status
                    if threshold_decision is not None
                    else ThresholdStatus.NOT_EVALUATED
                ),
                threshold_decision_id=(
                    threshold_decision.threshold_decision_id
                    if threshold_decision is not None
                    else None
                ),
                consensus_observed=float(getattr(opportunity, "consensus", 0.0) or 0.0),
                regime=getattr(opportunity, "regime", "UNKNOWN"),
                secondary_reasons=secondary_reasons,
            )

        raw_confidence = float(opportunity.raw_confidence)
        base = dict(
            opportunity_id=opportunity.opportunity_id,
            market_snapshot_id=opportunity.market_snapshot_id,
            symbol=opportunity.symbol,
            timeframe=opportunity.timeframe,
            side=opportunity.side,
            raw_confidence=raw_confidence,
            effective_confidence=raw_confidence,
            consensus_observed=float(opportunity.consensus),
            regime=opportunity.regime,
            modifiers=dict(modifiers or {}),
            secondary_reasons=secondary_reasons,
        )

        # No threshold was resolved. That is a real outcome — a hard-blocked
        # regime, stale data, or a threshold engine that could not run — and it
        # is reported as itself rather than as a confidence failure.
        if threshold_decision is None or not threshold_decision.evaluated:
            status = (
                threshold_decision.status
                if threshold_decision is not None
                else ThresholdStatus.NOT_EVALUATED
            )
            return EntryQualityDecision(
                approved=False,
                primary_reason=(
                    threshold_decision.reason
                    if threshold_decision is not None and threshold_decision.reason
                    else QualityReason.NO_OPPORTUNITY
                ),
                effective_entry_threshold=None,
                threshold_source="not_evaluated",
                threshold_status=status,
                threshold_decision_id=(
                    threshold_decision.threshold_decision_id
                    if threshold_decision is not None
                    else None
                ),
                threshold_inputs={},
                **base,
            )

        threshold = float(threshold_decision.final_threshold)

        # ── THE single top-level entry-quality comparison ────────────────────
        approved = raw_confidence >= threshold

        return EntryQualityDecision(
            approved=approved,
            primary_reason=(
                QualityReason.APPROVED_FOR_EXECUTION
                if approved
                else QualityReason.ENTRY_CONFIDENCE_BELOW_THRESHOLD
            ),
            effective_entry_threshold=threshold,
            threshold_source=(
                f"adaptive_entry_threshold_engine/{threshold_decision.threshold_engine_version}"
            ),
            threshold_status=threshold_decision.status,
            threshold_decision_id=threshold_decision.threshold_decision_id,
            threshold_inputs=threshold_decision.observability(),
            **base,
        )

    # ── Explicit vetoes applied AFTER quality, never re-comparing confidence ─

    @staticmethod
    def veto(decision: EntryQualityDecision, reason_code: str) -> EntryQualityDecision:
        """Attach a hard veto (HTF, regime, session, event) to an approved decision.

        This does not re-run the confidence comparison; it records that a
        non-quality condition blocked an otherwise quality-approved candidate.
        """
        if not decision.approved:
            return decision
        payload = decision.to_dict()
        payload["approved"] = False
        payload["secondary_reasons"] = tuple(decision.secondary_reasons) + (
            decision.primary_reason,
        )
        payload["primary_reason"] = reason_code
        payload.pop("decided_at", None)
        return EntryQualityDecision(**payload)
