"""``TradingDecisionEngine`` — the single entry-quality authority.

Before this module the active path could compare confidence against a threshold
in up to four places: Master Ensemble's own threshold step, the runner's
adaptive/dynamic gate, SafetyEngine's ``check_pre_trade``, and PolicyEngine.
The same opportunity could be approved and then rejected again for the same
underlying reason, with different numbers and different messages.

Now there is exactly one comparison, and it lives in
:meth:`TradingDecisionEngine.evaluate`:

    opportunity.raw_confidence  >=  effective_entry_threshold

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

#: The engine never resolves a threshold outside this band, whatever the
#: adaptive/dynamic inputs suggest. A threshold of 0 would approve everything.
ABSOLUTE_THRESHOLD_FLOOR = 0.0
ABSOLUTE_THRESHOLD_CEILING = 1.0


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

    effective_entry_threshold: float = 0.0
    threshold_source: str = "unresolved"
    raw_confidence: float = 0.0
    effective_confidence: float = 0.0

    consensus_required: float = 0.0
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
            "raw_confidence": round(float(self.raw_confidence), 4),
            "effective_confidence": round(float(self.effective_confidence), 4),
            "effective_entry_threshold": round(float(self.effective_entry_threshold), 4),
            "threshold_source": self.threshold_source,
            "consensus_required": round(float(self.consensus_required), 4),
            "consensus_observed": round(float(self.consensus_observed), 4),
            "regime": self.regime,
            "modifiers": dict(self.modifiers),
        }


class TradingDecisionEngine:
    """Resolves the entry threshold and performs the one quality comparison."""

    def __init__(
        self,
        *,
        threshold_floor: float = 0.0,
        threshold_ceiling: float = 1.0,
        consensus_threshold: float = 0.0,
    ) -> None:
        self.threshold_floor = float(threshold_floor)
        self.threshold_ceiling = float(threshold_ceiling)
        self.consensus_threshold = float(consensus_threshold)

    # ── Threshold resolution ────────────────────────────────────────────────

    def resolve_threshold(
        self,
        *,
        base_threshold: float,
        policy_floor: float | None = None,
        adaptive_gate: float | None = None,
        regime: str = "UNKNOWN",
    ) -> tuple[float, str, dict[str, Any]]:
        """Resolve the effective entry threshold from all contributing inputs.

        Precedence mirrors the behaviour that was previously spread across
        Master Ensemble and the runner's adaptive gate: the adaptive gate, when
        supplied, replaces the base dynamic threshold; the configured floor then
        raises it; nothing may leave the absolute band.

        Returns ``(threshold, source_label, inputs)``.
        """
        inputs: dict[str, Any] = {
            "base_threshold": float(base_threshold),
            "policy_floor": None if policy_floor is None else float(policy_floor),
            "adaptive_gate": None if adaptive_gate is None else float(adaptive_gate),
            "regime": regime,
        }

        if adaptive_gate is not None:
            threshold = float(adaptive_gate)
            source = "adaptive_engine"
        else:
            threshold = float(base_threshold)
            source = "dynamic_threshold"

        floor = max(self.threshold_floor, float(policy_floor or 0.0))
        if threshold < floor:
            threshold = floor
            source = f"{source}+floor"

        ceiling = min(self.threshold_ceiling, ABSOLUTE_THRESHOLD_CEILING)
        if threshold > ceiling:
            threshold = ceiling
            source = f"{source}+ceiling"

        threshold = max(ABSOLUTE_THRESHOLD_FLOOR, threshold)
        inputs["resolved"] = threshold
        return threshold, source, inputs

    # ── The single entry-quality comparison ─────────────────────────────────

    def evaluate(
        self,
        opportunity: TradingOpportunity | NoOpportunity,
        *,
        base_threshold: float = 0.0,
        policy_floor: float | None = None,
        adaptive_gate: float | None = None,
        consensus_required: float | None = None,
        modifiers: Mapping[str, Any] | None = None,
        secondary_reasons: tuple[str, ...] = (),
    ) -> EntryQualityDecision:
        """Return the one entry-quality verdict for this opportunity.

        A ``NoOpportunity`` is passed straight through with its own reason code:
        there is nothing to compare, and calling it a confidence failure would
        be a lie.
        """
        # No candidate: report why, and never invent a confidence verdict.
        if not opportunity.is_opportunity:
            return EntryQualityDecision(
                approved=False,
                primary_reason=opportunity.reason_code,
                market_snapshot_id=opportunity.market_snapshot_id,
                symbol=opportunity.symbol,
                timeframe=opportunity.timeframe,
                raw_confidence=0.0,
                effective_confidence=0.0,
                consensus_observed=float(getattr(opportunity, "consensus", 0.0) or 0.0),
                consensus_required=float(
                    consensus_required if consensus_required is not None else self.consensus_threshold
                ),
                regime=getattr(opportunity, "regime", "UNKNOWN"),
                secondary_reasons=secondary_reasons,
            )

        threshold, source, inputs = self.resolve_threshold(
            base_threshold=base_threshold,
            policy_floor=policy_floor,
            adaptive_gate=adaptive_gate,
            regime=opportunity.regime,
        )

        required_consensus = float(
            consensus_required if consensus_required is not None else self.consensus_threshold
        )
        observed_consensus = float(opportunity.consensus)
        raw_confidence = float(opportunity.raw_confidence)

        base = dict(
            opportunity_id=opportunity.opportunity_id,
            market_snapshot_id=opportunity.market_snapshot_id,
            symbol=opportunity.symbol,
            timeframe=opportunity.timeframe,
            side=opportunity.side,
            effective_entry_threshold=threshold,
            threshold_source=source,
            raw_confidence=raw_confidence,
            effective_confidence=raw_confidence,
            consensus_required=required_consensus,
            consensus_observed=observed_consensus,
            regime=opportunity.regime,
            modifiers=dict(modifiers or {}),
            threshold_inputs=inputs,
            secondary_reasons=secondary_reasons,
        )

        # Consensus is a separate question from confidence. Strategies failing to
        # agree is not the same as agreeing on a weak setup, so it gets its own
        # reason code and is checked first.
        if required_consensus > 0 and observed_consensus < required_consensus:
            return EntryQualityDecision(
                approved=False,
                primary_reason=QualityReason.CONSENSUS_INSUFFICIENT,
                **base,
            )

        # ── THE single top-level entry-quality comparison ────────────────────
        approved = raw_confidence >= threshold

        return EntryQualityDecision(
            approved=approved,
            primary_reason=(
                QualityReason.APPROVED_FOR_EXECUTION
                if approved
                else QualityReason.ENTRY_CONFIDENCE_BELOW_THRESHOLD
            ),
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
