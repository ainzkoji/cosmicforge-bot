"""External observations have no independent new-entry authority.

The compatibility boundary always denies external signals. CATI may observe
advisory data, but only its governed dispatcher can originate new entries.
Historical threshold evidence remains in the database and research reports.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Callable, Sequence

from app.decision.reasons import QualityReason

logger = logging.getLogger(__name__)

#: The regime classifier needs this many closed candles for a stable read.
MIN_REGIME_CANDLES = 100

REASON_CONTRACT = QualityReason.OPPORTUNITY_CONTRACT_UNSATISFIED
REASON_EVIDENCE_UNAVAILABLE = "THRESHOLD_EVIDENCE_UNAVAILABLE"


@dataclass(frozen=True)
class ExternalGateResult:
    """The verdict for one external candidate, and the evidence behind it."""

    passed: bool
    reason: str
    detail: str = ""
    threshold_decision_id: str | None = None
    threshold_status: str | None = None
    final_threshold: float | None = None
    confidence: float | None = None
    opportunity_id: str | None = None
    persisted: bool = False
    threshold_decision: Any = None
    entry_quality: Any = None

    def observability(self) -> dict[str, Any]:
        return {
            "passed": self.passed,
            "reason": self.reason,
            "detail": self.detail,
            "threshold_decision_id": self.threshold_decision_id,
            "threshold_status": self.threshold_status,
            "final_threshold": self.final_threshold,
            "confidence": self.confidence,
            "opportunity_id": self.opportunity_id,
            "persisted": self.persisted,
            "threshold_authority": "AdaptiveEntryThresholdEngine",
        }


def _reject(reason: str, detail: str, **extra: Any) -> ExternalGateResult:
    return ExternalGateResult(passed=False, reason=reason, detail=detail, **extra)


def evaluate_external_candidate(
    *,
    db: Any,
    symbol: str,
    side: str,
    confidence: Any,
    klines: Sequence[Any],
    timeframe: str,
    source: str,
    bot_instance_id: str,
    run_id: str | None = None,
    cycle_id: str | None = None,
    market_type: str | None = None,
    venue: str | None = None,
    provenance: str = "PAPER_FORWARD",
    engine: Any = None,
    policy_resolver: Callable[..., Any] | None = None,
    persist: Callable[..., str] | None = None,
    regime_classifier_factory: Callable[[], Any] | None = None,
) -> ExternalGateResult:
    """External source suggestions have no independent CATI entry authority."""
    return _reject(
        "CATI_ONLY_ENGINE_EXTERNAL_SIGNAL_ADVISORY_ONLY",
        "External signals are advisory observations; only CATI can originate new entries.",
    )



__all__ = [
    "ExternalGateResult",
    "MIN_REGIME_CANDLES",
    "REASON_CONTRACT",
    "REASON_EVIDENCE_UNAVAILABLE",
    "evaluate_external_candidate",
]
