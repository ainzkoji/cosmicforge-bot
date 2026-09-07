"""Canonical entry-decision layer.

    MarketSnapshot
        ↓
    Master Ensemble        — market interpretation, evidence aggregation
        ↓
    TradingOpportunity     — "here is a directional candidate and why"
        ↓
    TradingDecisionEngine  — the ONE entry-quality authority
        ↓
    EntryQualityDecision   — approved / rejected, with a structured reason
        ↓
    Risk → Execution feasibility → EntryProtection → Executor

Master Ensemble no longer decides whether a trade is good enough; it decides
what the market is doing. Whether that clears the bar belongs here, and only
here.
"""
from app.decision.decision_engine import EntryQualityDecision, TradingDecisionEngine
from app.decision.opportunity import NoOpportunity, TradingOpportunity
from app.decision.reasons import (
    CycleReason,
    ExecutionReason,
    LifecycleReason,
    ProtectionReason,
    QualityReason,
    RiskReason,
)

__all__ = [
    "TradingOpportunity",
    "NoOpportunity",
    "TradingDecisionEngine",
    "EntryQualityDecision",
    "CycleReason",
    "QualityReason",
    "RiskReason",
    "ExecutionReason",
    "ProtectionReason",
    "LifecycleReason",
]
