"""Section 15 contracts -- BotCycleEvaluationBatch, RankingPolicy,
RankedOpportunity and the per-symbol evaluation record.

Ranking is analytical only. Nothing here reserves a position slot, capital,
margin or a portfolio slot, and nothing submits an order.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Mapping, Optional, Tuple

from app.trading_intelligence.contracts.economics import CostEstimate, EconomicOpportunity
from app.trading_intelligence.contracts.forecast import OutcomeForecast
from app.trading_intelligence.contracts.instrument import InstrumentKey
from app.trading_intelligence.contracts.setup import SetupCandidate
from app.trading_intelligence.contracts.veto import VetoDecision
from app.trading_intelligence.hashing import short_id, stable_hash
from app.trading_intelligence.versions import (
    BATCH_SCHEMA_VERSION,
    RANKED_OPPORTUNITY_SCHEMA_VERSION,
    RANKING_POLICY_SCHEMA_VERSION,
)


class SymbolEvalKind(str, Enum):
    """Terminal states of one due instrument in a bot cycle (Section 15.2)."""

    EVALUATED = "EVALUATED"  # >=1 candidate reached a veto decision
    NO_CANDIDATES = "NO_CANDIDATES"  # evaluated; zero setup hypotheses (terminal, benign)
    CATI_COMPONENT_ERROR = "CATI_COMPONENT_ERROR"  # explicit terminal failure
    NOT_DUE = "NOT_DUE"  # no new closed candle: excluded from the due set


@dataclass(frozen=True)
class EvaluatedOpportunity:
    """Every piece of evidence behind one opportunity, kept together."""

    candidate: SetupCandidate
    market_state: Any
    regime: Any
    forecast: OutcomeForecast
    cost_estimate: CostEstimate
    opportunity: EconomicOpportunity
    veto: VetoDecision
    #: Section 17 VenueEconomicObservation behind ``cost_estimate`` (None for
    #: the Section 13 reference estimate). Required lineage for a TradePlan.
    venue_observation: Any = None

    @property
    def economics_basis(self) -> str:
        """CANONICAL_VENUE (certifiable) or REFERENCE_DIAGNOSTIC (never plannable)."""
        from app.trading_intelligence.economics.canonical import economics_basis

        return economics_basis(self)

    @property
    def approved(self) -> bool:
        return self.veto.approved_for_ranking


@dataclass(frozen=True)
class SymbolEvaluation:
    instrument: str  # venue symbol, upper case
    kind: str  # SymbolEvalKind
    opportunities: Tuple[EvaluatedOpportunity, ...] = ()
    error: Optional[str] = None
    #: The pinned snapshot's closed candles, carried so the cycle-level
    #: portfolio stage can build causal return histories without refetching.
    candle_rows: Tuple[Any, ...] = ()


BATCH_INCOMPLETE = "BATCH_INCOMPLETE"
BATCH_COMPLETE = "BATCH_COMPLETE"


@dataclass(frozen=True)
class BotCycleEvaluationBatch:
    cycle_batch_id: str

    user_id: Optional[str]
    bot_instance_id: str
    broker_account_id: Optional[str]
    run_id: Optional[str]
    cycle_id: str

    universe_version: str
    universe_hash: str
    decision_time: int

    expected_due_instruments: Tuple[str, ...]
    completed_instruments: Tuple[str, ...]
    failed_instruments: Tuple[str, ...]

    approved_opportunity_ids: Tuple[str, ...]
    watch_ids: Tuple[str, ...]
    rejected_ids: Tuple[str, ...]

    batch_complete: bool
    reason_codes: Tuple[str, ...] = ()
    batch_schema_version: str = BATCH_SCHEMA_VERSION

    @staticmethod
    def build_id(*, bot_instance_id: str, cycle_id: str, universe_hash: str, expected: Tuple[str, ...],
                 completed: Tuple[str, ...], failed: Tuple[str, ...]) -> str:
        return short_id("batch", {
            "bot_instance_id": bot_instance_id, "cycle_id": cycle_id, "universe_hash": universe_hash,
            "expected": list(expected), "completed": list(completed), "failed": list(failed),
        })


@dataclass(frozen=True)
class RankingPolicy:
    """RESEARCH DEFAULTS (Section 15.5) -- never described as calibrated.
    Component transforms are policy-defined, monotonic and bounded, so no
    hidden current-batch normalization exists."""

    schema_version: str = RANKING_POLICY_SCHEMA_VERSION

    w_edge: float = 1.0
    w_tail: float = 0.5
    w_support: float = 0.3
    w_liq: float = 0.2
    w_unc: float = 0.4
    w_ood: float = 0.4
    w_exec: float = 0.3

    # bounded monotonic transforms: clamp((x - floor) / (cap - floor), 0, 1)
    edge_floor_r: float = -0.20
    edge_cap_r: float = 1.00
    tail_floor_r: float = -1.50
    tail_cap_r: float = 1.00
    ess_cap: float = 100.0
    raw_support_cap: float = 200.0
    ci_width_cap: float = 0.60
    max_backoff_level: int = 7
    #: support_quality blend weights (sum to 1)
    support_w_ess: float = 0.4
    support_w_raw: float = 0.2
    support_w_ci: float = 0.3
    support_w_backoff: float = 0.1

    #: liquidity quality when no book capability exists -- deliberately low,
    #: never a fabricated high score (Section 15.10).
    missing_liquidity_quality: float = 0.25
    slippage_cap_r: float = 0.15
    stale_book_multiplier: float = 0.5
    liq_w_spread_percentile: float = 0.5
    liq_w_slippage: float = 0.5

    #: uncertainty blend weights (sum to 1)
    unc_w_state: float = 0.3
    unc_w_forecast: float = 0.4
    unc_w_ci: float = 0.3

    exec_uncertainty_cap_r: float = 0.20

    #: Documented, versioned tie-break sequence (Section 15.13).
    tie_break_sequence: Tuple[str, ...] = (
        "rank_score_desc", "conservative_edge_desc", "ess_desc", "total_cost_asc", "setup_candidate_id_asc",
    )
    incomplete_batch_policy: str = "FAIL_CLOSED"

    @property
    def policy_hash(self) -> str:
        return stable_hash({k: (list(v) if isinstance(v, tuple) else v) for k, v in self.__dict__.items()})


@dataclass(frozen=True)
class RankedOpportunity:
    ranked_opportunity_id: str

    economic_opportunity_id: str
    veto_decision_id: str
    setup_candidate_id: str

    bot_instance_id: Optional[str]
    broker_account_id: Optional[str]
    cycle_id: Optional[str]

    instrument_key: InstrumentKey
    side: str
    setup_family: str

    rank_score: float

    normalized_conservative_edge: float
    normalized_lower_tail: float
    support_quality: float
    liquidity_quality: float

    uncertainty_penalty_component: float
    ood_penalty_component: float
    execution_uncertainty_component: float

    rank_position: int

    ranking_policy_version: str
    ranking_policy_hash: str

    reason_codes: Tuple[str, ...] = ()
    schema_version: str = RANKED_OPPORTUNITY_SCHEMA_VERSION

    @staticmethod
    def build_id(*, economic_opportunity_id: str, veto_decision_id: str, ranking_policy_hash: str) -> str:
        return short_id("rank", {
            "economic_opportunity_id": economic_opportunity_id, "veto_decision_id": veto_decision_id,
            "ranking_policy_hash": ranking_policy_hash,
        })


__all__ = [
    "SymbolEvalKind", "EvaluatedOpportunity", "SymbolEvaluation", "BATCH_INCOMPLETE", "BATCH_COMPLETE",
    "BotCycleEvaluationBatch", "RankingPolicy", "RankedOpportunity",
]
