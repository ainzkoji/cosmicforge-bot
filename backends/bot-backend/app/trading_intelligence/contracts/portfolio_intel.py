"""Section 16 contracts -- PortfolioPolicy, PortfolioMarketContext,
correlation/beta estimates and PortfolioSelectionDecision.

Portfolio intelligence is NOT hard risk and NOT execution. Hard risk,
sizing, margin and slot reservation remain downstream and superior; the
selector only chooses which already-approved, already-ranked opportunities
are mutually compatible under bot/account exposure.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Mapping, Optional, Tuple

from app.trading_intelligence.contracts.factors import FactorSet, default_factor_sets
from app.trading_intelligence.hashing import short_id, stable_hash
from app.trading_intelligence.versions import (
    PORTFOLIO_CONTEXT_SCHEMA_VERSION,
    PORTFOLIO_POLICY_SCHEMA_VERSION,
    PORTFOLIO_SELECTION_SCHEMA_VERSION,
)

SOLVER_EXACT, SOLVER_BEAM, SOLVER_NONE = "EXACT", "BEAM", "NONE"

#: Portfolio veto/selection reason codes (superset of Section 14's portfolio family).
DUPLICATE_EXPOSURE = "DUPLICATE_EXPOSURE"
ACCOUNT_RESERVATION_CONFLICT = "ACCOUNT_RESERVATION_CONFLICT"
NOT_SELECTED_BY_OBJECTIVE = "NOT_SELECTED_BY_OBJECTIVE"
NO_AVAILABLE_SLOTS = "NO_AVAILABLE_SLOTS"
INSUFFICIENT_CORRELATION_HISTORY_FALLBACK = "INSUFFICIENT_CORRELATION_HISTORY_FALLBACK"
PORTFOLIO_DATA_QUALITY_FAULT = "PORTFOLIO_DATA_QUALITY_FAULT"
BETA_UNAVAILABLE_CONSERVATIVE_FALLBACK = "BETA_UNAVAILABLE_CONSERVATIVE_FALLBACK"
ACCOUNT_CORRELATION_CONFLICT = "ACCOUNT_CORRELATION_CONFLICT"
COMMON_FACTOR_CONCENTRATION = "COMMON_FACTOR_CONCENTRATION"
CAPACITY_CHANGED = "CAPACITY_CHANGED"
EXPOSURE_CHANGED = "EXPOSURE_CHANGED"
NOT_APPROVED_FOR_RANKING = "NOT_APPROVED_FOR_RANKING"
RESERVATION_SCHEMA_MISSING = "RESERVATION_SCHEMA_MISSING"
#: correlation-estimate reason codes (Section 16.5 hardening)
STATISTICAL_HISTORY_INSUFFICIENT = "STATISTICAL_HISTORY_INSUFFICIENT"
STATIC_CORRELATION_FALLBACK_USED = "STATIC_CORRELATION_FALLBACK_USED"
NO_STATIC_GROUP_AVAILABLE = "NO_STATIC_GROUP_AVAILABLE"
CROSS_ASSET_CLASS_PAIR = "CROSS_ASSET_CLASS_PAIR"


@dataclass(frozen=True)
class PortfolioPolicy:
    """RESEARCH DEFAULTS -- not certified. No constants live in the engine."""

    schema_version: str = PORTFOLIO_POLICY_SCHEMA_VERSION

    exact_enumeration_max_slots: int = 3
    beam_width: int = 8

    # correlation
    ewma_half_life_bars: float = 48.0
    max_history_bars: int = 500
    minimum_correlation_observations: int = 60
    correlation_shrinkage_lambda: float = 0.20
    correlation_target: float = 0.0  # identity / zero off-diagonal
    correlation_tolerance: float = 0.50
    static_group_correlation: float = 0.60  # fallback: same repo static group
    static_other_correlation: float = 0.0  # fallback: two DIFFERENT known groups (recorded as fallback)
    #: fallback when either side has NO static group: a conservative non-zero
    #: value, never a silent 0 (recorded NO_STATIC_GROUP_AVAILABLE).
    unknown_group_correlation: float = 0.30

    # factors -- asset-class-neutral; which factors exist is FactorSet config
    # (contracts/factors.default_factor_sets), not engine semantics.
    factor_sets: Tuple[FactorSet, ...] = field(default_factory=default_factor_sets)
    minimum_beta_observations: int = 60
    variance_floor: float = 1e-12
    missing_beta_policy: str = "CONSERVATIVE_UNIT"  # or "IGNORE_UNKNOWN"
    missing_beta_conservative_abs: float = 1.0

    # PRE_SIZE_EXPOSURE_PROXY: every candidate and every existing exposure is
    # ONE unit. Not real notional (no TradePlan sizing exists until Section 18).
    pre_size_unit_weight: float = 1.0
    factor_tolerance_units: float = 1.5
    sector_tolerance_units: float = 1.0
    liquidity_low_quality: float = 0.40
    liquidity_low_tolerance: int = 1

    lambda_corr: float = 0.50
    lambda_beta: float = 0.30
    lambda_sector: float = 0.30
    lambda_liq: float = 0.20

    #: Same canonical instrument on the account is refused unless hedge-mode
    #: duplicates are explicitly permitted.
    allow_hedge_mode_duplicates: bool = False

    #: HARD cross-asset currency-factor cap (multi-asset closure). Every FX
    #: position / reservation / candidate contributes its structural legs
    #: (LONG EURUSD = +EUR, -USD; stablecoin quotes fold into USD) in the same
    #: PRE_SIZE unit as the factor model; the account's |net units| per currency
    #: may not exceed this after selection unless the subset does not worsen it.
    #: EURUSD + EURGBP + EURJPY long = EUR +3 -> the third is refused at 2.0.
    #: None disables the hard cap (the soft factor penalty remains).
    max_net_currency_units: Optional[float] = 2.0
    #: LOGICAL asset-class allocations (risk budgets, not wallets): max number of
    #: open + reserved + selected positions per asset class on the account, e.g.
    #: (("FX", 3),). Empty = the account's slots are the only limit. Hard
    #: account risk (daily loss, margin, slots) stays superior either way.
    asset_class_max_positions: Tuple[Tuple[str, int], ...] = ()

    reservation_ttl_seconds: int = 900

    @property
    def ewma_lambda(self) -> float:
        return 0.5 ** (1.0 / self.ewma_half_life_bars)

    @property
    def policy_hash(self) -> str:
        return stable_hash(asdict(self))


@dataclass(frozen=True)
class CorrelationEstimate:
    a: str
    b: str
    rho_ewma: Optional[float]
    rho_shrunk: float
    n_obs: int
    source: str  # EWMA | STATIC_GROUP_FALLBACK
    fallback_used: bool
    #: Kish effective sample size of the EWMA weights (None when not estimated)
    effective_sample_size: Optional[float] = None
    shrinkage_lambda: float = 0.0
    shrinkage_target: float = 0.0
    reason_codes: Tuple[str, ...] = ()


@dataclass(frozen=True)
class BetaEstimate:
    asset: str
    factor: str
    beta: Optional[float]  # None = unknown, never 0
    sample_count: int
    quality: str  # OK | INSUFFICIENT_HISTORY | ZERO_VARIANCE | FACTOR_MISSING


@dataclass(frozen=True)
class PortfolioMarketContext:
    """Causal, tenant-free market context: returns known at ``decision_time``
    only, injected into the selector (the math never fetches)."""

    portfolio_market_context_id: str
    decision_time: int
    #: instrument -> ((close_time, log_return), ...) ascending, all <= decision_time
    return_histories: Mapping[str, Tuple[Tuple[int, float], ...]]
    factor_histories: Mapping[str, Tuple[Tuple[int, float], ...]]
    instrument_groups: Mapping[str, str]
    data_quality: Mapping[str, str]
    schema_version: str = PORTFOLIO_CONTEXT_SCHEMA_VERSION
    #: instrument -> asset class (so fallbacks can flag cross-asset pairs)
    instrument_asset_classes: Mapping[str, str] = field(default_factory=dict)

    @property
    def context_hash(self) -> str:
        return stable_hash({
            "decision_time": self.decision_time,
            "returns": {k: list(v) for k, v in sorted(self.return_histories.items())},
            "factors": {k: list(v) for k, v in sorted(self.factor_histories.items())},
            "groups": dict(sorted(self.instrument_groups.items())),
            "quality": dict(sorted(self.data_quality.items())),
            "asset_classes": dict(sorted(self.instrument_asset_classes.items())),
            "schema": self.schema_version,
        })


@dataclass(frozen=True)
class RejectedCandidate:
    ranked_opportunity_id: str
    setup_candidate_id: str
    reason_code: str
    detail: str = ""


@dataclass(frozen=True)
class ScoreBreakdown:
    raw_rank_sum: float
    correlation_penalty: float
    factor_penalty: float
    sector_penalty: float
    liquidity_penalty: float
    fallbacks_used: Tuple[str, ...] = ()
    #: (factor_id, net exposure) of the selected portfolio + existing account
    #: exposure, sorted -- evidence of what concentration was scored.
    factor_net_exposures: Tuple[Tuple[str, float], ...] = ()


@dataclass(frozen=True)
class PortfolioSelectionDecision:
    portfolio_selection_id: str

    broker_account_id: str
    bot_instance_id: str
    cycle_id: str

    account_exposure_snapshot_id: str
    portfolio_market_context_id: str

    ranked_opportunity_ids: Tuple[str, ...]
    selected_opportunity_ids: Tuple[str, ...]
    rejected_candidates: Tuple[RejectedCandidate, ...]

    available_slots: int
    portfolio_score: float
    score_breakdown: ScoreBreakdown
    solver: str

    portfolio_policy_version: str
    portfolio_policy_hash: str

    reservation_id: Optional[str]
    reservation_status: str  # RESERVED | NOT_REQUIRED | CONFLICT | RELEASED ...

    reason_codes: Tuple[str, ...]
    decision_time: int
    schema_version: str = PORTFOLIO_SELECTION_SCHEMA_VERSION

    @property
    def is_reserved(self) -> bool:
        return self.reservation_status == "RESERVED" and self.reservation_id is not None

    @staticmethod
    def build_id(*, broker_account_id: str, bot_instance_id: str, cycle_id: str, ranked_ids: Tuple[str, ...],
                 exposure_snapshot_hash: str, context_hash: str, policy_hash: str) -> str:
        return short_id("psel", {
            "broker_account_id": broker_account_id, "bot_instance_id": bot_instance_id, "cycle_id": cycle_id,
            "ranked": list(ranked_ids), "exposure": exposure_snapshot_hash, "context": context_hash, "policy": policy_hash,
        })


__all__ = [
    "SOLVER_EXACT", "SOLVER_BEAM", "SOLVER_NONE", "DUPLICATE_EXPOSURE", "ACCOUNT_RESERVATION_CONFLICT",
    "NOT_SELECTED_BY_OBJECTIVE", "NO_AVAILABLE_SLOTS", "INSUFFICIENT_CORRELATION_HISTORY_FALLBACK",
    "PORTFOLIO_DATA_QUALITY_FAULT", "BETA_UNAVAILABLE_CONSERVATIVE_FALLBACK", "ACCOUNT_CORRELATION_CONFLICT",
    "COMMON_FACTOR_CONCENTRATION", "CAPACITY_CHANGED", "EXPOSURE_CHANGED", "NOT_APPROVED_FOR_RANKING",
    "RESERVATION_SCHEMA_MISSING", "STATISTICAL_HISTORY_INSUFFICIENT", "STATIC_CORRELATION_FALLBACK_USED",
    "NO_STATIC_GROUP_AVAILABLE", "CROSS_ASSET_CLASS_PAIR",
    "PortfolioPolicy", "CorrelationEstimate", "BetaEstimate", "PortfolioMarketContext", "RejectedCandidate",
    "ScoreBreakdown", "PortfolioSelectionDecision",
]
