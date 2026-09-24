"""VenueCostPolicy (Section 17) -- every freshness bound, conservative
fallback and uncertainty coefficient the venue economics layer uses.

RESEARCH DEFAULTS, not calibrated: they exist so Section 17 can produce
honest, conservative evidence now. Planned-vs-realized execution cost
calibration (a later section) is what may tighten them. No constant lives in
an adapter or in the cost model.

Fallbacks are deliberately the EXPENSIVE end: the crypto fee fallback is the
Binance USD-M regular (VIP 0) tier -- the highest published tier, so an
unknown account tier is never assumed cheaper than it can be.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Mapping, Optional, Tuple

from app.trading_intelligence.hashing import stable_hash
from app.trading_intelligence.versions import VENUE_COST_POLICY_SCHEMA_VERSION


def _d(**kw):
    return field(default_factory=lambda: dict(kw))


@dataclass(frozen=True)
class VenueCostPolicy:
    schema_version: str = VENUE_COST_POLICY_SCHEMA_VERSION

    # -- causality / freshness --------------------------------------------------
    max_book_age_ms: int = 5_000
    max_depth_age_ms: int = 5_000
    max_funding_age_ms: int = 120_000
    max_metadata_age_ms: int = 24 * 3_600_000
    #: how long a cost observation stays usable as evidence (capped by the
    #: candidate's own validity downstream)
    observation_validity_ms: int = 300_000
    min_reliable_depth_levels: int = 5

    # -- fees ---------------------------------------------------------------------
    #: asset_class -> (maker, taker) as fractions of notional
    fallback_fee_rates: Mapping[str, Tuple[float, float]] = _d(CRYPTO=(0.0002, 0.0005))
    #: asset_class -> per-contract commission (quote currency) when unknown
    fallback_commission_per_contract: Mapping[str, float] = _d(FUTURES=2.50)
    #: fee source -> uncertainty as a fraction of the fee component
    fee_uncertainty_fraction: Mapping[str, float] = _d(
        OBSERVED_ACCOUNT_TIER=0.0, BROKER_METADATA=0.10, VENUE_DEFAULT=0.25, CONSERVATIVE_CONFIGURED_FALLBACK=0.50)
    #: CATI assumes it crosses the spread on entry and exit (conservative):
    #: taker fees both sides, full spread round trip.
    assume_taker_entry: bool = True
    assume_taker_exit: bool = True

    # -- spread -------------------------------------------------------------------
    liquidity_bucket_spread_bps: Mapping[str, float] = _d(HIGH=1.0, MEDIUM=3.0, LOW=10.0)
    fallback_spread_bps: Mapping[str, float] = _d(CRYPTO=4.0, FX=3.0, FUTURES=3.0)
    spread_uncertainty_fraction: Mapping[str, float] = _d(
        LIVE_TOP_OF_BOOK=0.25, RECENT_VENUE_DISTRIBUTION=0.50, LIQUIDITY_BUCKET_HISTORICAL=1.0,
        CONSERVATIVE_VENUE_FALLBACK=1.5)
    #: which percentile of a recent spread distribution stands in for a live spread
    spread_distribution_percentile: float = 0.75

    # -- slippage -------------------------------------------------------------------
    liquidity_bucket_slippage_bps: Mapping[str, float] = _d(HIGH=1.0, MEDIUM=3.0, LOW=10.0)
    venue_class_slippage_bps: Mapping[str, float] = _d(CRYPTO=5.0, FX=2.0, FUTURES=3.0)
    conservative_slippage_bps: float = 10.0
    #: latency/queue floor per side even when depth says "fills at best"
    min_slippage_bps_floor: float = 0.5
    slippage_uncertainty_fraction: Mapping[str, float] = _d(
        DEPTH_WALK=0.25, HISTORICAL_INSTRUMENT=0.50, HISTORICAL_LIQUIDITY_BUCKET=1.0, VENUE_ASSET_CLASS_DEFAULT=1.5,
        CONSERVATIVE_CONFIGURED_FALLBACK=2.0)
    #: an order larger than this fraction of visible same-side depth is "large"
    large_order_depth_fraction: float = 0.25
    #: price beyond the last visible level, per side, when depth runs out (bps)
    beyond_depth_penalty_bps: float = 25.0

    # -- perpetual funding ------------------------------------------------------------
    fallback_funding_rate_per_interval: float = 0.0001
    fallback_funding_interval_ms: int = 8 * 3_600_000
    funding_first_stamp_uncertainty: Mapping[str, float] = _d(
        PREDICTED_RATE=0.25, CURRENT_RATE_SCHEDULE=0.50, CONSERVATIVE_CONFIGURED_FALLBACK=1.0)
    #: each later stamp's rate is unknown at decision time
    funding_later_stamp_uncertainty: float = 1.0
    #: False: expected funding INCOME is recorded but never reduces cost.
    credit_funding_income: bool = False
    funding_income_haircut: float = 0.5

    # -- FX financing -----------------------------------------------------------------
    #: None = no certified fallback: a hold that crosses a rollover with no
    #: broker swap data fails closed.
    fx_financing_fallback_annual_rate: Optional[float] = None
    credit_financing_income: bool = False
    financing_uncertainty_fraction: float = 0.25

    # -- dated-futures carry -------------------------------------------------------------
    credit_carry_benefit: bool = False
    carry_uncertainty_fraction: float = 0.50
    carry_unavailable_uncertainty_bps: float = 5.0

    # -- adapter / broker / session ------------------------------------------------------
    adapter_uncertainty_fraction: Mapping[str, float] = _d(
        UNVALIDATED=1.0, SHADOW_VALIDATED=0.25, DEMO_VALIDATED=0.10, PRODUCTION_VALIDATED=0.0)
    trusted_adapter_statuses: Tuple[str, ...] = ("SHADOW_VALIDATED", "DEMO_VALIDATED", "PRODUCTION_VALIDATED")
    broker_degraded_uncertainty_fraction: float = 0.50
    thin_session_uncertainty_multiplier: float = 2.0
    unknown_session_fails_closed: bool = True

    # -- size -----------------------------------------------------------------------------
    #: asset_class -> reference notional in quote currency; None = one minimum
    #: tradable unit (e.g. one futures contract). An estimation assumption only.
    reference_notional: Mapping[str, Optional[float]] = _d(CRYPTO=1_000.0, FX=100_000.0, FUTURES=None)
    marginal_curve_multiples: Tuple[float, ...] = (1.0, 2.0, 5.0, 10.0, 20.0)

    # -- holding time -------------------------------------------------------------------------
    default_holding_bars: int = 12
    #: the forecast's label horizon: the longest a hold can last
    max_holding_bars: int = 48

    @property
    def policy_hash(self) -> str:
        return stable_hash(asdict(self))


def default_venue_cost_policy() -> VenueCostPolicy:
    return VenueCostPolicy()


__all__ = ["VenueCostPolicy", "default_venue_cost_policy"]
