"""Transfer / logical-allocation economics over capital-planner outcomes (Sections 16.11-16.12, 16.A-16.D).

The facts come from a DRY-RUN ``capital.planner.plan_capital`` (Section 17 account stage): nothing here
moves money, reserves capital or grants execution authority.

* ``NO_ACTION_SHARED_COLLATERAL`` / ``LOGICAL_REALLOCATION`` -- there is NO physical transfer: the
  transfer component is ``NOT_APPLICABLE`` (no fee, no delay), never a fabricated "transfer cost = 0".
* ``PHYSICAL_INTERNAL_TRANSFER_REQUIRED`` -- the fee and the latency must both be known (with their
  source); the capital must arrive before the opportunity's validity horizon, else the candidate is
  non-viable (``TRANSFER_DELAY_EXCEEDS_VALIDITY`` / ``OPPORTUNITY_EXPIRED_BEFORE_CAPITAL_READY``).
* any other planner outcome (topology unknown, route unavailable, insufficient capital, unresolved
  transfer) -> ``TRANSFER_ECONOMICS_UNAVAILABLE`` plus the planner's own reason.

Order of operations: the per-symbol VENUE stage cannot know the account route yet, so it records the
transfer component as ``PENDING_ACCOUNT_CAPITAL_PLAN`` (``defer_transfer=True``); the account stage then
finalizes it. A final (non-deferred) evaluation without transfer facts fails closed.
"""
from dataclasses import asdict, dataclass, replace
from typing import Optional, Tuple
import math
from app.trading_intelligence.hashing import short_id
from app.trading_intelligence.capital.planner import (
    NO_ACTION_SHARED_COLLATERAL, LOGICAL_REALLOCATION, PHYSICAL_INTERNAL_TRANSFER_REQUIRED,
)

TRANSFER_ECONOMICS_VERSION = "transfer-economics-v2"
NOT_APPLICABLE, KNOWN, UNAVAILABLE = "NOT_APPLICABLE", "KNOWN", "UNAVAILABLE"
PENDING_ACCOUNT_CAPITAL_PLAN = "PENDING_ACCOUNT_CAPITAL_PLAN"
TRANSFER_ECONOMICS_UNAVAILABLE = "TRANSFER_ECONOMICS_UNAVAILABLE"
TRANSFER_DELAY_EXCEEDS_VALIDITY = "TRANSFER_DELAY_EXCEEDS_VALIDITY"
OPPORTUNITY_EXPIRED_BEFORE_CAPITAL_READY = "OPPORTUNITY_EXPIRED_BEFORE_CAPITAL_READY"
LOGICAL_ROUTES = (NO_ACTION_SHARED_COLLATERAL, LOGICAL_REALLOCATION)


@dataclass(frozen=True)
class TransferEconomics:
    route: str                                 # CapitalPlan.outcome
    user_id: str
    broker_account_id: str
    observed_at: int
    valid_until: int                           # freshness of these facts
    source: str
    fee_quote: Optional[float] = None          # absolute fee in ``fee_currency`` (physical routes only)
    expected_latency_ms: Optional[int] = None  # physical routes only
    fee_currency: Optional[str] = None
    version: str = TRANSFER_ECONOMICS_VERSION
    fee_source: Optional[str] = None
    latency_source: Optional[str] = None
    source_wallet: Optional[str] = None
    destination_wallet: Optional[str] = None
    asset: Optional[str] = None
    amount: Optional[str] = None
    plan_reason_codes: Tuple[str, ...] = ()

    @property
    def physical_transfer_required(self) -> bool:
        return self.route == PHYSICAL_INTERNAL_TRANSFER_REQUIRED


@dataclass(frozen=True)
class TransferAssessment:
    status: str                  # NOT_APPLICABLE | KNOWN | UNAVAILABLE | PENDING_ACCOUNT_CAPITAL_PLAN
    fee_R: Optional[float]       # None unless a physical fee is KNOWN
    latency_ms: Optional[int]    # None unless a physical latency is KNOWN
    capital_ready_by: Optional[int]
    reason_codes: Tuple[str, ...] = ()

    @property
    def viable(self) -> bool:
        return not self.reason_codes and self.status in (NOT_APPLICABLE, KNOWN)

    def to_dict(self) -> dict:
        return asdict(self)


def assess_transfer(transfer: Optional[TransferEconomics], *, now: int, opportunity_valid_until: Optional[int],
                    user_id: Optional[str], broker_account_id: Optional[str], risk_ccy: Optional[float],
                    settlement_currency: Optional[str], defer: bool = False) -> TransferAssessment:
    """Pure. ``risk_ccy``: the candidate's risk in the settlement currency at the provisional executable size."""
    if transfer is None:
        if defer:
            return TransferAssessment(PENDING_ACCOUNT_CAPITAL_PLAN, None, None, None)
        return TransferAssessment(UNAVAILABLE, None, None, None, (TRANSFER_ECONOMICS_UNAVAILABLE,))
    if (transfer.user_id, transfer.broker_account_id) != (user_id, broker_account_id):
        return TransferAssessment(UNAVAILABLE, None, None, None, ("TRANSFER_ACCOUNT_SCOPE_MISMATCH",))
    if not transfer.source or not transfer.observed_at <= now < transfer.valid_until:
        return TransferAssessment(UNAVAILABLE, None, None, None, ("TRANSFER_ECONOMICS_STALE",))
    if transfer.route in LOGICAL_ROUTES:
        # no physical move exists: no fee and no delay apply -- NOT a zero-cost transfer
        return TransferAssessment(NOT_APPLICABLE, None, None, now)
    if transfer.route != PHYSICAL_INTERNAL_TRANSFER_REQUIRED:
        return TransferAssessment(UNAVAILABLE, None, None, None,
                                  (TRANSFER_ECONOMICS_UNAVAILABLE,) + tuple(transfer.plan_reason_codes or (transfer.route,)))
    reasons = []
    fee_r = latency = ready = None
    fee = transfer.fee_quote
    if (fee is None or not math.isfinite(fee) or fee < 0 or not transfer.fee_source
            or not transfer.fee_currency or transfer.fee_currency != settlement_currency):
        reasons.append("TRANSFER_COST_UNAVAILABLE")
    elif risk_ccy is None or not math.isfinite(risk_ccy) or risk_ccy <= 0:
        reasons.append("TRANSFER_COST_UNAVAILABLE")
    else:
        fee_r = fee / risk_ccy
    if transfer.expected_latency_ms is None or transfer.expected_latency_ms < 0 or not transfer.latency_source:
        reasons.append("TRANSFER_LATENCY_UNAVAILABLE")
    else:
        latency = int(transfer.expected_latency_ms)
        ready = now + latency
        if opportunity_valid_until is None:
            reasons.append("OPPORTUNITY_VALIDITY_UNKNOWN")
        elif ready >= opportunity_valid_until:
            reasons += [TRANSFER_DELAY_EXCEEDS_VALIDITY, OPPORTUNITY_EXPIRED_BEFORE_CAPITAL_READY]
    if "TRANSFER_COST_UNAVAILABLE" in reasons or "TRANSFER_LATENCY_UNAVAILABLE" in reasons:
        reasons.insert(0, TRANSFER_ECONOMICS_UNAVAILABLE)
    return TransferAssessment(KNOWN if not reasons else UNAVAILABLE, fee_r, latency, ready, tuple(reasons))


def attach_multi_asset_economics(cost, candidate, observation, transfer=None, *, defer_transfer: bool = False,
                                 risk_ccy: Optional[float] = None):
    """Explicit per-component availability for the versioned multi-asset policy. A KNOWN physical fee is
    added ONCE; an unavailable component is None (never 0). ``defer_transfer`` = the per-symbol venue
    stage, before any account route can be known (final evaluation happens in the account stage)."""
    now = observation.decision_time
    meta = observation.instrument_metadata
    rc = risk_ccy if risk_ccy is not None else cost.native_costs.get("risk_ccy")
    a = assess_transfer(transfer, now=now, opportunity_valid_until=candidate.valid_until,
                        user_id=observation.user_id, broker_account_id=observation.broker_account_id,
                        risk_ccy=rc, settlement_currency=meta.quote_currency if meta is not None else None,
                        defer=defer_transfer)
    reasons = list(cost.reason_codes) + list(a.reason_codes)
    invalid = cost.source_quality == "INVALID" or bool(a.reason_codes)
    if invalid and "COST_NOT_VIABLE" not in reasons:
        reasons.append("COST_NOT_VIABLE")
    obs = observation
    components = {
        "fee_R": cost.fee_R if obs.fee_observation.source != "UNAVAILABLE" else None,
        "spread_R": cost.spread_R if obs.spread_observation.source != "UNAVAILABLE" else None,
        "slippage_R": cost.slippage_R if obs.slippage_observation.source != "UNAVAILABLE" else None,
        "funding_R": cost.funding_R if obs.funding_observation.source != "UNAVAILABLE" else None,
        "financing_R": cost.carry_R if obs.financing_observation.source != "UNAVAILABLE" else None,
        "transfer_R": a.fee_R, "transfer_latency_ms": a.latency_ms, "transfer_status": a.status,
        "physical_transfer_required": bool(transfer.physical_transfer_required) if transfer else None,
        "capital_ready_by": a.capital_ready_by,
        "final": not defer_transfer,
        "availability": "UNAVAILABLE_WITH_REASON" if invalid else (
            PENDING_ACCOUNT_CAPITAL_PLAN if a.status == PENDING_ACCOUNT_CAPITAL_PLAN else "AVAILABLE"),
        "reason_codes": tuple(dict.fromkeys(reasons)), "version": "multi-asset-economics-v2",
        "policy_hash": cost.cost_policy_hash, "automatic_execution_authorized": False,
    }
    identity = {"cost": cost.cost_estimate_id, "transfer": asdict(transfer) if transfer else None,
                "signal_valid_until": candidate.valid_until, "evidence": components}
    extra = a.fee_R if a.fee_R is not None else 0.0  # added once; NOT_APPLICABLE / PENDING add nothing
    return replace(cost, cost_estimate_id=short_id("cost", identity),
                   source_quality="INVALID" if invalid else cost.source_quality,
                   reason_codes=tuple(dict.fromkeys(reasons)), total_cost_R=cost.total_cost_R + extra,
                   marginal_cost_curve=tuple((n, c + extra) for n, c in cost.marginal_cost_curve),
                   native_costs={**cost.native_costs, "multi_asset_economics": components})


__all__ = ["KNOWN", "LOGICAL_ROUTES", "NOT_APPLICABLE", "OPPORTUNITY_EXPIRED_BEFORE_CAPITAL_READY",
           "PENDING_ACCOUNT_CAPITAL_PLAN", "TRANSFER_DELAY_EXCEEDS_VALIDITY", "TRANSFER_ECONOMICS_UNAVAILABLE",
           "TRANSFER_ECONOMICS_VERSION", "TransferAssessment", "TransferEconomics", "UNAVAILABLE",
           "assess_transfer", "attach_multi_asset_economics"]
