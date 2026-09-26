"""Transfer cost and delay evidence over existing capital-planner outcomes.
No money movement, reservations or execution authority are granted here.
"""
from dataclasses import dataclass, asdict, replace
from typing import Optional
import math
from app.trading_intelligence.hashing import short_id
from app.trading_intelligence.capital.planner import (
    NO_ACTION_SHARED_COLLATERAL, LOGICAL_REALLOCATION, PHYSICAL_INTERNAL_TRANSFER_REQUIRED,
)

@dataclass(frozen=True)
class TransferEconomics:
    route: str
    user_id: str
    broker_account_id: str
    observed_at: int
    valid_until: int
    source: str
    fee_quote: Optional[float] = None
    expected_latency_ms: Optional[int] = None
    fee_currency: Optional[str] = None
    version: str = "transfer-economics-v1"


def attach_multi_asset_economics(cost, candidate, observation, transfer=None):
    """Add a documented physical fee once; absent economics block admission.
    Existing numeric INVALID estimates remain internal placeholders; explicit
    availability evidence represents each unavailable component as None.
    """
    now = observation.decision_time
    reasons = list(cost.reason_codes)
    fee_r = latency = None
    if transfer is None:
        reasons.append("TRANSFER_ECONOMICS_UNAVAILABLE")
    elif (transfer.user_id, transfer.broker_account_id) != (observation.user_id, observation.broker_account_id):
        reasons.append("TRANSFER_ACCOUNT_SCOPE_MISMATCH")
    elif not transfer.source or not transfer.observed_at <= now < transfer.valid_until:
        reasons.append("TRANSFER_ECONOMICS_STALE")
    elif transfer.route in (NO_ACTION_SHARED_COLLATERAL, LOGICAL_REALLOCATION):
        fee_r, latency = 0.0, 0  # not applicable: there is no physical move
    elif transfer.route == PHYSICAL_INTERNAL_TRANSFER_REQUIRED:
        meta = observation.instrument_metadata
        if (transfer.fee_quote is None or not math.isfinite(transfer.fee_quote) or transfer.fee_quote < 0
                or meta is None or transfer.fee_currency != meta.quote_currency):
            reasons.append("TRANSFER_COST_UNAVAILABLE")
        else:
            risk_ccy = cost.native_costs.get("risk_ccy")
            if risk_ccy and risk_ccy > 0:
                fee_r = transfer.fee_quote / risk_ccy
            else:
                reasons.append("TRANSFER_COST_UNAVAILABLE")
        latency = transfer.expected_latency_ms
        if latency is None or latency < 0:
            reasons.append("TRANSFER_LATENCY_UNAVAILABLE")
        elif candidate.valid_until is None or now + latency >= candidate.valid_until:
            reasons.append("TRANSFER_ARRIVES_AFTER_OPPORTUNITY_EXPIRY")
    else:
        reasons.append("TRANSFER_ROUTE_UNAVAILABLE")
    invalid = cost.source_quality == "INVALID" or len(reasons) > len(cost.reason_codes)
    if invalid and "COST_NOT_VIABLE" not in reasons:
        reasons.append("COST_NOT_VIABLE")
    obs = observation
    components = {
        "fee_R": cost.fee_R if obs.fee_observation.source != "UNAVAILABLE" else None,
        "spread_R": cost.spread_R if obs.spread_observation.source != "UNAVAILABLE" else None,
        "slippage_R": cost.slippage_R if obs.slippage_observation.source != "UNAVAILABLE" else None,
        "funding_R": cost.funding_R if obs.funding_observation.source != "UNAVAILABLE" else None,
        "financing_R": cost.carry_R if obs.financing_observation.source != "UNAVAILABLE" else None,
        "transfer_R": fee_r, "transfer_latency_ms": latency,
        "availability": "UNAVAILABLE_WITH_REASON" if invalid else "AVAILABLE",
        "reason_codes": tuple(dict.fromkeys(reasons)), "version": "multi-asset-economics-v1",
        "policy_hash": cost.cost_policy_hash, "automatic_execution_authorized": False,
    }
    identity = {"cost": cost.cost_estimate_id, "transfer": asdict(transfer) if transfer else None,
                "signal_valid_until": candidate.valid_until, "evidence": components}
    extra = fee_r if fee_r is not None else 0.0
    return replace(cost, cost_estimate_id=short_id("cost", identity), source_quality="INVALID" if invalid else cost.source_quality,
                   reason_codes=tuple(dict.fromkeys(reasons)), total_cost_R=cost.total_cost_R + extra,
                   marginal_cost_curve=tuple((n, c + extra) for n, c in cost.marginal_cost_curve),
                   native_costs={**cost.native_costs, "multi_asset_economics": components})
