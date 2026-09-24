"""CostEstimate construction (Section 13.1) -- reuses
``app.replay.cost_model.CostModel`` (the same fee/spread/slippage/funding
fields Section 12's research labeling reuses) rather than a second cost
model. This is the LIVE/venue-facing counterpart: one point-in-time estimate
for a current candidate, not a historical label.
"""
from __future__ import annotations

from typing import Optional

from app.replay.cost_model import CostModel
from app.trading_intelligence.contracts.economics import CostEstimate, CostScope
from app.trading_intelligence.contracts.setup import SetupCandidate, timeframe_to_ms
from app.trading_intelligence.hashing import stable_hash
from app.trading_intelligence.versions import VENUE_COST_MODEL_VERSION

#: RESEARCH DEFAULT: assumed holding period, in bars, used to estimate
#: funding cost before the trade's actual outcome is known.
DEFAULT_ASSUMED_HOLDING_BARS = 12


def _is_effectively_zero_cost(cost_model: CostModel) -> bool:
    return (
        cost_model.maker_fee == 0.0 and cost_model.taker_fee == 0.0
        and cost_model.spread == 0.0 and cost_model.slippage == 0.0 and cost_model.funding_rate == 0.0
    )


def build_cost_estimate(
    candidate: SetupCandidate,
    *,
    cost_model: CostModel,
    cost_scope: str = CostScope.REFERENCE_RESEARCH.value,
    venue: Optional[str] = None,
    liquidity_verified: bool = False,
    assumed_holding_bars: int = DEFAULT_ASSUMED_HOLDING_BARS,
    user_id: Optional[str] = None,
    broker_account_id: Optional[str] = None,
    bot_instance_id: Optional[str] = None,
) -> CostEstimate:
    """Fails closed via ``source_quality``/``reason_codes`` when the cost
    model cannot be meaningfully bounded (e.g. an all-zero model) -- never
    silently treats missing cost information as zero cost (P7)."""
    venue = venue or candidate.instrument_key.venue
    risk = candidate.initial_structural_risk
    entry = candidate.trigger_reference
    notional = entry  # see forecast/labels.py: 1-unit notional keeps cost in price units

    fee_currency = notional * cost_model.taker_fee * 2
    spread_currency = notional * cost_model.spread * 2
    slippage_currency = notional * cost_model.slippage * 2

    bar_ms = timeframe_to_ms(candidate.timeframe) or 0
    held_ms = bar_ms * assumed_holding_bars
    funding_currency = cost_model.funding_cost(notional, held_ms)

    fee_R = fee_currency / risk
    spread_R = spread_currency / risk
    slippage_R = slippage_currency / risk
    funding_R = funding_currency / risk
    carry_R = 0.0
    total_cost_R = fee_R + spread_R + slippage_R + funding_R + carry_R

    reason_codes = []
    source_quality = "VALID"
    if _is_effectively_zero_cost(cost_model):
        source_quality = "DEGRADED"
        from app.trading_intelligence.contracts.economics import EconomicsReasonCode

        reason_codes.append(EconomicsReasonCode.COST_NOT_VIABLE.value)

    # Unverified liquidity means actual slippage could differ materially
    # from the assumed rate -- reflected as cost uncertainty, never as
    # additional silent cost or as zero uncertainty.
    cost_uncertainty_R = 0.0 if liquidity_verified else spread_R + slippage_R

    cost_policy_hash = stable_hash({
        "cost_model": cost_model.to_dict(), "assumed_holding_bars": assumed_holding_bars,
        "venue_cost_model_version": VENUE_COST_MODEL_VERSION,
    })

    return CostEstimate(
        cost_estimate_id=CostEstimate.build_id(
            instrument_key=candidate.instrument_key, venue=venue, cost_scope=cost_scope,
            cost_policy_hash=cost_policy_hash, decision_time=candidate.decision_time,
        ),
        instrument_key=candidate.instrument_key,
        venue=venue,
        cost_scope=cost_scope,
        fee_R=fee_R, spread_R=spread_R, slippage_R=slippage_R, funding_R=funding_R, carry_R=carry_R,
        total_cost_R=total_cost_R,
        cost_uncertainty_R=cost_uncertainty_R,
        cost_model_version=VENUE_COST_MODEL_VERSION,
        cost_policy_hash=cost_policy_hash,
        source_quality=source_quality,
        reason_codes=tuple(reason_codes),
        user_id=user_id if cost_scope == CostScope.ACCOUNT.value else None,
        broker_account_id=broker_account_id if cost_scope == CostScope.ACCOUNT.value else None,
        bot_instance_id=bot_instance_id if cost_scope == CostScope.ACCOUNT.value else None,
    )


__all__ = ["DEFAULT_ASSUMED_HOLDING_BARS", "build_cost_estimate"]
