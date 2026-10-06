"""Live entry validity for frozen CATI residual plans.

The next-native-open price is EVIDENCE of where the frozen decision was
referenced; it is not a requirement that the market stay within the modeled
slippage of it minutes later. At submission the question is whether the trade
is still the same valid CATI opportunity at the executable price:

- the structural stop has not been crossed and the target is still ahead;
- the structural stop distance is inside the system's absolute bounds;
- after round-trip costs, remaining reward/risk is at least the policy's
  minimum (chasing an adverse move erodes it, so excessive chase fails);

The plan's stop, target and costs are never changed here.
"""
from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Optional


@dataclass(frozen=True)
class EntryEconomics:
    valid: bool
    reason: Optional[str]
    price: float
    stop_fraction: float
    gross_reward_risk: float
    net_reward_risk: float
    unit_cost: float
    chase_r: float

    def evidence(self) -> dict:
        return {k: getattr(self, k) for k in self.__dataclass_fields__}


def unit_round_trip_cost(price: float, *, fee: float, half_spread: float, slippage: float,
                         funding_buffer: float = 0.0) -> float:
    """Price-unit cost of entering and exiting one unit: fee, half spread and
    slippage on each side plus the lifecycle funding buffer, using the frozen
    decision's modeled rates."""
    return float(price) * (2.0 * (float(fee) + float(half_spread) + float(slippage)) + float(funding_buffer))


def evaluate_entry_economics(*, side: str, price: float, reference: float, stop: float, target: float,
                             min_reward_risk: float, min_stop_fraction: float, max_stop_fraction: float,
                             fee: float, half_spread: float, slippage: float,
                             funding_buffer: float = 0.0) -> EntryEconomics:
    sign = 1.0 if side == "LONG" else -1.0
    price = float(price)
    risk = sign * (price - float(stop))
    reward = sign * (float(target) - price)
    initial_risk = sign * (float(reference) - float(stop))
    chase = sign * (price - float(reference)) / initial_risk if initial_risk > 0 else math.inf
    stop_fraction = risk / price if price > 0 else math.inf
    cost = unit_round_trip_cost(price, fee=fee, half_spread=half_spread, slippage=slippage,
                                funding_buffer=funding_buffer)

    def result(reason, gross=0.0, net=0.0):
        return EntryEconomics(reason is None, reason, price, stop_fraction, gross, net, cost, chase)

    if not (math.isfinite(price) and price > 0):
        return result("CATI_LIVE_PRICE_INVALID")
    if risk <= 0:
        return result("STRUCTURAL_LEVEL_BROKEN")
    if reward <= 0:
        return result("CATI_TARGET_ALREADY_REACHED")
    if stop_fraction > max_stop_fraction:
        return result("CATI_STRUCTURAL_STOP_EXCEEDS_SYSTEM_MAX")
    if stop_fraction < min_stop_fraction:
        return result("CATI_STRUCTURAL_STOP_BELOW_SYSTEM_MIN")
    gross = reward / risk
    net = (reward - cost) / (risk + cost)
    if net < min_reward_risk:
        return result("CATI_ENTRY_ECONOMICS_DEGRADED", gross, net)
    return result(None, gross, net)


def depth_vwap(levels, quantity: float) -> Optional[float]:
    """Volume-weighted price to fill ``quantity`` against ``levels`` of
    (price, qty), best first. None when the visible book cannot fill it."""
    remaining, notional = float(quantity), 0.0
    for price, qty in levels:
        take = min(remaining, float(qty))
        notional += take * float(price)
        remaining -= take
        if remaining <= 1e-12:
            return notional / float(quantity)
    return None


__all__ = ["EntryEconomics", "depth_vwap", "evaluate_entry_economics", "unit_round_trip_cost"]
