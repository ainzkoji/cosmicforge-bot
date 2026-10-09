"""Daily trend portfolio rule for ONE decision day: targets, caps, brakes and the orders they imply.

Pure functions of what is known at the decision close. The research evaluator calls them once per simulated
day; the Step 3 pipeline will call them once per real day and must reproduce the evaluator's targets.

Two policies are distinguished everywhere and never mixed:

* ``MANDATE``    -- the frozen specification as written (the certification basis);
* ``EXECUTABLE`` -- the same rules under the engine's current hard limits: per-trade risk no higher than 0.40%
                    and no entry whose stop is more than 15% away. Reported separately; it can never rescue or
                    replace a mandate result.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Dict, List, Mapping, Optional, Sequence

from .spec import INTERPRETATION, SPECIFICATION

MANDATE, EXECUTABLE = "MANDATE", "EXECUTABLE"
ENTRY, EXIT, ADJUST = "ENTRY", "EXIT", "ADJUST"
REJECT_STOP_DISTANCE = "STOP_DISTANCE_EXCEEDS_ENGINE_MAX"
REJECT_DAILY_PAUSE = "DAILY_PAUSE_NO_NEW_POSITION"
REJECT_MIN_NOTIONAL = "BELOW_EXCHANGE_MINIMUM_NOTIONAL"
REJECT_ZERO_QUANTITY = "QUANTITY_ROUNDS_TO_ZERO"
REJECT_NO_BAR = "NO_BAR_AT_FILL"
REJECT_POSITION_LIMIT = "POSITION_LIMIT"


@dataclass(frozen=True)
class RiskLevel:
    name: str
    policy: str
    risk_per_trade: float
    approved_risk_per_trade: float
    max_open_risk: float
    max_positions: int
    leverage: float
    daily_pause: float
    halve_drawdown: float
    stop_drawdown: float
    max_entry_stop_distance: Optional[float]


def risk_level(name: str, policy: str = MANDATE) -> RiskLevel:
    row = SPECIFICATION["risk_levels"][name]
    ex = INTERPRETATION["executable_policy_scenario"]
    executable = policy == EXECUTABLE
    if policy not in (MANDATE, EXECUTABLE):
        raise ValueError(f"unknown policy {policy!r}")
    return RiskLevel(
        name=name, policy=policy, approved_risk_per_trade=row["risk_per_trade"],
        risk_per_trade=min(row["risk_per_trade"], ex["risk_per_trade_ceiling"]) if executable else row["risk_per_trade"],
        max_open_risk=row["max_open_risk"], max_positions=row["max_positions"], leverage=row["leverage"],
        daily_pause=row["daily_pause"], halve_drawdown=row["halve_drawdown"], stop_drawdown=row["stop_drawdown"],
        max_entry_stop_distance=ex["max_entry_stop_distance"] if executable else None)


@dataclass(frozen=True)
class Candidate:
    """A universe member on the decision day."""

    symbol: str
    strength: float
    stop_distance: float          # d; NaN when undefined
    volume: float                 # 30-day median quote volume (the universe ranking value)
    overlay_factor: float = 1.0   # funding overlay (secondary test); 1.0 in the registered strategy


@dataclass
class Decision:
    targets: Dict[str, float]                       # symbol -> target notional (> 0 only)
    stop_distance: Dict[str, float]                 # d of every targeted symbol
    strength: Dict[str, float]
    rejected: List[Dict[str, str]] = field(default_factory=list)
    scale: Dict[str, float] = field(default_factory=dict)
    qualified: int = 0
    open_risk: float = 0.0
    gross_notional: float = 0.0


def decide_targets(*, equity: float, candidates: Sequence[Candidate], held: Mapping[str, float], level: RiskLevel,
                   halve: bool = False, halted: bool = False, paused: bool = False) -> Decision:
    """``held`` maps a symbol to the notional currently held (at the decision close)."""
    if halted or equity <= 0:
        return Decision({}, {}, {}, scale={"halted": 1.0 if halted else 0.0})
    qualified = [c for c in candidates if c.strength > 0 and math.isfinite(c.stop_distance) and c.stop_distance > 0]
    ranked = sorted(qualified, key=lambda c: (-c.strength, -c.volume, c.symbol))
    selected, rejected = ranked[:level.max_positions], []
    rejected += [{"symbol": c.symbol, "reason": REJECT_POSITION_LIMIT} for c in ranked[level.max_positions:]]
    raw: Dict[str, float] = {}
    for c in selected:
        is_new = held.get(c.symbol, 0.0) <= 0
        if is_new and paused:
            rejected.append({"symbol": c.symbol, "reason": REJECT_DAILY_PAUSE})
            continue
        target = c.strength * level.risk_per_trade * equity / c.stop_distance * c.overlay_factor
        if level.max_entry_stop_distance is not None and c.stop_distance > level.max_entry_stop_distance:
            rejected.append({"symbol": c.symbol, "reason": REJECT_STOP_DISTANCE})
            if is_new:
                continue
            target = min(target, held[c.symbol])        # a position already held is kept, never added to
        if target > 0:
            raw[c.symbol] = target
    d = {c.symbol: c.stop_distance for c in selected}
    scale = {"halve": 0.5 if halve else 1.0}
    targets = {s: v * scale["halve"] for s, v in raw.items()}
    open_risk = sum(v * d[s] for s, v in targets.items())
    limit = level.max_open_risk * equity
    scale["open_risk"] = min(1.0, limit / open_risk) if open_risk > 0 else 1.0
    targets = {s: v * scale["open_risk"] for s, v in targets.items()}
    gross = sum(targets.values())
    cap = level.leverage * equity
    scale["leverage"] = min(1.0, cap / gross) if gross > 0 else 1.0
    targets = {s: v * scale["leverage"] for s, v in targets.items()}
    return Decision(targets=targets, stop_distance={s: d[s] for s in targets},
                    strength={c.symbol: c.strength for c in selected if c.symbol in targets}, rejected=rejected,
                    scale=scale, qualified=len(qualified), open_risk=sum(v * d[s] for s, v in targets.items()),
                    gross_notional=sum(targets.values()))


@dataclass(frozen=True)
class Order:
    symbol: str
    kind: str                 # ENTRY | EXIT | ADJUST
    quantity: float           # signed change in contracts; filled at the next open
    target_notional: float
    stop_distance: Optional[float]      # d at the decision (sets the stop of a new position)


def orders_from_targets(decision: Decision, *, held_quantity: Mapping[str, float], close: Mapping[str, float],
                        band: float = SPECIFICATION["rebalance_band"]) -> List[Order]:
    """A position changes only on entry, on exit, or when the target differs from the holding by more than
    ``band`` of the target. Quantities are sized at the decision close."""
    orders = []
    for symbol in sorted(set(held_quantity) | set(decision.targets)):
        qty = held_quantity.get(symbol, 0.0)
        target = decision.targets.get(symbol, 0.0)
        price = close.get(symbol)
        d = decision.stop_distance.get(symbol)
        if qty <= 0 and target > 0:
            orders.append(Order(symbol, ENTRY, target / price, target, d))
        elif qty > 0 and target <= 0:
            orders.append(Order(symbol, EXIT, -qty, 0.0, None))
        elif qty > 0 and target > 0 and abs(target - qty * price) > band * target:
            orders.append(Order(symbol, ADJUST, target / price - qty, target, d))
    return orders


def apply_exchange_filters(order: Order, *, price: float, step: Optional[float], min_notional: Optional[float]):
    """``(quantity, rejection reason or None)``. Quantities are rounded DOWN to the step. A full exit is never
    blocked by the minimum notional (the exchange exempts a reduce-only close)."""
    qty = order.quantity
    if order.kind == EXIT:
        return qty, None
    if step and step > 0:
        qty = math.copysign(math.floor(abs(qty) / step + 1e-9) * step, qty)
    if qty == 0:
        return 0.0, REJECT_ZERO_QUANTITY
    if min_notional and abs(qty) * price < min_notional:
        return 0.0, REJECT_MIN_NOTIONAL
    return qty, None


__all__ = ["MANDATE", "EXECUTABLE", "ENTRY", "EXIT", "ADJUST", "RiskLevel", "risk_level", "Candidate", "Decision",
           "Order", "decide_targets", "orders_from_targets", "apply_exchange_filters", "REJECT_STOP_DISTANCE",
           "REJECT_DAILY_PAUSE", "REJECT_MIN_NOTIONAL", "REJECT_ZERO_QUANTITY", "REJECT_NO_BAR",
           "REJECT_POSITION_LIMIT"]
