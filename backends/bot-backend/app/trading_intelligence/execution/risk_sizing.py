"""Risk-based position sizing (Step 1.4) -- the ONE formula preview and execution share.

    budget_usdt       = fixed budget, or account equity x budget percentage
    risk_usdt         = budget_usdt x risk_per_trade_fraction x signal_strength
    raw_notional_usdt = risk_usdt / stop_distance_fraction

A full-strength position with a correctly filled stop then loses ``risk_usdt``
before fees and slippage. Every cap below can only make the position SMALLER;
nothing here ever raises an order to satisfy an exchange minimum -- a size the
exchange will not accept is a blocker (``RISK_SIZE_BELOW_EXCHANGE_MINIMUM``).
Leverage is an OUTPUT of safe sizing (the margin a notional needs under the
resolved leverage), never a multiplier the customer picks.

The result feeds the existing execution authority unchanged: the orchestrator
hands the sized margin to hard risk (Layer A/B/C), the preflight re-checks the
quantity against current venue metadata and the capital ledger applies its own
caps -- all of them can still only shrink or reject. All arithmetic is Decimal.

Order of application (the validated order of the existing chain, caps as
notional ceilings):

1. risk-derived notional (authoritative ceiling)
2. combined open-position risk (profile ``max_open_risk``)
3. leverage: min(resolved leverage, profile ceiling); liquidation safety is
   already inside the resolved leverage (``resolve_cati_leverage``)
4. user maximum position (notional)
5. available margin (free balance less a fill buffer)
6. system-wide notional limit
7. exchange quantity step / minimum quantity / maximum quantity
8. exchange minimum notional (block, never inflate)
"""
from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal, ROUND_DOWN, InvalidOperation
from typing import Any, Dict, Mapping, Optional

ZERO = Decimal("0")
ONE = Decimal("1")
HUNDRED = Decimal("100")
#: Headroom kept between the sized margin and the free balance so a market fill
#: a few bps away from the reference cannot knowingly exceed the balance.
MARGIN_FILL_BUFFER = Decimal("0.0015")
#: Widest stop distance the sizing accepts (fraction); mirrors SystemLimits.max_stop_loss_pct.
MAX_STOP_DISTANCE = Decimal("0.15")
#: Tightest stop distance the sizing accepts (fraction); below it a size is noise, not a position.
MIN_STOP_DISTANCE = Decimal("0.0005")

RISK_SIZE_BELOW_EXCHANGE_MINIMUM = "RISK_SIZE_BELOW_EXCHANGE_MINIMUM"
RISK_SIZE_INVALID_INPUT = "RISK_SIZE_INVALID_INPUT"
RISK_SIZE_STOP_DISTANCE_INVALID = "RISK_SIZE_STOP_DISTANCE_INVALID"
RISK_SIZE_OPEN_RISK_CAP_REACHED = "RISK_SIZE_OPEN_RISK_CAP_REACHED"
RISK_SIZE_NO_LEGAL_LEVERAGE = "RISK_SIZE_NO_LEGAL_LEVERAGE"
RISK_SIZE_INSUFFICIENT_MARGIN = "RISK_SIZE_INSUFFICIENT_MARGIN"
RISK_SIZE_ZERO = "RISK_SIZE_ZERO"


def _dec(value: Any, what: str) -> Decimal:
    if value is None:
        raise ValueError(f"{RISK_SIZE_INVALID_INPUT}:{what}")
    if isinstance(value, float):
        value = repr(value)
    try:
        out = Decimal(str(value).strip())
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"{RISK_SIZE_INVALID_INPUT}:{what}") from exc
    if not out.is_finite():
        raise ValueError(f"{RISK_SIZE_INVALID_INPUT}:{what}")
    return out


def _opt(value: Any, what: str) -> Optional[Decimal]:
    return None if value in (None, "", 0, "0", 0.0) else _dec(value, what)


@dataclass(frozen=True)
class RiskSizing:
    approved: bool
    reason: Optional[str]
    budget_usdt: Decimal
    risk_fraction: Decimal
    signal_strength: Decimal
    stop_distance: Decimal
    risk_usdt: Decimal
    raw_notional_usdt: Decimal
    notional_usdt: Decimal
    quantity: Decimal
    margin_usdt: Decimal
    leverage: int
    loss_at_stop_usdt: Decimal
    binding: Optional[str]
    caps: Dict[str, Optional[str]] = field(default_factory=dict)

    def evidence(self) -> dict:
        return {"approved": self.approved, "reason": self.reason, "budget_usdt": str(self.budget_usdt),
                "risk_fraction": str(self.risk_fraction), "signal_strength": str(self.signal_strength),
                "stop_distance": str(self.stop_distance), "risk_usdt": str(self.risk_usdt),
                "raw_notional_usdt": str(self.raw_notional_usdt), "notional_usdt": str(self.notional_usdt),
                "quantity": str(self.quantity), "margin_usdt": str(self.margin_usdt), "leverage": self.leverage,
                "loss_at_stop_usdt": str(self.loss_at_stop_usdt), "binding": self.binding, "caps": dict(self.caps),
                "sizing_method": "risk_based"}


def effective_budget(*, budget_type: str, budget_value: Any, account_equity: Any) -> Decimal:
    """``fixed_amount`` is the value itself; ``percent_balance`` is that percentage
    of the account equity (never of a client-supplied number)."""
    kind = str(budget_type or "").lower()
    value = _dec(budget_value, "budget_value")
    if value <= 0:
        raise ValueError(f"{RISK_SIZE_INVALID_INPUT}:budget_value")
    if kind in {"fixed_amount", "fixed"}:
        return value
    if kind in {"percent_balance", "percent_equity", "percent"}:
        if value > HUNDRED:
            raise ValueError(f"{RISK_SIZE_INVALID_INPUT}:budget_percent")
        equity = _dec(account_equity, "account_equity")
        if equity <= 0:
            raise ValueError(f"{RISK_SIZE_INVALID_INPUT}:account_equity")
        return equity * value / HUNDRED
    raise ValueError(f"{RISK_SIZE_INVALID_INPUT}:budget_type")


def size_risk_based(*, budget_usdt: Any, risk_fraction: Any, stop_distance_fraction: Any, price: Any,
                    leverage: Any, leverage_ceiling: Any, signal_strength: Any = 1,
                    max_position_usdt: Any = None, free_margin_usdt: Any = None, open_risk_usdt: Any = 0,
                    max_open_risk_fraction: Any = None, system_max_notional_usdt: Any = None,
                    min_notional: Any = None, qty_step: Any = None, min_qty: Any = None, max_qty: Any = None,
                    contract_multiplier: Any = 1) -> RiskSizing:
    """Size one entry. Never raises for a sizing outcome: a blocked size is a
    ``RiskSizing`` with ``approved=False`` and a stable ``reason``; only
    malformed inputs raise ``ValueError(RISK_SIZE_INVALID_INPUT...)``."""
    budget = _dec(budget_usdt, "budget_usdt")
    risk = _dec(risk_fraction, "risk_fraction")
    strength = _dec(signal_strength, "signal_strength")
    px = _dec(price, "price")
    caps: Dict[str, Optional[str]] = {}

    def blocked(reason, stop=ZERO, risk_amount=ZERO, raw=ZERO, lev=0, binding=None):
        return RiskSizing(False, reason, budget, risk, strength, stop, risk_amount, raw, ZERO, ZERO, ZERO, int(lev),
                          ZERO, binding, caps)

    if budget <= 0 or risk <= 0 or risk > Decimal("0.05") or not (ZERO < strength <= ONE) or px <= 0:
        return blocked(RISK_SIZE_INVALID_INPUT)
    try:
        stop = _dec(stop_distance_fraction, "stop_distance")
    except ValueError:
        return blocked(RISK_SIZE_STOP_DISTANCE_INVALID)
    if not (MIN_STOP_DISTANCE <= stop <= MAX_STOP_DISTANCE):
        return blocked(RISK_SIZE_STOP_DISTANCE_INVALID, stop)

    # 1. the risk-derived size: the authoritative ceiling for everything below
    risk_usdt = budget * risk * strength
    raw_notional = risk_usdt / stop
    notional = raw_notional
    binding = "RISK_DERIVED"
    caps["RISK_DERIVED"] = str(raw_notional)

    # 2. combined open-position risk
    if max_open_risk_fraction is not None:
        max_open = budget * _dec(max_open_risk_fraction, "max_open_risk_fraction")
        remaining = max_open - _dec(open_risk_usdt, "open_risk_usdt")
        if remaining <= 0:
            return blocked(RISK_SIZE_OPEN_RISK_CAP_REACHED, stop, risk_usdt, raw_notional, binding="OPEN_RISK_CAP")
        cap = remaining / stop
        caps["OPEN_RISK_CAP"] = str(cap)
        if cap < notional:
            notional, binding = cap, "OPEN_RISK_CAP"

    # 3. leverage: the resolved leverage under the profile ceiling
    lev_resolved = _dec(leverage, "leverage")
    lev_ceiling = _dec(leverage_ceiling, "leverage_ceiling")
    lev = int(min(lev_resolved, lev_ceiling).to_integral_value(rounding=ROUND_DOWN))
    if lev < 1:
        return blocked(RISK_SIZE_NO_LEGAL_LEVERAGE, stop, risk_usdt, raw_notional, binding="LEVERAGE_CEILING")
    leverage_d = Decimal(lev)

    # 4. user maximum position (notional)
    max_pos = _opt(max_position_usdt, "max_position_usdt")
    if max_pos is not None:
        caps["USER_MAX_POSITION"] = str(max_pos)
        if max_pos < notional:
            notional, binding = max_pos, "USER_MAX_POSITION"

    # 5. available margin (less the fill buffer), expressed as a notional ceiling
    free = _opt(free_margin_usdt, "free_margin_usdt")
    if free_margin_usdt is not None:
        usable = (free or ZERO) * (ONE - MARGIN_FILL_BUFFER)
        cap = usable * leverage_d
        caps["AVAILABLE_MARGIN"] = str(cap)
        if cap <= 0:
            return blocked(RISK_SIZE_INSUFFICIENT_MARGIN, stop, risk_usdt, raw_notional, lev, "AVAILABLE_MARGIN")
        if cap < notional:
            notional, binding = cap, "AVAILABLE_MARGIN"

    # 6. system-wide notional limit
    system_cap = _opt(system_max_notional_usdt, "system_max_notional_usdt")
    if system_cap is not None:
        caps["SYSTEM_MAX_NOTIONAL"] = str(system_cap)
        if system_cap < notional:
            notional, binding = system_cap, "SYSTEM_MAX_NOTIONAL"

    # 7. exchange quantity filters: round DOWN to the step, respect min / max quantity
    mult = _dec(contract_multiplier, "contract_multiplier") if contract_multiplier not in (None, "") else ONE
    quantity = notional / (px * mult)
    step = _opt(qty_step, "qty_step")
    if step is not None:
        quantity = (quantity / step).to_integral_value(rounding=ROUND_DOWN) * step
    hi = _opt(max_qty, "max_qty")
    if hi is not None and quantity > hi:
        quantity = (hi / step).to_integral_value(rounding=ROUND_DOWN) * step if step is not None else hi
        caps["EXCHANGE_MAX_QTY"] = str(hi)
        binding = "EXCHANGE_MAX_QTY"
    lo = _opt(min_qty, "min_qty")
    if quantity <= 0 or (lo is not None and quantity < lo):
        return blocked(RISK_SIZE_BELOW_EXCHANGE_MINIMUM, stop, risk_usdt, raw_notional, lev, binding)
    final_notional = quantity * px * mult

    # 8. exchange minimum notional: blocked, never rounded up
    min_not = _opt(min_notional, "min_notional")
    if min_not is not None and final_notional < min_not:
        return blocked(RISK_SIZE_BELOW_EXCHANGE_MINIMUM, stop, risk_usdt, raw_notional, lev, binding)
    if final_notional <= 0:
        return blocked(RISK_SIZE_ZERO, stop, risk_usdt, raw_notional, lev, binding)

    margin = final_notional / leverage_d
    return RiskSizing(True, None, budget, risk, strength, stop, risk_usdt, raw_notional, final_notional, quantity,
                      margin, lev, final_notional * stop, binding, caps)


def preview_range(*, budget_usdt: Any, risk_fraction: Any, price: Any, leverage_ceiling: Any,
                  stop_distances: Mapping[str, Any], **filters: Any) -> Dict[str, dict]:
    """The same sizing at several assumed stop distances (deployment preview).
    ``stop_distances`` maps a label (``tight`` / ``typical`` / ``wide``) to a
    stop fraction; the result holds one ``RiskSizing.evidence()`` per label."""
    out = {}
    for label, stop in stop_distances.items():
        out[label] = size_risk_based(budget_usdt=budget_usdt, risk_fraction=risk_fraction, stop_distance_fraction=stop,
                                     price=price, leverage=leverage_ceiling, leverage_ceiling=leverage_ceiling,
                                     **filters).evidence()
    return out


__all__ = ["RiskSizing", "size_risk_based", "effective_budget", "preview_range", "MARGIN_FILL_BUFFER",
           "MAX_STOP_DISTANCE", "MIN_STOP_DISTANCE", "RISK_SIZE_BELOW_EXCHANGE_MINIMUM", "RISK_SIZE_INVALID_INPUT",
           "RISK_SIZE_STOP_DISTANCE_INVALID", "RISK_SIZE_OPEN_RISK_CAP_REACHED", "RISK_SIZE_NO_LEGAL_LEVERAGE",
           "RISK_SIZE_INSUFFICIENT_MARGIN", "RISK_SIZE_ZERO"]
