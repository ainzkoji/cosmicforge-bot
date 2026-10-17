"""Contract economics in ``Decimal`` (Step 7.2).

Broker-neutral arithmetic over a ``ContractSpec``. No float, no rounding except where a rule says so, and no
value assumed: a financing rate, a funding rate, an FX conversion rate or a dividend comes from the caller's
data, never from a default.

* notional = price x quantity x multiplier (in the settlement currency);
* PnL = (exit - entry) x quantity x multiplier x side (in the settlement currency), converted to the account
  currency only with an explicit rate;
* funding (crypto perpetual): -side x notional x rate per funding event (a long pays a positive rate);
* overnight financing (FX / CFD): -notional x annual rate / day basis per night held, with the documented triple
  night when the model names one;
* futures carry is inside the price and is realised by the roll: no separate accrual;
* dividends: a long receives, a short pays, quantity x multiplier x amount per unit;
* splits: quantity x ratio, price / ratio, value unchanged.
"""
from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal
from typing import Iterable, Optional, Tuple

from .contracts import (CARRY_FUNDING_RATE, CARRY_FUTURES_BASIS, CARRY_NONE, CARRY_OVERNIGHT_FINANCING, DIVIDEND,
                        SPLIT, ContractSpec, CorporateAction)

LONG, SHORT = 1, -1


def _side(side: int) -> int:
    if side not in (LONG, SHORT):
        raise ValueError("side is +1 (long) or -1 (short)")
    return side


def _d(value) -> Decimal:
    if isinstance(value, float):
        raise TypeError("economics takes Decimal or decimal strings, never float")
    return Decimal(str(value)) if not isinstance(value, Decimal) else value


def notional(spec: ContractSpec, price, quantity) -> Decimal:
    return _d(price) * _d(quantity) * spec.multiplier


def pnl(spec: ContractSpec, *, entry_price, exit_price, quantity, side: int) -> Decimal:
    """Profit (positive) or loss in the settlement currency."""
    return (_d(exit_price) - _d(entry_price)) * _d(quantity) * spec.multiplier * _side(side)


def ticks_to_money(spec: ContractSpec, ticks, quantity) -> Decimal:
    return _d(ticks) * spec.tick_value * _d(quantity)


def convert(amount, rate) -> Decimal:
    """Settlement currency -> account currency at an explicit rate (account units per settlement unit)."""
    rate = _d(rate)
    if rate <= 0:
        raise ValueError("a conversion rate is positive")
    return _d(amount) * rate


def funding_accrual(spec: ContractSpec, *, side: int, events: Iterable[Tuple[object, object]]) -> Decimal:
    """Sum over funding events of (position notional at the event, funding rate): a long pays a positive rate,
    a short receives it. Returns the cash flow to the position (negative = paid)."""
    if spec.carry.kind != CARRY_FUNDING_RATE:
        raise ValueError(f"{spec.symbol}: carry model is {spec.carry.kind}, not {CARRY_FUNDING_RATE}")
    s = _side(side)
    return sum((-s * abs(_d(n)) * _d(rate) for n, rate in events), Decimal("0"))


def financing_nights(start: date, end: date, *, triple_weekday: Optional[int]) -> int:
    """Nights charged for holding over the local dates in [start, end).

    Without a triple weekday every date counts one night. With one (0 = Monday), the weekend is charged on that
    weekday: it counts three, Saturday and Sunday count none, other dates one."""
    nights, day = 0, start
    while day < end:
        if triple_weekday is None:
            nights += 1
        elif day.weekday() == triple_weekday:
            nights += 3
        elif day.weekday() not in (5, 6):
            nights += 1
        day += timedelta(days=1)
    return nights


def financing_accrual(spec: ContractSpec, *, side: int, notional_value, nights: int) -> Decimal:
    """Overnight financing cash flow to the position (negative = paid) for ``nights`` nights."""
    carry = spec.carry
    if carry.kind in (CARRY_NONE, CARRY_FUTURES_BASIS):
        return Decimal("0")
    if carry.kind != CARRY_OVERNIGHT_FINANCING:
        raise ValueError(f"{spec.symbol}: use funding_accrual for {carry.kind}")
    rate = carry.long_rate if _side(side) == LONG else carry.short_rate
    return -abs(_d(notional_value)) * rate * Decimal(int(nights)) / Decimal(carry.day_count)


def dividend_adjustment(spec: ContractSpec, action: CorporateAction, *, quantity, side: int) -> Decimal:
    """Cash to the position on the ex-date: a long receives, a short pays."""
    if action.kind != DIVIDEND:
        raise ValueError("not a dividend")
    if action.currency != spec.settlement_currency:
        raise ValueError(f"dividend in {action.currency}, contract settles in {spec.settlement_currency}: "
                         "convert explicitly first")
    return _d(quantity) * spec.multiplier * action.amount * _side(side)


def split_adjustment(action: CorporateAction, *, quantity, price) -> Tuple[Decimal, Decimal]:
    """(new quantity, new price) after a split; quantity x price is unchanged."""
    if action.kind != SPLIT:
        raise ValueError("not a split")
    return _d(quantity) * action.ratio, _d(price) / action.ratio


def back_adjust_price(action: CorporateAction, price) -> Decimal:
    """A price recorded before a split's effective instant, expressed in post-split units (history made
    comparable; the record itself is never rewritten)."""
    if action.kind != SPLIT:
        raise ValueError("not a split")
    return _d(price) / action.ratio


__all__ = ["LONG", "SHORT", "notional", "pnl", "ticks_to_money", "convert", "funding_accrual", "financing_nights",
           "financing_accrual", "dividend_adjustment", "split_adjustment", "back_adjust_price"]
