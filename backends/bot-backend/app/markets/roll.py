"""Deterministic contract rolls (Step 7.2).

A ``ContractSeries`` is the dated contracts of one root on one venue, with one roll rule. Given the series and
its calendar, the roll schedule is a pure function: the same data always gives the same roll events, and the
contract that may be traded at an instant is the one whose holding window contains it.

Roll rules (``RollRule.kind``):

* ``SESSIONS_BEFORE_LAST_TRADE`` / ``SESSIONS_BEFORE_FIRST_NOTICE``: roll at the open of the session ``n``
  trading sessions before the reference session (the session containing the last-trade / first-notice instant,
  or the last session before it). Counting sessions, not calendar days, makes the rule independent of weekends
  and holidays -- and requires a calendar that covers the dates.
* ``AT_LAST_TRADE``: hold until the last trading instant (no early roll).

Which rule a family uses, and ``n``, are research decisions (see ``research/trend_v2_tradfi/SPEC.md`` D6);
nothing here chooses one.

Refusals: a contract is never traded before it is listed (``CONTRACT_NOT_LISTED``) or at / after its last
trading instant or expiry (``CONTRACT_EXPIRED``).
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Tuple

from .calendar import MarketCalendar
from .contracts import ContractSpec

SESSIONS_BEFORE_LAST_TRADE = "SESSIONS_BEFORE_LAST_TRADE"
SESSIONS_BEFORE_FIRST_NOTICE = "SESSIONS_BEFORE_FIRST_NOTICE"
AT_LAST_TRADE = "AT_LAST_TRADE"
ROLL_KINDS = (SESSIONS_BEFORE_LAST_TRADE, SESSIONS_BEFORE_FIRST_NOTICE, AT_LAST_TRADE)

CONTRACT_EXPIRED = "CONTRACT_EXPIRED"
CONTRACT_NOT_LISTED = "CONTRACT_NOT_LISTED"
NO_ACTIVE_CONTRACT = "NO_ACTIVE_CONTRACT"


class RollDefinitionError(ValueError):
    pass


@dataclass(frozen=True)
class RollRule:
    kind: str
    sessions: int = 0

    def __post_init__(self) -> None:
        if self.kind not in ROLL_KINDS:
            raise RollDefinitionError(f"roll rule must be one of {ROLL_KINDS}")
        if self.kind != AT_LAST_TRADE and (not isinstance(self.sessions, int) or self.sessions < 0):
            raise RollDefinitionError("a session-count roll rule needs sessions >= 0")


@dataclass(frozen=True)
class RollEvent:
    roll_ms: int
    from_symbol: str
    to_symbol: str

    def to_plain(self) -> Dict[str, object]:
        return {"roll_ms": self.roll_ms, "from_symbol": self.from_symbol, "to_symbol": self.to_symbol}


@dataclass(frozen=True)
class ContractSeries:
    root: str
    venue: str
    rule: RollRule
    contracts: Tuple[ContractSpec, ...]

    def __post_init__(self) -> None:
        if not self.contracts:
            raise RollDefinitionError(f"{self.root}: a series has contracts")
        for c in self.contracts:
            if not c.is_dated:
                raise RollDefinitionError(f"{self.root}: {c.symbol} is not a dated contract")
            if c.venue != self.venue:
                raise RollDefinitionError(f"{self.root}: {c.symbol} is on {c.venue}, not {self.venue}")
            if self.rule.kind == SESSIONS_BEFORE_FIRST_NOTICE and c.first_notice_ms is None:
                raise RollDefinitionError(f"{self.root}: {c.symbol} has no first notice instant for the roll rule")
        ends = [c.expiry_or_last_trade for c in self.ordered]
        if len(set(ends)) != len(ends):
            raise RollDefinitionError(f"{self.root}: two contracts expire at the same instant")

    @property
    def ordered(self) -> List[ContractSpec]:
        return sorted(self.contracts, key=lambda c: c.expiry_or_last_trade)


def roll_instant(contract: ContractSpec, rule: RollRule, calendar: MarketCalendar) -> int:
    """When holding of ``contract`` ends under ``rule``."""
    last = contract.expiry_or_last_trade
    if rule.kind == AT_LAST_TRADE:
        return last
    reference = contract.first_notice_ms if rule.kind == SESSIONS_BEFORE_FIRST_NOTICE else last
    # sessions up to the reference instant: the reference session is the one containing it or the last one before
    days = rule.sessions * 2 + 7
    while True:
        lookback_ms = days * 86_400_000
        sessions = [s for s in calendar.sessions_between(reference - lookback_ms, reference + 1)
                    if s.open_ms <= reference]
        if len(sessions) > rule.sessions or days >= 400:
            break
        days *= 2
    if len(sessions) <= rule.sessions:
        raise RollDefinitionError(f"{contract.symbol}: the calendar has fewer than {rule.sessions + 1} sessions "
                                  "before the reference instant")
    roll = sessions[-1 - rule.sessions].open_ms
    return min(roll, last)


def roll_schedule(series: ContractSeries, calendar: MarketCalendar) -> List[RollEvent]:
    """Every roll of the series, in order: from each contract to the next one."""
    ordered = series.ordered
    return [RollEvent(roll_instant(a, series.rule, calendar), a.symbol, b.symbol) for a, b in zip(ordered, ordered[1:])]


def holding_windows(series: ContractSeries, calendar: MarketCalendar) -> List[Tuple[ContractSpec, int, int]]:
    """(contract, start_ms, end_ms): the half-open window in which each contract is the one held."""
    out: List[Tuple[ContractSpec, int, int]] = []
    previous_end: Optional[int] = None
    for c in series.ordered:
        end = roll_instant(c, series.rule, calendar)
        start = previous_end if previous_end is not None else (c.listing_ms if c.listing_ms is not None else -(2 ** 62))
        if c.listing_ms is not None:
            start = max(start, c.listing_ms)
        if end > start:
            out.append((c, start, end))
        previous_end = end
    return out


def active_contract(series: ContractSeries, ts_ms: int, calendar: MarketCalendar) -> Optional[ContractSpec]:
    """The contract to hold at ``ts_ms``; None before the first listing or after the last roll."""
    for c, start, end in holding_windows(series, calendar):
        if start <= int(ts_ms) < end:
            return c
    return None


def tradability_refusal(contract: ContractSpec, ts_ms: int) -> Optional[str]:
    """Why ``contract`` may not be traded at ``ts_ms`` at all (independent of any roll rule)."""
    if not contract.listed_at(ts_ms):
        return CONTRACT_NOT_LISTED
    if contract.expired_at(ts_ms):
        return CONTRACT_EXPIRED
    return None


def series_from_contracts(root: str, venue: str, rule: RollRule, contracts: Sequence[ContractSpec]) -> ContractSeries:
    return ContractSeries(root=root, venue=venue, rule=rule, contracts=tuple(contracts))


__all__ = ["SESSIONS_BEFORE_LAST_TRADE", "SESSIONS_BEFORE_FIRST_NOTICE", "AT_LAST_TRADE", "ROLL_KINDS",
           "CONTRACT_EXPIRED", "CONTRACT_NOT_LISTED", "NO_ACTIVE_CONTRACT", "RollDefinitionError", "RollRule",
           "RollEvent", "ContractSeries", "roll_instant", "roll_schedule", "holding_windows", "active_contract",
           "tradability_refusal", "series_from_contracts"]
