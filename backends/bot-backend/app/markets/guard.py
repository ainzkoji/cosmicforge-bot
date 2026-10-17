"""Market availability guard (Step 7.2): why an order for a market may not be submitted now.

``MarketAvailabilityGuard.refuse_reason(market_id, ts_ms, contract=None, last_bar_open_ms=None)`` returns
``None`` (nothing in the market's calendar, contract or data refuses it) or exactly one reason code, checked
in this order:

1. ``CALENDAR_UNVERIFIED`` -- the market's calendar is not VERIFIED (a continuous crypto calendar under the
   PLATFORM_CONVENTION status is accepted: that is the platform's existing behaviour), or the instant is
   outside the period its holiday data covers. An unverified calendar is never trusted to say "open".
2. ``CONTRACT_NOT_LISTED`` / ``CONTRACT_EXPIRED`` -- the contract is not yet listed or past its last trading
   instant / expiry. Checked before the calendar so an expired contract is refused for that reason, not
   for an incidental closure.
3. ``MARKET_CLOSED`` -- outside every session, inside a holiday or a maintenance window.
4. ``DATA_STALE`` -- more completed bars are missing since the last observed bar than the market's freshness
   expectation allows. Closed periods are not counted: a weekend gap is not staleness.

The guard only refuses; it never authorizes anything. A ``None`` answer still leaves every other gate of the
platform (certification, governance stage, risk, adapters) in force.
"""
from __future__ import annotations

from typing import Any, Dict, Optional

from .calendar import PLATFORM_CONVENTION, TIMEFRAME_MS, VERIFIED, CalendarCoverageError, CalendarDefinitionError
from .contracts import ContractSpec
from .registry import MarketRegistry, MarketRegistryError, default_registry
from .roll import tradability_refusal

MARKET_CLOSED = "MARKET_CLOSED"
CONTRACT_EXPIRED = "CONTRACT_EXPIRED"
CONTRACT_NOT_LISTED = "CONTRACT_NOT_LISTED"
CALENDAR_UNVERIFIED = "CALENDAR_UNVERIFIED"
DATA_STALE = "DATA_STALE"
MARKET_UNKNOWN = "MARKET_UNKNOWN"
REFUSAL_CODES = (MARKET_CLOSED, CONTRACT_EXPIRED, CONTRACT_NOT_LISTED, CALENDAR_UNVERIFIED, DATA_STALE, MARKET_UNKNOWN)


class MarketAvailabilityGuard:
    def __init__(self, registry: Optional[MarketRegistry] = None) -> None:
        self._registry = registry

    @property
    def registry(self) -> MarketRegistry:
        return self._registry if self._registry is not None else default_registry()

    def calendar_trusted(self, market_id: str) -> bool:
        market = self.registry.market(market_id)
        calendar = self.registry.calendar(market.calendar_id)
        if calendar.verification_status == VERIFIED:
            return True
        return (calendar.verification_status == PLATFORM_CONVENTION and calendar.is_continuous
                and not market.is_tradfi)

    def refuse_reason(self, market_id: str, ts_ms: int, contract: Optional[ContractSpec] = None, *,
                      last_bar_open_ms: Optional[int] = None, check_data: bool = False) -> Optional[str]:
        """``check_data``: also require fresh data (``last_bar_open_ms`` is the open time of the newest bar the
        caller holds; None with ``check_data`` is stale)."""
        return self.evaluate(market_id, ts_ms, contract, last_bar_open_ms=last_bar_open_ms,
                             check_data=check_data)["reason"]

    def evaluate(self, market_id: str, ts_ms: int, contract: Optional[ContractSpec] = None, *,
                 last_bar_open_ms: Optional[int] = None, check_data: bool = False) -> Dict[str, Any]:
        ts_ms = int(ts_ms)
        try:
            market = self.registry.market(market_id)
            calendar = self.registry.calendar(market.calendar_id)
        except MarketRegistryError:
            return {"reason": MARKET_UNKNOWN, "market_id": market_id}
        out: Dict[str, Any] = {"market_id": market_id, "calendar_id": calendar.calendar_id,
                               "calendar_verification_status": calendar.verification_status, "ts_ms": ts_ms}
        if not self.calendar_trusted(market_id):
            return {**out, "reason": CALENDAR_UNVERIFIED}
        if contract is not None:
            refusal = tradability_refusal(contract, ts_ms)
            if refusal:
                return {**out, "reason": refusal, "contract": contract.symbol}
        try:
            if not calendar.is_open(ts_ms):
                nxt = None
                try:
                    nxt = calendar.next_open(ts_ms)
                except CalendarCoverageError:
                    pass
                return {**out, "reason": MARKET_CLOSED, "next_open_ms": nxt}
            if check_data:
                stale = self._stale(market, calendar, ts_ms, last_bar_open_ms)
                if stale is not None:
                    return {**out, "reason": DATA_STALE, **stale}
        except (CalendarCoverageError, CalendarDefinitionError) as exc:
            return {**out, "reason": CALENDAR_UNVERIFIED, "detail": type(exc).__name__}
        return {**out, "reason": None}

    @staticmethod
    def _stale(market, calendar, ts_ms: int, last_bar_open_ms: Optional[int]) -> Optional[Dict[str, Any]]:
        timeframe = market.freshness.timeframe
        step = TIMEFRAME_MS[timeframe]
        if last_bar_open_ms is None:
            return {"missing_bars": None, "max_missing_bars": market.freshness.max_missing_bars}
        # completed bars the calendar expected after the newest one held
        expected = calendar.expected_bars(int(last_bar_open_ms) + 1, ts_ms, timeframe)
        if timeframe == "1d":
            sessions = {s.open_ms: s for s in calendar.sessions_between(int(last_bar_open_ms) + 1, ts_ms)}
            completed = [t for t in expected if t in sessions and sessions[t].close_ms <= ts_ms]
        else:
            completed = [t for t in expected if t + step <= ts_ms]
        if len(completed) > market.freshness.max_missing_bars:
            return {"missing_bars": len(completed), "max_missing_bars": market.freshness.max_missing_bars}
        return None


__all__ = ["MARKET_CLOSED", "CONTRACT_EXPIRED", "CONTRACT_NOT_LISTED", "CALENDAR_UNVERIFIED", "DATA_STALE",
           "MARKET_UNKNOWN", "REFUSAL_CODES", "MarketAvailabilityGuard"]
