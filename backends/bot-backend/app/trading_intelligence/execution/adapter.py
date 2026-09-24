"""Broker-neutral ExecutionAdapter protocol (Sections 20.12-20.13).

CATI contracts stay broker-neutral: an adapter speaks canonical sides
(LONG/SHORT), notional, prices and fractions; venue symbols, order-type
strings, clientOrderId formats, reduce-only flags etc. live INSIDE the
adapter. Binance implements this by WRAPPING the existing executor
(``binance_adapter.py``) -- working execution is not rewritten.

Execution support is declared per adapter and is DISTINCT from Section 17
economic-adapter status. ``UnvalidatedExecutionAdapter`` is the fail-closed
default for every venue without a passing contract suite: every method
raises ``ExecutionNotSupported``.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Mapping, Optional, Protocol, Tuple, runtime_checkable

from app.trading_intelligence.contracts.execution import SUPPORT_RANK, ExecutionSupportStatus
from app.trading_intelligence.versions import EXECUTION_ADAPTER_PROTOCOL_VERSION


class ExecutionNotSupported(RuntimeError):
    """The venue has no validated execution adapter (fail closed)."""


class ProtectionWideningRefused(RuntimeError):
    """A protection modification would increase risk beyond the existing stop (P7)."""


@dataclass(frozen=True)
class EntryRequest:
    trade_plan_id: str
    trade_plan_hash: str
    risk_decision_id: str
    venue_symbol: str
    side: str  # LONG | SHORT
    notional: float
    stop_price: float
    target_price: Optional[float]
    leverage: float
    requested_order_type: str
    requested_price: float
    max_slippage_bps: float
    current_open_count: int
    current_equity: float
    cycle_id: Optional[str]
    #: execution identity: plan id + plan hash (Section 20.9)
    intent_identity: str


@dataclass(frozen=True)
class EntryResult:
    status: str  # ExecutionAttemptStatus
    raw_status: Optional[str]
    rejection_family: Optional[str] = None
    broker_order_id: Optional[str] = None
    client_order_id: Optional[str] = None
    requested_qty: Optional[float] = None
    submitted_qty: Optional[float] = None
    filled_qty: Optional[float] = None
    avg_fill_price: Optional[float] = None
    submitted_price: Optional[float] = None
    actual_order_type: Optional[str] = None
    fees: Optional[float] = None
    leverage: Optional[float] = None
    protection: Mapping[str, Any] = field(default_factory=dict)
    capital: Mapping[str, Any] = field(default_factory=dict)
    slot_reservation_id: Optional[str] = None
    margin_reservation_id: Optional[str] = None
    reason_codes: Tuple[str, ...] = ()
    detail: str = ""


@dataclass(frozen=True)
class OrderState:
    broker_order_id: Optional[str]
    client_order_id: Optional[str]
    status: Optional[str]
    executed_qty: float
    avg_price: float
    answered: bool


@dataclass(frozen=True)
class BrokerPositionState:
    venue_symbol: str
    side: str  # LONG | SHORT | FLAT
    quantity: float
    entry_price: Optional[float]
    answered: bool


@runtime_checkable
class ExecutionAdapter(Protocol):
    adapter_id: str
    venue: str
    execution_support_status: str
    protocol_version: str

    def submit_entry(self, request: EntryRequest) -> EntryResult: ...
    def query_order(self, venue_symbol: str, *, broker_order_id: Optional[str] = None,
                    client_order_id: Optional[str] = None) -> OrderState: ...
    def cancel_order(self, venue_symbol: str, broker_order_id: str) -> bool: ...
    def submit_protection(self, venue_symbol: str, *, side: str, quantity: float, stop_price: float,
                          target_price: Optional[float]) -> Dict[str, Any]: ...
    def modify_protection(self, venue_symbol: str, *, side: str, quantity: float, existing_stop: float,
                          new_stop: float, **kwargs: Any) -> Dict[str, Any]: ...
    def submit_reduce(self, venue_symbol: str, *, side: str, fraction: float, **kwargs: Any) -> Dict[str, Any]: ...
    def submit_exit(self, venue_symbol: str, *, side: str, quantity: Optional[float] = None) -> Dict[str, Any]: ...
    def resolve_fill(self, venue_symbol: str, *, order_response: Any, client_order_id: Optional[str] = None) -> Any: ...
    def reconcile_position(self, venue_symbol: str) -> BrokerPositionState: ...


def assert_not_widening(side: str, existing_stop: float, new_stop: float) -> None:
    """P7: LONG new >= existing, SHORT new <= existing. Equal is allowed
    (no change); anything wider is refused before any broker call."""
    if side == "LONG" and float(new_stop) < float(existing_stop):
        raise ProtectionWideningRefused(f"LONG stop {new_stop} is below existing {existing_stop}")
    if side == "SHORT" and float(new_stop) > float(existing_stop):
        raise ProtectionWideningRefused(f"SHORT stop {new_stop} is above existing {existing_stop}")
    if side not in ("LONG", "SHORT"):
        raise ProtectionWideningRefused(f"unknown side {side!r}")


class UnvalidatedExecutionAdapter:
    """Fail-closed stand-in for any venue without a passing contract suite."""

    execution_support_status = ExecutionSupportStatus.UNVALIDATED.value
    protocol_version = EXECUTION_ADAPTER_PROTOCOL_VERSION

    def __init__(self, venue: str) -> None:
        self.venue = venue
        self.adapter_id = f"unvalidated:{venue}"

    def _refuse(self, *_a, **_k):
        raise ExecutionNotSupported(f"{self.venue}: execution adapter UNVALIDATED -- no CATI execution")

    submit_entry = query_order = cancel_order = submit_protection = modify_protection = _refuse
    submit_reduce = submit_exit = resolve_fill = reconcile_position = _refuse


#: minimum execution-support status per environment while CATI is NOT promoted
MIN_SUPPORT_BY_ENVIRONMENT: Mapping[str, str] = {
    "PAPER": ExecutionSupportStatus.CONTRACT_VALIDATED.value,
    "TESTNET": ExecutionSupportStatus.CONTRACT_VALIDATED.value,
    "DEMO": ExecutionSupportStatus.CONTRACT_VALIDATED.value,
    "LIVE": ExecutionSupportStatus.PRODUCTION_VALIDATED.value,
    "PRODUCTION": ExecutionSupportStatus.PRODUCTION_VALIDATED.value,
}


def adapter_supports(adapter: Any, environment: str) -> bool:
    required = MIN_SUPPORT_BY_ENVIRONMENT.get(str(environment).upper(), ExecutionSupportStatus.PRODUCTION_VALIDATED.value)
    have = getattr(adapter, "execution_support_status", ExecutionSupportStatus.UNVALIDATED.value)
    return SUPPORT_RANK.get(have, 0) >= SUPPORT_RANK[required]


class ExecutionAdapterRegistry:
    """venue -> adapter. An unregistered venue resolves to the fail-closed
    ``UnvalidatedExecutionAdapter``; there is no silent default."""

    def __init__(self) -> None:
        self._adapters: Dict[str, Any] = {}

    def register(self, adapter: Any) -> None:
        if not isinstance(adapter, ExecutionAdapter):
            raise TypeError("adapter does not implement the ExecutionAdapter protocol")
        self._adapters[adapter.venue.upper()] = adapter

    def resolve(self, venue: str) -> Any:
        return self._adapters.get(str(venue).upper()) or UnvalidatedExecutionAdapter(venue)


__all__ = ["ExecutionNotSupported", "ProtectionWideningRefused", "EntryRequest", "EntryResult", "OrderState",
           "BrokerPositionState", "ExecutionAdapter", "assert_not_widening", "UnvalidatedExecutionAdapter",
           "MIN_SUPPORT_BY_ENVIRONMENT", "adapter_supports", "ExecutionAdapterRegistry"]
