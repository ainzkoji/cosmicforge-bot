"""Contract specifications (Step 7.2): what one tradable instrument is, as data.

``ContractSpec`` describes a perpetual, a dated future, an FX pair, a CFD or a share in the same terms:
multiplier, tick size (tick value is derived, never stated separately), quantity step and minimum, margin rule,
listing / first-notice / last-trade / expiry instants, the carry model and corporate-action records. A dated
future belongs to a ``ContractSeries`` that names its roll rule (``roll``).

Every value is ``Decimal`` (or epoch milliseconds). Every spec names its ``source`` and its
``verification_status``; nothing here asserts that a specification matches any venue's published one unless
it is ``VERIFIED`` with a citation.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from decimal import ROUND_DOWN, ROUND_HALF_EVEN, ROUND_UP, Decimal, InvalidOperation
from typing import Any, Dict, Mapping, Optional, Tuple

from app.trading_intelligence.contracts.instrument import (CFD, CRYPTO, EQUITY, FUTURE, FUTURES, FX, PERPETUAL,
                                                           SPOT)

from .calendar import VERIFICATION_STATUSES

ASSET_CLASSES = (CRYPTO, FUTURES, FX, EQUITY)
CONTRACT_TYPES = (PERPETUAL, FUTURE, SPOT, CFD)

# margin rules
PERCENT_OF_NOTIONAL, FIXED_PER_CONTRACT = "PERCENT_OF_NOTIONAL", "FIXED_PER_CONTRACT"
MARGIN_KINDS = (PERCENT_OF_NOTIONAL, FIXED_PER_CONTRACT)
# carry / financing models
CARRY_NONE = "NONE"                         # nothing accrues while held (e.g. a cash share held long)
CARRY_FUNDING_RATE = "FUNDING_RATE"         # crypto perpetual: venue funding rate at each funding event
CARRY_FUTURES_BASIS = "FUTURES_BASIS"       # dated future: carry is inside the price and is realised at the roll
CARRY_OVERNIGHT_FINANCING = "OVERNIGHT_FINANCING"  # FX / CFD: daily financing on the held notional
CARRY_KINDS = (CARRY_NONE, CARRY_FUNDING_RATE, CARRY_FUTURES_BASIS, CARRY_OVERNIGHT_FINANCING)
# corporate actions
DIVIDEND, SPLIT = "DIVIDEND", "SPLIT"


class ContractDefinitionError(ValueError):
    pass


def dec(value: Any, name: str, *, positive: bool = False, allow_none: bool = False) -> Optional[Decimal]:
    if value is None:
        if allow_none:
            return None
        raise ContractDefinitionError(f"{name} is required")
    if isinstance(value, float):
        raise ContractDefinitionError(f"{name}: give decimals as strings, not floats")
    try:
        out = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ContractDefinitionError(f"{name}: not a decimal: {value!r}") from exc
    if not out.is_finite() or (positive and out <= 0):
        raise ContractDefinitionError(f"{name} must be a finite{' positive' if positive else ''} decimal")
    return out


@dataclass(frozen=True)
class MarginRule:
    kind: str
    initial: Decimal
    maintenance: Decimal

    def __post_init__(self) -> None:
        if self.kind not in MARGIN_KINDS:
            raise ContractDefinitionError(f"margin kind must be one of {MARGIN_KINDS}")
        if self.initial <= 0 or self.maintenance <= 0 or self.maintenance > self.initial:
            raise ContractDefinitionError("margin: 0 < maintenance <= initial")

    def initial_margin(self, *, notional: Decimal, contracts: Decimal) -> Decimal:
        return abs(notional) * self.initial if self.kind == PERCENT_OF_NOTIONAL else abs(contracts) * self.initial

    def maintenance_margin(self, *, notional: Decimal, contracts: Decimal) -> Decimal:
        return abs(notional) * self.maintenance if self.kind == PERCENT_OF_NOTIONAL else abs(contracts) * self.maintenance


@dataclass(frozen=True)
class CarryModel:
    """``long_rate`` / ``short_rate`` are annual rates paid (positive) or received (negative) on the held
    notional by a long / short position; ``day_count`` is the year basis (360 or 365);
    ``triple_weekday`` the local weekday (0 = Monday) on which three days are charged, if the broker
    documents one. Funding-rate carry uses the venue's recorded rates instead (``economics.funding_accrual``)."""
    kind: str = CARRY_NONE
    long_rate: Optional[Decimal] = None
    short_rate: Optional[Decimal] = None
    day_count: int = 365
    triple_weekday: Optional[int] = None

    def __post_init__(self) -> None:
        if self.kind not in CARRY_KINDS:
            raise ContractDefinitionError(f"carry kind must be one of {CARRY_KINDS}")
        if self.day_count not in (360, 365):
            raise ContractDefinitionError("carry day_count is 360 or 365")
        if self.kind == CARRY_OVERNIGHT_FINANCING and (self.long_rate is None or self.short_rate is None):
            raise ContractDefinitionError("overnight financing needs a long and a short rate")


@dataclass(frozen=True)
class CorporateAction:
    """A dividend (``amount`` per unit of the underlying, in ``currency``) or a split (``ratio`` new units per
    old unit), effective at ``effective_ms`` (the ex-date open)."""
    kind: str
    effective_ms: int
    amount: Optional[Decimal] = None
    currency: Optional[str] = None
    ratio: Optional[Decimal] = None
    source: str = ""

    def __post_init__(self) -> None:
        if self.kind == DIVIDEND and (self.amount is None or not self.currency):
            raise ContractDefinitionError("a dividend has an amount and a currency")
        if self.kind == SPLIT and (self.ratio is None or self.ratio <= 0):
            raise ContractDefinitionError("a split has a positive ratio")
        if self.kind not in (DIVIDEND, SPLIT):
            raise ContractDefinitionError(f"unknown corporate action {self.kind!r}")


@dataclass(frozen=True)
class ContractSpec:
    symbol: str
    venue: str
    asset_class: str
    contract_type: str
    settlement_currency: str
    multiplier: Decimal
    tick_size: Decimal
    quantity_step: Decimal
    min_quantity: Decimal
    margin: MarginRule
    calendar_id: str
    source: str
    verification_status: str
    listing_ms: Optional[int] = None
    first_notice_ms: Optional[int] = None
    last_trade_ms: Optional[int] = None
    expiry_ms: Optional[int] = None
    carry: CarryModel = field(default_factory=CarryModel)
    corporate_actions: Tuple[CorporateAction, ...] = ()
    series: Optional[str] = None

    def __post_init__(self) -> None:
        if not self.symbol or not self.venue:
            raise ContractDefinitionError("a contract has a symbol and a venue")
        if self.asset_class not in ASSET_CLASSES:
            raise ContractDefinitionError(f"{self.symbol}: asset_class must be one of {ASSET_CLASSES}")
        if self.contract_type not in CONTRACT_TYPES:
            raise ContractDefinitionError(f"{self.symbol}: contract_type must be one of {CONTRACT_TYPES}")
        if self.verification_status not in VERIFICATION_STATUSES:
            raise ContractDefinitionError(f"{self.symbol}: verification_status must be one of {VERIFICATION_STATUSES}")
        if not str(self.source or "").strip():
            raise ContractDefinitionError(f"{self.symbol}: every contract names its source")
        for name in ("multiplier", "tick_size", "quantity_step", "min_quantity"):
            if getattr(self, name) <= 0:
                raise ContractDefinitionError(f"{self.symbol}: {name} must be positive")
        if self.min_quantity % self.quantity_step != 0:
            raise ContractDefinitionError(f"{self.symbol}: min_quantity is a multiple of quantity_step")
        if self.contract_type == FUTURE and self.last_trade_ms is None:
            raise ContractDefinitionError(f"{self.symbol}: a dated future has a last trading instant")
        if self.contract_type == PERPETUAL and (self.last_trade_ms or self.expiry_ms):
            raise ContractDefinitionError(f"{self.symbol}: a perpetual has no expiry")
        if self.first_notice_ms and self.last_trade_ms and self.first_notice_ms > self.expiry_or_last_trade:
            raise ContractDefinitionError(f"{self.symbol}: first notice after expiry")

    # ---------------------------------------------------------------------------------------- derived
    @property
    def tick_value(self) -> Decimal:
        """Settlement-currency value of one tick on one contract. Derived, never stated, so it cannot
        disagree with the tick size and the multiplier."""
        return self.tick_size * self.multiplier

    @property
    def expiry_or_last_trade(self) -> Optional[int]:
        candidates = [t for t in (self.last_trade_ms, self.expiry_ms) if t is not None]
        return min(candidates) if candidates else None

    @property
    def is_dated(self) -> bool:
        return self.expiry_or_last_trade is not None

    # -------------------------------------------------------------------------------------- price rules
    def round_price(self, price: Decimal, *, mode: str = "nearest") -> Decimal:
        """A price on the tick grid. ``down`` / ``up`` for a side that must not be worsened; ``nearest``
        rounds half to even."""
        rounding = {"down": ROUND_DOWN, "up": ROUND_UP, "nearest": ROUND_HALF_EVEN}[mode]
        return (Decimal(price) / self.tick_size).to_integral_value(rounding=rounding) * self.tick_size

    def price_on_tick(self, price: Decimal) -> bool:
        return Decimal(price) % self.tick_size == 0

    def floor_quantity(self, quantity: Decimal) -> Decimal:
        """The largest valid quantity not above ``quantity`` (0 when below the minimum: never rounded up)."""
        q = (Decimal(quantity) / self.quantity_step).to_integral_value(rounding=ROUND_DOWN) * self.quantity_step
        return q if q >= self.min_quantity else Decimal("0")

    def quantity_valid(self, quantity: Decimal) -> bool:
        q = Decimal(quantity)
        return q >= self.min_quantity and q % self.quantity_step == 0

    # ---------------------------------------------------------------------------------------- lifetime
    def listed_at(self, ts_ms: int) -> bool:
        return self.listing_ms is None or int(ts_ms) >= self.listing_ms

    def expired_at(self, ts_ms: int) -> bool:
        end = self.expiry_or_last_trade
        return end is not None and int(ts_ms) >= end

    def to_plain(self) -> Dict[str, Any]:
        return {"symbol": self.symbol, "venue": self.venue, "asset_class": self.asset_class,
                "contract_type": self.contract_type, "settlement_currency": self.settlement_currency,
                "multiplier": str(self.multiplier), "tick_size": str(self.tick_size), "tick_value": str(self.tick_value),
                "quantity_step": str(self.quantity_step), "min_quantity": str(self.min_quantity),
                "margin": {"kind": self.margin.kind, "initial": str(self.margin.initial),
                           "maintenance": str(self.margin.maintenance)},
                "calendar_id": self.calendar_id, "listing_ms": self.listing_ms, "first_notice_ms": self.first_notice_ms,
                "last_trade_ms": self.last_trade_ms, "expiry_ms": self.expiry_ms, "carry_kind": self.carry.kind,
                "series": self.series, "source": self.source, "verification_status": self.verification_status}

    @classmethod
    def from_plain(cls, d: Mapping[str, Any], *, defaults: Optional[Mapping[str, Any]] = None) -> "ContractSpec":
        d = {**(defaults or {}), **d}
        sym = str(d.get("symbol") or "")
        margin = d.get("margin") or {}
        carry = d.get("carry") or {}
        actions = tuple(CorporateAction(kind=str(a.get("kind")), effective_ms=int(a["effective_ms"]),
                                        amount=dec(a.get("amount"), f"{sym}.dividend", allow_none=True),
                                        currency=a.get("currency"), ratio=dec(a.get("ratio"), f"{sym}.split", allow_none=True),
                                        source=str(a.get("source") or ""))
                        for a in d.get("corporate_actions") or ())
        return cls(
            symbol=sym, venue=str(d.get("venue") or ""), asset_class=str(d.get("asset_class") or ""),
            contract_type=str(d.get("contract_type") or ""), settlement_currency=str(d.get("settlement_currency") or ""),
            multiplier=dec(d.get("multiplier"), f"{sym}.multiplier", positive=True),
            tick_size=dec(d.get("tick_size"), f"{sym}.tick_size", positive=True),
            quantity_step=dec(d.get("quantity_step"), f"{sym}.quantity_step", positive=True),
            min_quantity=dec(d.get("min_quantity"), f"{sym}.min_quantity", positive=True),
            margin=MarginRule(str(margin.get("kind") or ""), dec(margin.get("initial"), f"{sym}.margin.initial"),
                              dec(margin.get("maintenance"), f"{sym}.margin.maintenance")),
            calendar_id=str(d.get("calendar_id") or ""), source=str(d.get("source") or ""),
            verification_status=str(d.get("verification_status") or ""),
            listing_ms=d.get("listing_ms"), first_notice_ms=d.get("first_notice_ms"),
            last_trade_ms=d.get("last_trade_ms"), expiry_ms=d.get("expiry_ms"),
            carry=CarryModel(kind=str(carry.get("kind") or CARRY_NONE),
                             long_rate=dec(carry.get("long_rate"), f"{sym}.carry.long_rate", allow_none=True),
                             short_rate=dec(carry.get("short_rate"), f"{sym}.carry.short_rate", allow_none=True),
                             day_count=int(carry.get("day_count") or 365), triple_weekday=carry.get("triple_weekday")),
            corporate_actions=actions, series=d.get("series"))


__all__ = ["ASSET_CLASSES", "CONTRACT_TYPES", "PERCENT_OF_NOTIONAL", "FIXED_PER_CONTRACT", "CARRY_NONE",
           "CARRY_FUNDING_RATE", "CARRY_FUTURES_BASIS", "CARRY_OVERNIGHT_FINANCING", "CARRY_KINDS", "DIVIDEND", "SPLIT",
           "ContractDefinitionError", "MarginRule", "CarryModel", "CorporateAction", "ContractSpec", "dec"]
