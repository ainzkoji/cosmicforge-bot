"""Per-account multi-position portfolio authorization (Section H, Step 3.7). Pure: no database, no clock, no I/O.

A strategy family states intents (``pipeline.contracts``). This module answers, for ONE account and from ONE
immutable ``AccountSnapshot``, which of them may be executed and at what quantity. It replaces the
one-position limitation for new code; it does not select, rank or invent trades. An existing position is never
reduced here, because only a family may state a reduction, and no candidate is preferred over another beyond
the order the family gave them in.

**Limits** come from the shared risk-profile authority plus the engine's two hard limits (``AccountLimits``):
the effective per-trade risk is ``min(profile, 0.40 %)`` and an entry whose stop is more than 15 % away is
REFUSED. A stop is never clipped or moved to make an entry fit. No number is defined in this module.

**Deterministic order.** The intents are processed in the order given (the family's own ranking):

1. *Risk-reducing intents* (exit, reduce, rebalance down, stop replace). Never refused by a cap or a brake. An
   exit or a reduction releases its slot, risk, notional and margin to the increases of the same decision,
   because the frozen daily-trend rule replaces a position inside one decision; every increase that fits only
   thanks to such a release is marked ``awaits_releases`` and the execution stage must see the release at the
   venue before it submits that order (the reservation ledger enforces it).
2. *Gates* on each risk-increasing intent (entry, add, rebalance up): brake state, account equity, instrument
   rules, position and reservation state, rebalancing band, stop geometry and the 15 % limit, leverage, the
   stated per-trade risk.
3. *Per-intent ceilings*, which can only make an order smaller: the per-trade risk clamp measured on the
   snapshot's equity (halved while the halve brake holds), then the customer's maximum position.
4. *Position slots*, first come first served in the given order.
5. *Shared budgets*. Each one scales EVERY surviving increase of the decision by one common factor, the
   proportional form of the frozen rule, in its order: open risk, then gross leverage, then available margin.
6. *Exchange filters*: quantity rounded DOWN to the step, maximum quantity, minimum quantity, minimum notional.
   An order is never raised to reach a minimum; one that falls below it is refused, and the reason is the cap
   that made it too small, or the filter itself when no cap touched it.

Capacity released by a refusal in step 6 is not handed to another intent of the same decision.

**Brakes** (``pipeline.brakes``): ``halted`` refuses every increase; ``paused`` refuses NEW positions only, so
what is held stays manageable; ``halve`` is applied by the family to its own targets (it knows them), and this
layer enforces the bound that halving implies: no position may carry more than half the per-trade ceiling
while it holds. An unknown brake state refuses every increase.

**Open risk** is ``quantity x |mark - stop|`` summed over the positions, as a fraction of equity. A position
whose stop is unknown, or not on the protective side of the mark, counts with its whole notional and is
reported (``unprotected``): unknown is never zero.

Quantities are base units, prices quote units, fractions are fractions (0.004 = 0.40 %).
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field, replace
from typing import Any, Dict, List, Mapping, Optional, Sequence, Set, Tuple, Union

from app.exchange.platform_adapter.contract import (
    FLAT, TRADING, AccountBalance, InstrumentRules, PositionInfo, round_down_to_step,
)
from app.risk.system_limits import SystemLimits
from app.trading_intelligence.execution.risk_sizing import MARGIN_FILL_BUFFER
from app.trading_intelligence.hashing import stable_hash
from app.trading_intelligence.pipeline.brakes import BrakeFlags
from app.trading_intelligence.pipeline.contracts import (
    ADD, DIRECTIONS, ENTRY, EXIT, LONG, MARKET, REBALANCE, REDUCE, STOP_REPLACE, ExitIntent, Intent, IntentBatch,
    PortfolioConstraints, stop_distance_fraction, stop_side_valid, to_plain,
)
from shared_lib.risk_levels import SYSTEM_PER_TRADE_RISK_CEILING_PCT, effective_per_trade_risk_pct, get_profile

# -------------------------------------------------------------------------------------------- refusal reasons
POSITION_LIMIT = "POSITION_LIMIT"
OPEN_RISK_CAP = "OPEN_RISK_CAP"
LEVERAGE_CAP = "LEVERAGE_CAP"
INSUFFICIENT_MARGIN = "INSUFFICIENT_MARGIN"
RISK_ABOVE_CEILING = "RISK_ABOVE_CEILING"
ENTRY_STOP_TOO_WIDE = "ENTRY_STOP_TOO_WIDE"
BELOW_MIN_NOTIONAL = "BELOW_MIN_NOTIONAL"
BELOW_MIN_QUANTITY = "BELOW_MIN_QUANTITY"
QUANTITY_ROUNDS_TO_ZERO = "QUANTITY_ROUNDS_TO_ZERO"
CUSTOMER_MAX_POSITION = "CUSTOMER_MAX_POSITION"
BRAKE_ACTIVE = "BRAKE_ACTIVE"
BRAKE_STATE_UNKNOWN = "BRAKE_STATE_UNKNOWN"
EQUITY_UNAVAILABLE = "EQUITY_UNAVAILABLE"
INSTRUMENT_RULES_MISSING = "INSTRUMENT_RULES_MISSING"
INSTRUMENT_RULES_INCOMPLETE = "INSTRUMENT_RULES_INCOMPLETE"
INSTRUMENT_NOT_TRADING = "INSTRUMENT_NOT_TRADING"
POSITION_EXISTS = "POSITION_EXISTS"
NO_POSITION = "NO_POSITION"
DIRECTION_MISMATCH = "DIRECTION_MISMATCH"
DUPLICATE_RESERVATION = "DUPLICATE_RESERVATION"
LEGACY_CLAIM_UNSIZED = "LEGACY_CLAIM_UNSIZED"
INSIDE_REBALANCE_BAND = "INSIDE_REBALANCE_BAND"
STOP_WIDENING_REFUSED = "STOP_WIDENING_REFUSED"
INVALID_STOP_GEOMETRY = "INVALID_STOP_GEOMETRY"
CONTRADICTORY_ACTION = "CONTRADICTORY_ACTION"
REFUSAL_REASONS: Tuple[str, ...] = (
    POSITION_LIMIT, OPEN_RISK_CAP, LEVERAGE_CAP, INSUFFICIENT_MARGIN, RISK_ABOVE_CEILING, ENTRY_STOP_TOO_WIDE,
    BELOW_MIN_NOTIONAL, BELOW_MIN_QUANTITY, QUANTITY_ROUNDS_TO_ZERO, CUSTOMER_MAX_POSITION, BRAKE_ACTIVE,
    BRAKE_STATE_UNKNOWN, EQUITY_UNAVAILABLE, INSTRUMENT_RULES_MISSING, INSTRUMENT_RULES_INCOMPLETE,
    INSTRUMENT_NOT_TRADING, POSITION_EXISTS, NO_POSITION, DIRECTION_MISMATCH, DUPLICATE_RESERVATION,
    LEGACY_CLAIM_UNSIZED, INSIDE_REBALANCE_BAND, STOP_WIDENING_REFUSED, INVALID_STOP_GEOMETRY, CONTRADICTORY_ACTION)

#: adjustments that are not caps: what else made an executable quantity or price differ from the stated one
EXCHANGE_MAX_QUANTITY = "EXCHANGE_MAX_QUANTITY"
QUANTITY_STEP = "QUANTITY_STEP"
PRICE_TICK = "PRICE_TICK"
CAPPED_TO_POSITION = "CAPPED_TO_POSITION"
RULES_UNAVAILABLE = "RULES_UNAVAILABLE"

PIPELINE_SOURCE = "PIPELINE"
#: the halve brake halves: the approved action of Step 3.10 and the scale of the frozen rule (interpretation I8)
HALVE_FACTOR = 0.5
#: relative tolerance of a comparison against a limit, as in ``pipeline.contracts``
_TOL = 1e-9


class PortfolioDataError(ValueError):
    """The account state handed to this module cannot be interpreted. Fail closed; ``code`` is stable."""

    def __init__(self, code: str, message: str = "") -> None:
        super().__init__(f"{code}: {message}" if message else code)
        self.code = code


def _finite(code: str, name: str, value: Any, *, positive: bool = False) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise PortfolioDataError(code, f"{name} is not a finite number: {value!r}")
    if positive and value <= 0:
        raise PortfolioDataError(code, f"{name} must be positive: {value!r}")
    return float(value)


# ----------------------------------------------------------------------------------------------------- limits
@dataclass(frozen=True)
class AccountLimits:
    """The limits of one account: its risk profile under the engine's hard limits. Fractions of equity."""
    risk_level: str
    profile_version: str
    approved_per_trade_risk: float      # what the profile states
    per_trade_risk: float               # what the engine allows today: min(profile, the 0.40 % clamp)
    max_open_risk: float
    max_positions: int
    max_leverage: float
    max_entry_stop_distance: float      # the engine's widest entry stop (15 %): a refusal, never a clip
    max_position_notional: Optional[float] = None   # the customer's optional maximum position, quote units

    def __post_init__(self) -> None:
        for name in ("approved_per_trade_risk", "per_trade_risk", "max_open_risk", "max_leverage", "max_entry_stop_distance"):
            _finite("INVALID_LIMITS", name, getattr(self, name), positive=True)
        if isinstance(self.max_positions, bool) or not isinstance(self.max_positions, int) or self.max_positions < 1:
            raise PortfolioDataError("INVALID_LIMITS", "max_positions is a positive integer")
        if self.per_trade_risk > self.approved_per_trade_risk:
            raise PortfolioDataError("INVALID_LIMITS", "the engine clamp only ever lowers the per-trade risk")
        if self.max_position_notional is not None:
            _finite("INVALID_LIMITS", "max_position_notional", self.max_position_notional, positive=True)

    @classmethod
    def from_profile(cls, level: Any, *, version: Optional[str] = None,
                     max_position_notional: Optional[float] = None) -> "AccountLimits":
        """The profile of ``shared_lib.risk_levels`` under ``SYSTEM_PER_TRADE_RISK_CEILING_PCT`` and
        ``SystemLimits``; the stricter of the two statements of the per-trade ceiling wins."""
        profile, system = get_profile(level, version), SystemLimits()
        clamp_pct = min(SYSTEM_PER_TRADE_RISK_CEILING_PCT, effective_per_trade_risk_pct(profile))
        return cls(risk_level=profile.level, profile_version=profile.version,
                   approved_per_trade_risk=float(profile.fraction("per_trade_risk_pct")),
                   per_trade_risk=min(float(clamp_pct / 100), system.max_risk_per_trade_ceiling),
                   max_open_risk=float(profile.fraction("max_open_risk_pct")), max_positions=int(profile.max_positions),
                   max_leverage=float(profile.leverage_ceiling), max_entry_stop_distance=float(system.max_stop_loss_pct),
                   max_position_notional=max_position_notional)

    @property
    def ceiling_applied(self) -> bool:
        return self.per_trade_risk < self.approved_per_trade_risk

    def constraints(self, equity_basis: float) -> PortfolioConstraints:
        """These limits in the form an ``IntentBatch`` carries."""
        return PortfolioConstraints(max_positions=self.max_positions, max_open_risk=self.max_open_risk,
                                    max_leverage=self.max_leverage, per_trade_risk_ceiling=self.per_trade_risk,
                                    max_entry_stop_distance=self.max_entry_stop_distance, equity_basis=equity_basis)

    @property
    def limits_hash(self) -> str:
        return stable_hash(to_plain(self))


# --------------------------------------------------------------------------------------------------- snapshot
@dataclass(frozen=True)
class PortfolioPosition:
    """One open position of the account (one-way mode: one per symbol)."""
    symbol: str
    direction: str
    quantity: float
    entry_price: float
    mark_price: float
    stop_price: Optional[float]         # None: no protective stop is known
    family_id: str = ""
    leverage: Optional[float] = None

    def __post_init__(self) -> None:
        symbol = self.symbol
        if not isinstance(symbol, str) or not symbol or symbol != symbol.upper() or not symbol.isalnum():
            raise PortfolioDataError("INVALID_POSITION", f"symbol {symbol!r} is not canonical")
        if self.direction not in DIRECTIONS:
            raise PortfolioDataError("INVALID_POSITION", f"{self.symbol}: direction {self.direction!r}")
        for name in ("quantity", "entry_price", "mark_price"):
            _finite("INVALID_POSITION", f"{self.symbol}.{name}", getattr(self, name), positive=True)
        if self.stop_price is not None:
            _finite("INVALID_POSITION", f"{self.symbol}.stop_price", self.stop_price, positive=True)
        if self.leverage is not None:
            _finite("INVALID_POSITION", f"{self.symbol}.leverage", self.leverage, positive=True)

    @property
    def notional(self) -> float:
        return self.quantity * self.mark_price

    @property
    def protected(self) -> bool:
        """A stop is known and lies on the protective side of the mark."""
        return self.stop_price is not None and stop_side_valid(self.direction, self.mark_price, self.stop_price)

    @property
    def risk_amount(self) -> float:
        """Loss at the stop from the mark; the whole notional when the position is not protected."""
        return self.quantity * abs(self.mark_price - self.stop_price) if self.protected else self.notional


@dataclass(frozen=True)
class OpenReservation:
    """A claim on the account that is not (or not yet visibly) a position. ``None`` means the store that holds
    the claim does not state the value: the risk is then taken at the per-trade ceiling and an unknown notional
    blocks every increase (``LEGACY_CLAIM_UNSIZED``)."""
    reservation_id: str
    symbol: str
    direction: str
    kind: str                           # ENTRY | ADD | REBALANCE
    risk_amount: Optional[float]
    notional: Optional[float]
    margin: Optional[float]
    state: str
    source: str = PIPELINE_SOURCE


@dataclass(frozen=True)
class AccountSnapshot:
    """Everything one decision reads about one account, taken at one instant. ``snapshot_hash`` covers the whole
    content, so an authorization and the reservation made from it can be tied to the same state."""
    account_id: str
    equity: float                       # wallet balance plus unrealised result
    wallet_balance: float
    available_margin: float
    positions: Tuple[PortfolioPosition, ...]
    reservations: Tuple[OpenReservation, ...]
    as_of_ms: int
    snapshot_hash: str = field(init=False, default="")

    def __post_init__(self) -> None:
        if not isinstance(self.account_id, str) or not self.account_id.strip():
            raise PortfolioDataError("INVALID_SNAPSHOT", "account_id is required")
        for name in ("equity", "wallet_balance", "available_margin"):
            _finite("INVALID_SNAPSHOT", name, getattr(self, name))
        if isinstance(self.as_of_ms, bool) or not isinstance(self.as_of_ms, int) or self.as_of_ms <= 0:
            raise PortfolioDataError("INVALID_SNAPSHOT", "as_of_ms is an epoch in milliseconds")
        positions = tuple(sorted(self.positions, key=lambda p: p.symbol))
        if len({p.symbol for p in positions}) != len(positions):
            raise PortfolioDataError("INVALID_SNAPSHOT", "two positions on one symbol (one-way mode holds one)")
        reservations = tuple(sorted(self.reservations, key=lambda r: r.reservation_id))
        object.__setattr__(self, "positions", positions)
        object.__setattr__(self, "reservations", reservations)
        object.__setattr__(self, "snapshot_hash", stable_hash({
            "account_id": self.account_id, "equity": self.equity, "wallet_balance": self.wallet_balance,
            "available_margin": self.available_margin, "positions": to_plain(positions),
            "reservations": to_plain(reservations), "as_of_ms": self.as_of_ms}))

    @classmethod
    def from_venue(cls, account_id: str, balance: AccountBalance, positions: Sequence[PositionInfo], *,
                   stops: Mapping[str, Optional[float]], as_of_ms: int, families: Optional[Mapping[str, str]] = None,
                   reservations: Sequence[OpenReservation] = ()) -> "AccountSnapshot":
        """From the adapter contract's balance and positions. ``stops`` maps a symbol to its resting protective
        stop; a symbol that is absent has no known stop and is counted accordingly."""
        held = []
        for p in positions:
            if p.side == FLAT:
                continue
            if p.entry_price is None or p.mark_price is None:
                raise PortfolioDataError("POSITION_PRICE_UNKNOWN", f"{p.symbol}: a position without a price has no size")
            held.append(PortfolioPosition(symbol=p.symbol, direction=p.side, quantity=float(p.quantity),
                                          entry_price=float(p.entry_price), mark_price=float(p.mark_price),
                                          stop_price=stops.get(p.symbol), family_id=(families or {}).get(p.symbol, ""),
                                          leverage=p.leverage))
        return cls(account_id=account_id, equity=float(balance.equity), wallet_balance=float(balance.wallet_balance),
                   available_margin=float(balance.available_balance), positions=tuple(held),
                   reservations=tuple(reservations), as_of_ms=int(as_of_ms))

    def position(self, symbol: str) -> Optional[PortfolioPosition]:
        return next((p for p in self.positions if p.symbol == symbol), None)


@dataclass(frozen=True)
class OpenRisk:
    amount: float                       # quote units
    fraction: float                     # of equity; infinite when there is risk and no equity
    by_symbol: Tuple[Tuple[str, float], ...]
    unprotected: Tuple[str, ...]        # counted with their whole notional


def open_risk(snapshot: AccountSnapshot) -> OpenRisk:
    """The loss of the account if every stop is hit from the current mark, over its positions."""
    by_symbol = tuple((p.symbol, p.risk_amount) for p in snapshot.positions)
    amount = sum(v for _s, v in by_symbol)
    fraction = amount / snapshot.equity if snapshot.equity > 0 else (math.inf if amount > 0 else 0.0)
    return OpenRisk(amount=amount, fraction=fraction, by_symbol=by_symbol,
                    unprotected=tuple(p.symbol for p in snapshot.positions if not p.protected))


@dataclass(frozen=True)
class PortfolioUsage:
    """What the account is committed to: positions plus open reservations (and, after an authorization, the
    increases it accepted)."""
    positions: int                      # occupied position slots
    symbols: Tuple[str, ...]
    open_risk_amount: float
    open_risk_fraction: float
    gross_notional: float
    gross_leverage: float
    reserved_margin: float
    usable_margin: float                # what an increase may still take, after the fill buffer
    unprotected: Tuple[str, ...]
    unsized: Tuple[str, ...]            # claims of unknown size


@dataclass
class _Book:
    equity: float
    slots: Set[str]
    risk: Dict[str, float]
    notional: Dict[str, float]
    reserved_margin: float
    available_margin: float
    unprotected: Tuple[str, ...]
    unsized: Tuple[str, ...]
    reserved_symbols: Set[str]

    @property
    def usable_margin(self) -> float:
        return max(0.0, self.available_margin - self.reserved_margin) * (1.0 - float(MARGIN_FILL_BUFFER))

    def _of_equity(self, amount: float) -> float:
        return amount / self.equity if self.equity > 0 else (math.inf if amount > 0 else 0.0)

    def usage(self) -> PortfolioUsage:
        risk, gross = sum(self.risk.values()), sum(self.notional.values())
        return PortfolioUsage(positions=len(self.slots), symbols=tuple(sorted(self.slots)), open_risk_amount=risk,
                              open_risk_fraction=self._of_equity(risk), gross_notional=gross,
                              gross_leverage=self._of_equity(gross),
                              reserved_margin=self.reserved_margin, usable_margin=self.usable_margin,
                              unprotected=self.unprotected, unsized=self.unsized)


def _book(snapshot: AccountSnapshot, limits: AccountLimits, positions: Mapping[str, PortfolioPosition],
          released_margin: float = 0.0) -> _Book:
    """Commitments per symbol. A reservation and the position it became are one commitment, not two: an entry
    counts with the larger of the two measures, an increase of a held position is added to it, and a legacy
    claim that is already a position counts as that position."""
    equity = snapshot.equity
    ceiling_amount = limits.per_trade_risk * max(equity, 0.0)
    risk = {s: p.risk_amount for s, p in positions.items()}
    notional = {s: p.notional for s, p in positions.items()}
    slots, reserved_symbols, unsized, reserved_margin = set(positions), set(), [], 0.0
    legacy: Dict[str, List[OpenReservation]] = {}
    for r in sorted(snapshot.reservations, key=lambda r: (r.source != PIPELINE_SOURCE, r.kind != ENTRY, r.reservation_id)):
        slots.add(r.symbol)
        reserved_symbols.add(r.symbol)
        if r.source != PIPELINE_SOURCE:
            legacy.setdefault(r.symbol, []).append(r)
            continue
        r_risk = ceiling_amount if r.risk_amount is None else r.risk_amount
        if r.notional is None:
            unsized.append(r.symbol)
        r_notional = r.notional or 0.0
        if r.kind == ENTRY:
            risk[r.symbol] = max(risk.get(r.symbol, 0.0), r_risk)
            notional[r.symbol] = max(notional.get(r.symbol, 0.0), r_notional)
        else:
            risk[r.symbol] = risk.get(r.symbol, 0.0) + r_risk
            notional[r.symbol] = notional.get(r.symbol, 0.0) + r_notional
        reserved_margin += r_notional if r.margin is None else r.margin
    for symbol, rows in legacy.items():
        if symbol in positions:
            continue
        known = [r.notional for r in rows if r.notional is not None]
        margins = [r.margin for r in rows if r.margin is not None]
        risk[symbol] = max(risk.get(symbol, 0.0), ceiling_amount)
        if known:
            notional[symbol] = max(notional.get(symbol, 0.0), max(known))
        else:
            unsized.append(symbol)
        reserved_margin += max(margins) if margins else (max(known) if known else 0.0)
    return _Book(equity=equity, slots=slots, risk=risk, notional=notional, reserved_margin=reserved_margin,
                 available_margin=snapshot.available_margin + released_margin,
                 unprotected=tuple(sorted(s for s, p in positions.items() if not p.protected)),
                 unsized=tuple(sorted(set(unsized))), reserved_symbols=reserved_symbols)


def usage(snapshot: AccountSnapshot, limits: AccountLimits) -> PortfolioUsage:
    """Budget use of the account as the snapshot shows it (positions and open reservations)."""
    return _book(snapshot, limits, {p.symbol: p for p in snapshot.positions}).usage()


# ----------------------------------------------------------------------------------------------------- result
@dataclass(frozen=True)
class Release:
    """A position an accepted exit or reduction will shrink. ``quantity_after`` is 0 for an exit."""
    symbol: str
    quantity_after: float


@dataclass(frozen=True)
class AuthorizedIntent:
    intent: Intent
    symbol: str
    direction: str
    kind: str
    increases_risk: bool
    quantity: float                     # executable order quantity, base units (the whole position for an exit)
    requested_quantity: float
    position_quantity_after: float
    reference_price: float
    stop_price: Optional[float]         # the protective stop after the order, on the instrument's price tick
    limit_price: Optional[float]
    leverage: float
    notional: float                     # of the order
    risk_amount: float                  # risk the order adds at the stop; 0 for a reduction
    margin: float                       # margin the order needs; 0 for a reduction
    limited_by: Tuple[str, ...]         # what made it smaller or moved a price, in the order applied
    awaits_releases: bool               # fits only once the releases of this decision are visible at the venue

    @property
    def scale(self) -> float:
        return self.quantity / self.requested_quantity if self.requested_quantity > 0 else 1.0


@dataclass(frozen=True)
class RefusedIntent:
    intent: Intent
    symbol: str
    kind: str
    reason: str                         # one of REFUSAL_REASONS
    detail: str


@dataclass(frozen=True)
class BudgetScales:
    """The common factor each shared budget applied to the increases of this decision (1.0: not binding)."""
    open_risk: float = 1.0
    leverage: float = 1.0
    margin: float = 1.0


@dataclass(frozen=True)
class TargetAllocation:
    """One line of the portfolio the accepted intents lead to."""
    symbol: str
    direction: str
    quantity: float
    notional: float
    fraction_of_equity: float
    risk_amount: float


@dataclass(frozen=True)
class PortfolioAuthorization:
    account_id: str
    snapshot_hash: str
    limits_hash: str
    accepted: Tuple[AuthorizedIntent, ...]      # execution order: reductions first, then increases
    refused: Tuple[RefusedIntent, ...]
    releases: Tuple[Release, ...]
    scales: BudgetScales
    usage_before: PortfolioUsage
    usage_after: PortfolioUsage
    allocation: Tuple[TargetAllocation, ...]
    close_all_positions: bool                   # the terminal brake holds: see ``flatten_intents``
    authorization_hash: str

    @property
    def increases(self) -> Tuple[AuthorizedIntent, ...]:
        return tuple(a for a in self.accepted if a.increases_risk)

    def refusal(self, symbol: str) -> Optional[RefusedIntent]:
        return next((r for r in self.refused if r.symbol == symbol), None)

    def authorized(self, symbol: str) -> Optional[AuthorizedIntent]:
        return next((a for a in self.accepted if a.symbol == symbol), None)


# ---------------------------------------------------------------------------------------------------- helpers
def rebalance_required(held_quantity: float, target_quantity: float, band: float) -> bool:
    """The frozen band rule: a position moves only when it differs from its target by MORE than ``band`` of the
    target (``abs(target - held) > band * target``, in quantity at one price)."""
    return abs(target_quantity - held_quantity) > band * target_quantity


def _price_decimals(tick: float) -> int:
    return max(0, -int(math.floor(math.log10(tick))) + 2)


def conform_stop_price(direction: str, stop_price: float, tick: Optional[float]) -> float:
    """The stop on the price tick, moved (by less than one tick) TOWARD the market only, so the loss at the stop
    is never larger than the one that was authorized."""
    if not tick:
        return stop_price
    units = stop_price / tick
    steps = math.ceil(units - _TOL) if direction == LONG else math.floor(units + _TOL)
    return round(steps * tick, _price_decimals(tick))


def conform_limit_price(direction: str, limit_price: float, tick: Optional[float]) -> float:
    """An opening limit on the price tick, never at a worse price than stated (a buy rounds down, a sell up)."""
    if not tick:
        return limit_price
    units = limit_price / tick
    steps = math.floor(units + _TOL) if direction == LONG else math.ceil(units - _TOL)
    return round(steps * tick, _price_decimals(tick))


def flatten_intents(snapshot: AccountSnapshot, reason: str = "BRAKE") -> Tuple[ExitIntent, ...]:
    """What a terminal stop asks for: one exit per position of the account, in symbol order."""
    return tuple(ExitIntent(symbol=p.symbol, direction=p.direction, reason=reason, reference_price=p.mark_price)
                 for p in snapshot.positions)


def _wider(direction: str, new_stop: float, current_stop: float) -> bool:
    return new_stop < current_stop if direction == LONG else new_stop > current_stop


def _factor(headroom: float, demand: float) -> float:
    if demand <= 0 or (headroom > 0 and demand <= headroom * (1.0 + _TOL)):
        return 1.0
    return max(0.0, headroom) / demand


@dataclass
class _Candidate:
    intent: Intent
    symbol: str
    direction: str
    kind: str
    requested: float
    quantity: float
    reference: float
    stop: float
    leverage: float
    held: float
    rule: InstrumentRules
    limited: List[Tuple[str, float]] = field(default_factory=list)

    @property
    def distance(self) -> float:
        return abs(self.reference - self.stop)

    def cap(self, code: str, maximum: float) -> None:
        if self.quantity > maximum * (1.0 + _TOL):
            self.limited.append((code, maximum / self.quantity))
            self.quantity = maximum

    def binding(self, otherwise: str) -> str:
        """The cap that reduced this order most, or the exchange filter when no cap touched it."""
        return min(self.limited, key=lambda c: c[1])[0] if self.limited else otherwise


def _refuse(intent: Intent, reason: str, detail: str) -> RefusedIntent:
    return RefusedIntent(intent=intent, symbol=intent.symbol, kind=intent.kind, reason=reason, detail=detail)


def _reduction(intent: Intent, position: Optional[PortfolioPosition],
               rule: Optional[InstrumentRules]) -> Union[AuthorizedIntent, RefusedIntent]:
    """Exit, reduce, rebalance down or stop replace. Sized against the position as the snapshot holds it."""
    if position is None:
        return _refuse(intent, NO_POSITION, "the account holds no position on this symbol")
    if position.direction != intent.direction:
        return _refuse(intent, DIRECTION_MISMATCH, f"the position is {position.direction}")
    reference = getattr(intent, "reference_price", None) or position.mark_price
    held, stop, limited = position.quantity, position.stop_price, []

    def accept(quantity: float, requested: float, remaining: float, notional: float) -> AuthorizedIntent:
        return AuthorizedIntent(intent=intent, symbol=intent.symbol, direction=position.direction, kind=intent.kind,
                                increases_risk=False, quantity=quantity, requested_quantity=requested,
                                position_quantity_after=remaining, reference_price=reference, stop_price=stop,
                                limit_price=None, leverage=position.leverage or 1.0, notional=notional, risk_amount=0.0,
                                margin=0.0, limited_by=tuple(limited), awaits_releases=False)

    if intent.kind == EXIT:
        return accept(held, held, 0.0, held * reference)     # a full close passes every exchange filter
    if intent.kind == STOP_REPLACE:
        if position.protected and _wider(position.direction, intent.new_stop_price, position.stop_price):
            return _refuse(intent, STOP_WIDENING_REFUSED, f"the resting stop is {position.stop_price}")
        stop = conform_stop_price(position.direction, intent.new_stop_price, rule.price_tick if rule is not None else None)
        if not stop_side_valid(position.direction, position.mark_price, stop):
            return _refuse(intent, INVALID_STOP_GEOMETRY,
                           f"a stop at {stop} is not protective at the mark {position.mark_price}")
        if stop != intent.new_stop_price:
            limited.append(PRICE_TICK)
        requested = held if intent.quantity is None else intent.quantity
        if requested > held:
            limited.append(CAPPED_TO_POSITION)
        return accept(min(requested, held), requested, held, 0.0)
    if intent.kind == REDUCE:
        requested = intent.quantity if intent.quantity is not None else intent.fraction * held
    elif not rebalance_required(held, intent.target_quantity, intent.band):
        return _refuse(intent, INSIDE_REBALANCE_BAND,
                       f"held {held} is within {intent.band} of the target {intent.target_quantity}")
    else:
        requested = held - intent.target_quantity
    if requested >= held * (1.0 - _TOL):                     # the whole position: closed like an exit
        if requested > held * (1.0 + _TOL):
            limited.append(CAPPED_TO_POSITION)
        return accept(held, requested, 0.0, held * reference)
    if rule is None or rule.quantity_step is None:
        limited.append(RULES_UNAVAILABLE)                    # a reduction is not withheld for a missing rule
        return accept(requested, requested, held - requested, requested * reference)
    quantity = round_down_to_step(requested, rule.quantity_step)
    if quantity < requested * (1.0 - _TOL):
        limited.append(QUANTITY_STEP)
    if quantity <= 0:
        return _refuse(intent, QUANTITY_ROUNDS_TO_ZERO, f"{requested} is below the step {rule.quantity_step}")
    if rule.min_quantity is not None and quantity < rule.min_quantity:
        return _refuse(intent, BELOW_MIN_QUANTITY, f"{quantity} is below the minimum quantity {rule.min_quantity}")
    if rule.min_notional is not None and quantity * reference < rule.min_notional:
        return _refuse(intent, BELOW_MIN_NOTIONAL, f"{quantity * reference} is below the minimum notional {rule.min_notional}")
    return accept(quantity, requested, held - quantity, quantity * reference)


def _gate(intent: Intent, position: Optional[PortfolioPosition], rule: Optional[InstrumentRules], limits: AccountLimits,
          book: _Book, brake_state: Optional[BrakeFlags]) -> Optional[Tuple[str, str]]:
    """The first reason a risk-increasing intent cannot be sized at all, or None."""
    if brake_state is None:
        return BRAKE_STATE_UNKNOWN, "the brake state of the account was not supplied"
    if brake_state.halted:
        return BRAKE_ACTIVE, "TERMINAL_STOP: positions are closed and nothing is opened until an explicit restart"
    if brake_state.paused and intent.kind == ENTRY:
        return BRAKE_ACTIVE, "DAILY_LOSS_PAUSE: no new position at this decision"
    if not book.equity > 0:
        return EQUITY_UNAVAILABLE, f"account equity is {book.equity}"
    if rule is None or rule.symbol != intent.symbol:
        return INSTRUMENT_RULES_MISSING, "no instrument rules for this symbol"
    if not rule.complete:
        return INSTRUMENT_RULES_INCOMPLETE, "tick, step, minimum quantity or minimum notional is not known"
    if rule.status != TRADING:
        return INSTRUMENT_NOT_TRADING, f"venue status {rule.venue_status}"
    if intent.symbol in book.reserved_symbols:
        return DUPLICATE_RESERVATION, "an open reservation already claims this symbol"
    if intent.kind == ENTRY:
        if position is not None:
            return POSITION_EXISTS, "an entry opens a position that does not exist"
    else:
        if position is None:
            return NO_POSITION, "the account holds no position on this symbol"
        if position.direction != intent.direction:
            return DIRECTION_MISMATCH, f"the position is {position.direction}"
        if intent.kind == REBALANCE and not rebalance_required(position.quantity, intent.target_quantity, intent.band):
            return (INSIDE_REBALANCE_BAND,
                    f"held {position.quantity} is within {intent.band} of the target {intent.target_quantity}")
        if position.protected and _wider(position.direction, intent.stop_price, position.stop_price):
            return STOP_WIDENING_REFUSED, f"the resting stop is {position.stop_price}"
    if book.unsized:
        return LEGACY_CLAIM_UNSIZED, f"an open claim of unknown size exists on {', '.join(book.unsized)}"
    distance = stop_distance_fraction(intent.reference_price, intent.stop_price)
    if distance > limits.max_entry_stop_distance * (1.0 + _TOL):
        return ENTRY_STOP_TOO_WIDE, f"stop distance {distance:.6f} is above {limits.max_entry_stop_distance}; never clipped"
    if intent.kind == ENTRY:
        if intent.leverage > limits.max_leverage * (1.0 + _TOL):
            return LEVERAGE_CAP, f"leverage {intent.leverage} is above the account's {limits.max_leverage}"
        if rule.max_leverage is not None and intent.leverage > rule.max_leverage * (1.0 + _TOL):
            return LEVERAGE_CAP, f"leverage {intent.leverage} is above the instrument's {rule.max_leverage}"
        if intent.risk_fraction > limits.per_trade_risk * (1.0 + _TOL):
            return RISK_ABOVE_CEILING, f"stated risk {intent.risk_fraction} is above the ceiling {limits.per_trade_risk}"
    return None


def _candidate(intent: Intent, position: Optional[PortfolioPosition], rule: InstrumentRules, limits: AccountLimits,
               equity: float, halve: bool) -> Union[_Candidate, RefusedIntent]:
    """The intent with its per-intent ceilings applied: the per-trade risk clamp, then the customer's maximum."""
    held = position.quantity if position is not None else 0.0
    requested = intent.target_quantity - held if intent.kind == REBALANCE else intent.quantity
    leverage = intent.leverage if intent.kind == ENTRY else (position.leverage or 1.0)
    c = _Candidate(intent=intent, symbol=intent.symbol, direction=intent.direction, kind=intent.kind,
                   requested=requested, quantity=requested, reference=float(intent.reference_price),
                   stop=float(intent.stop_price), leverage=float(leverage), held=held, rule=rule)
    # the whole position after the order may lose at most the ceiling at its stop; half of it under the halve brake
    full = limits.per_trade_risk * equity / c.distance - held
    room = limits.per_trade_risk * HALVE_FACTOR * equity / c.distance - held if halve else full
    by_brake = halve and requested <= full * (1.0 + _TOL)
    code = BRAKE_ACTIVE if by_brake else RISK_ABOVE_CEILING
    if room <= 0:
        return _refuse(intent, code, ("DRAWDOWN_HALVE: " if by_brake else "")
                       + "the position already carries the per-trade risk the account allows")
    c.cap(code, room)
    if limits.max_position_notional is not None:
        room = limits.max_position_notional / c.reference - held
        if room <= 0:
            return _refuse(intent, CUSTOMER_MAX_POSITION,
                           f"the position is at the customer's maximum {limits.max_position_notional}")
        c.cap(CUSTOMER_MAX_POSITION, room)
    return c


def _filtered(c: _Candidate) -> Union[Tuple[float, List[str]], RefusedIntent]:
    """Exchange filters. Returns the executable quantity and what the filters changed, or the refusal."""
    rule, notes = c.rule, []
    quantity = round_down_to_step(c.quantity, rule.quantity_step)
    if quantity < c.quantity * (1.0 - _TOL):
        notes.append(QUANTITY_STEP)
    ceilings = [m for m in (rule.max_quantity, rule.market_max_quantity if getattr(c.intent, "order_style", MARKET) == MARKET
                            else None) if m is not None]
    if ceilings and quantity > min(ceilings):
        quantity = round_down_to_step(min(ceilings), rule.quantity_step)
        notes.append(EXCHANGE_MAX_QUANTITY)
    scaled = f"after {', '.join(f'{code} x{factor:.6f}' for code, factor in c.limited)}: " if c.limited else ""
    if quantity <= 0:
        return _refuse(c.intent, c.binding(QUANTITY_ROUNDS_TO_ZERO),
                       f"{scaled}{c.quantity} is below the step {rule.quantity_step}")
    if quantity < rule.min_quantity:
        return _refuse(c.intent, c.binding(BELOW_MIN_QUANTITY),
                       f"{scaled}{quantity} is below the minimum quantity {rule.min_quantity}")
    if quantity * c.reference < rule.min_notional:
        return _refuse(c.intent, c.binding(BELOW_MIN_NOTIONAL),
                       f"{scaled}{quantity * c.reference} is below the minimum notional {rule.min_notional}; never raised")
    return quantity, notes


# -------------------------------------------------------------------------------------------------- authorize
def authorize(snapshot: AccountSnapshot, limits: AccountLimits, intents: Union[IntentBatch, Sequence[Intent]],
              rules: Mapping[str, InstrumentRules], *, brake_state: Optional[BrakeFlags]) -> PortfolioAuthorization:
    """Size and admit the intents of one decision of one account. See the module note for the order.

    ``brake_state`` is the result of ``brakes.evaluate`` for this decision close; ``None`` (not known) refuses
    every increase. Raises ``PortfolioDataError`` only for inputs that cannot be interpreted (a batch of another
    account); every trading outcome is a value in the result.
    """
    if isinstance(intents, IntentBatch):
        if intents.account.broker_account_id != snapshot.account_id:
            raise PortfolioDataError("ACCOUNT_MISMATCH", "the batch and the snapshot belong to different accounts")
        items: Tuple[Intent, ...] = intents.intents
    else:
        items = tuple(intents)
    equity = snapshot.equity
    held = {p.symbol: p for p in snapshot.positions}
    before = _book(snapshot, limits, held)

    # 1. reductions, and what they release
    projected, releases, released_margin = dict(held), [], 0.0
    reductions: List[AuthorizedIntent] = []
    refused: List[RefusedIntent] = []
    raising: List[Intent] = []
    seen: Set[str] = set()
    for intent in items:
        if intent.symbol in seen:
            refused.append(_refuse(intent, CONTRADICTORY_ACTION, "a second action on one symbol in one decision"))
            continue
        seen.add(intent.symbol)
        position = held.get(intent.symbol)
        rises = intent.kind in (ENTRY, ADD) or (intent.kind == REBALANCE and position is not None
                                                and intent.target_quantity > position.quantity)
        if rises:
            raising.append(intent)
            continue
        outcome = _reduction(intent, position, rules.get(intent.symbol))
        if isinstance(outcome, RefusedIntent):
            refused.append(outcome)
            continue
        reductions.append(outcome)
        if outcome.kind != STOP_REPLACE:
            releases.append(Release(symbol=outcome.symbol, quantity_after=outcome.position_quantity_after))
            # the smallest margin the release can free: at the highest leverage the position may have
            released_margin += outcome.notional / (position.leverage or limits.max_leverage)
            if outcome.position_quantity_after > 0:
                projected[outcome.symbol] = replace(position, quantity=outcome.position_quantity_after)
            else:
                del projected[outcome.symbol]
    book = _book(snapshot, limits, projected, released_margin)

    # 2.-4. gates, per-intent ceilings, position slots
    halve = bool(brake_state.halve) if brake_state is not None else False
    candidates: List[_Candidate] = []
    slots = set(book.slots)
    for intent in raising:
        position, rule = projected.get(intent.symbol), rules.get(intent.symbol)
        blocked = _gate(intent, position, rule, limits, book, brake_state)
        if blocked is not None:
            refused.append(_refuse(intent, *blocked))
            continue
        candidate = _candidate(intent, position, rule, limits, equity, halve)
        if isinstance(candidate, RefusedIntent):
            refused.append(candidate)
            continue
        if candidate.kind == ENTRY:
            if len(slots) >= limits.max_positions:
                refused.append(_refuse(intent, POSITION_LIMIT, f"{len(slots)} of {limits.max_positions} positions are taken"))
                continue
            slots.add(candidate.symbol)
        candidates.append(candidate)

    # 5. shared budgets: one common factor each, in the frozen order
    def scale(code: str, factor: float) -> float:
        if factor < 1.0:
            for c in candidates:
                c.limited.append((code, factor))
                c.quantity *= factor
        return factor

    risk_factor = scale(OPEN_RISK_CAP, _factor(limits.max_open_risk * equity - sum(book.risk.values()),
                                               sum(c.quantity * c.distance for c in candidates)))
    leverage_factor = scale(LEVERAGE_CAP, _factor(limits.max_leverage * equity - sum(book.notional.values()),
                                                  sum(c.quantity * c.reference for c in candidates)))
    margin_factor = scale(INSUFFICIENT_MARGIN, _factor(book.usable_margin,
                                                       sum(c.quantity * c.reference / c.leverage for c in candidates)))

    # 6. exchange filters; then which accepted increases stand only on the releases
    unreleased = _book(snapshot, limits, held)
    free = {"slots": limits.max_positions - len(unreleased.slots),
            "risk": limits.max_open_risk * equity - sum(unreleased.risk.values()),
            "notional": limits.max_leverage * equity - sum(unreleased.notional.values()),
            "margin": unreleased.usable_margin}
    increases: List[AuthorizedIntent] = []
    for c in candidates:
        outcome = _filtered(c)
        if isinstance(outcome, RefusedIntent):
            refused.append(outcome)
            continue
        quantity, notes = outcome
        stop = conform_stop_price(c.direction, c.stop, c.rule.price_tick)
        if not stop_side_valid(c.direction, c.reference, stop):
            side = "below" if c.direction == LONG else "above"
            refused.append(_refuse(c.intent, INVALID_STOP_GEOMETRY,
                                   f"on the price tick the stop {c.stop} is not {side} the entry {c.reference}"))
            continue
        stated_limit = getattr(c.intent, "limit_price", None)
        limit = conform_limit_price(c.direction, stated_limit, c.rule.price_tick) if stated_limit is not None else None
        if stop != c.stop or (limit is not None and limit != stated_limit):
            notes.append(PRICE_TICK)
        notional, risk_amount = quantity * c.reference, quantity * c.distance
        margin = notional / c.leverage
        free["risk"] -= risk_amount
        free["notional"] -= notional
        free["margin"] -= margin
        if c.kind == ENTRY:
            free["slots"] -= 1
        tolerance = _TOL * max(1.0, equity)
        waits = bool(releases) and (free["slots"] < 0 or min(free["risk"], free["notional"], free["margin"]) < -tolerance)
        increases.append(AuthorizedIntent(
            intent=c.intent, symbol=c.symbol, direction=c.direction, kind=c.kind, increases_risk=True, quantity=quantity,
            requested_quantity=c.requested, position_quantity_after=c.held + quantity, reference_price=c.reference,
            stop_price=stop, limit_price=limit, leverage=c.leverage, notional=notional, risk_amount=risk_amount,
            margin=margin, limited_by=tuple(code for code, _f in c.limited) + tuple(notes), awaits_releases=waits))

    # the portfolio the accepted intents lead to
    after = _book(snapshot, limits, projected, released_margin)
    lines: Dict[str, List[Any]] = {s: [p.direction, p.quantity, p.notional, p.risk_amount] for s, p in projected.items()}
    for a in increases:
        after.slots.add(a.symbol)
        after.risk[a.symbol] = after.risk.get(a.symbol, 0.0) + a.risk_amount
        after.notional[a.symbol] = after.notional.get(a.symbol, 0.0) + a.notional
        after.reserved_margin += a.margin
        line = lines.setdefault(a.symbol, [a.direction, 0.0, 0.0, 0.0])
        line[1], line[2], line[3] = line[1] + a.quantity, line[2] + a.notional, line[3] + a.risk_amount
    allocation = tuple(TargetAllocation(symbol=s, direction=v[0], quantity=v[1], notional=v[2],
                                        fraction_of_equity=v[2] / equity if equity > 0 else 0.0, risk_amount=v[3])
                       for s, v in sorted(lines.items()))
    accepted = tuple(reductions) + tuple(increases)
    scales = BudgetScales(open_risk=risk_factor, leverage=leverage_factor, margin=margin_factor)
    halted = bool(brake_state.halted) if brake_state is not None else False
    digest = stable_hash({
        "snapshot": snapshot.snapshot_hash, "limits": limits.limits_hash,
        "brake": to_plain(brake_state) if brake_state else None,
        "accepted": [[a.symbol, a.kind, a.quantity, a.stop_price, a.limit_price, a.awaits_releases, list(a.limited_by)]
                     for a in accepted],
        "refused": [[r.symbol, r.kind, r.reason] for r in refused], "scales": to_plain(scales)})
    return PortfolioAuthorization(
        account_id=snapshot.account_id, snapshot_hash=snapshot.snapshot_hash, limits_hash=limits.limits_hash,
        accepted=accepted, refused=tuple(refused), releases=tuple(releases), scales=scales, usage_before=before.usage(),
        usage_after=after.usage(), allocation=allocation, close_all_positions=halted, authorization_hash=digest)


__all__ = list(REFUSAL_REASONS) + [
    "REFUSAL_REASONS", "EXCHANGE_MAX_QUANTITY", "QUANTITY_STEP", "PRICE_TICK", "CAPPED_TO_POSITION",
    "RULES_UNAVAILABLE", "PIPELINE_SOURCE", "HALVE_FACTOR", "PortfolioDataError", "AccountLimits", "PortfolioPosition",
    "OpenReservation", "AccountSnapshot", "OpenRisk", "open_risk", "PortfolioUsage", "usage", "Release",
    "AuthorizedIntent", "RefusedIntent", "BudgetScales", "TargetAllocation", "PortfolioAuthorization",
    "rebalance_required", "conform_stop_price", "conform_limit_price", "flatten_intents", "authorize"]
