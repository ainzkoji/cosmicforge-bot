"""Family-neutral trading contracts of the CATI platform pipeline (Section H, Step 3.5).

A strategy family states what it wants as **tagged intents**: one type per action, each validated when it is
constructed, so an object of these types that exists is already internally consistent. Nothing here is specific
to a family, a timeframe, a venue or a direction, and nothing requires a profit target: a family whose frozen
rule exits on a signal says so with ``ExitIntent`` and never invents a target to satisfy a contract.

The types:

``EntryIntent``         open a position that does not exist; a protective stop is mandatory
``AddIntent``           increase an existing position
``ReduceIntent``        partial exit, by quantity or by fraction (exactly one of the two)
``ExitIntent``          close the whole position
``RebalanceIntent``     move an existing position to a target quantity inside a band (up or down)
``StopReplaceIntent``   change the protective stop: trail, tighten, move to break-even, or re-size after a fill

``IntentBatch`` binds several intents of one account to one immutable decision (``DecisionProvenance``) and to
the portfolio constraints that were in force, and refuses contradictions between them. It is the unit the
final execution boundary authorizes; the existing ``contracts.trade_plan.TradePlan`` remains the unit of the
single-entry tail and carries the same provenance in its ``versions`` (see ``provenance_of``).

Quantities are base units and always positive; the direction of the position and the type of the intent say
which way the order goes. Prices are quote units. Fractions are fractions (0.004 = 0.40 %), never percent.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any, FrozenSet, Mapping, Optional, Sequence, Tuple, Union

from app.trading_intelligence.hashing import stable_hash

CONTRACT_VERSION = "1.0.0"

LONG, SHORT = "LONG", "SHORT"
DIRECTIONS: Tuple[str, ...] = (LONG, SHORT)

ENTRY, ADD, REDUCE, EXIT, REBALANCE, STOP_REPLACE = "ENTRY", "ADD", "REDUCE", "EXIT", "REBALANCE", "STOP_REPLACE"
INTENT_KINDS: Tuple[str, ...] = (ENTRY, ADD, REDUCE, EXIT, REBALANCE, STOP_REPLACE)

MARKET, LIMIT, LIMIT_THEN_MARKET = "MARKET", "LIMIT", "LIMIT_THEN_MARKET"
ORDER_STYLES: Tuple[str, ...] = (MARKET, LIMIT, LIMIT_THEN_MARKET)

#: why a stop moves. Only RESIZE and PROTECT may keep the price; none may move it away from the market.
TRAIL, TIGHTEN, BREAK_EVEN, RESIZE = "TRAIL", "TIGHTEN", "BREAK_EVEN", "RESIZE"
STOP_REASONS: Tuple[str, ...] = (TRAIL, TIGHTEN, BREAK_EVEN, RESIZE)

#: what a family may be run as. Only the last two can ever reach an exchange, and only through the boundary.
RESEARCH_SIMULATION, SHADOW, OPERATOR_CERTIFICATION, DEMO, LIVE = (
    "RESEARCH_SIMULATION", "SHADOW", "DEMO_CERTIFICATION", "DEMO", "LIVE")
MODES: Tuple[str, ...] = (RESEARCH_SIMULATION, SHADOW, OPERATOR_CERTIFICATION, DEMO, LIVE)
EXECUTING_MODES: Tuple[str, ...] = (DEMO, LIVE)

TIMEFRAMES: Mapping[str, int] = {"1m": 60_000, "5m": 300_000, "15m": 900_000, "1h": 3_600_000,
                                 "4h": 14_400_000, "1d": 86_400_000}
#: a decision time later than "now" by more than this is a clock fault or a forged record
FUTURE_TOLERANCE_MS = 5_000


class ContractViolation(ValueError):
    """The value cannot exist under these contracts. ``code`` is stable and machine-readable."""

    def __init__(self, code: str, message: str = "") -> None:
        super().__init__(f"{code}: {message}" if message else code)
        self.code = code


def _positive(code: str, name: str, value: Any, *, allow_zero: bool = False) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ContractViolation(code, f"{name} is not a number: {value!r}")
    v = float(value)
    if not math.isfinite(v) or v < 0 or (v == 0 and not allow_zero):
        raise ContractViolation(code, f"{name} must be a finite {'non-negative' if allow_zero else 'positive'} number: {value!r}")
    return v


def _text(code: str, name: str, value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ContractViolation(code, f"{name} is required")
    return value


def _symbol(value: Any) -> str:
    s = _text("UNSUPPORTED_INSTRUMENT", "symbol", value)
    if s != s.upper() or not s.isalnum():
        raise ContractViolation("UNSUPPORTED_INSTRUMENT", f"symbol {value!r} is not a canonical symbol")
    return s


def _direction(value: Any) -> str:
    if value not in DIRECTIONS:
        raise ContractViolation("INVALID_DIRECTION", f"{value!r} is not LONG or SHORT")
    return value


def stop_side_valid(direction: str, reference_price: float, stop_price: float) -> bool:
    """A long is protected below the price, a short above it. Equal is not protection."""
    return stop_price < reference_price if direction == LONG else stop_price > reference_price


def stop_distance_fraction(reference_price: float, stop_price: float) -> float:
    return abs(float(reference_price) - float(stop_price)) / float(reference_price)


# ------------------------------------------------------------------------------------------------- intents
@dataclass(frozen=True)
class EntryIntent:
    symbol: str
    direction: str
    quantity: float
    reference_price: float              # the price the decision was made at
    stop_price: float                   # mandatory: a position is never opened without stated risk
    risk_fraction: float                # share of equity lost if the stop is hit, as sized by the family
    leverage: float = 1.0               # account leverage the family assumes for this instrument
    take_profit_price: Optional[float] = None   # optional by design: signal-exit families have none
    order_style: str = MARKET
    limit_price: Optional[float] = None
    max_slippage_bps: Optional[float] = None    # bound of a market order or of the market fallback
    tag: str = ""
    kind: str = field(default=ENTRY, init=False)

    def __post_init__(self) -> None:
        _symbol(self.symbol)
        _direction(self.direction)
        _positive("INVALID_QUANTITY", "quantity", self.quantity)
        ref = _positive("INVALID_PRICE", "reference_price", self.reference_price)
        if self.stop_price is None or self.risk_fraction is None:
            raise ContractViolation("MISSING_RISK_INFORMATION", "an entry states its stop and its risk")
        stop = _positive("INVALID_STOP_GEOMETRY", "stop_price", self.stop_price)
        if not stop_side_valid(self.direction, ref, stop):
            raise ContractViolation("INVALID_STOP_GEOMETRY",
                                    f"{self.direction} entry at {ref} cannot be protected by a stop at {stop}")
        _positive("MISSING_RISK_INFORMATION", "risk_fraction", self.risk_fraction)
        if float(self.risk_fraction) >= 1.0:
            raise ContractViolation("MISSING_RISK_INFORMATION", "risk_fraction is a fraction of equity below 1")
        if _positive("UNAUTHORIZED_LEVERAGE", "leverage", self.leverage) < 1.0:
            raise ContractViolation("UNAUTHORIZED_LEVERAGE", "leverage below 1 is not a futures setting")
        if self.take_profit_price is not None:
            tp = _positive("INVALID_TARGET_GEOMETRY", "take_profit_price", self.take_profit_price)
            if (tp <= ref) if self.direction == LONG else (tp >= ref):
                raise ContractViolation("INVALID_TARGET_GEOMETRY", "a target lies on the profit side of the entry")
        _order_style(self.order_style, self.limit_price, self.max_slippage_bps)

    @property
    def stop_distance(self) -> float:
        return stop_distance_fraction(self.reference_price, self.stop_price)

    @property
    def notional(self) -> float:
        return float(self.quantity) * float(self.reference_price)


@dataclass(frozen=True)
class AddIntent:
    symbol: str
    direction: str
    quantity: float                     # the increase
    reference_price: float
    stop_price: float                   # the stop that will cover the enlarged position
    order_style: str = MARKET
    limit_price: Optional[float] = None
    max_slippage_bps: Optional[float] = None
    tag: str = ""
    kind: str = field(default=ADD, init=False)

    def __post_init__(self) -> None:
        _symbol(self.symbol)
        _direction(self.direction)
        _positive("INVALID_QUANTITY", "quantity", self.quantity)
        ref = _positive("INVALID_PRICE", "reference_price", self.reference_price)
        if self.stop_price is None:
            raise ContractViolation("MISSING_RISK_INFORMATION", "an increase states the stop that covers it")
        stop = _positive("INVALID_STOP_GEOMETRY", "stop_price", self.stop_price)
        if not stop_side_valid(self.direction, ref, stop):
            raise ContractViolation("INVALID_STOP_GEOMETRY", f"{self.direction} position at {ref} with a stop at {stop}")
        _order_style(self.order_style, self.limit_price, self.max_slippage_bps)

    @property
    def notional(self) -> float:
        return float(self.quantity) * float(self.reference_price)


@dataclass(frozen=True)
class ReduceIntent:
    symbol: str
    direction: str                      # of the position being reduced
    reason: str
    quantity: Optional[float] = None
    fraction: Optional[float] = None    # of the position held when the order is built
    reference_price: Optional[float] = None
    order_style: str = MARKET
    limit_price: Optional[float] = None
    max_slippage_bps: Optional[float] = None
    tag: str = ""
    kind: str = field(default=REDUCE, init=False)

    def __post_init__(self) -> None:
        _symbol(self.symbol)
        _direction(self.direction)
        _text("MISSING_REASON", "reason", self.reason)
        if (self.quantity is None) == (self.fraction is None):
            raise ContractViolation("CONTRADICTORY_ACTION", "a reduction is stated by quantity or by fraction, not both")
        if self.quantity is not None:
            _positive("INVALID_QUANTITY", "quantity", self.quantity)
        elif not 0.0 < _positive("INVALID_QUANTITY", "fraction", self.fraction) < 1.0:
            raise ContractViolation("INVALID_QUANTITY", "a partial exit takes a fraction strictly between 0 and 1")
        if self.reference_price is not None:
            _positive("INVALID_PRICE", "reference_price", self.reference_price)
        _order_style(self.order_style, self.limit_price, self.max_slippage_bps)


@dataclass(frozen=True)
class ExitIntent:
    symbol: str
    direction: str                      # of the position being closed
    reason: str                         # SIGNAL | STOP | BRAKE | KILL_SWITCH | DATA_GAP | ...
    reference_price: Optional[float] = None
    tag: str = ""
    kind: str = field(default=EXIT, init=False)

    def __post_init__(self) -> None:
        _symbol(self.symbol)
        _direction(self.direction)
        _text("MISSING_REASON", "reason", self.reason)
        if self.reference_price is not None:
            _positive("INVALID_PRICE", "reference_price", self.reference_price)


@dataclass(frozen=True)
class RebalanceIntent:
    symbol: str
    direction: str
    current_quantity: float             # what the decision saw
    target_quantity: float              # > 0: a rebalance never opens or closes a position
    reference_price: float
    stop_price: float
    band: float                         # the drift from target that triggered it
    tag: str = ""
    kind: str = field(default=REBALANCE, init=False)

    def __post_init__(self) -> None:
        _symbol(self.symbol)
        _direction(self.direction)
        cur = _positive("INVALID_QUANTITY", "current_quantity", self.current_quantity)
        tgt = _positive("INVALID_QUANTITY", "target_quantity", self.target_quantity)
        if cur == tgt:
            raise ContractViolation("CONTRADICTORY_ACTION", "a rebalance to the quantity already held is not an action")
        ref = _positive("INVALID_PRICE", "reference_price", self.reference_price)
        stop = _positive("INVALID_STOP_GEOMETRY", "stop_price", self.stop_price)
        if not stop_side_valid(self.direction, ref, stop):
            raise ContractViolation("INVALID_STOP_GEOMETRY", f"{self.direction} position at {ref} with a stop at {stop}")
        _positive("INVALID_BAND", "band", self.band, allow_zero=True)

    @property
    def delta(self) -> float:
        """Positive: increase; negative: decrease (base units)."""
        return float(self.target_quantity) - float(self.current_quantity)


@dataclass(frozen=True)
class StopReplaceIntent:
    symbol: str
    direction: str
    reason: str                         # TRAIL | TIGHTEN | BREAK_EVEN | RESIZE
    new_stop_price: float
    current_stop_price: Optional[float]  # None only when protection is being restored
    quantity: Optional[float] = None    # None: cover the whole position as held when the order is built
    entry_price: Optional[float] = None  # required for BREAK_EVEN
    tag: str = ""
    kind: str = field(default=STOP_REPLACE, init=False)

    def __post_init__(self) -> None:
        _symbol(self.symbol)
        _direction(self.direction)
        if self.reason not in STOP_REASONS:
            raise ContractViolation("INVALID_STOP_REASON", f"{self.reason!r} is not one of {STOP_REASONS}")
        new = _positive("INVALID_STOP_GEOMETRY", "new_stop_price", self.new_stop_price)
        if self.quantity is not None:
            _positive("INVALID_QUANTITY", "quantity", self.quantity)
        if self.current_stop_price is not None:
            cur = _positive("INVALID_STOP_GEOMETRY", "current_stop_price", self.current_stop_price)
            widened = new < cur if self.direction == LONG else new > cur
            if widened:
                raise ContractViolation("STOP_WIDENING_REFUSED",
                                        f"{self.direction} stop {cur} -> {new} would increase the loss at the stop")
            if new == cur and self.reason != RESIZE:
                raise ContractViolation("CONTRADICTORY_ACTION", "a stop replacement at the same price changes nothing")
        if self.reason == BREAK_EVEN:
            if self.entry_price is None:
                raise ContractViolation("MISSING_RISK_INFORMATION", "break-even needs the entry price")
            entry = _positive("INVALID_PRICE", "entry_price", self.entry_price)
            if (new < entry) if self.direction == LONG else (new > entry):
                raise ContractViolation("INVALID_STOP_GEOMETRY", "a break-even stop is at or beyond the entry price")


Intent = Union[EntryIntent, AddIntent, ReduceIntent, ExitIntent, RebalanceIntent, StopReplaceIntent]
#: intents that can increase exposure; everything else only reduces risk and is never blocked by an entry gate
RISK_INCREASING: Tuple[str, ...] = (ENTRY, ADD)


def increases_risk(intent: Intent) -> bool:
    return intent.kind in RISK_INCREASING or (intent.kind == REBALANCE and intent.delta > 0)


def _order_style(style: Any, limit_price: Any, max_slippage_bps: Any) -> None:
    if style not in ORDER_STYLES:
        raise ContractViolation("INVALID_ORDER_STYLE", f"{style!r} is not one of {ORDER_STYLES}")
    if style == MARKET:
        if limit_price is not None:
            raise ContractViolation("CONTRADICTORY_ACTION", "a market order carries no limit price")
    else:
        _positive("INVALID_PRICE", "limit_price", limit_price)
    if style == LIMIT_THEN_MARKET and max_slippage_bps is None:
        raise ContractViolation("MISSING_RISK_INFORMATION", "a market fallback is bounded by max_slippage_bps")
    if max_slippage_bps is not None:
        _positive("INVALID_PRICE", "max_slippage_bps", max_slippage_bps, allow_zero=True)


# ---------------------------------------------------------------------------------------- provenance / batch
@dataclass(frozen=True)
class DecisionProvenance:
    """Where an intent comes from. Every field is checked against the registry and the immutable decision
    record by the final boundary (``pipeline.provenance``); none of them is trusted because it is present."""
    family_id: str
    family_version: str
    mandate_id: str
    hypothesis_id: str
    rule_hash: str
    decision_id: str
    decision_hash: str
    decision_time_ms: int
    data_snapshot_id: str
    timeframe: str
    mode: str

    def __post_init__(self) -> None:
        for name in ("family_id", "family_version", "mandate_id", "hypothesis_id", "rule_hash", "decision_id",
                     "decision_hash", "data_snapshot_id"):
            _text("MISSING_PROVENANCE", name, getattr(self, name))
        if self.timeframe not in TIMEFRAMES:
            raise ContractViolation("INCONSISTENT_TIMEFRAME", f"unknown timeframe {self.timeframe!r}")
        if self.mode not in MODES:
            raise ContractViolation("INVALID_MODE", f"{self.mode!r} is not one of {MODES}")
        if isinstance(self.decision_time_ms, bool) or not isinstance(self.decision_time_ms, int) or self.decision_time_ms <= 0:
            raise ContractViolation("MISSING_PROVENANCE", "decision_time_ms is an epoch in milliseconds")


@dataclass(frozen=True)
class AccountRef:
    user_id: str
    broker_account_id: str
    venue: str
    environment: str                    # LIVE | DEMO

    def __post_init__(self) -> None:
        _text("INVALID_ACCOUNT_OWNERSHIP", "user_id", self.user_id)
        _text("INVALID_ACCOUNT_OWNERSHIP", "broker_account_id", self.broker_account_id)
        _text("INVALID_ACCOUNT_OWNERSHIP", "venue", self.venue)
        if self.environment not in ("LIVE", "DEMO"):
            raise ContractViolation("UNAUTHORIZED_ENVIRONMENT", f"{self.environment!r} is not LIVE or DEMO")


@dataclass(frozen=True)
class PortfolioConstraints:
    """The account-level limits in force when the decision was made (fractions of equity)."""
    max_positions: int
    max_open_risk: float
    max_leverage: float
    per_trade_risk_ceiling: float       # the engine's ceiling (0.40 % today), not the profile's wish
    max_entry_stop_distance: Optional[float]  # the engine's widest entry stop (15 % today); None = not applied
    equity_basis: float

    def __post_init__(self) -> None:
        if isinstance(self.max_positions, bool) or not isinstance(self.max_positions, int) or self.max_positions < 1:
            raise ContractViolation("INVALID_CONSTRAINTS", "max_positions is a positive integer")
        for name in ("max_open_risk", "max_leverage", "per_trade_risk_ceiling", "equity_basis"):
            _positive("INVALID_CONSTRAINTS", name, getattr(self, name))
        if self.max_entry_stop_distance is not None:
            _positive("INVALID_CONSTRAINTS", "max_entry_stop_distance", self.max_entry_stop_distance)


@dataclass(frozen=True)
class IntentBatch:
    """Several simultaneous intents of ONE account from ONE decision. Construction is the validation."""
    provenance: DecisionProvenance
    account: AccountRef
    intents: Tuple[Intent, ...]
    constraints: PortfolioConstraints
    instruments: FrozenSet[str]         # what the family may trade on this venue at decision time
    created_at_ms: int
    expires_at_ms: int
    intent_timeframe: Optional[str] = None  # defaults to the provenance timeframe

    def __post_init__(self) -> None:
        if not isinstance(self.intents, tuple):
            raise ContractViolation("INVALID_BATCH", "intents is a tuple")
        p = self.provenance
        frame = self.intent_timeframe or p.timeframe
        if frame != p.timeframe:
            raise ContractViolation("INCONSISTENT_TIMEFRAME", f"intents on {frame} from a {p.timeframe} decision")
        if self.created_at_ms + FUTURE_TOLERANCE_MS < p.decision_time_ms:
            raise ContractViolation("FUTURE_DATED_DECISION", "the decision is dated after the batch that carries it")
        if self.expires_at_ms <= p.decision_time_ms:
            raise ContractViolation("STALE_DECISION", "the batch expires before its decision was made")
        if self.created_at_ms >= self.expires_at_ms:
            raise ContractViolation("STALE_DECISION", "the decision had expired when the batch was built")
        seen = {}
        for intent in self.intents:
            if getattr(intent, "kind", None) not in INTENT_KINDS:
                raise ContractViolation("INVALID_BATCH", f"{intent!r} is not an intent")
            if intent.symbol not in self.instruments:
                raise ContractViolation("UNSUPPORTED_INSTRUMENT", f"{intent.symbol} is not tradable for this family here")
            if intent.symbol in seen:
                # one position per symbol (one-way mode): two actions on it in one decision cannot both be meant
                raise ContractViolation("CONTRADICTORY_ACTION",
                                        f"{intent.symbol}: {seen[intent.symbol]} and {intent.kind} in one decision")
            seen[intent.symbol] = intent.kind
            if intent.kind == ENTRY:
                c = self.constraints
                if intent.leverage > c.max_leverage:
                    raise ContractViolation("UNAUTHORIZED_LEVERAGE",
                                            f"{intent.symbol}: leverage {intent.leverage} above the account's {c.max_leverage}")
                if intent.risk_fraction > c.per_trade_risk_ceiling * (1.0 + 1e-9):
                    raise ContractViolation("RISK_ABOVE_CEILING",
                                            f"{intent.symbol}: risk {intent.risk_fraction} above the ceiling {c.per_trade_risk_ceiling}")
                if c.max_entry_stop_distance is not None and intent.stop_distance > c.max_entry_stop_distance * (1.0 + 1e-9):
                    # never clipped: a stop wider than the engine allows is a refused entry, not a moved stop
                    raise ContractViolation("ENTRY_STOP_TOO_WIDE",
                                            f"{intent.symbol}: stop distance {intent.stop_distance:.4f} above {c.max_entry_stop_distance}")
        entries = sum(1 for i in self.intents if i.kind == ENTRY)
        if entries > self.constraints.max_positions:
            raise ContractViolation("POSITION_LIMIT", f"{entries} entries above the limit {self.constraints.max_positions}")

    def stale(self, now_ms: int) -> bool:
        return int(now_ms) >= self.expires_at_ms

    def future_dated(self, now_ms: int) -> bool:
        return self.provenance.decision_time_ms > int(now_ms) + FUTURE_TOLERANCE_MS

    @property
    def risk_increasing(self) -> Tuple[Intent, ...]:
        return tuple(i for i in self.intents if increases_risk(i))

    @property
    def batch_hash(self) -> str:
        return stable_hash(to_plain(self))


def to_plain(value: Any) -> Any:
    """Dataclasses, tuples and frozensets to JSON-compatible values, deterministically."""
    if hasattr(value, "__dataclass_fields__"):
        return {k: to_plain(getattr(value, k)) for k in value.__dataclass_fields__}
    if isinstance(value, (frozenset, set)):
        return sorted(to_plain(v) for v in value)
    if isinstance(value, (tuple, list)):
        return [to_plain(v) for v in value]
    if isinstance(value, Mapping):
        return {str(k): to_plain(v) for k, v in sorted(value.items())}
    return value


_INTENT_TYPES = {ENTRY: EntryIntent, ADD: AddIntent, REDUCE: ReduceIntent, EXIT: ExitIntent,
                 REBALANCE: RebalanceIntent, STOP_REPLACE: StopReplaceIntent}


def intent_from_plain(document: Mapping[str, Any]) -> Intent:
    """Rebuild (and therefore re-validate) an intent stored as JSON."""
    kind = document.get("kind")
    if kind not in _INTENT_TYPES:
        raise ContractViolation("INVALID_BATCH", f"unknown intent kind {kind!r}")
    return _INTENT_TYPES[kind](**{k: v for k, v in document.items() if k != "kind"})


#: keys a ``TradePlan.versions`` tuple carries so the single-entry tail has the same provenance as a batch
PLAN_PROVENANCE_KEYS: Tuple[str, ...] = ("family_version", "mandate_id", "hypothesis_id", "family_rule_hash",
                                         "decision_hash", "data_snapshot_id", "timeframe")


def provenance_of(plan: Any) -> Mapping[str, Any]:
    """The provenance fields of an ``IntentBatch`` or of an existing ``TradePlan``, as one mapping. A field the
    object does not carry is absent from the mapping; the boundary refuses on absence, it never assumes."""
    p = getattr(plan, "provenance", None)
    if isinstance(p, DecisionProvenance):
        out = dict(to_plain(p))
        out.update(user_id=plan.account.user_id, broker_account_id=plan.account.broker_account_id,
                   environment=plan.account.environment, venue=plan.account.venue, expires_at_ms=plan.expires_at_ms)
        return out
    versions = dict(getattr(plan, "versions", ()) or ())
    out = {"family_id": getattr(plan, "setup_family", None), "decision_id": getattr(plan, "source_candidate_id", None),
           "decision_time_ms": getattr(plan, "decision_time", None), "mode": getattr(plan, "mode", None),
           "user_id": getattr(plan, "user_id", None), "broker_account_id": getattr(plan, "broker_account_id", None),
           "environment": getattr(plan, "environment", None), "venue": getattr(plan, "venue", None),
           "expires_at_ms": getattr(plan, "plan_expiry_time", None)}
    out["family_version"] = versions.get("family_version") or getattr(plan, "setup_version", None)
    out["rule_hash"] = versions.get("family_rule_hash")
    for key in ("mandate_id", "hypothesis_id", "decision_hash", "data_snapshot_id", "timeframe"):
        out[key] = versions.get(key)
    return {k: v for k, v in out.items() if v not in (None, "")}


__all__ = ["CONTRACT_VERSION", "ContractViolation", "LONG", "SHORT", "DIRECTIONS", "ENTRY", "ADD", "REDUCE", "EXIT",
           "REBALANCE", "STOP_REPLACE", "INTENT_KINDS", "MARKET", "LIMIT", "LIMIT_THEN_MARKET", "ORDER_STYLES", "TRAIL",
           "TIGHTEN", "BREAK_EVEN", "RESIZE", "STOP_REASONS", "RESEARCH_SIMULATION", "SHADOW",
           "OPERATOR_CERTIFICATION", "DEMO", "LIVE", "MODES", "EXECUTING_MODES", "TIMEFRAMES", "FUTURE_TOLERANCE_MS",
           "EntryIntent", "AddIntent", "ReduceIntent", "ExitIntent", "RebalanceIntent", "StopReplaceIntent", "Intent",
           "RISK_INCREASING", "increases_risk", "DecisionProvenance", "AccountRef", "PortfolioConstraints",
           "IntentBatch", "to_plain", "intent_from_plain", "provenance_of", "PLAN_PROVENANCE_KEYS", "stop_side_valid",
           "stop_distance_fraction"]
