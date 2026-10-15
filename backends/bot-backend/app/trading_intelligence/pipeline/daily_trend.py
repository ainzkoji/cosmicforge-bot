"""The daily trend family inside the CATI platform pipeline (Section H, Step 3.6).

The strategy is not written here. Signals, stop distances and the universe come from ``families.daily_trend.
rules`` through the evaluator's own ``prepare``; the selection, sizing and scaling from ``targets.
decide_targets``; the orders from ``targets.orders_from_targets``; the exchange filters from ``targets.
apply_exchange_filters``; the constants from ``spec``; and the mandate is verified with ``verify_mandate``
before every cycle. This module only does what a pipeline adds to a rule:

* a **market stage**, computed once per cycle for the whole platform: the causal features of the decision
  day, the point-in-time universe, the ranked candidates, a hash of exactly the data that was used;
* an **account stage**, computed per account from that one market stage: the account's shadow book is moved
  through the day that just closed and the frozen decision is taken for the next open;
* the translation of the frozen orders into the family-neutral intents of ``pipeline.contracts``.

**The shadow book** (``advance_day``) is an independent, incremental implementation of the evaluator's daily
accounting: gap exits, the 00:00 funding event, fills of yesterday's orders at the open, stops, later funding,
the mark to the close, the three brakes and the stop ratchet, in that order, with the frozen base costs. It
keeps its state in the database between days. The evaluator computes the same thing from scratch over the
whole period; ``pipeline.shadow_compare`` holds the two against each other every day. They share the rule
functions and nothing else, which is what makes the comparison a check of the pipeline rather than of itself.

No selection rule is added. In particular nothing here prefers a position already held over a stronger new
candidate: ``decide_targets`` ranks by strength, then liquidity, then symbol, and that is all.

The family is SHADOW_ONLY. Nothing in this module can reach an exchange: it imports no adapter, no client and
no execution boundary.
"""
from __future__ import annotations

import hashlib
import math
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

import numpy as np

from app.trading_intelligence.families.daily_trend import spec
from app.trading_intelligence.families.daily_trend import targets as T
from app.trading_intelligence.hashing import stable_hash

from . import contracts as C

FAMILY_ID = spec.FAMILY_ID
TIMEFRAME = "1d"
ADAPTER_VERSION = "daily-trend-pipeline-1"
DAY_MS = 86_400_000
_EPOCH = date(1970, 1, 1)

EXIT_SIGNAL, EXIT_STOP, EXIT_STOP_GAP, EXIT_DATA_GAP, EXIT_BRAKE = "SIGNAL", "STOP", "STOP_GAP", "DATA_GAP", "BRAKE"
#: the family or the platform is stopped for this account: nothing new, nothing enlarged
REJECT_ENTRIES_BLOCKED = "ENTRIES_BLOCKED"

#: share of yesterday's traded in-scope symbols that must have a bar on the decision day
MIN_BAR_COVERAGE = 0.90
STAGES: Tuple[str, ...] = (
    "MARKET_DATA", "POINT_IN_TIME_UNIVERSE", "MARKET_STATE", "REGIME", "FAMILY_SETUPS", "RULE_EVIDENCE",
    "TRANSACTION_COSTS", "TRADE_ECONOMICS", "RISK_AND_GOVERNANCE_VETOES", "CROSS_INSTRUMENT_RANKING",
    "PLATFORM_TARGET_PORTFOLIO", "ACCOUNT_ELIGIBLE_PORTFOLIO", "EXECUTION_BOUNDARY", "PERSISTENCE_AND_MONITORING")


class DataNotFresh(RuntimeError):
    """The decision day is not (yet) usable. ``code`` says why; the cycle is skipped, never guessed."""

    def __init__(self, code: str, message: str = "") -> None:
        super().__init__(f"{code}: {message}" if message else code)
        self.code = code


def day_of(text: str) -> int:
    return (date.fromisoformat(text) - _EPOCH).days


def day_text(day: int) -> str:
    return (_EPOCH + timedelta(days=int(day))).isoformat()


def close_ms(day: int) -> int:
    """The instant the daily bar of ``day`` closes (00:00 UTC of the next day): the decision time."""
    return (int(day) + 1) * DAY_MS


def cycle_key_due(now_ms: int) -> str:
    """The last day whose bar has closed at ``now_ms``."""
    return day_text(int(now_ms) // DAY_MS - 1)


def verify_mandate(register: Any) -> Dict[str, Any]:
    """Raises when the frozen specification, an amendment or the rule set in code differs from what was
    registered. Called before every cycle and by the registry before any promotion."""
    from app.trading_intelligence.research.governance.mandates import verify_mandate as _verify

    return _verify(register, spec.MANDATE_ID, rule_artifact=spec.RULE_ARTIFACT)


# ---------------------------------------------------------------------------------------------- market stage
@dataclass
class MarketDecision:
    """What the platform knows at the close of ``day``, the same for every account."""
    family_id: str
    day: int
    row: int
    features: Any                       # research.evaluator.simulator.Features (causal, built from <= day)
    universe: Tuple[str, ...]
    candidates: Tuple[T.Candidate, ...]
    data_snapshot_id: str
    freshness: Dict[str, Any]
    stages: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    _index: Dict[str, int] = field(default_factory=dict, repr=False)

    @property
    def cycle_key(self) -> str:
        return day_text(self.day)

    def index(self, symbol: str) -> int:
        return self._index[symbol]

    def summary(self) -> Dict[str, Any]:
        ranked = sorted((c for c in self.candidates if c.strength > 0 and math.isfinite(c.stop_distance)
                         and c.stop_distance > 0), key=lambda c: (-c.strength, -c.volume, c.symbol))
        return {"family_id": self.family_id, "day": self.day, "cycle_key": self.cycle_key,
                "data_snapshot_id": self.data_snapshot_id, "universe": list(self.universe),
                "ranking": [{"symbol": c.symbol, "strength": c.strength, "stop_distance": c.stop_distance,
                             "volume": c.volume} for c in ranked],
                "freshness": dict(self.freshness), "stages": {k: dict(v) for k, v in self.stages.items()}}


def snapshot_id(panel: Any) -> str:
    """Identity of exactly the bars and funding records a decision used."""
    h = hashlib.sha256()
    h.update(np.asarray(panel.days, dtype="<i8").tobytes())
    h.update("\x00".join(panel.symbols).encode("utf-8"))
    for name in sorted(panel.fields):
        h.update(name.encode("utf-8"))
        h.update(np.ascontiguousarray(panel.fields[name], dtype="<f8").tobytes())
    return h.hexdigest()


def market_stage(panel: Any, metadata: Mapping[str, Any], day: int, *,
                 min_bar_coverage: float = MIN_BAR_COVERAGE) -> MarketDecision:
    """``panel`` may extend past ``day``; it is cut at ``day`` before anything is computed, so no later bar
    can reach a signal. Raises ``DataNotFresh`` when the day's bars are not there."""
    from app.trading_intelligence.research.evaluator import simulator as S

    # a panel that already ends on or before the day holds nothing to cut away: it is used as it is (``prepare``
    # copies every field before it computes), which saves one whole copy of the history
    cut = panel if len(panel.days) and int(np.max(panel.days)) <= int(day) else panel.truncated(int(day))
    if len(cut.days) == 0 or int(cut.days[-1]) != int(day):
        raise DataNotFresh("DECISION_DAY_HAS_NO_BARS", day_text(day))
    feat = S.prepare(cut, metadata)
    i = len(feat.days) - 1
    traded_before = np.isfinite(feat.close[i - 1]) & feat.in_scope if i > 0 else np.zeros(len(feat.symbols), bool)
    present = np.isfinite(feat.close[i]) & traded_before
    coverage = float(present.sum() / traded_before.sum()) if traded_before.sum() else 0.0
    if i > 0 and coverage < min_bar_coverage:
        raise DataNotFresh("DECISION_DAY_INCOMPLETE", f"{day_text(day)}: {coverage:.3f} of yesterday's symbols have a bar")
    members = np.flatnonzero(feat.member[i])
    sym = feat.symbols
    candidates = tuple(T.Candidate(sym[j], float(feat.strength[i, j]), float(feat.stop_distance[i, j]),
                                   float(feat.volume[i, j]), 1.0) for j in members)
    stages = {
        "MARKET_DATA": {"status": "EXECUTED", "days": int(len(feat.days)), "symbols": int(len(sym)), "bar_coverage": coverage},
        "POINT_IN_TIME_UNIVERSE": {"status": "EXECUTED", "members": int(len(members)),
                                   "in_scope_symbols": int(feat.in_scope.sum())},
        "MARKET_STATE": {"status": "NOT_APPLICABLE", "reason": "the frozen daily rule reads prices only; it has no market-state input"},
        "REGIME": {"status": "NOT_APPLICABLE", "reason": "the frozen daily rule has no regime input"},
        "FAMILY_SETUPS": {"status": "EXECUTED", "candidates": len(candidates),
                          "with_signal": sum(1 for c in candidates if c.strength > 0)},
        "RULE_EVIDENCE": {"status": "EXECUTED", "rule_hash": stable_hash(dict(spec.RULE_ARTIFACT)),
                          "lookbacks": list(spec.LOOKBACKS)},
        "TRANSACTION_COSTS": {"status": "EXECUTED", "taker_fee": spec.SPECIFICATION["taker_fee"],
                              "slippage": spec.SPECIFICATION["slippage"], "source": "FROZEN_SPECIFICATION"},
        "CROSS_INSTRUMENT_RANKING": {"status": "EXECUTED", "order": "strength desc, 30-day median volume desc, symbol"},
    }
    return MarketDecision(family_id=FAMILY_ID, day=int(day), row=i, features=feat,
                          universe=tuple(sym[j] for j in members), candidates=candidates,
                          data_snapshot_id=snapshot_id(cut),
                          freshness={"bar_coverage": coverage, "decision_time_ms": close_ms(day)}, stages=stages,
                          _index={s: j for j, s in enumerate(sym)})


# --------------------------------------------------------------------------------------------- shadow book
@dataclass
class Position:
    quantity: float
    stop: float
    mark: float
    entry_day: int
    entry_price: float
    entry_stop_distance: float


@dataclass
class Book:
    """One account's shadow holdings of this family. ``positions`` keeps the order positions were opened in:
    the daily accounting sums in that order, as the evaluator does."""
    risk_level: str
    policy: str
    initial_equity: float
    equity: float
    peak: float
    first_day: int
    last_day: Optional[int] = None
    paused: bool = False
    halved: bool = False
    halted: bool = False
    positions: Dict[str, Position] = field(default_factory=dict)
    pending: List[Dict[str, Any]] = field(default_factory=list)

    @property
    def level(self) -> T.RiskLevel:
        return T.risk_level(self.risk_level, self.policy)


def new_book(risk_level: str, policy: str, initial_equity: float, first_day: int) -> Book:
    if policy not in (T.MANDATE, T.EXECUTABLE):
        raise ValueError(f"unknown sizing policy {policy!r}")
    if not math.isfinite(float(initial_equity)) or float(initial_equity) <= 0:
        raise ValueError("a shadow book starts with positive equity")
    return Book(risk_level=risk_level, policy=policy, initial_equity=float(initial_equity),
                equity=float(initial_equity), peak=float(initial_equity), first_day=int(first_day))


def advance_day(book: Book, market: MarketDecision) -> Dict[str, Any]:
    """Move the book through the day that just closed. Returns the fills, events, rejections and the day's
    figures. The order of the steps and of the sums is the evaluator's."""
    feat, i, day, level = market.features, market.row, market.day, book.level
    if book.last_day is not None and day != book.last_day + 1:
        raise ValueError(f"the book is at {day_text(book.last_day)}; it cannot jump to {day_text(day)}")
    if book.last_day is None and day != book.first_day:
        raise ValueError(f"the first day of this book is {day_text(book.first_day)}, not {day_text(day)}")
    fee_rate, slip_rate = spec.SPECIFICATION["taker_fee"], spec.SPECIFICATION["slippage"]
    gap_multiple = spec.SPECIFICATION["stress_cost_multiple"] / 1.0
    missing_rate = spec.INTERPRETATION["missing_funding_rate_per_8h"]
    fills: List[Dict[str, Any]] = []
    events: List[Dict[str, Any]] = []
    rejections: List[Dict[str, Any]] = []
    sums = {"pnl_price": 0.0, "fees": 0.0, "slippage": 0.0, "funding": 0.0}
    start_equity = book.equity
    order_of = market.index

    def post(key: str, amount: float) -> None:
        sums[key] += amount
        book.equity += amount if key == "pnl_price" else -amount

    def cost(signed_qty: float, price: float, multiple: float = 1.0) -> float:
        fill = price * (1.0 + math.copysign(slip_rate * multiple, signed_qty))
        post("slippage", abs(signed_qty) * price * slip_rate * multiple)
        post("fees", abs(signed_qty) * fill * fee_rate * multiple)
        return fill

    def close(symbol: str, price: float, reason: str, multiple: float = 1.0) -> None:
        pos = book.positions.pop(symbol)
        post("pnl_price", pos.quantity * (price - pos.mark))
        fills.append({"day": day, "symbol": symbol, "kind": reason, "quantity": -pos.quantity,
                      "price": cost(-pos.quantity, price, multiple)})

    def by_index(symbols: Sequence[str]) -> List[str]:
        return sorted(symbols, key=order_of)

    # 1. held coins without a bar
    for s in by_index([s for s in book.positions if math.isnan(feat.open[i, order_of(s)])]):
        events.append({"day": day, "symbol": s, "event": EXIT_DATA_GAP, "price": book.positions[s].mark})
        close(s, book.positions[s].mark, EXIT_DATA_GAP, gap_multiple)
    # 2. the 00:00 funding event on what was held coming into the day; then mark to the open
    for s, pos in book.positions.items():
        j = order_of(s)
        price, known = feat.open[i, j], feat.funding_hours[i, j] > 0
        amount = pos.quantity * price * (feat.funding_midnight[i, j] if known else missing_rate)
        if amount:
            post("funding", amount)
        post("pnl_price", pos.quantity * (price - pos.mark))
        pos.mark = float(price)
    # 3. yesterday's orders fill at the open
    for o in book.pending:
        order = T.Order(o["symbol"], o["kind"], o["quantity"], o["target_notional"], o["stop_distance"])
        j = order_of(order.symbol)
        price = feat.open[i, j]
        if math.isnan(price):
            if order.kind != T.EXIT:
                rejections.append({"day": day, "stage": "FILL", "symbol": order.symbol, "reason": T.REJECT_NO_BAR})
            continue
        pos = book.positions.get(order.symbol)
        if order.kind != T.ENTRY and pos is None:
            continue
        qty, reason = T.apply_exchange_filters(order, price=float(price), step=float(feat.step[j]) or None,
                                               min_notional=float(feat.min_notional[j]) or None)
        if reason:
            rejections.append({"day": day, "stage": "FILL", "symbol": order.symbol, "reason": reason})
            continue
        if order.kind == T.EXIT:
            close(order.symbol, float(price), EXIT_BRAKE if book.halted else EXIT_SIGNAL)
            continue
        fill = cost(qty, float(price))
        if order.kind == T.ENTRY:
            book.positions[order.symbol] = Position(quantity=qty, stop=fill * (1.0 - order.stop_distance),
                                                    mark=float(price), entry_day=day, entry_price=fill,
                                                    entry_stop_distance=float(order.stop_distance))
        else:
            pos.quantity += qty
        fills.append({"day": day, "symbol": order.symbol, "kind": order.kind, "quantity": qty, "price": fill})
    book.pending = []
    # 4./5. stops and the later funding events
    for s in by_index(list(book.positions)):
        pos, j = book.positions[s], order_of(s)
        price = feat.open[i, j]
        hours = min(24.0, float(feat.funding_hours[i, j]))
        missing = (24.0 - hours) / 8.0 if hours > 0 else 2.0
        imputed = pos.quantity * price * missing_rate * missing
        if feat.low[i, j] <= pos.stop:
            gap = price < pos.stop
            events.append({"day": day, "symbol": s, "event": EXIT_STOP_GAP if gap else EXIT_STOP, "stop": pos.stop,
                           "open": float(price), "low": float(feat.low[i, j])})
            amount = pos.quantity * price * feat.funding_later_positive[i, j] + imputed
            if amount:
                post("funding", amount)
            close(s, float(price) if gap else pos.stop, EXIT_STOP_GAP if gap else EXIT_STOP)
        else:
            amount = pos.quantity * price * (feat.funding_later_positive[i, j] + feat.funding_later_negative[i, j]) + imputed
            if amount:
                post("funding", amount)
    # 6. mark to the close; equity and brakes
    gross = 0.0
    for s, pos in book.positions.items():
        price = feat.close[i, order_of(s)]
        post("pnl_price", pos.quantity * (price - pos.mark))
        pos.mark = float(price)
        gross += pos.quantity * pos.mark
    ret = book.equity / start_equity - 1.0 if start_equity > 0 else 0.0
    book.peak = max(book.peak, book.equity)
    drawdown = book.equity / book.peak - 1.0 if book.peak > 0 else 0.0
    book.paused = bool(ret <= -level.daily_pause)
    book.halved = bool(drawdown <= -level.halve_drawdown)
    if not book.halted and drawdown <= -level.stop_drawdown:
        book.halted = True
        events.append({"day": day, "event": "DRAWDOWN_STOP_FIRED", "drawdown": drawdown})
    if book.equity <= 0 and not book.halted:
        book.halted = True
        events.append({"day": day, "event": "ACCOUNT_EQUITY_EXHAUSTED"})
    # 7. stops ratchet up
    for s, pos in book.positions.items():
        j = order_of(s)
        d = feat.stop_distance[i, j]
        if math.isfinite(d):
            pos.stop = max(pos.stop, float(feat.close[i, j]) * (1.0 - float(d)))
    book.last_day = day
    return {"day": day, "equity": book.equity, "return": ret, "drawdown": drawdown, "gross_notional": gross,
            "positions": len(book.positions), "paused": book.paused, "halved": book.halved, "halted": book.halted,
            "fills": fills, "events": events, "rejections": rejections, "sums": sums}


# ------------------------------------------------------------------------------------------- account stage
@dataclass
class AccountDecision:
    document: Dict[str, Any]            # stored immutably; ``intents`` inside are plain, re-validatable
    orders: List[T.Order]
    intents: Tuple[C.Intent, ...]
    constraints: C.PortfolioConstraints
    instruments: Tuple[str, ...]


def decide(book: Book, market: MarketDecision, *, entries_blocked: bool = False,
           blocked_reason: Optional[str] = None) -> AccountDecision:
    """The frozen decision at the close of ``market.day`` for the next open, from the book as it stands.

    ``entries_blocked`` is a platform control (family, account or platform kill switch), not a strategy rule:
    new positions and increases are dropped after the frozen decision was taken, exits and reductions stay.
    """
    feat, i, level = market.features, market.row, book.level
    sym_index = market.index
    held_notional = {s: p.quantity * float(feat.close[i, sym_index(s)]) for s, p in book.positions.items()}
    decision = T.decide_targets(equity=book.equity, candidates=market.candidates, held=held_notional, level=level,
                                halve=book.halved, halted=book.halted, paused=book.paused)
    prices = {s: float(feat.close[i, sym_index(s)]) for s in set(market.universe) | set(book.positions)}
    orders = T.orders_from_targets(decision, held_quantity={s: p.quantity for s, p in book.positions.items()},
                                   close=prices)
    rejections = [{"stage": "DECISION", **r} for r in decision.rejected]
    if entries_blocked:
        kept = []
        for o in orders:
            if o.kind == T.ENTRY or (o.kind == T.ADJUST and o.quantity > 0):
                rejections.append({"stage": "PLATFORM_CONTROL", "symbol": o.symbol, "reason": REJECT_ENTRIES_BLOCKED,
                                   "control": blocked_reason or "KILL_SWITCH"})
            else:
                kept.append(o)
        orders = kept
    equity = book.equity
    targets = {s: {"notional": v, "fraction_of_equity": v / equity if equity > 0 else 0.0,
                   "strength": decision.strength.get(s), "stop_distance": decision.stop_distance[s]}
               for s, v in sorted(decision.targets.items())}
    constraints = C.PortfolioConstraints(
        max_positions=level.max_positions, max_open_risk=level.max_open_risk, max_leverage=level.leverage,
        per_trade_risk_ceiling=level.risk_per_trade, max_entry_stop_distance=level.max_entry_stop_distance,
        equity_basis=max(equity, 1e-12))
    intents = tuple(_intent(o, book, prices, equity, level) for o in orders)
    document = {
        "family_id": FAMILY_ID, "adapter_version": ADAPTER_VERSION, "day": market.day, "cycle_key": market.cycle_key,
        "risk_level": book.risk_level, "policy": book.policy, "equity_basis": equity,
        "flags": {"paused": book.paused, "halved": book.halved, "halted": book.halted,
                  "entries_blocked": bool(entries_blocked), "blocked_reason": blocked_reason},
        "universe": len(market.universe), "qualified": decision.qualified, "scale": dict(decision.scale),
        "open_risk": decision.open_risk, "gross_notional": decision.gross_notional, "targets": targets,
        "orders": [{"symbol": o.symbol, "kind": o.kind, "quantity": o.quantity, "target_notional": o.target_notional,
                    "stop_distance": o.stop_distance} for o in orders],
        "rejections": rejections, "intents": [C.to_plain(x) for x in intents],
        "held": {s: {"quantity": p.quantity, "stop": p.stop, "mark": p.mark} for s, p in book.positions.items()},
        "data_snapshot_id": market.data_snapshot_id,
        "stages": {
            "TRADE_ECONOMICS": {"status": "EXECUTED", "open_risk": decision.open_risk,
                                "gross_notional": decision.gross_notional,
                                "expected_round_trip_cost_fraction": 2 * (spec.SPECIFICATION["taker_fee"] + spec.SPECIFICATION["slippage"])},
            "RISK_AND_GOVERNANCE_VETOES": {"status": "EXECUTED", "paused": book.paused, "halved": book.halved,
                                           "halted": book.halted, "entries_blocked": bool(entries_blocked),
                                           "refused": len(rejections)},
            "PLATFORM_TARGET_PORTFOLIO": {"status": "EXECUTED", "targets": len(targets), "scale": dict(decision.scale)},
            "ACCOUNT_ELIGIBLE_PORTFOLIO": {"status": "EXECUTED", "orders": len(orders), "policy": book.policy},
        },
    }
    return AccountDecision(document=document, orders=orders, intents=intents, constraints=constraints,
                           instruments=tuple(sorted(set(market.universe) | set(book.positions))))


def _intent(order: T.Order, book: Book, prices: Mapping[str, float], equity: float, level: T.RiskLevel) -> C.Intent:
    price = prices[order.symbol]
    if order.kind == T.EXIT:
        return C.ExitIntent(order.symbol, C.LONG, EXIT_BRAKE if book.halted else EXIT_SIGNAL, reference_price=price)
    if order.kind == T.ENTRY:
        return C.EntryIntent(symbol=order.symbol, direction=C.LONG, quantity=order.quantity, reference_price=price,
                             stop_price=price * (1.0 - order.stop_distance),
                             risk_fraction=order.quantity * price * order.stop_distance / equity,
                             leverage=float(level.leverage))
    pos = book.positions[order.symbol]
    return C.RebalanceIntent(symbol=order.symbol, direction=C.LONG, current_quantity=pos.quantity,
                             target_quantity=pos.quantity + order.quantity, reference_price=price,
                             stop_price=min(pos.stop, price * (1.0 - 1e-9)), band=float(spec.SPECIFICATION["rebalance_band"]))


def pending_from(orders: Sequence[T.Order]) -> List[Dict[str, Any]]:
    return [{"symbol": o.symbol, "kind": o.kind, "quantity": o.quantity, "target_notional": o.target_notional,
             "stop_distance": o.stop_distance} for o in orders]


def decision_time(day: int) -> datetime:
    return datetime.fromtimestamp(close_ms(day) / 1000.0, tz=timezone.utc)


class BookOutOfSequence(RuntimeError):
    code = "SHADOW_BOOK_OUT_OF_SEQUENCE"


class DailyTrendPipeline:
    """What the platform cycle driver calls (``driver.FamilyPipeline``)."""

    family_id = FAMILY_ID
    timeframe = TIMEFRAME

    def compare_models(self, db: Any, market: MarketDecision, accounts: Sequence[Any], *, now_ms: int) -> None:
        """Daily model audit against the frozen evaluator, after account commits.

        Recompute each reference book from its actual start; persist today's
        comparison only. A crash retries this audit before completing the cycle.
        Customer-specific operational differences aren't mixed into model parity.
        """
        from . import shadow_book, shadow_compare as comparison

        for account in accounts:
            with db.connect() as conn:
                book = shadow_book.load(conn, account.account_id, FAMILY_ID)
            if book is None or book.last_day is None:
                continue
            simulation = comparison.evaluator_simulation(
                market.features, risk_level=book.risk_level, policy=book.policy,
                first_day=book.first_day, last_day=market.day, initial_equity=book.initial_equity)
            result = comparison.compare(
                comparison.shadow_from_db(db, account.account_id, FAMILY_ID),
                comparison.reference_from_simulation(simulation), days=[market.day])
            comparison.record(db, family_id=FAMILY_ID, account_id=account.account_id,
                              reference_label="DAILY_FROZEN_EVALUATOR", comparisons=result, now_ms=now_ms)

    def due_key(self, now_ms: int) -> str:
        return cycle_key_due(now_ms)

    def key_after(self, key: str) -> str:
        return day_text(day_of(key) + 1)

    def decision_time_ms(self, key: str) -> int:
        return close_ms(day_of(key))

    def verify(self, research_register: Any) -> Dict[str, Any]:
        return verify_mandate(research_register)

    def market(self, data: Any, key: str) -> MarketDecision:
        if data is None:
            raise DataNotFresh("NO_DAILY_BAR_PROVIDER", "no market-data provider is configured for daily families")
        panel, metadata = data.panel_until(day_of(key))
        return market_stage(panel, metadata, day_of(key))

    def account(self, conn: Any, market: MarketDecision, account: Any, *, cycle_id: str, now_ms: int,
                entries_blocked: bool, blocked_reason: Optional[str]) -> Dict[str, Any]:
        """Inside the driver's transaction for this account: account the day that closed, take the decision
        for the next open, store the book. A new account's first decision is taken on a book that starts the
        next day, as a fresh account in the evaluator may trade at its first open."""
        from . import shadow_book

        book = shadow_book.load(conn, account.account_id, FAMILY_ID)
        if book is None:
            book = new_book(account.risk_level, account.policy, account.initial_equity, market.day + 1)
        else:
            expected = book.first_day if book.last_day is None else book.last_day + 1
            if market.day < expected:           # an older cycle than the book: it has nothing to add
                return {"skipped": "SHADOW_BOOK_IS_PAST_THIS_DAY"}
            if market.day > expected:           # a day was missed: never jump, the missing cycle runs first
                raise BookOutOfSequence(f"book expects {day_text(expected)}, cycle is {market.cycle_key}")
            shadow_book.record_day(conn, account.account_id, FAMILY_ID, cycle_id=cycle_id,
                                   result=advance_day(book, market), now_ms=now_ms)
        decision = decide(book, market, entries_blocked=entries_blocked, blocked_reason=blocked_reason)
        book.pending = pending_from(decision.orders)
        shadow_book.save(conn, account.account_id, FAMILY_ID, book, cycle_id=cycle_id, now_ms=now_ms)
        return {"document": decision.document, "intents": decision.intents, "constraints": decision.constraints,
                "instruments": decision.instruments}


PIPELINE = DailyTrendPipeline()


__all__ = ["FAMILY_ID", "TIMEFRAME", "ADAPTER_VERSION", "STAGES", "MIN_BAR_COVERAGE", "DataNotFresh", "MarketDecision",
           "BookOutOfSequence", "DailyTrendPipeline", "PIPELINE",
           "Position", "Book", "AccountDecision", "new_book", "market_stage", "advance_day", "decide", "pending_from",
           "verify_mandate", "snapshot_id", "day_of", "day_text", "close_ms", "cycle_key_due", "decision_time",
           "REJECT_ENTRIES_BLOCKED", "EXIT_SIGNAL", "EXIT_STOP", "EXIT_STOP_GAP", "EXIT_DATA_GAP", "EXIT_BRAKE"]
