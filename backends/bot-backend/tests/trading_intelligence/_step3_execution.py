"""Shared fixtures of the Step 3.8 / 3.9 execution tests.

Everything runs against ``FakeVenue`` (an in-memory venue with exact rules) and a temporary database. The only
family that is ever authorized is a SYNTHETIC one, registered and promoted to M6 through the real governance
operations on top of a synthetic research register (``_step3``); the real families of the repository are used
only to prove they are refused. None of this is evidence about a venue or about a strategy.

Three test doubles live here and nowhere else:

``Clock``         the injected clock; it moves one millisecond per reading so that "later" is observable, and
                  by ``advance`` when a test says so. No test sleeps.
``Controlled``    a pass-through around the venue that counts calls, lets a test die before or after the N-th
                  one (``SimulatedCrash`` is a ``BaseException``, like a killed process: nothing catches it),
                  runs a hook before or after a named call, and checks at every venue call that no write
                  transaction is open in the database.
``OverlapVenue``  a HYPOTHETICAL capability: a ``FakeVenue`` that declares ``multiple_stops_per_position`` and
                  accepts a second resting stop. No adapter and no ``FakeVenue`` profile of this repository
                  declares that today; it exists so that the create-confirm-retire protocol and its failure
                  cases can be exercised at all.
"""
from __future__ import annotations

import contextlib
import dataclasses
import itertools
import sqlite3
from decimal import Decimal
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

import _step3 as S
from app.exchange.platform_adapter import contract as c
from app.exchange.platform_adapter.contract import AdapterError, OrderRequest
from app.exchange.platform_adapter.fake import FakeVenue
from app.exchange.platform_adapter.rules_cache import RulesCache
from app.trading_intelligence.governance.promotion import StaticAuthority
from app.trading_intelligence.pipeline import contracts as C
from app.trading_intelligence.pipeline import execution as X
from app.trading_intelligence.pipeline import family_registry as FR
from app.trading_intelligence.pipeline import portfolio as P
from app.trading_intelligence.pipeline import position_management as PM
from app.trading_intelligence.pipeline import protection as PR
from app.trading_intelligence.pipeline import provenance as PV
from app.trading_intelligence.pipeline.brakes import NO_BRAKE
from shared_lib.persistence.cati_platform_schema import ensure_cati_platform_schema
from shared_lib.persistence.db import DB
from shared_lib.persistence.migrations import migrate

T0 = 1_780_000_000_000
HOUR = 3_600_000
EQUITY = 10_000.0
BTC, ETH, SOL, XRP = "BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT"
PRICES = {BTC: 50_000.0, ETH: 2_500.0, SOL: 100.0, XRP: 2.0}
TICKS = {BTC: 0.1, ETH: 0.01, SOL: 0.01, XRP: 0.0001}
STEPS = {BTC: 0.001, ETH: 0.01, SOL: 0.1, XRP: 1.0}
USER, ACCOUNT = "user-1", "acct-1"
VENUE_CALLS = tuple(m for m in c.MUTATING_METHODS + c.READ_METHODS if m not in ("capabilities", "rate_limit"))
_cycles = itertools.count(1)
_TEMPLATE: Dict[str, Any] = {}


class Clock:
    """The injected clock. One millisecond passes per reading."""

    def __init__(self, start: int = T0) -> None:
        self.t = int(start)

    def __call__(self) -> int:
        self.t += 1
        return self.t

    def advance(self, ms: int) -> int:
        self.t += int(ms)
        return self.t


class SimulatedCrash(BaseException):
    """The process died here. Deliberately not an ``Exception``: no handler of the code under test sees it."""


class OverlapVenue(FakeVenue):
    """HYPOTHETICAL: a venue on which a second protective stop may rest beside the first. See the module note."""

    def capabilities(self) -> c.AdapterCapabilities:
        return dataclasses.replace(super().capabilities(), multiple_stops_per_position=True, notes=(
            "FAKE VENUE with a HYPOTHETICAL overlap capability no current adapter declares",))

    def _place_trigger(self, request: OrderRequest, rules: Any) -> Any:
        try:
            return super()._place_trigger(request, rules)
        except AdapterError as error:
            if error.code != c.STOP_ALREADY_EXISTS:
                raise
        # "one stop of a kind" is the last rule the fake checks: every other rule has passed for this request
        whole = request.close_position or not self._quantity_stop
        quantity = None if whole else Decimal(str(request.quantity))
        return self._new_order(request.symbol, request.side, request.order_type, request.client_order_id, quantity, None,
                               Decimal(str(request.stop_price)), not whole, whole)


class Controlled:
    """A pass-through around a venue adapter that a test can make die, hook and audit. It changes no answer."""

    def __init__(self, inner: Any, db_path: Optional[str] = None) -> None:
        self.inner, self._db_path = inner, db_path
        self.calls: List[str] = []
        self.crash_at: Optional[int] = None
        self.crash_after_effect = False
        self.before: Dict[str, List[Callable[[], Any]]] = {}
        self.after: Dict[str, List[Callable[[], Any]]] = {}
        self.open_transactions: List[str] = []

    @property
    def venue(self) -> str:
        return self.inner.venue

    @property
    def environment(self) -> str:
        return self.inner.environment

    @property
    def adapter_version(self) -> str:
        return self.inner.adapter_version

    def capabilities(self) -> c.AdapterCapabilities:
        return self.inner.capabilities()

    def rate_limit(self) -> Any:
        return self.inner.rate_limit()

    def hook(self, method: str, action: Callable[[], Any]) -> None:
        """Run ``action`` once, immediately before the next call of ``method`` reaches the venue."""
        self.before.setdefault(method, []).append(action)

    def hook_after(self, method: str, action: Callable[[], Any]) -> None:
        """Run ``action`` once, immediately after the next call of ``method`` has returned from the venue."""
        self.after.setdefault(method, []).append(action)

    def die_at(self, index: int, *, after_effect: bool) -> None:
        """Die at the ``index``-th venue call from now: before it reaches the venue, or after the venue acted."""
        self.crash_at, self.crash_after_effect = len(self.calls) + int(index), bool(after_effect)

    def _probe(self, method: str) -> None:
        """No database transaction spans a venue call: a second connection can take the write lock right now."""
        if self._db_path is None:
            return
        with contextlib.closing(sqlite3.connect(self._db_path, timeout=0)) as probe:
            try:
                probe.execute("BEGIN IMMEDIATE")
                probe.execute("ROLLBACK")
            except sqlite3.OperationalError:
                self.open_transactions.append(method)

    def __getattr__(self, name: str) -> Any:
        target = getattr(self.inner, name)
        if name not in VENUE_CALLS:
            return target

        def call(*args: Any, **kwargs: Any) -> Any:
            index = len(self.calls)
            self.calls.append(name)
            self._probe(name)
            for action in self.before.pop(name, ()):
                action()
            if self.crash_at == index and not self.crash_after_effect:
                raise SimulatedCrash(f"before {name}")
            result = target(*args, **kwargs)
            if self.crash_at == index and self.crash_after_effect:
                raise SimulatedCrash(f"after {name}")
            for action in self.after.pop(name, ()):
                action()
            return result

        return call


# ------------------------------------------------------------------------------------------------- database
def _template(tmp_path_factory: Any) -> Dict[str, Any]:
    """A migrated database in which the synthetic family is registered, certified by a synthetic research
    register and promoted to M6 through the real governance operations. Built once; every test copies it."""
    if not _TEMPLATE:
        folder = tmp_path_factory.mktemp("step3_execution")
        database = DB(str(folder / "template.db"))
        migrate(database)
        ensure_cati_platform_schema(database)
        definition = S.definition(supported_markets=tuple((venue, "USDT_PERPETUAL") for venue in c.VENUES))
        register = S.research_register(folder, status="RESEARCH_PASS")
        registry = S.registry(database, register, definition)
        registry.register_all(actor_ref="system:test")
        S.promote(registry, definition.family_id, "M6", prereq=S.prerequisites(definition, certification="CERTIFIED"))
        _TEMPLATE.update(path=database.path, definition=definition, register=register)
    return _TEMPLATE


def copy_database(source: str, target: str) -> DB:
    with contextlib.closing(sqlite3.connect(source)) as origin, contextlib.closing(sqlite3.connect(target)) as copy:
        origin.backup(copy)
    return DB(target)


def make_venue(profile: str = c.BINANCE, *, clock: Optional[Clock] = None, overlap: bool = False, environment: str = c.DEMO,
               wallet: float = EQUITY, **kw: Any) -> FakeVenue:
    fake = (OverlapVenue if overlap else FakeVenue)(profile, clock=clock or Clock(), environment=environment,
                                                    wallet_balance=wallet, default_leverage=1, **kw)
    for symbol, price in PRICES.items():
        fake.add_instrument(symbol, price=price, price_tick=TICKS[symbol], quantity_step=STEPS[symbol], min_notional=5.0)
    return fake


@dataclasses.dataclass
class World:
    """One account on one venue with everything the boundary needs, real except for the venue."""
    db: DB
    clock: Clock
    fake: FakeVenue
    adapter: Controlled
    account: C.AccountRef
    limits: P.AccountLimits
    definition: FR.FamilyDefinition
    registry: FR.FamilyRegistry
    gate: PV.FamilyExecutionGate
    rules: RulesCache
    certified: Dict[Tuple[str, str], Tuple[bool, str]]

    def boundary(self, **over: Any) -> X.PortfolioExecutionBoundary:
        """A boundary as a new process would build it. The order-submission flag and the governance phase are
        replaced by visible test doubles (both are off in this repository and stay off); the family gate, the
        kill switches, the reservation ledger, the rules cache and the schema are the real ones."""
        values: Dict[str, Any] = dict(
            db=self.db, adapter=self.adapter, account=self.account, limits=self.limits, rules=self.rules,
            adapter_certification=lambda venue, environment: self.certified.get((venue, environment), (False, "NOT_CERTIFIED")),
            family_gate=self.gate, authority=StaticAuthority(True, "TEST_AUTHORITY"),
            submission_gate=lambda environment: {"enabled": True, "reason": "", "environment": environment},
            clock=self.clock)
        values.update(over)
        return X.PortfolioExecutionBoundary(**values)

    def protection(self) -> PR.ProtectionManager:
        return PR.ProtectionManager(db=self.db, adapter=self.adapter, account_id=self.account.broker_account_id,
                                    clock=self.clock)

    def decide(self, *intents: C.Intent, mode: str = C.DEMO, ttl: int = HOUR, definition: Optional[FR.FamilyDefinition] = None,
               account: Optional[C.AccountRef] = None, decision_time: Optional[int] = None,
               instruments: Sequence[str] = tuple(PRICES), constraints: Optional[C.PortfolioConstraints] = None) -> C.IntentBatch:
        """Record an immutable decision of the synthetic family for these intents and return its batch."""
        now = self.clock()
        return PM.record_decision(
            self.db, definition=definition or self.definition, account=account or self.account, intents=intents,
            constraints=constraints or self.limits.constraints(EQUITY), instruments=instruments,
            cycle_id=f"cycle-{next(_cycles)}",
            decision_time_ms=decision_time if decision_time is not None else now - 10, expires_at_ms=now + int(ttl),
            data_snapshot_id="test", mode=mode, now_ms=now)

    def run(self, *intents: C.Intent, brake: Any = NO_BRAKE, boundary: Optional[X.PortfolioExecutionBoundary] = None,
            **kw: Any) -> X.BatchResult:
        return (boundary or self.boundary()).execute(self.decide(*intents, **kw), brake_state=brake)

    # -- venue truth ---------------------------------------------------------------------------------------
    def signed(self, symbol: str) -> float:
        held = self.fake.positions(symbol)
        return held[0].signed_quantity if held else 0.0

    def stops(self, symbol: str) -> List[Any]:
        return [o for o in self.fake.open_orders(symbol) if o.order_type == c.STOP_MARKET]

    def stop_prices(self, symbol: str) -> List[float]:
        return sorted(o.stop_price for o in self.stops(symbol))

    def sent(self, method: str = "place_order", *, symbol: Optional[str] = None) -> List[Any]:
        """Mutations the venue received, as its own journal recorded them."""
        out = []
        for entry in self.fake.mutations:
            target = entry.arguments.get("symbol") or getattr(entry.arguments.get("request"), "symbol", None)
            if entry.method == method and symbol in (None, target):
                out.append(entry)
        return out

    def orders_sent(self, symbol: Optional[str] = None, *, protective: Optional[bool] = None) -> List[OrderRequest]:
        requests = [e.arguments["request"] for e in self.sent("place_order", symbol=symbol)]
        return [r for r in requests if protective is None or r.protective == protective]

    def rows(self, table: str, where: str = "1=1", params: Sequence[Any] = ()) -> List[Dict[str, Any]]:
        with self.db.connect() as conn:
            return [dict(r) for r in conn.execute(f"SELECT * FROM {table} WHERE {where}", tuple(params)).fetchall()]

    def reservations(self, account: Optional[str] = None) -> Dict[str, str]:
        rows = self.rows("cati_reservation_ledger", "account_id=?", (account or self.account.broker_account_id,))
        return {r["symbol"]: r["state"] for r in rows}


def make_world(tmp_path_factory: Any, tmp_path: Any, *, profile: str = c.BINANCE, overlap: bool = False,
               account_id: str = ACCOUNT, user_id: str = USER, level: str = "conservative", db: Optional[DB] = None,
               clock: Optional[Clock] = None, certified: bool = True, **venue_kw: Any) -> World:
    template = _template(tmp_path_factory)
    database = db or copy_database(template["path"], str(tmp_path / f"world_{account_id}.db"))
    clock = clock or Clock()
    fake = make_venue(profile, clock=clock, overlap=overlap, **venue_kw)
    definition = template["definition"]
    registry = S.registry(database, template["register"], definition)
    cache = RulesCache(database)
    refreshed = cache.refresh(fake, clock())
    assert refreshed.ok, refreshed
    fake.reads.clear()                                                   # the setup's own read is not part of any test
    account = C.AccountRef(user_id=user_id, broker_account_id=account_id, venue=profile, environment=fake.environment)
    answers: Dict[Tuple[str, str], Tuple[bool, str]] = {(profile, fake.environment): (True, "")} if certified else {}
    # the real gate class; its adapter-certification half is answered by the same table the boundary is given
    gate = PV.FamilyExecutionGate(database, registry=registry, venue_certified=lambda venue, environment, now_ms:
                                  answers.get((venue, environment), (False, "NOT_CERTIFIED")))
    return World(db=database, clock=clock, fake=fake, adapter=Controlled(fake, database.path), account=account,
                 limits=P.AccountLimits.from_profile(level), definition=definition, registry=registry, gate=gate,
                 rules=cache, certified=answers)


# -------------------------------------------------------------------------------------------------- intents
def entry(symbol: str, *, direction: str = C.LONG, risk: float = 0.002, distance: float = 0.05, **over: Any) -> C.EntryIntent:
    """An entry sized so that it loses ``risk`` of the equity at a stop ``distance`` away (on the tick and step)."""
    price = PRICES[symbol]
    sign = -1.0 if direction == C.LONG else 1.0
    quantity = round(risk * EQUITY / (distance * price) / STEPS[symbol]) * STEPS[symbol]
    values = dict(symbol=symbol, direction=direction, quantity=round(quantity, 8), reference_price=price,
                  stop_price=round(price * (1.0 + sign * distance), 8), risk_fraction=risk)
    values.update(over)
    return C.EntryIntent(**values)


def exit_(symbol: str, *, direction: str = C.LONG, reason: str = "SIGNAL") -> C.ExitIntent:
    return C.ExitIntent(symbol=symbol, direction=direction, reason=reason, reference_price=PRICES[symbol])


def open_long(world: World, symbol: str, quantity: float, stop_price: float) -> Tuple[Any, ...]:
    """Put a protected long on the venue directly (the market and the account owner, not the boundary) and
    return the protection gaps that exist afterwards: the baseline a scenario must not add to. Where the venue
    takes a stop while flat the baseline is empty; elsewhere the fill-to-stop interval of the setup is in it."""
    fake = world.fake
    stop = OrderRequest(symbol=symbol, side=c.SELL, order_type=c.STOP_MARKET, client_order_id=f"setup-stop-{symbol}",
                        stop_price=stop_price, close_position=True) if fake.capabilities().close_position_stop else \
        OrderRequest(symbol=symbol, side=c.SELL, order_type=c.STOP_MARKET, client_order_id=f"setup-stop-{symbol}",
                     stop_price=stop_price, quantity=quantity, reduce_only=True)
    buy = OrderRequest(symbol=symbol, side=c.BUY, order_type=c.MARKET, client_order_id=f"setup-entry-{symbol}", quantity=quantity)
    try:
        fake.place_order(stop)                                           # a stop while flat: where the venue takes it
        fake.place_order(buy)
    except AdapterError:
        fake.place_order(buy)
        fake.place_order(stop)
    fake.mutations.clear()
    fake.reads.clear()
    return fake.protection_gaps(symbol)


__all__ = ["T0", "HOUR", "EQUITY", "BTC", "ETH", "SOL", "XRP", "PRICES", "TICKS", "STEPS", "USER", "ACCOUNT", "Clock",
           "SimulatedCrash", "OverlapVenue", "Controlled", "World", "make_world", "make_venue", "copy_database", "entry",
           "exit_", "open_long", "NO_BRAKE"]
