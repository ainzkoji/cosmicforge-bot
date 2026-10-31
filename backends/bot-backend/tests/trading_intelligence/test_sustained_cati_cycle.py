"""Section 18: a sustained CATI operating cycle -- 30 simulated days of the platform cycle driver and the
30-second maintenance loop over several accounts, with a restart in the middle.

**What this is NOT.** Simulated days on ``FakeVenue`` with a synthetic market are not shadow evidence, not venue
evidence and not a strategy result. The family that trades here is the SYNTHETIC test family of
``_step3_execution`` (registered and promoted to M6 in a temporary database on a synthetic research register);
its "strategy" is a deterministic rotation written in this file. The only thing a pass says is that the
machinery -- scheduling, data freshness, immutable decisions, brakes, the price/liquidity checks, the family
gate, fills, protection, reconciliation, durable storage and restart -- keeps its invariants over many days.

**The loop.** One injected clock drives everything (driver, scheduler, boundary, venues). Every tick is exactly
``TICK_MS`` = 30 s after the previous one and runs, in this order: the market moves; ``ShadowScheduler.tick``
(the real scheduler of ``integration.platform_runtime``, which calls ``PlatformCycleDriver.run_due``); the
synthetic family's execution step once the day's market stage has completed on fresh data; and the maintenance
step of every account (``PortfolioExecutionBoundary.recover`` -- reconciliation, protection completion,
reservation settlement -- followed by a coverage check on the venue). To keep the test fast the ticks run in a
window straddling each daily close (``WINDOW_BEFORE_MS`` before to ``WINDOW_AFTER_MS`` after) and the clock then
jumps over the quiet hours of the day (with an overnight price gap): 65 maintenance ticks per simulated day,
not the 2,880 a full day would have. The cadence inside the window is the real 30 s; the daily decision interval
is the real one day. Every tick runs inside ``DB.scope`` (one SQLite connection per tick, as the driver itself
does); without it each ``connect()`` pays a WAL open/close and one simulated day took ~80 s on this workstation.

**Stages exercised.**

* data freshness: bars are published ``PUBLISH_DELAY_MS`` after the close; on ``LATE_DAY`` they arrive late and
  the driver reports ``WAITING_FOR_DATA`` on every tick until they do; no entry is decided from stale data, and
  yesterday's decision handed to the boundary is refused ``DECISION_STALE``;
* decision recording: one immutable shadow decision per (cycle, account) of the REAL ``DAILY_TREND`` family,
  replays reproduce it, a different document for the same key is a ``DecisionConflict``;
* account eligibility: a connected account whose bot is stopped is never enrolled;
* certification: the real family handed to the boundary under the real gate is refused at the family gate
  every day; every order comes from the synthetic family;
* sizing: per account from the venue equity and the profile's per-trade risk; an oversized entry is refused;
* brakes: ``pipeline.brakes`` evaluated at every close from venue equity; a manual loss by the owner pauses one
  account for one decision (entries refused, its exits executed);
* liquidity / price check: an adverse move between decision and order refuses the entry;
* fills, protection, reconciliation, persistence, restart: on ``CRASH_DAY`` the driver dies in the account stage
  (the successor waits out the dead owner's heartbeat, then resumes the cycle), and a boundary dies right after an
  opening order filled and before its stop; a new process (new database objects on the same files) recovers
  first -- protecting that position and settling its reservation -- and continues the same decision.

Final invariants: no position ever rested without a stop beyond the boundary's own fill->stop step, no client
order id was sent twice, one opening order per (decision, symbol), no reservation is left open, the risk of
every filled entry at its fill price was at most the 0.40 % per-trade ceiling, and the cadence counts.
"""
from __future__ import annotations

import json
import random
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal
from typing import Any, Dict, List, Optional

import pytest

import _step2
import _step3 as S
import _step3_execution as E
from app.exchange.platform_adapter import contract as c
from app.exchange.platform_adapter.rules_cache import RulesCache
from app.trading_intelligence.families.daily_trend import spec
from app.trading_intelligence.families.daily_trend import targets as T
from app.trading_intelligence.integration.platform_runtime import ShadowScheduler
from app.trading_intelligence.pipeline import brakes as B
from app.trading_intelligence.pipeline import contracts as C
from app.trading_intelligence.pipeline import daily_trend as DT
from app.trading_intelligence.pipeline import driver as D
from app.trading_intelligence.pipeline import execution as X
from app.trading_intelligence.pipeline import family_registry as FR
from app.trading_intelligence.pipeline import portfolio as P
from app.trading_intelligence.pipeline import provenance as PV
from app.trading_intelligence.pipeline import reservations as R
from app.trading_intelligence.pipeline.accounts import CombinedAccounts, ConnectedAccounts, ModelAccounts
from app.trading_intelligence.pipeline.market_data import FrozenPanelBars
from shared_lib.persistence.cati_execution_schema import INTENT_FINAL_STATES, INTENT_TABLE
from shared_lib.persistence.db import DB
from shared_lib.risk_levels import SYSTEM_PER_TRADE_RISK_CEILING_PCT

DAYS = 30
TICK_MS = 30_000
WINDOW_BEFORE_MS = 2 * 60_000
WINDOW_AFTER_MS = 30 * 60_000
PUBLISH_DELAY_MS = 90_000
LATE_DAY, LATE_DELAY_MS = 6, 12 * 60_000
BRAKE_LOSS_DAY = 9                      # the owner's manual loss happens in this day's window ...
PAUSED_DAY = BRAKE_LOSS_DAY + 1         # ... so the close of the next day pauses the account
SLIP_DAY = 12
OVERSIZE_DAY = 13
CRASH_DAY = 16
HOLD_DAYS = 3
MAX_HELD = 2
SLIPPAGE_BPS = 50.0
START = DT.day_of("2020-09-01")
MODEL_EQUITY = spec.INTERPRETATION["initial_equity_usdt"]
CEILING = float(SYSTEM_PER_TRADE_RISK_CEILING_PCT) / 100.0
SYMBOLS = tuple(E.PRICES)
ACCOUNTS = (("acct-1", "user-1", "conservative"), ("acct-2", "user-2", "balanced"), ("acct-3", "user-3", "aggressive"))
STOPPED_ACCOUNT = ("acct-9", "user-9", "balanced")
MANUAL = "DOGEUSDT"                     # the owner's own instrument on acct-1: not the family's, not checked for stops


def _on_tick(value: float, tick: float, rounding) -> float:
    q = Decimal(str(tick))
    return float((Decimal(str(value)) / q).to_integral_value(rounding=rounding) * q)


class LaggingFeed:
    """``FrozenPanelBars`` whose decision day appears only once the simulated feed has published it."""

    def __init__(self, panel: Any, metadata: Any, clock: E.Clock) -> None:
        self._bars, self._clock = FrozenPanelBars(panel, metadata), clock
        self.delays: Dict[int, int] = {}
        self.calls = 0

    def published(self, day: int) -> bool:
        return self._clock.t >= DT.close_ms(day) + self.delays.get(day, PUBLISH_DELAY_MS)

    def panel_until(self, day: int):
        self.calls += 1
        return self._bars.panel_until(day if self.published(day) else day - 1)


class CrashingRunner:
    """The driver's account runner. ``crash_after`` = n: the process dies after n accounts of a cycle."""

    def __init__(self) -> None:
        self.crash_after: Optional[int] = None

    def __call__(self, calls):
        out = []
        for index, call in enumerate(calls):
            if self.crash_after is not None and index == self.crash_after:
                self.crash_after = None
                raise E.SimulatedCrash("driver killed in the account stage")
            out.append(call())
        return out


class Process:
    """Everything one process builds from the durable files: database objects, registry, driver, scheduler,
    one boundary per account. A restart throws this away and builds a new one; the venues persist."""

    def __init__(self, sim: "Sim", generation: int) -> None:
        self.db = DB(sim.db_path)
        self.runner = CrashingRunner()
        registry = FR.FamilyRegistry(self.db, research_register=sim.real_register)
        registry.register_all(actor_ref="system:sustained")
        self.driver = D.PlatformCycleDriver(
            self.db, registry=registry, data={"1d": sim.feed}, clock=lambda: sim.clock.t,
            owner=f"sustained-process-{generation}", account_runner=self.runner,
            accounts=CombinedAccounts(ModelAccounts(initial_equity=MODEL_EQUITY, levels=("balanced", "aggressive"),
                                                    policies=(T.MANDATE,)), ConnectedAccounts(self.db)))
        self.scheduler = ShadowScheduler(self.db, owns_runtime=lambda db: True, driver=self.driver,
                                         clock=lambda: sim.clock.t)
        template = sim.template
        self.worlds: Dict[str, E.World] = {}
        for account_id, user_id, level in ACCOUNTS:
            fake = sim.fakes[account_id]
            synthetic = S.registry(self.db, template["register"], template["definition"])
            answers = {(fake.venue, fake.environment): (True, "")}
            gate = PV.FamilyExecutionGate(self.db, registry=synthetic, venue_certified=lambda venue, environment, now_ms,
                                          a=answers: a.get((venue, environment), (False, "NOT_CERTIFIED")))
            self.worlds[account_id] = E.World(
                db=self.db, clock=sim.clock, fake=fake, adapter=E.Controlled(fake, self.db.path),
                account=C.AccountRef(user_id=user_id, broker_account_id=account_id, venue=fake.venue,
                                     environment=fake.environment),
                limits=P.AccountLimits.from_profile(level), definition=template["definition"], registry=synthetic,
                gate=gate, rules=RulesCache(self.db), certified=answers)
        self.boundaries = {a: w.boundary() for a, w in self.worlds.items()}
        # the REAL gate, the committed research register: what any real family meets at the boundary
        self.real_gate = PV.FamilyExecutionGate(self.db, venue_certified=lambda venue, environment, now_ms: (True, ""))


class Sim:
    def __init__(self, tmp_path_factory, tmp_path) -> None:
        self.template = E._template(tmp_path_factory)
        self.db_path = str(tmp_path / "sustained.db")
        E.copy_database(self.template["path"], self.db_path)
        self.clock = E.Clock(DT.close_ms(START) - WINDOW_BEFORE_MS - TICK_MS)
        self.rng = random.Random(20201)
        self.price = dict(E.PRICES)
        self.fakes = {a: E.make_venue(c.BINANCE, clock=self.clock) for a, _, _ in ACCOUNTS}
        self.fakes["acct-1"].add_instrument(MANUAL, price=0.1, price_tick=0.00001, quantity_step=1.0, min_notional=5.0)
        dataset = _step2.SyntheticDataset()
        self.feed = LaggingFeed(dataset._load("2020-01-01", "2021-06-30"), dataset.metadata, self.clock)
        self.feed.delays[START + LATE_DAY] = LATE_DELAY_MS
        self.real_register = _step2.new_register(tmp_path / "real_register.jsonl")
        self._connect_accounts()
        self.proc = Process(self, 0)
        self.generation = 0
        # what the run observed
        self.ticks: Dict[int, int] = {}
        self.maintenance: Dict[int, int] = {}
        self.waiting: Dict[int, int] = {}
        self.elsewhere: Dict[int, int] = {}
        self.executed: Dict[int, bool] = {}
        self.entry_risks: List[Dict[str, Any]] = []
        self.refusals: Dict[str, List[str]] = {}
        self.real_family_refusals: List[str] = []
        self.brake_flags: Dict[int, Dict[str, B.BrakeFlags]] = {}
        self.closing_equity: Dict[int, Dict[str, float]] = {}
        self.held_since: Dict[str, Dict[str, int]] = {a: {} for a, _, _ in ACCOUNTS}
        self.batches: Dict[int, Dict[str, C.IntentBatch]] = {}
        self.stale_probe: Optional[X.BatchResult] = None
        self.restarts = 0
        self.oversized: Optional[str] = None
        self.ceiling_refused = 0
        self.crash_account: Optional[str] = None
        self.slip_account: Optional[str] = None
        self.crash_gap_open = False

    def _connect_accounts(self) -> None:
        with DB(self.db_path).connect() as conn:
            for (account, user, level), status in [(a, "active") for a in ACCOUNTS] + [(STOPPED_ACCOUNT, "stopped")]:
                conn.execute("INSERT INTO broker_accounts (id, user_id, broker_id, market_type, label, status, environment, "
                             "created_at, updated_at) VALUES (?,?,?,?,?,?,?,?,?)",
                             (account, user, "binance", "futures", account, "connected", "demo", 1, 1))
                conn.execute("INSERT INTO bot_instances (id, user_id, broker_account_id, market_type, strategy_id, "
                             "strategy_version, symbols, timeframes, allocation_type, allocation_value, mode, status, "
                             "created_at, updated_at, risk_level, capital_allocation, capital_allocation_type) "
                             "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                             (f"bot-{account}", user, account, "futures", "cati", "1", "[]", "[]", "risk_based", 0.5,
                              "live", status, 1, 1, level, E.EQUITY, "fixed_amount"))

    def restart(self) -> None:
        """The process died. A new one is built from the files and recovers every account before anything else."""
        self.generation += 1
        self.restarts += 1
        self.proc = Process(self, self.generation)
        for account_id, boundary in self.proc.boundaries.items():
            report = boundary.recover()
            assert report.status in (X.BATCH_PROCESSED, X.BATCH_DEFERRED), (account_id, report)

    # ------------------------------------------------------------------------------------------ the market
    def move(self, sigma: float) -> None:
        for symbol in SYMBOLS:
            step = self.rng.gauss(0.0, sigma)
            self.price[symbol] = max(_on_tick(self.price[symbol] * (1.0 + step), E.TICKS[symbol], ROUND_FLOOR),
                                     E.TICKS[symbol] * 10)
        for fake in self.fakes.values():
            for symbol in SYMBOLS:
                fake.set_price(symbol, self.price[symbol])

    def jump(self, symbol: str, factor: float) -> None:
        self.price[symbol] = _on_tick(self.price[symbol] * factor, E.TICKS[symbol], ROUND_FLOOR)
        for fake in self.fakes.values():
            fake.set_price(symbol, self.price[symbol])

    # ------------------------------------------------------------------------------------------ the family
    def sized_entry(self, symbol: str, direction: str, equity: float, risk: float, *, quantity_risk: Optional[float] = None
                    ) -> C.EntryIntent:
        """Sized from the account's own equity and risk: quantity rounded down to the step, stop rounded towards
        the price, so the stated risk is never exceeded at the reference price."""
        price, tick, step = self.price[symbol], E.TICKS[symbol], E.STEPS[symbol]
        if direction == C.LONG:
            stop = _on_tick(price * 0.95, tick, ROUND_CEILING)
        else:
            stop = _on_tick(price * 1.05, tick, ROUND_FLOOR)
        per_unit = abs(price - stop)
        quantity = _on_tick((quantity_risk or risk) * equity / per_unit, step, ROUND_FLOOR)
        return C.EntryIntent(symbol=symbol, direction=direction, quantity=quantity, reference_price=price,
                             stop_price=stop, risk_fraction=round(quantity * per_unit / equity, 10),
                             max_slippage_bps=SLIPPAGE_BPS)

    def intents_for(self, d: int, index: int, account_id: str, world: E.World, equity: float) -> List[C.Intent]:
        """The synthetic rotation: exit what was held ``HOLD_DAYS`` days, hold at most ``MAX_HELD`` positions,
        fill the rest with entries on free symbols. On the paused day acct-1 also exits its oldest position, so
        a paused decision carries an exit (executed) and entries (refused)."""
        held = {p.symbol: p for p in world.fake.positions() if p.symbol in SYMBOLS and p.signed_quantity}
        since = self.held_since[account_id]
        for symbol in list(since):
            if symbol not in held:                    # stopped out (or never filled): the venue is the truth
                del since[symbol]
        intents: List[C.Intent] = []
        for symbol, position in sorted(held.items()):
            since.setdefault(symbol, d)
            if d - since[symbol] >= HOLD_DAYS:
                direction = C.LONG if position.signed_quantity > 0 else C.SHORT
                intents.append(C.ExitIntent(symbol=symbol, direction=direction, reason="SIGNAL",
                                            reference_price=self.price[symbol]))
        if d == PAUSED_DAY and account_id == "acct-1" and not intents and held:
            symbol = min(held, key=lambda s: (since[s], s))
            direction = C.LONG if held[symbol].signed_quantity > 0 else C.SHORT
            intents.append(C.ExitIntent(symbol=symbol, direction=direction, reason="SIGNAL",
                                        reference_price=self.price[symbol]))
        exiting = {i.symbol for i in intents}
        free = [s for s in SYMBOLS if s not in held and s not in exiting]
        room = MAX_HELD - len(set(held) - exiting)
        chosen = [free[(d + index + k) % len(free)] for k in range(min(room, len(free)))] if free else []
        risk = world.limits.per_trade_risk
        for k, symbol in enumerate(dict.fromkeys(chosen)):
            direction = C.LONG if (d + index + k) % 3 else C.SHORT
            intents.append(self.sized_entry(symbol, direction, equity, risk))
        return intents

    def execute_day(self, d: int, now: int) -> None:
        """The synthetic family's decisions of day ``d`` for every account, after the day's market stage
        completed on fresh data."""
        self.batches[d] = {}
        for index, (account_id, user_id, level) in enumerate(ACCOUNTS):
            world, boundary = self.proc.worlds[account_id], self.proc.boundaries[account_id]
            refreshed = world.rules.refresh(world.fake, self.clock())          # instrument rules: fresh every day
            assert refreshed.ok, refreshed
            flags = self.brake_flags[d][account_id]
            equity = float(world.fake.balance().equity)
            intents = self.intents_for(d, index, account_id, world, equity)
            if d == OVERSIZE_DAY and account_id == "acct-2":
                # above the 0.40 % system ceiling: not even recordable as a decision
                symbol = next(s for s in SYMBOLS if s not in {i.symbol for i in intents} and not world.fake.positions(s))
                with pytest.raises(C.ContractViolation, match="RISK_ABOVE_CEILING"):
                    world.decide(*intents, self.sized_entry(symbol, C.LONG, equity, 0.0, quantity_risk=0.006),
                                 constraints=world.limits.constraints(equity), ttl=E.HOUR)
                self.ceiling_refused += 1
            constraints = world.limits.constraints(equity)
            if d == OVERSIZE_DAY and account_id == "acct-1":
                # inside the system ceiling but above this conservative account's own per-trade risk (0.25 %): not
                # recordable under the account's own limits; recorded under a looser profile's limits, the boundary
                # still measures it against the account's and refuses it -- never resized
                symbol = next(s for s in SYMBOLS if s not in {i.symbol for i in intents} and not world.fake.positions(s))
                oversized = self.sized_entry(symbol, C.LONG, equity, 0.0, quantity_risk=0.0035)
                with pytest.raises(C.ContractViolation, match="RISK_ABOVE_CEILING"):
                    world.decide(*intents, oversized, constraints=constraints, ttl=E.HOUR)
                self.ceiling_refused += 1
                intents.append(oversized)
                constraints = P.AccountLimits.from_profile("balanced").constraints(equity)
                self.oversized = symbol
            batch = world.decide(*intents, constraints=constraints, ttl=E.HOUR)
            self.batches[d][account_id] = batch
            entries = [i for i in intents if i.kind == C.ENTRY]
            if d == SLIP_DAY and self.slip_account is None and entries:
                self.slip_account = account_id
                for i in entries:                     # the market runs away between the decision and the order
                    self.jump(i.symbol, 1.012 if i.direction == C.LONG else 0.988)
            if d == CRASH_DAY and self.crash_account is None and any(i.kind == C.ENTRY for i in intents):
                # the process dies right after an opening order left and before its stop: the position is on the
                # venue unprotected and the reservation is SUBMITTED; the next process must protect and settle it
                adapter, fake = world.adapter, world.fake

                def unprotected() -> bool:
                    return any(g.open for symbol in SYMBOLS for g in fake.protection_gaps(symbol)[-1:])

                def check() -> None:                  # after every order: die at the next call once a fill is bare
                    if unprotected():
                        adapter.die_at(0, after_effect=False)
                    else:
                        adapter.hook_after("place_order", check)

                adapter.hook_after("place_order", check)
                with pytest.raises(E.SimulatedCrash):
                    boundary.execute(batch, brake_state=flags)
                self.crash_account = account_id
                self.crash_gap_open = unprotected()
                self.restart()
                boundary = self.proc.boundaries[account_id]
                world = self.proc.worlds[account_id]
            result = boundary.execute(batch, brake_state=flags)
            assert result.status in (X.BATCH_PROCESSED, X.BATCH_HALTED), (d, account_id, result)
            self._record(d, account_id, world, batch, result, equity)
            # the REAL family, given the same intents and an intact decision record: refused at the family gate
            mutations = len(world.fake.mutations)
            real = world.boundary(family_gate=self.proc.real_gate)
            refused = real.execute(world.decide(*[i for i in intents if i.kind == C.ENTRY] or intents,
                                                definition=FR.definitions()["DAILY_TREND"], constraints=constraints),
                                   brake_state=B.NO_BRAKE)
            assert refused.status == X.BATCH_REFUSED and refused.outcomes == (), refused
            assert len(world.fake.mutations) == mutations
            self.real_family_refusals.append(refused.reasons[0].split(":")[0])
        self.executed[d] = True

    def _record(self, d: int, account_id: str, world: E.World, batch: C.IntentBatch, result: X.BatchResult,
                equity: float) -> None:
        for outcome in result.outcomes:
            if outcome.state == X.REFUSED:
                self.refusals.setdefault(account_id, []).append(f"{d}:{outcome.symbol}:{' | '.join(outcome.reasons)}")
        with world.db.connect() as conn:
            rows = [dict(r) for r in conn.execute(
                f"SELECT * FROM {INTENT_TABLE} WHERE account_id=? AND decision_id=? AND kind='ENTRY' AND filled_quantity>0",
                (account_id, batch.provenance.decision_id))]
        for row in rows:
            risk = float(row["filled_quantity"]) * abs(float(row["average_price"]) - float(row["stop_price"])) / equity
            self.entry_risks.append({"day": d, "account": account_id, "symbol": row["symbol"], "risk": risk,
                                     "stop": float(row["stop_price"]), "client_order_id": row["client_order_id"]})
            self.held_since[account_id][row["symbol"]] = d

    # --------------------------------------------------------------------------------------------- one day
    def close_brakes(self, d: int, now: int) -> None:
        day = START + d
        self.closing_equity[d], self.brake_flags[d] = {}, {}
        for account_id, _, level in ACCOUNTS:
            fake = self.fakes[account_id]
            equity = float(fake.balance().equity)
            kw = {"initial_equity": E.EQUITY} if d == 0 else {}
            decision = B.evaluate(self.proc.db, account_id=account_id, scope=B.ACCOUNT_SCOPE, risk_day=DT.day_text(day),
                                  day_definition=B.UTC_DAY, closing_equity=equity,
                                  thresholds=B.BrakeThresholds.from_profile(level), now_ms=now, **kw)
            self.closing_equity[d][account_id] = equity
            self.brake_flags[d][account_id] = decision.flags

    def manual_loss(self) -> None:
        """The owner trades by hand on acct-1 and loses 1.5 % of the equity (not a family position)."""
        fake = self.fakes["acct-1"]
        fake.external_trade(MANUAL, c.BUY, 10_000, price=0.1)
        fake.external_trade(MANUAL, c.SELL, 10_000, price=0.085)

    def maintain(self, d: int) -> None:
        for account_id, boundary in self.proc.boundaries.items():
            report = boundary.recover()
            assert report.status == X.BATCH_PROCESSED, (d, account_id, report)
            fake = self.fakes[account_id]
            for symbol in SYMBOLS:                    # every 30 s: no family position rests without a stop
                gaps = fake.protection_gaps(symbol)
                assert not gaps or not gaps[-1].open, (d, account_id, symbol, gaps[-1])
        self.maintenance[d] = self.maintenance.get(d, 0) + 1

    def run_day(self, d: int) -> None:
        day = START + d
        close = DT.close_ms(day)
        if d == CRASH_DAY:
            self.proc.runner.crash_after = 2          # this process's driver dies inside the day's account stage
        t = close - WINDOW_BEFORE_MS
        first = True
        while t <= close + WINDOW_AFTER_MS:
            self.clock.t = max(self.clock.t, t)
            self.move(0.03 if first else 0.0004)      # an overnight gap at the first tick, then small steps
            first = False
            # one connection for the tick, as the driver itself does (``DB.scope``): every ``connect()`` block keeps
            # its own commit / rollback; only the cost of opening and closing a WAL connection per block is saved
            with self.proc.db.scope():
                self._tick(d, day, close, t)
            t += TICK_MS
        assert self.executed.get(d), f"day {d}: the family never decided"

    def _tick(self, d: int, day: int, close: int, t: int) -> None:
        if d == BRAKE_LOSS_DAY and t == close + 10 * TICK_MS:
            self.manual_loss()
        if t >= close and d not in self.brake_flags:
            self.close_brakes(d, self.clock.t)
        try:
            results = self.proc.scheduler.tick()
        except E.SimulatedCrash:
            self.restart()
            results = []
        self.ticks[d] = self.ticks.get(d, 0) + 1
        for r in results:
            if r.cycle_key != DT.day_text(day):
                continue
            if r.status == D.WAITING_FOR_DATA:
                assert r.reason == "DECISION_DAY_HAS_NO_BARS", r
                self.waiting[d] = self.waiting.get(d, 0) + 1
                if d == LATE_DAY and self.stale_probe is None:
                    # the data are late: yesterday's decision, handed over again, is stale and refused
                    world = self.proc.worlds["acct-1"]
                    mutations = len(world.fake.mutations)
                    self.stale_probe = self.proc.boundaries["acct-1"].execute(self.batches[d - 1]["acct-1"],
                                                                                brake_state=B.NO_BRAKE)
                    assert len(world.fake.mutations) == mutations
            elif r.reason == "CYCLE_RUNNING_ELSEWHERE":
                self.elsewhere[d] = self.elsewhere.get(d, 0) + 1
        if self._completed(day) and not self.executed.get(d):
            assert self.feed.published(day)       # an execution step is never taken on unpublished bars
            self.execute_day(d, self.clock.t)
        self.maintain(d)

    def _completed(self, day: int) -> bool:
        with self.proc.db.connect() as conn:
            row = conn.execute("SELECT status FROM cati_platform_cycles WHERE family_id='DAILY_TREND' AND cycle_key=?",
                               (DT.day_text(day),)).fetchone()
        return row is not None and row[0] == D.COMPLETED


@pytest.fixture(scope="module")
def sim(tmp_path_factory):
    simulation = Sim(tmp_path_factory, tmp_path_factory.mktemp("sustained"))
    for d in range(DAYS):
        simulation.run_day(d)
    return simulation


def _rows(sim: Sim, sql: str, args=()):
    with sim.proc.db.connect() as conn:
        return [dict(r) for r in conn.execute(sql, args)]


# =============================================================================================== cadence
def test_maintenance_runs_every_30_seconds_while_the_daily_decision_runs_once_a_day(sim):
    per_day = (WINDOW_BEFORE_MS + WINDOW_AFTER_MS) // TICK_MS + 1
    assert sorted(sim.ticks) == list(range(DAYS)) and set(sim.ticks.values()) == {per_day}
    assert all(sim.maintenance[d] == per_day for d in range(DAYS)) and per_day >= 60
    cycles = _rows(sim, "SELECT cycle_key, status, COUNT(*) AS n FROM cati_platform_cycles WHERE family_id='DAILY_TREND' "
                        "GROUP BY cycle_key, status ORDER BY cycle_key")
    # the first tick (before the first close) finds the previous close due -- a family seen for the first time
    # starts at the latest close, it does not backfill -- then one cycle per close follows
    assert [(r["cycle_key"], r["status"], r["n"]) for r in cycles] == [
        (DT.day_text(START + d), D.COMPLETED, 1) for d in range(-1, DAYS)]
    decisions = _rows(sim, "SELECT account_id, data_cutoff, COUNT(*) AS n FROM cati_family_decisions WHERE "
                           "family_id='DAILY_TREND' AND mode='SHADOW' GROUP BY account_id, data_cutoff")
    accounts = {r["account_id"] for r in decisions}
    assert {a for a, _, _ in ACCOUNTS} <= accounts and len(accounts) == len(ACCOUNTS) + 2     # + two model books
    assert all(r["n"] == 1 for r in decisions) and len(decisions) == (DAYS + 1) * len(accounts)
    # the synthetic family decided once per account per day too, never twice
    assert all(sim.executed.get(d) for d in range(DAYS)) and all(len(sim.batches[d]) == len(ACCOUNTS) for d in range(DAYS))


def test_data_freshness_waits_for_late_bars_and_a_stale_decision_is_refused(sim):
    assert sim.waiting.get(LATE_DAY, 0) >= (LATE_DELAY_MS - PUBLISH_DELAY_MS) // TICK_MS
    on_time = [d for d in range(DAYS) if d not in (LATE_DAY,)]
    assert all(sim.waiting.get(d, 0) <= PUBLISH_DELAY_MS // TICK_MS + 1 for d in on_time)
    assert sim.stale_probe is not None and sim.stale_probe.reasons == (X.DECISION_STALE,)
    assert sim.stale_probe.status == X.BATCH_REFUSED and sim.stale_probe.outcomes == ()


def test_shadow_decisions_are_immutable_and_replays_reproduce_them(sim):
    sql = ("SELECT account_id, data_cutoff, decision_hash FROM cati_family_decisions WHERE family_id='DAILY_TREND' "
           "AND mode='SHADOW' ORDER BY 1, 2")
    before = _rows(sim, sql)
    for d in (0, LATE_DAY, CRASH_DAY, DAYS - 1):
        replay = sim.proc.driver.run_cycle("DAILY_TREND", DT.day_text(START + d), now_ms=sim.clock.t)
        assert (replay.status, replay.replayed) == (D.COMPLETED, True)
    assert _rows(sim, sql) == before
    row = _rows(sim, "SELECT * FROM cati_family_decisions WHERE family_id='DAILY_TREND' AND account_id='acct-1' "
                     "AND data_cutoff=?", (DT.day_text(START + CRASH_DAY),))[0]
    fields = {k: row[k] for k in ("cycle_id", "family_id", "family_version", "rule_hash", "mandate_id", "account_id",
                                  "user_id", "environment", "mode", "decision_time", "data_cutoff", "expires_at")}
    document = json.loads(row["document"])
    again = PV.DecisionStore(sim.proc.db).record(now_ms=sim.clock.t, document=document, **fields)
    assert again == {"decision_id": row["decision_id"], "decision_hash": row["decision_hash"], "replayed": True}
    document["stages"] = dict(document["stages"], TAMPERED={"status": "EXECUTED"})   # a different decision, same key
    with pytest.raises(PV.DecisionConflict):
        PV.DecisionStore(sim.proc.db).record(now_ms=sim.clock.t, document=document, **fields)
    assert _rows(sim, sql) == before
    for r in _rows(sim, "SELECT document FROM cati_family_decisions WHERE family_id='DAILY_TREND' AND mode='SHADOW'"):
        doc = json.loads(r["document"])
        assert doc["order_authority"]["handoff"] == "NONE" and doc["stages"]["EXECUTION_BOUNDARY"]["status"] == "SHADOW_ONLY"


def test_an_account_whose_bot_is_stopped_is_never_enrolled(sim):
    assert _rows(sim, "SELECT * FROM cati_family_decisions WHERE account_id=?", (STOPPED_ACCOUNT[0],)) == []
    assert _rows(sim, "SELECT * FROM cati_platform_cycle_accounts WHERE account_id=?", (STOPPED_ACCOUNT[0],)) == []


def test_every_real_family_decision_is_refused_at_the_family_gate(sim):
    assert len(sim.real_family_refusals) == DAYS * len(ACCOUNTS)
    assert set(sim.real_family_refusals) <= {PV.FAMILY_NOT_REGISTERED, PV.FAMILY_NOT_CERTIFIED}
    families = {r["family_id"] for r in _rows(sim, f"SELECT DISTINCT family_id FROM {INTENT_TABLE}")}
    assert families == {S.TEST_FAMILY}                # every order row is the synthetic family's


# ============================================================================================ the stages
def test_brakes_pause_entries_for_one_decision_and_exits_still_run(sim):
    flags = sim.brake_flags
    assert flags[PAUSED_DAY]["acct-1"].paused and not flags[PAUSED_DAY]["acct-1"].new_entries_allowed
    paused_days = [(d, a) for d in range(DAYS) for a, f in flags[d].items() if f.paused or f.halted]
    assert paused_days == [(PAUSED_DAY, "acct-1")]
    batch = sim.batches[PAUSED_DAY]["acct-1"]
    rows = _rows(sim, f"SELECT kind, state, reason, filled_quantity FROM {INTENT_TABLE} WHERE decision_id=?",
                 (batch.provenance.decision_id,))
    entries = [r for r in rows if r["kind"] == C.ENTRY]
    exits = [r for r in rows if r["kind"] == C.EXIT]
    assert entries and all(r["state"] == X.REFUSED and r["filled_quantity"] == 0 for r in entries), rows
    assert exits and all(r["state"] == X.FILLED for r in exits), rows
    assert not flags[PAUSED_DAY + 1]["acct-1"].paused                 # it holds for that decision only


def test_sizing_and_the_price_check_refuse_and_never_resize(sim):
    assert sim.slip_account is not None
    slipped = [r for r in sim.refusals.get(sim.slip_account, []) if r.startswith(f"{SLIP_DAY}:")]
    assert slipped and all("SLIPPAGE_BOUND_EXCEEDED" in r or "EXECUTABLE_PRICE_RISK_CHANGED" in r for r in slipped), slipped
    oversized = [r for r in sim.refusals.get("acct-1", []) if r.startswith(f"{OVERSIZE_DAY}:{sim.oversized}:")]
    assert sim.oversized and len(oversized) == 1, sim.refusals.get("acct-1")
    assert sim.ceiling_refused == 2                    # neither oversized entry became a decision under its own limits
    batch = sim.batches[SLIP_DAY][sim.slip_account]
    sent = _rows(sim, f"SELECT * FROM {INTENT_TABLE} WHERE decision_id=? AND kind='ENTRY' AND client_order_id IS NOT NULL "
                      "AND state NOT IN ('REFUSED', 'PLANNED')", (batch.provenance.decision_id,))
    assert sent == []


def test_the_restart_in_the_middle_recovered_and_the_driver_resumed_the_abandoned_cycle(sim):
    assert sim.restarts == 2 and sim.generation == 2
    assert sim.elsewhere.get(CRASH_DAY, 0) >= 1      # the dead owner's cycle was left alone until its heartbeat aged
    cycle = _rows(sim, "SELECT attempt, owner, status FROM cati_platform_cycles WHERE family_id='DAILY_TREND' AND cycle_key=?",
                  (DT.day_text(START + CRASH_DAY),))[0]
    assert cycle["status"] == D.COMPLETED and cycle["attempt"] >= 2 and cycle["owner"] == "sustained-process-1"
    assert sim.crash_account is not None and sim.crash_gap_open      # the crash really left a position without its stop
    batch = sim.batches[CRASH_DAY][sim.crash_account]
    rows = _rows(sim, f"SELECT symbol, state FROM {INTENT_TABLE} WHERE decision_id=?", (batch.provenance.decision_id,))
    assert rows and all(r["state"] in INTENT_FINAL_STATES for r in rows), rows


# ========================================================================================= the invariants
def test_no_position_ever_rested_without_a_stop(sim):
    gaps_seen = 0
    for account_id, fake in sim.fakes.items():
        for symbol in SYMBOLS:
            for gap in fake.protection_gaps(symbol):
                gaps_seen += 1
                assert not gap.open, (account_id, symbol, gap)
                # the only uncovered interval is the boundary's own step from the opening fill to its stop: the very
                # next state change of the venue after the fill is the stop being placed
                assert gap.end_event == gap.start_event + 1, (account_id, symbol, gap)
                assert gap.duration_ms < TICK_MS, (account_id, symbol, gap)
    assert gaps_seen >= len(sim.entry_risks) // 2     # entries did happen and were each covered


def test_no_order_was_ever_sent_twice(sim):
    for account_id, fake in sim.fakes.items():
        opening = [e for e in fake.mutations if e.method == "place_order" and e.effect
                   and not e.arguments["request"].protective and not e.arguments["request"].reduce_only]
        ids = [e.arguments["request"].client_order_id for e in opening]
        assert len(ids) == len(set(ids)), account_id
        rows = _rows(sim, f"SELECT decision_id, symbol, COUNT(DISTINCT client_order_id) AS n FROM {INTENT_TABLE} WHERE "
                          "account_id=? AND kind='ENTRY' AND client_order_id IS NOT NULL AND state NOT IN ('REFUSED') "
                          "GROUP BY decision_id, symbol", (account_id,))
        assert all(r["n"] == 1 for r in rows), (account_id, rows)
        known = {r["client_order_id"] for r in _rows(sim, f"SELECT client_order_id FROM {INTENT_TABLE} WHERE account_id=?",
                                                     (account_id,))}
        assert set(ids) <= known, account_id          # every opening order the venue holds is a recorded intent
    assert sum(len(sim.batches[d]) for d in sim.batches) == DAYS * len(ACCOUNTS)


def test_no_reservation_is_left_open_and_nothing_is_unresolved(sim):
    states = {r["state"] for r in _rows(sim, "SELECT state FROM cati_reservation_ledger")}
    assert states and states <= {R.FILLED, R.RELEASED}, states
    for account_id, boundary in sim.proc.boundaries.items():
        assert boundary.ledger.unresolved(account_id) == [], account_id
        assert boundary.journal.unanswered() == [], account_id
    open_rows = _rows(sim, f"SELECT * FROM {INTENT_TABLE} WHERE state IN ('SUBMITTING', 'UNKNOWN', 'CANCELING')")
    assert open_rows == []
    for world in sim.proc.worlds.values():
        assert world.adapter.open_transactions == []   # no database transaction spanned a venue call


def test_risk_of_every_filled_entry_stayed_within_the_per_trade_ceiling(sim):
    assert len(sim.entry_risks) >= DAYS              # the order path really ran
    worst = max(r["risk"] for r in sim.entry_risks)
    assert worst <= CEILING + 1e-12, max(sim.entry_risks, key=lambda r: r["risk"])
    for account_id, _, level in ACCOUNTS:
        limit = P.AccountLimits.from_profile(level).per_trade_risk
        mine = [r["risk"] for r in sim.entry_risks if r["account"] == account_id]
        assert mine and max(mine) <= limit + 1e-12, (account_id, max(mine), limit)


def test_the_market_really_moved_positions_into_their_stops_and_signals_closed_others(sim):
    stops_fired = exits_filled = 0
    for fake in sim.fakes.values():
        protective = {e.arguments["request"].client_order_id for e in fake.mutations
                      if e.method == "place_order" and e.arguments["request"].protective}
        for symbol in SYMBOLS:
            stops_fired += len({f.client_order_id for f in fake.fills(symbol) if f.client_order_id in protective})
    exits_filled = len(_rows(sim, f"SELECT * FROM {INTENT_TABLE} WHERE kind='EXIT' AND state='FILLED'"))
    assert stops_fired >= 1 and exits_filled >= 1, (stops_fired, exits_filled)
