"""Step 3.8: the execution boundary for a list of portfolio intents (``pipeline.execution``).

Everything runs against ``FakeVenue`` and a temporary database. The family that is authorized is synthetic
(``_step3_execution``); the real families of the repository appear in one test, which proves they are refused.
A refusal is always proved on the venue's own journals: ``fake.mutations`` holds every call that could change
something and ``fake.reads`` every question the venue was asked.

What a passing test says: the state machine, the order of calls and the durable records are right against a
venue that follows the adapter contract exactly. It says nothing about how a real venue behaves.
"""
from __future__ import annotations

import dataclasses
import sqlite3

import pytest

import _step3 as S
import _step3_execution as E
from app.exchange.platform_adapter import contract as c
from app.exchange.platform_adapter.contract import AdapterError
from app.trading_intelligence.governance.promotion import GovernanceAuthority, PromotionGovernance
from app.trading_intelligence.pipeline import brakes
from app.trading_intelligence.pipeline import contracts as C
from app.trading_intelligence.pipeline import execution as X
from app.trading_intelligence.pipeline import family_registry as FR
from app.trading_intelligence.pipeline import protection as PR
from app.trading_intelligence.pipeline import provenance as PV
from app.trading_intelligence.pipeline import reservations as R
from shared_lib.persistence import cati_execution_schema as schema
from shared_lib.persistence import cati_platform_schema as platform_schema
from shared_lib.persistence import cati_portfolio_schema as portfolio_schema
from shared_lib.persistence.cati_platform_schema import ensure_cati_platform_schema
from shared_lib.persistence.db import DB

BTC, ETH, SOL, XRP = E.BTC, E.ETH, E.SOL, E.XRP
NEW_TABLES = {schema.INTENT_TABLE, schema.EVENT_TABLE, schema.PROTECTION_TABLE, schema.REPLACEMENT_TABLE,
              schema.BLOCK_TABLE, schema.BACKOFF_TABLE}


@pytest.fixture
def world(tmp_path_factory, tmp_path):
    return E.make_world(tmp_path_factory, tmp_path)


def states(result):
    return {o.symbol: o.state for o in result.outcomes}


def why(result, symbol):
    return " | ".join(result.outcome(symbol).reasons)


def covered(world, symbol):
    """The position on ``symbol`` rests under a stop that covers it now (or there is no position)."""
    gaps = world.fake.protection_gaps(symbol)
    return not gaps or not gaps[-1].open


def untouched(world):
    """The venue was neither changed nor asked anything."""
    assert world.fake.mutations == [] and world.fake.reads == [] and world.adapter.calls == []


def entry_orders(world, symbol=None):
    """Orders that could open or enlarge a position, as the venue received them (answered or not)."""
    return [r for r in world.orders_sent(symbol, protective=False) if not r.reduce_only]


def open_three(world):
    result = world.run(E.entry(BTC), E.entry(ETH), E.entry(SOL))
    assert states(result) == {BTC: X.FILLED, ETH: X.FILLED, SOL: X.FILLED}
    return result


# ==================================================================================================== schema
def master(db):
    with db.connect() as conn:
        return sorted((r[0], r[1], r[2] or "") for r in conn.execute("SELECT type, name, sql FROM sqlite_master").fetchall())


def test_the_schema_applies_twice_and_on_a_database_that_already_holds_the_earlier_tables(tmp_path):
    db = DB(str(tmp_path / "earlier.db"))
    with db.connect() as conn:                                      # a database of the build before this module
        for statement in platform_schema.DDL + portfolio_schema.DDL:
            conn.execute(statement)
        conn.execute(f"INSERT INTO {platform_schema.DECISION_TABLE} VALUES ('d1','c1','F','1','h',NULL,'a1','u1','DEMO','DEMO',1,'x',2,'{{}}','hash',3)")
    earlier = master(db)
    assert not NEW_TABLES & {name for _type, name, _sql in earlier}
    ensure_cati_platform_schema(db)
    first = master(db)
    assert NEW_TABLES <= {name for kind, name, _sql in first if kind == "table"}
    assert set(earlier) <= set(first)                               # nothing that existed was rewritten or dropped
    with db.connect() as conn:
        assert conn.execute(f"SELECT COUNT(*) FROM {platform_schema.DECISION_TABLE}").fetchone()[0] == 1
    ensure_cati_platform_schema(db)
    with db.connect() as conn:
        platform_schema.ensure_on_connection(conn)
        for statement in schema.DDL:
            conn.execute(statement)
        schema.upgrade(conn)
    assert master(db) == first                                      # applied again: no difference at all


def test_the_database_itself_keeps_the_rules_of_the_state_machine(tmp_path):
    db = DB(str(tmp_path / "rules.db"))
    ensure_cati_platform_schema(db)

    def insert(intent_id, state, client_order_id="cid", role="PRIMARY"):
        with db.connect() as conn:
            conn.execute(
                f"INSERT INTO {schema.INTENT_TABLE} (intent_id, account_id, venue, environment, family_id, decision_id, "
                "decision_hash, batch_hash, symbol, kind, direction, sequence, role, increases_risk, client_order_id, state, "
                "intent_document, expires_at, created_at, updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (intent_id, "a1", "binance", "DEMO", "F", f"d-{intent_id}", "h", "b", BTC, "ENTRY", "LONG", 0, role, 1,
                 client_order_id, state, "{}", 10, 1, 1))

    def refused(sql, *params):
        with pytest.raises(sqlite3.DatabaseError):
            with db.connect() as conn:
                conn.execute(sql, params)

    update = f"UPDATE {schema.INTENT_TABLE} SET "
    insert("i-final", "FILLED", "c1")
    refused(update + "state='OPEN' WHERE intent_id='i-final'")                        # a final intent does not change state
    insert("i-sent", "SUBMITTING", "c2")
    refused(update + "state='PLANNED' WHERE intent_id='i-sent'")                      # an order that may have left is never unsent
    refused(update + "state='REFUSED' WHERE intent_id='i-sent'")
    refused(update + "client_order_id='other' WHERE intent_id='i-sent'")              # an order id is never reassigned
    refused(update + "symbol='ETHUSDT' WHERE intent_id='i-sent'")                     # the identity is immutable
    refused(f"DELETE FROM {schema.INTENT_TABLE} WHERE intent_id='i-sent'")            # an open intent is never deleted
    insert("i-unknown", "UNKNOWN", "c3")
    refused(update + "state='ABSENT' WHERE intent_id='i-unknown'")                    # not without evidence
    with db.connect() as conn:
        conn.execute(update + "state='ABSENT', evidence='ORDER_NOT_FOUND after 130 s' WHERE intent_id='i-unknown'")
    insert("i-plain", "PLANNED", None)
    refused(update + "state='SUBMITTING' WHERE intent_id='i-plain'")                  # identified before it is sent
    with pytest.raises(sqlite3.DatabaseError):                                        # one order id, one order
        insert("i-twin", "PLANNED", "c1")
    with db.connect() as conn:                                                        # the database wrote the history itself
        history = [tuple(r) for r in conn.execute(
            f"SELECT from_state, to_state FROM {schema.EVENT_TABLE} WHERE intent_id='i-unknown' ORDER BY seq").fetchall()]
    assert history == [(None, "UNKNOWN"), ("UNKNOWN", "ABSENT")]
    refused(f"UPDATE {schema.EVENT_TABLE} SET outcome='x'")                           # the journal is append-only
    refused(f"DELETE FROM {schema.EVENT_TABLE}")
    journal = PR.VenueJournal(db, account_id="a1", venue="binance", environment="DEMO", clock=E.Clock())
    block = journal.block("TEST_BLOCK", symbol=BTC, operator_only=True)
    refused(f"UPDATE {schema.BLOCK_TABLE} SET state='CLEARED' WHERE block_id=?", block)      # not without evidence and an actor
    refused(f"DELETE FROM {schema.BLOCK_TABLE} WHERE block_id=?", block)
    assert not journal.clear("TEST_BLOCK", symbol=BTC, evidence="automatic")          # an operator-only block is not lifted by code
    assert not journal.clear_by_operator(block, operator_ref="", evidence="looked at it")
    assert journal.clear_by_operator(block, operator_ref="admin-7", evidence="position reviewed, account flat")
    refused(f"UPDATE {schema.BLOCK_TABLE} SET state='ACTIVE' WHERE block_id=?", block)       # a cleared block is final
    journal.note_limit(AdapterError(c.RATE_LIMITED, retry_after_s=30))
    refused(f"UPDATE {schema.BACKOFF_TABLE} SET retry_not_before=1")                  # a back-off ends by time only
    assert journal.note_limit(AdapterError(c.RATE_LIMITED, retry_after_s=1)) == journal.backoff_until()


# ======================================================================================== refusals: no call
def test_a_shadow_batch_and_every_other_non_executing_mode_never_reach_the_venue(world):
    for mode in (C.SHADOW, C.RESEARCH_SIMULATION, C.OPERATOR_CERTIFICATION):
        result = world.run(E.entry(BTC), E.entry(ETH), mode=mode)
        assert (result.status, result.reasons, result.outcomes) == (X.BATCH_REFUSED, (f"{X.MODE_NOT_EXECUTABLE}:{mode}",), ())
    untouched(world)                                                # the proof of a shadow run: fake.mutations == []
    assert world.rows(schema.INTENT_TABLE) == [] and world.reservations() == {}
    gates = world.boundary().journal.events(event_type="GATE")      # refused, and recorded as refused
    assert [g["outcome"] for g in gates] == [f"{X.MODE_NOT_EXECUTABLE}:{m}" for m in (C.SHADOW, C.RESEARCH_SIMULATION, C.OPERATOR_CERTIFICATION)]


def test_no_real_family_of_this_repository_can_send_an_order(world):
    """The committed research register, the families the code defines, the real gate: none is certified, all are
    at M0. Each is given an intact decision record and a certified adapter, and is still refused."""
    gate = PV.FamilyExecutionGate(world.db, venue_certified=lambda venue, environment, now_ms: (True, ""))
    families = FR.definitions()
    assert {"DAILY_TREND", "RESIDUAL_MOMENTUM_PORTFOLIO_TOP1"} <= set(families) and len(families) >= 6
    boundary = world.boundary(family_gate=gate)
    seen = {}
    for registered in (False, True):
        if registered:
            gate.registry.register_all(actor_ref="system:test")
        for family_id, definition in sorted(families.items()):
            for intents in ((E.entry(BTC), E.entry(ETH)), (E.exit_(BTC),)):
                result = boundary.execute(world.decide(*intents, definition=definition), brake_state=E.NO_BRAKE)
                assert result.status == X.BATCH_REFUSED and result.outcomes == (), (family_id, result)
                seen.setdefault(family_id, set()).add(result.reasons[0].split(":")[0])
    untouched(world)
    assert world.rows(schema.INTENT_TABLE) == [] and world.reservations() == {}
    for family_id, reasons in seen.items():
        assert reasons <= {PV.FAMILY_NOT_REGISTERED, PV.FAMILY_NOT_CERTIFIED, PV.FAMILY_FAILED_RESEARCH}, (family_id, reasons)
    assert seen["RESIDUAL_MOMENTUM_PORTFOLIO_TOP1"] == {PV.FAMILY_NOT_REGISTERED, PV.FAMILY_FAILED_RESEARCH}
    assert seen["DAILY_TREND"] == {PV.FAMILY_NOT_REGISTERED, PV.FAMILY_NOT_CERTIFIED}
    assert gate.registry.executable() == []                         # and the registry agrees: nothing is executable


def test_the_real_gate_is_the_default_and_the_real_flags_refuse(world, monkeypatch):
    from app.core.config import settings

    # the value the repository ships (false), whatever an operator's local environment file says
    monkeypatch.setattr(settings, "DEMO_ORDER_SUBMISSION_ENABLED", False, raising=False)
    boundary = X.PortfolioExecutionBoundary(db=world.db, adapter=world.adapter, account=world.account, limits=world.limits,
                                            rules=world.rules, adapter_certification=lambda venue, environment: (True, ""),
                                            clock=world.clock)
    assert type(boundary.family_gate) is PV.FamilyExecutionGate
    result = boundary.execute(world.decide(E.entry(BTC)), brake_state=E.NO_BRAKE)
    assert result.status == X.BATCH_REFUSED and result.reasons[0].split(":")[0] == PV.FAMILY_UNKNOWN   # the synthetic family is not in the code
    # with the family authorized, the real order-submission flag of the environment refuses everything, exits included
    flagged = world.boundary(submission_gate=None)
    result = flagged.execute(world.decide(E.entry(BTC), E.exit_(ETH)), brake_state=E.NO_BRAKE)
    assert (result.status, result.reasons) == (X.BATCH_REFUSED, ("DEMO_ORDER_SUBMISSION_DISABLED",))
    assert flagged.recover().reasons == ("DEMO_ORDER_SUBMISSION_DISABLED",)           # recovery sends nothing either
    untouched(world)


def _other_user(w):
    return w.boundary(), w.decide(E.entry(BTC), account=C.AccountRef("user-2", E.ACCOUNT, c.BINANCE, c.DEMO)), None


def _other_account(w):
    return w.boundary(), w.decide(E.entry(BTC), account=C.AccountRef(E.USER, "acct-9", c.BINANCE, c.DEMO)), None


def _other_venue(w):
    return w.boundary(), w.decide(E.entry(BTC), account=C.AccountRef(E.USER, E.ACCOUNT, c.BYBIT, c.DEMO)), None


def _other_environment(w):
    return w.boundary(), w.decide(E.entry(BTC), mode=C.LIVE, account=C.AccountRef(E.USER, E.ACCOUNT, c.BINANCE, c.LIVE)), None


def _mode_is_not_the_environment(w):
    return w.boundary(), w.decide(E.entry(BTC), mode=C.LIVE), None


def _stale(w):
    batch = w.decide(E.entry(BTC), ttl=1_000)
    w.clock.advance(2_000)
    return w.boundary(), batch, None


def _future_dated(w):
    batch = w.decide(E.entry(BTC))
    return w.boundary(), batch, batch.provenance.decision_time_ms - 60_000


def _forged_decision(w):
    batch = w.decide(E.entry(BTC))
    return w.boundary(), dataclasses.replace(batch, provenance=dataclasses.replace(batch.provenance, decision_id="fdec_forged")), None


def _enlarged_after_the_decision(w):
    batch = w.decide(E.entry(BTC))
    return w.boundary(), dataclasses.replace(batch, intents=(E.entry(BTC, risk=0.0025),)), None


def _added_after_the_decision(w):
    batch = w.decide(E.entry(BTC))
    return w.boundary(), dataclasses.replace(batch, intents=batch.intents + (E.entry(ETH),)), None


def _wrong_decision_hash(w):
    batch = w.decide(E.entry(BTC))
    return w.boundary(), dataclasses.replace(batch, provenance=dataclasses.replace(batch.provenance, decision_hash="0" * 64)), None


def _unknown_family(w):
    return w.boundary(), w.decide(E.entry(BTC), definition=S.definition(family_id="NOT_A_FAMILY")), None


def _certification_revoked(w):
    w.registry.governance.revoke_certification(w.definition.family_id, operator=S.OPERATOR, reason="test")
    return w.boundary(), w.decide(E.exit_(BTC), E.entry(ETH)), None


def _family_disabled(w):
    w.registry.governance.set_disabled(w.definition.family_id, True, operator=S.OPERATOR, reason="test")
    return w.boundary(), w.decide(E.exit_(BTC)), None


def _gate_says_no(w):
    return w.boundary(family_gate=PV.StaticFamilyGate(False, "NOT_TODAY")), w.decide(E.entry(BTC)), None


def _gate_cannot_decide(w):
    class Broken:
        def authorize(self, plan, *, now_ms):
            raise RuntimeError("registry unreadable")

    return w.boundary(family_gate=Broken()), w.decide(E.entry(BTC)), None


def _adapter_not_certified_for_the_gate(w):
    w.certified.clear()
    return w.boundary(adapter_certification=lambda venue, environment: (True, "")), w.decide(E.entry(BTC)), None


def _adapter_not_certified_for_the_boundary(w):
    return w.boundary(adapter_certification=lambda venue, environment: (False, "ADAPTER_EXPIRED")), w.decide(E.exit_(BTC)), None


def _certification_unreadable(w):
    def unreadable(venue, environment):
        raise OSError("register unreadable")

    return w.boundary(adapter_certification=unreadable), w.decide(E.entry(BTC)), None


def _submission_gate_unreadable(w):
    def unreadable(environment):
        raise OSError("settings unreadable")

    return w.boundary(submission_gate=unreadable), w.decide(E.entry(BTC)), None


@pytest.mark.parametrize("case,code", [
    (_other_user, X.EXECUTION_ACCOUNT_SCOPE_MISMATCH), (_other_account, X.EXECUTION_ACCOUNT_SCOPE_MISMATCH),
    (_other_venue, X.EXECUTION_VENUE_MISMATCH), (_other_environment, X.BROKER_ENVIRONMENT_MISMATCH),
    (_mode_is_not_the_environment, X.MODE_ENVIRONMENT_MISMATCH), (_stale, X.DECISION_STALE),
    (_future_dated, X.DECISION_FUTURE_DATED), (_forged_decision, PV.DECISION_EVIDENCE_MISSING),
    (_enlarged_after_the_decision, PV.DECISION_PLAN_MISMATCH), (_added_after_the_decision, PV.DECISION_PLAN_MISMATCH),
    (_wrong_decision_hash, PV.DECISION_TAMPERED), (_unknown_family, PV.FAMILY_UNKNOWN),
    (_certification_revoked, PV.FAMILY_CERTIFICATION_REVOKED), (_family_disabled, PV.FAMILY_DISABLED),
    (_gate_says_no, "NOT_TODAY"), (_gate_cannot_decide, PV.FAMILY_GATE_UNAVAILABLE),
    (_adapter_not_certified_for_the_gate, PV.VENUE_ADAPTER_NOT_CERTIFIED),
    (_adapter_not_certified_for_the_boundary, X.ADAPTER_NOT_CERTIFIED), (_certification_unreadable, X.ADAPTER_NOT_CERTIFIED),
    (_submission_gate_unreadable, "ORDER_SUBMISSION_GATE_UNAVAILABLE"),
], ids=lambda value: value.__name__.strip("_") if callable(value) else "")
def test_every_batch_gate_refuses_before_the_venue_is_asked_anything(world, case, code):
    open_three(world)                                               # there ARE positions an exit could close
    world.fake.mutations.clear()
    world.fake.reads.clear()
    world.adapter.calls.clear()
    rows, reservations = len(world.rows(schema.INTENT_TABLE)), world.reservations()
    boundary, batch, now_ms = case(world)
    result = boundary.execute(batch, brake_state=E.NO_BRAKE, now_ms=now_ms)
    assert result.status == X.BATCH_REFUSED and result.outcomes == (), result
    assert result.reasons[0].split(":")[0] == code, result.reasons
    untouched(world)
    assert len(world.rows(schema.INTENT_TABLE)) == rows and world.reservations() == reservations
    assert world.signed(BTC) > 0                                    # not even the exit in the batch was sent


def test_production_settings_accept_only_the_real_gates_and_never_the_fake_venue(world, monkeypatch):
    from app.core.config import settings

    class NotTheFake(E.Controlled):
        adapter_version = "pretend-venue-1.0"

    batch = world.decide(E.entry(BTC))
    pretend = NotTheFake(world.fake, world.db.path)
    monkeypatch.setattr(type(settings), "production", property(lambda self: True))
    monkeypatch.setattr(settings, "DEMO_ORDER_SUBMISSION_ENABLED", False, raising=False)      # as the repository ships it
    assert world.boundary().execute(batch, brake_state=E.NO_BRAKE).reasons == (X.TEST_ADAPTER_NOT_ALLOWED,)
    static = world.boundary(adapter=pretend, family_gate=PV.StaticFamilyGate(True))
    assert static.execute(batch, brake_state=E.NO_BRAKE).reasons == (X.FAMILY_GATE_NOT_AUTHORITATIVE,)
    doubled = world.boundary(adapter=pretend)                       # the real family gate, a substitute submission gate
    assert doubled.execute(batch, brake_state=E.NO_BRAKE).reasons == (X.SUBMISSION_GATE_NOT_AUTHORITATIVE,)
    real = world.boundary(adapter=pretend, submission_gate=None)    # the real one reads the real flag: false
    assert real.execute(batch, brake_state=E.NO_BRAKE).reasons == ("DEMO_ORDER_SUBMISSION_DISABLED",)
    assert real.recover().status == X.BATCH_REFUSED
    assert world.boundary()._increase_refusal() == X.GOVERNANCE_AUTHORITY_NOT_AUTHORITATIVE     # a substitute authority neither
    untouched(world)


# ============================================================================================== capabilities
def test_new_entries_on_several_symbols_are_reserved_identified_sent_once_and_protected(world):
    batch = world.decide(E.entry(BTC), E.entry(ETH), E.entry(SOL, direction=C.SHORT))
    boundary = world.boundary()
    result = boundary.execute(batch, brake_state=E.NO_BRAKE)
    assert result.status == X.BATCH_PROCESSED and states(result) == {BTC: X.FILLED, ETH: X.FILLED, SOL: X.FILLED}
    assert (world.signed(BTC), world.signed(ETH), world.signed(SOL)) == (0.008, 0.16, -4.0)
    # every position that exists rests under ONE exchange-side stop for the whole of it, read back
    assert [world.stop_prices(s) for s in (BTC, ETH, SOL)] == [[47_500.0], [2_375.0], [105.0]]
    assert world.stops(SOL)[0].side == c.BUY and all(covered(world, s) for s in (BTC, ETH, SOL))
    for outcome in result.outcomes:
        assert (outcome.protection, outcome.reservation_state, outcome.increases_risk) == (PR.PROTECTED, R.FILLED, True)
        # the order identity is a function of (account, decision, symbol, kind, sequence): a retry names the same order
        assert outcome.client_order_ids == (X.order_client_id(E.ACCOUNT, batch.provenance.decision_id, outcome.symbol, "ENTRY", 0),)
        assert len(outcome.client_order_ids[0]) == 32
    assert [(e.method, e.arguments.get("leverage")) for e in world.fake.mutations if e.method == "set_leverage"] == [
        ("set_leverage", 1)] * 3
    assert len(entry_orders(world)) == 3 and world.reservations() == {BTC: R.FILLED, ETH: R.FILLED, SOL: R.FILLED}
    # the reservation said SUBMITTED before the order left, and the journal brackets every mutation
    journal = boundary.journal
    for outcome in result.outcomes:
        (order_id,) = outcome.client_order_ids
        before = next(e for e in journal.events(event_type="VENUE_BEFORE") if e["client_order_id"] == order_id)
        ledger = [h for h in boundary.ledger.history(E.ACCOUNT, boundary.ledger.unresolved() and "" or
                                                     world.rows("cati_reservation_ledger", "symbol=?", (outcome.symbol,))[0]["reservation_id"])]
        assert [h["to_state"] for h in ledger] == [R.RESERVED, R.SUBMITTED, R.FILLED]
        assert ledger[1]["recorded_at"] < before["recorded_at"]
        states_of_row = [e["to_state"] for e in journal.events(intent_id=outcome.intent_id, event_type="STATE")]
        assert states_of_row == [X.PLANNED, X.SUBMITTING, X.FILLED]
    assert len(journal.events(event_type="VENUE_BEFORE")) == len(journal.events(event_type="VENUE_AFTER")) == len(world.fake.mutations)
    assert journal.unanswered() == [] and world.adapter.open_transactions == []     # no transaction spans a venue call
    assert boundary.entry_blocks() == ()


def test_a_replay_of_the_same_batch_sends_nothing_and_asks_nothing(world):
    batch = world.decide(E.entry(BTC), E.entry(ETH))
    first = world.boundary().execute(batch, brake_state=E.NO_BRAKE)
    mutations, reads = len(world.fake.mutations), len(world.fake.reads)
    for _ in range(3):
        again = world.boundary().execute(batch, brake_state=E.NO_BRAKE)             # a new process each time
        assert again.outcomes == first.outcomes
    assert (len(world.fake.mutations), len(world.fake.reads)) == (mutations, reads)
    assert (world.signed(BTC), world.signed(ETH)) == (0.008, 0.16)


def test_exit_reduce_add_and_rebalance_run_together_and_reductions_go_first(world):
    world.run(E.entry(BTC), E.entry(ETH), E.entry(SOL), E.entry(XRP))
    world.fake.mutations.clear()
    result = world.run(
        C.AddIntent(symbol=SOL, direction=C.LONG, quantity=0.5, reference_price=100.0, stop_price=95.0),
        C.RebalanceIntent(symbol=XRP, direction=C.LONG, current_quantity=200.0, target_quantity=220.0, reference_price=2.0,
                          stop_price=1.9, band=0.05),
        E.exit_(BTC),
        C.ReduceIntent(symbol=ETH, direction=C.LONG, reason="DE_RISK", fraction=0.5))
    assert states(result) == {SOL: X.FILLED, XRP: X.FILLED, BTC: X.FILLED, ETH: X.FILLED}
    assert (world.signed(BTC), world.signed(ETH), world.signed(SOL), world.signed(XRP)) == (0.0, 0.08, 4.5, 220.0)
    sent = world.orders_sent(protective=False)
    assert [(r.symbol, r.side, r.quantity, r.reduce_only) for r in sent] == [
        (BTC, c.SELL, 0.008, True), (ETH, c.SELL, 0.08, True),     # both reductions before either increase
        (SOL, c.BUY, 0.5, False), (XRP, c.BUY, 20.0, False)]
    assert world.stops(BTC) == []                                   # the closed position's stop was retired
    assert [e.method for e in world.sent("cancel_order", symbol=BTC)] == ["cancel_order"]
    for symbol, stop in ((ETH, 2_375.0), (SOL, 95.0), (XRP, 1.9)):  # bound to the position: read back after the fill
        assert world.stop_prices(symbol) == [stop] and covered(world, symbol)
        assert result.outcome(symbol).protection == PR.PROTECTED
    assert result.outcome(BTC).protection == PR.FLAT
    assert world.orders_sent(protective=True) == []                 # no stop had to be created or moved
    assert world.reservations() == {BTC: R.FILLED, ETH: R.FILLED, SOL: R.FILLED, XRP: R.FILLED}
    # and back down, by a rebalance in the other direction
    down = world.run(C.RebalanceIntent(symbol=XRP, direction=C.LONG, current_quantity=220.0, target_quantity=110.0,
                                       reference_price=2.0, stop_price=1.9, band=0.05))
    assert states(down) == {XRP: X.FILLED} and world.signed(XRP) == 110.0 and world.stop_prices(XRP) == [1.9]
    assert not down.outcome(XRP).increases_risk and world.orders_sent(XRP, protective=False)[-1].reduce_only


def test_a_close_never_opens_the_opposite_position(world, tmp_path_factory, tmp_path):
    open_three(world)
    world.fake.mutations.clear()
    # the position is gone when the close is built: nothing is sent, and its stale stop is retired
    world.adapter.hook("balance", lambda: world.fake.external_trade(BTC, c.SELL, 0.008))    # closes during the snapshot
    gone = world.run(E.exit_(BTC))
    assert states(gone) == {BTC: X.REFUSED} or states(gone) == {BTC: X.DONE}
    assert world.orders_sent(BTC, protective=False) == [] and world.signed(BTC) == 0.0
    # the position shrinks between the read and the order: reduce-only makes the venue refuse the excess
    world.adapter.hook("place_order", lambda: world.fake.external_trade(ETH, c.SELL, 0.10))
    shrunk = world.run(E.exit_(ETH))
    assert states(shrunk) == {ETH: X.REJECTED} and f"{X.VENUE_REFUSED}:{c.REDUCE_ONLY_REJECTED}" in why(shrunk, ETH)
    assert world.signed(ETH) == pytest.approx(0.06)                 # still long: nothing flipped
    again = world.run(E.exit_(ETH))                                 # the next decision closes what is really there
    assert states(again) == {ETH: X.FILLED} and world.signed(ETH) == 0.0
    # a position on the other side than the intent says is not "closed" by a buy
    wrong = world.run(E.exit_(SOL, direction=C.SHORT))
    assert states(wrong) == {SOL: X.REFUSED} and "DIRECTION_MISMATCH" in why(wrong, SOL) and world.signed(SOL) == 4.0
    flat = world.run(E.exit_(XRP))                                  # nothing is held at all
    assert states(flat) == {XRP: X.REFUSED} and "NO_POSITION" in why(flat, XRP)
    closes = [r for r in world.orders_sent(protective=False)]
    assert closes and all(r.reduce_only for r in closes)            # every close this boundary sent was reduce-only

    class Hedged(E.Controlled):
        def positions(self, symbol=None):
            self.calls.append("positions")
            return tuple(dataclasses.replace(p, position_mode="HEDGE") for p in self.inner.positions(symbol))

    hedged = world.boundary(adapter=Hedged(world.fake, world.db.path))
    refused = hedged.execute(world.decide(E.exit_(SOL)), brake_state=E.NO_BRAKE)
    assert states(refused) == {SOL: X.REFUSED} and X.POSITION_MODE_NOT_ONE_WAY in why(refused, SOL) and world.signed(SOL) == 4.0


def test_a_limit_order_rests_fills_in_parts_and_its_cancel_is_reconciled(world):
    limit = E.entry(BTC, order_style=C.LIMIT, limit_price=49_900.0)
    batch = world.decide(limit, E.entry(ETH))
    boundary = world.boundary()
    result = boundary.execute(batch, brake_state=E.NO_BRAKE)
    resting = result.outcome(BTC)
    # nothing filled: no position, no stop yet, but the stop it will need is already on record (PENDING)
    assert (resting.state, resting.filled_quantity, resting.reservation_state, resting.protection) == (X.OPEN, 0.0, R.SUBMITTED, PR.PENDING)
    (order_id,) = resting.client_order_ids
    (request,) = entry_orders(world, BTC)
    assert (request.order_type, request.price, request.quantity) == (c.LIMIT, 49_900.0, 0.008)
    assert world.stops(BTC) == [] and world.signed(BTC) == 0.0 and states(result)[ETH] == X.FILLED
    # a first partial fill: protection is placed for what filled, the reservation keeps holding the whole order
    world.fake.fill(order_id, 0.003)
    report = world.boundary().recover()
    assert report.status == X.BATCH_PROCESSED and report.unresolved == ()
    (row,) = world.rows(schema.INTENT_TABLE, "symbol=?", (BTC,))
    assert (row["state"], row["filled_quantity"]) == (X.OPEN, 0.003)
    assert world.stop_prices(BTC) == [47_500.0] and covered(world, BTC) and world.reservations()[BTC] == R.SUBMITTED
    # the cancel: a further fill arrives before the cancel does. What the venue reports about the order decides.
    world.adapter.hook("cancel_order", lambda: world.fake.fill(order_id, 0.002))
    cancelled = world.boundary().cancel(resting.intent_id, reason="TEST_CANCEL")
    assert (cancelled.state, cancelled.filled_quantity) == (X.PARTIAL, 0.005)
    assert world.signed(BTC) == pytest.approx(0.005) and covered(world, BTC) and world.stop_prices(BTC) == [47_500.0]
    (reservation,) = world.rows("cati_reservation_ledger", "symbol=?", (BTC,))
    # consumed for what filled; what was cancelled no longer holds anything
    assert (reservation["state"], reservation["filled_quantity"]) == (R.FILLED, 0.005)
    assert "PARTIAL filled 0.005" in reservation["resolution_evidence"]
    assert len(entry_orders(world, BTC)) == 1 and world.boundary().cancel(resting.intent_id).state == X.PARTIAL
    assert world.boundary().cancel("xint_of_nobody") is None


def test_a_cancel_that_is_not_acknowledged_is_never_taken_for_done(world):
    result = world.run(E.entry(BTC, order_style=C.LIMIT, limit_price=49_900.0))
    intent_id = result.outcome(BTC).intent_id
    world.fake.fail_next("cancel_order", c.VENUE_UNAVAILABLE)       # the cancel did not reach the venue
    pending = world.boundary().cancel(intent_id)
    assert pending.state == X.CANCELING and world.reservations()[BTC] == R.SUBMITTED     # nothing is released on a request
    assert len(world.fake.open_orders(BTC)) == 1
    world.fake.fail_next("cancel_order", c.NETWORK_UNCERTAIN, after_effect=True)        # cancelled; the answer is lost
    world.boundary().recover()
    (row,) = world.rows(schema.INTENT_TABLE)
    assert (row["state"], row["filled_quantity"]) == (X.CANCELED, 0.0)
    (reservation,) = world.rows("cati_reservation_ledger")
    assert reservation["state"] == R.RELEASED and "CANCELED filled 0.0" in reservation["resolution_evidence"]
    assert world.fake.open_orders(BTC) == () and world.signed(BTC) == 0.0


def test_a_limit_falls_back_to_a_market_order_only_inside_its_slippage_bound(world):
    patient = E.entry(ETH, order_style=C.LIMIT_THEN_MARKET, limit_price=2_490.0, max_slippage_bps=50.0)
    runaway = E.entry(SOL, order_style=C.LIMIT_THEN_MARKET, limit_price=99.5, max_slippage_bps=20.0)
    batch = world.decide(patient, runaway)
    first = world.boundary().execute(batch, brake_state=E.NO_BRAKE)
    assert states(first) == {ETH: X.OPEN, SOL: X.OPEN} and world.orders_sent(protective=True) == []
    world.fake.fill(first.outcome(ETH).client_order_ids[0], 0.05)   # part of the ETH limit fills while it rests
    early = world.boundary().execute(batch, brake_state=E.NO_BRAKE)
    assert states(early) == {ETH: X.OPEN, SOL: X.OPEN} and world.sent("cancel_order") == []     # not due yet
    assert world.stop_prices(ETH) == [2_375.0]                      # but the fill is protected
    world.clock.advance(61_000)
    world.fake.set_price(SOL, 100.5)                                # SOL ran away from its limit: 100 bps > 20 bps
    done = world.boundary().execute(batch, brake_state=E.NO_BRAKE)
    eth, sol = done.outcome(ETH), done.outcome(SOL)
    assert (eth.state, eth.filled_quantity, eth.reservation_state) == (X.FILLED, pytest.approx(0.16), R.FILLED)
    assert eth.client_order_ids == tuple(X.order_client_id(E.ACCOUNT, batch.provenance.decision_id, ETH, "ENTRY", n) for n in (0, 1))
    market = entry_orders(world, ETH)[-1]
    assert (market.order_type, market.quantity) == (c.MARKET, pytest.approx(0.11))    # only what the limit left
    methods = [(e.method, getattr(e.arguments.get("request"), "order_type", None)) for e in world.sent(symbol=ETH)] + \
        [(e.method, None) for e in world.sent("cancel_order", symbol=ETH)]
    assert methods.index(("cancel_order", None)) >= 0 and world.signed(ETH) == pytest.approx(0.16) and covered(world, ETH)
    assert (sol.state, sol.filled_quantity, sol.reservation_state) == (X.REFUSED, 0.0, R.RELEASED)
    assert X.SLIPPAGE_BOUND_EXCEEDED in why(done, SOL) and X.LIMIT_NOT_FILLED_IN_TIME in why(done, SOL)
    assert len(entry_orders(world, SOL)) == 1 and world.signed(SOL) == 0.0 and world.fake.open_orders(SOL) == ()
    # a market entry carries the same bound
    bounded = world.run(E.entry(XRP, max_slippage_bps=10.0))
    assert states(bounded) == {XRP: X.FILLED}
    world.fake.set_price(BTC, 50_200.0)                             # 40 bps above the decision's reference
    refused = world.run(E.entry(BTC, max_slippage_bps=10.0))
    assert states(refused) == {BTC: X.REFUSED} and X.SLIPPAGE_BOUND_EXCEEDED in why(refused, BTC)
    assert entry_orders(world, BTC) == [] and refused.outcome(BTC).reservation_state == R.RELEASED


def test_adverse_quote_exceeds_reserved_risk_even_without_an_optional_slippage_bound(world):
    world.fake.set_price(BTC, 50_200)
    result = world.run(E.entry(BTC))
    assert states(result) == {BTC: X.REFUSED}
    assert "EXECUTABLE_PRICE_RISK_CHANGED" in why(result, BTC)
    assert entry_orders(world, BTC) == [] and result.outcome(BTC).reservation_state == R.RELEASED


def test_favorable_quote_stays_inside_reserved_stop_risk(world):
    world.fake.set_price(BTC, 49_800)
    result = world.run(E.entry(BTC))
    assert states(result) == {BTC: X.FILLED} and covered(world, BTC)


def test_limit_price_cannot_offer_more_stop_risk_than_the_reservation(world):
    result = world.run(E.entry(BTC, order_style=C.LIMIT_THEN_MARKET, limit_price=51_000, max_slippage_bps=50))
    assert states(result) == {BTC: X.REFUSED}
    assert "EXECUTABLE_PRICE_RISK_CHANGED" in why(result, BTC)
    assert entry_orders(world, BTC) == []


def test_fallback_accounts_for_stop_risk_consumed_by_the_partial_limit_fill(world):
    batch = world.decide(E.entry(ETH, order_style=C.LIMIT_THEN_MARKET,
                                 limit_price=2_490, max_slippage_bps=100))
    first = world.boundary().execute(batch, brake_state=E.NO_BRAKE)
    world.fake.fill(first.outcome(ETH).client_order_ids[0], 0.05)
    world.clock.advance(61_000)
    world.fake.set_price(ETH, 2_510)  # within slippage; combined stop risk would exceed the reservation
    done = world.boundary().execute(batch, brake_state=E.NO_BRAKE)
    assert "EXECUTABLE_PRICE_RISK_CHANGED" in why(done, ETH)
    assert len(entry_orders(world, ETH)) == 1
    assert world.signed(ETH) == pytest.approx(0.05) and covered(world, ETH)
    assert done.outcome(ETH).reservation_state == R.FILLED


def test_where_the_stop_cannot_grow_a_partly_filled_order_is_cut_before_it_is_protected(tmp_path_factory, tmp_path):
    """BingX-like: a stop carries a quantity, a second stop is refused and none can be amended. A limit order
    that fills in parts would outgrow its stop, so its remainder is cancelled first and the stop is placed once."""
    world = E.make_world(tmp_path_factory, tmp_path, profile=c.BINGX)
    result = world.run(E.entry(BTC, order_style=C.LIMIT, limit_price=49_900.0))
    order_id = result.outcome(BTC).client_order_ids[0]
    world.fake.fill(order_id, 0.003)
    world.fake.mutations.clear()
    world.boundary().recover()
    assert [e.method for e in world.fake.mutations] == ["cancel_order", "place_order"]      # cut, then protected
    (stop,) = world.stops(BTC)
    assert (stop.quantity, stop.stop_price, world.signed(BTC)) == (0.003, 47_500.0, 0.003) and covered(world, BTC)
    (row,) = world.rows(schema.INTENT_TABLE)
    assert (row["state"], row["filled_quantity"], row["reason"]) == (X.PARTIAL, 0.003, X.PARTIAL_FILL_CUT)
    assert world.reservations() == {BTC: R.FILLED}
    # and an increase of a position whose stop could not follow is refused before anything is sent
    grow = world.run(C.AddIntent(symbol=BTC, direction=C.LONG, quantity=0.002, reference_price=50_000.0, stop_price=47_500.0))
    assert states(grow) == {BTC: X.REFUSED} and X.PROTECTION_CANNOT_FOLLOW_INCREASE in why(grow, BTC)
    assert PR.REPLACEMENT_NOT_POSSIBLE_SAFELY in why(grow, BTC) and world.signed(BTC) == 0.003


@pytest.mark.parametrize("profile", c.VENUES)
def test_entries_and_exits_work_on_every_profile(tmp_path_factory, tmp_path, profile):
    world = E.make_world(tmp_path_factory, tmp_path, profile=profile)
    opened = world.run(E.entry(BTC), E.entry(ETH, direction=C.SHORT))
    assert states(opened) == {BTC: X.FILLED, ETH: X.FILLED} and covered(world, BTC) and covered(world, ETH)
    assert world.stop_prices(BTC) == [47_500.0] and world.stop_prices(ETH) == [2_625.0]
    assert all(world.rows(schema.PROTECTION_TABLE, "symbol=?", (s,))[0]["venue_order_id"] for s in (BTC, ETH))
    closed = world.run(E.exit_(BTC), C.ReduceIntent(symbol=ETH, direction=C.SHORT, reason="DE_RISK", quantity=0.06))
    assert states(closed) == {BTC: X.FILLED, ETH: X.FILLED}
    assert world.signed(BTC) == 0.0 and world.signed(ETH) == pytest.approx(-0.10)
    assert world.stops(BTC) == [] and covered(world, ETH)           # the stop still covers what is left, read back
    assert closed.outcome(ETH).protection == PR.PROTECTED and world.adapter.open_transactions == []


# ================================================================================================ protection
def test_a_position_that_cannot_be_protected_is_closed_and_the_account_is_blocked(world):
    world.adapter.hook_after("place_order", lambda: world.fake.fail_next("place_order", c.STOP_WOULD_TRIGGER_IMMEDIATELY))
    result = world.run(E.entry(BTC), E.entry(ETH))
    btc = result.outcome(BTC)
    assert btc.state == X.FILLED and btc.protection == PR.FLAT and world.signed(BTC) == 0.0   # opened, then closed
    close = world.orders_sent(BTC, protective=False)[-1]
    assert (close.side, close.quantity, close.reduce_only, close.order_type) == (c.SELL, 0.008, True, c.MARKET)
    assert f"{X.FAILSAFE_CLOSE}:{X.FILLED}" in btc.reasons and any("STOP_REFUSED" in r for r in btc.reasons)
    # the account takes no new entry: not the next one of this batch, and not one of the next decision
    assert states(result)[ETH] == X.REFUSED and f"{X.ENTRIES_BLOCKED}:{X.BLOCK_PROTECTION_FAILED}" in why(result, ETH)
    assert world.sent(symbol=ETH) == [] and world.sent("set_leverage", symbol=ETH) == []
    boundary = world.boundary()                                     # a new process: the block is durable
    (block,) = boundary.entry_blocks()
    assert (block["reason"], block["scope"], block["operator_only"], block["symbol"]) == (X.BLOCK_PROTECTION_FAILED, "ACCOUNT", 1, BTC)
    later = world.run(E.entry(SOL), boundary=boundary)
    assert states(later) == {SOL: X.REFUSED} and world.signed(SOL) == 0.0
    # nothing lifts it but an operator who says who they are and why
    assert boundary.recover().status == X.BATCH_PROCESSED and len(boundary.entry_blocks()) == 1
    assert not boundary.clear_entry_block(block["block_id"], operator_ref="admin-7", evidence=" ")
    assert boundary.clear_entry_block(block["block_id"], operator_ref="admin-7", evidence="position closed; stop geometry reviewed")
    assert states(world.run(E.entry(SOL))) == {SOL: X.FILLED} and covered(world, SOL)
    records = {r["reason"]: r for r in world.rows(schema.BLOCK_TABLE)}
    assert (records[X.BLOCK_PROTECTION_FAILED]["state"], records[X.BLOCK_PROTECTION_FAILED]["cleared_by"]) == ("CLEARED", "operator:admin-7")
    # the protection layer's own block was lifted by the read that found the position flat, not by a person
    assert records[PR.BLOCK_PROTECTION_ABSENT]["cleared_by"] == "reconciliation" and len(records) == 2


def test_a_stop_that_would_already_be_through_the_market_is_never_moved_to_fit(world):
    world.adapter.hook_after("place_order", lambda: world.fake.set_price(BTC, 47_000.0))    # the market fell through the stop
    result = world.run(E.entry(BTC))
    assert world.signed(BTC) == 0.0 and world.stops(BTC) == []      # closed reduce-only; no stop was placed elsewhere
    assert any(PR.STOP_NOT_PROTECTIVE_AT_MARK in r for r in result.outcome(BTC).reasons)
    assert world.orders_sent(BTC, protective=True) == [] and world.boundary().entry_blocks() != ()


def test_while_protection_is_uncertain_entries_wait_and_exits_do_not(world):
    open_three(world)
    world.adapter.hook_after("place_order", lambda: world.fake.fail_next("open_orders", c.NETWORK_UNCERTAIN))
    result = world.run(E.entry(XRP))                                # filled; its stop could not be read back
    assert result.outcome(XRP).protection in (PR.UNCERTAIN, PR.PENDING) and world.signed(XRP) == 200.0
    blocked = world.run(E.exit_(BTC), E.entry(XRP, risk=0.0001))    # XRP is held now, so use a symbol-free check below
    assert states(blocked)[BTC] == X.FILLED and world.signed(BTC) == 0.0             # the exit is not held back
    refused = world.run(C.AddIntent(symbol=ETH, direction=C.LONG, quantity=0.01, reference_price=2_500.0, stop_price=2_375.0))
    assert states(refused) == {ETH: X.REFUSED} and f"{X.ENTRIES_BLOCKED}:{PR.BLOCK_PROTECTION_UNSETTLED}" in why(refused, ETH)
    report = world.boundary().recover()                             # a later read settles it
    assert [p.status for p in report.protections if p.symbol == XRP] == [PR.PROTECTED]
    assert world.boundary().entry_blocks() == () and covered(world, XRP) and world.stop_prices(XRP) == [1.9]
    allowed = world.run(C.AddIntent(symbol=ETH, direction=C.LONG, quantity=0.01, reference_price=2_500.0, stop_price=2_375.0))
    assert states(allowed) == {ETH: X.FILLED}


# ====================================================================================== errors stay local
def test_an_error_on_one_symbol_produces_no_mutation_on_another(world):
    world.fake.fail_next("place_order", c.INSUFFICIENT_MARGIN, symbol=ETH)
    world.fake.fail_next("set_leverage", c.LEVERAGE_NOT_ALLOWED, symbol=XRP)
    result = world.run(E.entry(BTC), E.entry(ETH), E.entry(XRP), E.entry(SOL))
    assert states(result) == {BTC: X.FILLED, ETH: X.REJECTED, XRP: X.REFUSED, SOL: X.FILLED}
    assert f"{X.VENUE_REFUSED}:{c.INSUFFICIENT_MARGIN}" in why(result, ETH) and X.LEVERAGE_NOT_SET in why(result, XRP)
    assert [e.method for e in world.sent("place_order", symbol=ETH)] == ["place_order"] and world.signed(ETH) == 0.0
    assert world.sent("place_order", symbol=XRP) == [] and world.signed(XRP) == 0.0
    assert (world.signed(BTC), world.signed(SOL)) == (0.008, 4.0) and covered(world, BTC) and covered(world, SOL)
    reservations = {r["symbol"]: r for r in world.rows("cati_reservation_ledger")}
    assert {s: r["state"] for s, r in reservations.items()} == {BTC: R.FILLED, ETH: R.RELEASED, XRP: R.RELEASED, SOL: R.FILLED}
    assert "INSUFFICIENT_MARGIN" in reservations[ETH]["resolution_evidence"]          # released WITH the venue's refusal
    assert world.boundary().entry_blocks() == ()                    # a refused order is not an uncertainty


def test_an_unexpected_failure_inside_one_intent_does_not_reach_the_next(world, monkeypatch):
    original = X.PortfolioExecutionBoundary._request

    def broken(self, row):
        if row.symbol == ETH:
            raise RuntimeError("a defect in this intent's path")
        return original(self, row)

    monkeypatch.setattr(X.PortfolioExecutionBoundary, "_request", broken)
    result = world.run(E.entry(BTC), E.entry(ETH), E.entry(SOL))
    assert states(result)[BTC] == X.FILLED and states(result)[SOL] == X.FILLED
    assert "INTERNAL_ERROR:RuntimeError" in why(result, ETH) and world.sent("place_order", symbol=ETH) == []
    assert world.signed(ETH) == 0.0 and world.reservations()[ETH] == R.RESERVED      # never marked submitted, never sent


# ===================================================================================== unknown submissions
def test_an_order_whose_answer_was_lost_is_looked_up_and_not_sent_again(world):
    world.fake.fail_next("place_order", c.NETWORK_UNCERTAIN, after_effect=True, symbol=BTC)
    result = world.run(E.entry(BTC), E.entry(ETH))
    assert states(result) == {BTC: X.FILLED, ETH: X.FILLED}         # the lookup by client order id found it filled
    assert len(entry_orders(world, BTC)) == 1 and world.signed(BTC) == 0.008 and covered(world, BTC)
    assert world.reservations() == {BTC: R.FILLED, ETH: R.FILLED}


def test_an_unknown_outcome_keeps_its_reservation_blocks_entries_and_is_resolved_from_the_venue(world):
    world.fake.fail_next("place_order", c.NETWORK_UNCERTAIN, after_effect=True, symbol=BTC)     # it filled...
    world.fake.fail_next("order", c.NETWORK_UNCERTAIN, symbol=BTC)                             # ...and the lookup fails too
    batch = world.decide(E.entry(BTC), E.entry(ETH))
    result = world.boundary().execute(batch, brake_state=E.NO_BRAKE)
    btc = result.outcome(BTC)
    assert (btc.state, btc.reservation_state) == (X.UNKNOWN, R.UNKNOWN) and X.SUBMIT_OUTCOME_UNKNOWN in why(result, BTC)
    assert states(result)[ETH] == X.REFUSED and f"{X.UNRESOLVED_SUBMISSION}:{BTC}" in why(result, ETH)
    assert world.sent(symbol=ETH) == [] and world.signed(BTC) == 0.008 and not covered(world, BTC)
    world.fake.fail_next("order", c.VENUE_UNAVAILABLE, symbol=BTC)
    still = world.boundary().execute(batch, brake_state=E.NO_BRAKE)                 # asked again: looked up, not re-sent
    assert still.outcome(BTC).state == X.UNKNOWN and len(entry_orders(world, BTC)) == 1
    report = world.boundary().recover()                             # a new process resolves it from venue truth
    assert report.unresolved == () and world.reservations()[BTC] == R.FILLED
    (row,) = world.rows(schema.INTENT_TABLE, "symbol=?", (BTC,))
    assert (row["state"], row["filled_quantity"]) == (X.FILLED, 0.008) and "FILLED filled 0.008" in row["evidence"]
    assert covered(world, BTC) and world.stop_prices(BTC) == [47_500.0] and len(entry_orders(world, BTC)) == 1
    (reservation,) = world.rows("cati_reservation_ledger", "symbol=?", (BTC,))
    assert reservation["unknown_since"] is not None and reservation["filled_quantity"] == 0.008


def test_an_unacknowledged_order_is_not_treated_as_absent_until_it_can_no_longer_arrive(world):
    world.fake.fail_next("place_order", c.NETWORK_UNCERTAIN, symbol=BTC)            # the request never reached the venue
    batch = world.decide(E.entry(BTC))
    result = world.boundary().execute(batch, brake_state=E.NO_BRAKE)
    assert (result.outcome(BTC).state, result.outcome(BTC).reservation_state) == (X.UNKNOWN, R.UNKNOWN)
    assert X.ORDER_NOT_FOUND_YET in why(result, BTC)
    for _ in range(2):                                              # the venue says "no such order": still not absent
        report = world.boundary().recover()
        assert len(report.unresolved) == 1 and world.reservations()[BTC] == R.UNKNOWN
        world.clock.advance(30_000)
    assert states(world.run(E.entry(ETH))) == {ETH: X.REFUSED}      # and no other entry while it may still appear
    world.clock.advance(70_000)                                     # past the window in which it could arrive
    report = world.boundary().recover()
    assert report.unresolved == ()
    (row,) = world.rows(schema.INTENT_TABLE, "symbol=?", (BTC,))
    assert (row["state"], row["reason"]) == (X.ABSENT, X.ORDER_ABSENT) and "ORDER_NOT_FOUND" in row["evidence"]
    (reservation,) = world.rows("cati_reservation_ledger", "symbol=?", (BTC,))
    assert reservation["state"] == R.RELEASED and "ABSENT" in reservation["resolution_evidence"]
    again = world.boundary().execute(batch, brake_state=E.NO_BRAKE)                 # the decision is still valid...
    assert again.outcome(BTC).state == X.ABSENT and len(entry_orders(world, BTC)) == 1      # ...and it is never sent again
    assert world.signed(BTC) == 0.0 and states(world.run(E.entry(ETH))) == {ETH: X.FILLED}


def test_a_lookup_that_returns_another_order_is_never_adopted(world):
    class WrongAnswer(E.Controlled):
        def order(self, symbol, **kw):
            self.calls.append("order")
            return dataclasses.replace(self.inner.order(symbol, **kw), side=c.SELL)

    world.fake.fail_next("place_order", c.NETWORK_UNCERTAIN, after_effect=True, symbol=BTC)
    boundary = world.boundary(adapter=WrongAnswer(world.fake, world.db.path))
    result = boundary.execute(world.decide(E.entry(BTC)), brake_state=E.NO_BRAKE)
    assert result.outcome(BTC).state == X.UNKNOWN and "VENUE_RETURNED_ANOTHER_ORDER" in why(result, BTC)
    assert world.reservations()[BTC] == R.UNKNOWN


# ================================================================================================= recovery
def _clean_run(tmp_path_factory, folder):
    world = E.make_world(tmp_path_factory, folder)
    world.run(E.entry(SOL))
    batch = world.decide(E.exit_(SOL), E.entry(BTC), E.entry(ETH))
    world.adapter.calls.clear()
    return world, batch


def test_a_new_process_resolves_everything_from_durable_state_wherever_the_old_one_died(tmp_path_factory, tmp_path):
    (tmp_path / "probe").mkdir()
    probe, batch = _clean_run(tmp_path_factory, tmp_path / "probe")
    probe.boundary().execute(batch, brake_state=E.NO_BRAKE)
    total = len(probe.adapter.calls)
    assert total > 25
    for index in range(total):
        for after_effect in (False, True):
            folder = tmp_path / f"crash_{index}_{int(after_effect)}"
            folder.mkdir()
            world, batch = _clean_run(tmp_path_factory, folder)
            world.adapter.die_at(index, after_effect=after_effect)
            with pytest.raises(E.SimulatedCrash):
                world.boundary().execute(batch, brake_state=E.NO_BRAKE)
            where = (index, after_effect, world.adapter.calls[-1])
            world.adapter.crash_at = None
            world.boundary().recover()                              # the new process, before anything else
            world.clock.advance(121_000)                            # past the window of an order that never arrived
            report = world.boundary().recover()
            assert report.status == X.BATCH_PROCESSED and report.unresolved == (), where
            final = world.boundary().execute(batch, brake_state=E.NO_BRAKE)         # and the decision is continued
            assert final.status == X.BATCH_PROCESSED, where
            # exactly once: no symbol ever received a second opening order, and no position is larger than authorized
            for symbol, quantity in ((BTC, 0.008), (ETH, 0.16)):
                sent = entry_orders(world, symbol)
                assert len({r.client_order_id for r in sent}) <= 1, where
                assert len([e for e in world.sent(symbol=symbol) if e.effect and not e.arguments["request"].protective
                            and not e.arguments["request"].reduce_only]) <= 1, where
                assert world.signed(symbol) in (0.0, quantity), where
                outcome = final.outcome(symbol)
                assert outcome.final and (outcome.state == X.FILLED) == (world.signed(symbol) == quantity), (where, outcome)
                assert covered(world, symbol) and len(world.stops(symbol)) == (1 if world.signed(symbol) else 0), where
            # the exit closed the position once, reduce-only, and left no stop of it behind
            assert world.signed(SOL) in (0.0, 4.0), where
            if world.signed(SOL) == 0.0:
                assert world.stops(SOL) == [], where
            else:
                assert final.outcome(SOL).state == X.ABSENT and covered(world, SOL), where   # never arrived; never re-sent
            assert all(r.reduce_only for r in world.orders_sent(SOL, protective=False)[1:]), where
            # nothing is left in doubt: no order row may still have left, no reservation is still unsettled
            assert world.rows(schema.INTENT_TABLE, "state IN ('SUBMITTING', 'UNKNOWN', 'CANCELING', 'OPEN')") == [], where
            assert set(world.reservations().values()) <= {R.FILLED, R.RELEASED}, (where, world.reservations())
            assert world.boundary().ledger.unresolved(E.ACCOUNT) == [], where
            assert world.adapter.open_transactions == [], where


def test_a_death_between_an_orders_end_and_its_reservation_is_settled_by_recovery(world, monkeypatch):
    def dies(self, run, row):
        raise E.SimulatedCrash("before the reservation was settled")

    with monkeypatch.context() as patched:
        patched.setattr(X.PortfolioExecutionBoundary, "_finalize", dies)
        with pytest.raises(E.SimulatedCrash):
            world.run(E.entry(BTC))
    assert world.reservations() == {BTC: R.SUBMITTED} and world.signed(BTC) == 0.008
    (row,) = world.rows(schema.INTENT_TABLE)
    assert row["state"] == X.FILLED                                 # the order row had already ended
    world.boundary().recover()
    assert world.reservations() == {BTC: R.FILLED} and covered(world, BTC)


def test_a_death_right_after_the_order_left_still_gets_the_position_protected(world):
    """The intended protection is written before the order leaves, so a process that dies with a filled order
    and no stop leaves a record the next process acts on."""
    world.run(E.entry(ETH))
    batch = world.decide(E.entry(BTC))
    calls = len(world.adapter.calls)
    world.adapter.hook_after("place_order", lambda: world.adapter.die_at(0, after_effect=False))
    with pytest.raises(E.SimulatedCrash):
        world.boundary().execute(batch, brake_state=E.NO_BRAKE)
    assert len(world.adapter.calls) > calls and world.signed(BTC) == 0.008 and not covered(world, BTC)
    (record,) = world.rows(schema.PROTECTION_TABLE, "symbol=?", (BTC,))
    assert (record["status"], record["intended_stop_price"]) == (PR.PENDING, 47_500.0)
    world.adapter.crash_at = None
    report = world.boundary().recover()
    assert covered(world, BTC) and world.stop_prices(BTC) == [47_500.0] and report.unresolved == ()
    assert world.boundary().execute(batch, brake_state=E.NO_BRAKE).outcome(BTC).state == X.FILLED
    assert len(entry_orders(world, BTC)) == 1


def test_recovery_ends_what_expired_and_cancels_orders_whose_decision_ran_out(world):
    batch = world.decide(E.entry(BTC, order_style=C.LIMIT, limit_price=49_900.0), E.entry(ETH), ttl=60_000)
    world.fake.fail_next("place_order", c.VENUE_UNAVAILABLE, symbol=ETH)             # definitively not created; retryable
    result = world.boundary().execute(batch, brake_state=E.NO_BRAKE)
    assert states(result) == {BTC: X.OPEN, ETH: X.DEFERRED} and world.reservations() == {BTC: R.SUBMITTED, ETH: R.SUBMITTED}
    world.clock.advance(61_000)
    assert world.boundary().execute(batch, brake_state=E.NO_BRAKE).reasons == (X.DECISION_STALE,)
    world.boundary().recover()
    rows = {r["symbol"]: r for r in world.rows(schema.INTENT_TABLE)}
    assert (rows[BTC]["state"], rows[BTC]["reason"]) == (X.CANCELED, X.DECISION_EXPIRED)
    assert (rows[ETH]["state"], rows[ETH]["reason"]) == (X.EXPIRED, X.DECISION_EXPIRED)
    assert world.fake.open_orders() == () and world.reservations() == {BTC: R.RELEASED, ETH: R.RELEASED}
    assert len(entry_orders(world, ETH)) == 1                       # the deferred order was not sent after its decision ended


def test_a_newer_decision_replaces_the_outstanding_order_of_an_older_one(world):
    old = world.run(E.entry(BTC, order_style=C.LIMIT, limit_price=49_900.0))
    assert states(old) == {BTC: X.OPEN}
    world.fake.mutations.clear()
    new = world.run(E.entry(BTC, order_style=C.LIMIT, limit_price=49_950.0))
    assert states(new) == {BTC: X.OPEN}
    assert [e.method for e in world.fake.mutations if e.method != "set_leverage"] == ["cancel_order", "place_order"]
    (resting,) = world.fake.open_orders(BTC)
    assert resting.client_order_id == new.outcome(BTC).client_order_ids[0] != old.outcome(BTC).client_order_ids[0]
    rows = sorted(world.rows(schema.INTENT_TABLE), key=lambda r: r["created_at"])
    assert [(r["state"], r["reason"]) for r in rows] == [(X.CANCELED, X.SUPERSEDED), (X.OPEN, "")]
    ledger = sorted(world.rows("cati_reservation_ledger"), key=lambda r: r["created_at"])
    assert [r["state"] for r in ledger] == [R.RELEASED, R.SUBMITTED]                # one claim on the symbol at a time
    # an exit supersedes it too: a resting entry must not fill after the position was closed
    world.fake.fill(new.outcome(BTC).client_order_ids[0], 0.004)
    closing = world.run(E.exit_(BTC))
    assert states(closing) == {BTC: X.FILLED} and world.signed(BTC) == 0.0 and world.fake.open_orders(BTC) == ()


# =============================================================================================== rate limits
def test_a_rate_limit_stops_every_further_order_and_nothing_is_retried_before_the_time_named(world):
    world.fake.fail_next("place_order", AdapterError(c.RATE_LIMITED, "too many orders", retry_after_s=30), symbol=ETH)
    batch = world.decide(E.entry(BTC), E.entry(ETH), E.entry(SOL))
    result = world.boundary().execute(batch, brake_state=E.NO_BRAKE)
    assert result.status == X.BATCH_HALTED and result.reasons == (PR.RATE_LIMIT_BACKOFF,)
    assert states(result) == {BTC: X.FILLED, ETH: X.DEFERRED, SOL: X.PLANNED}
    assert result.retry_at_ms == pytest.approx(world.clock.t + 30_000, abs=100)     # the retry time is recorded
    assert world.sent(symbol=SOL) == [] and world.sent("set_leverage", symbol=SOL) == []     # the next entry was not attempted
    assert covered(world, BTC) and world.reservations() == {BTC: R.FILLED, ETH: R.SUBMITTED, SOL: R.RESERVED}
    calls = len(world.adapter.calls)
    for _ in range(2):                                              # asked again too early: not one venue call
        early = world.boundary().execute(batch, brake_state=E.NO_BRAKE)
        assert (early.status, early.reasons, early.retry_at_ms) == (X.BATCH_DEFERRED, (PR.RATE_LIMIT_BACKOFF,), result.retry_at_ms)
        assert world.boundary().recover().status == X.BATCH_DEFERRED
        world.clock.advance(10_000)
    assert len(world.adapter.calls) == calls
    world.clock.advance(11_000)
    done = world.boundary().execute(batch, brake_state=E.NO_BRAKE)
    assert states(done) == {BTC: X.FILLED, ETH: X.FILLED, SOL: X.FILLED}
    eth = entry_orders(world, ETH)
    assert len(eth) == 2 and len({r.client_order_id for r in eth}) == 1              # the SAME order, after the wait
    assert len(world.fake.fills(ETH)) == 1 and world.signed(ETH) == 0.16 and covered(world, ETH)


def test_a_venue_wide_rate_limit_sends_nothing_and_reductions_go_first_when_it_ends(world):
    open_three(world)
    world.fake.mutations.clear()
    world.fake.rate_limit_for(20)
    batch = world.decide(E.entry(XRP), E.exit_(BTC))
    halted = world.boundary().execute(batch, brake_state=E.NO_BRAKE)
    assert halted.status == X.BATCH_HALTED and states(halted) == {XRP: X.PLANNED, BTC: X.PLANNED}
    assert f"{X.SNAPSHOT_UNAVAILABLE}:{c.RATE_LIMITED}" in why(halted, BTC) and world.fake.mutations == []
    world.clock.advance(21_000)
    done = world.boundary().execute(batch, brake_state=E.NO_BRAKE)
    assert states(done) == {XRP: X.FILLED, BTC: X.FILLED}
    first = world.orders_sent(protective=False)[0]
    assert (first.symbol, first.reduce_only) == (BTC, True)         # the reduction kept its priority


# ====================================================================================== kill switches, brakes
def test_kill_switches_stop_entries_and_never_an_exit(world):
    open_three(world)
    governance = PromotionGovernance(world.db)
    add = C.AddIntent(symbol=ETH, direction=C.LONG, quantity=0.01, reference_price=2_500.0, stop_price=2_375.0)
    governance.set_kill_switch(True, reason="test", actor_ref="operator:tester")    # the platform switch
    result = world.run(E.entry(XRP), add, E.exit_(BTC))
    assert states(result) == {XRP: X.REFUSED, ETH: X.REFUSED, BTC: X.FILLED}
    assert X.CATI_NEW_ENTRY_KILL_SWITCH in why(result, XRP) and X.CATI_NEW_ENTRY_KILL_SWITCH in why(result, ETH)
    assert world.signed(BTC) == 0.0 and entry_orders(world, XRP) == [] and world.signed(ETH) == 0.16
    governance.set_kill_switch(False, reason="test", actor_ref="operator:tester")
    governance.set_kill_switch(True, reason="test", actor_ref="operator:tester", scope=E.ACCOUNT)    # this account only
    result = world.run(E.entry(XRP), E.exit_(SOL))
    assert states(result) == {XRP: X.REFUSED, SOL: X.FILLED} and X.CATI_NEW_ENTRY_KILL_SWITCH in why(result, XRP)
    governance.set_kill_switch(False, reason="test", actor_ref="operator:tester", scope=E.ACCOUNT)
    # the global governance phase of this repository is M0: the real authority gives no order authority to anyone
    real = world.boundary(authority=GovernanceAuthority(world.db))
    result = world.run(E.entry(XRP), E.entry(BTC), boundary=real)
    assert states(result) == {XRP: X.REFUSED, BTC: X.REFUSED} and "GOVERNANCE_PHASE_M0_NO_CATI_AUTHORITY" in why(result, XRP)
    assert entry_orders(world, XRP) == [] and world.signed(XRP) == 0.0


def test_a_killed_family_manages_its_positions_to_their_exit_and_opens_nothing(world):
    open_three(world)
    world.registry.governance.set_kill_switch(world.definition.family_id, True, operator=S.OPERATOR, reason="test")
    assert world.gate.authorize(world.decide(E.exit_(SOL)), now_ms=world.clock())[1] == PV.FAMILY_KILL_SWITCH
    result = world.run(E.entry(XRP), E.exit_(BTC), C.ReduceIntent(symbol=ETH, direction=C.LONG, reason="DE_RISK", fraction=0.5))
    assert states(result) == {XRP: X.REFUSED, BTC: X.FILLED, ETH: X.FILLED} and why(result, XRP) == PV.FAMILY_KILL_SWITCH
    assert (world.signed(BTC), world.signed(ETH), world.signed(XRP)) == (0.0, 0.08, 0.0) and entry_orders(world, XRP) == []
    # the exception is exactly that narrow: with a second reason against the family, nothing at all is sent
    world.certified.clear()
    refused = world.run(E.exit_(SOL))
    assert (refused.status, refused.reasons) == (X.BATCH_REFUSED, (PV.FAMILY_KILL_SWITCH,)) and world.signed(SOL) == 4.0
    # and a substitute gate that merely says "kill switch" gets no exception either
    static = world.boundary(family_gate=PV.StaticFamilyGate(False, PV.FAMILY_KILL_SWITCH))
    assert static.execute(world.decide(E.exit_(SOL)), brake_state=E.NO_BRAKE).status == X.BATCH_REFUSED


def test_caps_and_brakes_refuse_entries_and_never_reductions(world):
    open_three(world)
    world.fake.mutations.clear()
    unknown = world.run(E.entry(XRP), E.exit_(BTC), brake=None)     # the brake state was not supplied
    assert states(unknown) == {XRP: X.REFUSED, BTC: X.FILLED} and "BRAKE_STATE_UNKNOWN" in why(unknown, XRP)
    halted = world.run(E.entry(XRP), E.exit_(ETH), brake=brakes.BrakeFlags(paused=False, halve=False, halted=True))
    assert states(halted) == {XRP: X.REFUSED, ETH: X.FILLED} and "BRAKE_ACTIVE" in why(halted, XRP)
    paused = world.run(E.entry(XRP), brake=brakes.BrakeFlags(paused=True, halve=False, halted=False))
    assert states(paused) == {XRP: X.REFUSED} and "DAILY_LOSS_PAUSE" in why(paused, XRP)
    assert entry_orders(world) == [] and world.signed(XRP) == 0.0 and world.reservations().get(XRP) is None
    # the position limit of the account, checked by the portfolio layer and again inside the reservation
    tight = dataclasses.replace(world.limits, max_positions=2)
    result = world.run(E.entry(XRP), E.entry(BTC), E.entry(ETH), boundary=world.boundary(limits=tight))
    assert states(result) == {XRP: X.FILLED, BTC: X.REFUSED, ETH: X.REFUSED} and "POSITION_LIMIT" in why(result, BTC)
    assert entry_orders(world, BTC) == [] and world.signed(BTC) == 0.0


def test_a_stop_wider_than_fifteen_percent_refuses_the_entry_and_is_never_clipped(world):
    wide = E.entry(BTC, distance=0.20)
    with pytest.raises(C.ContractViolation) as refused:             # a batch under the engine's limits cannot even exist
        world.decide(wide)
    assert refused.value.code == "ENTRY_STOP_TOO_WIDE"
    # a batch that claims no stop limit is still measured against the account's: refused, not moved to fit
    unlimited = dataclasses.replace(world.limits.constraints(E.EQUITY), max_entry_stop_distance=None)
    result = world.run(wide, E.entry(ETH), constraints=unlimited)
    assert states(result) == {BTC: X.REFUSED, ETH: X.FILLED} and "ENTRY_STOP_TOO_WIDE" in why(result, BTC)
    assert "never clipped" in why(result, BTC) and world.sent(symbol=BTC) == [] and world.stops(BTC) == []
    with pytest.raises(C.ContractViolation) as refused:             # 0.40 % stated against a 0.25 % account
        world.decide(E.entry(SOL, risk=0.004))
    assert refused.value.code == "RISK_ABOVE_CEILING"
    loose = dataclasses.replace(world.limits.constraints(E.EQUITY), per_trade_risk_ceiling=0.004)
    risky = world.run(E.entry(SOL, risk=0.004), constraints=loose)  # a batch that claims a higher ceiling than the account has
    assert states(risky) == {SOL: X.REFUSED} and "RISK_ABOVE_CEILING" in why(risky, SOL) and world.sent(symbol=SOL) == []
    assert world.limits.per_trade_risk == 0.0025 and world.limits.max_entry_stop_distance == 0.15


# ============================================================================================ instrument rules
def test_stale_unknown_or_incomplete_instrument_rules_refuse_entries_and_nothing_is_defaulted(world):
    open_three(world)
    world.fake.add_instrument("DOGEUSDT", price=0.1, price_tick=0.00001, quantity_step=1.0)      # listed; never cached
    doge = C.EntryIntent(symbol="DOGEUSDT", direction=C.LONG, quantity=4_000.0, reference_price=0.1, stop_price=0.095,
                         risk_fraction=0.002)
    world.fake.set_rules(XRP, min_notional=None)                    # the venue stopped reporting a rule
    assert world.rules.refresh(world.fake, world.clock(), symbols=[XRP], force=True).ok
    world.fake.mutations.clear()
    result = world.run(doge, E.entry(XRP), instruments=tuple(E.PRICES) + ("DOGEUSDT",))
    assert states(result) == {"DOGEUSDT": X.REFUSED, XRP: X.REFUSED}
    assert f"{X.INSTRUMENT_RULES}:RULES_UNKNOWN_INSTRUMENT" in why(result, "DOGEUSDT")
    assert f"{X.INSTRUMENT_RULES}:RULES_INCOMPLETE" in why(result, XRP) and world.fake.mutations == []
    world.clock.advance(25 * E.HOUR)                                # every cached rule is now older than a day
    stale = world.run(C.AddIntent(symbol=ETH, direction=C.LONG, quantity=0.01, reference_price=2_500.0, stop_price=2_375.0),
                      E.exit_(BTC), C.ReduceIntent(symbol=SOL, direction=C.LONG, reason="DE_RISK", quantity=1.0))
    assert states(stale) == {ETH: X.REFUSED, BTC: X.FILLED, SOL: X.FILLED}           # reductions are not held back by it
    assert f"{X.INSTRUMENT_RULES}:RULES_STALE" in why(stale, ETH) and world.signed(ETH) == 0.16
    assert (world.signed(BTC), world.signed(SOL)) == (0.0, 3.0)

    class Unreadable:
        def rules_for_entry(self, *args, **kw):
            raise OSError("cache unreadable")

        rules_for_reduce = rules_for_entry

    broken = world.run(E.entry(BTC), E.exit_(SOL), boundary=world.boundary(rules=Unreadable()))
    assert states(broken) == {BTC: X.REFUSED, SOL: X.FILLED} and f"{X.INSTRUMENT_RULES}:UNAVAILABLE" in why(broken, BTC)
    assert entry_orders(world) == [] and world.signed(SOL) == 0.0


# ============================================================================================ releases, order
def test_an_entry_that_only_fits_after_an_exit_waits_until_the_venue_shows_the_exit(world):
    one = dataclasses.replace(world.limits, max_positions=1)
    world.run(E.entry(BTC), boundary=world.boundary(limits=one))
    world.fake.mutations.clear()
    result = world.run(E.entry(ETH), E.exit_(BTC), boundary=world.boundary(limits=one))
    assert states(result) == {ETH: X.FILLED, BTC: X.FILLED} and (world.signed(BTC), world.signed(ETH)) == (0.0, 0.16)
    order = [(r.symbol, r.reduce_only) for r in world.orders_sent(protective=False)]
    assert order == [(BTC, True), (ETH, False)]                     # the slot was free at the venue before it was used
    (reservation,) = world.rows("cati_reservation_ledger", "symbol=?", (ETH,))
    assert reservation["awaits_releases"] != "[]" and reservation["state"] == R.FILLED
    # when the exit does not happen, the entry that depends on it is not sent
    world.fake.fail_next("place_order", c.VENUE_UNAVAILABLE, symbol=ETH)
    stuck = world.run(E.entry(SOL), E.exit_(ETH), boundary=world.boundary(limits=one))
    assert states(stuck) == {SOL: X.PLANNED, ETH: X.DEFERRED} and X.RELEASE_NOT_CONFIRMED in why(stuck, SOL)
    assert world.sent(symbol=SOL) == [] and world.signed(ETH) == 0.16 and world.reservations()[SOL] == R.RESERVED


# ================================================================================================= isolation
def test_two_accounts_never_affect_each_other(world, tmp_path_factory, tmp_path):
    other = E.make_world(tmp_path_factory, tmp_path, account_id="acct-2", user_id="user-2", db=world.db, clock=world.clock)
    # account 1: a position that cannot be protected (closed, account blocked), an unknown submission, a rate limit
    world.adapter.hook_after("place_order", lambda: world.fake.fail_next("place_order", c.PERMISSION_DENIED))
    blocked = world.run(E.entry(BTC))
    assert world.boundary().entry_blocks() != () and world.signed(BTC) == 0.0 and blocked.outcome(BTC).state == X.FILLED
    block = world.boundary().entry_blocks()[0]
    world.boundary().clear_entry_block(block["block_id"], operator_ref="admin-7", evidence="reviewed for the test")
    world.fake.fail_next("place_order", c.NETWORK_UNCERTAIN, symbol=ETH)
    unknown = world.run(E.entry(ETH))
    assert unknown.outcome(ETH).state == X.UNKNOWN and world.reservations() == {BTC: R.FILLED, ETH: R.UNKNOWN}
    world.fake.fail_next("positions", AdapterError(c.RATE_LIMITED, retry_after_s=600))
    assert world.run(E.exit_(SOL)).status == X.BATCH_HALTED and world.boundary().journal.backoff_until() is not None
    world.boundary().journal.block("TEST_OPERATOR_BLOCK", operator_only=True)
    # account 2 is not blocked, not backed off, not short of capacity, and sees none of it
    same = other.run(E.entry(BTC), E.entry(ETH), E.entry(SOL), E.entry(XRP))
    assert states(same) == {BTC: X.FILLED, ETH: X.FILLED, SOL: X.FILLED, XRP: X.FILLED}
    assert other.boundary().entry_blocks() == () and other.boundary().journal.backoff_until() is None
    assert other.reservations("acct-2") == {s: R.FILLED for s in E.PRICES} and all(covered(other, s) for s in E.PRICES)
    assert world.reservations("acct-1") == {BTC: R.FILLED, ETH: R.UNKNOWN}           # untouched by the other account
    assert world.signed(BTC) == 0.0 and world.fake.positions() == ()                # and its venue holds nothing of account 2
    # identities: the same decision content for two accounts never yields the same order id
    ids_1 = {r["client_order_id"] for r in world.rows(schema.INTENT_TABLE, "account_id='acct-1'")}
    ids_2 = {r["client_order_id"] for r in world.rows(schema.INTENT_TABLE, "account_id='acct-2'")}
    assert ids_1 and ids_2 and not ids_1 & ids_2
    assert {r.client_order_id for r in other.orders_sent(protective=False)} == ids_2
    assert X.order_client_id("acct-1", "d", BTC, "ENTRY", 0) != X.order_client_id("acct-2", "d", BTC, "ENTRY", 0)
    # every read and every operation is scoped by the account
    mine = {r["intent_id"] for r in other.boundary().intents()}
    theirs = {r["intent_id"] for r in world.boundary().intents()}
    assert mine and theirs and not mine & theirs
    assert other.boundary().cancel(next(iter(theirs))) is None and other.boundary().recover().unresolved == ()
    assert len(world.boundary().recover().reasons) >= 0 and world.reservations("acct-1")[ETH] == R.UNKNOWN
    # a batch of account 1 handed to account 2's boundary is refused before anything happens
    calls = len(other.adapter.calls)
    crossed = other.boundary().execute(world.decide(E.entry(SOL)), brake_state=E.NO_BRAKE)
    assert crossed.reasons == (X.EXECUTION_ACCOUNT_SCOPE_MISMATCH,) and len(other.adapter.calls) == calls
    assert other.protection().unsettled() == () and world.adapter.open_transactions == [] and other.adapter.open_transactions == []


# ======================================================================================== one runtime handoff
def test_only_the_canonical_connected_runtime_reaches_the_boundary():
    """Only the broker-backed runtime dispatches; shadow and APIs cannot submit."""
    import ast
    from pathlib import Path

    app = Path(X.__file__).resolve().parents[2]
    pipeline = Path(X.__file__).resolve().parent
    target = "app.trading_intelligence.pipeline.execution"
    users = []
    for path in app.rglob("*.py"):
        if path.resolve() == Path(X.__file__).resolve():
            continue
        text = path.read_text(encoding="utf-8", errors="ignore")
        found = target in text or "PortfolioExecutionBoundary" in text          # a direct, a dynamic or a string import
        for node in ast.walk(ast.parse(text)):
            if isinstance(node, ast.ImportFrom):
                module = node.module or ""
                inside = node.level > 0 and path.resolve().parent == pipeline
                names = {alias.name for alias in node.names}
                found |= (module == "app.trading_intelligence.pipeline" or (inside and module == "")) and "execution" in names
                found |= inside and module == "execution"
        if found:
            users.append(path.name)
    assert users == ["portfolio_runtime.py"]
    driver = (pipeline / "driver.py").read_text(encoding="utf-8")   # the driver's hand-off exists and is empty
    assert "handoff: Any = None" in driver
    for module in (X, PR):                                          # and neither module can switch a gate off
        source = Path(module.__file__).read_text(encoding="utf-8")
        assert "os.environ" not in source and "getenv" not in source
