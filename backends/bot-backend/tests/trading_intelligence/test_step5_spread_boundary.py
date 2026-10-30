"""Step 5.5 / EXT-LIQ-01: the platform order budget inside ``CATIExecutionBoundary``.

The first part drives the REAL boundary, orchestrator and executor (``_exec.Harness``) with a REAL
``PlatformSpreadGate`` injected over a platform database of each dialect (SQLite always, PostgreSQL when
``PLATFORM_TEST_POSTGRES_URL`` is set; ``PLATFORM_TEST_POSTGRES_REQUIRED=1`` turns its absence into a failure).
The second part runs under production settings on the end-to-end production tail (``process_account``) with a
SYNTHETIC certified family (``test_step3_execution_tail.certified``): there the gate applies without being
asked, and the production default refuses.

Nothing here is a statement about real venue liquidity: depth comes from a fake venue client.
"""
from __future__ import annotations

import os
import uuid
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from _exec import HARNESS_SPREAD_POLICY, Harness, _entry, _order, order_book

from app.platform_workers.assignments import WorkerAssignments
from app.trading_intelligence.contracts.execution import ExecutionAttemptStatus as X
from app.trading_intelligence.contracts.position import ExitDecision
from app.trading_intelligence.execution.boundary import BoundaryStatus as B
from app.trading_intelligence.execution.config import CATIExecutionConfig
from app.trading_intelligence.scale import spread_gate as G
from app.trading_intelligence.scale.order_spreading import SpreadPolicy
from shared_lib.persistence.sql import Database, decimal_out, migrate
from shared_lib.persistence.sql.platform_schema import all_migrations

from test_production_execution import live  # noqa: F401  (fixture)
from test_production_demo_execution import demo  # noqa: F401  (fixture)
from test_production_research_separation import fresh, process  # noqa: F401  (fixture)
from test_step3_execution_tail import certified  # noqa: F401  (fixture)

PG_URL = os.environ.get("PLATFORM_TEST_POSTGRES_URL", "")
PG_REQUIRED = os.environ.get("PLATFORM_TEST_POSTGRES_REQUIRED") == "1"


def _pg_admin(sql):
    import psycopg
    with psycopg.connect(PG_URL, autocommit=True) as conn:
        conn.execute(sql)


@pytest.fixture(params=["sqlite", "postgresql"])
def platform_url(request, tmp_path):
    if request.param == "sqlite":
        yield f"sqlite:///{(tmp_path / 'platform_spread.db').as_posix()}"
        return
    if not PG_URL:
        if PG_REQUIRED:
            pytest.fail("PLATFORM_TEST_POSTGRES_REQUIRED=1 but PLATFORM_TEST_POSTGRES_URL is not set")
        pytest.skip("PLATFORM_TEST_POSTGRES_URL not set")
    name = f"cfsb_{uuid.uuid4().hex[:12]}"
    _pg_admin(f"CREATE DATABASE {name}")
    try:
        yield PG_URL.rpartition("/")[0] + "/" + name
    finally:
        _pg_admin(f"DROP DATABASE IF EXISTS {name} WITH (FORCE)")


@pytest.fixture
def pdb(platform_url):
    db = Database(platform_url, pool_size=4)
    migrate(db, all_migrations())
    yield db
    db.close()


def register(db, plan, tenant=None):
    WorkerAssignments(db).upsert_accounts([(plan.broker_account_id, tenant or plan.user_id, plan.venue,
                                            plan.environment)])


def harness(tmp_path, pdb, policy=HARNESS_SPREAD_POLICY, **kw):
    h = Harness(tmp_path, **kw)
    register(pdb, h.plan)
    h.gate = G.PlatformSpreadGate(pdb, policy=policy)
    return h


def run(h, **kw):
    b = h.boundary(spread_gate=h.gate, **kw)
    return b, h.run(b)


def rows(db, sql, params=()):
    with db.transaction(write=False) as conn:
        return conn.all(sql, params)


def slots(db):
    return rows(db, "SELECT account_id, status, granted_notional, requested_notional, reason FROM platform_order_slots")


def links(db):
    return rows(db, "SELECT outcome, code, execution_attempt_id, client_order_id, settled_status, settled_notional, "
                    "slot_id FROM platform_spread_decisions ORDER BY created_at")


# ═══════════════════════════════════════════════════ the boundary, gate injected (both dialects)
@pytest.mark.parametrize("book,code", [
    (lambda symbol, limit=50: (_ for _ in ()).throw(TimeoutError("venue")), "DEPTH_UNAVAILABLE"),
    (lambda symbol, limit=50: order_book(age_ms=60_000), "DEPTH_STALE"),
    (lambda symbol, limit=50: order_book(levels=3), "DEPTH_INSUFFICIENT_LEVELS"),
    (lambda symbol, limit=50: {"bids": [], "asks": []}, "DEPTH_TIMESTAMP_MISSING")])
def test_entry_without_usable_depth_is_refused_before_the_claim(tmp_path, pdb, book, code):
    h = harness(tmp_path, pdb)
    h.client.get_orderbook.side_effect = book
    b, res = run(h)
    assert (res.status, res.reason_codes) == (B.ORDER_SPREADING_REFUSED, (code,))
    assert h.seen["orders"] == [] and h.client.place_order.call_count == 0
    assert b.attempts.for_account(h.plan.broker_account_id) == []
    assert h.reservation_status() == "RELEASED" and slots(pdb) == []
    assert [(r[0], r[1]) for r in links(pdb)] == [("REFUSED", code)]


def test_partial_grant_is_refused_and_the_quantity_never_resized(tmp_path, pdb):
    small = SpreadPolicy(policy_version="test-small-cap", allow_market_fallback=True,
                         absolute_cap_notional=Decimal("50"))
    h = harness(tmp_path, pdb, policy=small)
    b, res = run(h)
    assert (res.status, res.reason_codes) == (B.ORDER_SPREADING_REFUSED, ("PLATFORM_SYMBOL_BUDGET_PARTIAL_GRANT",))
    assert h.seen["orders"] == [] and slots(pdb) == []
    [refusal] = rows(pdb, "SELECT requested_notional FROM platform_spread_decisions")
    assert decimal_out(refusal[0]) > Decimal("50")          # the risk-approved notional, not a reduced one


def test_market_entry_is_refused_by_the_default_policy(tmp_path, pdb):
    h = harness(tmp_path, pdb, policy=G.CONSERVATIVE_DEFAULT)
    b, res = run(h)
    assert (res.status, res.reason_codes) == (B.ORDER_SPREADING_REFUSED, ("MARKET_FALLBACK_DISABLED_BY_POLICY",))
    assert h.seen["orders"] == []


def test_single_account_under_the_cap_reaches_the_venue_and_the_slot_follows_the_fill(tmp_path, pdb):
    h = harness(tmp_path, pdb)
    b, res = run(h)
    assert res.status == B.EXECUTED and len(h.seen["orders"]) == 1
    a = res.attempt
    [(outcome, code, attempt_id, cid, settled, settled_notional, slot_id)] = links(pdb)
    # the account's own identity stays the boundary's / executor's; the slot is linked for audit
    assert (outcome, code, attempt_id, cid) == ("RESERVED", None, a.execution_attempt_id, a.client_order_id)
    assert h.seen["orders"][0].client_order_id == a.client_order_id
    filled = Decimal(str(a.filled_quantity)) * Decimal(str(a.filled_price))
    [(account, status, granted, requested, _)] = slots(pdb)
    assert (account, status, decimal_out(granted)) == (h.plan.broker_account_id, "FILLED", filled)
    assert settled == "FILLED" and decimal_out(settled_notional) == filled
    [obs] = rows(pdb, "SELECT reference_id, fill_price, filled_quantity, partial_fill, impact_model, impact_calibrated "
                      "FROM platform_spread_observations")
    assert obs[0] == a.execution_attempt_id and decimal_out(obs[1]) == Decimal(str(a.filled_price))
    assert decimal_out(obs[2]) == Decimal(str(a.filled_quantity)) and obs[3:] == (0, G.IMPACT_MODEL, 0)
    # a replay is a duplicate before the gate: no second slot, no second order
    assert h.run(b).status == B.DUPLICATE_PLAN and len(slots(pdb)) == 1 and len(h.seen["orders"]) == 1


def test_partial_fill_adjusts_the_granted_notional(tmp_path, pdb):
    h = harness(tmp_path, pdb, entry=_entry(), orders=[_order("PARTIALLY_FILLED", "3.5", "100.0"),
                                                       _order("CANCELED", "3.5", "100.0")])
    b, res = run(h)
    assert res.status == B.EXECUTED and res.attempt.status == X.PARTIALLY_FILLED.value
    [(_, status, granted, requested, _)] = slots(pdb)
    assert status == "FILLED" and decimal_out(granted) == Decimal("350.0") < decimal_out(requested)
    assert rows(pdb, "SELECT partial_fill FROM platform_spread_observations") == [(1,)]


@pytest.mark.parametrize("kw", [dict(entry=_entry(), orders=[_order("CANCELED", "0")]),
                                dict(place_error=RuntimeError('{"code":-1111,"msg":"invalid precision"}'))])
def test_rejected_or_cancelled_entry_releases_the_slot(tmp_path, pdb, kw):
    h = harness(tmp_path, pdb, **kw)
    b, res = run(h)
    assert res.status == B.EXECUTION_REJECTED and h.reservation_status() == "RELEASED"
    [(_, status, _, _, _)] = slots(pdb)
    assert status == "RELEASED" and links(pdb)[0][4] == "RELEASED"


@pytest.mark.parametrize("truth", ["position", "no_position"])
def test_submit_unknown_keeps_the_slot_until_reconciliation(tmp_path, pdb, truth):
    h = harness(tmp_path, pdb, entry=_entry(), order_error=RuntimeError("timeout"))
    b, res = run(h)
    assert res.status == B.SUBMIT_UNKNOWN
    assert [s[1] for s in slots(pdb)] == ["SUBMITTED"] and links(pdb)[0][4] is None
    assert b.reconcile_submit_unknown(h.plan, now_ms=h.now + 1).status == B.STILL_UNKNOWN
    assert [s[1] for s in slots(pdb)] == ["SUBMITTED"]
    if truth == "position":
        h.client.get_order.side_effect = lambda s, o: _order("FILLED", "6.0", "100.0")
        h.client.get_position_info.return_value = {"positionAmt": "6.0", "entryPrice": "100.0"}
        assert b.reconcile_submit_unknown(h.plan, now_ms=h.now + 2).status == B.RECONCILED
        [(_, status, granted, _, _)] = slots(pdb)
        assert status == "FILLED" and decimal_out(granted) == Decimal("600.0")
    else:
        h.client.get_order.side_effect = lambda s, o: _order("CANCELED", "0")
        assert b.reconcile_submit_unknown(h.plan, now_ms=h.now + 2).status == B.RECONCILED
        assert [s[1] for s in slots(pdb)] == ["RELEASED"]
    assert links(pdb)[0][4] in ("FILLED", "RELEASED")


def test_a_second_account_is_refused_while_the_policy_is_uncalibrated(tmp_path, pdb):
    h = harness(tmp_path, pdb)
    # another customer's account already holds an active slot on this venue/symbol/side (another signal even)
    other = SimpleNamespace(**{**{k: getattr(h.plan, k) for k in ("venue", "environment", "instrument_key", "side",
                                                                  "setup_version", "decision_time", "versions",
                                                                  "execution_preferences", "plan_expiry_time")},
                               "broker_account_id": "other-acct", "user_id": "other-user", "setup_family": "OTHER",
                               "trade_plan_id": "tp-other", "mode": "DEMO", "source_candidate_id": "d-other"})
    register(pdb, other)
    h.gate.reserve(other, client=h.client, adapter=h.adapter, quantity=Decimal("1"), notional=Decimal("100"),
                   attempt_id="att-other")
    _, res = run(h)
    assert (res.status, res.reason_codes) == (B.ORDER_SPREADING_REFUSED, ("ORDER_SPREADING_CALIBRATION_REQUIRED",))
    assert h.seen["orders"] == [] and h.reservation_status() == "RELEASED"
    assert [(s[0], s[1]) for s in slots(pdb)] == [("other-acct", "RESERVED")]


def test_crash_after_the_claim_is_recovered_by_the_sweep(tmp_path, pdb):
    h = harness(tmp_path, pdb)
    gate = G.PlatformSpreadGate(pdb, policy=HARNESS_SPREAD_POLICY, claim_grace_ms=0)
    b = h.boundary(spread_gate=gate)
    gate.settle_attempt = MagicMock(side_effect=RuntimeError("platform database lost mid-settle"))
    res = h.run(b)                                    # the order path is not affected by a settle failure
    assert res.status == B.EXECUTED
    assert [s[1] for s in slots(pdb)] == ["SUBMITTED"] and links(pdb)[0][4] is None
    del gate.settle_attempt                           # the process restarts: recovery settles from the attempt
    b.recover_pending(now_ms=h.now + 10)
    [(_, status, granted, _, _)] = slots(pdb)
    assert status == "FILLED" and decimal_out(granted) == \
        Decimal(str(res.attempt.filled_quantity)) * Decimal(str(res.attempt.filled_price))
    assert links(pdb)[0][4] == "FILLED"


class Crash(BaseException):
    """A killed process: nothing catches it."""


@pytest.mark.parametrize("kw,reservation,slot", [
    ({}, "CONSUMED", "FILLED"),
    (dict(entry=_entry(), orders=[_order("CANCELED", "0")]), "RELEASED", "RELEASED")])
def test_crash_between_terminal_attempt_and_reservation_is_repaired_without_resubmitting(tmp_path, pdb, kw,
                                                                                         reservation, slot):
    """The process dies after the terminal attempt row is durable and before the reservation (and the slot) are
    settled. Recovery resolves both from the durable attempt; the venue is not asked to create anything again."""
    from app.trading_intelligence.trade_plan.evidence_store import TradePlanEvidenceStore
    h = harness(tmp_path, pdb, **kw)
    TradePlanEvidenceStore(h.db).append(h.plan)       # recovery reloads the immutable plan, as in production
    b = h.boundary(spread_gate=h.gate)

    def die(*a, **k):
        raise Crash()

    b._settle_spread = die                            # first step after the terminal attempt append
    with pytest.raises(Crash):
        h.run(b)
    assert h.reservation_status() == "RESOLUTION_PENDING" and [s[1] for s in slots(pdb)] == ["SUBMITTED"]
    orders = len(h.seen["orders"])
    restarted = h.boundary(spread_gate=h.gate)        # a new process: nothing in memory survives
    out = restarted.recover_pending(now_ms=h.now + 5)
    assert [r.reason_codes for r in out] == [("TERMINAL_ATTEMPT_RECOVERED",)]
    assert h.reservation_status() == reservation and [s[1] for s in slots(pdb)] == [slot]
    assert links(pdb)[0][4] == slot
    again = restarted.recover_pending(now_ms=h.now + 6)            # idempotent
    assert again == [] and h.reservation_status() == reservation
    assert len(h.seen["orders"]) == orders                          # never re-submitted


def test_slot_mark_failure_refuses_the_order(tmp_path, pdb):
    h = harness(tmp_path, pdb)
    h.gate.mark_submitted = MagicMock(side_effect=RuntimeError("platform database lost"))
    b, res = run(h)
    assert res.status == B.EXECUTION_REJECTED and res.reason_codes == (G.SLOT_TRANSITION_FAILED,)
    assert h.seen["orders"] == [] and h.reservation_status() == "RELEASED"
    assert [s[1] for s in slots(pdb)] == ["RELEASED"]
    assert b.attempts.latest(h.plan.broker_account_id, res.attempt.execution_attempt_id)["status"] == \
        X.ERROR_PRE_SUBMIT.value


def test_recorder_failure_never_affects_the_order_path(tmp_path, pdb):
    h = harness(tmp_path, pdb)
    with pdb.transaction() as conn:
        conn.execute("DROP TABLE platform_spread_observations")
    b, res = run(h)
    assert res.status == B.EXECUTED and [s[1] for s in slots(pdb)] == ["FILLED"]


def test_exits_and_protective_orders_never_meet_the_budget(tmp_path, pdb):
    h = harness(tmp_path, pdb)
    h.client.get_orderbook.side_effect = lambda *a, **k: (_ for _ in ()).throw(AssertionError("no depth read"))
    b = h.boundary(spread_gate=h.gate, config=CATIExecutionConfig(active_execution_enabled=True,
                                                                  exit_intent_routing_enabled=True))
    h.adapter.submit_exit = MagicMock(return_value={"status": "ok"})
    decision = ExitDecision.build(
        position_forecast_id="pfc_x", position_id="pos_1", trade_plan_id=h.plan.trade_plan_id, position_path_id="pp",
        user_id=h.plan.user_id, broker_account_id=h.plan.broker_account_id, bot_instance_id=h.plan.bot_instance_id,
        action="EXIT", requested_fraction=None, suggested_protection_price=None, existing_protection_price=95.0,
        side=h.plan.side, decision_time=1, conservative_remaining_edge_R=0.0, thesis_status="VALID", reason_codes=(),
        policy_version="1", policy_hash="h", engine_version="1")
    assert b.process_exit_decision(decision, h.plan, live_qty=6.0).status == B.EXIT_ROUTED
    h.adapter.submit_exit.assert_called_once()
    assert slots(pdb) == [] and links(pdb) == []


def test_outside_production_without_an_injected_gate_nothing_changes(tmp_path):
    h = Harness(tmp_path)
    h.client.get_orderbook.side_effect = AssertionError("no depth read outside production")
    assert h.run(h.boundary()).status == B.EXECUTED


# ═══════════════════════════════════════════════════ production settings: the gate applies unasked
def _boundary(h):
    return h.boundary_for()


def test_production_without_a_platform_database_refuses_every_entry(certified, monkeypatch):  # noqa: F811
    monkeypatch.delenv("PLATFORM_DATABASE_URL", raising=False)
    _boundary(certified).spread_gate = None                    # what production constructs: the default gate
    result = process(certified)["boundary"]
    assert result["status"] == B.ORDER_SPREADING_REFUSED
    assert result["reason_codes"][0] == "PLATFORM_ORDER_BUDGET_UNAVAILABLE"
    certified.client.place_order.assert_not_called()


def test_production_default_policy_refuses_the_market_entry(certified, platform_url, pdb, monkeypatch):  # noqa: F811
    monkeypatch.setenv("PLATFORM_DATABASE_URL", platform_url)
    register(pdb, certified.plan)
    boundary = _boundary(certified)
    boundary.spread_gate = None
    try:
        result = process(certified)["boundary"]
        assert type(boundary.spread_gate) is G.PlatformSpreadGate and boundary.spread_gate.policy.calibrated is False
    finally:
        cached = G._databases.pop(platform_url, None)
        if cached is not None:
            cached.close()
    assert result["status"] == B.ORDER_SPREADING_REFUSED
    assert result["reason_codes"][0] == "MARKET_FALLBACK_DISABLED_BY_POLICY"
    certified.client.place_order.assert_not_called()
    assert [r[1] for r in rows(pdb, "SELECT outcome, code FROM platform_spread_decisions")] == \
        ["MARKET_FALLBACK_DISABLED_BY_POLICY"]


def test_production_refuses_a_substitute_gate(certified):  # noqa: F811
    _boundary(certified).spread_gate = SimpleNamespace(reserve=lambda *a, **k: None)
    result = process(certified)["boundary"]
    assert (result["status"], result["reason_codes"][0]) == (B.ORDER_SPREADING_REFUSED, G.GATE_NOT_AUTHORITATIVE)
    certified.client.place_order.assert_not_called()


def test_production_entry_passes_the_real_gate_with_real_inputs(certified):  # noqa: F811
    boundary = _boundary(certified)
    assert type(boundary.spread_gate) is G.PlatformSpreadGate        # injected by the harness, never skipped
    result = process(certified)["boundary"]
    assert result["status"] == B.EXECUTED, result
    pdb = boundary.spread_gate.database()
    [(outcome, _, attempt_id, cid, settled, _, _)] = links(pdb)
    assert (outcome, settled) == ("RESERVED", "FILLED") and attempt_id and cid
    assert [s[1] for s in slots(pdb)] == ["FILLED"]


def test_production_stale_depth_refuses(certified):  # noqa: F811
    certified.client.get_orderbook.side_effect = lambda symbol, limit=50: order_book(age_ms=30_000)
    result = process(certified)["boundary"]
    assert (result["status"], result["reason_codes"][0]) == (B.ORDER_SPREADING_REFUSED, "DEPTH_STALE")
    certified.client.place_order.assert_not_called()
