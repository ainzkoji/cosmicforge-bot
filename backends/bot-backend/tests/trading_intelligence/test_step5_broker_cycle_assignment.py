"""Step 5.2 / EXT-WRK-01 -- the REAL production broker cycle under durable worker assignments.

Entry points: ``production_runtime.sync`` / ``_sync_accounts`` / ``sync_account`` / ``flatten`` / ``wait_idle`` and
the execution path below them (``production_execution``, ``CATIExecutionBoundary``, native protection, reduce-only
close), driven over the stateful fake Binance venue of the demo-certification tests (``broker`` fixture: entries,
closePosition protection legs, reduce-only closes and cancels kept in one in-memory book), with a temporary legacy
SQLite database and a temporary PLATFORM database of each dialect (SQLite; PostgreSQL through
``PLATFORM_TEST_POSTGRES_URL``, required when ``PLATFORM_TEST_POSTGRES_REQUIRED=1``).

A worker is a ``BrokerCycleAssignments`` with its own worker id on the shared platform database. A KILLED worker is
a thread that stops inside its account step -- blocked at a chosen venue call -- and never releases anything; the
test later lets it run on, to prove that whatever it does next is refused before it reaches the venue.

The Step 5.5 order budget (``CATIExecutionBoundary`` spread gate) is not this module's subject and is switched off
here; ``tests/platform_workers/test_order_spreading.py`` covers it. Nothing here weakens any other gate.
"""
from __future__ import annotations

import json
import os
import threading
import time
import uuid
from types import SimpleNamespace
from unittest.mock import DEFAULT, Mock

import pytest
from test_demo_boundary_certification import broker
from test_production_execution import live
from test_production_demo_execution import demo
from test_production_research_separation import fresh

from app.core import config
from app.execution import demo_boundary_certification as cert
from app.ops import runtime_shutdown
from app.platform_workers import account_fence, broker_assignment
from app.platform_workers.assignments import AssignmentLost, WorkerAssignments
from app.platform_workers.broker_assignment import ASSIGNED, OWNER, BrokerCycleAssignments
from app.trading_intelligence.execution.boundary import CATIExecutionBoundary
from app.trading_intelligence.integration import production_execution as production
from app.trading_intelligence.integration import production_runtime as runtime
from app.trading_intelligence.integration import residual_prospective
from shared_lib.persistence.sql import Database, migrate
from shared_lib.persistence.sql.platform_schema import all_migrations

__all__ = ["broker", "fresh", "demo", "live"]  # fixtures re-exported for pytest

TTL_MS, MARGIN_MS = 1_500, 300
INTERVAL_S = 0.1            # the successor's loop interval in these tests
SLACK_S = 1.0               # scheduling + one account step on a loaded workstation
PG_URL = os.environ.get("PLATFORM_TEST_POSTGRES_URL", "")
PG_REQUIRED = os.environ.get("PLATFORM_TEST_POSTGRES_REQUIRED") == "1"
#: Takeover measurements, written to ``BROKER_TAKEOVER_EVIDENCE_OUT`` (JSON) at the end of the module when set.
MEASUREMENTS: list = []
#: The unpatched runtime entry point (the rig wraps it per test).
REAL_SYNC_ACCOUNT = runtime.sync_account


# ── platform database of each dialect ──────────────────────────────────────────────────────────────────────
def _pg_admin(sql):
    import psycopg
    with psycopg.connect(PG_URL, autocommit=True) as conn:
        conn.execute(sql)


@pytest.fixture(params=["sqlite", "postgresql"])
def platform(request, tmp_path):
    if request.param == "sqlite":
        db = Database(f"sqlite:///{(tmp_path / 'platform.db').as_posix()}", pool_size=16)
        name = None
    else:
        if not PG_URL:
            if PG_REQUIRED:
                pytest.fail("PLATFORM_TEST_POSTGRES_REQUIRED=1 but PLATFORM_TEST_POSTGRES_URL is not set")
            pytest.skip("PLATFORM_TEST_POSTGRES_URL not set")
        name = f"cfbc_{uuid.uuid4().hex[:12]}"
        _pg_admin(f"CREATE DATABASE {name}")
        db = Database(f"{PG_URL.rpartition('/')[0]}/{name}", pool_size=16)
    migrate(db, all_migrations())
    db.dialect_name = request.param
    try:
        yield db
    finally:
        db.close()
        if name:
            _pg_admin(f"DROP DATABASE IF EXISTS {name} WITH (FORCE)")


@pytest.fixture(scope="module", autouse=True)
def _write_measurements():
    yield
    out = os.environ.get("BROKER_TAKEOVER_EVIDENCE_OUT")
    if out and MEASUREMENTS:
        existing = []
        if os.path.exists(out):
            existing = json.loads(open(out, encoding="utf-8").read())
        with open(out, "w", encoding="utf-8") as handle:
            handle.write(json.dumps(existing + MEASUREMENTS, indent=1, sort_keys=True))


# ── the rig: the real cycle on the fake venue, workers on the platform database ───────────────────────────
class KillPoint:
    """``worker``'s thread stops inside the venue call ``method`` until ``release()`` -- a worker killed there."""

    def __init__(self, client, method, worker_id):
        self.reached, self.released = threading.Event(), threading.Event()
        self.mock = getattr(client, method)
        self.original = self.mock.side_effect
        self.worker_id = worker_id

        def side_effect(*a, **kw):
            fence = account_fence.current()
            if fence is not None and fence.worker_id == self.worker_id and not self.released.is_set():
                self.reached.set()
                self.released.wait(60)
            return self.original(*a, **kw) if callable(self.original) else DEFAULT

        self.mock.side_effect = side_effect

    def release(self):
        self.released.set()


@pytest.fixture
def rig(broker, platform, monkeypatch):
    h, state = broker
    runtime_shutdown.reset_for_tests()
    lease = {"held": True}

    def owner(db):
        return lease["held"]

    for module in (runtime, production, residual_prospective):
        monkeypatch.setattr(module, "owner_current", owner)
    monkeypatch.setattr(runtime, "settings", config.settings)
    versions = {}

    def auth_for(account_id, user_id, db):
        return SimpleNamespace(environment="demo", account_id=account_id, user_id=user_id, broker_type="binance",
                               base_url=h.client.base_url, credential_version=versions.get(account_id, 1),
                               key_fingerprint="fp")

    monkeypatch.setattr(runtime, "resolve_broker_auth", auth_for)
    from app.activation import account_status
    monkeypatch.setattr(account_status, "refresh_if_stale", lambda *a, **kw: {"status": "SYNCED"})
    monkeypatch.setattr(production, "eligibility", lambda row, now: "AWAITING_NATURAL_CATI_DECISION")
    monkeypatch.setattr(CATIExecutionBoundary, "_active_spread_gate", lambda self: None, raising=False)
    original_process, original_recover = production.process_account, production.recover_account
    monkeypatch.setattr(production, "process_account", lambda db, account, client, snapshot, **kw: original_process(
        db, account, client, snapshot, boundary_factory=h.boundary_for, **kw))
    monkeypatch.setattr(production, "recover_account", lambda db, account, client, **kw: original_recover(
        db, account, client, boundary_factory=h.boundary_for, **kw))
    clients = {h.account["id"]: h.client}
    steps = []                                  # (worker, account, started, ended, outcome) of every account step
    original_sync = runtime.sync_account

    def sync_account(db, account, **kw):
        fence = account_fence.current()
        started = time.monotonic()
        outcome = "ok"
        try:
            return original_sync(db, account, factory=lambda auth: clients[account["id"]], **kw)
        except Exception as exc:
            outcome = type(exc).__name__
            raise
        finally:
            steps.append((fence.worker_id if fence else None, account["id"], started, time.monotonic(), outcome,
                          bool(kw.get("recovery_only"))))

    monkeypatch.setattr(runtime, "sync_account", sync_account)

    # Every mutation the fake venue ACCEPTED, with the worker whose fence was active in the calling thread.
    accepted = []

    def tag(kind, cid):
        fence = account_fence.current()
        accepted.append({"worker": fence.worker_id if fence else None, "kind": kind, "cid": cid,
                         "at": time.monotonic()})

    place, post, delete = (h.client.place_order.side_effect, h.client._signed_post.side_effect,
                           h.client._signed_delete.side_effect)

    def placed(req):
        out = place(req)
        tag("ENTRY", req.client_order_id)
        return out

    def posted(path, params):
        out = post(path, params)
        tag("PROTECTION" if path.endswith("algoOrder") else "CLOSE",
            params.get("clientAlgoId") or params.get("newClientOrderId"))
        return out

    def deleted(path, params):
        out = delete(path, params)
        tag("CANCEL", params.get("algoId"))
        return out

    h.client.place_order.side_effect = placed
    h.client._signed_post.side_effect = posted
    h.client._signed_delete.side_effect = deleted
    runtime._clients.clear()
    runtime.initialize(h.db)
    with h.db.connect() as c:                   # the harness's second (LIVE) account plays no part here
        c.execute("UPDATE broker_accounts SET status='disconnected' WHERE id<>?", (h.account["id"],))
    workers = []

    def worker(wid, mode=ASSIGNED, ttl_ms=TTL_MS):
        layer = BrokerCycleAssignments(platform, mode=mode, ttl_ms=ttl_ms, margin_ms=MARGIN_MS, worker_id=wid)
        layer.register()
        workers.append(layer)
        return layer

    kills = []

    def kill(method, worker_id, client=None):
        point = KillPoint(client or h.client, method, worker_id)
        kills.append(point)
        return point

    def hold_position(run_id="hold"):
        """An open position with its two native protection legs and full durable lineage (labelled DEMO
        certification through the real boundary, outside any assignment)."""
        report = cert.run(h.db, h.account["id"], run_id, action="hold")
        assert report["status"] == "PROTECTED", report
        assert state["qty"] > 0 and len(state["legs"]) == 2
        return report

    def round_of(layer):
        return runtime.sync(h.db, layer=layer)

    yield SimpleNamespace(h=h, state=state, db=h.db, platform=platform, lease=lease, worker=worker, kill=kill,
                          accepted=accepted, steps=steps, clients=clients, versions=versions, round=round_of,
                          hold_position=hold_position, account=h.account["id"])
    for point in kills:
        point.release()
    broker_assignment.install(None)
    runtime_shutdown.reset_for_tests()
    runtime._clients.clear()


def holder(rig, account=None):
    owner = WorkerAssignments(rig.platform).owner_of(account or rig.account)
    return owner[0] if owner else None


def history(rig, account=None):
    return WorkerAssignments(rig.platform).history(account or rig.account)


def state_doc(rig, account=None):
    with rig.db.connect() as c:
        row = c.execute("SELECT document FROM cati_production_state WHERE account_id=?", (account or rig.account,)).fetchone()
    return json.loads(row[0]) if row else None


def accepted_cids(rig):
    return [a["cid"] for a in rig.accepted if a["kind"] in ("ENTRY", "PROTECTION", "CLOSE")]


def until(predicate, timeout_s, step_s=INTERVAL_S, between=None):
    started = time.monotonic()
    while time.monotonic() - started < timeout_s:
        if between is not None:
            between()
        if predicate():
            return time.monotonic() - started
        time.sleep(step_s)
    return None


# ── owner mode: the canonical runtime owner holds every free account ─────────────────────────────────────
def test_owner_mode_claims_every_free_account_recovers_first_then_runs_the_cycle(rig):
    rig.hold_position()
    setup = len(rig.accepted)                                     # the entry and its two legs
    owner = rig.worker("owner-1", mode=OWNER)
    rig.round(owner)
    assert holder(rig) == "owner-1"
    first = state_doc(rig)
    assert first["status"] == "SYNCED"
    assert first["execution"]["stage"] == "ASSIGNMENT_RECOVERY" and first["execution"]["assignment_recovery"] == "COMPLETE"
    assert first["execution"]["execution_history"][0]["protection"]["state"] == "CONFIRMED"
    assert not owner.owned[rig.account].needs_recovery
    rig.round(owner)
    second = state_doc(rig)
    assert second["status"] == "SYNCED" and second["execution"].get("stage") != "ASSIGNMENT_RECOVERY"
    assert [r[3] for r in history(rig)] == ["INITIAL"]
    assert [s[5] for s in rig.steps] == [True, False]          # recovery only, then the full step
    assert len(rig.accepted) == setup and rig.state["create_count"] == 1  # the open position needed nothing new
    # Without the lease an owner-mode worker does no account work at all.
    rig.lease["held"] = False
    rig.round(owner)
    assert len(rig.steps) == 2


def test_owner_mode_never_steals_a_live_assignment(rig):
    worker = rig.worker("assigned-1")
    worker.begin_round(runtime.execution_accounts(rig.db))
    owner = rig.worker("owner-1", mode=OWNER)
    rig.round(owner)
    assert holder(rig) == "assigned-1" and rig.account not in owner.owned and rig.steps == []


# ── a worker killed in the middle of its account step ──────────────────────────────────────────────────────
@pytest.mark.parametrize("kill_at", [
    "account",            # account processing: the first venue read of the step
    "get_algo_orders",    # protection verification (native conditional-order read)
    "user_trades",        # reconciliation of the attempt's fills
    "get_order",          # scheduled maintenance: the persisted order's resolution
])
def test_killed_worker_is_taken_over_within_one_ttl_and_never_mutates_again(rig, kill_at):
    rig.hold_position()
    rig.lease["held"] = False                    # assigned workers: the fence is their only authority
    a, b = rig.worker("A"), rig.worker("B")
    rig.round(a)                                  # A: claim + recovery
    rig.round(a)                                  # A: an ordinary step
    assert holder(rig) == "A" and not a.owned[rig.account].needs_recovery
    point = rig.kill(kill_at, "A")
    dying = threading.Thread(target=rig.round, args=(a,), daemon=True)
    dying.start()
    assert point.reached.wait(10), f"A never reached {kill_at}"
    killed_at = time.monotonic()
    claimed = until(lambda: holder(rig) == "B", (TTL_MS / 1000) * 3, between=lambda: rig.round(b))
    assert claimed is not None, "B never took the account over"
    recovered = until(lambda: rig.account in b.owned and not b.owned[rig.account].needs_recovery, 10,
                      between=lambda: rig.round(b))
    assert recovered is not None
    total = time.monotonic() - killed_at
    MEASUREMENTS.append({"scenario": f"kill_at:{kill_at}", "dialect": rig.platform.dialect_name, "ttl_ms": TTL_MS,
                         "loop_interval_ms": int(INTERVAL_S * 1000),
                         "kill_to_claim_ms": round(claimed * 1000), "kill_to_recovered_ms": round(total * 1000)})
    assert claimed <= TTL_MS / 1000 + INTERVAL_S + SLACK_S, claimed
    # The successor recovered first: protection verified, nothing re-sent.
    takeover = [r for r in history(rig) if r[3] == "EXPIRED_TAKEOVER"]
    assert takeover and takeover[-1][:2] == ("A", "B")
    recovery_steps = [s for s in rig.steps if s[0] == "B" and s[5]]
    assert recovery_steps and recovery_steps[0][4] == "ok"
    assert rig.state["qty"] > 0 and len(rig.state["legs"]) == 2       # still protected
    # A is let go: everything it does next is refused before the venue; it writes no state for the account.
    before = len(rig.accepted)
    saved = state_doc(rig)
    point.release()
    dying.join(30)
    assert not dying.is_alive()
    assert [x for x in rig.accepted[before:] if x["worker"] == "A"] == []
    a_steps = [s for s in rig.steps if s[0] == "A"]
    assert a_steps[-1][4] == "AssignmentLost", a_steps[-1]
    assert state_doc(rig) == saved
    rig.round(b)
    assert state_doc(rig)["status"] == "SYNCED"
    assert len(accepted_cids(rig)) == len(set(accepted_cids(rig)))       # no client order id accepted twice
    assert rig.state["create_count"] == 1                                  # never a second entry


def test_kill_before_the_boundary_submit_never_creates_twice(rig):
    """A worker dies while preparing an entry (after its reservation, before the boundary's CREATE); the
    successor recovers; the dead worker, let go, is refused at the boundary before the venue."""
    rig.lease["held"] = False
    a, b = rig.worker("A"), rig.worker("B")
    rig.round(a)                                            # A: claim + recovery of a flat account
    point = rig.kill("last_price", "A")                     # read by prepare_submission, before the boundary
    outcome = {}

    def entry_under_a():
        fence = a._fence(a.owned[rig.account])
        with account_fence.active(fence):
            try:
                outcome["report"] = cert.run(rig.db, rig.account, "prep-kill", action="hold")
            except Exception as exc:
                outcome["error"] = exc

    # cert.run asks the runtime lease itself (an operator harness); the boundary's own check is the fence.
    rig.lease["held"] = True
    dying = threading.Thread(target=entry_under_a, daemon=True)
    dying.start()
    assert point.reached.wait(10)
    claimed = until(lambda: holder(rig) == "B", 5, between=lambda: rig.round(b))
    assert claimed is not None and claimed <= TTL_MS / 1000 + INTERVAL_S + SLACK_S
    until(lambda: not b.owned[rig.account].needs_recovery, 5, between=lambda: rig.round(b))
    point.release()
    dying.join(30)
    assert isinstance(outcome.get("error"), AssignmentLost), outcome
    assert rig.state["create_count"] == 0 and rig.accepted == []
    with rig.db.connect() as c:                                 # refused before the attempt claim: no CREATE intent
        assert c.execute("SELECT COUNT(*) FROM cati_execution_attempts").fetchone()[0] == 0
    MEASUREMENTS.append({"scenario": "kill_at:order_preparation", "dialect": rig.platform.dialect_name,
                         "ttl_ms": TTL_MS, "loop_interval_ms": int(INTERVAL_S * 1000),
                         "kill_to_claim_ms": round(claimed * 1000)})


def test_unknown_submission_is_resolved_by_the_successor_without_a_second_create(rig):
    """A's entry reaches the venue but the acknowledgement is lost (SUBMIT_UNKNOWN), and A dies before it can
    reconcile. B takes over: its recovery resolves the attempt from venue truth and protects the position --
    exactly one CREATE, a protected position, no lost durable intent."""
    a, b = rig.worker("A"), rig.worker("B")
    rig.round(a)
    place = rig.h.client.place_order.side_effect

    def lost_ack(req):
        place(req)
        raise TimeoutError("acknowledgement lost")

    rig.h.client.place_order.side_effect = lost_ack
    with account_fence.active(a._fence(a.owned[rig.account])):
        try:
            cert.run(rig.db, rig.account, "unknown-submit", action="hold")
        except Exception:
            pass                                            # A dies here, whatever the harness reported
    rig.h.client.place_order.side_effect = place
    assert rig.state["create_count"] == 1 and rig.state["qty"] > 0
    with rig.db.connect() as c:
        statuses = [r[0] for r in c.execute("SELECT status FROM cati_execution_attempts ORDER BY recorded_at, sequence")]
    assert statuses[-1] in ("SUBMIT_UNKNOWN", "PENDING_SUBMIT"), statuses
    rig.lease["held"] = False                               # A is dead; B is an assigned worker
    claimed = until(lambda: holder(rig) == "B", 5, between=lambda: rig.round(b))
    assert claimed is not None
    until(lambda: rig.account in b.owned and not b.owned[rig.account].needs_recovery, 10,
          between=lambda: rig.round(b))
    assert not b.owned[rig.account].needs_recovery
    with rig.db.connect() as c:
        latest = c.execute("SELECT status FROM cati_execution_attempts ORDER BY recorded_at DESC, sequence DESC").fetchone()[0]
    assert latest in ("RECONCILED_POSITION_EXISTS", "FILLED"), latest
    assert rig.state["create_count"] == 1 and len(rig.state["legs"]) == 2       # protected, never re-sent
    assert len(accepted_cids(rig)) == len(set(accepted_cids(rig)))
    assert [x for x in rig.accepted if x["kind"] == "PROTECTION"] and \
        all(x["worker"] == "B" for x in rig.accepted if x["kind"] == "PROTECTION")


# ── every mutation point refuses a stale fence before the venue ────────────────────────────────────────────
def test_a_stale_fence_is_refused_at_every_mutation_point(rig):
    from app.execution.production_close import close_position
    from app.execution.production_protection import cancel_flat_protection, place_native_protection
    from app.models.unified_trading import ProtectionRequest, Side
    from decimal import Decimal
    from shared_lib.core.production import require_broker_mutation_permission
    from app.trading_intelligence.trade_plan.evidence_store import TradePlanEvidenceStore
    report = rig.hold_position()
    plan = TradePlanEvidenceStore(rig.db).load_plan(rig.account, report["trade_plan_id"])
    identity = f"{plan.trade_plan_id}|{plan.trade_plan_hash}"
    a = rig.worker("A")
    rig.round(a)
    stale = a._fence(a.owned[rig.account])
    # B preempts (emergency path) -- A's token is now old, its claim unexpired: only fencing can stop it.
    taken = WorkerAssignments(rig.platform, ttl_ms=TTL_MS).preempt(rig.account, "B-emergency", "TEST")
    assert taken.fencing_token == stale.fencing_token + 1
    client = rig.h.client
    client._production_db, client._production_account_id = rig.db, rig.account
    client._production_intent_identity = identity
    before = (len(rig.accepted), rig.state["create_count"], list(rig.state["legs"]))
    request = ProtectionRequest(symbol="ADAUSDT", position_side=Side.BUY, qty=Decimal("1"), sl_price="90", tp_price="110")
    with account_fence.active(stale):
        with pytest.raises(AssignmentLost):                                    # (a) entry at the boundary
            cert.run(rig.db, rig.account, "stale-entry", action="hold")
        with pytest.raises(AssignmentLost):                                    # (b) protection create
            place_native_protection(client, request)
        qty, rig.state["qty"] = rig.state["qty"], 0.                          # (b) protection cancel: needs a
        try:                                                                   #     flat account to get as far
            with pytest.raises(AssignmentLost):
                cancel_flat_protection(client, identity)
        finally:
            rig.state["qty"] = qty
        with pytest.raises(AssignmentLost):                                    # reduce-only close
            close_position(client, "ADAUSDT")
        with pytest.raises(AssignmentLost):                                    # (d) account management
            production.maintain_only(rig.db, rig.h.account, client, boundary_factory=rig.h.boundary_for)
        with pytest.raises(AssignmentLost):                                    # any transport mutation
            require_broker_mutation_permission("POST", "/fapi/v1/algoOrder", environment="DEMO", broker="binance",
                                               base_url=client.base_url, client=client, payload={})
        with pytest.raises(AssignmentLost):                                    # the boundary's own check
            account_fence.entry_refusal(rig.db, rig.account)
        assert not account_fence.permits(rig.account)                          # (c) reservation release
        with pytest.raises(AssignmentLost):
            rig.h.boundary_for()._release_unsubmitted(rig.h.plan, rig.h.now)
    assert (len(rig.accepted), rig.state["create_count"], list(rig.state["legs"])) == before


def test_outside_a_fence_the_boundary_refuses_an_account_another_worker_holds(rig):
    a = rig.worker("A", ttl_ms=60_000)                                  # A stays alive for the whole test
    rig.round(a)                                                         # A (assigned) holds the account
    owner = rig.worker("owner-1", mode=OWNER)
    broker_assignment.install(owner)                                      # this process: the runtime owner
    assert account_fence.entry_refusal(rig.db, rig.account) == account_fence.HELD_BY_ANOTHER_WORKER
    assert not account_fence.runtime_authority(rig.db, rig.account)
    report = cert.run(rig.db, rig.account, "outside-fence", action="hold")  # the real boundary, no fence
    assert report.get("status") != "PROTECTED" and rig.state["create_count"] == 0 and rig.accepted == []
    rig.lease["held"] = False
    assert account_fence.entry_refusal(rig.db, rig.account) == account_fence.LEASE_REQUIRED
    rig.lease["held"] = True
    a.drain()
    assert account_fence.entry_refusal(rig.db, rig.account) is None       # nobody else holds it
    owner.assignments.db = _unreachable_platform(rig)
    assert account_fence.entry_refusal(rig.db, rig.account) == account_fence.ASSIGNMENT_UNREADABLE


def test_an_expired_fence_is_never_revived(rig):
    a = rig.worker("A")
    rig.round(a)
    fence = a._fence(a.owned[rig.account])
    time.sleep(TTL_MS / 1000 + 0.2)                 # nobody took it, but the claim is over
    with pytest.raises(AssignmentLost) as lost:
        fence.verify()
    assert lost.value.reason == "ASSIGNMENT_EXPIRED"
    assert holder(rig) is None


# ── isolation between workers and modes ──────────────────────────────────────────────────────────────────────
def _second_account(rig, account_id="acct-b", user_id="bob"):
    client = Mock()
    client.broker_environment = rig.h.client.broker_environment
    client.base_url = rig.h.client.base_url
    client._broker_account_id = account_id
    client.account.return_value = {"totalWalletBalance": "1000", "totalMarginBalance": "1000",
                                   "availableBalance": "1000", "totalInitialMargin": "0", "totalUnrealizedProfit": "0"}
    client.position_risk.return_value = [{"symbol": "ADAUSDT", "positionAmt": "0", "positionSide": "BOTH"}]
    client.open_orders.return_value = []
    with rig.db.connect() as c:
        c.execute("INSERT INTO broker_accounts (id,user_id,broker_id,market_type,environment,status,created_at,updated_at) "
                  "VALUES(?,?,'binance','crypto','demo','connected','2026-10-29','2026-10-29')", (account_id, user_id))
    rig.clients[account_id] = client
    return client


def test_workers_never_touch_each_others_accounts(rig):
    second = _second_account(rig)
    seen = {}
    for account_id, client in rig.clients.items():
        for name in ("account", "position_risk", "open_orders"):
            mock = getattr(client, name)
            original = mock.side_effect

            def spy(*a, _account=account_id, _original=original, **kw):
                fence = account_fence.current()
                seen.setdefault(_account, set()).add(fence.worker_id if fence else None)
                return _original(*a, **kw) if callable(_original) else DEFAULT

            mock.side_effect = spy
    rig.lease["held"] = False
    a, b = rig.worker("A"), rig.worker("B")
    for _ in range(3):
        rig.round(a)
        rig.round(b)
    owned_a, owned_b = set(a.owned), set(b.owned)
    assert owned_a and owned_b and not owned_a & owned_b and owned_a | owned_b == {rig.account, "acct-b"}
    assert {acct: workers for acct, workers in seen.items()} == {
        acct: {"A" if acct in owned_a else "B"} for acct in seen}
    # A step of one account can never mutate another account through the transport.
    from shared_lib.core.production import require_broker_mutation_permission
    mine = next(iter(owned_a))
    other_client = rig.clients[next(iter(owned_b))]
    with account_fence.active(a._fence(a.owned[mine])):
        with pytest.raises(AssignmentLost):
            require_broker_mutation_permission("POST", "/fapi/v1/algoOrder", environment="DEMO", broker="binance",
                                               base_url=other_client.base_url, client=other_client, payload={})


def test_owner_mode_and_assigned_mode_never_process_one_account_together(rig):
    _second_account(rig)
    _second_account(rig, "acct-c", "carol")
    assigned, owner = rig.worker("assigned-1"), rig.worker("owner-1", mode=OWNER)
    rig.round(assigned)                                      # the assigned worker holds its fair share first
    assert len(assigned.owned) == 2
    stop = threading.Event()
    errors = []

    def loop(layer):
        while not stop.is_set():
            try:
                rig.round(layer)
            except Exception as exc:  # pragma: no cover - surfaced below
                errors.append(exc)
            time.sleep(0.02)

    threads = [threading.Thread(target=loop, args=(layer,), daemon=True) for layer in (assigned, owner)]
    for t in threads:
        t.start()
    time.sleep(2.5)
    assigned.drain()                                         # a rolling deploy of the assigned worker
    time.sleep(1.5)
    stop.set()
    for t in threads:
        t.join(30)
    assert not errors, errors
    by_account = {}
    for worker_id, account_id, started, ended, outcome, _ in rig.steps:
        by_account.setdefault(account_id, []).append((started, ended, worker_id))
    for account_id, spans in by_account.items():
        spans.sort()
        for (s1, e1, w1), (s2, e2, w2) in zip(spans, spans[1:]):
            assert w1 == w2 or e1 <= s2, (account_id, w1, w2)     # never two workers on one account at once
    assert {s[0] for s in rig.steps} == {"assigned-1", "owner-1"}
    assert set(owner.owned) == {rig.account, "acct-b", "acct-c"}  # the owner took the drained accounts over


# ── emergency flatten ─────────────────────────────────────────────────────────────────────────────────────────
def test_emergency_flatten_preempts_a_live_holder_and_nothing_reopens(rig, monkeypatch):
    from app.trading_intelligence.governance.promotion import PromotionGovernance
    flatten_account = production.flatten_account               # the harness boundary, as for every other step
    monkeypatch.setattr(production, "flatten_account", lambda db, account, client, **kw: flatten_account(
        db, account, client, boundary_factory=rig.h.boundary_for, **kw))
    rig.hold_position()
    rig.lease["held"] = False
    a = rig.worker("A", ttl_ms=60_000)                           # a LIVE holder: only preemption can take it
    rig.round(a)
    rig.round(a)
    stale = a._fence(a.owned[rig.account])
    owner = rig.worker("owner-1", mode=OWNER)
    broker_assignment.install(owner)
    rig.lease["held"] = True                                  # the flatten runs in the runtime owner
    results = runtime.flatten(rig.db, account_id=rig.account, request_id="req-1", factory=lambda auth: rig.h.client)
    assert results and results[0]["status"] in ("closed", "submitted"), results
    assert "PROTECTION_CLEANUP_PENDING" not in results[0]["detail"], results
    assert rig.state["qty"] == 0 and rig.state["legs"] == []          # closed, protection legs cancelled
    reasons = [r[3] for r in history(rig)]
    assert "EMERGENCY_PREEMPT:EMERGENCY_FLATTEN" in reasons and reasons[-1] == "EMERGENCY_DONE"
    assert holder(rig) is None                                         # released after the flatten
    assert all(x["worker"] == "emergency:req-1" for x in rig.accepted if x["kind"] in ("CLOSE", "CANCEL"))
    with account_fence.active(stale):                                  # A's fence is dead
        with pytest.raises(AssignmentLost):
            stale.verify()
    assert PromotionGovernance(rig.db).kill_switch_on(scope=rig.account)
    # The next owner claims it as NEW and recovers first; the kill switch refuses every new entry.
    rig.lease["held"] = False
    rig.round(a)
    assert holder(rig) == "A" and not a.owned[rig.account].needs_recovery
    assert [s for s in rig.steps if s[0] == "A"][-1][5] is True        # recovery-only step
    rig.round(a)                                                       # the next, entry-capable step
    assert state_doc(rig)["execution"].get("orders_submitted") is not True
    assert rig.state["create_count"] == 1 and rig.state["qty"] == 0
    with account_fence.active(a._fence(a.owned[rig.account])):          # and the boundary's own authority
        assert rig.h.boundary_for().authority.authorize_entry(rig.h.plan) == (False, "CATI_NEW_ENTRY_KILL_SWITCH")
        rig.lease["held"] = True
        try:
            cert.run(rig.db, rig.account, "after-flatten", action="hold")
        except Exception:
            pass
    assert rig.state["create_count"] == 1 and rig.state["qty"] == 0      # nothing reopened


# ── graceful drain, removal, credential rotation ─────────────────────────────────────────────────────────────
def test_drain_hands_the_accounts_over_immediately(rig):
    rig.lease["held"] = False
    a, b = rig.worker("A", ttl_ms=60_000), rig.worker("B", ttl_ms=60_000)
    rig.round(a)
    assert holder(rig) == "A"
    started = time.monotonic()
    assert a.drain() == 1
    rig.round(b)
    handover = time.monotonic() - started
    assert holder(rig) == "B" and handover < 5                          # far below the 60 s TTL
    assert [r[3] for r in history(rig)][-2:] == ["DRAIN", "INITIAL"] or "DRAIN" in [r[3] for r in history(rig)]
    with rig.platform.transaction(write=False) as conn:
        assert conn.scalar("SELECT state FROM platform_workers WHERE worker_id='A'") == "STOPPED"
    rig.round(a)
    assert "A" not in {s[0] for s in rig.steps[-1:]}                     # a drained worker claims nothing
    MEASUREMENTS.append({"scenario": "drain_handover", "dialect": rig.platform.dialect_name, "ttl_ms": 60_000,
                         "drain_to_successor_claim_ms": round(handover * 1000)})


def test_a_stop_drains_through_wait_idle(rig):
    a = rig.worker("A", mode=OWNER)
    broker_assignment.install(a)
    rig.round(a)
    assert holder(rig) == "A"
    runtime_shutdown.request_stop()
    assert runtime.wait_idle(1)
    assert holder(rig) is None and "DRAIN" in [r[3] for r in history(rig)]
    assert a.begin_round(runtime.execution_accounts(rig.db)) == []


def test_a_removed_account_is_released_and_its_holder_stops(rig):
    rig.lease["held"] = False
    a = rig.worker("A")
    rig.round(a)
    fence = a._fence(a.owned[rig.account])
    with rig.db.connect() as c:
        c.execute("UPDATE broker_accounts SET status='disconnected' WHERE id=?", (rig.account,))
    rig.round(a)
    assert holder(rig) is None and rig.account not in a.owned
    assert history(rig)[-1][3] == "ACCOUNT_REMOVED"
    with pytest.raises(AssignmentLost):
        fence.verify()
    with rig.platform.transaction(write=False) as conn:
        assert conn.scalar("SELECT state FROM platform_accounts WHERE account_id=?", (rig.account,)) == "REMOVED"


def test_credential_rotation_rebuilds_the_client_and_keeps_the_assignment(rig, monkeypatch):
    built = []

    def factory(auth):
        built.append(auth.credential_version)
        return rig.h.client

    # The runtime's own client cache (it caches only the production factory's clients, keyed on the credentials).
    monkeypatch.setattr(runtime, "_DEFAULT_CLIENT_FACTORY", factory)
    monkeypatch.setattr(runtime, "sync_account", lambda db, account, **kw: REAL_SYNC_ACCOUNT(
        db, account, factory=factory, **kw))
    rig.lease["held"] = False
    a = rig.worker("A")
    rig.round(a)
    rig.round(a)
    token = a.owned[rig.account].fencing_token
    assert built == [1]                                    # cached across steps of the same credentials
    rig.versions[rig.account] = 2                          # the customer rotated the API key
    rig.round(a)
    assert built == [1, 2]                                 # rebuilt on the new credentials
    assert holder(rig) == "A" and a.owned[rig.account].fencing_token == token


# ── configuration, roles, failure of the platform database ────────────────────────────────────────────────────
def test_configuration_paths(monkeypatch, tmp_path):
    settings = SimpleNamespace(BROKER_CYCLE_ASSIGNMENT_MODE="owner", BROKER_ASSIGNMENT_TTL_MS=60_000,
                               BROKER_ASSIGNMENT_FENCE_MARGIN_MS=15_000)
    monkeypatch.delenv("PROCESS_ROLE", raising=False)
    try:
        assert broker_assignment.configure(env={}, settings_obj=settings) is None        # today's path
        settings.BROKER_CYCLE_ASSIGNMENT_MODE = "assigned"
        refused = broker_assignment.configure(env={}, settings_obj=settings)
        assert refused.refused == broker_assignment.PLATFORM_DATABASE_REQUIRED and not refused.enforced
        settings.BROKER_CYCLE_ASSIGNMENT_MODE = "bogus"
        assert broker_assignment.configure(env={}, settings_obj=settings).refused == broker_assignment.MODE_INVALID
        settings.BROKER_CYCLE_ASSIGNMENT_MODE = "owner"
        url = f"sqlite:///{(tmp_path / 'p.db').as_posix()}"
        layer = broker_assignment.configure(env={"PLATFORM_DATABASE_URL": url}, settings_obj=settings)
        assert layer.enforced and layer.mode == OWNER and broker_assignment.installed() is layer
        monkeypatch.setenv("PROCESS_ROLE", "api")
        assert broker_assignment.configure(env={"PLATFORM_DATABASE_URL": url}, settings_obj=settings) is None
        with pytest.raises(ValueError, match="PROCESS_ROLE_API_CLAIMS_NO_ACCOUNT"):
            BrokerCycleAssignments(Database(url), worker_id="api-worker").register()
    finally:
        broker_assignment.install(None)


def test_an_api_role_process_never_runs_the_broker_cycle(monkeypatch):
    import asyncio
    monkeypatch.setenv("PROCESS_ROLE", "api")
    monkeypatch.setattr(runtime, "settings", SimpleNamespace(production=True))
    with pytest.raises(ValueError, match="PROCESS_ROLE_API_RUNS_NO_BROKER_CYCLE"):
        asyncio.run(runtime.run("db"))


def test_assigned_mode_without_a_platform_database_does_no_account_work(rig):
    refusing = BrokerCycleAssignments.refusing(ASSIGNED, broker_assignment.PLATFORM_DATABASE_REQUIRED)
    rig.lease["held"] = True
    assert rig.round(refusing) is None
    assert rig.steps == [] and runtime._progress["assignment_refused"] == broker_assignment.PLATFORM_DATABASE_REQUIRED
    rig.h.client.account.assert_not_called()


def test_an_unreadable_platform_database_stops_account_work_in_owner_mode(rig):
    owner = rig.worker("owner-1", mode=OWNER)
    rig.round(owner)
    calls = rig.h.client.account.call_count
    owner.assignments.db = _unreachable_platform(rig)
    with pytest.raises(Exception):
        rig.round(owner)
    assert rig.h.client.account.call_count == calls       # no account step without the assignment table


def _unreachable_platform(rig):
    """A platform database that cannot be opened (a path below a directory that does not exist)."""
    import tempfile
    from pathlib import Path
    return Database("sqlite:///" + (Path(tempfile.gettempdir()) / f"no-such-dir-{uuid.uuid4().hex}" / "p.db").as_posix())


def test_the_job_runner_runs_after_the_cycle_with_this_workers_fences(rig, monkeypatch):
    """The real ``runtime_jobs.RuntimeJobRunner`` (agent C), built by the layer with ``assignment_fence``: work is
    scheduled for exactly this worker's accounts, run outside the cycle lock, and no fence is handed out for an
    account before its takeover recovery or after the account was taken away."""
    from app.platform_workers import runtime_jobs
    seen = {"schedule": [], "run": []}
    built = {}
    original_init = runtime_jobs.RuntimeJobRunner.__init__

    def init(self, legacy_db, platform_db, *, worker_id, fence_for_account, **kw):
        built.update(legacy=legacy_db, platform=platform_db, worker=worker_id, fence=fence_for_account)
        original_init(self, legacy_db, platform_db, worker_id=worker_id, fence_for_account=fence_for_account, **kw)

    monkeypatch.setattr(runtime_jobs.RuntimeJobRunner, "__init__", init)
    monkeypatch.setattr(runtime_jobs.RuntimeJobRunner, "schedule", lambda self, accounts=(), **kw: seen["schedule"].append(
        list(accounts)) or {"enqueued": 0, "errors": []})
    monkeypatch.setattr(runtime_jobs.RuntimeJobRunner, "run_once", lambda self, limit=10: seen["run"].append(
        (limit, runtime._cycle_lock._is_owned())) or [])
    a = rig.worker("A")
    a.begin_round(runtime.execution_accounts(rig.db))      # claimed, NOT yet recovered
    runtime._run_jobs(rig.db, a)
    assert built["fence"](rig.account) is None              # no job before the takeover recovery
    rig.round(a)                                            # claim + recovery
    runtime._run_jobs(rig.db, a)
    assert built["worker"] == "A" and built["platform"] is rig.platform
    assert seen["schedule"][-1] == [(rig.account, rig.h.account["user_id"])]
    assert seen["run"][-1] == (10, False)                   # outside the cycle lock
    assert built["fence"](rig.account) == (rig.account, "A", a.owned[rig.account].fencing_token)
    WorkerAssignments(rig.platform, ttl_ms=TTL_MS).preempt(rig.account, "someone-else", "TEST")
    assert built["fence"](rig.account) is None              # the live claim decides, not the cached one
    assert account_fence.fence_for(rig.account) is None     # not this process's installed layer
    a.drain()
    assert a._job_runner[1].stopping                        # drain stops the job runner


def test_invalid_lease_settings_refuse_with_their_own_reason(monkeypatch, tmp_path):
    """Section K4 (WORKER_LEASE_SECONDS): the assignment TTL must be positive and the fence margin below it; the
    pool size is an integer from 1 to 64. A bad value is refused with its own code, never reported as a database
    failure and never replaced silently."""
    from types import SimpleNamespace
    from pydantic import ValidationError
    from app.core.config import Settings
    monkeypatch.delenv("PROCESS_ROLE", raising=False)
    url = f"sqlite:///{(tmp_path / 'p.db').as_posix()}"
    try:
        for ttl, margin in ((60_000, 60_000), (10_000, 20_000)):
            settings = SimpleNamespace(BROKER_CYCLE_ASSIGNMENT_MODE="owner", BROKER_ASSIGNMENT_TTL_MS=ttl,
                                       BROKER_ASSIGNMENT_FENCE_MARGIN_MS=margin)
            layer = broker_assignment.configure(env={"PLATFORM_DATABASE_URL": url}, settings_obj=settings)
            assert layer.refused == broker_assignment.FENCE_MARGIN_INVALID and not layer.enforced
        settings = SimpleNamespace(BROKER_CYCLE_ASSIGNMENT_MODE="owner", BROKER_ASSIGNMENT_TTL_MS=60_000,
                                   BROKER_ASSIGNMENT_FENCE_MARGIN_MS=15_000)
        for pool in ("four", "0", "500"):
            layer = broker_assignment.configure(env={"PLATFORM_DATABASE_URL": url, "PLATFORM_DB_POOL_SIZE": pool},
                                                settings_obj=settings)
            assert layer.refused == broker_assignment.POOL_SIZE_INVALID
    finally:
        broker_assignment.install(None)
    for bad in ({"BROKER_ASSIGNMENT_TTL_MS": 0}, {"BROKER_ASSIGNMENT_TTL_MS": -5},
                {"BROKER_ASSIGNMENT_FENCE_MARGIN_MS": -1}):
        with pytest.raises(ValidationError):
            Settings(_env_file=None, APP_ENV="TEST", **bad)
