"""Step 3.13: per-account leases, the bounded account runner and preemptive emergency control.

Every concurrency claim here is proved with barriers and events, never with sleeps: a schedule either
happens or the test fails on a timeout. Exchanges are fakes; nothing leaves the process (one test starts
child processes that open the same temporary database, to prove the lease holds across processes).
"""
from __future__ import annotations

import contextvars
import json
import os
import sqlite3
import subprocess
import sys
import threading
import time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import _step2
import _step3 as S
from app.api import emergency as emergency_api
from app.core import config
from app.core.auth import require_admin_emergency
from app.core.config import Settings
from app.ops import runtime_shutdown
from app.ops.runtime_supervisor import Limits
from app.trading_intelligence.families.daily_trend import spec
from app.trading_intelligence.families.daily_trend import targets as T
from app.trading_intelligence.governance.promotion import GovernanceAuthority, PromotionGovernance
from app.trading_intelligence.integration import production_execution, production_runtime as runtime
from app.trading_intelligence.pipeline import contracts as C
from app.trading_intelligence.pipeline import coordination as K
from app.trading_intelligence.pipeline import daily_trend as DT
from app.trading_intelligence.pipeline import driver as D
from app.trading_intelligence.pipeline import family_registry as FR
from app.trading_intelligence.pipeline.accounts import CombinedAccounts, ConnectedAccounts, ModelAccounts
from app.trading_intelligence.pipeline.market_data import FrozenPanelBars

WAIT = 20                                   # seconds a schedule may take before the test is declared stuck
EQUITY = spec.INTERPRETATION["initial_equity_usdt"]
START = DT.day_of("2020-09-01")
BACKEND = Path(runtime.__file__).resolve().parents[3]
SHARED = BACKEND.parent / "shared"


@pytest.fixture
def db(tmp_path):
    return S.fresh_db(tmp_path)


class Clock:
    def __init__(self, now=1_000_000):
        self.now = now

    def __call__(self):
        return self.now


def plan(account_id):
    return SimpleNamespace(environment="DEMO", broker_account_id=account_id, venue="BINANCE_USDM")


def in_thread(target, *args, **kwargs):
    box = {}

    def body():
        try:
            box["value"] = target(*args, **kwargs)
        except BaseException as exc:  # noqa: BLE001 -- handed to the asserting thread
            box["error"] = exc

    thread = threading.Thread(target=body, daemon=True)
    thread.start()
    return thread, box


def finished(thread, box):
    thread.join(WAIT)
    assert not thread.is_alive(), "the schedule did not finish"
    if "error" in box:
        raise box["error"]
    return box.get("value")


# ═══════════════════════════════════════════════════════════════════════════════════════════ leases
def test_exactly_one_of_many_threads_takes_an_account_and_every_other_is_told_who_holds_it(db):
    leases, barrier, out = K.AccountLeases(db), threading.Barrier(8), [None] * 8

    def contend(n):
        barrier.wait(WAIT)                               # eight separate connections ask at the same instant
        out[n] = leases.acquire("acct", f"worker-{n}")

    threads = [threading.Thread(target=contend, args=(n,)) for n in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(WAIT)
    winners = [o for o in out if o]
    assert len(winners) == 1 and winners[0].fencing_token == 1 and winners[0].takeover is None
    for denied in (o for o in out if not o):
        assert (denied.reason, denied.holder, denied.holder_purpose) == (K.LEASE_HELD, winners[0].owner_id, K.ORDINARY)
        assert denied.retry_after_ms > 0
    assert leases.holder("acct")["owner_id"] == winners[0].owner_id
    assert leases.release(winners[0]) and leases.holder("acct") is None
    assert leases.acquire("acct", "next").fencing_token == 2          # the token never goes back


def test_the_lease_holds_between_processes(db):
    """Four interpreters, one database, twelve accounts: every account has exactly one holder."""
    code = ("import sys\n"
            "from shared_lib.persistence.db import DB\n"
            "from app.trading_intelligence.pipeline.coordination import AccountLeases\n"
            "leases = AccountLeases(DB(sys.argv[1]))\n"
            "print(','.join(a for a in (f'acct-{n}' for n in range(12)) if leases.acquire(a, sys.argv[2], ttl_ms=600000)))\n")
    env = {**os.environ, "PYTHONPATH": os.pathsep.join([str(BACKEND), str(SHARED)])}
    children = [subprocess.Popen([sys.executable, "-c", code, db.path, f"process-{n}"], env=env, stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE, text=True) for n in range(4)]
    won = []
    for child in children:
        stdout, stderr = child.communicate(timeout=120)
        assert child.returncode == 0, stderr[-2000:]
        won += [a for a in stdout.strip().split(",") if a]
    assert sorted(won) == sorted(f"acct-{n}" for n in range(12))       # each account once, none twice, none lost
    with db.connect() as conn:
        owners = dict(conn.execute("SELECT account_id, owner_id FROM cati_account_leases"))
    assert len(owners) == 12 and set(owners.values()) <= {f"process-{n}" for n in range(4)}


def test_a_lease_is_renewed_released_and_ends_by_time(db):
    clock = Clock()
    leases = K.AccountLeases(db, clock=clock)
    lease = leases.acquire("acct", "a", ttl_ms=1_000)
    assert not leases.acquire("acct", "a", ttl_ms=1_000)             # not re-entrant: one owner id, still one holder
    clock.now += 900
    lease = leases.renew(lease, ttl_ms=1_000)
    assert lease.expires_at == clock.now + 1_000
    clock.now += 999
    leases.verify(lease)
    clock.now += 1                                                    # its time ran out
    with pytest.raises(K.LeaseLost, match="LEASE_EXPIRED"):
        leases.verify(lease)
    with pytest.raises(K.LeaseLost, match="LEASE_EXPIRED"):
        leases.renew(lease)                                           # never revived, even though nobody took it
    assert leases.holder("acct") is None
    for bad in ({"ttl_ms": 0}, {"purpose": "VIP"}):
        with pytest.raises(ValueError):
            leases.acquire("acct", "a", **bad)


def test_a_dead_holders_account_is_taken_over_and_its_late_writes_are_fenced_off(db):
    clock = Clock()
    leases = K.AccountLeases(db, clock=clock)
    dead = leases.acquire("acct", "crashed-worker", ttl_ms=60_000)
    clock.now += 59_999
    assert not leases.acquire("acct", "recovery")                     # still within its time
    clock.now += 1
    taken = leases.acquire("acct", "recovery")
    assert (taken.takeover, taken.previous_owner, taken.fencing_token) == (K.STALE, "crashed-worker", 2)
    with pytest.raises(K.LeaseLost, match="LEASE_TAKEN_OVER") as lost:
        leases.verify(dead)
    assert lost.value.holder == "recovery" and lost.value.code == "ACCOUNT_LEASE_LOST"
    assert leases.release(dead) is False                              # a late release frees nobody else's lease
    assert leases.holder("acct")["owner_id"] == "recovery"
    assert leases.release(taken) and leases.release(taken) is False


def test_a_fencing_check_inside_the_mutations_own_transaction_rolls_the_mutation_back(db):
    leases = K.AccountLeases(db)
    old = leases.acquire("acct", "ordinary")
    leases.acquire("acct", "emergency:r1", purpose=K.EMERGENCY)       # taken over while the work was thinking
    with db.connect() as conn:
        conn.execute("CREATE TABLE work (account_id TEXT, step INTEGER)")
    with pytest.raises(K.LeaseLost):
        with db.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute("INSERT INTO work VALUES ('acct', 1)")
            leases.guard(old).check_on(conn)                          # under the write lock: nothing can overtake it
    with db.connect() as conn:
        assert conn.execute("SELECT COUNT(*) FROM work").fetchone()[0] == 0


def test_an_emergency_takes_the_lease_with_priority_and_ordinary_work_cannot_take_it_back(db):
    leases = K.AccountLeases(db)
    ordinary = leases.acquire("acct", "cycle-worker")
    emergency = leases.acquire("acct", "emergency:req-1", purpose=K.EMERGENCY)
    assert (emergency.takeover, emergency.previous_owner, emergency.fencing_token) == (K.PREEMPTED, "cycle-worker", 2)
    with pytest.raises(K.LeaseLost, match="LEASE_TAKEN_OVER"):
        leases.verify(ordinary)
    denied = leases.acquire("acct", "cycle-worker-2")
    assert (denied.reason, denied.holder_purpose) == (K.LEASE_HELD, K.EMERGENCY)
    assert not leases.acquire("acct", "emergency:req-2", purpose=K.EMERGENCY)    # one emergency at a time per account
    assert leases.release(emergency) and leases.acquire("acct", "cycle-worker-2").fencing_token == 3


# ══════════════════════════════════════════════════════════════════════════════════ emergency control
def test_a_request_switches_its_scopes_kill_switch_on_and_records_who_when_and_why(db):
    control, gov = K.EmergencyControl(db, clock=Clock(5_000)), PromotionGovernance(db)
    one = control.request(scope=K.ACCOUNT, account_id="acct-a", requested_by="admin:7", reason="runaway position",
                          request_id="req-a")
    assert (one["scope"], one["account_id"], one["requested_by"], one["reason"], one["state"], one["requested_at"]) == (
        K.ACCOUNT, "acct-a", "admin:7", "runaway position", K.REQUESTED, 5_000)
    assert (one["hold_scope"], one["hold_raised"], one["active"]) == ("acct-a", True, True)
    assert gov.kill_switch_on(scope="acct-a") and not gov.kill_switch_on() and not gov.kill_switch_on(scope="acct-b")
    assert "req-a" in gov.kill_switch_status("acct-a")["reason"] and gov.kill_switch_status("acct-a")["set_by"] == "admin:7"
    # the control the execution boundary consults refuses this account's entries, and only this account's
    assert GovernanceAuthority(db).authorize_entry(plan("acct-a")) == (False, "CATI_NEW_ENTRY_KILL_SWITCH")
    assert GovernanceAuthority(db).authorize_entry(plan("acct-b"))[1] != "CATI_NEW_ENTRY_KILL_SWITCH"
    assert control.entries_blocked("acct-a") == "EMERGENCY_FLATTEN_ACTIVE" and control.entries_blocked("acct-b") is None

    everything = control.request(scope=K.ALL, requested_by="admin:7", reason="incident", request_id="req-all")
    assert (everything["hold_scope"], everything["hold_raised"]) == ("GLOBAL", True) and gov.kill_switch_on()
    again = control.request(scope=K.ALL, requested_by="admin:9", reason="second request", request_id="req-all-2")
    assert again["hold_raised"] is False                              # it was already on: recorded as found
    assert control.active_for("acct-b")["request_id"] == "req-all" and control.entries_blocked("acct-b")
    assert [e["event"] for e in control.events("req-a")] == [K.REQUESTED]
    assert control.events("req-a")[0]["detail"]["hold_raised"] is True


def test_a_request_is_idempotent_and_an_invalid_one_changes_nothing(db):
    control = K.EmergencyControl(db)
    first = control.request(scope=K.ACCOUNT, account_id="a", requested_by="op", reason="drill", request_id="r1")
    assert control.request(scope=K.ACCOUNT, account_id="a", requested_by="other", reason="replay", request_id="r1") == first
    with pytest.raises(K.EmergencyError, match="REUSED"):
        control.request(scope=K.ALL, requested_by="op", reason="drill", request_id="r1")
    for bad in ({"scope": "ACCOUNT"}, {"scope": "ALL", "account_id": "a"}, {"scope": "EVERYONE"},
                {"scope": "ACCOUNT", "account_id": "GLOBAL"}, {"scope": "ALL", "reason": "x"},
                {"scope": "ALL", "requested_by": " "}):
        with pytest.raises(K.EmergencyError):
            control.request(**{"requested_by": "op", "reason": "drill", **bad})
    assert not PromotionGovernance(db).kill_switch_on()                # no invalid request raised the platform switch
    with db.connect() as conn:
        assert conn.execute("SELECT COUNT(*) FROM cati_emergency_requests").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM cati_emergency_events").fetchone()[0] == 1


def test_the_emergency_log_is_append_only(db):
    K.EmergencyControl(db).request(scope=K.ALL, requested_by="op", reason="drill", request_id="r1")
    for statement in ("UPDATE cati_emergency_events SET actor_ref='nobody'", "DELETE FROM cati_emergency_events"):
        with pytest.raises(sqlite3.DatabaseError, match="append-only"):
            with db.connect() as conn:
                conn.execute(statement)


def test_ordinary_work_is_refused_while_a_request_is_active_and_only_for_its_scope(db):
    clock = Clock()
    control, leases = K.EmergencyControl(db, clock=clock), K.AccountLeases(db, clock=clock)
    control.request(scope=K.ACCOUNT, account_id="a", requested_by="op", reason="drill", request_id="r1")
    refused = leases.acquire("a", "worker")
    assert (refused.reason, refused.holder, refused.holder_purpose) == (K.EMERGENCY_ACTIVE, "emergency:r1", K.EMERGENCY)
    assert leases.acquire("b", "worker")                               # another account is untouched
    assert leases.acquire("a", "emergency:r1", purpose=K.EMERGENCY)    # the emergency itself is not refused
    control.begin("r1")
    assert control.get("r1")["state"] == K.IN_PROGRESS and not leases.acquire("a", "worker-2")
    control.finish("r1", outcome=K.COMPLETED, detail={"closed": 1})
    done = control.get("r1")
    assert (done["state"], done["outcome"], done["active"], done["detail"]["closed"]) == (K.DONE, K.COMPLETED, False, 1)
    assert control.active() == [] and control.active_for("a") is None
    assert [e["event"] for e in control.events("r1")] == [K.REQUESTED, "STARTED", "FINISHED"]


def test_the_scope_stays_closed_after_the_flatten_until_an_operator_lifts_its_switch(db):
    control, gov = K.EmergencyControl(db), PromotionGovernance(db)
    control.request(scope=K.ACCOUNT, account_id="a", requested_by="admin:7", reason="incident", request_id="r1")
    control.begin("r1")
    control.finish("r1", outcome=K.COMPLETED)
    # the flatten is over; nothing here, and no passage of time, reopens the account
    assert control.entries_blocked("a") == "CATI_NEW_ENTRY_KILL_SWITCH"
    assert GovernanceAuthority(db).authorize_entry(plan("a")) == (False, "CATI_NEW_ENTRY_KILL_SWITCH")
    status = control.status()
    assert status["active"] == 0 and status["requests"][0]["hold"]["enabled"] is True
    assert status["requests"][0]["hold"]["set_by"] == "admin:7"
    gov.set_kill_switch(False, reason="reviewed and reopened", actor_ref="admin:8", scope="a")   # the operator's act
    assert control.entries_blocked("a") is None and control.status()["requests"][0]["hold"]["enabled"] is False


def test_a_request_whose_executor_died_stops_preempting_but_its_scope_stays_closed(db):
    clock = Clock()
    control, leases = K.EmergencyControl(db, clock=clock), K.AccountLeases(db, clock=clock)
    control.request(scope=K.ALL, requested_by="op", reason="drill", request_id="r1")
    control.begin("r1")
    clock.now += K.EMERGENCY_ACTIVE_TTL_MS - 1
    assert control.heartbeat("r1") and control.active_for("a")
    clock.now += K.EMERGENCY_ACTIVE_TTL_MS                             # no heartbeat for the whole window
    assert control.active() == [] and [r["request_id"] for r in control.stalled()] == ["r1"]
    assert control.get("r1")["stalled"] and leases.acquire("a", "worker")     # evaluation and maintenance resume
    assert control.entries_blocked("a") == "CATI_NEW_ENTRY_KILL_SWITCH"       # entries do not
    observation = K.platform_observation(db, now_ms=clock.now + 60_000)
    assert observation["emergency"]["pending"] == 1
    assert K.platform_faults(observation, Limits()) == ((K.EMERGENCY_REQUEST_STALLED,), ())


def test_an_unreadable_kill_switch_is_never_read_as_open(db, monkeypatch):
    control = K.EmergencyControl(db)
    monkeypatch.setattr(PromotionGovernance, "kill_switch_on", Mock(side_effect=sqlite3.OperationalError("locked")))
    assert control.entries_blocked("a") == "KILL_SWITCH_STATE_UNAVAILABLE"


def test_a_request_is_recorded_even_when_its_switch_could_not_be_set(db, monkeypatch):
    monkeypatch.setattr(PromotionGovernance, "kill_switch_status", Mock(side_effect=sqlite3.OperationalError("locked")))
    record = K.EmergencyControl(db).request(scope=K.ALL, requested_by="op", reason="drill", request_id="r1")
    assert record["hold_raised"] is False and record["detail"]["hold_error"] == "OperationalError" and record["active"]


def test_the_preemption_gate_is_scoped_and_wakes_whoever_waits_for_it():
    gate = K.PreemptionGate()
    assert not gate.pending() and gate.wait_clear(0)
    one = gate.request("r1", "acct-a")
    assert gate.pending() and gate.covers("acct-a") and not gate.covers("acct-b") and not gate.wait_clear(0)
    everything = gate.request("r2")
    assert gate.covers("acct-b") and gate.requests() == [("r1", "acct-a"), ("r2", None)]
    waiter, box = in_thread(gate.wait_clear, WAIT)
    gate.release(one)
    gate.release(everything)
    assert finished(waiter, box) is True and not gate.pending()


# ═══════════════════════════════════════════════════════════════════════════════════════ the runner
def test_concurrency_is_bounded_reached_and_results_keep_their_order():
    limit, barrier, lock, state = 3, threading.Barrier(3), threading.Lock(), {"now": 0, "peak": 0}

    def work(n):
        def call():
            with lock:
                state["now"] += 1
                state["peak"] = max(state["peak"], state["now"])
            barrier.wait(WAIT)                           # passes only when three accounts are in flight together
            with lock:
                state["now"] -= 1
            return n
        return K.AccountTask(f"acct-{n}", call)

    runner = K.BoundedAccountRunner(max_concurrency=limit)
    outcomes = runner.run([work(n) for n in range(9)])
    assert [o.status for o in outcomes] == [K.RUN_DONE] * 9 and [o.value for o in outcomes] == list(range(9))
    assert state["peak"] == limit == runner.stats["max_in_flight"] and runner.stats["workers"] == limit
    assert runner.stats["max_queue_depth"] == 9 and runner.queue_depth() == 0
    with pytest.raises(ValueError):
        K.BoundedAccountRunner(max_concurrency=0)
    assert K.BoundedAccountRunner().max_concurrency == K.SHADOW_CYCLE_CONCURRENCY == 1


def test_one_account_is_never_in_two_workers_in_one_process():
    lock, inside, overlaps, order = threading.Lock(), set(), [], []

    def work(account, n):
        def call():
            with lock:
                if account in inside:
                    overlaps.append(account)
                inside.add(account)
            for _ in range(200):                         # give another worker every chance to come in
                pass
            with lock:
                inside.discard(account)
                order.append((account, n))
        return K.AccountTask(account, call)

    tasks = [work(f"acct-{n % 3}", n) for n in range(30)]                # ten tasks for each of three accounts
    outcomes = K.BoundedAccountRunner(max_concurrency=6).run(tasks)
    assert overlaps == [] and all(o.status == K.RUN_DONE for o in outcomes)
    for account in ("acct-0", "acct-1", "acct-2"):                       # and each account's tasks ran in input order
        assert [n for a, n in order if a == account] == sorted(n for a, n in order if a == account)


def test_one_account_is_never_in_two_workers_of_two_processes(db):
    """Two runners with their own lease owners stand in for two worker processes on one database."""
    entered, release = threading.Event(), threading.Event()

    def held():
        entered.set()
        assert release.wait(WAIT)
        return "first"

    first = K.BoundedAccountRunner(leases=K.AccountLeases(db), owner="worker-1")
    second = K.BoundedAccountRunner(leases=K.AccountLeases(db), owner="worker-2")
    thread, box = in_thread(first.run, [K.AccountTask("acct", held)])
    assert entered.wait(WAIT)                            # worker 1 is inside the account
    outcome = second.run([K.AccountTask("acct", lambda: "second"), K.AccountTask("other", lambda: "free")])
    assert (outcome[0].status, outcome[0].reason) == (K.RUN_NOT_STARTED, K.LEASE_HELD) and outcome[0].retry_after > 0
    assert (outcome[1].status, outcome[1].value) == (K.RUN_DONE, "free")
    release.set()
    assert finished(thread, box)[0].value == "first"
    again = second.run([K.AccountTask("acct", lambda: "second")])        # released: the account is free again
    assert (again[0].status, again[0].value, again[0].fencing_token) == (K.RUN_DONE, "second", 2)
    assert K.AccountLeases(db).holder("acct") is None


def test_one_accounts_exception_is_isolated_from_the_others(db):
    def boom():
        raise RuntimeError("this account's book is corrupt")

    runner = K.BoundedAccountRunner(max_concurrency=2, db=db, leases=K.AccountLeases(db))
    outcomes = runner.run([K.AccountTask("a", lambda: 1), K.AccountTask("b", boom), K.AccountTask("c", lambda: 3)])
    assert [(o.status, o.value) for o in outcomes] == [(K.RUN_DONE, 1), (K.RUN_FAILED, None), (K.RUN_DONE, 3)]
    assert (outcomes[1].error, outcomes[1].message) == ("RuntimeError", "this account's book is corrupt")
    assert K.AccountLeases(db).holder("b") is None                       # its lease was given back all the same
    assert (runner.stats["done"], runner.stats["failed"]) == (2, 1)


def test_a_stop_starts_nothing_more_and_never_interrupts_an_account_in_flight():
    started, release, finished_accounts = [threading.Event(), threading.Event()], threading.Event(), []

    def running(n):
        def call():
            started[n].set()
            assert release.wait(WAIT)
            finished_accounts.append(n)                  # its transaction ran to the end
            return n
        return K.AccountTask(f"acct-{n}", call)

    runner = K.BoundedAccountRunner(max_concurrency=2)
    tasks = [running(0), running(1)] + [K.AccountTask(f"acct-{n}", lambda: pytest.fail("started after the stop")) for n in range(2, 8)]
    thread, box = in_thread(runner.run, tasks)
    assert started[0].wait(WAIT) and started[1].wait(WAIT)
    assert runner.queue_depth() == 6                     # six accounts are waiting, two are in flight
    runner.request_stop()
    release.set()
    outcomes = finished(thread, box)
    assert sorted(finished_accounts) == [0, 1] and [o.status for o in outcomes[:2]] == [K.RUN_DONE, K.RUN_DONE]
    assert [(o.status, o.reason) for o in outcomes[2:]] == [(K.RUN_NOT_STARTED, K.STOP_REQUESTED)] * 6
    assert runner.stats["not_started"] == {K.STOP_REQUESTED: 6} and runner.queue_depth() == 0
    # the process-wide stop is honoured the same way
    stopping = K.BoundedAccountRunner(stop=lambda: True).run([K.AccountTask("a", lambda: 1)])
    assert (stopping[0].status, stopping[0].reason) == (K.RUN_NOT_STARTED, K.STOP_REQUESTED)


def test_an_emergency_cancels_the_queued_work_of_its_scope_and_nothing_else(db):
    gate = K.PreemptionGate()
    ticket = gate.request("req", "b")
    outcomes = K.BoundedAccountRunner(gate=gate).run([K.AccountTask(a, lambda a=a: a) for a in "abc"])
    assert [(o.status, o.reason, o.value) for o in outcomes] == [
        (K.RUN_DONE, None, "a"), (K.RUN_NOT_STARTED, K.EMERGENCY_PREEMPTED, None), (K.RUN_DONE, None, "c")]
    gate.release(ticket)
    control = K.EmergencyControl(db)
    control.request(scope=K.ACCOUNT, account_id="b", requested_by="op", reason="drill", request_id="r1")
    for runner in (K.BoundedAccountRunner(emergency=control),                          # the durable record alone
                   K.BoundedAccountRunner(db=db, leases=K.AccountLeases(db), emergency=control)):   # and through the lease
        outcomes = runner.run([K.AccountTask(a, lambda a=a: a) for a in "abc"])
        assert [(o.status, o.reason) for o in outcomes] == [
            (K.RUN_DONE, None), (K.RUN_NOT_STARTED, K.EMERGENCY_PREEMPTED), (K.RUN_DONE, None)]
    control.request(scope=K.ALL, requested_by="op", reason="drill", request_id="r2")
    everything = K.BoundedAccountRunner(max_concurrency=3, emergency=control).run([K.AccountTask(a, lambda: 1) for a in "abc"])
    assert {(o.status, o.reason) for o in everything} == {(K.RUN_NOT_STARTED, K.EMERGENCY_PREEMPTED)}


def test_work_that_loses_its_lease_stops_before_its_next_mutation_and_reports_it(db):
    leases, first_done, taken = K.AccountLeases(db), threading.Event(), threading.Event()
    with db.connect() as conn:
        conn.execute("CREATE TABLE work (account_id TEXT, step INTEGER)")

    def two_steps(guard):
        with db.connect() as conn:
            conn.execute("INSERT INTO work VALUES ('acct', 1)")
        first_done.set()
        assert taken.wait(WAIT)                          # the emergency takes the account between the two steps
        guard.check()
        with db.connect() as conn:                       # never reached
            conn.execute("INSERT INTO work VALUES ('acct', 2)")

    runner = K.BoundedAccountRunner(leases=leases, owner="cycle")
    thread, box = in_thread(runner.run, [K.AccountTask("acct", two_steps, wants_guard=True)])
    assert first_done.wait(WAIT)
    emergency = leases.acquire("acct", "emergency:r1", purpose=K.EMERGENCY)
    assert emergency.takeover == K.PREEMPTED and emergency.previous_owner.startswith("cycle#")
    taken.set()
    outcome = finished(thread, box)[0]
    assert (outcome.status, outcome.reason, outcome.lease_lost) == (K.RUN_LEASE_LOST, "LEASE_TAKEN_OVER", "LEASE_TAKEN_OVER")
    with db.connect() as conn:
        assert [r[0] for r in conn.execute("SELECT step FROM work")] == [1]
    assert leases.holder("acct")["owner_id"] == "emergency:r1" and runner.stats["lease_lost"] == 1


def test_work_that_outlived_its_lease_is_reported_even_when_nobody_took_the_account(db):
    clock = Clock()
    leases = K.AccountLeases(db, clock=clock)

    def slow():
        clock.now += 61_000                              # longer than the 60 s it was given

    outcome = K.BoundedAccountRunner(leases=leases, clock=clock).run([K.AccountTask("acct", slow)])[0]
    assert (outcome.status, outcome.lease_lost) == (K.RUN_DONE, "LEASE_EXPIRED")


def test_the_rate_budget_decides_what_is_started():
    answers = {"go": True, "full": False, "later": 2.5, "broken": RuntimeError("budget store unreachable")}

    def admit(account_id):
        answer = answers[account_id]
        if isinstance(answer, Exception):
            raise answer
        return answer

    asked = []
    runner = K.BoundedAccountRunner(admit=lambda a: (asked.append(a), admit(a))[1])
    outcomes = runner.run([K.AccountTask(a, lambda a=a: a) for a in answers])
    assert asked == list(answers)                        # exactly once per account, in order
    assert [(o.status, o.reason, o.retry_after) for o in outcomes] == [
        (K.RUN_DONE, None, None), (K.RUN_NOT_STARTED, K.RATE_BUDGET_EXHAUSTED, None),
        (K.RUN_NOT_STARTED, K.RATE_BUDGET_EXHAUSTED, 2.5), (K.RUN_NOT_STARTED, K.RATE_BUDGET_UNAVAILABLE, None)]
    assert runner.stats["not_started"] == {K.RATE_BUDGET_EXHAUSTED: 2, K.RATE_BUDGET_UNAVAILABLE: 1}


def test_context_local_state_follows_the_work_onto_the_workers():
    permit = contextvars.ContextVar("permit", default=None)
    permit.set("granted-to-this-request")
    barrier = threading.Barrier(2)

    def call():
        barrier.wait(WAIT)                               # two distinct worker threads, not the caller's
        return permit.get(), threading.current_thread().name

    outcomes = K.BoundedAccountRunner(max_concurrency=2).run([K.AccountTask("a", call), K.AccountTask("b", call)])
    assert [o.value[0] for o in outcomes] == ["granted-to-this-request"] * 2
    assert all(o.value[1].startswith("account-runner-") for o in outcomes)


def test_work_whose_account_is_unknown_is_never_run_unprotected(db):
    anonymous = lambda: "ran"  # noqa: E731
    assert K.account_id_of_call(anonymous) is None
    protected = K.BoundedAccountRunner(leases=K.AccountLeases(db))
    assert protected([anonymous]) == [(None, {"status": "PENDING", "not_started": True, "reason": K.ACCOUNT_ID_UNKNOWN,
                                              "retry_after": None})]
    assert K.BoundedAccountRunner()([anonymous]) == ["ran"]              # nothing to protect: plain bounded execution
    tagged = lambda: ("acct", {"status": "DONE"})  # noqa: E731
    tagged.account_id = "acct"
    assert K.account_id_of_call(tagged) == "acct" and protected([tagged]) == [("acct", {"status": "DONE"})]


# ════════════════════════════════════════════════════════════════════════ the real cycle driver
class Platform:
    """The real registry, driver and shadow book on the synthetic market, with connected demo accounts."""

    def __init__(self, tmp_path, connected=4):
        self.db = S.fresh_db(tmp_path)
        research = _step2.new_register(tmp_path / "register.jsonl")
        dataset = _step2.SyntheticDataset()
        self.data = FrozenPanelBars(dataset._load("2020-01-01", "2021-06-30"), dataset.metadata)
        self.registry = FR.FamilyRegistry(self.db, research_register=research)
        self.registry.register_all(actor_ref="system:test")
        with self.db.connect() as conn:
            for n in range(connected):
                account = f"acct-{n}"
                conn.execute("INSERT INTO broker_accounts (id, user_id, broker_id, market_type, label, status, environment, "
                             "created_at, updated_at) VALUES (?,?,?,?,?,?,?,?,?)",
                             (account, f"user-{n}", "binance", "futures", account, "connected", "demo", 1, 1))
                conn.execute("INSERT INTO bot_instances (id, user_id, broker_account_id, market_type, strategy_id, "
                             "strategy_version, symbols, timeframes, allocation_type, allocation_value, mode, status, "
                             "created_at, updated_at, risk_level, capital_allocation, capital_allocation_type) "
                             "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                             (f"bot-{account}", f"user-{n}", account, "futures", "cati", "1", "[]", "[]", "risk_based", 0.5,
                              "live", "active", 1, 1, ("conservative", "balanced", "aggressive")[n % 3], 5_000.0 + 500 * n,
                              "fixed_amount"))
        self.accounts = CombinedAccounts(ModelAccounts(initial_equity=EQUITY, levels=("balanced",), policies=(T.MANDATE,)),
                                         ConnectedAccounts(self.db))

    def driver(self, runner=None):
        return D.PlatformCycleDriver(self.db, accounts=self.accounts, data={"1d": self.data}, registry=self.registry,
                                     account_runner=runner)

    def run(self, driver, day):
        return driver.run_cycle("DAILY_TREND", DT.day_text(day), now_ms=DT.close_ms(day) + 60_000)

    def decisions(self):
        with self.db.connect() as conn:
            return [tuple(r) for r in conn.execute(
                "SELECT account_id, data_cutoff, decision_hash FROM cati_family_decisions ORDER BY 1, 2")]


def sub(tmp_path, name):
    path = tmp_path / name
    path.mkdir()
    return path


def test_the_runner_drives_the_real_cycle_and_decides_exactly_what_a_sequential_cycle_decides(tmp_path):
    reference, parallel = Platform(sub(tmp_path, "sequential")), Platform(sub(tmp_path, "parallel"))
    runner = K.BoundedAccountRunner(max_concurrency=4, db=parallel.db, leases=K.AccountLeases(parallel.db),
                                    emergency=K.EmergencyControl(parallel.db))
    sequential, driven = reference.driver(), parallel.driver(runner)
    for day in range(START, START + 6):
        assert reference.run(sequential, day).status == D.COMPLETED
        result = parallel.run(driven, day)
        assert (result.status, result.accounts_done) == (D.COMPLETED, 5)
        assert (runner.stats["done"], runner.stats["not_started"], runner.stats["lease_lost"]) == (5, {}, 0)
    assert parallel.decisions() == reference.decisions() and len(parallel.decisions()) == 30
    with parallel.db.connect() as conn:                                  # every lease taken was given back
        assert conn.execute("SELECT COUNT(*) FROM cati_account_leases WHERE released_at IS NULL").fetchone()[0] == 0
        assert conn.execute("SELECT MIN(fencing_token), MAX(fencing_token) FROM cati_account_leases").fetchone()[:] == (6, 6)


def test_an_emergency_cancels_the_cycles_queued_accounts_and_the_cycle_then_cannot_reopen_them(tmp_path):
    p = Platform(tmp_path)
    control = K.EmergencyControl(p.db)
    driver = p.driver(K.BoundedAccountRunner(db=p.db, leases=K.AccountLeases(p.db), emergency=control))
    for day in range(START, START + 3):
        assert p.run(driver, day).status == D.COMPLETED
    control.request(scope=K.ACCOUNT, account_id="acct-1", requested_by="admin:7", reason="incident", request_id="r1")
    preempted = p.run(driver, START + 3)
    assert (preempted.status, preempted.accounts_done) == (D.PARTIAL, 4)
    assert preempted.account_results["acct-1"] == {"status": "PENDING", "not_started": True,
                                                   "reason": K.EMERGENCY_PREEMPTED, "retry_after": None}
    with p.db.connect() as conn:
        assert conn.execute("SELECT status FROM cati_platform_cycle_accounts WHERE account_id='acct-1' AND cycle_id=?",
                            (preempted.cycle_id,)).fetchone()[0] == D.PENDING
        assert conn.execute("SELECT COUNT(*) FROM cati_family_decisions WHERE account_id='acct-1' AND data_cutoff=?",
                            (DT.day_text(START + 3),)).fetchone()[0] == 0
    control.begin("r1")
    control.finish("r1", outcome=K.COMPLETED)                            # the flatten has run; the hold stays
    resumed = p.run(driver, START + 3)
    assert (resumed.status, resumed.accounts_done) == (D.COMPLETED, 5)
    assert resumed.account_results["acct-1"]["blocked"] == D.ACCOUNT_KILL       # the cycle may not enter for it
    assert resumed.account_results["acct-0"]["replayed"] is True                # the others were not run twice
    later = p.run(driver, START + 4)
    assert later.account_results["acct-1"]["blocked"] == D.ACCOUNT_KILL and later.account_results["acct-0"]["blocked"] is None
    with p.db.connect() as conn:
        for row in conn.execute("SELECT document FROM cati_family_decisions WHERE account_id='acct-1' AND data_cutoff>=?",
                                (DT.day_text(START + 3),)):
            document = json.loads(row[0])
            assert document["flags"]["entries_blocked"] is True
            assert not [o for o in document["orders"] if o["kind"] == T.ENTRY or (o["kind"] == T.ADJUST and o["quantity"] > 0)]
    everything = control.request(scope=K.ALL, requested_by="admin:7", reason="incident", request_id="r2")
    assert everything["active"] and p.run(driver, START + 5).accounts_done == 0   # all queued, none started


def test_the_platform_observation_reads_cycles_leases_and_requests(tmp_path):
    p = Platform(tmp_path, connected=1)
    assert p.run(p.driver(), START).status == D.COMPLETED
    now = DT.close_ms(START) + 3_600_000
    clock = Clock(now)
    K.AccountLeases(p.db, clock=clock).acquire("acct-0", "a-worker-that-died", ttl_ms=60_000)
    with p.db.connect() as conn:
        conn.execute("INSERT INTO cati_platform_cycles (cycle_id, family_id, cycle_key, mode, status, owner, started_at, "
                     "heartbeat_at) VALUES ('cyc_stuck','DAILY_TREND','2099-01-01','SHADOW','MARKET_DECIDED','w',?,?)", (now, now))
    limits = Limits()
    healthy = K.platform_observation(p.db, now_ms=now + 1_000)
    assert healthy["cycles"]["running"] == 1 and healthy["leases"] == {"held": 1, "abandoned": 0, "oldest_abandoned_age": None}
    assert K.platform_faults(healthy, limits) == ((), ())
    late = now + 60_000 + int(limits.platform_cycle_stall_seconds * 1000) + 1_000
    stalled = K.platform_observation(p.db, now_ms=late)
    assert stalled["leases"]["abandoned"] == 1 and stalled["cycles"]["oldest_running_heartbeat_age"] > limits.platform_cycle_stall_seconds
    assert K.platform_faults(stalled, limits) == ((K.PLATFORM_CYCLE_STALLED,), (K.ACCOUNT_LEASE_ABANDONED,))
    assert K.platform_faults({}, limits) == ((), ())                     # what is unknown is never a fault


def test_the_supervisor_has_platform_thresholds_beside_the_unchanged_residual_ones(monkeypatch):
    limits = Limits()
    # the residual loop's thresholds keep their values and their meaning
    assert (limits.interval_seconds, limits.cycle_stall_seconds, limits.lease_heartbeat_stale_seconds,
            limits.collector_stall_seconds, limits.decision_stall_seconds) == (15.0, 600.0, 150.0, 3600.0, 9000.0)
    assert (limits.platform_cycle_stall_seconds, limits.platform_account_stall_seconds,
            limits.account_lease_abandoned_seconds, limits.emergency_stall_seconds) == (120.0, 10.0, 30.0, 240.0)
    # each relates to what it was derived from
    assert limits.platform_cycle_stall_seconds * 1000 < D.HEARTBEAT_STALE_MS          # seen before the takeover
    assert limits.emergency_stall_seconds * 1000 >= runtime.FLATTEN_LOCK_WAIT_SECONDS * 1000 + K.EMERGENCY_ACTIVE_TTL_MS
    monkeypatch.setenv("RUNTIME_SUPERVISOR_PLATFORM_CYCLE_STALL_SECONDS", "900")
    monkeypatch.setenv("RUNTIME_SUPERVISOR_EMERGENCY_STALL_SECONDS", "not-a-number")
    tuned = Limits.from_env()
    assert (tuned.platform_cycle_stall_seconds, tuned.emergency_stall_seconds, tuned.cycle_stall_seconds) == (900.0, 240.0, 600.0)


# ═══════════════════════════════════════════════════════════════ the production broker cycle and flatten
def account(n):
    return {"id": f"a{n}", "user_id": "u", "broker_id": "binance", "environment": "DEMO", "status": "connected"}


@pytest.fixture
def engine(monkeypatch, db):
    """The real ``broker_cycle`` / ``sync`` / ``flatten`` over fake accounts, a fake venue and a real database."""
    settings = Settings(_env_file=None, APP_ENV="PRODUCTION", DATABASE_ROLE="production", ENVIRONMENT_NAME="production",
                        EXECUTION_MODE="live", DEMO_ORDER_SUBMISSION_ENABLED=True, LIVE_ORDER_SUBMISSION_ENABLED=False)
    from app.execution import demo_boundary_certification, demo_transport_smoke

    runtime_shutdown.reset_for_tests()
    monkeypatch.setattr(config, "settings", settings)
    monkeypatch.setattr(runtime, "settings", settings)
    monkeypatch.setattr(runtime, "owner_current", lambda database: True)
    monkeypatch.setattr(runtime, "resolve_broker_auth", lambda account_id, user, database: SimpleNamespace(
        account_id=account_id, user_id=user, broker_type="binance", environment="demo", base_url="https://x",
        credential_version=1, key_fingerprint="fp"))
    labelled = []
    monkeypatch.setattr(demo_transport_smoke, "process_local_request", lambda database: labelled.append("smoke"))
    monkeypatch.setattr(demo_boundary_certification, "process_local_request", lambda database: labelled.append("certify"))
    events, lock = [], threading.Lock()

    def log(event):
        with lock:
            events.append(event)

    def flatten_account(database, scoped, client, *, request_id):
        log(f"flatten:{scoped['id']}")
        return [{"account_id": scoped["id"], "symbol": "ADAUSDT", "status": "closed", "detail": "ok"}]

    monkeypatch.setattr(production_execution, "flatten_account", flatten_account)
    registered = threading.Event()
    real_request = runtime._preemption.request

    def request(request_id, account_id=None):
        ticket = real_request(request_id, account_id)
        registered.set()                                 # the cycle can see the flatten from this instant
        return ticket

    monkeypatch.setattr(runtime._preemption, "request", request)
    runtime._clients.clear()
    yield SimpleNamespace(db=db, events=events, log=log, registered=registered, labelled=labelled,
                          factory=lambda auth: SimpleNamespace(account_id=auth.account_id))
    runtime._clients.clear()
    runtime._idle.set()
    runtime_shutdown.reset_for_tests()
    assert not runtime._preemption.pending()             # no test leaves a flatten registered


@pytest.mark.parametrize("accounts", [5, 60])
def test_flatten_preempts_the_broker_cycle_at_the_next_account_boundary_and_the_cycle_resumes(engine, monkeypatch, accounts):
    """Under load: the round has many accounts left when the flatten arrives. It reaches the close boundary as
    soon as the account in flight is finished -- not after the round -- and no account is skipped for it."""
    in_flight, let_it_finish = threading.Event(), threading.Event()

    def sync_account(database, acct, **kw):
        engine.log(f"sync:{acct['id']}")
        if acct["id"] == "a2":
            in_flight.set()
            assert let_it_finish.wait(WAIT)              # an entry is being protected: it is never cut short
            engine.log("protected:a2")

    monkeypatch.setattr(runtime, "execution_accounts", lambda database: [account(n) for n in range(accounts)])
    monkeypatch.setattr(runtime, "sync_account", sync_account)
    cycle, cycle_box = in_thread(runtime.broker_cycle, engine.db)
    assert in_flight.wait(WAIT) and not runtime.wait_idle(0)
    flatten, flatten_box = in_thread(runtime.flatten, engine.db, request_id="req-1", factory=engine.factory)
    assert engine.registered.wait(WAIT)
    assert not [e for e in engine.events if e.startswith("flatten")]     # it waits for the account in flight, only
    let_it_finish.set()
    results = finished(flatten, flatten_box)
    finished(cycle, cycle_box)
    ids = [f"a{n}" for n in range(accounts)]
    assert engine.events == ([f"sync:{a}" for a in ids[:3]] + ["protected:a2"] + [f"flatten:{a}" for a in ids]
                             + [f"sync:{a}" for a in ids[3:]])
    assert [r["status"] for r in results] == ["closed"] * accounts
    assert runtime.wait_idle(0) and runtime._cycle_lock.acquire(blocking=False)
    runtime._cycle_lock.release()
    # durable: who / scope / state, the hold, the audit log, and the leases taken with priority and given back
    record = K.EmergencyControl(engine.db).get("req-1")
    assert (record["scope"], record["state"], record["outcome"], record["hold_scope"]) == (K.ALL, K.DONE, K.COMPLETED, "GLOBAL")
    assert [e["event"] for e in K.EmergencyControl(engine.db).events("req-1")] == [K.REQUESTED, "STARTED", "FINISHED"]
    assert PromotionGovernance(engine.db).kill_switch_on()               # nothing reopens the scope
    with engine.db.connect() as conn:
        leases = conn.execute("SELECT COUNT(*), SUM(purpose='EMERGENCY'), SUM(released_at IS NOT NULL) FROM cati_account_leases").fetchone()
    assert tuple(leases) == (accounts, accounts, accounts)


def test_a_flatten_of_one_account_lets_the_round_go_on_for_the_others(engine, monkeypatch):
    in_flight, let_it_finish = threading.Event(), threading.Event()

    def sync_account(database, acct, **kw):
        engine.log(f"sync:{acct['id']}")
        if acct["id"] == "a0":
            in_flight.set()
            assert let_it_finish.wait(WAIT)

    monkeypatch.setattr(runtime, "execution_accounts", lambda database: [account(n) for n in range(4)])
    monkeypatch.setattr(runtime, "sync_account", sync_account)
    cycle, cycle_box = in_thread(runtime.broker_cycle, engine.db)
    assert in_flight.wait(WAIT)
    flatten, flatten_box = in_thread(runtime.flatten, engine.db, account_id="a2", request_id="req-2", factory=engine.factory)
    assert engine.registered.wait(WAIT)
    let_it_finish.set()
    assert [r["account_id"] for r in finished(flatten, flatten_box)] == ["a2"]
    finished(cycle, cycle_box)
    assert engine.events == ["sync:a0", "flatten:a2", "sync:a1", "sync:a2", "sync:a3"]
    gov = PromotionGovernance(engine.db)
    assert gov.kill_switch_status("a2")["enabled"] and not gov.kill_switch_status()["enabled"]   # that account, held


def test_an_account_in_scope_does_not_start_its_entry_capable_step_once_a_flatten_is_pending(engine, monkeypatch):
    from app.activation import account_status

    client = Mock()
    client.account.return_value = None
    client.get_balance.return_value = {"equity": 1000}
    client.position_risk.return_value = []
    client.open_orders.return_value = []
    monkeypatch.setattr(account_status, "refresh_if_stale", lambda *a, **kw: {"status": "SYNCED"})
    entry_capable = Mock(return_value={"execution_permission": "WAITING_SIGNAL", "reason": "AWAITING_NATURAL_CATI_DECISION"})
    monkeypatch.setattr(production_execution, "process_account", entry_capable)
    runtime.initialize(engine.db)
    ticket = runtime._preemption.request("req-3", "a1")
    try:
        held = runtime.sync_account(engine.db, account(1), factory=lambda auth: client, execute=True)
        entry_capable.assert_not_called()                # nothing that can submit an entry was started
        assert held["execution"] == {"execution_permission": "BLOCKED_ACCOUNT", "reason": "CATI_NEW_ENTRY_KILL_SWITCH",
                                     "emergency_preempted": True}
        assert held["execution_permission"] == "BLOCKED_ACCOUNT" and held["status"] == "SYNCED"
        other = runtime.sync_account(engine.db, account(3), factory=lambda auth: client, execute=True)
        entry_capable.assert_called_once()               # an account outside the scope is processed as always
        assert other["execution"]["reason"] == "AWAITING_NATURAL_CATI_DECISION"
    finally:
        runtime._preemption.release(ticket)
    runtime.sync_account(engine.db, account(1), factory=lambda auth: client, execute=True)
    assert entry_capable.call_count == 2                 # and the account itself again, once the flatten is over


def test_no_labelled_order_work_is_started_ahead_of_a_pending_flatten(engine, monkeypatch):
    monkeypatch.setattr(runtime, "execution_accounts", lambda database: [])
    runtime.broker_cycle(engine.db)
    assert engine.labelled == ["smoke", "certify"]
    ticket = runtime._preemption.request("req-4")
    releaser = threading.Timer(0, lambda: None)          # nothing to wait for: the round has no accounts
    try:
        runtime.broker_cycle(engine.db)
    finally:
        runtime._preemption.release(ticket)
        releaser.cancel()
    assert engine.labelled == ["smoke", "certify"]       # not run again while the flatten was pending


def test_a_cycle_that_cannot_yield_bounds_the_flatten_and_nothing_is_claimed(engine, monkeypatch):
    held, release = threading.Event(), threading.Event()

    def stuck():
        with runtime._cycle_lock:                        # an account whose venue call hangs: no safe point is reached
            held.set()
            release.wait(WAIT)

    monkeypatch.setattr(runtime, "execution_accounts", lambda database: [account(0)])
    holder = threading.Thread(target=stuck, daemon=True)
    holder.start()
    assert held.wait(WAIT)
    try:
        started = time.monotonic()
        with pytest.raises(runtime.EngineUnavailable, match="BROKER_CYCLE_BUSY"):
            runtime.flatten(engine.db, request_id="req-5", factory=engine.factory, lock_wait=0.05)
        assert time.monotonic() - started < 5
    finally:
        release.set()
        holder.join(WAIT)
    assert engine.events == [] and not runtime._preemption.pending()
    record = K.EmergencyControl(engine.db).get("req-5")
    assert (record["state"], record["outcome"]) == (K.DONE, "ENGINE_UNAVAILABLE:BROKER_CYCLE_BUSY")
    assert PromotionGovernance(engine.db).kill_switch_on()               # entries stay stopped all the same


def test_the_flatten_takes_each_accounts_lease_from_ordinary_work_and_the_work_finds_out(engine, monkeypatch):
    leases, seen = K.AccountLeases(engine.db), {}
    ordinary = leases.acquire("a0", "platform-cycle-worker")

    def flatten_account(database, scoped, client, *, request_id):
        seen["holder"] = leases.holder(scoped["id"])
        return [{"account_id": scoped["id"], "symbol": "*", "status": "no_position", "detail": "BROKER_REPORTS_FLAT"}]

    monkeypatch.setattr(runtime, "execution_accounts", lambda database: [account(0)])
    monkeypatch.setattr(production_execution, "flatten_account", flatten_account)
    assert runtime.flatten(engine.db, account_id="a0", request_id="req-6", factory=engine.factory)[0]["status"] == "no_position"
    assert (seen["holder"]["owner_id"], seen["holder"]["purpose"], seen["holder"]["takeover"], seen["holder"]["previous_owner"]) == (
        "emergency:req-6", K.EMERGENCY, K.PREEMPTED, "platform-cycle-worker")
    with pytest.raises(K.LeaseLost, match="LEASE_TAKEN_OVER"):
        leases.verify(ordinary)                          # the ordinary worker stops before its next mutation
    assert leases.holder("a0") is None and leases.release(ordinary) is False


def test_a_flatten_runs_even_when_its_coordination_record_cannot_be_written(engine, monkeypatch):
    """The record coordinates other workers and keeps the audit trail. The close never depends on it."""
    monkeypatch.setattr(runtime, "execution_accounts", lambda database: [account(0)])
    monkeypatch.setattr(K.EmergencyControl, "begin", Mock(side_effect=sqlite3.OperationalError("database is locked")))
    monkeypatch.setattr(K.AccountLeases, "acquire", Mock(side_effect=sqlite3.OperationalError("database is locked")))
    assert [r["status"] for r in runtime.flatten(engine.db, request_id="req-7", factory=engine.factory)] == ["closed"]
    assert engine.events == ["flatten:a0"]


# ════════════════════════════════════════════════════════════════════════════════════ the admin route
@pytest.fixture
def api(db):
    app = FastAPI()
    app.include_router(emergency_api.router)
    app.dependency_overrides[require_admin_emergency] = lambda: "admin-1"
    app.dependency_overrides[emergency_api.get_db] = lambda: db
    return TestClient(app)


BASE = "/api/v1/admin/emergency"


def test_the_flatten_route_sets_the_kill_switch_then_records_the_request_then_calls_the_engine(api, db, monkeypatch):
    seen = {}

    def engine_flatten(database, *, account_id, request_id):
        record = K.EmergencyControl(database).get(request_id)
        seen.update(kill_switch=PromotionGovernance(database).kill_switch_on(), record=record,
                    account_switch=PromotionGovernance(database).kill_switch_status("acct")["enabled"])
        return [{"account_id": "acct", "symbol": "ADAUSDT", "status": "closed", "detail": "ok"}]

    monkeypatch.setattr(runtime, "flatten", engine_flatten)
    response = api.post(f"{BASE}/flatten", json={"scope": "account", "account_id": "acct", "confirm": "FLATTEN",
                                                 "reason": "incident drill"})
    assert response.status_code == 200 and response.json()["ok"] is True
    assert seen["kill_switch"] is True and seen["account_switch"] is True
    assert (seen["record"]["state"], seen["record"]["requested_by"], seen["record"]["reason"], seen["record"]["scope"],
            seen["record"]["account_id"], seen["record"]["hold_raised"]) == (
        K.REQUESTED, "admin:admin-1", "incident drill", K.ACCOUNT, "acct", True)
    listed = api.get(f"{BASE}/requests").json()
    assert listed["active"] == 0 and len(listed["requests"]) == 1
    request = listed["requests"][0]
    assert (request["state"], request["outcome"], request["detail"]["result_counts"]) == (K.DONE, K.COMPLETED, {"closed": 1})
    assert request["hold"] == {"scope": "acct", "enabled": True, "set_by": "admin:admin-1",
                               "set_at_ms": request["hold"]["set_at_ms"], "reason": request["hold"]["reason"]}
    # The operator lifts the PLATFORM switch: every other account may trade again, the flattened one may not.
    assert api.post(f"{BASE}/kill-switch", json={"enabled": False, "reason": "incident over"}).status_code == 200
    assert not PromotionGovernance(db).kill_switch_on()
    assert GovernanceAuthority(db).authorize_entry(plan("acct")) == (False, "CATI_NEW_ENTRY_KILL_SWITCH")
    assert GovernanceAuthority(db).authorize_entry(plan("other"))[1] != "CATI_NEW_ENTRY_KILL_SWITCH"
    assert api.get(f"{BASE}/requests").json()["requests"][0]["hold"]["enabled"] is True


def test_the_route_closes_the_record_with_what_really_happened(api, db, monkeypatch):
    body = {"scope": "all", "account_id": None, "confirm": "FLATTEN", "reason": "incident drill"}
    assert api.post(f"{BASE}/flatten", json=body).status_code == 409           # no production engine in this process
    monkeypatch.setattr(runtime, "flatten", lambda database, **kw: [
        {"account_id": "a", "symbol": "ADAUSDT", "status": "failed", "detail": "CLOSE_SUBMIT_OUTCOME_UNKNOWN"}])
    assert api.post(f"{BASE}/flatten", json=body).status_code == 502
    monkeypatch.setattr(runtime, "flatten", Mock(side_effect=RuntimeError("boom")))
    assert api.post(f"{BASE}/flatten", json=body).status_code == 502
    outcomes = [r["outcome"] for r in reversed(api.get(f"{BASE}/requests").json()["requests"])]
    assert sorted(outcomes) == sorted(["ENGINE_UNAVAILABLE:CATI_PRODUCTION_PROFILE_REQUIRED", K.INCOMPLETE, "FAILED:RuntimeError"])
    assert all(r["state"] == K.DONE and r["hold"]["enabled"] for r in api.get(f"{BASE}/requests").json()["requests"])


def test_a_record_that_cannot_be_written_never_stops_the_flatten_and_the_new_route_is_admin_only(db, api, monkeypatch):
    monkeypatch.setattr(K.EmergencyControl, "request", Mock(side_effect=sqlite3.OperationalError("database is locked")))
    monkeypatch.setattr(runtime, "flatten", lambda database, **kw: [
        {"account_id": "a", "symbol": "ADAUSDT", "status": "closed", "detail": "ok"}])
    response = api.post(f"{BASE}/flatten", json={"scope": "all", "account_id": None, "confirm": "FLATTEN", "reason": "incident"})
    assert response.status_code == 200 and response.json()["ok"] is True and PromotionGovernance(db).kill_switch_on()
    anonymous = FastAPI()
    anonymous.include_router(emergency_api.router)
    anonymous.dependency_overrides[emergency_api.get_db] = lambda: db
    assert TestClient(anonymous).get(f"{BASE}/requests").status_code == 401


# ═════════════════════════════════════════════════════════════════════════ measurement (opt-in, no assertion on speed)
@pytest.mark.skipif(not os.environ.get("STEP3_COORDINATION_MEASURE"), reason="set STEP3_COORDINATION_MEASURE=<connected accounts>")
def test_measure_what_parallelism_buys_the_shadow_cycle(tmp_path):
    """Run the real driver over N connected accounts, sequentially and through the runner at several
    concurrencies, and print (or write to STEP3_COORDINATION_OUT) the timings the document quotes."""
    import statistics

    connected, days, out = int(os.environ["STEP3_COORDINATION_MEASURE"]), 5, []
    variants = [("driver default (sequential)", lambda db: None)]
    for workers in (1, 2, 4, 8):
        variants.append((f"runner x{workers}", lambda db, w=workers: K.BoundedAccountRunner(max_concurrency=w, db=db)))
        variants.append((f"runner x{workers} + lease", lambda db, w=workers: K.BoundedAccountRunner(
            max_concurrency=w, db=db, leases=K.AccountLeases(db), emergency=K.EmergencyControl(db))))
    for label, make in variants:
        p = Platform(sub(tmp_path, label.replace(" ", "_").replace("(", "").replace(")", "")), connected=connected)
        driver, walls, stage = p.driver(make(p.db)), [], []
        for day in range(START, START + days):
            began = time.perf_counter()
            result = p.run(driver, day)
            walls.append((time.perf_counter() - began) * 1000)
            stage.append(result.timings_ms["ACCOUNT_STAGE"])
            assert result.status == D.COMPLETED
        with p.db.connect() as conn:
            slowest = conn.execute("SELECT MAX(duration_ms) FROM cati_platform_cycle_accounts").fetchone()[0]
        out.append({"variant": label, "accounts": connected + 1, "cycles": days,
                    "cycle_ms_median": round(statistics.median(walls), 1), "cycle_ms_max": round(max(walls), 1),
                    "account_stage_ms_median": round(statistics.median(stage), 1),
                    "account_stage_ms_max": round(max(stage), 1), "slowest_account_ms": slowest})
    text = json.dumps(out, indent=2)
    if os.environ.get("STEP3_COORDINATION_OUT"):
        Path(os.environ["STEP3_COORDINATION_OUT"]).write_text(text + "\n", encoding="utf-8")
    print("\nSTEP3 COORDINATION MEASUREMENT", text)
