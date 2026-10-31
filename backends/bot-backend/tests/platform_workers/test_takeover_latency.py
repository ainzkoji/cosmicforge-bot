"""Takeover latency of the broker-cycle assignment layer (Step 5.2 follow-up, 31 October 2026) -- on both dialects.

Two scheduling delays used to sit between a worker's death and the successor owning its accounts:

1. the fair share counted the dead worker as live capacity until its HEARTBEAT was ``worker_liveness_ms`` old
   (2 x TTL in the broker cycle), although its claims had expired after one TTL -- so the successor, already at its
   share, took nothing for a second TTL;
2. the successor claimed only at the start of a round, so an expiry that fell inside its round waited for the next.

Fixed: a worker that holds an EXPIRED, unreleased claim (a live worker renews before expiry), or that this process
has just taken an expired claim from and that has not heartbeated since, is not live capacity; and the broker
cycle claims free accounts between two account steps (at most every TTL/6) and adds them to the round in progress.

What must not change, and is asserted here: a LIVE claim is never taken; the stale holder's fence is refused
after the takeover; the fencing token rises; the new owner recovers before anything else; a worker that comes
back is counted again (the fleet rebalances at a round boundary, never mid-step).
"""
from __future__ import annotations

import time

import pytest

from app.ops import runtime_shutdown
from app.platform_workers.assignments import AssignmentLost, WorkerAssignments
from app.platform_workers.broker_assignment import ASSIGNED, BrokerCycleAssignments

TTL = 600


def _accounts(asg, n):
    asg.upsert_accounts([(f"acct{i:04d}", f"t{i % 5}", "binance", "DEMO") for i in range(n)])


def _execution_accounts(n):
    return [{"id": f"acct{i:04d}", "user_id": f"t{i % 5}", "broker_id": "binance", "environment": "DEMO"}
            for i in range(n)]


def _wait_expiry(ms=TTL):
    time.sleep(ms / 1000 + 0.15)


@pytest.fixture(autouse=True)
def _no_stop():
    runtime_shutdown.reset_for_tests()
    yield
    runtime_shutdown.reset_for_tests()


def test_successor_takes_a_dead_workers_accounts_after_one_ttl_not_two(platform_db):
    # The broker cycle's liveness window is 2 x TTL. The dead worker's heartbeat is still inside it when its claims
    # expire; the successor must not wait for the heartbeat to age out.
    asg = WorkerAssignments(platform_db, ttl_ms=TTL, worker_liveness_ms=2 * TTL)
    _accounts(asg, 10)
    a, b = asg.register_worker("A"), asg.register_worker("B")
    held_by_a = asg.claim(a)
    asg.claim(b)
    assert len(held_by_a) == 5
    time.sleep(TTL / 2000)
    asg.renew(b)                                                # B is alive and renews; A is dead
    time.sleep(TTL / 2000 + 0.15)
    with platform_db.transaction(write=False) as conn:          # A's heartbeat is still "live" by the window
        assert conn.now_ms() - int(conn.scalar("SELECT heartbeat_at FROM platform_workers WHERE worker_id='A'")) < 2 * TTL
    after = asg.claim(b)
    assert len(after) == 10
    taken = {x.account_id: x for x in after if x.kind == "TAKEOVER"}
    assert set(taken) == {x.account_id for x in held_by_a}
    for old in held_by_a:                                       # fencing: token rises, recovery first
        assert taken[old.account_id].fencing_token == old.fencing_token + 1
        assert taken[old.account_id].needs_recovery and taken[old.account_id].previous_worker == "A"
        with pytest.raises(AssignmentLost):
            asg.verify(old)                                     # the stale holder is refused


def test_taken_accounts_are_not_handed_back_while_the_dead_heartbeat_ages_out(platform_db):
    asg = WorkerAssignments(platform_db, ttl_ms=TTL, worker_liveness_ms=10 * TTL)
    _accounts(asg, 10)
    a, b = asg.register_worker("A"), asg.register_worker("B")
    asg.claim(a)
    asg.claim(b)
    _wait_expiry()
    assert len(asg.claim(b)) == 10
    assert asg.surplus(b) == []                     # no REBALANCE of A's accounts back into the void
    assert len(asg.claim(b)) == 10                  # and the next round keeps them


def test_a_worker_that_comes_back_counts_again_and_the_fleet_rebalances(platform_db):
    asg = WorkerAssignments(platform_db, ttl_ms=TTL, worker_liveness_ms=10 * TTL)
    _accounts(asg, 10)
    a, b = asg.register_worker("A"), asg.register_worker("B")
    asg.claim(a)
    asg.claim(b)
    _wait_expiry()
    asg.claim(b)                                    # B takes A's expired claims
    other_process = WorkerAssignments(platform_db, ttl_ms=TTL, worker_liveness_ms=10 * TTL)
    assert other_process.claim(a) == []             # A was only stalled: it heartbeats again, nothing is free
    assert len(asg.surplus(b)) == 5                 # A counts as live again: B gives back its excess at a safe point
    asg.release(b, asg.surplus(b), reason="REBALANCE")
    assert len(other_process.claim(a)) == 5


def test_a_live_claim_is_never_taken_however_the_share_is_computed(platform_db):
    asg = WorkerAssignments(platform_db, ttl_ms=TTL, worker_liveness_ms=2 * TTL)
    _accounts(asg, 6)
    a = asg.register_worker("A")
    held = asg.claim(a, max_new=6)
    assert len(held) == 6
    b = asg.register_worker("B")
    assert asg.claim(b) == []                       # A renews in time: B gets nothing
    time.sleep(TTL / 2000)
    asg.renew(a)
    time.sleep(TTL / 2000 + 0.05)
    assert asg.claim(b) == []
    assert {x.account_id for x in asg.claim(a)} == {x.account_id for x in held}


def _layer(platform_db, wid):
    layer = BrokerCycleAssignments(platform_db, mode=ASSIGNED, ttl_ms=TTL, margin_ms=TTL // 5, worker_id=wid)
    layer.register()
    return layer


def test_the_broker_cycle_claims_between_accounts_and_extends_the_round(platform_db):
    accounts = _execution_accounts(8)
    a, b = _layer(platform_db, "A"), _layer(platform_db, "B")
    round_a = a.begin_round(accounts)
    round_b = b.begin_round(accounts)
    assert len(round_a) == 4 and len(round_b) == 4
    a_ids = {x["id"] for x in round_a}
    stale = {i: a._fence(a.owned[i]) for i in a_ids}
    # A dies here (no renewal). B is in the middle of its round: it processes its own accounts one by one.
    processed, recovery = [], {}
    deadline = time.monotonic() + 10
    for account in round_b:                                    # the production loop: keepalive, then the step
        b.keepalive()
        b.run_account(account, lambda recovery_only, _a=account: (processed.append(_a["id"]),
                                                                  recovery.setdefault(_a["id"], recovery_only)))
        if len(processed) == 3:                                # A's claims run out while B's round is still going
            time.sleep(TTL / 2000)
            b.assignments.renew(b.worker_id)                   # (B, alive, keeps renewing its own)
            time.sleep(TTL / 2000 + 0.15)
            b._claim_polled_at = 0.0                           # (the poll interval has long passed in real time)
        assert time.monotonic() < deadline
    assert set(processed) == {x["id"] for x in accounts}       # A's accounts were taken over IN this round
    assert all(recovery[i] is True for i in a_ids)             # recovery-only first step under the new owner
    assert sorted(b.last_round["takeovers"]) == sorted(a_ids)
    for i, fence in stale.items():                             # A's fences are dead
        with pytest.raises(AssignmentLost):
            fence.verify()
    assert len(processed) == len(set(processed))               # no account processed twice in the round


def test_no_claim_between_accounts_while_draining_or_stopping(platform_db):
    accounts = _execution_accounts(4)
    a, b = _layer(platform_db, "A"), _layer(platform_db, "B")
    a.begin_round(accounts)
    round_b = b.begin_round(accounts)
    _wait_expiry()
    runtime_shutdown.request_stop()
    b._claim_polled_at = 0.0
    b.keepalive()
    assert len(round_b) == 2                                   # a stopping worker takes nothing new
    runtime_shutdown.reset_for_tests()
    b.drain()
    b._claim_polled_at = 0.0
    b.keepalive()
    assert len(round_b) == 2 and b.owned == {}


def test_a_failing_claim_between_accounts_never_fails_the_account_step(platform_db, monkeypatch):
    accounts = _execution_accounts(2)
    b = _layer(platform_db, "B")
    round_b = b.begin_round(accounts)

    def broken():
        raise RuntimeError("platform database hiccup")
    monkeypatch.setattr(b.assignments, "has_free", broken)
    b._claim_polled_at = 0.0
    b.keepalive()                                              # logged, not raised
    assert [x["id"] for x in round_b] == ["acct0000", "acct0001"]
    b.run_account(round_b[0], lambda recovery_only: None)


def test_claim_polling_is_rate_limited(platform_db, monkeypatch):
    accounts = _execution_accounts(2)
    b = _layer(platform_db, "B")
    b.begin_round(accounts)
    calls = []
    monkeypatch.setattr(b.assignments, "has_free", lambda: calls.append(1) or False)
    for _ in range(20):
        b.keepalive()
    assert len(calls) <= 1                                      # at most one probe per TTL/6
    assert b.claim_poll_ms == TTL // 6
