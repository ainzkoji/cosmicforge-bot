"""Closure item 7.5 -- REAL concurrency: two cycles for the same broker
account cannot reserve contradictory portfolio capacity.

* threads: separate connections, released together by a barrier
* processes: separate interpreters (the in-process account lock cannot help
  them), serialized only by SQLite's ``BEGIN IMMEDIATE`` writer lock
"""
from __future__ import annotations

import multiprocessing as mp
import threading

from _pf import NOW, add_bot, make_db, sel

from app.trading_intelligence.portfolio.exposure_builder import account_state_fingerprint
from app.trading_intelligence.portfolio.reservation_store import CATIReservationStore


def _db(tmp_path):
    db = make_db(tmp_path / "race.db")
    add_bot(db, "botA", "acct1")
    add_bot(db, "botB", "acct1")
    add_bot(db, "botC", "acct2")
    return db


def test_threads_same_instrument_exactly_one_wins(tmp_path):
    db = _db(tmp_path)
    n = 8
    barrier = threading.Barrier(n)
    results = []
    lock = threading.Lock()
    empty = account_state_fingerprint("acct1", [])

    def worker(i):
        store = CATIReservationStore(db)  # own store, own connections
        bot = "botA" if i % 2 else "botB"
        barrier.wait()
        out = store.reserve(broker_account_id="acct1", bot_instance_id=bot, cycle_id=f"cyc{i}", selected=[sel("SOLUSDT")],
                            now_ms=NOW, ttl_seconds=900, expected_exposure_fingerprint=empty,
                            expected_reservation_fingerprint=empty)
        with lock:
            results.append((out.reserved, out.conflict_reason))

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(60)
    assert len(results) == n
    assert sum(1 for ok, _ in results if ok) == 1
    assert {r for ok, r in results if not ok} == {"ACCOUNT_RESERVATION_CONFLICT"}
    assert len(CATIReservationStore(db).active_reservations("acct1", NOW)) == 1


def test_processes_contradictory_capacity_exactly_one_wins(tmp_path):
    """Same bot, two concurrent cycles, ONE free slot, different instruments:
    only one reservation may exist afterwards."""
    db = _db(tmp_path)
    ctx = mp.get_context("spawn")
    start, results = ctx.Event(), ctx.Queue()
    from _race_worker import reserve_worker

    procs = [
        ctx.Process(target=reserve_worker, args=(db.path, "acct1",
                                                 "botA", cyc, [sel(sym)], NOW, 1, 1, start, results))
        for cyc, sym in (("cycle_1", "SOLUSDT"), ("cycle_2", "XRPUSDT"))
    ]
    for p in procs:
        p.start()
    start.set()
    got = [results.get(timeout=120) for _ in procs]
    for p in procs:
        p.join(60)
    assert all(ok is not None for _b, _c, ok, _r in got), got  # no worker crashed
    assert sum(1 for _b, _c, ok, _r in got if ok) == 1, got
    loser = [r for _b, _c, ok, r in got if not ok][0]
    assert loser in ("ACCOUNT_RESERVATION_CONFLICT", "CAPACITY_CHANGED")
    active = CATIReservationStore(db).active_reservations("acct1", NOW)
    assert len(active) == 1 and active[0].bot_instance_id == "botA"


def test_other_account_is_not_blocked_by_a_concurrent_reservation(tmp_path):
    db = _db(tmp_path)
    barrier = threading.Barrier(2)
    out = {}

    def worker(account, bot):
        store = CATIReservationStore(db)
        barrier.wait()
        out[account] = store.reserve(broker_account_id=account, bot_instance_id=bot, cycle_id="c", selected=[sel("SOLUSDT")],
                                     now_ms=NOW, ttl_seconds=900).reserved

    ts = [threading.Thread(target=worker, args=a) for a in (("acct1", "botA"), ("acct2", "botC"))]
    for t in ts:
        t.start()
    for t in ts:
        t.join(60)
    assert out == {"acct1": True, "acct2": True}
