"""The runtime-owner lease loop, asserted on behaviour (Section I Batch 7, ``RuntimeOwnerLoop``).

In production ``MultiBotRunner.run_loop`` does one job: hold the runtime-ownership lease for the database,
renew it every iteration, keep the watchdog heartbeat fresh, and stop scheduling the moment the lease is lost.
(``run_once`` returns at once in production; only the frozen CATI worker trades.) That job now lives in
``app.trading_intelligence.integration.runtime_owner_loop.RuntimeOwnerLoop``; ``MultiBotRunner`` delegates
to it and keeps its public ownership surface, which ``app.main`` (background-job gate), ``app.ops.runtime_
shutdown`` (quiesce, lease release) and ``app.ops.runtime_supervisor`` (health) read.

These tests drive both through a real lease table on a temporary database; nothing reads source text.
"""
from __future__ import annotations

import asyncio
import os
import socket
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from app.ops import runtime_watchdog as wd_mod
from app.ops.runtime_ownership import (TRADING_SCHEDULER, RuntimeOwnership, ensure_ownership_schema,
                                       lease_database_path)
from shared_lib.persistence.db import DB
from shared_lib.persistence.migrations import migrate

OTHER_HOST = "bot-trading-2f9c"   # another container: its PID cannot be inspected from here
DEAD_PID = 999_999


def run_async(coro):
    """Run a coroutine on a private loop. Unlike asyncio.run this never calls set_event_loop(None), which would
    make a later first ``import app.main`` (it asks for the current loop) fail in the same test session."""
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


@pytest.fixture
def db(tmp_path):
    database = DB(str(tmp_path / "owner_loop.db"))
    migrate(database)
    ensure_ownership_schema(database)
    return database


@pytest.fixture
def watchdog(monkeypatch):
    fresh = wd_mod.RuntimeWatchdog()
    monkeypatch.setattr(wd_mod, "_WATCHDOG", fresh)
    return fresh


@pytest.fixture
def multi(db, monkeypatch, watchdog):
    from app.runner import multi_runner as mr

    monkeypatch.setattr(mr, "get_bot_instance_service", lambda: SimpleNamespace(db=db))
    monkeypatch.setattr(mr, "get_equity_snapshot_service", lambda: MagicMock())
    monkeypatch.setattr(mr.MultiBotRunner, "_runtime_session_id", staticmethod(lambda: "rts_owner_loop"))
    return mr.MultiBotRunner()


def lease_row(db):
    with db.connect() as conn:
        row = conn.execute("SELECT * FROM runtime_ownership WHERE lease_name=?", (TRADING_SCHEDULER,)).fetchone()
    return dict(row) if row else None


def write_foreign_lease(db, *, pid=DEAD_PID, hostname=OTHER_HOST):
    now = datetime.now(timezone.utc).isoformat()
    with db.connect() as conn:
        conn.execute("INSERT OR REPLACE INTO runtime_ownership VALUES (?,?,?,?,?,?,?,?,?,NULL,NULL)",
                     (TRADING_SCHEDULER, lease_database_path(db.path), "own_other", "rts_other", pid, hostname,
                      now, now, "production"))


def audit_events(db, event_type):
    with db.connect() as conn:
        return conn.execute("SELECT * FROM events WHERE event_type=?", (event_type,)).fetchall()


def ticking(runner, *, stop_after, on_tick=None):
    """Replace run_once with a counting tick; request a stop after ``stop_after`` ticks."""
    calls = []

    async def run_once():
        calls.append(datetime.now(timezone.utc))
        if on_tick is not None:
            on_tick(len(calls))
        if len(calls) >= stop_after:
            runner._stop_requested = True

    runner.run_once = run_once
    return calls


# ── surface before any acquisition ───────────────────────────────────────────


def test_ownership_is_undecided_until_acquisition_is_attempted(multi):
    assert multi.owns_runtime is False
    assert multi.ownership_decided is False
    assert multi.ownership_reason == "NOT_ACQUIRED"
    assert multi.runtime_owner_id is None
    multi.release_runtime_ownership(reason="NOTHING_HELD")   # a no-op, never an error


# ── acquisition ──────────────────────────────────────────────────────────────


def test_acquisition_takes_the_lease_for_this_database(multi, db):
    assert multi.acquire_runtime_ownership() is True
    assert multi.owns_runtime and multi.ownership_decided and multi.ownership_reason == "ACQUIRED"
    row = lease_row(db)
    assert row["runtime_owner_id"] == multi.runtime_owner_id and row["released_at"] is None
    assert row["pid"] == os.getpid() and row["hostname"] == socket.gethostname()
    assert row["runtime_session_id"] == "rts_owner_loop"
    assert multi.acquire_runtime_ownership() is True          # idempotent while held
    assert lease_row(db)["runtime_owner_id"] == row["runtime_owner_id"]


def test_a_fresh_lease_of_another_host_is_never_taken(multi, db):
    write_foreign_lease(db)
    assert multi.acquire_runtime_ownership() is False
    assert multi.owns_runtime is False and multi.ownership_decided is True
    assert multi.ownership_reason == "RUNTIME_OWNERSHIP_HELD_BY_ANOTHER_PROCESS"
    assert lease_row(db)["runtime_owner_id"] == "own_other"
    collisions = audit_events(db, "RUNTIME_OWNERSHIP_COLLISION")
    assert len(collisions) == 1 and OTHER_HOST in collisions[0]["details_json"]


def test_a_dead_local_holder_is_taken_over(multi, db):
    write_foreign_lease(db, hostname=socket.gethostname())
    assert multi.acquire_runtime_ownership() is True
    assert lease_row(db)["runtime_owner_id"] == multi.runtime_owner_id


def test_an_acquisition_error_fails_closed(multi, monkeypatch):
    def broken(*_a, **_k):
        raise RuntimeError("lease table unreadable")

    monkeypatch.setattr("app.ops.runtime_ownership.RuntimeOwnership.acquire", broken)
    assert multi.acquire_runtime_ownership() is False
    assert multi.owns_runtime is False and multi.ownership_decided is True
    assert multi.ownership_reason == "RUNTIME_OWNERSHIP_ERROR:RuntimeError"


# ── the loop ─────────────────────────────────────────────────────────────────


def test_the_loop_refuses_to_schedule_without_the_lease(multi, db, watchdog):
    write_foreign_lease(db)
    calls = ticking(multi, stop_after=1)
    run_async(multi.run_loop(interval_seconds=0))
    assert calls == [] and multi.running is False
    assert watchdog.last_scheduler_heartbeat is None
    assert lease_row(db)["runtime_owner_id"] == "own_other"


def test_the_loop_renews_the_lease_and_beats_the_heartbeat_every_iteration(multi, db, watchdog):
    beats = []
    calls = ticking(multi, stop_after=2,
                    on_tick=lambda n: beats.append((watchdog.last_scheduler_heartbeat, lease_row(db)["heartbeat_at"])))
    run_async(multi.run_loop(interval_seconds=0))
    assert len(calls) == 2
    assert all(hb is not None for hb, _ in beats)
    assert beats[1][1] >= beats[0][1]                      # the persisted lease heartbeat moved forward
    assert watchdog.scheduler_health()[0] == wd_mod.HEALTHY
    assert multi.owns_runtime                              # stopping the loop does not release the lease
    assert lease_row(db)["released_at"] is None


def test_a_failing_iteration_is_recorded_and_the_loop_continues(multi, watchdog):
    calls = []

    async def run_once():
        calls.append(1)
        if len(calls) == 1:
            raise ValueError("one bad cycle")
        multi._stop_requested = True

    multi.run_once = run_once
    run_async(multi.run_loop(interval_seconds=0))
    assert len(calls) == 2
    assert "ValueError: one bad cycle" in str(watchdog.last_error)


def test_losing_the_lease_stops_the_loop_and_is_reported(multi, db, watchdog):
    def steal(n):
        if n == 1:
            with db.connect() as conn:
                conn.execute("UPDATE runtime_ownership SET runtime_owner_id='own_thief' WHERE lease_name=?",
                             (TRADING_SCHEDULER,))

    calls = ticking(multi, stop_after=5, on_tick=steal)
    run_async(multi.run_loop(interval_seconds=0))
    assert len(calls) == 1                                 # never scheduled again after the loss
    assert multi.running is False and multi.owns_runtime is False
    assert watchdog.scheduler_health()[1] == wd_mod.RUNTIME_OWNERSHIP_LOST
    assert len(audit_events(db, "RUNTIME_OWNERSHIP_LOST")) == 1
    assert lease_row(db)["runtime_owner_id"] == "own_thief"   # the new holder is left alone


def test_a_transient_renew_failure_is_not_a_loss(multi, monkeypatch):
    real = RuntimeOwnership.renew
    state = {"n": 0}

    def flaky(self):
        state["n"] += 1
        if state["n"] == 1:
            self.renew_failures += 1      # what renew() does on a write error: ownership kept
            return True
        return real(self)

    monkeypatch.setattr(RuntimeOwnership, "renew", flaky)
    calls = ticking(multi, stop_after=2)
    run_async(multi.run_loop(interval_seconds=0))
    assert len(calls) == 2 and multi.owns_runtime


def test_stop_ends_the_loop_at_the_next_check(multi):
    calls = ticking(multi, stop_after=10, on_tick=lambda n: multi.stop() if n == 1 else None)
    run_async(multi.run_loop(interval_seconds=0))
    assert len(calls) == 1 and multi.running is False


# ── shutdown ─────────────────────────────────────────────────────────────────


def test_release_on_shutdown_frees_the_lease_for_a_replacement(multi, db):
    assert multi.acquire_runtime_ownership()
    multi.release_runtime_ownership(reason="APPLICATION_SHUTDOWN")
    row = lease_row(db)
    assert row["released_at"] is not None and row["release_reason"] == "APPLICATION_SHUTDOWN"
    assert multi.owns_runtime is False
    replacement = RuntimeOwnership(db, database_path=db.path, database_role="production")
    assert replacement.acquire().acquired is True


def test_quiesce_stops_the_loop_and_releases_the_lease_through_the_runner(multi, db, monkeypatch):
    """The production shutdown path (app.ops.runtime_shutdown.quiesce) drives exactly this surface."""
    try:
        import app.main as main_module
        from app.ops import runtime_shutdown

        runtime_shutdown.reset_for_tests()
        assert multi.acquire_runtime_ownership()
        multi.running = True
        monkeypatch.setenv("DATABASE_URL", "sqlite:///" + db.path.replace("\\", "/"))
        monkeypatch.setattr(main_module, "runner_service", SimpleNamespace(running=True, multi_runner=multi),
                            raising=False)
        monkeypatch.setattr(main_module, "RUNTIME_SESSION_ID", None, raising=False)
        monkeypatch.setattr("app.trading_intelligence.integration.production_runtime.wait_idle", lambda _t: True)

        report = runtime_shutdown.quiesce(reason="APPLICATION_SHUTDOWN", timeout_s=1.0)

        assert multi.running is False and multi._stop_requested is True
        assert report.scheduler_stopped and report.lease_released, report.errors
        assert lease_row(db)["release_reason"] == "APPLICATION_SHUTDOWN"
        assert multi.owns_runtime is False
    finally:
        from app.ops import runtime_shutdown

        runtime_shutdown.reset_for_tests()


# ── RuntimeOwnerLoop on its own: no legacy runner involved ───────────────────


def _owner(db, **kw):
    from app.trading_intelligence.integration.runtime_owner_loop import RuntimeOwnerLoop

    return RuntimeOwnerLoop(db, runtime_session_id=lambda: "rts_cati_owner", **kw)


def test_the_owner_loop_holds_the_lease_without_any_runner(db, watchdog):
    from app.trading_intelligence.integration.runtime_owner_loop import STOPPED

    owner, ticks = _owner(db), []

    async def tick():
        ticks.append(lease_row(db)["runtime_owner_id"])

    outcome = run_async(owner.run(tick, interval_seconds=0, keep_running=lambda: len(ticks) < 2))
    assert outcome == STOPPED and len(ticks) == 2 and set(ticks) == {owner.runtime_owner_id}
    assert owner.owns_runtime and watchdog.iteration_count == 2


def test_the_owner_loop_reports_why_it_stopped(db, watchdog):
    from app.trading_intelligence.integration.runtime_owner_loop import LOST, REFUSED

    write_foreign_lease(db)
    alerts = []
    refused = _owner(db, alert=lambda t, m: alerts.append(t))
    assert run_async(refused.run(lambda: asyncio.sleep(0), interval_seconds=0, keep_running=lambda: True)) == REFUSED
    assert alerts == ["RUNTIME_OWNERSHIP_COLLISION"]

    with db.connect() as conn:
        conn.execute("DELETE FROM runtime_ownership")
    owner = _owner(db, alert=lambda t, m: alerts.append(t))

    async def stolen():
        with db.connect() as conn:
            conn.execute("UPDATE runtime_ownership SET runtime_owner_id='own_thief'")

    assert run_async(owner.run(stolen, interval_seconds=0, keep_running=lambda: True)) == LOST
    assert alerts[-1] == "RUNTIME_OWNERSHIP_LOST" and not owner.owns_runtime


def test_a_stale_lease_of_another_host_is_taken_only_after_the_stale_window(db):
    """Cross-host staleness through the owner loop (the rule itself: test_runtime_ownership_cross_host.py)."""
    from datetime import timedelta

    from app.ops.runtime_ownership import LEASE_STALE_SECONDS

    write_foreign_lease(db)
    assert _owner(db).acquire() is False                       # fresh: the other container is alive
    old = (datetime.now(timezone.utc) - timedelta(seconds=LEASE_STALE_SECONDS + 30)).isoformat()
    with db.connect() as conn:
        conn.execute("UPDATE runtime_ownership SET heartbeat_at=?", (old,))
    taker = _owner(db)
    assert taker.acquire() is True and lease_row(db)["runtime_owner_id"] == taker.runtime_owner_id
