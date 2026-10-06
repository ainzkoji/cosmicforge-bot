"""Always-on operation of the single canonical trading runtime.

What a service manager needs from this process, proven without one:

* one owner per database, through simultaneous starts, a crash and a reboot;
* a scheduler that has stopped is DETECTED and the process exits non-zero,
  instead of the web server staying "healthy" around a dead trading loop;
* an outage of the venue is not mistaken for such a fault;
* a stop lets the broker cycle in flight finish before the lease is released.
"""
from __future__ import annotations

import os
import subprocess
import sys
import threading
import time
from datetime import datetime, timedelta, timezone

import pytest

from app.ops import runtime_shutdown, runtime_supervisor as supervisor_module
from app.ops.runtime_ownership import (RuntimeOwnership, TRADING_SCHEDULER, current_owner, ensure_ownership_schema,
                                       holder_is_alive)
from app.ops.runtime_preflight import PreflightStatus, port_is_free, preflight
from app.ops.runtime_supervisor import (EXIT_CODE, FAILING, HEALTHY, STARTING, STOPPING, Limits, RuntimeSupervisor,
                                        assess)
from shared_lib.persistence.db import DB
from shared_lib.persistence.migrations import migrate

DEAD_PID = 999_999


@pytest.fixture
def db(tmp_path):
    database = DB(str(tmp_path / "runtime.db"))
    migrate(database)
    ensure_ownership_schema(database)
    return database


def owner(db, **kw):
    return RuntimeOwnership(db, database_path=db.path, database_role="production", **kw)


def write_lease(db, *, pid, started_at, heartbeat_at=None, session_id="rts_previous"):
    """A lease row exactly as an earlier process would have left it."""
    heartbeat_at = heartbeat_at or datetime.now(timezone.utc)
    with db.connect() as conn:
        conn.execute("INSERT OR REPLACE INTO runtime_ownership VALUES (?,?,?,?,?,?,?,?,?,NULL,NULL)",
                     (TRADING_SCHEDULER, os.path.abspath(db.path), "own_previous", session_id, pid, "host",
                      started_at.isoformat(), heartbeat_at.isoformat(), "production"))


# ── One owner ───────────────────────────────────────────────────────────────


def test_two_simultaneous_starts_yield_exactly_one_owner(db):
    contenders = [owner(db) for _ in range(8)]
    gate, results = threading.Barrier(len(contenders)), []

    def start(candidate):
        gate.wait()
        results.append(candidate.acquire())

    threads = [threading.Thread(target=start, args=(c,)) for c in contenders]
    [t.start() for t in threads]
    [t.join() for t in threads]
    assert sum(r.acquired for r in results) == 1
    refused = [r for r in results if not r.acquired]
    assert {r.reason for r in refused} == {"RUNTIME_OWNERSHIP_HELD_BY_ANOTHER_PROCESS"}
    assert [c.is_owner for c in contenders].count(True) == 1


def test_a_running_holder_in_another_process_is_refused_and_recovered_after_it_dies(db):
    """Crash recovery end to end: a real second process holds, then is killed."""
    holder = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(120)"])
    try:
        time.sleep(0.5)
        write_lease(db, pid=holder.pid, started_at=datetime.now(timezone.utc))
        refused = owner(db).acquire()
        assert not refused.acquired and refused.holder_pid == holder.pid
    finally:
        holder.kill()
        holder.wait()
    # The heartbeat is still fresh: recovery does not wait for it to age out.
    recovered = owner(db)
    assert recovered.acquire().acquired
    assert current_owner(db, db.path)["pid"] == os.getpid()


def test_a_reboot_that_reuses_the_pid_does_not_leave_the_runtime_locked_out(db):
    # The previous boot's lease names this very PID and its heartbeat is fresh.
    write_lease(db, pid=os.getpid(), started_at=datetime.now(timezone.utc) - timedelta(days=1))
    assert not holder_is_alive(os.getpid(), (datetime.now(timezone.utc) - timedelta(days=1)).isoformat())
    assert preflight(db=db, port=free_port()).status == PreflightStatus.STALE_LEASE
    from app.trading_intelligence.integration.residual_prospective import owner_current

    assert not owner_current(db)          # a ghost lease authorizes nothing
    fresh = owner(db)
    assert fresh.acquire().acquired
    assert owner_current(db)


def test_a_dead_holder_is_taken_over_and_an_uninspectable_one_is_never_robbed(db):
    write_lease(db, pid=DEAD_PID, started_at=datetime.now(timezone.utc))
    assert owner(db).acquire().acquired
    # Written by this process: a second claimant in it is refused.
    assert not owner(db).acquire().acquired


def test_a_heartbeat_that_cannot_be_written_is_not_a_lost_lease(db, monkeypatch):
    mine = owner(db)
    assert mine.acquire().acquired
    real_connect, failing = db.connect, {"on": True}

    def flaky():
        if failing["on"]:
            raise RuntimeError("database is locked")
        return real_connect()

    monkeypatch.setattr(db, "connect", flaky)
    assert mine.renew() and mine.is_owner and mine.renew_failures == 1
    failing["on"] = False
    assert mine.renew() and mine.renew_failures == 0
    # Taken by someone else: that IS a loss, and it is reported as one.
    with db.connect() as conn:
        conn.execute("UPDATE runtime_ownership SET runtime_owner_id='own_other'")
    assert not mine.renew() and not mine.is_owner


def free_port():
    import socket

    probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    probe.bind(("127.0.0.1", 0))
    port = probe.getsockname()[1]
    probe.close()
    return port


def test_the_port_probe_sees_a_live_listener_but_not_a_departed_one():
    import socket

    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.bind(("0.0.0.0", 0))
    listener.listen(1)
    port = listener.getsockname()[1]
    assert not port_is_free(port)
    # A connection the old server accepted and closed first lingers in TIME_WAIT
    # on POSIX; the port must still read as free once the listener is gone.
    client = socket.create_connection(("127.0.0.1", port))
    accepted, _ = listener.accept()
    accepted.close()
    client.close()
    listener.close()
    assert port_is_free(port)


# ── Liveness verdicts ───────────────────────────────────────────────────────

LIMITS = Limits(startup_grace_seconds=180, confirmations=3)


def running(**changes):
    """An observation of a healthy runtime, twenty minutes after start."""
    obs = {"uptime": 1200.0, "stopping": False, "errors": [], "runner_task_alive": True, "owns_runtime": True,
           "production_task_alive": True, "lease_renew_failures": 0,
           "lease": {"held": True, "ours": True, "heartbeat_age": 4.0},
           "cycle": {"loop_running_seconds": 1100.0, "last_cycle_completed_age_seconds": 12.0, "last_error": None},
           "tracker": {"status": "COLLECTING", "heartbeat_age": 30.0, "last_decision_age": 1500.0}}
    for key, value in changes.items():
        obs[key] = {**obs[key], **value} if isinstance(value, dict) and isinstance(obs.get(key), dict) else value
    return obs


def test_a_runtime_that_is_advancing_is_healthy():
    verdict = assess(running(), LIMITS)
    assert verdict.state == HEALTHY and not verdict.faults


@pytest.mark.parametrize("change,fault", [
    ({"runner_task_alive": False}, "RUNNER_LOOP_EXITED"),
    ({"owns_runtime": False}, "RUNTIME_OWNERSHIP_NOT_ACQUIRED"),
    ({"lease": {"held": False}}, "RUNTIME_OWNERSHIP_LOST"),
    ({"lease": {"ours": False}}, "RUNTIME_OWNERSHIP_LOST"),
    ({"lease": {"heartbeat_age": 400.0}}, "LEASE_HEARTBEAT_STALE"),
    ({"production_task_alive": False}, "CATI_PRODUCTION_TASK_NOT_RUNNING"),
    ({"production_task_alive": None}, "CATI_PRODUCTION_TASK_NOT_RUNNING"),
    ({"cycle": {"last_cycle_completed_age_seconds": 900.0}}, "CATI_PRODUCTION_CYCLE_STALLED"),
    ({"cycle": {"last_cycle_completed_age_seconds": None}}, "CATI_PRODUCTION_CYCLE_STALLED"),
    ({"tracker": {"heartbeat_age": 4000.0}}, "MARKET_DATA_COLLECTION_STALLED"),
    ({"tracker": {"last_decision_age": 10_000.0}}, "CATI_DECISION_STALLED"),
])
def test_each_way_the_trading_loop_can_die_behind_a_live_web_server_is_a_fault(change, fault):
    verdict = assess(running(**change), LIMITS)
    assert verdict.state == FAILING and fault in verdict.faults


def test_a_venue_outage_is_reported_but_is_not_a_reason_to_restart():
    # Reads are failing and collection is behind, but the loop itself cycles.
    verdict = assess(running(tracker={"status": "RATE_LIMIT_BACKOFF", "heartbeat_age": 900.0},
                             cycle={"last_error": "broker_cycle:ConnectionError"}, errors=[]), LIMITS)
    assert verdict.state == HEALTHY
    assert "MARKET_DATA_COLLECTION_DELAYED" in verdict.warnings


def test_a_filling_disk_is_announced_before_it_stops_every_write():
    verdict = assess(running(database={"disk_free_bytes": 500 * 1024 ** 2}), LIMITS)
    assert verdict.state == HEALTHY and "DATABASE_DISK_LOW" in verdict.warnings
    assert "DATABASE_DISK_LOW" not in assess(running(database={"disk_free_bytes": 50 * 1024 ** 3}), LIMITS).warnings


def test_startup_and_shutdown_are_not_faults():
    booting = {"uptime": 20.0, "runner_task_alive": True, "owns_runtime": False, "production_task_alive": None}
    assert assess(booting, LIMITS).state == STARTING
    assert assess(running(stopping=True, runner_task_alive=False), LIMITS).state == STOPPING
    # What cannot be read is never guessed into a fault.
    assert assess(running(lease=None, tracker=None, errors=["database:OperationalError"]), LIMITS).state == HEALTHY


def test_an_api_only_process_is_tolerated_only_when_explicitly_allowed():
    idle = running(owns_runtime=False, production_task_alive=None)
    assert assess(idle, LIMITS).faults == ("RUNTIME_OWNERSHIP_NOT_ACQUIRED",)
    assert assess(idle, Limits(require_ownership=False)).state == STARTING


def supervised(observations, **kw):
    sent, exits, stops = [], [], []
    feed = iter(observations)
    sup = RuntimeSupervisor(observe=lambda: next(feed), limits=LIMITS, notify=sent.append,
                            exit_process=exits.append, quiesce=stops.append, **kw)
    return sup, sent, exits, stops


def test_a_persistent_fault_stops_entries_and_exits_nonzero_for_the_service_manager():
    dead = running(runner_task_alive=False)
    sup, sent, exits, stops = supervised([running(), dead, dead, dead])
    sup.tick()
    assert "READY=1" in sent and sent[-1] == "WATCHDOG=1"
    sent.clear()
    sup.tick(), sup.tick()
    assert exits == [] and "WATCHDOG=1" not in sent      # no keep-alive while failing
    sup.tick()
    assert exits == [EXIT_CODE] and EXIT_CODE != 0
    assert stops == ["SUPERVISOR_RUNNER_LOOP_EXITED"]
    assert sup.fatal == ("RUNNER_LOOP_EXITED",)


def test_a_fault_that_clears_is_forgotten():
    dead = running(lease={"heartbeat_age": 400.0})
    sup, sent, exits, _ = supervised([dead, dead, running(), dead, dead, running()])
    for _ in range(6):
        sup.tick()
    assert exits == []
    assert sup.snapshot()["state"] == HEALTHY and sup.snapshot()["consecutive_failing_checks"] == 0


def test_collector_faults_get_time_to_clear_after_a_suspend_or_a_long_outage():
    # Back from a three-hour suspend: the last enrolled decision is old until
    # the collector has caught up, which takes longer than a failed heartbeat.
    behind = running(tracker={"last_decision_age": 12_000.0})
    sup, _, exits, _ = supervised([behind] * 8 + [running()])
    sup.limits = Limits(confirmations=3, slow_confirmations=10)
    for _ in range(9):
        sup.tick()
    assert exits == [] and sup.snapshot()["state"] == HEALTHY
    # A collector that never catches up is still a stuck runtime.
    sup, _, exits, _ = supervised([behind] * 10)
    sup.limits = Limits(confirmations=3, slow_confirmations=10)
    for _ in range(10):
        sup.tick()
    assert exits == [EXIT_CODE] and sup.fatal == ("CATI_DECISION_STALLED",)


def test_a_runtime_too_broken_to_stop_cleanly_is_still_ended():
    dead = running(owns_runtime=False)
    released = threading.Event()
    sup, _, exits, _ = supervised([dead] * 3)
    sup.quiesce = lambda reason: released.wait(30)       # a shutdown that hangs
    sup.limits = Limits(confirmations=3, shutdown_grace_seconds=0.2)
    started = time.monotonic()
    for _ in range(3):
        sup.tick()
    released.set()
    assert exits == [EXIT_CODE] and time.monotonic() - started < 5


def test_the_snapshot_is_public_liveness_only():
    sup, *_ = supervised([running()])
    sup.tick()
    snapshot = sup.snapshot()
    assert snapshot["state"] == HEALTHY and snapshot["ready"]
    assert snapshot["lease"] == {"owned": True, "held_by_this_process": True, "heartbeat_age_seconds": 4.0,
                                 "renew_failures": 0}
    assert snapshot["scheduler"]["last_cycle_completed_age_seconds"] == 12.0
    flat = repr(snapshot).lower()
    assert not any(word in flat for word in ("secret", "api_key", "brk_", "credential"))


def test_tests_and_tools_are_never_supervised(monkeypatch):
    supervisor_module.reset_for_tests()
    assert not supervisor_module.enabled()               # COSMICFORGE_TEST_MODE
    assert supervisor_module.start() is None


@pytest.mark.skipif(not hasattr(__import__("socket"), "AF_UNIX"), reason="systemd notification is POSIX-only")
def test_readiness_and_keepalive_reach_the_systemd_socket(tmp_path, monkeypatch):
    import socket

    path = str(tmp_path / "notify.sock")
    server = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
    server.bind(path)
    server.settimeout(2)
    monkeypatch.setenv("NOTIFY_SOCKET", path)
    assert supervisor_module.sd_notify("READY=1")
    assert server.recv(64) == b"READY=1"
    server.close()


def test_without_a_service_manager_notification_is_a_noop(monkeypatch):
    monkeypatch.delenv("NOTIFY_SOCKET", raising=False)
    assert supervisor_module.sd_notify("WATCHDOG=1") is False


ABSTRACT = chr(0) + "cosmicforge/notify"


@pytest.mark.parametrize("address,connected", [("/run/systemd/notify", "/run/systemd/notify"),
                                               ("@cosmicforge/notify", ABSTRACT)])
def test_the_notification_datagram_is_addressed_as_systemd_expects(monkeypatch, address, connected):
    """Runs everywhere: the socket is replaced, the protocol is not."""
    seen = {}

    class Datagram:
        def __init__(self, family, kind):
            seen["socket"] = (family, kind)

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def connect(self, where):
            seen["address"] = where

        def sendall(self, payload):
            seen["payload"] = payload

    monkeypatch.setattr(supervisor_module.socket, "AF_UNIX", 1, raising=False)
    monkeypatch.setattr(supervisor_module.socket, "socket", Datagram)
    monkeypatch.setenv("NOTIFY_SOCKET", address)
    assert supervisor_module.sd_notify("READY=1")
    assert seen == {"socket": (1, supervisor_module.socket.SOCK_DGRAM), "address": connected, "payload": b"READY=1"}


# ── The production loop ─────────────────────────────────────────────────────


@pytest.fixture
def production_loop(monkeypatch):
    from app.trading_intelligence.integration import production_runtime

    runtime_shutdown.reset_for_tests()
    yield production_runtime
    runtime_shutdown.reset_for_tests()
    production_runtime._idle.set()


def test_a_failing_auxiliary_step_never_costs_the_broker_cycle(production_loop, monkeypatch):
    from app.execution import demo_boundary_certification, demo_transport_smoke

    synced = []
    monkeypatch.setattr(demo_transport_smoke, "process_local_request", lambda db: 1 / 0)
    monkeypatch.setattr(demo_boundary_certification, "process_local_request", lambda db: 1 / 0)
    monkeypatch.setattr(production_loop, "sync", synced.append)
    production_loop.broker_cycle("db")
    assert synced == ["db"]
    assert production_loop.progress()["last_error"] == "demo_certification_request:ZeroDivisionError"
    assert production_loop.wait_idle(0)


def test_a_stop_waits_for_the_cycle_in_flight_before_the_lease_is_released(production_loop, monkeypatch, db):
    """An entry that reached the broker gets its protection before the exit."""
    import app.main as main_module

    events, entered, release = [], threading.Event(), threading.Event()

    def protecting(_db):
        entered.set()
        release.wait(10)
        events.append("protection_placed")

    class Multi:
        running, _stop_requested, _runners, _ownership = False, False, {}, None

        def release_runtime_ownership(self, *, reason):
            events.append("lease_released")

    monkeypatch.setattr(production_loop, "sync", protecting)
    monkeypatch.setattr(main_module, "runner_service", type("S", (), {"running": True, "multi_runner": Multi()})(),
                        raising=False)
    monkeypatch.setattr(main_module, "RUNTIME_SESSION_ID", None, raising=False)
    worker = threading.Thread(target=production_loop.broker_cycle, args=(db,))
    worker.start()
    assert entered.wait(5) and production_loop.progress()["cycle_in_flight"]
    threading.Timer(0.4, release.set).start()
    report = runtime_shutdown.quiesce(reason="SIGTERM", timeout_s=1.0)
    worker.join(5)
    assert events == ["protection_placed", "lease_released"]
    assert report.production_cycle_drained and runtime_shutdown.stop_requested()


def test_a_stopping_runtime_starts_no_new_account_work(production_loop, monkeypatch, db):
    touched = []
    monkeypatch.setattr(production_loop, "owner_current", lambda _db: True)
    monkeypatch.setattr(production_loop, "execution_accounts",
                        lambda _db: [{"id": "a", "user_id": "u", "broker_id": "binance", "environment": "DEMO"}])
    monkeypatch.setattr(production_loop, "sync_account", lambda _db, account, **kw: touched.append(account["id"]))
    production_loop.sync(db)
    runtime_shutdown.request_stop()
    production_loop.sync(db)
    assert touched == ["a"]


# ── Durability ──────────────────────────────────────────────────────────────


@pytest.mark.parametrize("configured,expected", [(None, 1), ("FULL", 2), ("full", 2), ("OFF", 1), ("nonsense", 1)])
def test_commit_durability_is_configurable_and_never_off(tmp_path, monkeypatch, configured, expected):
    if configured is None:
        monkeypatch.delenv("SQLITE_SYNCHRONOUS", raising=False)
    else:
        monkeypatch.setenv("SQLITE_SYNCHRONOUS", configured)
    database = DB(str(tmp_path / "durable.db"))
    with database.connect() as conn:
        assert conn.execute("PRAGMA synchronous").fetchone()[0] == expected     # 1 NORMAL, 2 FULL
        assert conn.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
        assert conn.execute("PRAGMA busy_timeout").fetchone()[0] == 10000
