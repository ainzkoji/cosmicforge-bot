"""Who reads runtime ownership, characterized end to end (Section I Batch 7, E-05).

Every process that trades holds the ``trading_scheduler`` lease through ``app.main.runner_loop``. These
readers answer questions about that lease, and they must give the same answers whatever object hosts the
lease loop:

* ``app.main._may_run_background_jobs`` / ``_await_runtime_ownership`` (the background-job gate);
* ``app.main.runner_status`` (``/runner/status``) and ``/health`` (``components.runtime_owns_lease``);
* ``app.api.bot_instances.get_effective_policy`` (``runner_policy_hash`` / ``policy_stale``);
* ``app.ops.runtime_supervisor`` (observation, verdict and the public liveness snapshot);
* ``app.ops.runtime_shutdown.quiesce`` (what stops, in which order, and the lease release).

The tests start the REAL ``runner_loop`` against a real lease table on a temporary database and look only at
what those readers return and at the database. They do not name the host class, so they held before the
production lease moved off ``MultiBotRunner`` and must hold after it.
"""
from __future__ import annotations

import asyncio
import os
import socket
import time
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from app.ops.runtime_ownership import TRADING_SCHEDULER, ensure_ownership_schema, lease_database_path
from shared_lib.persistence.db import DB
from shared_lib.persistence.migrations import migrate

SESSION = "rts_readers"
OTHER_HOST = "bot-trading-elsewhere"
PRODUCTION_STATUS_KEYS = {
    "running", "mode", "engine", "strategy", "configuration", "owns_runtime", "live_order_submission_enabled",
    "demo_order_submission_enabled", "broker_execution_scope", "legacy_instances_scheduled", "bots",
}
SIMULATION_STATUS_KEYS = {
    "running", "mode", "interval_seconds", "max_symbols", "started_at", "last_cycle_at", "cycle_count",
    "last_error", "process_execution_mode", "multi_bot_runner_initialized", "iteration", "bots",
}


def lease_row(db):
    with db.connect() as conn:
        row = conn.execute("SELECT * FROM runtime_ownership WHERE lease_name=?", (TRADING_SCHEDULER,)).fetchone()
    return dict(row) if row else None


@pytest.fixture
def runtime(tmp_path, monkeypatch):
    """A serving trading process on a throwaway database: real lease rules, real runner_loop."""
    import app.main as main_module
    from app.core import bot_instance_service as bis
    from app.ops import runtime_shutdown, runtime_supervisor
    from app.ops import runtime_watchdog as wd

    path = str(tmp_path / "runtime.db")
    db = DB(path)
    migrate(db)
    ensure_ownership_schema(db)
    with db.connect() as conn:
        conn.execute("INSERT INTO runtime_sessions (runtime_session_id, started_at, status) VALUES (?,?,?)",
                     (SESSION, datetime.now(timezone.utc).isoformat(), "RUNNING"))

    monkeypatch.setenv("DATABASE_URL", "sqlite:///" + path.replace("\\", "/"))
    monkeypatch.delenv("COSMICFORGE_TEST_MODE", raising=False)        # look like a serving process
    monkeypatch.delenv("COSMICFORGE_SKIP_RUNTIME_PREFLIGHT", raising=False)
    monkeypatch.setattr(bis, "_bot_instance_service", bis.BotInstanceService(db=db))
    monkeypatch.setattr(main_module, "runner_service", main_module.RunnerServiceState(running=True))
    monkeypatch.setattr(main_module, "RUNTIME_SESSION_ID", SESSION, raising=False)
    monkeypatch.setattr(main_module, "get_db_path", lambda: path)
    monkeypatch.setattr(main_module.app.state, "cati_production_task", None, raising=False)
    monkeypatch.setattr(wd, "_WATCHDOG", wd.RuntimeWatchdog())
    drains = []

    def wait_idle(_timeout):
        row = lease_row(db)
        drains.append({"lease_held_by_us": bool(row and row["released_at"] is None and row["pid"] == os.getpid()),
                       "entries_stopped": runtime_shutdown.stop_requested(),
                       "runner_service_running": main_module.runner_service.running})
        return True

    monkeypatch.setattr("app.trading_intelligence.integration.production_runtime.wait_idle", wait_idle)
    runtime_shutdown.reset_for_tests()
    runtime_supervisor.reset_for_tests()
    loop = asyncio.new_event_loop()
    env = SimpleNamespace(db=db, path=path, main=main_module, loop=loop, drains=drains, task=None)
    try:
        yield env
    finally:
        if env.task is not None and not env.task.done():
            env.task.cancel()
            try:
                loop.run_until_complete(env.task)
            except BaseException:
                pass
        loop.close()
        runtime_shutdown.reset_for_tests()
        runtime_supervisor.reset_for_tests()


@pytest.fixture
def production(monkeypatch):
    from app.core.config import settings

    monkeypatch.setattr(type(settings), "production", property(lambda self: True))


def start(env):
    """What ``_startup_run_manager`` does, then wait (as the background-job startup does) for the verdict."""
    env.task = env.loop.create_task(env.main.runner_loop())
    env.main.runner_service.task = env.task
    return env.loop.run_until_complete(env.main._await_runtime_ownership(timeout_s=10))


def finish(env, timeout=5.0):
    env.loop.run_until_complete(asyncio.wait_for(env.task, timeout))


def write_foreign_lease(db):
    now = datetime.now(timezone.utc).isoformat()
    with db.connect() as conn:
        conn.execute("INSERT OR REPLACE INTO runtime_ownership VALUES (?,?,?,?,?,?,?,?,?,NULL,NULL)",
                     (TRADING_SCHEDULER, lease_database_path(db.path), "own_other", "rts_other", 999_999,
                      OTHER_HOST, now, now, "production"))


def health(env):
    from fastapi.testclient import TestClient

    return TestClient(env.main.app).get("/health").json()


class _PolicyService:
    """The two calls get_effective_policy makes on BotInstanceService, on the temporary database."""

    def __init__(self, db, instance):
        from app.core.bot_instance_service import BotInstanceService

        self.db, self._instance = db, instance
        self.get_risk_profile_preset = BotInstanceService.get_risk_profile_preset

    def get_bot_instance(self, _instance_id):
        return self._instance


def effective_policy(env):
    from app.api.bot_instances import get_effective_policy
    from app.models.bot_instance_models import BotInstance

    instance = BotInstance(id="bot-1", user_id="user-1", broker_account_id="acct-1", market_type="CRYPTO",
                           strategy_id="cati", strategy_version="1.0.0", risk_level="balanced",
                           symbols=["BTCUSDT"], timeframes=["15m"], allocation_type="fixed_amount",
                           allocation_value=50.0, mode="paper", capital_allocation=500.0,
                           capital_allocation_type="fixed_amount")
    return get_effective_policy("bot-1", user={"id": "user-1"}, service=_PolicyService(env.db, instance),
                                _perm="bot:read")


def supervise(env, *, uptime=0.0):
    from app.ops import runtime_supervisor as sup

    supervisor = sup.RuntimeSupervisor(observe=lambda: sup.observe_runtime(time.monotonic() - uptime),
                                       notify=lambda _m: None, exit_process=lambda _c: None)
    verdict = supervisor.tick()
    return supervisor.last_observation, verdict, supervisor.snapshot()


# ── production: this process holds the lease ────────────────────────────────


def test_before_the_loop_starts_no_background_job_may_run(runtime, production):
    assert runtime.main._may_run_background_jobs() == (False, "RUNNER_NOT_STARTED")
    assert runtime.main.runner_status()["owns_runtime"] is False


def test_while_acquisition_is_pending_the_gate_says_so(runtime, production, monkeypatch):
    from app.ops.runtime_ownership import RuntimeOwnership

    seen, real = [], RuntimeOwnership.acquire

    def observing(self):
        seen.append(runtime.main._may_run_background_jobs())
        return real(self)

    monkeypatch.setattr(RuntimeOwnership, "acquire", observing)
    assert start(runtime) == (True, "RUNTIME_OWNER")
    assert seen == [(False, "OWNERSHIP_PENDING")]


def test_the_owner_is_reported_as_the_owner_by_every_reader(runtime, production):
    assert start(runtime) == (True, "RUNTIME_OWNER")
    row = lease_row(runtime.db)
    assert row["released_at"] is None and row["pid"] == os.getpid() and row["hostname"] == socket.gethostname()
    assert row["runtime_session_id"] == SESSION

    assert runtime.main._may_run_background_jobs() == (True, "RUNTIME_OWNER")
    status = runtime.main.runner_status()
    assert set(status) == PRODUCTION_STATUS_KEYS
    assert status["owns_runtime"] is True and status["running"] is True and status["bots"] == []
    assert (status["mode"], status["engine"], status["broker_execution_scope"]) == ("live", "CATI", "ACCOUNT_SCOPED")
    assert status["legacy_instances_scheduled"] is False
    assert runtime.main.runner_status_endpoint() == status
    assert health(runtime)["components"]["runtime_owns_lease"] is True

    policy = effective_policy(runtime)
    assert policy["runner_policy_hash"] is None and policy["policy_stale"] is False

    obs, verdict, snapshot = supervise(runtime)
    assert obs["owns_runtime"] is True and obs["ownership_reason"] == "ACQUIRED"
    assert obs["lease_renew_failures"] == 0 and obs["runner_task_alive"] is True
    assert obs["lease"]["held"] and obs["lease"]["ours"] and obs["lease"]["runtime_session_id"] == SESSION
    assert verdict.state == "STARTING" and verdict.faults == ()     # no CATI production task yet
    runtime.main.app.state.cati_production_task = runtime.loop.create_future()   # a task still running
    obs, verdict, snapshot = supervise(runtime)
    assert verdict.state == "HEALTHY" and verdict.faults == () and obs["production_task_alive"] is True
    assert snapshot["lease"] == {"owned": True, "held_by_this_process": True,
                                 "heartbeat_age_seconds": snapshot["lease"]["heartbeat_age_seconds"],
                                 "renew_failures": 0}
    assert snapshot["scheduler"]["runner_loop_alive"] is True
    # past the startup grace, the owner without a production task is a fault -- the ownership itself is not
    runtime.main.app.state.cati_production_task = None
    _obs, late, _snap = supervise(runtime, uptime=10_000)
    assert late.faults == ("CATI_PRODUCTION_TASK_NOT_RUNNING",)


def test_graceful_shutdown_drains_under_the_lease_then_releases_it(runtime, production):
    from app.ops.runtime_shutdown import quiesce

    assert start(runtime)[0] is True
    report = quiesce(reason="APPLICATION_SHUTDOWN", timeout_s=2.0)
    finish(runtime)                                       # the scheduler loop ends on its own

    assert runtime.drains == [{"lease_held_by_us": True, "entries_stopped": True, "runner_service_running": False}]
    assert report.to_dict() | {"started_at": None, "completed_at": None} == {
        "reason": "APPLICATION_SHUTDOWN", "started_at": None, "completed_at": None, "clean": True,
        "entries_stopped": True, "scheduler_stopped": True, "cycle_drained": True,
        "production_cycle_drained": True, "lease_released": True, "session_closed": True,
        "runtime_session_id": SESSION, "lease_owner_pid": os.getpid(), "errors": []}
    row = lease_row(runtime.db)
    assert row["released_at"] is not None and row["release_reason"] == "APPLICATION_SHUTDOWN"
    with runtime.db.connect() as conn:
        assert conn.execute("SELECT status FROM runtime_sessions WHERE runtime_session_id=?",
                            (SESSION,)).fetchone()["status"] == "STOPPED"
    # after the stop: nothing claims the runtime any more
    assert runtime.main.runner_status()["owns_runtime"] is False
    obs, verdict, _snap = supervise(runtime)
    assert obs["owns_runtime"] is False and obs["stopping"] is True and verdict.state == "STOPPING"
    assert quiesce(reason="SECOND") is report             # idempotent


def test_a_released_lease_is_taken_by_the_replacement_process(runtime, production):
    from app.ops.runtime_ownership import RuntimeOwnership
    from app.ops.runtime_shutdown import quiesce

    assert start(runtime)[0] is True
    first_owner = lease_row(runtime.db)["runtime_owner_id"]
    quiesce(reason="RESTART", timeout_s=2.0)
    finish(runtime)
    replacement = RuntimeOwnership(runtime.db, database_path=runtime.db.path, database_role="production")
    assert replacement.acquire().acquired is True
    assert lease_row(runtime.db)["runtime_owner_id"] not in (None, first_owner)


def test_losing_the_lease_stops_the_loop_and_every_reader_sees_it(runtime, production):
    assert start(runtime)[0] is True
    with runtime.db.connect() as conn:
        conn.execute("UPDATE runtime_ownership SET runtime_owner_id='own_thief' WHERE lease_name=?",
                     (TRADING_SCHEDULER,))
    finish(runtime, timeout=15.0)                         # the next renewal (<= one interval) notices
    assert runtime.main.runner_status()["owns_runtime"] is False
    obs, verdict, _snap = supervise(runtime, uptime=10_000)
    assert obs["owns_runtime"] is False and obs["runner_task_alive"] is False
    assert verdict.faults == ("RUNNER_LOOP_EXITED",)
    assert lease_row(runtime.db)["runtime_owner_id"] == "own_thief"


# ── production: another process holds the lease ─────────────────────────────


def test_a_process_refused_the_lease_reports_it_and_leaves_the_holder_alone(runtime, production):
    from app.ops.runtime_shutdown import quiesce

    write_foreign_lease(runtime.db)
    allowed, reason = start(runtime)
    finish(runtime)                                       # REFUSED: the loop never schedules
    assert (allowed, reason) == (False, "NOT_RUNTIME_OWNER:RUNTIME_OWNERSHIP_HELD_BY_ANOTHER_PROCESS")
    assert runtime.main._may_run_background_jobs() == (False, reason)
    assert runtime.main.runner_status()["owns_runtime"] is False
    assert health(runtime)["components"]["runtime_owns_lease"] is False

    obs, verdict, snapshot = supervise(runtime, uptime=10_000)
    assert obs["owns_runtime"] is False
    assert obs["ownership_reason"] == "RUNTIME_OWNERSHIP_HELD_BY_ANOTHER_PROCESS"
    assert obs["runner_task_alive"] is False and obs["lease"]["held"] and not obs["lease"]["ours"]
    assert verdict.faults == ("RUNNER_LOOP_EXITED",)
    assert snapshot["lease"]["owned"] is False and snapshot["lease"]["held_by_this_process"] is False

    report = quiesce(reason="OPERATOR_STOP", timeout_s=1.0)
    assert report.scheduler_stopped and not report.lease_released and report.session_closed
    assert lease_row(runtime.db)["runtime_owner_id"] == "own_other"
    assert lease_row(runtime.db)["released_at"] is None


# ── outside production: the per-bot simulation stays on MultiBotRunner ──────


def test_outside_production_the_simulation_runner_hosts_the_lease_and_its_bots(runtime):
    from app.ops.runtime_shutdown import quiesce
    from app.runner.multi_runner import MultiBotRunner

    assert start(runtime) == (True, "RUNTIME_OWNER")
    multi = runtime.main.runner_service.multi_runner
    assert isinstance(multi, MultiBotRunner)              # the non-production simulation host
    status = runtime.main.runner_status()
    assert set(status) == SIMULATION_STATUS_KEYS
    assert status["multi_bot_runner_initialized"] is True and status["iteration"] >= 1

    with runtime.db.connect() as conn:
        conn.execute("INSERT INTO bot_instances (id, user_id, broker_account_id, market_type, strategy_id, "
                     "strategy_version, symbols_json, timeframes_json, mode, status, created_at, updated_at) "
                     "VALUES ('bot-1','user-1','acct-1','CRYPTO','cati','1.0.0','[\"BTCUSDT\"]','[\"15m\"]',"
                     "'paper','paused',?,?)", (datetime.now(timezone.utc).isoformat(),) * 2)
    cached = SimpleNamespace(effective_policy_hash="hash-old", initialization_status="READY",
                             initialization_report=lambda: {"components": {"executor": "READY"}},
                             _stop_requested=False)
    multi._runners["bot-1"] = cached
    bot = runtime.main.runner_status()["bots"][0]
    assert (bot["id"], bot["runner_present"], bot["runner_policy_hash"]) == ("bot-1", True, "hash-old")
    assert bot["runner_initialization_status"] == "READY" and bot["runner_components"] == {"executor": "READY"}
    policy = effective_policy(runtime)
    assert policy["runner_policy_hash"] == "hash-old" and policy["policy_stale"] is True

    report = quiesce(reason="APPLICATION_SHUTDOWN", timeout_s=2.0)
    finish(runtime)
    assert cached._stop_requested is True                 # each cached runner is asked to stop, never flattened
    assert report.clean and report.lease_released
    assert lease_row(runtime.db)["release_reason"] == "APPLICATION_SHUTDOWN"


# ── the migration itself (E-05): production ownership without the legacy runner ──
# These pin the change, so unlike the tests above they did not hold before it.


def test_production_holds_the_lease_without_building_the_legacy_runner(runtime, production, monkeypatch):
    from app.runner.multi_runner import MultiBotRunner
    from app.trading_intelligence.integration.runtime_owner import RuntimeOwnerHost

    def refuse(*_a, **_k):
        raise AssertionError("production must not build MultiBotRunner")

    monkeypatch.setattr(MultiBotRunner, "__init__", refuse)
    assert start(runtime) == (True, "RUNTIME_OWNER")
    service = runtime.main.runner_service
    assert isinstance(service.runtime_owner, RuntimeOwnerHost) and service.multi_runner is None
    assert service.runtime_owner.iteration >= 1
    assert lease_row(runtime.db)["runtime_owner_id"] == service.runtime_owner.owner_loop.runtime_owner_id
    from app.ops.runtime_shutdown import quiesce

    assert quiesce(reason="APPLICATION_SHUTDOWN", timeout_s=2.0).clean
    finish(runtime)
    assert service.runtime_owner.running is False and service.runtime_owner.stop_requested is True


def test_the_accessor_reads_a_host_without_an_owner_loop_through_the_legacy_surface():
    from app.trading_intelligence.integration import runtime_owner

    held = SimpleNamespace(pid=4242, renew_failures=3)
    released = []
    legacy = SimpleNamespace(owns_runtime=True, ownership_decided=True, ownership_reason="ACQUIRED",
                             _ownership=held, running=True, _stop_requested=False, iteration=7,
                             _runners={"bot-1": "cached"},
                             release_runtime_ownership=lambda *, reason: released.append(reason))
    assert runtime_owner.host_of(SimpleNamespace(multi_runner=legacy)) is legacy
    assert runtime_owner.host_of(SimpleNamespace(runtime_owner="cati", multi_runner=legacy)) == "cati"
    assert runtime_owner.host_of(None) is None
    assert runtime_owner.ownership(legacy) == runtime_owner.OwnershipView(True, True, "ACQUIRED", 4242, 3)
    assert runtime_owner.ownership(None) == runtime_owner.OwnershipView()
    assert runtime_owner.iteration(legacy) == 7 and runtime_owner.simulation_runner(legacy, "bot-1") == "cached"
    runtime_owner.stop_scheduler(legacy)
    runtime_owner.release(legacy, reason="R")
    assert (legacy.running, legacy._stop_requested, released) == (False, True, ["R"])
    assert runtime_owner.simulation_runners(None) == {} and runtime_owner.iteration(None) == 0
