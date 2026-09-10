"""Runtime activation: one owner, decided before anything is written.

The defect these pin is uvicorn's sequencing. ``Server.startup()`` awaits
``lifespan.startup()`` and only afterwards creates the socket, so a second
``uvicorn app.main:app --port 9000`` ran the *entire* trading startup --
canonical runtime session, signal scheduler, MultiBotRunner, bot restore --
and only then died on Errno 10048, leaving evidence behind. The runtime
database had 48 sessions marked RUNNING against one live process.

The port and the lease are separate authorities and neither subsumes the other:
a second backend on a different port against the same database is still a
duplicate scheduler, and a foreign process on 9000 is not a scheduler at all
but still blocks the bind.
"""
from __future__ import annotations

import os
import socket
from datetime import datetime, timedelta, timezone

import pytest

from app.ops.runtime_ownership import (
    LEASE_STALE_SECONDS,
    TRADING_SCHEDULER,
    ensure_ownership_schema,
    lease_is_stale,
    pid_is_alive,
)
from app.ops.runtime_preflight import (
    PreflightStatus,
    port_is_free,
    preflight,
    render_refusal,
    should_skip,
)
from shared_lib.persistence.db import DB
from shared_lib.persistence.migrations import migrate

DEAD_PID = 999_999  # far above Windows' and Linux' usual allocation


@pytest.fixture
def db():
    database = DB(":memory:")
    migrate(database)
    ensure_ownership_schema(database)
    return database


@pytest.fixture
def free_port():
    """A port nothing is listening on, chosen by the OS."""
    probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    probe.bind(("127.0.0.1", 0))
    port = probe.getsockname()[1]
    probe.close()
    return port


@pytest.fixture
def occupied_port():
    """A port this test process is genuinely listening on."""
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.bind(("0.0.0.0", 0))
    listener.listen(1)
    yield listener.getsockname()[1]
    listener.close()


def hold_lease(db, *, pid: int, heartbeat: datetime | None = None,
               session_id: str = "rts_test"):
    heartbeat = heartbeat or datetime.now(timezone.utc)
    with db.connect() as conn:
        conn.execute(
            """INSERT INTO runtime_ownership
               (lease_name, database_path, runtime_owner_id, runtime_session_id,
                pid, hostname, started_at, heartbeat_at, database_role,
                released_at, release_reason)
               VALUES (?,?,?,?,?,?,?,?,?,NULL,NULL)
               ON CONFLICT(lease_name, database_path) DO UPDATE SET
                 pid=excluded.pid, heartbeat_at=excluded.heartbeat_at,
                 runtime_session_id=excluded.runtime_session_id,
                 released_at=NULL, release_reason=NULL""",
            (TRADING_SCHEDULER, os.path.abspath(db.path), "own_test", session_id,
             pid, "test-host", heartbeat.isoformat(), heartbeat.isoformat(),
             "development"),
        )


# ══════════════════════════════════════════════════════════════════════════
# §13 case matrix
# ══════════════════════════════════════════════════════════════════════════


def test_case_a_free_port_free_lease_may_start(db, free_port):
    result = preflight(port=free_port, db=db)
    assert result.status is PreflightStatus.READY
    assert result.may_start


def test_case_b_port_and_lease_held_by_the_same_live_process(db, occupied_port):
    hold_lease(db, pid=os.getpid())
    result = preflight(port=occupied_port, db=db)

    assert result.status is PreflightStatus.ALREADY_RUNNING
    assert not result.may_start
    assert result.lease_pid == os.getpid()
    assert result.lease_owner_alive is True


def test_case_c_dead_lease_holder_and_free_port_is_recoverable(db, free_port):
    hold_lease(db, pid=DEAD_PID)
    result = preflight(port=free_port, db=db)

    assert result.status is PreflightStatus.STALE_LEASE
    assert result.may_start, "a dead owner must not block startup forever"
    assert result.lease_owner_alive is False


def test_case_c_stale_heartbeat_from_a_live_pid_is_also_recoverable(db, free_port):
    old = datetime.now(timezone.utc) - timedelta(seconds=LEASE_STALE_SECONDS + 60)
    hold_lease(db, pid=os.getpid(), heartbeat=old)
    result = preflight(port=free_port, db=db)

    assert result.status is PreflightStatus.STALE_LEASE
    assert result.may_start
    assert result.lease_stale is True


def test_case_d_occupied_port_with_no_lease_fails_closed(db, occupied_port):
    """Something is listening but owns no trading lease. Do not guess."""
    result = preflight(port=occupied_port, db=db)

    assert result.status is PreflightStatus.PORT_OCCUPIED_BY_OTHER_PROCESS
    assert not result.may_start


def test_case_e_port_owner_and_lease_owner_disagree_fails_closed(db, occupied_port):
    # The port is held by this process; the lease is held by another live one.
    other = os.getppid() or 1
    if other == os.getpid():
        pytest.skip("cannot obtain a second live pid in this environment")
    hold_lease(db, pid=other)

    result = preflight(port=occupied_port, db=db)
    assert result.status is PreflightStatus.OWNERSHIP_CONFLICT
    assert not result.may_start


def test_case_f_same_database_on_a_different_port_is_refused(db, free_port):
    """The lease is scoped to the database, not the port.

    A second backend serving its API elsewhere is still a second scheduler
    against one database, which is the thing the lease exists to prevent.
    """
    hold_lease(db, pid=os.getpid())
    result = preflight(port=free_port, db=db)

    assert result.status is PreflightStatus.ALREADY_RUNNING
    assert not result.may_start
    assert "second scheduler" in result.message


def test_a_healthy_owner_is_never_stolen(db, occupied_port):
    hold_lease(db, pid=os.getpid())
    for _ in range(3):
        assert not preflight(port=occupied_port, db=db).may_start


# ══════════════════════════════════════════════════════════════════════════
# Refusal happens before anything is written
# ══════════════════════════════════════════════════════════════════════════


def test_preflight_writes_nothing(db, occupied_port):
    hold_lease(db, pid=os.getpid())

    def counts():
        with db.connect() as conn:
            return (
                conn.execute("SELECT COUNT(*) FROM runtime_sessions").fetchone()[0],
                conn.execute("SELECT COUNT(*) FROM bot_runs").fetchone()[0],
                conn.execute("SELECT COUNT(*) FROM trading_cycles").fetchone()[0],
                conn.execute("SELECT COUNT(*) FROM runtime_ownership").fetchone()[0],
            )

    before = counts()
    preflight(port=occupied_port, db=db)
    assert counts() == before


def test_the_refusal_tells_the_operator_what_is_running(db, occupied_port):
    hold_lease(db, pid=os.getpid(), session_id="rts_visible")
    text = render_refusal(preflight(port=occupied_port, db=db))

    assert "[COSMICFORGE_RUNTIME]" in text
    assert "rts_visible" in text
    assert str(os.getpid()) in text
    assert "No second scheduler was started." in text
    assert "No runtime evidence was written." in text
    assert "trading_runtime.ps1 stop" in text


def test_errno_10048_is_not_how_the_operator_finds_out(db, occupied_port):
    """The whole point: refuse up front, not after a full startup."""
    result = preflight(port=occupied_port, db=db)
    assert not result.may_start
    assert "10048" not in result.message


# ══════════════════════════════════════════════════════════════════════════
# The escape hatch, and test isolation
# ══════════════════════════════════════════════════════════════════════════


def test_test_mode_is_never_refused():
    """conftest sets COSMICFORGE_TEST_MODE; importing app.main must still work."""
    assert os.environ.get("COSMICFORGE_TEST_MODE")
    assert should_skip()


def test_the_skip_flag_is_explicit(monkeypatch):
    monkeypatch.delenv("COSMICFORGE_TEST_MODE", raising=False)
    monkeypatch.delenv("COSMICFORGE_SKIP_RUNTIME_PREFLIGHT", raising=False)
    assert not should_skip()

    monkeypatch.setenv("COSMICFORGE_SKIP_RUNTIME_PREFLIGHT", "1")
    assert should_skip()


def test_enforce_is_inert_when_skipping(monkeypatch, free_port):
    from app.ops.runtime_preflight import enforce

    monkeypatch.setenv("COSMICFORGE_TEST_MODE", "1")
    result = enforce(port=free_port)
    assert result.may_start


def test_enforce_exits_rather_than_raising_into_the_importer(db, occupied_port, capsys):
    from app.ops.runtime_preflight import enforce

    os.environ.pop("COSMICFORGE_TEST_MODE", None)
    try:
        hold_lease(db, pid=os.getpid())
        with pytest.raises(SystemExit) as exit_info:
            enforce(preflight(port=occupied_port, db=db), port=occupied_port)
        assert exit_info.value.code == 1
        assert "[COSMICFORGE_RUNTIME]" in capsys.readouterr().err
    finally:
        os.environ["COSMICFORGE_TEST_MODE"] = "1"


# ══════════════════════════════════════════════════════════════════════════
# Port and lease primitives
# ══════════════════════════════════════════════════════════════════════════


def test_port_is_free_answers_the_bind_question(free_port, occupied_port):
    assert port_is_free(free_port)
    assert not port_is_free(occupied_port)


def test_liveness_and_staleness_share_the_lease_policy():
    assert pid_is_alive(os.getpid())
    assert not pid_is_alive(DEAD_PID)
    assert not pid_is_alive(None)

    assert lease_is_stale(None)
    assert lease_is_stale("not-a-timestamp")
    assert not lease_is_stale(datetime.now(timezone.utc).isoformat())
    assert lease_is_stale(
        (datetime.now(timezone.utc) - timedelta(seconds=LEASE_STALE_SECONDS + 5)).isoformat()
    )


def test_a_preflight_error_never_silently_permits_startup(occupied_port):
    class Broken:
        path = "/nonexistent/broken.db"

        def connect(self):
            raise RuntimeError("database gone")

    result = preflight(port=occupied_port, db=Broken())
    assert result.status is PreflightStatus.ERROR
    assert not result.may_start


# ══════════════════════════════════════════════════════════════════════════
# Graceful stop
# ══════════════════════════════════════════════════════════════════════════


class FakeOwnership:
    def __init__(self, db):
        self.db = db
        self.released_reason = None

    def release(self, *, reason="SHUTDOWN"):
        self.released_reason = reason
        with self.db.connect() as conn:
            conn.execute(
                "UPDATE runtime_ownership SET released_at=?, release_reason=? "
                "WHERE lease_name=?",
                (datetime.now(timezone.utc).isoformat(), reason, TRADING_SCHEDULER),
            )


class FakeMultiRunner:
    def __init__(self, db):
        self.running = True
        self._stop_requested = False
        self._runners = {}
        self._ownership = FakeOwnership(db)
        self.flattened = []

    def release_runtime_ownership(self, *, reason="SHUTDOWN"):
        self._ownership.release(reason=reason)


class FakeRunnerService:
    def __init__(self, multi):
        self.running = True
        self.multi_runner = multi


@pytest.fixture
def quiesce_env(db, monkeypatch, tmp_path):
    """Point the shutdown path at a temp database and a fake runner."""
    import app.main as main_module
    from app.ops import runtime_shutdown

    runtime_shutdown.reset_for_tests()

    path = str(tmp_path / "runtime.db")
    real = DB(path)
    migrate(real)
    ensure_ownership_schema(real)
    with real.connect() as conn:
        conn.execute(
            "INSERT INTO runtime_sessions (runtime_session_id, started_at, status) "
            "VALUES (?,?,?)",
            ("rts_stop_test", datetime.now(timezone.utc).isoformat(), "RUNNING"),
        )
    hold_lease(real, pid=os.getpid(), session_id="rts_stop_test")

    monkeypatch.setenv("DATABASE_URL", "sqlite:///" + path.replace("\\", "/"))
    multi = FakeMultiRunner(real)
    monkeypatch.setattr(main_module, "runner_service", FakeRunnerService(multi), raising=False)
    monkeypatch.setattr(main_module, "RUNTIME_SESSION_ID", "rts_stop_test", raising=False)
    yield real, multi
    runtime_shutdown.reset_for_tests()


def test_graceful_stop_releases_the_lease_and_closes_the_session(quiesce_env):
    from app.ops.runtime_shutdown import quiesce

    real, multi = quiesce_env
    multi.running = False  # the loop has already noticed and stopped

    report = quiesce(reason="OPERATOR_STOP", timeout_s=1.0)

    assert report.entries_stopped
    assert report.scheduler_stopped
    assert report.lease_released, report.errors
    assert report.session_closed, report.errors
    assert report.clean, report.errors

    with real.connect() as conn:
        lease = conn.execute(
            "SELECT released_at, release_reason FROM runtime_ownership"
        ).fetchone()
        session = conn.execute(
            "SELECT status, stopped_at FROM runtime_sessions "
            "WHERE runtime_session_id='rts_stop_test'"
        ).fetchone()
    assert lease["released_at"] is not None
    assert lease["release_reason"] == "OPERATOR_STOP"
    assert session["status"] == "STOPPED"
    assert session["stopped_at"] is not None


def test_graceful_stop_never_flattens_a_position(quiesce_env):
    """A process stopping is not an exit signal."""
    import inspect

    from app.ops import runtime_shutdown

    real, multi = quiesce_env
    multi.running = False
    quiesce = runtime_shutdown.quiesce
    quiesce(reason="OPERATOR_STOP", timeout_s=1.0)

    assert multi.flattened == []
    source = runtime_shutdown._quiesce_inner.__doc__ or ""
    body = inspect.getsource(runtime_shutdown._quiesce_inner)
    for forbidden in ("close_position", "flatten", "activate_kill_switch"):
        assert forbidden not in body, f"shutdown must not {forbidden}"


def test_quiescing_twice_does_the_work_once(quiesce_env):
    from app.ops.runtime_shutdown import quiesce

    real, multi = quiesce_env
    multi.running = False

    first = quiesce(reason="FIRST", timeout_s=1.0)
    second = quiesce(reason="SECOND", timeout_s=1.0)

    assert second is first
    assert first.reason == "FIRST"


def test_a_process_that_never_owned_anything_still_reports_clean(monkeypatch, tmp_path):
    import app.main as main_module
    from app.ops import runtime_shutdown

    runtime_shutdown.reset_for_tests()
    monkeypatch.setattr(main_module, "runner_service", None, raising=False)
    monkeypatch.setattr(main_module, "RUNTIME_SESSION_ID", None, raising=False)

    report = runtime_shutdown.quiesce(reason="NO_RUNTIME", timeout_s=0.5)
    assert report.lease_released
    assert report.session_closed
    runtime_shutdown.reset_for_tests()


def test_the_stop_file_is_where_the_operator_scripts_put_it():
    from app.ops.runtime_shutdown import STOP_FILE

    assert STOP_FILE.replace("\\", "/").endswith("logs/runtime/STOP")


# ══════════════════════════════════════════════════════════════════════════
# Wiring: the guard must actually be reachable
# ══════════════════════════════════════════════════════════════════════════


def _module_source(module) -> str:
    from pathlib import Path

    return Path(module.__file__).read_text(encoding="utf-8")


def test_the_preflight_runs_at_import_before_any_trading_startup():
    """It has to be at import: uvicorn runs lifespan startup before binding."""
    import app.main as main_module

    source = _module_source(main_module)
    guard = source.index("_enforce_runtime_preflight")
    for later in ("_open_runtime_session_once", "_startup_run_manager", "MultiBotRunner()"):
        assert guard < source.index(later), (
            f"preflight must run before {later}"
        )


def test_the_shutdown_hook_quiesces_before_cancelling_the_task():
    import app.main as main_module

    source = _module_source(main_module)
    shutdown = source.index("async def _shutdown_run_manager")
    body = source[shutdown:shutdown + 3000]
    assert "quiesce(reason=" in body
    assert body.index("quiesce(reason=") < body.index("runner_service.task.cancel()")


def test_the_launcher_no_longer_force_kills_as_its_normal_path():
    from pathlib import Path

    launcher = Path(__file__).resolve().parents[3] / "scripts" / "start_trading_runtime.ps1"
    text = launcher.read_text(encoding="utf-8")

    stop_block = text[text.index("stop file detected"):]
    graceful = stop_block.index("waiting for graceful shutdown")
    forced = stop_block.index("FORCED_RUNTIME_TERMINATION")
    assert graceful < forced, "graceful shutdown must be attempted first"
    assert "GracefulStopSeconds" in text


def test_the_operator_cli_exists_with_the_four_commands():
    from pathlib import Path

    cli = Path(__file__).resolve().parents[3] / "scripts" / "trading_runtime.ps1"
    text = cli.read_text(encoding="utf-8")
    for command in ("status", "start", "stop", "restart"):
        assert f"'{command}'" in text


# ══════════════════════════════════════════════════════════════════════════
# §14 Background jobs belong to the owner, not to any process
# ══════════════════════════════════════════════════════════════════════════


class OwnershipStub:
    """Stands in for MultiBotRunner's ownership surface."""

    def __init__(self, *, owns: bool, decided: bool, reason: str):
        self.owns_runtime = owns
        self.ownership_decided = decided
        self.ownership_reason = reason


@pytest.fixture
def not_skipping(monkeypatch):
    """Look like a serving process rather than a test import."""
    monkeypatch.delenv("COSMICFORGE_TEST_MODE", raising=False)
    monkeypatch.delenv("COSMICFORGE_SKIP_RUNTIME_PREFLIGHT", raising=False)


def _set_runner(monkeypatch, multi):
    import app.main as main_module

    service = type("S", (), {"multi_runner": multi})()
    monkeypatch.setattr(main_module, "runner_service", service, raising=False)
    return main_module


def test_the_owner_may_register_background_jobs(monkeypatch, not_skipping):
    main_module = _set_runner(
        monkeypatch, OwnershipStub(owns=True, decided=True, reason="ACQUIRED")
    )
    allowed, reason = main_module._may_run_background_jobs()
    assert allowed
    assert reason == "RUNTIME_OWNER"


def test_a_process_that_lost_the_lease_registers_nothing(monkeypatch, not_skipping):
    """Passing preflight is not the same as owning the runtime."""
    main_module = _set_runner(
        monkeypatch,
        OwnershipStub(owns=False, decided=True, reason="HELD_BY_ANOTHER_PROCESS"),
    )
    allowed, reason = main_module._may_run_background_jobs()
    assert not allowed
    assert "NOT_RUNTIME_OWNER" in reason
    assert "HELD_BY_ANOTHER_PROCESS" in reason


def test_no_runner_means_no_background_jobs(monkeypatch, not_skipping):
    main_module = _set_runner(monkeypatch, None)
    allowed, reason = main_module._may_run_background_jobs()
    assert not allowed
    assert reason == "RUNNER_NOT_STARTED"


def test_ownership_not_yet_decided_is_not_a_refusal(monkeypatch, not_skipping):
    main_module = _set_runner(
        monkeypatch, OwnershipStub(owns=False, decided=False, reason="NOT_ACQUIRED")
    )
    allowed, reason = main_module._may_run_background_jobs()
    assert not allowed
    assert reason == "OWNERSHIP_PENDING", "must keep waiting, not refuse forever"


def test_waiting_for_ownership_resolves_once_the_lease_is_taken(monkeypatch, not_skipping):
    import asyncio

    stub = OwnershipStub(owns=False, decided=False, reason="NOT_ACQUIRED")
    main_module = _set_runner(monkeypatch, stub)

    async def scenario():
        async def acquire_shortly():
            await asyncio.sleep(0.2)
            stub.owns_runtime = True
            stub.ownership_decided = True
            stub.ownership_reason = "ACQUIRED"

        asyncio.create_task(acquire_shortly())
        return await main_module._await_runtime_ownership(timeout_s=5.0)

    allowed, reason = asyncio.run(scenario())
    assert allowed
    assert reason == "RUNTIME_OWNER"


def test_waiting_stops_as_soon_as_the_answer_is_no(monkeypatch, not_skipping):
    import asyncio
    import time

    main_module = _set_runner(
        monkeypatch, OwnershipStub(owns=False, decided=True, reason="HELD_BY_ANOTHER")
    )
    started = time.monotonic()
    allowed, reason = asyncio.run(main_module._await_runtime_ownership(timeout_s=30.0))

    assert not allowed
    assert "NOT_RUNTIME_OWNER" in reason
    assert time.monotonic() - started < 5, "a decided refusal must not wait out the timeout"


def test_test_imports_are_not_treated_as_a_competing_process(monkeypatch):
    """The existing scheduler tests call the coroutine directly; keep them valid."""
    import app.main as main_module

    monkeypatch.setenv("COSMICFORGE_TEST_MODE", "1")
    allowed, reason = main_module._may_run_background_jobs()
    assert allowed
    assert reason == "PREFLIGHT_SKIPPED"


def test_every_background_job_sits_behind_the_ownership_gate():
    """All four jobs are registered by the one gated coroutine."""
    import app.main as main_module

    source = _module_source(main_module)
    gate = source.index("async def _startup_background_jobs")
    registration = source.index("async def _startup_signal_scheduler")
    assert gate < registration

    body = source[registration:]
    for job_id in (
        "signal_expiry",
        "organic_dataset_nightly",
        "ml_monthly_retrain",
        "daily_paper_validation_monitor",
    ):
        assert f'id="{job_id}"' in body, f"{job_id} must be registered behind the gate"

    # and the gate must be the only thing that starts it
    assert source.count("await _startup_signal_scheduler()") == 1
    assert '@app.on_event("startup")\nasync def _startup_signal_scheduler' not in source


# ══════════════════════════════════════════════════════════════════════════
# §16/§17 Database identity is declared, and never chosen for the operator
# ══════════════════════════════════════════════════════════════════════════


def test_the_deployment_declares_what_it_is():
    """environment_name="unknown" was written on all 48 existing sessions."""
    from app.core.config import Settings

    assert hasattr(Settings(), "ENVIRONMENT_NAME")


def test_the_runtime_session_records_the_declared_environment():
    import app.main as main_module

    source = _module_source(main_module)
    assert 'getattr(settings, "ENVIRONMENT_NAME"' in source
    assert 'getattr(settings, "ENVIRONMENT", "unknown")' not in source, (
        "no setting ever defined ENVIRONMENT; it always resolved to 'unknown'"
    )


def test_the_configured_role_is_what_gets_recorded(monkeypatch):
    """The role is declared in config, never inferred from the filename."""
    from app.ops import database_registry

    assert database_registry.classify(
        __import__("pathlib").Path("cosmicforge.db"),
        active_path=__import__("pathlib").Path("cosmicforge.db"),
    ) == database_registry.ACTIVE


def test_every_candidate_database_gets_a_truthful_label(tmp_path):
    from app.ops import database_registry as reg

    active = tmp_path / "cosmicforge.db"
    cases = {
        "cosmicforge.db": reg.ACTIVE,
        "cosmicforge-LAPTOP-5B3QOQDJ.db": reg.FORENSIC,
        "cosmicforge-DESKTOP-ABC.db": reg.FORENSIC,
        "cosmicforge.pre_capital_recovery.20260907T203236Z.db": reg.BACKUP,
        "phase12_paper_validation.db": reg.VALIDATION,
        "shadow_research.db": reg.RESEARCH,
        "something_else.db": reg.STALE_COPY,
    }
    for name, expected in cases.items():
        assert reg.classify(tmp_path / name, active_path=active) == expected, name


def test_a_validation_database_is_not_called_a_stale_copy(tmp_path):
    from app.ops import database_registry as reg

    label = reg.classify(
        tmp_path / "phase12_paper_validation.db", active_path=tmp_path / "cosmicforge.db"
    )
    assert label == reg.VALIDATION
    assert label != reg.STALE_COPY


def test_registration_never_deletes_or_switches(tmp_path):
    """Classification is advisory. The active file comes from DATABASE_URL."""
    import inspect

    from app.ops import database_registry as reg

    source = inspect.getsource(reg)
    for destructive in ("os.remove", "unlink", "shutil.move", "rename", "DATABASE_URL ="):
        assert destructive not in source, f"registry must not {destructive}"

    active = tmp_path / "cosmicforge.db"
    active.write_bytes(b"")
    (tmp_path / "cosmicforge-LAPTOP-X.db").write_bytes(b"")

    registry_home = tmp_path / "registry"
    registry_home.mkdir()
    database = DB(str(registry_home / "registry.db"))
    migrate(database)
    rows = reg.register_database_candidates(
        database, active_path=str(active), database_role="paper"
    )
    assert {r["classification"] for r in rows} == {reg.ACTIVE, reg.FORENSIC}
    assert active.exists() and (tmp_path / "cosmicforge-LAPTOP-X.db").exists()
    assert [r for r in rows if r["classification"] == reg.ACTIVE][0]["database_role"] == "paper"
    assert all(
        r["database_role"] is None for r in rows if r["classification"] != reg.ACTIVE
    ), "a non-active file must never carry the runtime role"


# ══════════════════════════════════════════════════════════════════════════
# §15 Runtime session evidence tells the truth about what is running
# ══════════════════════════════════════════════════════════════════════════


def _insert_session(db, session_id: str, pid: int, status: str = "RUNNING"):
    with db.connect() as conn:
        conn.execute(
            "INSERT INTO runtime_sessions (runtime_session_id, started_at, pid, status) "
            "VALUES (?,?,?,?)",
            (session_id, datetime.now(timezone.utc).isoformat(), pid, status),
        )


def test_sessions_whose_process_is_gone_stop_claiming_to_be_running(db):
    from app.evidence.writers import reap_abandoned_sessions

    _insert_session(db, "rts_dead", DEAD_PID)
    assert reap_abandoned_sessions(db) == 1

    with db.connect() as conn:
        row = conn.execute(
            "SELECT status, stopped_at, shutdown_reason FROM runtime_sessions "
            "WHERE runtime_session_id='rts_dead'"
        ).fetchone()
    assert row["status"] == "ABANDONED"
    assert row["shutdown_reason"] == "PROCESS_GONE_NO_CLEAN_SHUTDOWN"


def test_an_abandoned_session_is_not_given_an_invented_stop_time(db):
    """We know it is gone. We do not know when, so we do not say."""
    from app.evidence.writers import reap_abandoned_sessions

    _insert_session(db, "rts_dead", DEAD_PID)
    reap_abandoned_sessions(db)

    with db.connect() as conn:
        stopped_at = conn.execute(
            "SELECT stopped_at FROM runtime_sessions WHERE runtime_session_id='rts_dead'"
        ).fetchone()[0]
    assert stopped_at is None


def test_abandoned_is_a_different_fact_from_stopped(db):
    from app.evidence.writers import close_runtime_session, reap_abandoned_sessions

    _insert_session(db, "rts_clean", os.getpid())
    close_runtime_session(db, "rts_clean", reason="OPERATOR_STOP")
    _insert_session(db, "rts_killed", DEAD_PID)
    reap_abandoned_sessions(db)

    with db.connect() as conn:
        statuses = dict(
            conn.execute(
                "SELECT runtime_session_id, status FROM runtime_sessions"
            ).fetchall()
        )
    assert statuses["rts_clean"] == "STOPPED"
    assert statuses["rts_killed"] == "ABANDONED"


def test_a_live_session_is_never_reaped(db):
    from app.evidence.writers import reap_abandoned_sessions

    _insert_session(db, "rts_live", os.getpid())
    assert reap_abandoned_sessions(db) == 0


def test_the_current_session_is_never_reaped_by_itself(db):
    from app.evidence.writers import reap_abandoned_sessions

    _insert_session(db, "rts_me", DEAD_PID)
    assert reap_abandoned_sessions(db, exclude_session_id="rts_me") == 0


def test_reaping_preserves_every_row(db):
    from app.evidence.writers import reap_abandoned_sessions

    for i in range(5):
        _insert_session(db, f"rts_{i}", DEAD_PID)
    reap_abandoned_sessions(db)

    with db.connect() as conn:
        assert conn.execute("SELECT COUNT(*) FROM runtime_sessions").fetchone()[0] == 5


def test_a_refused_start_opens_no_session_at_all():
    """The 48 RUNNING rows came from duplicate starts that got that far."""
    import app.main as main_module

    source = _module_source(main_module)
    assert source.index("_enforce_runtime_preflight") < source.index(
        "def _open_runtime_session_once"
    )


# ══════════════════════════════════════════════════════════════════════════
# §8 The supervisor must not restart what the operator just stopped
# ══════════════════════════════════════════════════════════════════════════


def test_the_exit_reason_outlives_the_process(tmp_path, monkeypatch):
    from app.ops import runtime_shutdown

    monkeypatch.setattr(runtime_shutdown, "_RUNTIME_LOG_DIR", str(tmp_path))
    monkeypatch.setattr(runtime_shutdown, "STOPPED_MARKER", str(tmp_path / "STOPPED_BY_OPERATOR"))

    runtime_shutdown.record_operator_stop("OPERATOR_STOP_FILE")
    text = (tmp_path / "STOPPED_BY_OPERATOR").read_text(encoding="utf-8")

    assert "OPERATOR_STOP_FILE" in text
    assert f"pid={os.getpid()}" in text


def test_a_leftover_stop_request_does_not_kill_the_next_start(tmp_path, monkeypatch):
    from app.ops import runtime_shutdown

    stop = tmp_path / "STOP"
    marker = tmp_path / "STOPPED_BY_OPERATOR"
    stop.write_text("", encoding="utf-8")
    marker.write_text("", encoding="utf-8")
    monkeypatch.setattr(runtime_shutdown, "STOP_FILE", str(stop))
    monkeypatch.setattr(runtime_shutdown, "STOPPED_MARKER", str(marker))

    runtime_shutdown.clear_stop_signals()
    assert not stop.exists()
    assert not marker.exists()


def test_clearing_stop_signals_is_safe_when_there_are_none(tmp_path, monkeypatch):
    from app.ops import runtime_shutdown

    monkeypatch.setattr(runtime_shutdown, "STOP_FILE", str(tmp_path / "STOP"))
    monkeypatch.setattr(runtime_shutdown, "STOPPED_MARKER", str(tmp_path / "MARK"))
    runtime_shutdown.clear_stop_signals()  # must not raise


def test_the_runtime_does_not_consume_the_stop_request():
    """It polls every 1s, the supervisor every 2s. Deleting it lost the race."""
    import app.main as main_module

    source = _module_source(main_module)
    watcher = source.index("async def _startup_stop_file_watcher")
    body = source[watcher:watcher + 3000]

    assert "record_operator_stop(" in body
    assert "_os.remove(STOP_FILE)" not in body, (
        "consuming the stop file is what made the supervisor see a crash"
    )
    assert "clear_stop_signals()" in body


def test_the_supervisor_asks_why_the_child_exited(tmp_path):
    from pathlib import Path

    launcher = Path(__file__).resolve().parents[3] / "scripts" / "start_trading_runtime.ps1"
    text = launcher.read_text(encoding="utf-8")

    assert "$StoppedMarker" in text
    marker_check = text.index("if (Test-Path $StoppedMarker) {")
    crash_branch = text.index('Write-Step "CRASH: exit code')
    assert marker_check < crash_branch, (
        "the exit reason must be read before the crash-restart decision"
    )
    assert "not restarting" in text[marker_check:crash_branch]


def test_the_operator_cli_clears_a_previous_stop_marker():
    from pathlib import Path

    cli = Path(__file__).resolve().parents[3] / "scripts" / "trading_runtime.ps1"
    text = cli.read_text(encoding="utf-8")

    drop = text.index("New-Item -ItemType File -Path $StopFile")
    clear = text.index("Remove-Item $StoppedMarker")
    assert clear < drop, "a stale marker must be cleared before asking for a new stop"


def test_the_startup_hook_actually_runs_the_registration(monkeypatch, not_skipping):
    """Regression: ``_await_runtime_ownership()`` was called without await.

    The coroutine was never awaited, ``allowed, reason = <coroutine>`` raised
    TypeError inside the task, and the process ran with no signal scheduler,
    no nightly dataset build, no retrain and no daily monitor -- silently,
    because the failure lived in a task nobody inspected.
    """
    import asyncio

    import app.main as main_module

    main_module = _set_runner(
        monkeypatch, OwnershipStub(owns=True, decided=True, reason="ACQUIRED")
    )
    registered: list[str] = []

    async def fake_register():
        registered.append("called")

    monkeypatch.setattr(main_module, "_startup_signal_scheduler", fake_register)

    async def scenario():
        await main_module._startup_background_jobs()
        # let the spawned task run to completion
        pending = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        await asyncio.gather(*pending)

    asyncio.run(scenario())
    assert registered == ["called"]


def test_a_non_owner_startup_registers_nothing_and_does_not_raise(monkeypatch, not_skipping):
    import asyncio

    import app.main as main_module

    main_module = _set_runner(
        monkeypatch, OwnershipStub(owns=False, decided=True, reason="HELD_BY_ANOTHER")
    )
    registered: list[str] = []

    async def fake_register():
        registered.append("called")

    monkeypatch.setattr(main_module, "_startup_signal_scheduler", fake_register)

    async def scenario():
        await main_module._startup_background_jobs()
        pending = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        await asyncio.gather(*pending)

    asyncio.run(scenario())
    assert registered == []


def test_the_supervisor_clears_stale_requests_before_launching():
    """Regression: a marker from the previous stop force-killed a healthy child.

    The supervisor launched, immediately saw the old STOPPED_BY_OPERATOR file,
    decided a shutdown was in progress, waited out the grace period for an exit
    nobody had requested, and then force-killed the runtime mid-cycle -- leaving
    an unreleased lease and a session stuck at RUNNING.
    """
    from pathlib import Path

    launcher = Path(__file__).resolve().parents[3] / "scripts" / "start_trading_runtime.ps1"
    text = launcher.read_text(encoding="utf-8")

    clear = text.index("Remove-Item $StoppedMarker -Force -ErrorAction SilentlyContinue")
    launch = text.index("$proc = Start-Process -FilePath $Python")
    detect = text.index("if ((Test-Path $StopFile) -or (Test-Path $StoppedMarker))")

    assert clear < launch < detect, (
        "stale stop requests must be cleared before the child is launched"
    )


def test_starting_from_the_cli_also_clears_a_stale_request():
    from pathlib import Path

    cli = Path(__file__).resolve().parents[3] / "scripts" / "trading_runtime.ps1"
    text = cli.read_text(encoding="utf-8")
    start = text.index("function Invoke-Start")
    body = text[start:]
    assert "Remove-Item $StoppedMarker" in body[: body.index("if ($Mode -eq 'manual')")]


def test_a_successful_stop_leaves_no_request_behind():
    from pathlib import Path

    cli = Path(__file__).resolve().parents[3] / "scripts" / "trading_runtime.ps1"
    text = cli.read_text(encoding="utf-8")
    ok = text.index("graceful stop complete")
    after = text[ok : ok + 400]
    assert "Remove-Item $StopFile" in after
    assert "Remove-Item $StoppedMarker" in after
