"""Tests can never write the canonical paper database.

The audit of 2026-09-10 found 9,339 test rows in each of
``canonical_trade_decisions`` and ``decision_traces`` in the canonical paper
database -- 849 of them written while the live runtime was running. The
bootstrap now builds a per-session temporary database before any application
import, and ``DB()`` / ``TraceRecorder`` refuse any runtime database outright
under ``COSMICFORGE_TEST_MODE``.

None of these tests opens a real runtime database: the protected targets are
temporary files that merely *look* like one (``cosmicforge.db``, a paper role),
and the configured runtime path is only ever passed to the pure
``protection_reason`` function.
"""
from __future__ import annotations

import os
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest

from shared_lib.persistence.db import DB
from shared_lib.persistence.migrations import migrate
from shared_lib.persistence.test_isolation import (
    PERSISTENCE_DIR,
    TestDatabaseIsolationError,
    configured_runtime_database,
    protection_reason,
)
from shared_lib.persistence.trace_recorder import (
    TraceRecorder,
    get_trace_recorder,
    reset_trace_recorder,
)

BOT_BACKEND = Path(__file__).resolve().parents[1]


# ── The session database ────────────────────────────────────────────────────


def test_the_session_database_is_a_temporary_test_database():
    path = Path(DB().path)
    assert path.name == "test_session.db"
    assert path.parent.name.startswith("cosmicforge_pytest_")
    assert protection_reason(str(path)) is None
    assert os.environ["DATABASE_ROLE"] == "test"
    assert os.environ["COSMICFORGE_TEST_MODE"] == "1"
    assert Path(os.environ["COSMICFORGE_TEST_DATABASE_PATH"]) == path


def test_the_configured_runtime_database_is_protected():
    runtime = configured_runtime_database()
    if runtime is None:
        pytest.skip("no DATABASE_URL in bot-backend/.env on this machine")
    # Pure check only: the runtime database is never opened by this test.
    assert protection_reason(runtime) is not None


# ── Refusals, before anything is created ────────────────────────────────────


def test_a_canonically_named_database_is_refused_before_it_is_created(tmp_path):
    target = tmp_path / "cosmicforge.db"
    with pytest.raises(TestDatabaseIsolationError):
        DB(str(target))
    assert not target.exists(), "a refused database must not even be created"


def test_the_canonical_persistence_directory_is_refused():
    target = PERSISTENCE_DIR / "isolation_probe_never_created.db"
    with pytest.raises(TestDatabaseIsolationError):
        DB(str(target))
    assert not target.exists()


def test_a_runtime_role_is_refused_whatever_the_file_is_called(tmp_path, monkeypatch):
    monkeypatch.setenv("DATABASE_ROLE", "paper")
    target = tmp_path / "harmless_name.db"
    with pytest.raises(TestDatabaseIsolationError):
        DB(str(target))
    assert not target.exists()


def test_the_trace_recorder_refuses_a_runtime_database(tmp_path):
    with pytest.raises(TestDatabaseIsolationError):
        TraceRecorder(str(tmp_path / "cosmicforge.db"))


# ── No cached object keeps an old database ──────────────────────────────────


def test_the_trace_recorder_follows_the_database_in_force(tmp_path, monkeypatch):
    reset_trace_recorder()
    first = get_trace_recorder()
    assert os.path.normcase(first._db_path) == os.path.normcase(DB().path)

    other = tmp_path / "other_test.db"
    monkeypatch.setenv("DATABASE_URL", "sqlite:///" + other.as_posix())
    second = get_trace_recorder()
    assert second is not first
    assert Path(second._db_path).resolve() == other.resolve()


# ── Provenance is written, not inferred ─────────────────────────────────────


def test_both_trace_ledgers_record_the_provenance_they_were_given(tmp_path):
    db = DB(str(tmp_path / "trace_provenance.db"))
    migrate(db)
    recorder = TraceRecorder(db.path)
    trace_id = recorder.start_trace(
        run_id="run_prov", cycle_id="cyc_prov", symbol="BTCUSDT",
        bot_instance_id="bot_prov", provenance="TEST_FIXTURE",
    )
    recorder.record_gate(trace_id, allowed=False, reason_code="NO_OPPORTUNITY",
                         reason="nothing to trade", details={})
    recorder.finalize(trace_id)

    with db.connect() as conn:
        trace = conn.execute(
            "SELECT provenance FROM decision_traces WHERE trace_id=?", (trace_id,)
        ).fetchone()
        canonical = conn.execute(
            "SELECT provenance FROM canonical_trade_decisions WHERE decision_id=?", (trace_id,)
        ).fetchone()
    assert trace[0] == "TEST_FIXTURE"
    assert canonical[0] == "TEST_FIXTURE"


def test_the_migration_adds_provenance_to_both_trace_ledgers(tmp_path):
    db = DB(str(tmp_path / "migrated.db"))
    migrate(db)
    with db.connect() as conn:
        for table in ("decision_traces", "canonical_trade_decisions"):
            columns = {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
            assert "provenance" in columns, table


# ── The bootstrap refuses a runtime database before writing one row ─────────


@pytest.mark.skipif(
    os.environ.get("COSMICFORGE_ISOLATION_PROBE") != "1",
    reason="only runs inside the refusal subprocess, where it must never be reached",
)
def test_isolation_probe_would_write_evidence():
    from app.evidence.writers import open_runtime_session

    open_runtime_session(
        DB(), database_role="paper", database_path=DB().path,
        process_execution_mode="paper", environment_name="probe",
    )


def _sentinel_database(path: Path) -> None:
    conn = sqlite3.connect(str(path))
    conn.execute("CREATE TABLE sentinel (x INTEGER)")
    conn.execute("INSERT INTO sentinel VALUES (1)")
    conn.commit()
    conn.close()


def _tables(path: Path) -> set[str]:
    conn = sqlite3.connect(str(path))
    try:
        return {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    finally:
        conn.close()


def _run_probe(env_overrides: dict[str, str]) -> subprocess.CompletedProcess:
    env = {k: v for k, v in os.environ.items()
           if k not in {"COSMICFORGE_TEST_MODE", "DATABASE_ROLE", "DATABASE_URL",
                        "ENVIRONMENT_NAME", "COSMICFORGE_TEST_DATABASE_PATH"}}
    env.update(env_overrides)
    env["COSMICFORGE_ISOLATION_PROBE"] = "1"
    return subprocess.run(
        [sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider",
         "tests/test_test_database_isolation.py::test_isolation_probe_would_write_evidence"],
        cwd=str(BOT_BACKEND), env=env, capture_output=True, text=True, timeout=300,
    )


def test_pytest_refuses_to_start_with_a_paper_database_url(tmp_path):
    fake_paper = tmp_path / "cosmicforge.db"
    _sentinel_database(fake_paper)

    proc = _run_probe({"DATABASE_URL": "sqlite:///" + fake_paper.as_posix()})

    output = proc.stdout + proc.stderr
    assert proc.returncode != 0, output
    assert "TEST_DATABASE_ISOLATION_VIOLATION" in output
    assert _tables(fake_paper) == {"sentinel"}, "not one evidence table may be created"


def test_pytest_refuses_to_start_with_a_paper_database_role(tmp_path):
    other = tmp_path / "some_other_name.db"
    _sentinel_database(other)

    proc = _run_probe({
        "DATABASE_URL": "sqlite:///" + other.as_posix(),
        "DATABASE_ROLE": "paper",
    })

    output = proc.stdout + proc.stderr
    assert proc.returncode != 0, output
    assert "TEST_DATABASE_ISOLATION_VIOLATION" in output
    assert _tables(other) == {"sentinel"}
