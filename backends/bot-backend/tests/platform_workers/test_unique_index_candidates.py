"""The K2 additive unique indexes: the read-only duplicate check (``scripts/check_unique_index_candidates.py``).

Synthetic databases only, on SQLite and (with ``PLATFORM_TEST_POSTGRES_URL``) on throwaway PostgreSQL databases.
The check must find planted duplicates, report clean tables as READY, never write, never print a key, refuse an
operator database, and the DDL it prints must be valid on both dialects -- proved by applying it to the synthetic
copy only, after which the duplicate it guards against is refused by the schema.
"""
from __future__ import annotations

import importlib.util
import sqlite3
import sys
import uuid
from pathlib import Path

import pytest

from _pg_server import PG_REQUIRED, PG_URL, _pg_admin, _pg_url

ROOT = Path(__file__).resolve().parents[4]
NOW = "2026-10-31T12:00:00+00:00"


def _checker():
    spec = importlib.util.spec_from_file_location("check_unique_index_candidates_under_test",
                                                  ROOT / "scripts" / "check_unique_index_candidates.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module                     # dataclasses resolve annotations through sys.modules
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def checker(monkeypatch):
    monkeypatch.setenv("COSMICFORGE_TEST_MODE", "1")
    return _checker()


# Minimal tables with the real column names the candidates use (the real-schema test below proves the names).
_LEGACY_DDL = (
    "CREATE TABLE bot_instances (id TEXT PRIMARY KEY, user_id TEXT NOT NULL, deploy_request_id TEXT)",
    "CREATE TABLE deployment_consents (consent_id TEXT PRIMARY KEY, user_id TEXT NOT NULL, request_id TEXT NOT NULL)",
    "CREATE TABLE billing_operator_grants (grant_id TEXT PRIMARY KEY, user_id TEXT NOT NULL, expires_at TEXT NOT NULL, "
    "revoked_at TEXT)",
    "CREATE TABLE platform_job_attempts (id INTEGER PRIMARY KEY, job_id TEXT NOT NULL, attempt INTEGER NOT NULL)",
)


def _fill(execute, *, duplicates: bool) -> None:
    rows = [
        ("INSERT INTO bot_instances VALUES (?,?,?)", ("bot_1", "u1", "req-1")),
        ("INSERT INTO bot_instances VALUES (?,?,?)", ("bot_2", "u1", None)),        # no request id: out of scope
        ("INSERT INTO bot_instances VALUES (?,?,?)", ("bot_3", "u1", None)),
        ("INSERT INTO bot_instances VALUES (?,?,?)", ("bot_4", "u2", "req-1")),     # same id, other user: allowed
        ("INSERT INTO deployment_consents VALUES (?,?,?)", ("c1", "u1", "req-1")),
        ("INSERT INTO deployment_consents VALUES (?,?,?)", ("c2", "u2", "req-1")),
        ("INSERT INTO billing_operator_grants VALUES (?,?,?,?)", ("g1", "u1", "2026-01-01T00:00:00+00:00",
                                                                  "2026-01-01T00:00:00+00:00")),
        ("INSERT INTO billing_operator_grants VALUES (?,?,?,?)", ("g2", "u1", "2027-01-01T00:00:00+00:00", None)),
        ("INSERT INTO platform_job_attempts (job_id, attempt) VALUES (?,?)", ("job-1", 1)),
        ("INSERT INTO platform_job_attempts (job_id, attempt) VALUES (?,?)", ("job-1", 2)),
    ]
    if duplicates:
        rows += [
            ("INSERT INTO bot_instances VALUES (?,?,?)", ("bot_5", "u1", "req-1")),
            ("INSERT INTO deployment_consents VALUES (?,?,?)", ("c3", "u1", "req-1")),
            ("INSERT INTO deployment_consents VALUES (?,?,?)", ("c4", "u1", "req-1")),
            # u2: an expired grant never revoked, then a live one -> legitimate history the index would refuse
            ("INSERT INTO billing_operator_grants VALUES (?,?,?,?)", ("g3", "u2", "2026-02-01T00:00:00+00:00", None)),
            ("INSERT INTO billing_operator_grants VALUES (?,?,?,?)", ("g4", "u2", "2027-02-01T00:00:00+00:00", None)),
            # u3: two live unrevoked grants -> a real violation
            ("INSERT INTO billing_operator_grants VALUES (?,?,?,?)", ("g5", "u3", "2027-03-01T00:00:00+00:00", None)),
            ("INSERT INTO billing_operator_grants VALUES (?,?,?,?)", ("g6", "u3", "2027-04-01T00:00:00+00:00", None)),
            ("INSERT INTO platform_job_attempts (job_id, attempt) VALUES (?,?)", ("job-1", 2)),
        ]
    for sql, params in rows:
        execute(sql, params)


def _by_index(report):
    return {r["index"]: r for r in report["candidates"]}


def _assert_duplicates(report):
    r = _by_index(report)
    assert report["duplicates_found"] is True and report["ddl_executed"] is False and report["read_only"] is True
    bots = r["uq_bot_instances_user_deploy_request"]
    assert (bots["verdict"], bots["rows_in_scope"], bots["duplicate_groups"], bots["duplicate_rows"]) == \
        ("DUPLICATES_FOUND", 3, 1, 2)
    consents = r["uq_deployment_consents_user_request"]
    assert (consents["verdict"], consents["duplicate_groups"], consents["duplicate_rows"]) == ("DUPLICATES_FOUND", 1, 3)
    grants = r["uq_billing_operator_grants_user_unrevoked"]
    assert (grants["verdict"], grants["duplicate_groups"], grants["expired_unrevoked_groups"],
            grants["live_duplicate_groups"]) == ("DUPLICATES_FOUND", 2, 1, 1)
    jobs = r["uq_platform_job_attempts_job_attempt"]
    assert (jobs["verdict"], jobs["duplicate_groups"], jobs["duplicate_rows"]) == ("DUPLICATES_FOUND", 1, 2)
    text = repr(report)
    for secret in ("req-1", "'u1'", "'u2'", "'u3'", "job-1"):
        assert secret not in text                       # keys are reported only as digests


def _assert_ready(report):
    assert report["duplicates_found"] is False
    assert {r["verdict"] for r in report["candidates"]} == {"READY"}


# ════════════════════════════════════════════════════════════════════════════════════════════════════ SQLite
def _sqlite_db(tmp_path, *, duplicates):
    path = tmp_path / f"synthetic_{uuid.uuid4().hex[:8]}.db"
    conn = sqlite3.connect(path)
    for ddl in _LEGACY_DDL:
        conn.execute(ddl)
    _fill(conn.execute, duplicates=duplicates)
    conn.commit()
    conn.close()
    return path


def _sqlite_fingerprint(path):
    conn = sqlite3.connect(path)
    try:
        return [tuple(r) for r in conn.execute("SELECT type, name, sql FROM sqlite_master ORDER BY name")], \
               {t: conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
                for t in ("bot_instances", "deployment_consents", "billing_operator_grants", "platform_job_attempts")}
    finally:
        conn.close()


def test_sqlite_duplicates_are_found_and_nothing_is_written(checker, tmp_path):
    path = _sqlite_db(tmp_path, duplicates=True)
    before, digest = _sqlite_fingerprint(path), path.read_bytes()
    report = checker.run_check(sqlite_path=path, now_iso=NOW)
    assert report["dialect"] == "sqlite"
    _assert_duplicates(report)
    assert _sqlite_fingerprint(path) == before and path.read_bytes() == digest


def test_sqlite_clean_database_is_ready_and_the_ddl_is_valid_and_enforced(checker, tmp_path):
    path = _sqlite_db(tmp_path, duplicates=False)
    report = checker.run_check(sqlite_path=path, now_iso=NOW)
    _assert_ready(report)
    # Apply the printed DDL to this synthetic copy only, then prove it enforces the rule.
    conn = sqlite3.connect(path)
    for r in report["candidates"]:
        conn.execute(r["ddl"])
    conn.commit()
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute("INSERT INTO bot_instances VALUES ('bot_9','u1','req-1')")
    conn.execute("INSERT INTO bot_instances VALUES ('bot_10','u1',NULL)")      # outside the partial index
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute("INSERT INTO platform_job_attempts (job_id, attempt) VALUES ('job-1', 1)")
    conn.close()
    again = checker.run_check(sqlite_path=path, now_iso=NOW)
    assert {r["verdict"] for r in again["candidates"]} == {"ALREADY_PRESENT"}


def test_sqlite_real_schema_has_the_candidate_columns(checker, tmp_path, monkeypatch):
    """The legacy schema, the operator-grant table and the platform migrations, as the runtime creates them."""
    from shared_lib.persistence.db import DB
    from shared_lib.billing import operator_grants
    from shared_lib.persistence.sql import Database, migrate
    from shared_lib.persistence.sql.platform_schema import all_migrations
    path = tmp_path / "real_schema.db"
    db = DB(str(path))
    from shared_lib.persistence.migrations import migrate as legacy_migrate
    legacy_migrate(db)
    from app.core import deployment_service
    with db.connect() as conn:
        operator_grants.ensure_schema(conn)
        deployment_service.ensure_schema(conn)
    platform = Database(f"sqlite:///{path.as_posix()}")
    migrate(platform, all_migrations())
    platform.close()
    report = checker.run_check(sqlite_path=path, now_iso=NOW)
    verdicts = {r["index"]: r["verdict"] for r in report["candidates"]}
    assert verdicts == {c.name: "READY" for c in checker.CANDIDATES}, report


def test_missing_tables_are_reported_not_failed(checker, tmp_path):
    path = tmp_path / "empty.db"
    sqlite3.connect(path).close()
    report = checker.run_check(sqlite_path=path, now_iso=NOW)
    assert {r["verdict"] for r in report["candidates"]} == {"TABLE_ABSENT"} and not report["duplicates_found"]


def test_operator_databases_are_refused(checker, tmp_path):
    inside = ROOT / "backends" / "bot-backend" / "data" / "trading.db"      # resolved only, never opened
    with pytest.raises(checker.SourceRefused):
        checker.run_check(sqlite_path=inside)
    with pytest.raises(checker.SourceRefused):
        checker.run_check(postgres_url="postgresql://u@127.0.0.1:5432/cosmicforge")
    assert checker.main(["--sqlite", str(inside)]) == checker.EXIT_REFUSED


def test_cli_exit_status(checker, tmp_path):
    dirty, clean = _sqlite_db(tmp_path, duplicates=True), _sqlite_db(tmp_path, duplicates=False)
    out = tmp_path / "report.json"
    assert checker.main(["--sqlite", str(dirty), "--out", str(out)]) == checker.EXIT_DUPLICATES and out.is_file()
    assert checker.main(["--sqlite", str(clean)]) == checker.EXIT_OK


# ════════════════════════════════════════════════════════════════════════════════════════════════ PostgreSQL
@pytest.fixture
def pg_database():
    if not PG_URL:
        if PG_REQUIRED:
            pytest.fail("PLATFORM_TEST_POSTGRES_REQUIRED=1 but PLATFORM_TEST_POSTGRES_URL is not set")
        pytest.skip("PLATFORM_TEST_POSTGRES_URL not set")
    name = f"cftest_uq_{uuid.uuid4().hex[:12]}"
    _pg_admin(f"CREATE DATABASE {name}")
    try:
        yield _pg_url(name)
    finally:
        _pg_admin(f"DROP DATABASE IF EXISTS {name} WITH (FORCE)")


def _pg_fill(url, *, duplicates):
    import psycopg
    with psycopg.connect(url, autocommit=True) as conn:
        for ddl in _LEGACY_DDL:
            conn.execute(ddl.replace("id INTEGER PRIMARY KEY", "id BIGSERIAL PRIMARY KEY"))
        _fill(lambda sql, params: conn.execute(sql.replace("?", "%s"), params), duplicates=duplicates)


def test_postgres_duplicates_are_found_and_nothing_is_written(checker, pg_database):
    import psycopg
    _pg_fill(pg_database, duplicates=True)
    report = checker.run_check(postgres_url=pg_database, now_iso=NOW)
    assert report["dialect"] == "postgresql"
    _assert_duplicates(report)
    with psycopg.connect(pg_database) as conn:
        assert conn.execute("SELECT COUNT(*) FROM pg_indexes WHERE indexname LIKE 'uq\\_%'").fetchone()[0] == 0


def test_postgres_clean_database_is_ready_and_the_ddl_is_valid_and_enforced(checker, pg_database):
    import psycopg
    _pg_fill(pg_database, duplicates=False)
    report = checker.run_check(postgres_url=pg_database, now_iso=NOW)
    _assert_ready(report)
    with psycopg.connect(pg_database, autocommit=True) as conn:
        for r in report["candidates"]:
            conn.execute(r["ddl"])
        with pytest.raises(psycopg.errors.UniqueViolation):
            conn.execute("INSERT INTO bot_instances VALUES ('bot_9','u1','req-1')")
        conn.execute("INSERT INTO bot_instances VALUES ('bot_10','u1',NULL)")
        with pytest.raises(psycopg.errors.UniqueViolation):
            conn.execute("INSERT INTO billing_operator_grants VALUES ('g9','u1','2028-01-01T00:00:00+00:00',NULL)")
    again = checker.run_check(postgres_url=pg_database, now_iso=NOW)
    assert {r["verdict"] for r in again["candidates"]} == {"ALREADY_PRESENT"}


def test_postgres_connection_is_read_only(checker, pg_database):
    import psycopg
    _pg_fill(pg_database, duplicates=False)
    reader = checker._PostgresReader(pg_database)
    try:
        with pytest.raises(psycopg.errors.ReadOnlySqlTransaction):
            reader.rows("INSERT INTO bot_instances VALUES ('x','u9','r9')")
    finally:
        reader.close()


def test_postgres_platform_schema_has_the_job_attempt_columns(checker, pg_database):
    from shared_lib.persistence.sql import Database, migrate
    from shared_lib.persistence.sql.platform_schema import all_migrations
    platform = Database(pg_database)
    migrate(platform, all_migrations())
    platform.close()
    jobs = _by_index(checker.run_check(postgres_url=pg_database, now_iso=NOW))["uq_platform_job_attempts_job_attempt"]
    assert jobs["verdict"] == "READY"
