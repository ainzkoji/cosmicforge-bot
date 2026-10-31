"""``scripts/backup_platform_db.py``: scheduled pg_dump of the platform PostgreSQL database (Section J follow-up).

The real dump runs against ``PLATFORM_TEST_POSTGRES_URL`` (a disposable cluster) when it is set and the PostgreSQL
client tools are on PATH; the rest needs neither. The password never reaches a command line.
"""
from __future__ import annotations

import importlib.util
import json
import os
import shutil
import subprocess
import uuid
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[3]
PG_URL = os.environ.get("PLATFORM_TEST_POSTGRES_URL", "")


def _script():
    spec = importlib.util.spec_from_file_location("backup_platform_db", REPO / "scripts" / "backup_platform_db.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_the_password_goes_to_pg_dump_through_its_environment_never_its_command_line(tmp_path, monkeypatch):
    m = _script()
    seen = []

    def run(argv, env=None, **kw):
        seen.append((list(argv), dict(env or {})))
        if "--list" in argv:
            return subprocess.CompletedProcess(argv, 0, "; header\n1; 1259 16390 TABLE DATA public t owner\n", "")
        Path(argv[argv.index("-f") + 1]).write_bytes(b"PGDMP fake")
        return subprocess.CompletedProcess(argv, 0, "", "")
    monkeypatch.setattr(m.subprocess, "run", run)
    url = "postgresql://cosmicforge:s3cret-Pa55word@db.internal:6543/cosmicforge?sslmode=require"
    code = m.main(["--url", url, "--output-dir", str(tmp_path), "--pg-dump", "pg_dump", "--pg-restore", "pg_restore"])
    assert code == 0
    dump_argv, dump_env = seen[0]
    assert "s3cret-Pa55word" not in " ".join(dump_argv) and url not in " ".join(dump_argv)
    assert (dump_env["PGPASSWORD"], dump_env["PGHOST"], dump_env["PGPORT"], dump_env["PGDATABASE"], dump_env["PGSSLMODE"]) \
        == ("s3cret-Pa55word", "db.internal", "6543", "cosmicforge", "require")
    manifest = json.loads(next(tmp_path.glob("platform-*.dump.json")).read_text(encoding="utf-8"))
    assert "s3cret" not in json.dumps(manifest) and manifest["table_data_entries"] == 1
    status = json.loads((tmp_path / "platform-backup-status.json").read_text(encoding="utf-8"))
    assert status["outcome"] == "OK" and status["tool"] == "backup_platform_db"


def test_failures_leave_a_failed_marker_and_no_dump(tmp_path, monkeypatch):
    m = _script()
    monkeypatch.setattr(m.subprocess, "run", lambda argv, **kw: subprocess.CompletedProcess(
        argv, 1, "", "pg_dump: error: connection to server failed: password=s3cret-Pa55word rejected"))
    code = m.main(["--url", "postgresql://u:s3cret-Pa55word@h/db", "--output-dir", str(tmp_path),
                   "--pg-dump", "pg_dump", "--pg-restore", "pg_restore"])
    status = json.loads((tmp_path / "platform-backup-status.json").read_text(encoding="utf-8"))
    assert code == 2 and status["outcome"] == "FAILED" and "s3cret" not in json.dumps(status)
    assert not list(tmp_path.glob("platform-*.dump*"))
    assert m.main(["--url", "sqlite:///x.db", "--output-dir", str(tmp_path)]) == 1     # not PostgreSQL: refused


def test_a_dump_without_table_data_fails_verification(tmp_path, monkeypatch):
    m = _script()

    def run(argv, **kw):
        if "--list" in argv:
            return subprocess.CompletedProcess(argv, 0, "; empty archive\n", "")
        Path(argv[argv.index("-f") + 1]).write_bytes(b"PGDMP")
        return subprocess.CompletedProcess(argv, 0, "", "")
    monkeypatch.setattr(m.subprocess, "run", run)
    assert m.main(["--url", "postgresql://u@h/db", "--output-dir", str(tmp_path), "--pg-dump", "x",
                   "--pg-restore", "y"]) == 3
    assert not list(tmp_path.glob("platform-*.dump"))


@pytest.mark.skipif(not PG_URL or not shutil.which("pg_dump"), reason="needs PLATFORM_TEST_POSTGRES_URL and pg_dump")
def test_a_real_dump_of_a_migrated_platform_database_verifies_and_rotates(tmp_path):
    import psycopg
    from shared_lib.persistence.sql import Database, migrate
    from shared_lib.persistence.sql.platform_schema import all_migrations
    name = f"cfbk_{uuid.uuid4().hex[:10]}"
    with psycopg.connect(PG_URL, autocommit=True) as conn:
        conn.execute(f"CREATE DATABASE {name}")
    url = f"{PG_URL.rpartition('/')[0]}/{name}"
    try:
        db = Database(url)
        migrate(db, all_migrations())
        db.close()
        m = _script()
        for _ in range(3):
            assert m.main(["--url", url, "--output-dir", str(tmp_path), "--keep", "2"]) == 0
            import time
            time.sleep(1.1)                                   # one-second stamps
        dumps = sorted(tmp_path.glob("platform-*.dump"))
        assert len(dumps) == 2
        listed = subprocess.run([shutil.which("pg_restore"), "--list", str(dumps[-1])], capture_output=True, text=True)
        assert listed.returncode == 0 and "platform_account_assignments" in listed.stdout
        manifest = json.loads(dumps[-1].with_name(dumps[-1].name + ".json").read_text(encoding="utf-8"))
        assert manifest["table_data_entries"] > 10 and manifest["size_bytes"] == dumps[-1].stat().st_size
    finally:
        with psycopg.connect(PG_URL, autocommit=True) as conn:
            conn.execute(f"DROP DATABASE IF EXISTS {name} WITH (FORCE)")


def test_the_tick_alerts_on_a_failed_or_stale_platform_dump(tmp_path):
    from datetime import datetime, timedelta, timezone
    from app.ops.alerting import evaluate_platform_backup_status

    class Engine:
        def __init__(self):
            self.raised, self.resolved = [], []

        def raise_alert(self, category, *, key, **kw):
            self.raised.append(key)

        def resolve_condition(self, category, *, key, **kw):
            self.resolved.append(key)
            return False
    e = Engine()
    assert not any(ev.firing for ev in evaluate_platform_backup_status(e, tmp_path))   # never scheduled: silent
    now = datetime(2026, 10, 31, 12, tzinfo=timezone.utc)
    (tmp_path / "platform-backup-status.json").write_text(json.dumps(
        {"outcome": "FAILED", "exit_code": 2, "last_ok_at": (now - timedelta(hours=50)).isoformat()}))
    evs = evaluate_platform_backup_status(e, tmp_path, now_ms=int(now.timestamp() * 1000))
    assert [ev.firing for ev in evs] == [True, True] and e.raised == ["backup:platform_run", "backup:platform_age"]
