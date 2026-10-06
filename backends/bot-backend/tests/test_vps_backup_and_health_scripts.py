"""The two operator scripts a server deployment depends on.

``backup_trading_db.py`` must produce a consistent, verified copy of a WAL
database that is being written to, and must fail loudly rather than leave a
bad file behind. ``vps_health_check.py`` must turn the runtime's own health
report into an exit status, and must never read missing evidence as a pass.
"""
from __future__ import annotations

import gzip
import importlib.util
import json
import sqlite3
from pathlib import Path

import pytest

SCRIPTS = Path(__file__).resolve().parents[3] / "scripts"


def load(name):
    spec = importlib.util.spec_from_file_location(name, SCRIPTS / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


backup = load("backup_trading_db")
health_check = load("vps_health_check")


@pytest.fixture
def live_database(tmp_path):
    """A WAL database with a writer connection still open, as in production."""
    path = tmp_path / "source" / "cosmicforge.db"
    path.parent.mkdir()
    writer = sqlite3.connect(str(path))
    writer.execute("PRAGMA journal_mode=WAL")
    writer.execute("PRAGMA wal_autocheckpoint=0")       # keep every commit in the WAL
    writer.execute("CREATE TABLE intents(id INTEGER PRIMARY KEY, body TEXT)")
    writer.executemany("INSERT INTO intents(body) VALUES (?)", [("x" * 500,) for _ in range(400)])
    writer.commit()
    assert Path(str(path) + "-wal").stat().st_size > 0   # the main file alone is incomplete
    yield path, writer
    writer.close()


def run(source, out, *extra):
    return backup.main(["--database", str(source), "--output-dir", str(out), *extra])


def backups_in(out):
    return sorted(p for p in out.iterdir() if backup.NAME.match(p.name))


def test_backup_captures_committed_work_still_in_the_wal(live_database, tmp_path):
    source, writer = live_database
    out = tmp_path / "backups"
    assert run(source, out) == backup.EXIT_OK
    [copy] = backups_in(out)
    # An uncommitted transaction in flight is not part of the snapshot.
    writer.execute("INSERT INTO intents(body) VALUES ('uncommitted')")
    restored = sqlite3.connect(str(copy))
    assert restored.execute("SELECT COUNT(*) FROM intents").fetchone()[0] == 400
    assert restored.execute("PRAGMA quick_check").fetchone()[0] == "ok"
    assert restored.execute("PRAGMA journal_mode").fetchone()[0] == "delete"
    restored.close()
    # One self-contained file, described by a manifest that matches it.
    assert not Path(str(copy) + "-wal").exists()
    manifest = json.loads(Path(str(copy) + ".json").read_text())
    assert manifest["quick_check"] == "ok" and manifest["sha256"] == backup.sha256_of(copy)
    assert manifest["size_bytes"] == copy.stat().st_size
    # The source was not disturbed.
    writer.rollback()
    assert writer.execute("SELECT COUNT(*) FROM intents").fetchone()[0] == 400


def test_a_plain_file_copy_of_the_same_database_would_have_lost_the_data(live_database, tmp_path):
    source, _writer = live_database
    naive = tmp_path / "naive.db"
    naive.write_bytes(source.read_bytes())               # what `cp cosmicforge.db` does
    conn = sqlite3.connect(str(naive))
    try:
        with pytest.raises(sqlite3.DatabaseError):
            conn.execute("SELECT COUNT(*) FROM intents").fetchone()
    finally:
        conn.close()


def test_compressed_backup_restores_to_a_sound_database(live_database, tmp_path):
    source, _writer = live_database
    out = tmp_path / "backups"
    assert run(source, out, "--compress") == backup.EXIT_OK
    [packed] = backups_in(out)
    assert packed.name.endswith(".db.gz")
    restored = tmp_path / "restored.db"
    restored.write_bytes(gzip.decompress(packed.read_bytes()))
    assert backup.quick_check(restored) == "ok"
    assert not list(out.glob("*.partial"))


def test_retention_keeps_only_the_newest(live_database, tmp_path):
    source, _writer = live_database
    out = tmp_path / "backups"
    out.mkdir()
    for day in ("01", "02", "03", "04"):
        (out / f"cosmicforge-202610{day}T000000Z.db").write_bytes(b"old")
        (out / f"cosmicforge-202610{day}T000000Z.db.json").write_text("{}")
    (out / "unrelated.db").write_bytes(b"not ours")
    assert run(source, out, "--keep", "2") == backup.EXIT_OK
    kept = [p.name for p in backups_in(out)]
    assert len(kept) == 2 and "cosmicforge-20261004T000000Z.db" in kept
    assert not (out / "cosmicforge-20261001T000000Z.db.json").exists()
    assert (out / "unrelated.db").exists()                # never touches other files


def test_a_copy_that_fails_its_integrity_check_is_discarded(live_database, tmp_path, monkeypatch):
    source, _writer = live_database
    out = tmp_path / "backups"
    monkeypatch.setattr(backup, "quick_check", lambda path: "row 7 missing from index intents_idx")
    assert run(source, out) == backup.EXIT_INTEGRITY_FAILED
    assert list(out.iterdir()) == []


def test_missing_or_unreadable_sources_fail_with_a_distinct_status(tmp_path):
    out = tmp_path / "backups"
    assert run(tmp_path / "absent.db", out) == backup.EXIT_USAGE
    garbage = tmp_path / "garbage.db"
    garbage.write_bytes(b"this is not a database" * 100)
    assert run(garbage, out) in (backup.EXIT_BACKUP_FAILED, backup.EXIT_INTEGRITY_FAILED)
    assert not backups_in(out) and not list(out.glob("*.partial"))
    assert run(garbage, out, "--keep", "0") == backup.EXIT_USAGE


def test_the_source_is_resolved_exactly_as_the_backend_resolves_it(monkeypatch, tmp_path):
    absolute = tmp_path / "data" / "cosmicforge.db"
    assert backup.database_from_url("sqlite:///" + absolute.as_posix()) == absolute
    relative = backup.database_from_url("sqlite:///../shared/shared_lib/persistence/cosmicforge.db")
    assert relative == (backup.BACKEND / ".." / "shared" / "shared_lib" / "persistence" / "cosmicforge.db").resolve()
    assert backup.database_from_url("postgresql://host/db") is None
    monkeypatch.setenv("DATABASE_URL", "sqlite:///" + absolute.as_posix())
    assert backup.resolve_source(None) == absolute


# ── vps_health_check ────────────────────────────────────────────────────────


def healthy():
    return {
        "status": "ok", "runtime_revision": "b8e60749127c", "cati_production_running": True,
        "discovered_accounts": 1, "synced_accounts": 1, "reconciliation_health": "SYNCED",
        "risk_engine_health": "HEALTHY", "market_data_status": "COLLECTING",
        "demo_system_gate": True, "live_system_gate": False,
        "components": {"runtime_owns_lease": True},
        "production_configuration": {"DATABASE ROLE": "PRODUCTION"},
        "runtime": {"state": "HEALTHY", "faults": [], "warnings": [],
                    "database": {"size_bytes": 8 * 1024 ** 3, "disk_free_bytes": 40 * 1024 ** 3},
                    "lease": {"held_by_this_process": True, "heartbeat_age_seconds": 3.0},
                    "scheduler": {"runner_loop_alive": True, "last_cycle_completed_age_seconds": 11.0}},
        "trading": {"auto_trading_enabled_accounts": 1, "kill_switch_engaged_accounts": 0,
                    "broker_sync_max_age_seconds": 14.0, "execution_portfolio_states": ["AVAILABLE"],
                    "latest_decision": {"age_seconds": 900.0, "reason": "SELECTED_TOP1", "symbol": "ADAUSDT"}},
    }


def verdict(health, **kw):
    options = dict(max_broker_sync_age=180, max_decision_age=7800, allow_live_gate=False, expect_revision=None)
    checks = health_check.evaluate(health, **{**options, **kw})
    worst = max((level for level, _, _ in checks), key=lambda level: health_check.EXIT[level])
    return health_check.EXIT[worst], {name: level for level, name, _ in checks}


def test_a_trading_capable_runtime_is_healthy():
    code, checks = verdict(healthy())
    assert code == 0 and set(checks.values()) == {"OK"}


@pytest.mark.parametrize("path,value,check", [
    (("runtime", "state"), "FAILING", "runtime_supervisor"),
    (("runtime", "lease", "heartbeat_age_seconds"), 400.0, "runtime_lease"),
    (("components", "runtime_owns_lease"), False, "runtime_lease"),
    (("cati_production_running",), False, "trading_scheduler"),
    (("production_configuration", "DATABASE ROLE"), "PAPER", "database_role"),
    (("synced_accounts",), 0, "broker_sync"),
    (("trading", "broker_sync_max_age_seconds"), 900.0, "broker_sync"),
    (("market_data_status",), "STALE", "market_data"),
    (("trading", "latest_decision", "age_seconds"), 20_000.0, "cati_decision"),
    (("live_system_gate",), True, "live_order_gate"),
    (("runtime", "database", "disk_free_bytes"), 200 * 1024 ** 2, "database_disk"),
])
def test_each_condition_that_stops_trading_is_unhealthy(path, value, check):
    health = healthy()
    target = health
    for key in path[:-1]:
        target = target[key]
    target[path[-1]] = value
    code, checks = verdict(health)
    assert code == 2 and checks[check] == "FAIL"


def test_missing_evidence_is_never_a_pass():
    code, checks = verdict({"status": "ok"})
    assert code == 2
    assert checks["runtime_lease"] == checks["broker_sync"] == checks["cati_decision"] == "FAIL"


def test_attention_states_are_degraded_not_unhealthy_and_revision_can_be_pinned():
    health = healthy()
    health["trading"]["kill_switch_engaged_accounts"] = 1
    assert verdict(health)[0] == 1
    filling = healthy()
    filling["runtime"]["database"]["disk_free_bytes"] = 3 * 1024 ** 3
    assert verdict(filling) == (1, {**verdict(healthy())[1], "database_disk": "WARN"})
    assert verdict(healthy(), expect_revision="b8e60749")[0] == 0
    assert verdict(healthy(), expect_revision="deadbeef")[0] == 2
    live = healthy()
    live["live_system_gate"] = True
    assert verdict(live, allow_live_gate=True)[0] == 0


def test_an_unreachable_backend_is_unhealthy(capsys):
    assert health_check.main(["--url", "http://127.0.0.1:9", "--timeout", "1", "--json"]) == 2
    report = json.loads(capsys.readouterr().out)
    assert report["verdict"] == "UNHEALTHY" and report["checks"][0]["check"] == "api"
