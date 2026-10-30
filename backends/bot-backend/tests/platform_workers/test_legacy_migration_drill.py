"""The legacy SQLite -> PostgreSQL migration drill (``scripts/migration_drill.py --legacy``, Step 5.1, EXT-DB-01).

Fast variants: a small synthetic source built from the REAL legacy schema (3 rows per table), throwaway PostgreSQL
databases created and dropped per test. The PostgreSQL tests need ``PLATFORM_TEST_POSTGRES_URL`` (skipped without
it; a failure under ``PLATFORM_TEST_POSTGRES_REQUIRED=1``). The operator's databases are never opened: the refusal
tests only resolve paths.
"""
from __future__ import annotations

import importlib.util
import os
import sqlite3
import uuid
from decimal import Decimal
from pathlib import Path

import pytest

from _pg_server import PG_REQUIRED, PG_URL, _pg_admin, _pg_url

ROOT = Path(__file__).resolve().parents[4]


def _drill():
    spec = importlib.util.spec_from_file_location("migration_drill_under_test", ROOT / "scripts" / "migration_drill.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def drill(monkeypatch):
    monkeypatch.setenv("COSMICFORGE_TEST_MODE", "1")
    monkeypatch.setenv("DATABASE_URL", os.environ.get("DATABASE_URL", ""))       # restored after the test
    return _drill()


def need_postgres():
    if not PG_URL:
        if PG_REQUIRED:
            pytest.fail("PLATFORM_TEST_POSTGRES_REQUIRED=1 but PLATFORM_TEST_POSTGRES_URL is not set")
        pytest.skip("PLATFORM_TEST_POSTGRES_URL not set")


def need_compatibility_layer():
    try:
        from shared_lib.persistence import legacy_postgres  # noqa: F401
    except ImportError as exc:
        pytest.fail(f"shared_lib.persistence.legacy_postgres (the PostgreSQL compatibility layer) is missing: {exc}")


def _no_triggers(conn, table):
    for (name,) in conn.execute("SELECT name FROM sqlite_master WHERE type='trigger' AND tbl_name=?", (table,)).fetchall():
        conn.execute(f'DROP TRIGGER "{name}"')


# ═══════════════════════════════════════════════════════════════════════════════════════ source safety
def test_operator_locations_are_refused_without_owner_authorization(drill, tmp_path):
    persistence = ROOT / "backends" / "shared" / "shared_lib" / "persistence"
    refused = [persistence / "cosmicforge.db", persistence / "phase12_paper_validation.db", ROOT / "data" / "bot.db",
               ROOT / "backends" / "bot-backend" / "cosmicforge_trading.db", tmp_path / "data" / "copy.db",
               tmp_path / "backups" / "copy.db", tmp_path / "logs" / "copy.db", tmp_path / "x" / "cosmicforge.db",
               tmp_path / "phase12_paper_validation.db"]
    for path in refused:
        assert drill.legacy_source_refusal(path), path
    assert drill.legacy_source_refusal(tmp_path / "sanitized" / "source.db") is None
    # refused BEFORE anything is read: a path that does not even exist raises the refusal, not FileNotFoundError
    missing = ROOT / "data" / f"never-{uuid.uuid4().hex}.db"
    with pytest.raises(drill.LegacySourceRefused, match="owner-authorization"):
        drill.run_legacy(tmp_path / "work", target_url="postgresql://nobody@127.0.0.1:1/postgres", source=missing)
    code = drill.main(["--legacy", "--workdir", str(tmp_path / "w2"), "--out", str(tmp_path / "o.json"),
                       "--target-url", "postgresql://nobody@127.0.0.1:1/postgres", "--source", str(missing)])
    assert code == drill.LEGACY_EXIT_REFUSED and not (tmp_path / "o.json").exists()


# ═══════════════════════════════════════════════════════════════════════════════════════ source audit
@pytest.fixture(scope="module")
def synthetic_template(tmp_path_factory):
    os.environ.setdefault("COSMICFORGE_TEST_MODE", "1")
    d = _drill()
    path = tmp_path_factory.mktemp("legacy_src") / "template.db"
    info = d.build_legacy_source(path, 3)
    d.single_file(path)
    return path, info


def _copy(template: Path, target: Path) -> Path:
    s, t = sqlite3.connect(str(template)), sqlite3.connect(str(target))
    s.backup(t)
    s.close()
    t.close()
    return target


def test_synthetic_source_is_the_real_schema_with_financial_values(drill, synthetic_template):
    path, info = synthetic_template
    assert info["tables"] >= 290 and info["seeded_tables"] >= info["tables"] - 5
    assert "shared_lib.persistence.migrations.migrate" in info["schema_steps"]
    conn = sqlite3.connect(str(path))
    money = [v for r in conn.execute("SELECT fee, price, qty, realized_pnl, net_pnl, funding_fees FROM trade_fills")
             for v in r if v is not None]
    pool = set(drill.SYNTHETIC_MONEY + drill.SYNTHETIC_SIGNED)
    assert money and set(money) <= pool and len(set(money)) >= 6       # fractional qty, tiny fees, big notionals ...
    joined = conn.execute("SELECT COUNT(*) FROM position_events e JOIN positions p ON p.position_id=e.position_id").fetchone()[0]
    assert joined > 0                                                    # associations are real, not coincidental
    assert conn.execute("PRAGMA foreign_key_check").fetchall() == []
    conn.close()
    assert drill.audit_legacy_source(path)["ok"]


def test_audit_reports_every_inconsistency_by_table_column_and_row(drill, synthetic_template, tmp_path):
    path = _copy(synthetic_template[0], tmp_path / "bad.db")
    conn = sqlite3.connect(str(path))
    for t in ("trade_fills", "positions", "users", "backtest_fills", "bot_instances"):
        _no_triggers(conn, t)
    conn.execute("UPDATE trade_fills SET fee = 9e999 WHERE rowid = 1")                      # +inf money
    conn.execute("UPDATE trade_fills SET price = 'abc' WHERE rowid = 2")                     # text money
    conn.execute("UPDATE trade_fills SET realized_pnl = 1.23456789012345e-07 WHERE rowid = 3")   # 21 fractional digits
    conn.execute("UPDATE positions SET leverage = 'x2' WHERE rowid = 1")                     # text in a REAL ratio
    conn.execute("UPDATE bot_instances SET total_trades = 'many' WHERE rowid = 1")          # text in INTEGER
    conn.execute("UPDATE users SET full_name = 'a' || char(0) || 'b' WHERE rowid = 1")       # NUL in text
    conn.execute("UPDATE backtest_fills SET run_id = 'no-such-run' WHERE rowid = 1")         # FK orphan in the source
    conn.commit()
    conn.close()
    audit = drill.audit_legacy_source(path, target_types={("trade_fills", "realized_pnl"): "numeric:38:18"})
    assert not audit["ok"]
    got = {(i["kind"], i["table"], i["column"], i["rowid"]) for i in audit["issues"]}
    assert ("NON_FINITE_MONEY", "trade_fills", "fee", 1) in got
    assert ("MONEY_TYPE_MISMATCH", "trade_fills", "price", 2) in got
    assert ("MONEY_PRECISION_LOSS", "trade_fills", "realized_pnl", 3) in got
    assert ("TYPE_AFFINITY_MISMATCH", "positions", "leverage", 1) in got
    assert ("TYPE_AFFINITY_MISMATCH", "bot_instances", "total_trades", 1) in got
    assert ("TEXT_WITH_NUL_CHARACTER", "users", "full_name", 1) in got
    assert ("FOREIGN_KEY_ORPHAN", "backtest_fills", None, 1) in got
    # the legacy target type (unconstrained NUMERIC, the default) holds the 21-digit value exactly
    relaxed = drill.audit_legacy_source(path)
    assert ("MONEY_PRECISION_LOSS", "trade_fills", "realized_pnl", 3) not in {
        (i["kind"], i["table"], i["column"], i["rowid"]) for i in relaxed["issues"]}


# ═══════════════════════════════════════════════════════════════════════════════════════ copier (PostgreSQL)
@pytest.fixture
def pg_database():
    need_postgres()
    name = f"cftest_legacy_t_{uuid.uuid4().hex[:10]}"
    _pg_admin(f"CREATE DATABASE {name}")
    try:
        yield _pg_url(name)
    finally:
        _pg_admin(f"DROP DATABASE IF EXISTS {name} WITH (FORCE)")


def test_copier_resumes_from_its_checkpoint_after_a_crash_and_a_rerun_changes_nothing(drill, tmp_path, pg_database):
    import psycopg
    src = tmp_path / "small.db"
    conn = sqlite3.connect(str(src))
    conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
    conn.execute("CREATE TABLE child (id TEXT PRIMARY KEY, parent_id INTEGER REFERENCES parent(id), amount REAL, "
                 "ratio REAL, note TEXT)")
    conn.executemany("INSERT INTO parent (id, name) VALUES (?, ?)", [(i, f"p{i}") for i in range(1, 6)])
    amounts = [0.1 + 0.2, 0.00012345, 123456789.12345679, 1e-08, 2.675, -12.75, 0.001]
    conn.executemany("INSERT INTO child VALUES (?,?,?,?,?)",
                     [(f"c{i}", 1 + i % 5, a, 0.123456789 * i, "O'Brien \\ é") for i, a in enumerate(amounts)])
    conn.commit()
    conn.close()
    with psycopg.connect(pg_database) as pg:
        pg.execute("CREATE TABLE parent (id BIGSERIAL PRIMARY KEY, name TEXT NOT NULL)")
        pg.execute("CREATE TABLE child (id TEXT PRIMARY KEY, parent_id BIGINT REFERENCES parent(id), "
                   "amount NUMERIC(38,18), ratio DOUBLE PRECISION, note TEXT)")
    copier = drill.LegacyCopier(src, pg_database, batch_size=2)
    assert copier.order.index("parent") < copier.order.index("child")
    copier.ensure_checkpoints("sha-of-small")
    with pytest.raises(drill.SimulatedCrash):
        copier.copy(crash=("child", 2))                               # batches 1-2 committed, batch 3 in flight
    assert copier.checkpoint("child") == (4, 4, False)
    copier.close()
    copier = drill.LegacyCopier(src, pg_database, batch_size=3)     # resume with another batch size
    with pytest.raises(RuntimeError, match="ANOTHER_SOURCE"):
        copier.ensure_checkpoints("sha-of-another-file")
    copier.ensure_checkpoints("sha-of-small")
    stats = copier.copy()
    assert stats["resumed"] == {"child": {"from_rowid": 4, "rows_already_copied": 4}}
    assert drill.target_snapshot(pg_database, copier) == drill.source_snapshot(copier)
    with psycopg.connect(pg_database) as pg:
        got = [r[0] for r in pg.execute("SELECT amount FROM child ORDER BY id").fetchall()]
        assert got == [Decimal(repr(a)) for a in amounts]
        assert [float(g) for g in got] == amounts
        pg.execute("INSERT INTO parent (name) VALUES ('after migration')")          # sequence moved past copied ids
        assert pg.execute("SELECT MAX(id) FROM parent").fetchone()[0] == 6
        pg.execute("DELETE FROM parent WHERE id = 6")
    before = drill.target_snapshot(pg_database, copier)
    again = copier.copy()
    assert again["rows_inserted"] == 0 and again["tables_skipped_done"] == 2
    assert drill.target_snapshot(pg_database, copier) == before
    copier.close()


# ═══════════════════════════════════════════════════════════════════════════════════════ the full drill
def test_full_legacy_drill_passes_all_thirteen_checks(drill, tmp_path):
    need_postgres()
    need_compatibility_layer()
    result = drill.run_legacy(tmp_path / "work", target_url=PG_URL, rows=3, batch_size=2)
    failed = {k: result["evidence"].get(k) for k, v in result["checks"].items() if v is not True}
    assert result["passed"] and not failed, failed
    assert len(result["checks"]) == 13 and result["outcome"] == "PASSED"
    ev = result["evidence"]
    assert ev["10_interrupted_migration_recovery"]["resumed_from"] is not None
    assert ev["11_rerun_safety"]["rows_inserted"] == 0
    assert ev["06_financial_value_preservation"]["monetary_values_compared"] > 500
    assert ev["09_research_governance_preservation"]["rows"] > 0
    assert result["detail"]["postgres_float8_cast_comparison"]["changed_by_float8_cast"] > 0
    import psycopg
    with psycopg.connect(PG_URL, autocommit=True) as c:                # nothing left behind
        assert c.execute("SELECT COUNT(*) FROM pg_database WHERE datname = %s",
                         (result["target"]["database"],)).fetchone()[0] == 0


def test_an_inconsistent_source_is_refused_before_anything_is_copied(drill, synthetic_template, tmp_path):
    need_postgres()
    need_compatibility_layer()
    path = _copy(synthetic_template[0], tmp_path / "inconsistent.db")
    conn = sqlite3.connect(str(path))
    _no_triggers(conn, "cati_user_stream_fills")
    conn.execute("UPDATE cati_user_stream_fills SET fee = -9e999 WHERE rowid = 2")
    conn.commit()
    conn.close()
    before = path.read_bytes()
    result = drill.run_legacy(tmp_path / "work", target_url=PG_URL, source=path, rows=3, batch_size=2)
    assert result["outcome"] == "REFUSED_SOURCE_INCONSISTENT" and not result["passed"]
    assert {(i["kind"], i["table"], i["column"], i["rowid"]) for i in result["source_audit"]["issues"]} >= {
        ("NON_FINITE_MONEY", "cati_user_stream_fills", "fee", 2)}
    assert path.read_bytes() == before                                   # the source is never written
