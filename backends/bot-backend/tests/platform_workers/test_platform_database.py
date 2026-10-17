"""Step 5.1: dialect-neutral persistence, versioned migrations, pooling, integrity and exact money."""
from __future__ import annotations

import threading
from decimal import Decimal

import pytest

from shared_lib.persistence.sql import (ChecksumMismatch, Database, IrreversibleMigration, Migration, PoolExhausted,
                                        compatibility, current_version, decimal_in, decimal_out, migrate, rollback)
from shared_lib.persistence.sql.platform_schema import all_migrations

T = ["CREATE TABLE t_one (id INTEGER PRIMARY KEY, v TEXT)", "CREATE TABLE t_two (id INTEGER PRIMARY KEY)"]


def _tables(db):
    with db.transaction(write=False) as conn:
        if db.dialect == "sqlite":
            return {r[0] for r in conn.all("SELECT name FROM sqlite_master WHERE type='table'")}
        return {r[0] for r in conn.all("SELECT tablename FROM pg_tables WHERE schemaname='public'")}


def test_fresh_apply_and_idempotent_rerun(database_url):
    db = Database(database_url)
    first = migrate(db, all_migrations())
    assert first.applied == [m.version for m in all_migrations()]
    again = migrate(db, all_migrations())
    assert again.applied == [] and again.skipped == first.applied
    assert {"platform_jobs", "platform_account_assignments", "platform_symbol_budgets"} <= _tables(db)
    assert db.health()["ready"] and db.health()["schema_version"] == first.final_version


def test_interrupted_migration_leaves_no_partial_version_and_resumes(database_url):
    db = Database(database_url)
    migs = [Migration(1, "base", ["CREATE TABLE base_t (id INTEGER PRIMARY KEY)"]), Migration(2, "two", T)]

    def crash(version, index):
        if version == 2 and index == 0:
            raise RuntimeError("power loss between statements")

    with pytest.raises(RuntimeError):
        migrate(db, migs, fail_after=crash)
    assert current_version(db) == 1
    assert "t_one" not in _tables(db), "first statement of the interrupted version must be rolled back"
    report = migrate(db, migs)
    assert report.applied == [2] and {"t_one", "t_two"} <= _tables(db)


def test_edited_applied_migration_is_refused(database_url):
    db = Database(database_url)
    migrate(db, [Migration(1, "one", T)])
    with pytest.raises(ChecksumMismatch):
        migrate(db, [Migration(1, "one", ["CREATE TABLE t_one (id INTEGER PRIMARY KEY, v TEXT, extra TEXT)", T[1]])])


def test_rollback_and_reapply_and_irreversible_refusal(database_url):
    db = Database(database_url)
    migs = all_migrations()
    migrate(db, migs)
    undone = rollback(db, migs, to_version=0)
    assert undone == sorted((m.version for m in migs), reverse=True)
    assert not {"platform_jobs", "platform_workers"} & _tables(db)
    assert migrate(db, migs).final_version == max(m.version for m in migs)

    db2 = Database(database_url.replace("platform.db", "irrev.db")) if db.dialect == "sqlite" else db
    rollback(db2, migs, to_version=0) if db2 is db else None
    irrev = [Migration(1, "one", T, irreversible=True)]
    migrate(db2, irrev)
    with pytest.raises(IrreversibleMigration):
        rollback(db2, irrev, to_version=0)
    assert current_version(db2) == 1


def test_compatibility_blocks_old_binary_on_irreversible_schema(database_url):
    db = Database(database_url)
    migs = [Migration(1, "a", ["CREATE TABLE ca (id INTEGER)"]), Migration(2, "b", ["CREATE TABLE cb (id INTEGER)"]),
            Migration(3, "c", ["CREATE TABLE cc (id INTEGER)"], irreversible=True)]
    migrate(db, migs, target=2)
    assert compatibility(db, app_min_schema=1, app_max_schema=1).compatible  # additive newer schema
    assert not compatibility(db, app_min_schema=3, app_max_schema=3).compatible  # binary needs migration first
    migrate(db, migs)
    verdict = compatibility(db, app_min_schema=1, app_max_schema=2)
    assert not verdict.compatible and "IRREVERSIBLE" in verdict.reason


def test_concurrent_migration_jobs_apply_each_version_once(database_url):
    errors = []

    def run():
        try:
            migrate(Database(database_url), all_migrations())
        except Exception as exc:  # pragma: no cover - reported below
            errors.append(exc)

    threads = [threading.Thread(target=run) for _ in range(4)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    assert not errors
    db = Database(database_url)
    with db.transaction(write=False) as conn:
        rows = conn.all("SELECT version, COUNT(*) FROM platform_schema_migrations GROUP BY version")
    assert all(n == 1 for _, n in rows) and len(rows) == len(all_migrations())


def test_foreign_keys_are_enforced(platform_db):
    with pytest.raises(Exception):
        with platform_db.transaction() as conn:
            conn.execute("INSERT INTO platform_account_assignments (account_id, worker_id, fencing_token) VALUES (?,?,?)",
                         ("no-such-account", None, 1))
    with platform_db.transaction(write=False) as conn:
        assert conn.scalar("SELECT COUNT(*) FROM platform_account_assignments") == 0


def test_uniqueness_constraints(platform_db):
    with platform_db.transaction() as conn:
        conn.execute("INSERT INTO platform_accounts VALUES ('a1','t1','binance','DEMO','ACTIVE',0)")
    with pytest.raises(Exception):
        with platform_db.transaction() as conn:
            conn.execute("INSERT INTO platform_accounts VALUES ('a1','t2','binance','DEMO','ACTIVE',0)")


def test_money_is_exact_and_floats_are_rejected(platform_db):
    with pytest.raises(TypeError):
        decimal_in(0.1)
    with platform_db.transaction() as conn:
        conn.execute("INSERT INTO platform_accounts VALUES ('a1','t1','binance','DEMO','ACTIVE',0)")
        for i in range(10):
            conn.execute(
                "INSERT INTO platform_order_intents (intent_id, idempotency_key, account_id, tenant_id, venue, symbol, "
                "side, quantity, client_order_id, status, fencing_token, created_at, updated_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (f"i{i}", f"k{i}", "a1", "t1", "binance", "BTCUSDT", "BUY", decimal_in("0.1"), f"c{i}", "PENDING", 1, 0, 0))
        values = [decimal_out(r[0]) for r in conn.all("SELECT quantity FROM platform_order_intents")]
    assert sum(values, Decimal(0)) == Decimal("1")  # exact; 0.1 summed ten times as binary floats is not 1
    assert all(isinstance(v, Decimal) for v in values)


def test_server_clock_is_used(platform_db):
    import time
    with platform_db.transaction(write=False) as conn:
        db_now = conn.now_ms()
    assert abs(db_now - time.time() * 1000) < 5_000  # same host here; the point is that it is the server's clock


def test_pool_is_bounded_and_discards_broken_connections(database_url):
    db = Database(database_url, pool_size=2, checkout_timeout_s=0.2)
    a, b = db.pool.get(), db.pool.get()
    with pytest.raises(PoolExhausted):
        db.pool.get()
    db.pool.put(a)
    db.pool.put(b, broken=True)
    assert db.pool.stats["discarded"] == 1 and db.pool.stats["peak_in_use"] == 2
    with db.transaction() as conn:
        assert conn.scalar("SELECT 1") == 1


def test_failed_transaction_rolls_back(platform_db):
    with pytest.raises(ZeroDivisionError):
        with platform_db.transaction() as conn:
            conn.execute("INSERT INTO platform_accounts VALUES ('a1','t1','binance','DEMO','ACTIVE',0)")
            1 / 0
    with platform_db.transaction(write=False) as conn:
        assert conn.scalar("SELECT COUNT(*) FROM platform_accounts") == 0


def test_health_reports_unavailable_database():
    db = Database("postgresql://nobody@127.0.0.1:1/none", checkout_timeout_s=0.5, connect_timeout_s=1)
    health = db.health()
    assert health["ready"] is False and "error" in health
