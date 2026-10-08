"""Step 1.0c -- production schema initialisation happens once per database, is
additive and safe to repeat, and the per-cycle code runs no DDL afterwards."""
import os
import sqlite3

import pytest
from shared_lib.persistence import db as shared_db
from shared_lib.persistence.db import DB

from app.execution import production_schema
from app.trading_intelligence.integration import production_execution as production
from app.trading_intelligence.integration import production_runtime as runtime

TABLES = {"cati_production_decisions", "cati_production_daily_risk", "cati_production_fills",
          "cati_production_protection", "cati_production_closes", "cati_account_period_risk",
          "cati_production_state", "cati_execution_evaluations", "cati_production_protection_uncertainty",
          "cati_account_income", "cati_account_income_cursor"}


@pytest.fixture
def db(tmp_path):
    production_schema.forget()
    database = DB(str(tmp_path / "prod.db"))
    yield database
    production_schema.forget()


def tables(db):
    with db.connect() as c:
        return {r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table'")}


def test_startup_creates_every_production_table_once(db):
    assert production_schema.ensure(db) is True
    assert TABLES <= tables(db)
    shared_db.instrument(True)
    try:
        for _ in range(5):                                      # repeated evaluations
            assert production_schema.ensure(db) is False
            production.initialize(db)
            runtime.initialize(db)
        assert shared_db.METRICS.schema_statements == 0 and shared_db.METRICS.connections_opened == 0
    finally:
        shared_db.instrument(False)


def test_repeating_the_schema_on_an_existing_database_is_additive_and_safe(db):
    production_schema.ensure(db)
    with db.connect() as c:
        c.execute("INSERT INTO cati_production_daily_risk VALUES('a','2026-10-08',1000,0)")
    production_schema.ensure(db, force=True)                    # a restart, or an operator re-running init
    with db.connect() as c:
        assert c.execute("SELECT opening_wallet FROM cati_production_daily_risk").fetchone()[0] == 1000


def test_a_database_an_older_build_created_gains_the_new_tables(tmp_path):
    production_schema.forget()
    path = str(tmp_path / "legacy.db")
    legacy = sqlite3.connect(path)
    legacy.execute("CREATE TABLE cati_production_daily_risk (account_id TEXT NOT NULL, day TEXT NOT NULL, "
                   "opening_wallet REAL NOT NULL, loss_latched INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(account_id,day))")
    legacy.execute("INSERT INTO cati_production_daily_risk VALUES('a','2026-10-07',900,1)")
    legacy.commit()
    legacy.close()
    db = DB(path)
    production_schema.ensure(db)
    assert TABLES <= tables(db)
    with db.connect() as c:
        assert c.execute("SELECT loss_latched FROM cati_production_daily_risk").fetchone()[0] == 1


def test_a_database_recreated_at_the_same_path_is_initialised_again(tmp_path):
    production_schema.forget()
    path = str(tmp_path / "again.db")
    first = DB(path)
    production_schema.ensure(first)
    assert production_schema.ensure(first) is False
    for suffix in ("", "-wal", "-shm"):
        if os.path.exists(path + suffix):
            os.remove(path + suffix)
    second = DB(path)                                           # a brand-new file at the old path
    assert production_schema.ensure(second) is True             # the memo keyed on the file, not the path
    assert TABLES <= tables(second)


def test_an_interrupted_initialisation_is_completed_by_the_next_call(db, monkeypatch):
    calls = []
    original = production_schema.DDL

    class Boom(Exception):
        pass

    def failing_connect():
        raise Boom()
    # The first attempt dies before the schema is complete: nothing is memoised.
    monkeypatch.setattr(production_schema, "DDL", original[:2] + ("CREATE TABLE this is not sql",))
    with pytest.raises(sqlite3.OperationalError):
        production_schema.ensure(db)
    monkeypatch.setattr(production_schema, "DDL", original)
    assert production_schema.ensure(db) is True
    assert TABLES <= tables(db)


def test_the_runtime_evaluates_each_account_on_one_scoped_connection(db, monkeypatch):
    """``sync_account`` wraps the whole account cycle in ``db.scope()``."""
    from types import SimpleNamespace
    from unittest.mock import Mock
    from app.activation import account_status
    runtime.initialize(db)
    with db.connect() as c:
        c.execute("CREATE TABLE IF NOT EXISTS bot_instances (id TEXT PRIMARY KEY, broker_account_id TEXT, user_id TEXT, status TEXT)")
    client = Mock()
    client.account.return_value = {"totalWalletBalance": "1000", "totalMarginBalance": "1000", "availableBalance": "1000",
                                   "totalInitialMargin": "0", "totalUnrealizedProfit": "0"}
    client.position_risk.return_value = []
    client.open_orders.return_value = []
    monkeypatch.setattr(runtime, "resolve_broker_auth", lambda *a: SimpleNamespace(environment="demo"))
    monkeypatch.setattr(account_status, "refresh_if_stale", lambda *a, **kw: {"status": "SYNCED"})
    seen = {}

    def process(database, account, client, snapshot):
        seen["scoped"] = getattr(database._local(), "conn", None) is not None
        return {"execution_permission": "BLOCKED_ACCOUNT", "reason": "AUTO_TRADING_DISABLED"}
    monkeypatch.setattr(production, "process_account", process)
    shared_db.instrument(True)
    try:
        runtime.sync_account(db, {"id": "acct", "user_id": "u", "broker_id": "binance", "environment": "DEMO"},
                             factory=lambda credentials: client, execute=True)
        assert seen["scoped"] is True
        assert shared_db.METRICS.connections_opened == 1
        assert shared_db.METRICS.schema_statements == 0
    finally:
        shared_db.instrument(False)
    assert getattr(db._local(), "conn", None) is None
