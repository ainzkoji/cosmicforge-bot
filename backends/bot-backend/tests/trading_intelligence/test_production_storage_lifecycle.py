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


# ── file identity across platforms (found by Linux CI, 8 October 2026) ──────
# The first version keyed the memo on st_ctime, which is the creation time on
# Windows and the last-change time on Linux. On Linux the key moved with every
# write, the memo never matched, and the whole schema ran on every call. The
# tests below emulate the Linux view of a file so that a Windows run guards it too.

def _stat(*, ino=11, dev=3, size=4096, ctime=1, birth=None):
    from types import SimpleNamespace
    st = SimpleNamespace(st_dev=dev, st_ino=ino, st_size=size, st_ctime_ns=ctime)
    if birth is not None:
        st.st_birthtime_ns = birth
    return st


def test_the_file_key_does_not_move_when_a_linux_file_is_written():
    key = production_schema.file_key
    # Linux: st_ctime moves with every write and there is no creation time.
    assert key("p", _stat(ctime=1, size=4096), windows=False) == key("p", _stat(ctime=999, size=65536), windows=False)
    assert production_schema.birth_ns(_stat(ctime=5), windows=False) is None
    # Windows: st_ctime is the creation time, so a recreated file is another file.
    assert key("p", _stat(ctime=1), windows=True) == key("p", _stat(ctime=1, size=65536), windows=True)
    assert key("p", _stat(ctime=1), windows=True) != key("p", _stat(ctime=2), windows=True)
    # A reported creation time is used wherever it exists, and never st_ctime instead of it.
    assert key("p", _stat(ctime=1, birth=7), windows=False) == key("p", _stat(ctime=50, birth=7), windows=False)
    assert key("p", _stat(ctime=1, birth=7), windows=False) != key("p", _stat(ctime=1, birth=8), windows=False)
    assert key("p", _stat(ctime=1, birth=7), windows=True) == key("p", _stat(ctime=2, birth=7), windows=True)
    # Another inode, device or path is another file.
    base = key("p", _stat(), windows=False)
    assert base != key("p", _stat(ino=12), windows=False) != key("p", _stat(dev=4), windows=False)
    assert base != key("q", _stat(), windows=False)


@pytest.fixture
def linux_identity(monkeypatch):
    """Make this process see database files the way Linux reports them: the
    change time moves on every write, there is no creation time, and (worst
    case) a file recreated at a path gets the same inode back."""
    original = production_schema._identity
    state = {"size": None}

    def linux_stat(path):
        from types import SimpleNamespace
        real = os.stat(path)
        return SimpleNamespace(st_dev=7, st_ino=42, st_ctime_ns=real.st_mtime_ns,
                               st_size=real.st_size if state["size"] is None else state["size"])
    monkeypatch.setattr(production_schema, "_identity", lambda db: original(db, stat=linux_stat, windows=False))
    return state


def test_on_linux_the_schema_still_runs_once_while_the_database_is_written(db, linux_identity):
    assert production_schema.ensure(db) is True
    shared_db.instrument(True)
    try:
        for i in range(5):
            with db.connect() as c:                             # every write moves the Linux change time
                c.execute("INSERT INTO cati_production_daily_risk VALUES(?,?,?,0)", (f"a{i}", "2026-10-08", 1000))
            shared_db.METRICS.reset()
            assert production_schema.ensure(db) is False
            production.initialize(db)
            runtime.initialize(db)
            assert shared_db.METRICS.schema_statements == 0 and shared_db.METRICS.connections_opened == 0
    finally:
        shared_db.instrument(False)


def test_on_linux_a_recreated_database_that_got_its_inode_back_is_initialised_again(tmp_path, linux_identity):
    production_schema.forget()
    path = str(tmp_path / "reused.db")
    first = DB(path)
    production_schema.ensure(first)
    with first.connect() as c:
        c.execute("INSERT INTO cati_production_daily_risk VALUES('a','2026-10-08',1000,0)")
    assert production_schema.ensure(first) is False
    for suffix in ("", "-wal", "-shm"):
        if os.path.exists(path + suffix):
            os.remove(path + suffix)
    second = DB(path)
    with second.connect() as c:                                 # the new, smaller file now exists at the old path
        c.execute("CREATE TABLE marker (x)")
    assert production_schema.ensure(second) is True             # same path, device and inode: the size gave it away
    assert TABLES <= tables(second)


def test_a_missing_table_drops_the_memo_when_nothing_else_could_tell(tmp_path, linux_identity):
    production_schema.forget()
    path = str(tmp_path / "stale.db")
    db = DB(path)
    production_schema.ensure(db)
    linux_identity["size"] = os.stat(path).st_size              # same inode AND not smaller: the memo cannot know
    with db.connect() as c:
        c.execute("DROP TABLE cati_production_state")           # what a replaced file looks like to a statement
    assert production_schema.ensure(db) is False
    assert production_schema.missing_table(db, ValueError("ACCOUNT_SUBMIT_OUTCOME_UNRESOLVED")) is False
    assert production_schema.missing_table(db, sqlite3.OperationalError("database is locked")) is False
    assert production_schema.ensure(db) is False                # neither of those is a reason to rebuild
    with pytest.raises(sqlite3.OperationalError) as failure:
        with db.connect() as c:
            c.execute("SELECT 1 FROM cati_production_state")
    assert production_schema.missing_table(db, failure.value) is True
    assert production_schema.ensure(db) is True
    assert TABLES <= tables(db)


def test_a_failed_evaluation_on_a_missing_table_heals_on_the_next_cycle(db, monkeypatch, linux_identity):
    from unittest.mock import Mock
    production_schema.ensure(db)
    linux_identity["size"] = os.stat(db.path).st_size

    def lost(*a, **k):
        raise sqlite3.OperationalError("no such table: cati_production_daily_risk")
    monkeypatch.setattr(production, "_process_account", lost)
    with pytest.raises(sqlite3.OperationalError) as failure:    # the evaluation is recorded, then the failure surfaces
        production.process_account(db, {"id": "acct", "user_id": "u", "broker_id": "binance", "environment": "DEMO"},
                                   Mock(), {"positions": [], "orders": []})
    result = failure.value.production_evaluation
    assert result["stage"] == "EVALUATION_FAILED" and result["reason"] == "OperationalError"
    assert result["execution_permission"] == "BLOCKED_ACCOUNT"  # the failed cycle sent nothing
    assert production_schema.ensure(db) is True                 # and the next one starts from a complete schema

