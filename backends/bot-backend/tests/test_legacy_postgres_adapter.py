"""The legacy runtime's PostgreSQL adapter against a real server (EXT-DB-01, Step 5.1).

Needs ``LEGACY_TEST_POSTGRES_URL`` (a maintenance database URL; every test creates and drops its own
``cftest_adapter_*`` database). Skipped without it, unless ``LEGACY_TEST_POSTGRES_REQUIRED=1`` (CI), which turns
the skip into a failure. Run it WITHOUT ``-p legacy_pg_plugin`` (it opens PostgreSQL itself and deliberately provokes
the dialect gaps the plugin would report).

Several tests are differential: the same SQL and data run on ``sqlite3`` and through the adapter, and the results
must be identical.
"""
from __future__ import annotations

import os
import socket
import sqlite3
import threading
import time
import uuid

import pytest

ADMIN_URL = os.environ.get("LEGACY_TEST_POSTGRES_URL")
REQUIRED = os.environ.get("LEGACY_TEST_POSTGRES_REQUIRED") == "1"

if not ADMIN_URL:
    if REQUIRED:
        raise RuntimeError("LEGACY_TEST_POSTGRES_REQUIRED=1 but LEGACY_TEST_POSTGRES_URL is not set")
    pytest.skip("LEGACY_TEST_POSTGRES_URL not set", allow_module_level=True)

psycopg = pytest.importorskip("psycopg")

from shared_lib.persistence import legacy_postgres as LP  # noqa: E402
from shared_lib.persistence.sql import legacy_dialect as D  # noqa: E402


def _url(name: str) -> str:
    from urllib.parse import urlsplit, urlunsplit

    p = urlsplit(ADMIN_URL)
    return urlunsplit((p.scheme, p.netloc, "/" + name, "", ""))


@pytest.fixture
def pg_url():
    name = "cftest_adapter_" + uuid.uuid4().hex[:10]
    with psycopg.connect(ADMIN_URL, autocommit=True) as admin:
        admin.execute(f"CREATE DATABASE {name} TEMPLATE template0 ENCODING 'UTF8' LC_COLLATE 'C' LC_CTYPE 'C'")
    url = _url(name)
    try:
        yield url
    finally:
        LP.close_pools(url)
        with psycopg.connect(ADMIN_URL, autocommit=True) as admin:
            admin.execute(f"DROP DATABASE IF EXISTS {name} WITH (FORCE)")


@pytest.fixture
def conn(pg_url):
    c = LP.connect(pg_url)
    yield c
    c.close()


def both(pg_conn, statements, query, params=()):
    """Run ``statements`` then ``query`` on an in-memory SQLite database and on the adapter."""
    s = sqlite3.connect(":memory:")
    for st in statements:
        s.execute(st)
        pg_conn.execute(st)
    s.commit()
    pg_conn.commit()
    return s.execute(query, params).fetchall(), [tuple(r) for r in pg_conn.execute(query, params).fetchall()]


# ------------------------------------------------------------------------------------------------ rows & basics
def test_rows_behave_like_sqlite3_rows(conn):
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, Name TEXT, qty REAL)")
    conn.execute("INSERT INTO t (Name, qty) VALUES (?, ?)", ("a", 1.5))
    conn.commit()
    conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT * FROM t").fetchone()
    assert row[0] == 1 and row["name"] == "a" and row["NAME"] == "a" and row[2] == 1.5
    # the declared (mixed-case) column name, as SQLite reports it
    assert row.keys() == ["id", "Name", "qty"] and dict(row) == {"id": 1, "Name": "a", "qty": 1.5}
    s = sqlite3.connect(":memory:")
    s.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, Name TEXT, qty REAL)")
    assert [d[0] for d in s.execute("SELECT * FROM t").description] == row.keys()
    assert tuple(row) == (1, "a", 1.5) and len(row) == 3
    with pytest.raises(IndexError):
        row["missing"]
    conn.row_factory = None
    assert conn.execute("SELECT id, name FROM t").fetchall() == [(1, "a")]
    conn.row_factory = lambda cur, r: {d[0]: v for d, v in zip(cur.description, r)}
    assert conn.execute("SELECT COUNT(*) FROM t").fetchone() == {"COUNT(*)": 1}
    cur = conn.execute("SELECT id FROM t")
    assert cur.rowcount == -1 and cur.description[0][0] == "id"
    assert [r for r in conn.execute("SELECT id FROM t")] == [{"id": 1}]


def test_implicit_transaction_commit_and_rollback(conn, pg_url):
    conn.execute("CREATE TABLE t (v INTEGER)")
    assert not conn.in_transaction  # DDL does not open a transaction (Python sqlite3 >= 3.6)
    conn.execute("INSERT INTO t VALUES (1)")
    assert conn.in_transaction
    other = LP.connect(pg_url)
    try:
        assert other.execute("SELECT COUNT(*) FROM t").fetchone()[0] == 0  # not committed yet
        conn.rollback()
        assert not conn.in_transaction
        conn.execute("INSERT INTO t VALUES (2)")
        conn.commit()
        assert other.execute("SELECT v FROM t").fetchall() == [(2,)]
    finally:
        other.close()


def test_context_manager_commits_on_success_and_rolls_back_on_exception(conn):
    conn.execute("CREATE TABLE t (v INTEGER)")
    with conn:
        conn.execute("INSERT INTO t VALUES (1)")
    with pytest.raises(ZeroDivisionError):
        with conn:
            conn.execute("INSERT INTO t VALUES (2)")
            1 / 0
    assert conn.execute("SELECT v FROM t").fetchall() == [(1,)]


def test_a_failing_statement_inside_a_transaction_is_undone_alone(conn):
    conn.execute("CREATE TABLE t (v INTEGER UNIQUE)")
    conn.execute("INSERT INTO t VALUES (1)")
    with pytest.raises(sqlite3.IntegrityError, match="UNIQUE constraint failed: t.v"):
        conn.execute("INSERT INTO t VALUES (1)")
    with pytest.raises(sqlite3.OperationalError, match="no such column: nope"):
        conn.execute("SELECT nope FROM t")
    conn.execute("INSERT INTO t VALUES (2)")  # the transaction is still usable, as on SQLite
    conn.commit()
    assert conn.execute("SELECT v FROM t ORDER BY v").fetchall() == [(1,), (2,)]


def test_lastrowid_for_integer_primary_key_and_rowid_tables(conn):
    conn.execute("CREATE TABLE a (id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)")
    conn.execute("CREATE TABLE b (k TEXT PRIMARY KEY, v TEXT)")
    assert conn.execute("INSERT INTO a (v) VALUES ('x')").lastrowid == 1
    assert conn.execute("INSERT INTO a (v) VALUES ('y')").lastrowid == 2
    rid = conn.execute("INSERT INTO b VALUES ('k1', 'v')").lastrowid
    assert isinstance(rid, int) and rid >= 1
    ignored = conn.execute("INSERT OR IGNORE INTO b VALUES ('k1', 'again')")
    assert ignored.rowcount == 0 and ignored.lastrowid == rid  # unchanged, as sqlite3
    conn.execute("INSERT INTO a (v) VALUES ('z')")
    conn.execute("UPDATE a SET v = 'last' WHERE id = last_insert_rowid()")
    assert conn.execute("SELECT v FROM a WHERE id = 3").fetchone()[0] == "last"
    conn.execute("INSERT INTO a (id, v) VALUES (10, 'explicit')")
    assert conn.execute("INSERT INTO a (v) VALUES ('after')").lastrowid == 11  # SQLite: max(rowid) + 1
    assert [r[0] for r in conn.execute("SELECT rowid FROM b")] == [rid]
    conn.commit()


def test_insert_or_replace_has_sqlite_semantics(conn):
    conn.execute("CREATE TABLE r (id TEXT PRIMARY KEY, email TEXT UNIQUE, note TEXT DEFAULT 'd', n INTEGER)")
    conn.execute("INSERT INTO r VALUES ('1', 'a@x', 'first', 1)")
    conn.execute("INSERT INTO r VALUES ('2', 'b@x', 'second', 2)")
    # conflicts with row 1 on the PK and with row 2 on email: SQLite deletes BOTH, then inserts
    cur = conn.execute("INSERT OR REPLACE INTO r (id, email, n) VALUES (?, ?, ?)", ("1", "b@x", 3))
    assert cur.rowcount == 1
    assert conn.execute("SELECT id, email, note, n FROM r").fetchall() == [("1", "b@x", "d", 3)]
    conn.execute("REPLACE INTO r VALUES (?, ?, ?, ?)", ("1", "c@x", "n", 4))  # no column list: catalog order
    assert conn.execute("SELECT id, email, note, n FROM r").fetchall() == [("1", "c@x", "n", 4)]
    conn.executemany("INSERT OR REPLACE INTO r (id, email) VALUES (?, ?)", [("5", "e@x"), ("5", "f@x")])
    assert conn.execute("SELECT email FROM r WHERE id = '5'").fetchall() == [("f@x",)]
    conn.execute("INSERT OR REPLACE INTO r (id, email) VALUES ('6', NULL)")
    conn.execute("INSERT OR REPLACE INTO r (id, email) VALUES ('7', NULL)")  # NULLs never conflict
    assert conn.execute("SELECT COUNT(*) FROM r WHERE email IS NULL").fetchone()[0] == 2
    conn.commit()


def test_insert_or_ignore_skips_unique_and_not_null_violations(conn):
    conn.execute("CREATE TABLE g (k TEXT PRIMARY KEY, v TEXT NOT NULL)")
    assert conn.execute("INSERT OR IGNORE INTO g VALUES ('a', 'x')").rowcount == 1
    assert conn.execute("INSERT OR IGNORE INTO g VALUES ('a', 'y')").rowcount == 0
    assert conn.execute("INSERT OR IGNORE INTO g (k) VALUES ('b')").rowcount == 0  # NOT NULL: skipped, as SQLite
    with pytest.raises(sqlite3.IntegrityError, match="NOT NULL constraint failed: g.v"):
        conn.execute("INSERT INTO g (k) VALUES ('c')")
    conn.commit()
    assert conn.execute("SELECT k, v FROM g").fetchall() == [("a", "x")]


# ------------------------------------------------------------------------------------------------ triggers
def test_trigger_guards_are_preserved(conn):
    conn.executescript("""
        CREATE TABLE ev (id INTEGER PRIMARY KEY, kind TEXT, state TEXT);
        CREATE TABLE log (event_id INTEGER, state TEXT);
        CREATE TRIGGER IF NOT EXISTS ev_no_delete BEFORE DELETE ON ev BEGIN SELECT RAISE(ABORT, 'ev is append-only'); END;
        CREATE TRIGGER ev_final BEFORE UPDATE OF state ON ev WHEN OLD.state = 'DONE' AND NEW.state <> OLD.state
            BEGIN SELECT RAISE(ABORT, 'ev: a final event does not change'); END;
        CREATE TRIGGER ev_log AFTER INSERT ON ev BEGIN INSERT INTO log VALUES (NEW.id, NEW.state); END;
        CREATE TRIGGER ev_skip BEFORE INSERT ON ev WHEN NEW.kind = 'noise' BEGIN SELECT RAISE(IGNORE); END;
    """)
    conn.execute("INSERT INTO ev (kind, state) VALUES ('a', 'OPEN')")
    conn.execute("INSERT INTO ev (kind, state) VALUES ('noise', 'OPEN')")  # RAISE(IGNORE): silently skipped
    conn.execute("UPDATE ev SET state = 'DONE' WHERE id = 1")
    with pytest.raises(sqlite3.IntegrityError) as excinfo:
        conn.execute("UPDATE ev SET state = 'OPEN' WHERE id = 1")
    assert str(excinfo.value) == "ev: a final event does not change"  # SQLite's exact class and text
    with pytest.raises(sqlite3.IntegrityError, match="^ev is append-only$"):
        conn.execute("DELETE FROM ev")
    conn.execute("UPDATE ev SET state = 'DONE' WHERE id = 1")  # no change of state: allowed
    conn.commit()
    assert conn.execute("SELECT COUNT(*) FROM ev").fetchone()[0] == 1
    assert conn.execute("SELECT event_id, state FROM log").fetchall() == [(1, "OPEN")]
    conn.execute("CREATE TRIGGER IF NOT EXISTS ev_no_delete BEFORE DELETE ON ev BEGIN SELECT RAISE(ABORT, 'x'); END")
    names = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type = 'trigger'")}
    assert names == {"ev_no_delete", "ev_final", "ev_log", "ev_skip"}
    conn.execute("DROP TRIGGER IF EXISTS ev_no_delete")
    conn.execute("DELETE FROM ev")
    conn.commit()


# ------------------------------------------------------------------------------------------------ locking
def test_the_legacy_write_lock_serializes_two_writers(pg_url):
    a, b = LP.connect(pg_url), LP.connect(pg_url)
    try:
        a.execute("CREATE TABLE c (n INTEGER)")
        a.execute("INSERT INTO c VALUES (0)")
        a.commit()
        order = []
        a.execute("BEGIN IMMEDIATE")
        n = a.execute("SELECT n FROM c").fetchone()[0]

        def writer_b():
            b.execute("BEGIN IMMEDIATE")  # must wait for A's write lock
            order.append("b-locked")
            v = b.execute("SELECT n FROM c").fetchone()[0]
            b.execute("UPDATE c SET n = ?", (v + 1,))
            b.commit()

        t = threading.Thread(target=writer_b)
        t.start()
        time.sleep(0.5)
        assert order == []  # B is blocked while A holds the lock
        a.execute("UPDATE c SET n = ?", (n + 1,))
        order.append("a-commit")
        a.commit()
        t.join(10)
        assert order == ["a-commit", "b-locked"]
        assert a.execute("SELECT n FROM c").fetchone()[0] == 2  # no lost update
    finally:
        a.close()
        b.close()


def test_a_lock_not_granted_in_time_is_database_is_locked(pg_url):
    a, b = LP.connect(pg_url), LP.connect(pg_url)
    try:
        a.execute("CREATE TABLE c (n INTEGER)")
        a.execute("BEGIN IMMEDIATE")
        b.execute("PRAGMA busy_timeout = 300")
        with pytest.raises(sqlite3.OperationalError, match="database is locked"):
            b.execute("INSERT INTO c VALUES (1)")  # implicit BEGIN + first write takes the lock
        assert not b.in_transaction or b.rollback() is None
        a.rollback()
    finally:
        a.close()
        b.close()


# ------------------------------------------------------------------------------------------------ pool
def test_the_pool_recovers_after_the_backend_connection_is_killed(pg_url):
    c = LP.connect(pg_url)
    pid = c._raw.info.backend_pid
    c.close()  # back to the pool
    with psycopg.connect(pg_url, autocommit=True) as killer:
        killer.execute("SELECT pg_terminate_backend(%s)", (pid,))
    time.sleep(0.2)
    c2 = LP.connect(pg_url)  # the dead connection fails its checkout health check and is replaced
    try:
        assert c2._raw.info.backend_pid != pid
        assert c2.execute("SELECT 1").fetchone()[0] == 1
    finally:
        c2.close()
    stats = LP.pool_stats(pg_url)
    assert stats["discarded"] >= 1


def test_a_connection_killed_mid_transaction_is_never_reused(pg_url):
    c = LP.connect(pg_url)
    c.execute("CREATE TABLE t (v INTEGER)")
    c.execute("INSERT INTO t VALUES (1)")
    with psycopg.connect(pg_url, autocommit=True) as killer:
        killer.execute("SELECT pg_terminate_backend(%s)", (c._raw.info.backend_pid,))
    with pytest.raises(sqlite3.OperationalError):
        c.execute("INSERT INTO t VALUES (2)")
    c.close()
    c2 = LP.connect(pg_url)
    try:
        assert c2.execute("SELECT COUNT(*) FROM t").fetchone()[0] == 0  # the killed transaction never committed
    finally:
        c2.close()


# ------------------------------------------------------------------------------------------------ catalog
def test_pragmas_and_sqlite_master(conn):
    conn.execute("CREATE TABLE p (id INTEGER PRIMARY KEY, name TEXT NOT NULL DEFAULT 'x', amt REAL, k TEXT)")
    conn.execute("CREATE UNIQUE INDEX ux_p ON p(name, k)")
    info = conn.execute("PRAGMA table_info(p)").fetchall()
    assert [tuple(r) for r in info] == [(0, "id", "INTEGER", 1, None, 1), (1, "name", "TEXT", 1, "'x'", 0),
                                        (2, "amt", "REAL", 0, None, 0), (3, "k", "TEXT", 0, None, 0)]
    assert conn.execute("PRAGMA table_info(missing)").fetchall() == []
    idx = {r[1]: (r[2], r[3]) for r in conn.execute("PRAGMA index_list(p)")}
    assert idx["ux_p"] == (1, "c")
    assert [r[2] for r in conn.execute("PRAGMA index_info(ux_p)")] == ["name", "k"]
    assert conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", ("p",)).fetchone() == (1,)
    assert conn.execute("SELECT sql FROM sqlite_master WHERE name='p'").fetchone()[0].startswith("CREATE TABLE p")
    conn.execute("ALTER TABLE p ADD COLUMN extra TEXT")
    assert "extra TEXT" in conn.execute("SELECT sql FROM sqlite_master WHERE name='p'").fetchone()[0]
    assert conn.execute("PRAGMA user_version").fetchone()[0] == 0
    conn.execute("PRAGMA user_version = 5")
    assert conn.execute("PRAGMA user_version").fetchone()[0] == 5
    assert conn.execute("PRAGMA journal_mode=WAL").fetchone()[0] == "wal"
    conn.execute("PRAGMA foreign_keys=ON")  # documented no-op: legacy foreign keys are declarative
    assert conn.execute("PRAGMA foreign_keys").fetchone()[0] == 0
    with pytest.raises(D.DialectUnsupported):
        conn.execute("PRAGMA integrity_check")


def test_errors_carry_sqlite_classes_and_messages(conn):
    with pytest.raises(sqlite3.OperationalError, match="^no such table: nope$"):
        conn.execute("SELECT * FROM nope")
    conn.execute("CREATE TABLE e (a TEXT)")
    with pytest.raises(sqlite3.OperationalError, match="^duplicate column name: a$"):
        conn.execute("ALTER TABLE e ADD COLUMN a TEXT")
    with pytest.raises(sqlite3.ProgrammingError, match="Incorrect number of bindings"):
        conn.execute("SELECT ?", ())
    with pytest.raises(sqlite3.ProgrammingError, match="type 'dict' is not supported"):
        conn.execute("SELECT ?", ({"a": 1},))
    with pytest.raises(sqlite3.ProgrammingError, match="one statement at a time"):
        conn.execute("SELECT 1; SELECT 2")


def test_type_mismatches_are_raised_and_recorded_never_coerced(conn):
    conn.execute("CREATE TABLE m (txt TEXT, n INTEGER)")
    before = len(D.unsupported_events())
    with pytest.raises(sqlite3.OperationalError, match="type mismatch"):
        conn.execute("SELECT 1 FROM m WHERE txt = ? + 1", (5,))
    with pytest.raises(sqlite3.OperationalError, match="type mismatch"):
        conn.execute("INSERT INTO m (n) VALUES (?)", (1.5,))  # PostgreSQL would round; SQLite stores REAL
    assert len(D.unsupported_events()) >= before + 2
    conn.rollback()


# ------------------------------------------------------------------------------------------------ values
def test_money_columns_store_the_exact_decimal_and_real_affinity(conn):
    from shared_lib.persistence.legacy_money import MONETARY_COLUMNS

    assert "opening_wallet" in MONETARY_COLUMNS["cati_production_daily_risk"]
    conn.execute("CREATE TABLE cati_production_daily_risk (account_id TEXT, day TEXT, opening_wallet REAL NOT NULL, "
                 "loss_latched INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(account_id, day))")
    typ = conn._raw.execute("SELECT format_type(atttypid, atttypmod) FROM pg_attribute WHERE attrelid = "
                            "'cati_production_daily_risk'::regclass AND attname = 'opening_wallet'").fetchone()[0]
    assert typ.startswith("numeric")
    value = 0.1 + 0.2
    conn.execute("INSERT INTO cati_production_daily_risk VALUES (?, ?, ?, 0)", ("a", "d1", value))
    conn.execute("INSERT INTO cati_production_daily_risk VALUES (?, ?, ?, 0)", ("a", "d2", 5))
    conn.commit()
    stored = conn._raw.execute("SELECT opening_wallet::text FROM cati_production_daily_risk ORDER BY day").fetchall()
    assert [r[0] for r in stored] == ["0.30000000000000004", "5.0"]  # repr, never the 15-digit float8 cast
    back = [r[0] for r in conn.execute("SELECT opening_wallet FROM cati_production_daily_risk ORDER BY day")]
    assert back == [value, 5.0] and all(type(v) is float for v in back)
    assert conn.execute("SELECT SUM(loss_latched) FROM cati_production_daily_risk").fetchone()[0] == 0  # int


def test_a_rebuilt_table_takes_the_money_types_of_its_final_name(conn):
    conn.execute("CREATE TABLE monthly_snapshots_new (id TEXT, start_equity REAL, note REAL)")
    conn.execute("INSERT INTO monthly_snapshots_new VALUES ('x', 0.30000000000000004, 1.5)")
    conn.commit()
    conn.execute("ALTER TABLE monthly_snapshots_new RENAME TO monthly_snapshots")
    from shared_lib.persistence.legacy_money import MONETARY_COLUMNS

    types = dict(conn._raw.execute("SELECT attname, format_type(atttypid, atttypmod) FROM pg_attribute WHERE "
                                   "attrelid = 'monthly_snapshots'::regclass AND attnum > 0").fetchall())
    if "start_equity" in MONETARY_COLUMNS.get("monthly_snapshots", ()):
        assert types["start_equity"].startswith("numeric")
        assert conn._raw.execute("SELECT start_equity::text FROM monthly_snapshots").fetchone()[0] == \
            "0.30000000000000004"
    assert conn.execute("SELECT start_equity FROM monthly_snapshots").fetchone()[0] == 0.30000000000000004


# ------------------------------------------------------------------------------------------------ differential
SEMANTICS = [
    "CREATE TABLE s (k TEXT, a INTEGER, b REAL, ts TEXT)",
    "INSERT INTO s VALUES ('x', 1, 2.5, '2026-01-31T10:20:30.123456+00:00')",
    "INSERT INTO s VALUES ('X', NULL, -2.5, '2026-02-28 23:59:59')",
    "INSERT INTO s VALUES ('y', 3, NULL, NULL)",
]


@pytest.mark.parametrize("query", [
    "SELECT k, a FROM s ORDER BY a",
    "SELECT k, a FROM s ORDER BY a DESC",
    "SELECT k FROM s WHERE k LIKE 'x'",
    "SELECT k FROM s WHERE k NOT LIKE 'X%'",
    "SELECT MAX(a, 2), MIN(a, 2) FROM s ORDER BY k",
    "SELECT CAST(b AS INTEGER), CAST('12abc' AS INTEGER), CAST('x' AS REAL) FROM s ORDER BY k",
    "SELECT ROUND(b), ROUND(2.345, 2), ROUND(-2.5) FROM s ORDER BY k",
    "SELECT IFNULL(a, -1), COALESCE(b, 0) FROM s ORDER BY k",
    "SELECT TOTAL(a), SUM(a), COUNT(*), AVG(b) FROM s",
    "SELECT instr(k, 'y'), length(k), upper(k), substr(ts, 1, 10) FROM s ORDER BY k",
    "SELECT datetime(ts), date(ts), time(ts) FROM s ORDER BY k",
    "SELECT datetime(ts, '+1 day'), datetime(ts, '-2 hours'), datetime(ts, '+1 months'), date(ts, 'start of month') FROM s ORDER BY k",
    "SELECT strftime('%Y-%m-%d %H:%M:%S', ts), strftime('%Y-%m', ts), strftime('%H', ts), strftime('%s', ts), strftime('%j %w %W', ts) FROM s ORDER BY k",
    "SELECT strftime('%f', ts), julianday(ts) FROM s ORDER BY k",
    "SELECT julianday('2026-03-01') - julianday('2026-02-01'), datetime(1700000000, 'unixepoch'), date('2026-01-31', '+1 month')",
    "SELECT datetime('not a date'), date('2026-13-01'), strftime('%Y', NULL)",
    "SELECT a IS NULL, a IS NOT NULL, k IS 'x' FROM s ORDER BY k",
    "SELECT a FROM s WHERE a IN () ORDER BY k",
    "SELECT k FROM s ORDER BY k LIMIT 1, 2",
    "SELECT 7 / 2, 7 % 3, 7.0 / 2",
    "SELECT COUNT(*) FROM s WHERE b > ?",
])
def test_sql_semantics_match_sqlite(conn, query):
    params = (0.0,) if "?" in query else ()
    expected, actual = both(conn, SEMANTICS, query, params)

    def norm(rows):
        return [tuple(round(v, 9) if isinstance(v, float) else v for v in r) for r in rows]

    assert norm(actual) == norm(expected)


def test_group_concat_and_now(conn):
    conn.executescript("CREATE TABLE g (k TEXT, v TEXT); INSERT INTO g VALUES ('a', 'x'); INSERT INTO g VALUES ('a', 'y');")
    assert sorted(conn.execute("SELECT GROUP_CONCAT(v) FROM g").fetchone()[0].split(",")) == ["x", "y"]
    assert conn.execute("SELECT GROUP_CONCAT(DISTINCT k) FROM g").fetchone()[0] == "a"
    now = conn.execute("SELECT datetime('now'), CURRENT_TIMESTAMP").fetchone()
    assert len(now[0]) == 19 and now[0] == now[1]


def test_savepoints_and_executescript(conn):
    conn.execute("CREATE TABLE s (v INTEGER)")
    conn.execute("SAVEPOINT outer_sp")  # outside a transaction: starts one, as SQLite
    assert conn.in_transaction
    conn.execute("INSERT INTO s VALUES (1)")
    conn.execute("SAVEPOINT inner_sp")
    conn.execute("INSERT INTO s VALUES (2)")
    conn.execute("ROLLBACK TO SAVEPOINT inner_sp")
    conn.execute("RELEASE SAVEPOINT inner_sp")
    conn.execute("RELEASE SAVEPOINT outer_sp")  # releasing the outermost savepoint commits
    assert not conn.in_transaction
    assert conn.execute("SELECT v FROM s").fetchall() == [(1,)]
    conn.execute("INSERT INTO s VALUES (9)")
    conn.executescript("INSERT INTO s VALUES (3); INSERT INTO s VALUES (4);")  # commits the pending 9 first
    assert sorted(r[0] for r in conn.execute("SELECT v FROM s")) == [1, 3, 4, 9]


# ------------------------------------------------------------------------------------------------ DB + runtime
def test_db_runs_on_a_postgres_url(pg_url, monkeypatch):
    from shared_lib.persistence.db import DB

    monkeypatch.setenv("COSMICFORGE_TEST_MODE", "1")
    db = DB(url=pg_url)
    assert db.dialect == "postgresql" and db.path.startswith("postgresql://") and ":" not in db.path.split("@")[0][13:]
    with db.connect() as c:
        c.execute("CREATE TABLE scoped (v INTEGER)")
    with db.scope():
        with db.connect() as c:
            c.execute("INSERT INTO scoped VALUES (1)")
        with pytest.raises(RuntimeError):
            with db.connect() as c:
                c.execute("INSERT INTO scoped VALUES (2)")
                raise RuntimeError("boom")
        with db.connect() as c:
            assert [r[0] for r in c.execute("SELECT v FROM scoped")] == [1]
    with db.connect() as c:
        assert c.execute("SELECT 1 FROM sqlite_master WHERE name = 'users'").fetchone() is not None  # DB._init ran
    assert DB(pg_url).dialect == "postgresql"


def test_test_mode_refuses_a_postgres_database_that_is_not_a_test_database(monkeypatch):
    from shared_lib.persistence.test_isolation import TestDatabaseIsolationError, enforce_test_database

    monkeypatch.setenv("COSMICFORGE_TEST_MODE", "1")
    with pytest.raises(TestDatabaseIsolationError):
        enforce_test_database("postgresql://u@h:5432/cosmicforge")
    enforce_test_database("postgresql://u@h:5432/cftest_x")


def test_runtime_ownership_lease_on_a_shared_server(pg_url, monkeypatch):
    from shared_lib.persistence.db import DB
    from app.ops import runtime_ownership as RO

    monkeypatch.setenv("COSMICFORGE_TEST_MODE", "1")
    db = DB(url=pg_url)
    first = RO.RuntimeOwnership(db, database_path=db.path, runtime_session_id="s1")
    assert first.database_path == db.path  # a URL is a lease key as it is (never abspath'd)
    assert first.acquire().acquired
    second = RO.RuntimeOwnership(db, database_path=db.path, runtime_session_id="s2")
    second.hostname = "another-host"  # a process on another host: its PID is not in this host's table
    result = second.acquire()
    assert not result.acquired and result.reason == "RUNTIME_OWNERSHIP_HELD_BY_ANOTHER_PROCESS"
    owner = RO.current_owner(db, db.path)
    assert owner["hostname"] == socket.gethostname() and owner["runtime_owner_id"] == first.runtime_owner_id
    with db.connect() as c:  # the heartbeat is the database clock
        server = c.execute("SELECT datetime('now')").fetchone()[0]
    assert owner["heartbeat_at"][:16].replace("T", " ") == server[:16] or abs(
        RO.datetime.fromisoformat(owner["heartbeat_at"]).timestamp()
        - RO.datetime.fromisoformat(server).replace(tzinfo=RO.timezone.utc).timestamp()) < 5
    with db.connect() as c:  # make the holder's heartbeat stale: the other host may now take over
        c.execute("UPDATE runtime_ownership SET heartbeat_at = ?", ("2000-01-01T00:00:00+00:00",))
    assert second.acquire().acquired
    assert not first.renew()


def test_create_schema_builds_the_same_legacy_schema_as_sqlite(pg_url, tmp_path, monkeypatch):
    from shared_lib.persistence.db import DB
    from shared_lib.persistence.migrations import migrate
    from app.execution import production_schema
    from app.ops.runtime_ownership import ensure_ownership_schema

    monkeypatch.setenv("COSMICFORGE_TEST_MODE", "1")
    LP.create_schema(pg_url)
    LP.create_schema(pg_url)  # idempotent
    db = DB(str(tmp_path / "parity.db"))
    migrate(db)
    production_schema.ensure(db, force=True)
    ensure_ownership_schema(db)
    LP._lazy_runtime_schema(db)

    def inventory(c):
        tables = {t: [(r[1], r[5]) for r in c.execute(f"PRAGMA table_info({t})")]
                  for (t,) in c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE "
                                        "'sqlite_%'").fetchall()}
        triggers = {r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='trigger'")}
        indexes = {r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='index' AND sql IS NOT NULL")}
        return tables, triggers, indexes

    with sqlite3.connect(db.path) as s:
        expected = inventory(s)
    pg = LP.connect(pg_url)
    try:
        actual = inventory(pg)
    finally:
        pg.close()
    assert actual[0] == expected[0]  # every table, every column in order, every primary key position
    assert actual[1] == expected[1] and len(actual[1]) > 100  # every guard trigger
    assert actual[2] == expected[2]  # every explicit index
    assert not [e for e in D.unsupported_events() if "create_schema" in e]


def test_sum_over_a_comparison_counts_like_sqlite(conn):
    expected, actual = both(conn, ["CREATE TABLE l (purpose TEXT, released_at TEXT)",
                                   "INSERT INTO l VALUES ('EMERGENCY', NULL)", "INSERT INTO l VALUES ('CYCLE', 'x')",
                                   "INSERT INTO l VALUES (NULL, NULL)"],
                            "SELECT COUNT(*), SUM(purpose='EMERGENCY'), SUM(released_at IS NOT NULL) FROM l")
    assert actual == expected == [(3, 1, 1)]
