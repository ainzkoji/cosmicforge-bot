"""DB.scope(): one connection per scope, with every inner block keeping its own
commit / rollback semantics; and the measurement counters behind Step 1.0c."""
import os
import sqlite3
import sys
import threading

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "shared"))
from shared_lib.persistence import db as shared_db  # noqa: E402
from shared_lib.persistence.db import DB


@pytest.fixture
def db(tmp_path):
    return DB(str(tmp_path / "scope.db"))


def rows(db):
    with db.connect() as c:
        return [r[0] for r in c.execute("SELECT v FROM t ORDER BY v")]


def test_blocks_inside_a_scope_share_one_connection_and_still_commit_each(db):
    shared_db.instrument(True)
    try:
        with db.scope():
            with db.connect() as c:
                c.execute("CREATE TABLE t(v)")
            with db.connect() as c:
                c.execute("INSERT INTO t VALUES(1)")
            # Committed by the inner block: visible to a connection outside the scope.
            other = sqlite3.connect(db.path)
            assert other.execute("SELECT COUNT(*) FROM t").fetchone()[0] == 1
            other.close()
            with db.connect() as c:
                c.execute("INSERT INTO t VALUES(2)")
        snap = shared_db.METRICS.snapshot()
        assert snap["connections_opened"] == 1 and snap["connections_reused"] == 3
        assert snap["pragmas"] == 3 and snap["schema_statements"] == 1
    finally:
        shared_db.instrument(False)
    assert rows(db) == [1, 2]


def test_a_raising_block_rolls_back_only_its_own_writes(db):
    with db.scope():
        with db.connect() as c:
            c.execute("CREATE TABLE t(v)")
            c.execute("INSERT INTO t VALUES(1)")
        with pytest.raises(RuntimeError):
            with db.connect() as c:
                c.execute("INSERT INTO t VALUES(2)")
                raise RuntimeError("boom")
        with db.connect() as c:
            c.execute("INSERT INTO t VALUES(3)")
    assert rows(db) == [1, 3]


def test_begin_immediate_blocks_keep_working_inside_a_scope(db):
    with db.connect() as c:
        c.execute("CREATE TABLE t(v)")
    with db.scope():
        with db.connect() as c:
            c.execute("BEGIN IMMEDIATE")
            c.execute("INSERT INTO t VALUES(1)")
        with db.connect() as c:
            c.execute("INSERT INTO t VALUES(2)")
    assert rows(db) == [1, 2]


def test_nested_scopes_reuse_the_outer_connection_and_leave_nothing_open(db):
    shared_db.instrument(True)
    try:
        with db.scope():
            with db.scope():
                with db.connect() as c:
                    c.execute("CREATE TABLE t(v)")
            with db.connect() as c:
                c.execute("INSERT INTO t VALUES(1)")
        assert shared_db.METRICS.connections_opened == 1
        assert getattr(db._local(), "conn", None) is None
        # After the scope, blocks open their own connections again.
        with db.connect() as c:
            c.execute("INSERT INTO t VALUES(2)")
        assert shared_db.METRICS.connections_opened == 2
    finally:
        shared_db.instrument(False)
    assert rows(db) == [1, 2]


def test_a_scope_is_thread_local(db):
    with db.connect() as c:
        c.execute("CREATE TABLE t(v)")
    seen = {}

    def worker():
        # No scope on this thread: a private connection, never the main thread's.
        seen["conn_before"] = getattr(db._local(), "conn", None)
        with db.connect() as c:
            c.execute("INSERT INTO t VALUES(2)")
            seen["thread_conn"] = c
    with db.scope():
        with db.connect() as main_conn:
            main_conn.execute("INSERT INTO t VALUES(1)")
        t = threading.Thread(target=worker)
        t.start()
        t.join()
        assert seen["conn_before"] is None and seen["thread_conn"] is not main_conn
    assert rows(db) == [1, 2]


def test_a_scope_closes_its_connection_even_when_the_body_raises(db):
    with pytest.raises(ValueError):
        with db.scope():
            with db.connect() as c:
                c.execute("CREATE TABLE t(v)")
            raise ValueError("cycle failed")
    assert getattr(db._local(), "conn", None) is None
    assert rows(db) == []                                       # the committed DDL survived; nothing is stuck
    with db.connect() as c:
        c.execute("INSERT INTO t VALUES(1)")                    # no lock left behind
    assert rows(db) == [1]
