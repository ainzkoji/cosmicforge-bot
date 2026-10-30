"""Opt-in pytest plugin: run EXISTING legacy-runtime test modules on PostgreSQL (EXT-DB-01).

    LEGACY_TEST_POSTGRES_URL=postgresql://user@host:port/postgres \\
        ../venv/Scripts/python.exe -m pytest -p tests.legacy_pg_plugin -q -p no:cacheprovider <modules>

Every ``DB(<path>)`` a test creates -- including the per-session ``DB()`` the bootstrap points ``DATABASE_URL`` at,
and ``DB(":memory:")`` -- opens a fresh throwaway PostgreSQL database instead (one per distinct path; one per
``:memory:`` object), created with the C locale from a template that already holds the adapter's bootstrap objects
and ``DB._init``'s tables. The test modules run unchanged; ``DB.dialect == "postgresql"`` and ``DB.path`` stays the
path the test asked for (nothing is written there).

Two test HELPERS that copy or probe a database through the ``sqlite3`` module are given PostgreSQL equivalents, so
the execution-tail tests built on them keep proving what they prove on SQLite:

* ``_step3_execution.copy_database(source, target)`` (``sqlite3`` online backup of a template) -> ``CREATE DATABASE
  target TEMPLATE source``;
* ``_step3_execution.Controlled._probe`` ("no transaction spans a venue call": a second connection must get the
  write lock right now) -> ``pg_try_advisory_xact_lock(LEGACY_WRITE_LOCK_KEY)`` from a second connection;
* ``sqlite3.connect(template).backup(sqlite3.connect(target))`` (the brakes/reservations fixtures' template copy)
  -> ``CREATE DATABASE target TEMPLATE source`` when ``template`` is a routed path (``_PathConnection``).

Anything else a test does with ``sqlite3`` on the file is NOT redirected: such failures are test-side and are listed
separately in the both-dialect report.

Loudness: a test fails if, while it ran, the translator raised ``DialectUnsupported`` or PostgreSQL reported a type
mismatch -- even when the code under test swallowed the exception.

The databases are dropped when the test (function-scoped ``tmp_path``) or the session ends. Throwaway databases
run with ``synchronous_commit = off`` (crash durability of test data only; no semantic difference).
"""
from __future__ import annotations

import os
import sys
import threading
import uuid
from pathlib import Path
from typing import Dict, List, Optional

import pytest

ENV = "LEGACY_TEST_POSTGRES_URL"
PREFIX = "cftest_"

_STATE: Dict[str, object] = {}
_ROUTES: Dict[str, str] = {}          # normalised path -> database name
_CREATED: List[str] = []
_LOCK = threading.RLock()


def _admin():
    import psycopg

    conn = _STATE.get("admin")
    if conn is None or conn.closed:
        conn = psycopg.connect(_STATE["admin_url"], autocommit=True)
        _STATE["admin"] = conn
    return conn


def _url(name: str) -> str:
    from urllib.parse import urlsplit, urlunsplit

    parts = urlsplit(_STATE["admin_url"])
    return urlunsplit((parts.scheme, parts.netloc, "/" + name, "", ""))


def _key(path) -> str:
    return os.path.normcase(os.path.abspath(str(path)))


def _new_database(template: str) -> str:
    with _LOCK:
        name = f"{PREFIX}{_STATE['token']}_{len(_CREATED)}"
        _admin().execute(f'CREATE DATABASE "{name}" TEMPLATE "{template}"')
        _admin().execute(f'ALTER DATABASE "{name}" SET synchronous_commit = off')
        _CREATED.append(name)
        return name


def _router(path, db) -> Optional[str]:
    from shared_lib.persistence.db import DB

    with _LOCK:
        if str(path) == ":memory:":
            name = _new_database(_STATE["template"])
        else:
            key = _key(path)
            name = _ROUTES.get(key)
            if name is None:
                name = _new_database(_STATE["template"])
                _ROUTES[key] = name
        url = _url(name)
        DB._pg_initialized.add(url)  # the template already holds DB._init's schema
        return url


def _drop(names: List[str]) -> None:
    from shared_lib.persistence import legacy_postgres

    for name in names:
        legacy_postgres.close_pools(_url(name))
        try:
            _admin().execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
        except Exception as exc:  # pragma: no cover - reported, never hidden
            print(f"[legacy_pg_plugin] could not drop {name}: {exc}", file=sys.stderr)


# ----------------------------------------------------------------------------------------------- helper ports
def _pg_copy_database(source: str, target: str):
    from shared_lib.persistence import legacy_postgres
    from shared_lib.persistence.db import DB

    with _LOCK:
        src = _ROUTES.get(_key(source))
        if src is None:
            raise RuntimeError(f"legacy_pg_plugin: {source} was never opened as a database")
        legacy_postgres.close_pools(_url(src))
        name = _new_database(src)
        _ROUTES[_key(target)] = name
    return DB(target)


def _pg_probe(self, method: str) -> None:
    from shared_lib.persistence import legacy_postgres
    from shared_lib.persistence.sql.legacy_dialect import LEGACY_WRITE_LOCK_KEY

    if self._db_path is None:
        return
    name = _ROUTES.get(_key(self._db_path))
    if name is None:
        return
    probe = legacy_postgres.connect(_url(name))
    try:
        raw = probe._raw
        raw.execute("BEGIN")
        got = raw.execute(f"SELECT pg_try_advisory_xact_lock({LEGACY_WRITE_LOCK_KEY})").fetchone()[0]
        raw.execute("ROLLBACK")
        if not got:
            self.open_transactions.append(method)
    finally:
        probe.close()


_REAL_CONNECT = None


class _PathConnection:
    """What ``sqlite3.connect("<...>.db")`` returns while the plugin routes databases: a lazy stand-in for the
    real SQLite connection (every attribute is the real connection's, opened on first use) whose ``backup()``
    clones the PostgreSQL database when the source path is routed -- the fixtures' "copy the migrated template"
    step. Any other use of a routed path through ``sqlite3`` still opens the (empty) SQLite file: such tests are
    reported as test-side failures, never made to pass."""

    def __init__(self, database, args, kwargs) -> None:
        self._path = str(database)
        self._args, self._kwargs = args, kwargs
        self._real = None

    def _conn(self):
        if self._real is None:
            self._real = _REAL_CONNECT(self._path, *self._args, **self._kwargs)
        return self._real

    def backup(self, target, *args, **kwargs):
        from shared_lib.persistence import legacy_postgres

        with _LOCK:
            src = _ROUTES.get(_key(self._path))
            if src is not None:
                if not isinstance(target, _PathConnection):
                    raise RuntimeError("legacy_pg_plugin: backup of a PostgreSQL-routed database into a non-file target")
                legacy_postgres.close_pools(_url(src))
                _ROUTES[_key(target._path)] = _new_database(src)
                return None
        real_target = target._conn() if isinstance(target, _PathConnection) else target
        return self._conn().backup(real_target, *args, **kwargs)

    def close(self) -> None:
        if self._real is not None:
            self._real.close()

    def __enter__(self):
        return self._conn().__enter__()

    def __exit__(self, *exc):
        return self._conn().__exit__(*exc)

    def __getattr__(self, name):
        return getattr(self._conn(), name)

    def __setattr__(self, name, value):
        if name.startswith("_"):
            object.__setattr__(self, name, value)
        else:
            setattr(self._conn(), name, value)  # row_factory, isolation_level, ... belong to the real connection


def _routing_connect(database, *args, **kwargs):
    text = str(database) if isinstance(database, (str, os.PathLike)) else ""
    if text.endswith(".db") and not text.startswith("file:"):
        return _PathConnection(database, args, kwargs)
    return _REAL_CONNECT(database, *args, **kwargs)


def _patch_helpers() -> None:
    module = sys.modules.get("_step3_execution")
    if module is None or getattr(module, "_legacy_pg_patched", False):
        return
    module.copy_database = _pg_copy_database
    module.Controlled._probe = _pg_probe
    module._legacy_pg_patched = True


# ----------------------------------------------------------------------------------------------- hooks
def pytest_configure(config):
    admin_url = os.environ.get(ENV)
    if not admin_url:
        raise pytest.UsageError(f"legacy_pg_plugin needs {ENV}=postgresql://user@host:port/<maintenance db>")
    import psycopg

    from shared_lib.persistence import legacy_postgres
    from shared_lib.persistence.db import DB

    _STATE["admin_url"] = admin_url
    _STATE["token"] = uuid.uuid4().hex[:8]
    base = f"{PREFIX}{_STATE['token']}_tpl"
    admin = _admin()
    admin.execute(f"CREATE DATABASE \"{base}\" TEMPLATE template0 ENCODING 'UTF8' LC_COLLATE 'C' LC_CTYPE 'C'")
    _CREATED.append(base)
    url = _url(base)
    with psycopg.connect(url, autocommit=True) as raw:
        legacy_postgres._bootstrap(raw)
    # DB._init on the template, so every routed database starts where DB(path) leaves a new SQLite file.
    DB._test_router = None
    template_db = DB(url=url)
    del template_db
    legacy_postgres.close_pools(url)
    _STATE["template"] = base
    DB._test_router = staticmethod(_router)
    global _REAL_CONNECT
    import sqlite3

    _REAL_CONNECT = sqlite3.connect
    sqlite3.connect = _routing_connect
    reporter = config.pluginmanager.get_plugin("terminalreporter")
    if reporter is not None:
        reporter.write_line(f"[legacy_pg_plugin] DB(<path>) -> PostgreSQL ({legacy_postgres.redact(admin_url)}), "
                            f"template {base}")


def pytest_unconfigure(config):
    if "admin_url" not in _STATE:
        return
    from shared_lib.persistence import legacy_postgres
    from shared_lib.persistence.db import DB

    DB._test_router = None
    if _REAL_CONNECT is not None:
        import sqlite3

        sqlite3.connect = _REAL_CONNECT
    legacy_postgres.close_pools()
    _drop(list(reversed(_CREATED)))
    admin = _STATE.get("admin")
    if admin is not None:
        admin.close()


@pytest.fixture(autouse=True)
def _legacy_pg_test_guard(request):
    from shared_lib.persistence.sql import legacy_dialect

    _patch_helpers()
    before = len(legacy_dialect.unsupported_events())
    with _LOCK:
        created_before = len(_CREATED)
    yield
    from shared_lib.persistence import legacy_postgres

    # this test's own databases (under its tmp_path) are dropped now; others live until the session ends
    tmp = request.node.funcargs.get("tmp_path") if hasattr(request.node, "funcargs") else None
    with _LOCK:
        mine = []
        if tmp is not None:
            prefix = _key(tmp)
            for key, name in list(_ROUTES.items()):
                if key.startswith(prefix) and name in _CREATED[created_before:]:
                    mine.append(name)
                    del _ROUTES[key]
            for name in mine:
                _CREATED.remove(name)
    legacy_postgres.close_pools()
    if mine:
        _drop(mine)
    events = legacy_dialect.unsupported_events()[before:]
    if events:
        pytest.fail("legacy PostgreSQL dialect gaps during this test (possibly swallowed by the code under test):\n"
                    + "\n".join(f"  - {e}" for e in events[:20]), pytrace=False)
