"""Dialect-neutral database access for the Step 5 platform layer (Section H, Step 5.1).

The legacy persistence (``shared_lib.persistence.db.DB`` and the ``cati_*_schema`` modules) is raw SQLite. This
module is the first layer that runs unchanged on SQLite and on PostgreSQL, so the multi-worker infrastructure
(account assignments, the durable job queue, platform order budgets, go-live and launch records) can move to a
shared PostgreSQL server without a second implementation.

What it guarantees on both dialects:

* one placeholder style for callers (``?``), rewritten to ``%s`` for psycopg;
* ``transaction()`` -- one explicit transaction, committed on success, rolled back on any exception, the
  connection returned to the pool afterwards; ``write=True`` takes the write lock at the start (SQLite
  ``BEGIN IMMEDIATE``; on PostgreSQL row locks are taken by the statements themselves);
* foreign keys enforced (SQLite ``PRAGMA foreign_keys=ON`` on every connection; always on in PostgreSQL);
* ``now_ms(conn)`` -- the **database server's** clock in epoch milliseconds, so lease decisions made by
  workers on different hosts compare against one clock, not their own;
* a bounded connection pool with checkout health checks (``Pool``);
* ``health()`` -- a readiness probe that reports the dialect, the schema version and pool state.

Money is never stored as a binary float by this layer: schemas use ``NUMERIC(38,18)`` on PostgreSQL and TEXT
decimals on SQLite (``decimal_out``/``decimal_in``); arithmetic happens in ``decimal.Decimal``.

Nothing here calls an exchange or can place an order.
"""
from __future__ import annotations

import contextlib
import os
import queue
import re
import sqlite3
import threading
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Dict, Iterator, Optional, Sequence
from urllib.parse import urlsplit

SQLITE, POSTGRESQL = "sqlite", "postgresql"
DEFAULT_POOL_SIZE = 10
DEFAULT_CHECKOUT_TIMEOUT_S = 10.0
SQLITE_BUSY_TIMEOUT_MS = 10_000


class DatabaseUnavailable(RuntimeError):
    code = "DATABASE_UNAVAILABLE"


class PoolExhausted(DatabaseUnavailable):
    code = "DATABASE_POOL_EXHAUSTED"


def dialect_of(url: str) -> str:
    scheme = urlsplit(url).scheme.split("+", 1)[0].lower()
    if scheme == "sqlite":
        return SQLITE
    if scheme in ("postgres", "postgresql"):
        return POSTGRESQL
    raise ValueError(f"DATABASE_URL_UNSUPPORTED:{scheme or 'none'}")


def sqlite_path(url: str) -> str:
    if not url.startswith("sqlite:///"):
        raise ValueError("SQLITE_URL_INVALID")
    return url[len("sqlite:///"):] or ":memory:"


def decimal_in(value: Any) -> Optional[str]:
    """Bind a monetary value: exact decimal text, never a float. Rejects ``float`` inputs outright -- a float
    that reached a money column has already lost the exact value, and accepting it would hide that."""
    if value is None:
        return None
    if isinstance(value, float):
        raise TypeError("MONEY_FLOAT_REJECTED")
    d = Decimal(str(value))
    if not d.is_finite():
        raise ValueError("MONEY_NOT_FINITE")
    return format(d, "f")


def decimal_out(value: Any) -> Optional[Decimal]:
    if value is None:
        return None
    if isinstance(value, float):
        raise TypeError("MONEY_FLOAT_STORED")
    return Decimal(str(value))


_QMARK = re.compile(r"\?")


class Connection:
    """A thin, dialect-neutral wrapper. ``execute`` takes ``?`` placeholders on both dialects."""

    def __init__(self, raw: Any, dialect: str) -> None:
        self.raw = raw
        self.dialect = dialect

    def _sql(self, sql: str) -> str:
        if self.dialect == POSTGRESQL:
            if "%" in sql:
                sql = sql.replace("%", "%%")
            return _QMARK.sub("%s", sql)
        return sql

    def execute(self, sql: str, params: Sequence[Any] = ()) -> Any:
        cur = self.raw.cursor()
        cur.execute(self._sql(sql), tuple(params))
        return cur

    def executemany(self, sql: str, rows: Sequence[Sequence[Any]]) -> None:
        cur = self.raw.cursor()
        cur.executemany(self._sql(sql), [tuple(r) for r in rows])

    def one(self, sql: str, params: Sequence[Any] = ()) -> Optional[tuple]:
        row = self.execute(sql, params).fetchone()
        return tuple(row) if row is not None else None

    def all(self, sql: str, params: Sequence[Any] = ()) -> list:
        return [tuple(r) for r in self.execute(sql, params).fetchall()]

    def scalar(self, sql: str, params: Sequence[Any] = ()) -> Any:
        row = self.one(sql, params)
        return row[0] if row is not None else None

    def now_ms(self) -> int:
        """The database server's clock. On SQLite the server is this host (one file, one clock)."""
        if self.dialect == POSTGRESQL:
            return int(self.scalar("SELECT CAST(EXTRACT(EPOCH FROM clock_timestamp()) * 1000 AS BIGINT)"))
        return int(self.scalar("SELECT CAST(ROUND((julianday('now') - 2440587.5) * 86400000.0) AS INTEGER)"))

    def lock_rows_suffix(self) -> str:
        """``FOR UPDATE`` on PostgreSQL; SQLite already holds the database write lock (``BEGIN IMMEDIATE``)."""
        return " FOR UPDATE" if self.dialect == POSTGRESQL else ""

    def skip_locked_suffix(self) -> str:
        return " FOR UPDATE SKIP LOCKED" if self.dialect == POSTGRESQL else ""


class Pool:
    """A bounded pool. A connection is health-checked at checkout, rolled back at return, and discarded if
    either fails; the pool never hands out more than ``size`` connections at once."""

    def __init__(self, factory, *, size: int = DEFAULT_POOL_SIZE, checkout_timeout_s: float = DEFAULT_CHECKOUT_TIMEOUT_S,
                 ping=None) -> None:
        if size < 1:
            raise ValueError("POOL_SIZE_INVALID")
        self._factory = factory
        self._ping = ping
        self._idle: "queue.LifoQueue[Any]" = queue.LifoQueue()
        self._slots = threading.BoundedSemaphore(size)
        self._timeout = checkout_timeout_s
        self.size = size
        self._lock = threading.Lock()
        self.stats = {"created": 0, "discarded": 0, "checkouts": 0, "in_use": 0, "peak_in_use": 0}

    def get(self) -> Any:
        if not self._slots.acquire(timeout=self._timeout):
            raise PoolExhausted(f"no connection within {self._timeout}s (size {self.size})")
        try:
            while True:
                try:
                    raw = self._idle.get_nowait()
                except queue.Empty:
                    raw = self._factory()
                    with self._lock:
                        self.stats["created"] += 1
                    break
                try:
                    if self._ping is not None:
                        self._ping(raw)
                    break
                except Exception:
                    self._discard(raw)
            with self._lock:
                self.stats["checkouts"] += 1
                self.stats["in_use"] += 1
                self.stats["peak_in_use"] = max(self.stats["peak_in_use"], self.stats["in_use"])
            return raw
        except Exception as exc:
            self._slots.release()
            if isinstance(exc, DatabaseUnavailable):
                raise
            raise DatabaseUnavailable(f"{type(exc).__name__}: {exc}") from exc

    def put(self, raw: Any, *, broken: bool = False) -> None:
        try:
            if broken:
                self._discard(raw)
            else:
                try:
                    raw.rollback()
                    self._idle.put(raw)
                except Exception:
                    self._discard(raw)
        finally:
            with self._lock:
                self.stats["in_use"] -= 1
            self._slots.release()

    def _discard(self, raw: Any) -> None:
        with self._lock:
            self.stats["discarded"] += 1
        with contextlib.suppress(Exception):
            raw.close()

    def close(self) -> None:
        while True:
            try:
                self._discard(self._idle.get_nowait())
            except queue.Empty:
                return


@dataclass
class _Counters:
    statements: int = 0
    transactions: int = 0
    rollbacks: int = 0


class Database:
    """One database, one pool. ``Database.from_env()`` reads ``PLATFORM_DATABASE_URL`` (falling back to
    ``DATABASE_URL``)."""

    def __init__(self, url: str, *, pool_size: int = DEFAULT_POOL_SIZE,
                 checkout_timeout_s: float = DEFAULT_CHECKOUT_TIMEOUT_S, connect_timeout_s: int = 10) -> None:
        self.url = url
        self.dialect = dialect_of(url)
        self.counters = _Counters()
        if self.dialect == SQLITE:
            path = sqlite_path(url)
            if path == ":memory:":
                raise ValueError("SQLITE_MEMORY_UNSUPPORTED: a pool of separate in-memory databases is not one database")
            self._path = path

            def factory():
                raw = sqlite3.connect(path, timeout=SQLITE_BUSY_TIMEOUT_MS / 1000, check_same_thread=False,
                                      isolation_level=None)
                raw.execute("PRAGMA foreign_keys=ON")
                raw.execute(f"PRAGMA busy_timeout={SQLITE_BUSY_TIMEOUT_MS}")
                raw.execute("PRAGMA journal_mode=WAL")
                return raw
            ping = lambda raw: raw.execute("SELECT 1").fetchone()  # noqa: E731
        else:
            try:
                import psycopg  # noqa: F401  (imported lazily: SQLite deployments do not need it)
            except ImportError as exc:  # pragma: no cover - depends on the environment
                raise DatabaseUnavailable("POSTGRES_DRIVER_MISSING: install psycopg") from exc
            dsn = url.replace("postgresql+psycopg://", "postgresql://", 1)

            def factory():
                import psycopg
                return psycopg.connect(dsn, connect_timeout=connect_timeout_s, autocommit=False)

            def ping(raw):
                raw.execute("SELECT 1").fetchone()
                raw.rollback()
        self.pool = Pool(factory, size=pool_size, checkout_timeout_s=checkout_timeout_s, ping=ping)

    @classmethod
    def from_env(cls, env: Optional[Dict[str, str]] = None, **kw) -> "Database":
        env = os.environ if env is None else env
        url = env.get("PLATFORM_DATABASE_URL") or env.get("DATABASE_URL")
        if not url:
            raise DatabaseUnavailable("PLATFORM_DATABASE_URL_MISSING")
        size = int(env.get("PLATFORM_DB_POOL_SIZE", kw.pop("pool_size", DEFAULT_POOL_SIZE)))
        return cls(url, pool_size=size, **kw)

    @contextlib.contextmanager
    def transaction(self, *, write: bool = True) -> Iterator[Connection]:
        raw = self.pool.get()
        broken = False
        conn = _Counting(raw, self.dialect, self.counters)
        try:
            if self.dialect == SQLITE:
                raw.execute("BEGIN IMMEDIATE" if write else "BEGIN")
            self.counters.transactions += 1
            yield conn
            if self.dialect == SQLITE:
                raw.execute("COMMIT")
            else:
                raw.commit()
        except BaseException:
            self.counters.rollbacks += 1
            try:
                if self.dialect == SQLITE:
                    if raw.in_transaction:
                        raw.execute("ROLLBACK")
                else:
                    raw.rollback()
            except Exception:
                broken = True
            raise
        finally:
            self.pool.put(raw, broken=broken)

    def health(self) -> Dict[str, Any]:
        """Readiness: can a connection be checked out, does it answer, and which schema version is applied."""
        started = time.perf_counter()
        try:
            with self.transaction(write=False) as conn:
                conn.scalar("SELECT 1")
                version = None
                try:
                    version = conn.scalar("SELECT MAX(version) FROM platform_schema_migrations")
                except Exception:
                    pass
            return {"ready": True, "dialect": self.dialect, "schema_version": version,
                    "latency_ms": round((time.perf_counter() - started) * 1000, 3), "pool": dict(self.pool.stats),
                    "pool_size": self.pool.size}
        except Exception as exc:
            return {"ready": False, "dialect": self.dialect, "error": f"{type(exc).__name__}: {exc}"[:300],
                    "pool": dict(self.pool.stats), "pool_size": self.pool.size}

    def close(self) -> None:
        self.pool.close()


class _Counting(Connection):
    def __init__(self, raw, dialect, counters) -> None:
        super().__init__(raw, dialect)
        self._counters = counters

    def execute(self, sql, params=()):
        self._counters.statements += 1
        return super().execute(sql, params)

    def executemany(self, sql, rows):
        self._counters.statements += 1
        return super().executemany(sql, rows)
