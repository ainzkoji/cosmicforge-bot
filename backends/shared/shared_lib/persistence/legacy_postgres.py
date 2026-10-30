"""The legacy runtime on PostgreSQL: a connection adapter with the ``sqlite3.Connection`` surface (EXT-DB-01).

``shared_lib.persistence.db.DB`` hands its callers ``sqlite3.Connection`` objects and about 270 modules use them
directly. :class:`LegacyConnection` presents the same surface over psycopg 3 so that code runs unchanged on
PostgreSQL; every statement goes through the closed translator in
:mod:`shared_lib.persistence.sql.legacy_dialect`.

Semantics mirrored from Python's ``sqlite3`` (legacy transaction control, ``isolation_level=""``):

* an implicit ``BEGIN`` before INSERT/UPDATE/DELETE/REPLACE (judged by the statement's first word, exactly as
  CPython does); SELECT and DDL outside a transaction run on their own;
* ``commit()``/``rollback()``; ``with conn:`` commits on success and rolls back on an exception (and does not close);
  ``in_transaction``, ``isolation_level`` (``None`` = autocommit), ``total_changes``;
* statement-level atomicity: inside a transaction every statement runs under a savepoint, so a failing statement
  is undone alone and the transaction stays usable -- as in SQLite, where PostgreSQL would abort the transaction;
* ONE WRITER AT A TIME, like SQLite's database write lock: a transaction takes
  ``pg_advisory_xact_lock(LEGACY_WRITE_LOCK_KEY)`` at ``BEGIN IMMEDIATE/EXCLUSIVE`` or at its first write, and a
  write outside a transaction runs in its own transaction holding the lock. ``lock_timeout`` is SQLite's 10 s busy
  timeout; a lock that is not granted in time, a serialization failure or a deadlock is raised as
  ``sqlite3.OperationalError("database is locked")``. An explicit ``BEGIN``/``BEGIN DEFERRED`` runs at REPEATABLE
  READ, so a read-then-write that lost a race fails as SQLite's snapshot does instead of silently overwriting;
* rows: ``row_factory = sqlite3.Row`` gives rows indexable by position and (case-insensitively) by name, with
  ``keys()`` and ``dict(row)``; ``None`` gives tuples; any other callable is called as ``factory(cursor, row)``;
* ``cursor.lastrowid`` (the inserted row's rowid, read with ``RETURNING cf_rowid``), ``rowcount`` (-1 for queries),
  ``description``, ``fetchone/fetchmany/fetchall``, iteration;
* errors are raised as the ``sqlite3`` exception classes with SQLite's messages where code inspects them
  (``no such table: x``, ``no such column: x``, ``duplicate column name: x``, ``UNIQUE constraint failed: t.c``,
  ``NOT NULL constraint failed: t.c``, trigger ``RAISE(ABORT, msg)`` -> ``IntegrityError(msg)``,
  ``database is locked``).

Values. Parameters accept exactly what ``sqlite3`` accepts (None, int, float, str, bytes; other types only through
``sqlite3.register_adapter``) and are refused otherwise, with sqlite3's error. A Python ``float`` is bound as the
NUMERIC literal of its shortest round-trip decimal (``repr``): a ``NUMERIC(38,18)`` money column then stores exactly
the decimal the float stands for (never PostgreSQL's 15-digit ``float8 -> numeric`` cast), and a ``DOUBLE
PRECISION`` column gets the identical binary64 back. ``bool`` binds as 0/1. NUMERIC results come back to legacy
callers as ``float`` (as SQLite REAL did; legacy arithmetic is float) -- or ``int`` when the value has no fractional
digits (``SUM`` over INTEGER columns); the platform layer (``sql.Database``) keeps Decimal. PostgreSQL booleans come
back as 0/1, json as text. Type-affinity mismatches SQLite would coerce silently (comparing TEXT with an integer
parameter, ``x / 0``) are raised, and recorded with :func:`legacy_dialect.unsupported_events` so a caller's
``except Exception`` cannot hide them from the test plugin.

Connections come from a bounded per-URL pool; a connection is health-checked (state + ``SELECT 1``) at checkout,
rolled back and reset at return, and discarded when it is broken or was left in a failed transaction.

Public entry points used by other workstreams: :func:`connect` and :func:`create_schema`.
"""
from __future__ import annotations

import atexit
import datetime as _dt
import logging
import math
import os
import queue
import re
import sqlite3
import threading
import time
from collections.abc import Mapping
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import urlsplit, urlunsplit

from shared_lib.persistence.sql import legacy_dialect as D

logger = logging.getLogger(__name__)

BUSY_TIMEOUT_MS = 10_000
DEFAULT_POOL_SIZE = int(os.environ.get("LEGACY_PG_POOL_SIZE", "16"))
CHECKOUT_TIMEOUT_S = 30.0
_SP = "cf_sp"
_LOCK_SQL = f"SELECT pg_advisory_xact_lock({D.LEGACY_WRITE_LOCK_KEY})"


class LegacyPostgresConfigError(RuntimeError):
    """The PostgreSQL database cannot host the legacy schema faithfully (for example its collation)."""

    code = "LEGACY_POSTGRES_CONFIG"


def is_postgres_url(value: Any) -> bool:
    text = str(value or "")
    return text.startswith(("postgresql://", "postgres://", "postgresql+psycopg://"))


def redact(url: str) -> str:
    """The URL without its password (safe to log and to use as a stable database identity)."""
    parts = urlsplit(url)
    netloc = parts.netloc
    if "@" in netloc:
        cred, host = netloc.rsplit("@", 1)
        user = cred.split(":", 1)[0]
        netloc = f"{user}@{host}"
    scheme = "postgresql"
    return urlunsplit((scheme, netloc, parts.path, "", ""))


def _conninfo(url: str) -> str:
    if url.startswith("postgresql+psycopg://"):
        return "postgresql://" + url[len("postgresql+psycopg://"):]
    return url


# ═══════════════════════════════════════════════════════════════════════════════════════════════ adaptation
_psycopg = None


def _pg():
    global _psycopg
    if _psycopg is None:
        import psycopg  # noqa: WPS433 - optional dependency, imported only when PostgreSQL is used
        _psycopg = psycopg
    return _psycopg


_ADAPTERS = None


def _adapters():
    """psycopg dumpers/loaders giving sqlite3's value semantics (built once)."""
    global _ADAPTERS
    if _ADAPTERS is not None:
        return _ADAPTERS
    psycopg = _pg()
    from psycopg.adapt import Dumper, Loader

    numeric_oid = psycopg.postgres.types["numeric"].oid
    int8_oid = psycopg.postgres.types["int8"].oid

    class FloatAsNumeric(Dumper):
        oid = numeric_oid

        def dump(self, obj):
            if math.isnan(obj):
                return b"NaN"
            if math.isinf(obj):
                return b"Infinity" if obj > 0 else b"-Infinity"
            return repr(obj).encode()

    class IntAsInt8(Dumper):
        oid = int8_oid

        def dump(self, obj):
            return str(int(obj)).encode()

    class NumericLoader(Loader):
        def load(self, data):
            text = bytes(data).decode()
            if "." in text or "e" in text or "E" in text:
                return float(text)
            if text in ("NaN", "Infinity", "-Infinity"):
                return float(text.replace("Infinity", "inf"))
            return int(text)

    class BoolLoader(Loader):
        def load(self, data):
            return 1 if bytes(data) in (b"t", b"true") else 0

    class TextLoader(Loader):
        def load(self, data):
            return bytes(data).decode()

    _ADAPTERS = {"dumpers": [(float, FloatAsNumeric), (int, IntAsInt8), (bool, IntAsInt8)],
                 "loaders": [("numeric", NumericLoader), ("bool", BoolLoader), ("json", TextLoader),
                             ("jsonb", TextLoader)]}
    return _ADAPTERS


def _register(raw: Any) -> None:
    spec = _adapters()
    for typ, dumper in spec["dumpers"]:
        raw.adapters.register_dumper(typ, dumper)
    for name, loader in spec["loaders"]:
        raw.adapters.register_loader(name, loader)


_NATIVE = (type(None), int, float, str, bytes, bytearray, memoryview)


def _adapt_value(value: Any, index: Any) -> Any:
    if isinstance(value, _NATIVE):
        if isinstance(value, (bytearray, memoryview)):
            return bytes(value)
        return value
    adapter = sqlite3.adapters.get((type(value), sqlite3.PrepareProtocol))
    if adapter is not None:
        adapted = adapter(value)
        if isinstance(adapted, _NATIVE):
            return _adapt_value(adapted, index)
    raise sqlite3.ProgrammingError(f"Error binding parameter {index}: type '{type(value).__name__}' is not supported")


def _sqlite_text(value: Any) -> str:
    """The text SQLite's TEXT affinity stores for a number (its own rendering, e.g. REAL with 15 digits)."""
    return _SQLITE_MEMORY.execute("SELECT CAST(? AS TEXT)", (value,)).fetchone()[0]


_SQLITE_MEMORY = sqlite3.connect(":memory:", check_same_thread=False)


def _apply_affinity(types: Dict[str, str], table: str, param_columns: Dict[str, str], bound: Dict[str, Any],
                    tr: Any) -> None:
    """SQLite column affinity for a value bound straight into a column (``INSERT ... VALUES (?)``,
    ``SET col = ?``) -- what SQLite itself does on storage:

    * TEXT column: an int or float is stored as its text;
    * REAL column: an int is stored as a float (so ``5`` reads back as ``5.0``, as on SQLite);
    * INTEGER column: a float with no fractional part is stored as an int; a fractional float is REFUSED
      (SQLite would keep 1.5 as REAL in an INTEGER column; PostgreSQL would round it -- never silently).
    """
    for name, column in param_columns.items():
        value = bound.get(name)
        if value is None or isinstance(value, (str, bytes)) or type(value) is bool:
            continue
        aff = types.get(column)
        if aff == "TEXT" and isinstance(value, (int, float)):
            bound[name] = _sqlite_text(value)
        elif aff == "REAL" and type(value) is int:
            bound[name] = float(value)
        elif aff == "INTEGER" and isinstance(value, float):
            if math.isfinite(value) and value == int(value):
                bound[name] = int(value)
            else:
                _record_mismatch("AFFINITY", f"{value!r} into INTEGER column {table}.{column}", tr.sql or "")
                raise sqlite3.OperationalError(f"PostgreSQL type mismatch: {value!r} into INTEGER column "
                                               f"{table}.{column} (SQLite would store a REAL there)")


# ═══════════════════════════════════════════════════════════════════════════════════════════════ rows
class Row:
    """``sqlite3.Row`` semantics: index by position or (case-insensitively) by column name; ``keys()``."""

    __slots__ = ("_values", "_keys", "_index")

    def __init__(self, keys: Tuple[str, ...], index: Dict[str, int], values: tuple) -> None:
        self._values = values
        self._keys = keys
        self._index = index

    def __getitem__(self, key: Any) -> Any:
        if isinstance(key, str):
            try:
                return self._values[self._index[key.lower()]]
            except KeyError:
                raise IndexError("No item with that key") from None
        return self._values[key]

    def keys(self) -> List[str]:
        return list(self._keys)

    def __iter__(self):
        return iter(self._values)

    def __len__(self) -> int:
        return len(self._values)

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, Row):
            return self._keys == other._keys and self._values == other._values
        return NotImplemented

    def __hash__(self) -> int:
        return hash(self._keys) ^ hash(self._values)

    def __repr__(self) -> str:
        return f"<legacy Row {dict(zip(self._keys, self._values))!r}>"


# ═══════════════════════════════════════════════════════════════════════════════════════════════ errors
_RE_RELATION = re.compile(r'relation "([^"]+)" does not exist')
_RE_COLUMN = re.compile(r'column "?([^" ]+)"? (?:of relation "[^"]+" )?does not exist')
_RE_DUP_COLUMN = re.compile(r'column "([^"]+)" of relation "[^"]+" already exists')
_RE_EXISTS = re.compile(r'relation "([^"]+)" already exists')
_RE_KEY = re.compile(r"Key \((.*?)\)=")

_MISMATCH_STATES = {"42883", "42804", "22P02", "22012", "42P18", "42725", "22003", "42846"}


def _record_mismatch(sqlstate: str, message: str, sql: str) -> None:
    text = f"PG_{sqlstate}: {message} | SQL: {' '.join(str(sql).split())[:300]}"
    with D._EVENTS_LOCK:
        if len(D._EVENTS) < 10_000:
            D._EVENTS.append(text)
    logger.error("[LEGACY_POSTGRES_TYPE_MISMATCH] %s", text)


def map_error(exc: BaseException, sql: str = "") -> BaseException:
    """The ``sqlite3`` exception (class and message) SQLite would have raised for the same failure."""
    psycopg = _pg()
    if isinstance(exc, (sqlite3.Error, D.DialectUnsupported)):
        return exc
    if not isinstance(exc, psycopg.Error):
        return exc
    state = getattr(exc, "sqlstate", None) or ""
    diag = getattr(exc, "diag", None)
    primary = (getattr(diag, "message_primary", None) or str(exc)).strip()
    table = getattr(diag, "table_name", None)
    if state.startswith("23"):
        if state == "23505":
            m = _RE_KEY.search(getattr(diag, "message_detail", None) or "")
            cols = [c.strip().strip('"') for c in m.group(1).split(",")] if m else []
            msg = "UNIQUE constraint failed: " + ", ".join(f"{table}.{c}" for c in cols) if cols else primary
            return sqlite3.IntegrityError(msg)
        if state == "23502":
            return sqlite3.IntegrityError(f"NOT NULL constraint failed: {table}.{getattr(diag, 'column_name', '')}")
        if state == "23514":
            return sqlite3.IntegrityError(f"CHECK constraint failed: {getattr(diag, 'constraint_name', '')}")
        if state == "23503":
            return sqlite3.IntegrityError("FOREIGN KEY constraint failed")
        return sqlite3.IntegrityError(primary)
    if state == "42P01":
        m = _RE_RELATION.search(primary)
        return sqlite3.OperationalError(f"no such table: {m.group(1) if m else primary}")
    if state == "42703":
        m = _RE_COLUMN.search(primary)
        return sqlite3.OperationalError(f"no such column: {m.group(1) if m else primary}")
    if state == "42701":
        m = _RE_DUP_COLUMN.search(primary)
        return sqlite3.OperationalError(f"duplicate column name: {m.group(1) if m else primary}")
    if state == "42P07":
        m = _RE_EXISTS.search(primary)
        return sqlite3.OperationalError(f"table {m.group(1) if m else primary} already exists")
    if state == "42710":
        return sqlite3.OperationalError(primary)
    if state in ("55P03", "40001", "40P01"):
        return sqlite3.OperationalError(f"database is locked ({state}: {primary})")
    if state == "25006":
        return sqlite3.OperationalError("attempt to write a readonly database")
    if state == "0A000" and "cf_sqlite" in primary:
        return D._unsupported(primary, sql)
    if state in _MISMATCH_STATES:
        _record_mismatch(state, primary, sql)
        if state == "22012":
            return sqlite3.OperationalError(f"division by zero (SQLite returns NULL): {primary}")
        return sqlite3.OperationalError(f"PostgreSQL type mismatch ({state}): {primary}")
    if state.startswith("42"):
        return sqlite3.OperationalError(f"{primary} (sqlstate {state})")
    if isinstance(exc, psycopg.OperationalError):
        return sqlite3.OperationalError(f"disk I/O error: PostgreSQL connection failed: {primary}")
    if isinstance(exc, psycopg.InterfaceError):
        return sqlite3.ProgrammingError(primary)
    if isinstance(exc, psycopg.ProgrammingError):
        return sqlite3.ProgrammingError(primary)
    if isinstance(exc, psycopg.DataError):
        _record_mismatch(state, primary, sql)
        return sqlite3.OperationalError(f"PostgreSQL data error ({state}): {primary}")
    return sqlite3.OperationalError(f"{primary} (sqlstate {state})")


# ═══════════════════════════════════════════════════════════════════════════════════════════════ catalog
class _Catalog:
    """Per-database catalog facts the adapter needs (cached, dropped on DDL executed through the adapter)."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.clear()

    def clear(self) -> None:
        with self._lock:
            self._rowid: Dict[str, bool] = {}
            self._columns: Dict[str, List[Tuple[str, Optional[str], bool]]] = {}
            self._keys: Dict[str, Any] = {}
            self._types: Dict[str, Dict[str, str]] = {}
            self._declared: Optional[Dict[str, str]] = None

    def has_rowid(self, raw: Any, table: str) -> bool:
        hit = self._rowid.get(table)
        if hit:
            return True
        row = raw.execute(
            "SELECT 1 FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid WHERE c.relname = %s "
            "AND pg_table_is_visible(c.oid) AND a.attname = %s AND NOT a.attisdropped", (table, D.ROWID)).fetchone()
        if row:
            self._rowid[table] = True
        return bool(row)

    def columns(self, raw: Any, table: str) -> List[Tuple[str, Optional[str], bool]]:
        """(name, default expression, is_identity) in table order, without cf_rowid and generated columns."""
        hit = self._columns.get(table)
        if hit is not None:
            return hit
        rows = raw.execute(
            "SELECT a.attname, pg_get_expr(d.adbin, d.adrelid), a.attidentity <> '' FROM pg_attribute a "
            "JOIN pg_class c ON c.oid = a.attrelid LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid "
            "AND d.adnum = a.attnum WHERE c.relname = %s AND pg_table_is_visible(c.oid) AND a.attnum > 0 "
            "AND NOT a.attisdropped AND a.attgenerated = '' AND a.attname <> %s ORDER BY a.attnum",
            (table, D.ROWID)).fetchall()
        cols = [(r[0], r[1], bool(r[2])) for r in rows]
        if cols:
            self._columns[table] = cols
        return cols

    def declared_names(self, raw: Any) -> Dict[str, str]:
        """folded column name -> declared mixed-case SQLite name (from the column comments the translator writes)."""
        hit = self._declared
        if hit is not None:
            return hit
        rows = raw.execute(
            "SELECT DISTINCT a.attname, substr(d.description, %s::integer) FROM pg_description d "
            "JOIN pg_attribute a ON a.attrelid = d.objoid AND a.attnum = d.objsubid "
            "JOIN pg_class c ON c.oid = a.attrelid WHERE d.classoid = 'pg_class'::regclass "
            "AND d.description LIKE %s AND pg_table_is_visible(c.oid)",
            (len(D.DECLARED_NAME_PREFIX) + 1, D.DECLARED_NAME_PREFIX + "%")).fetchall()
        self._declared = {r[0]: r[1] for r in rows}
        return self._declared

    def column_types(self, raw: Any, table: str) -> Dict[str, str]:
        """column -> SQLite affinity of its PostgreSQL type (INTEGER, REAL, TEXT, BLOB, NUMERIC)."""
        hit = self._types.get(table)
        if hit is not None:
            return hit
        rows = raw.execute(
            "SELECT a.attname, format_type(a.atttypid, a.atttypmod) FROM pg_attribute a JOIN pg_class c "
            "ON c.oid = a.attrelid WHERE c.relname = %s AND pg_table_is_visible(c.oid) AND a.attnum > 0 "
            "AND NOT a.attisdropped", (table,)).fetchall()
        aff = {"bigint": "INTEGER", "integer": "INTEGER", "smallint": "INTEGER", "double precision": "REAL",
               "numeric": "REAL", "real": "REAL", "text": "TEXT", "bytea": "BLOB"}
        types = {r[0]: aff.get(r[1], "REAL" if r[1].startswith("numeric") else "OTHER") for r in rows}
        if types:
            self._types[table] = types
        return types

    def unique_keys(self, raw: Any, table: str) -> List[Tuple[str, ...]]:
        hit = self._keys.get(table)
        if hit is not None:
            return hit
        rows = raw.execute(
            "SELECT x.indexrelid, x.indpred IS NOT NULL, x.indexprs IS NOT NULL, "
            "ARRAY(SELECT a.attname FROM unnest(x.indkey::int2[]) WITH ORDINALITY k(n, i) "
            "JOIN pg_attribute a ON a.attrelid = x.indrelid AND a.attnum = k.n ORDER BY k.i)::text[] "
            "FROM pg_index x JOIN pg_class c ON c.oid = x.indrelid "
            "WHERE c.relname = %s AND pg_table_is_visible(c.oid) AND x.indisunique", (table,)).fetchall()
        keys: List[Tuple[str, ...]] = []
        for _oid, partial, expr, cols in rows:
            if partial or expr:
                raise D._unsupported(f"INSERT OR REPLACE into {table}, which has a partial or expression unique "
                                     f"index (SQLite's REPLACE semantics for it are not emulated)")
            keys.append(tuple(cols))
        self._keys[table] = keys
        return keys


_CATALOGS: Dict[str, _Catalog] = {}


# ═══════════════════════════════════════════════════════════════════════════════════════════════ pool
class _Pool:
    def __init__(self, url: str, size: int = DEFAULT_POOL_SIZE) -> None:
        self.url = url
        self.size = max(1, size)
        self._idle: "queue.LifoQueue[Any]" = queue.LifoQueue()
        self._slots = threading.BoundedSemaphore(self.size)
        self._lock = threading.Lock()
        self._bootstrapped = False
        self.stats = {"created": 0, "discarded": 0, "checkouts": 0, "in_use": 0}
        self.catalog = _CATALOGS.setdefault(redact(url), _Catalog())

    def _new(self) -> Any:
        psycopg = _pg()
        raw = psycopg.connect(_conninfo(self.url), autocommit=True, prepare_threshold=None,
                              options=f"-c TimeZone=UTC -c lock_timeout={BUSY_TIMEOUT_MS}",
                              application_name="cosmicforge-legacy")
        _register(raw)
        with self._lock:
            self.stats["created"] += 1
        if not self._bootstrapped:
            _bootstrap(raw)
            self._bootstrapped = True
        return raw

    def get(self) -> Any:
        if not self._slots.acquire(timeout=CHECKOUT_TIMEOUT_S):
            raise sqlite3.OperationalError(f"database is locked (legacy PostgreSQL pool of {self.size} exhausted)")
        try:
            while True:
                try:
                    raw = self._idle.get_nowait()
                except queue.Empty:
                    raw = self._new()
                    break
                if self._healthy(raw):
                    break
                self._discard(raw)
            with self._lock:
                self.stats["checkouts"] += 1
                self.stats["in_use"] += 1
            return raw
        except BaseException as exc:
            self._slots.release()
            raise map_error(exc) from exc

    def _healthy(self, raw: Any) -> bool:
        psycopg = _pg()
        try:
            if raw.closed or raw.broken:
                return False
            if raw.info.transaction_status != psycopg.pq.TransactionStatus.IDLE:
                return False
            raw.execute("SELECT 1")
            return True
        except Exception:
            return False

    def _discard(self, raw: Any) -> None:
        with self._lock:
            self.stats["discarded"] += 1
        try:
            raw.close()
        except Exception:
            pass

    def put(self, raw: Any, *, broken: bool = False, dirty: bool = False) -> None:
        psycopg = _pg()
        try:
            if broken or raw.closed or raw.broken:
                self._discard(raw)
                return
            try:
                if raw.info.transaction_status != psycopg.pq.TransactionStatus.IDLE:
                    raw.execute("ROLLBACK")
                if dirty:
                    raw.execute("RESET ALL")
                if raw.info.transaction_status != psycopg.pq.TransactionStatus.IDLE:
                    raise RuntimeError("connection not idle after rollback")
                self._idle.put(raw)
            except Exception:
                self._discard(raw)
        finally:
            with self._lock:
                self.stats["in_use"] -= 1
            self._slots.release()

    def close_idle(self) -> int:
        n = 0
        while True:
            try:
                raw = self._idle.get_nowait()
            except queue.Empty:
                return n
            n += 1
            try:
                raw.close()
            except Exception:
                pass


_POOLS: Dict[str, _Pool] = {}
_POOLS_LOCK = threading.Lock()


def _pool(url: str) -> _Pool:
    p = _POOLS.get(url)
    if p is None:
        with _POOLS_LOCK:
            p = _POOLS.get(url)
            if p is None:
                p = _POOLS[url] = _Pool(url)
    return p


def close_pools(url: Optional[str] = None) -> None:
    """Close idle pooled connections (all URLs, or one). Checked-out connections close when returned."""
    with _POOLS_LOCK:
        targets = [(k, v) for k, v in _POOLS.items() if url is None or k == url]
        for k, _ in targets:
            _POOLS.pop(k, None)
    for _, p in targets:
        p.close_idle()


def pool_stats(url: str) -> Dict[str, int]:
    p = _POOLS.get(url)
    return dict(p.stats) if p else {}


atexit.register(close_pools)


def _bootstrap(raw: Any) -> None:
    """Check the database can host the legacy schema faithfully and install the compatibility objects."""
    row = raw.execute("SELECT datcollate, datctype FROM pg_database WHERE datname = current_database()").fetchone()
    if row and (row[0] not in ("C", "POSIX", "C.UTF-8", "C.utf8") or row[1] not in ("C", "POSIX", "C.UTF-8",
                                                                                     "C.utf8")):
        raise LegacyPostgresConfigError(
            f"the legacy schema needs a database created with the C locale (SQLite compares text bytewise and folds "
            f"case for ASCII only); this database has LC_COLLATE={row[0]!r}, LC_CTYPE={row[1]!r}. Create it with "
            f"CREATE DATABASE <name> TEMPLATE template0 ENCODING 'UTF8' LC_COLLATE 'C' LC_CTYPE 'C'.")
    try:
        version = raw.execute("SELECT value FROM cf_legacy_meta WHERE key = 'bootstrap_version'").fetchone()
    except _pg().errors.UndefinedTable:
        version = None
    if version and version[0] == D.BOOTSTRAP_VERSION:
        return
    with raw.transaction():
        raw.execute(_LOCK_SQL)
        raw.execute(D.BOOTSTRAP_SQL)
        raw.execute("INSERT INTO cf_legacy_meta(key, value) VALUES ('bootstrap_version', %s) "
                    "ON CONFLICT (key) DO UPDATE SET value = excluded.value", (D.BOOTSTRAP_VERSION,))


# ═══════════════════════════════════════════════════════════════════════════════════════════════ cursor
class Cursor:
    def __init__(self, connection: "LegacyConnection") -> None:
        self.connection = connection
        self.row_factory = connection.row_factory
        self.arraysize = 1
        self.description: Optional[tuple] = None
        self.rowcount = -1
        self.lastrowid: Optional[int] = None
        self._rows: List[tuple] = []
        self._pos = 0
        self._keys: Tuple[str, ...] = ()
        self._index: Dict[str, int] = {}
        self._closed = False

    # -- results
    def _set_result(self, names: Optional[Sequence[str]], rows: Optional[List[tuple]], rowcount: int) -> None:
        self.rowcount = rowcount
        self._pos = 0
        if names is None:
            self.description = None
            self._rows = []
            self._keys = ()
            self._index = {}
            return
        keep = [k for k, n in enumerate(names) if n != D.ROWID]
        if len(keep) != len(names):
            names = [names[k] for k in keep]
            rows = [tuple(r[k] for k in keep) for r in rows or ()]
        declared = self.connection._declared_names()
        if declared:
            names = [declared.get(n, n) for n in names]
        self._keys = tuple(names)
        self._index = {}
        for k, n in enumerate(names):
            self._index.setdefault(n.lower(), k)
        self.description = tuple((n, None, None, None, None, None, None) for n in names)
        self._rows = list(rows or ())

    def _make(self, values: tuple) -> Any:
        factory = self.row_factory
        if factory is None:
            return values
        if factory is sqlite3.Row:
            return Row(self._keys, self._index, values)
        return factory(self, values)

    # -- DB-API
    def execute(self, sql: str, parameters: Any = ()) -> "Cursor":
        self._check()
        self.connection._execute(self, sql, parameters)
        return self

    def executemany(self, sql: str, seq_of_parameters: Iterable[Any]) -> "Cursor":
        self._check()
        self.connection._executemany(self, sql, seq_of_parameters)
        return self

    def executescript(self, script: str) -> "Cursor":
        self._check()
        self.connection._executescript(self, script)
        return self

    def fetchone(self) -> Any:
        if self._pos >= len(self._rows):
            return None
        row = self._rows[self._pos]
        self._pos += 1
        return self._make(row)

    def fetchmany(self, size: Optional[int] = None) -> List[Any]:
        size = self.arraysize if size is None else size
        out = []
        for _ in range(size):
            r = self.fetchone()
            if r is None:
                break
            out.append(r)
        return out

    def fetchall(self) -> List[Any]:
        rows = self._rows[self._pos:]
        self._pos = len(self._rows)
        return [self._make(r) for r in rows]

    def __iter__(self):
        return self

    def __next__(self) -> Any:
        r = self.fetchone()
        if r is None:
            raise StopIteration
        return r

    def close(self) -> None:
        self._closed = True
        self._rows = []

    def _check(self) -> None:
        if self._closed:
            raise sqlite3.ProgrammingError("Cannot operate on a closed cursor.")

    def setinputsizes(self, *_a: Any) -> None:
        pass

    def setoutputsize(self, *_a: Any) -> None:
        pass


# ═══════════════════════════════════════════════════════════════════════════════════════════════ connection
class LegacyConnection:
    """The ``sqlite3.Connection`` surface the legacy code uses, over one pooled psycopg connection."""

    def __init__(self, pool: _Pool, raw: Any) -> None:
        self._pool = pool
        self._raw = raw
        self._catalog = pool.catalog
        self.row_factory: Any = None
        self.isolation_level: Optional[str] = ""
        self._in_txn = False
        self._locked = False
        self._sp_open = False
        self._user_savepoints: List[str] = []
        self._outer_savepoint: Optional[str] = None
        self._lastrowid = 0
        self._changes = 0
        self.total_changes = 0
        self._trace: Optional[Callable[[str], None]] = None
        self._closed = False
        self._broken = False
        self._dirty = False
        self.dialect = "postgresql"

    # -- sqlite3.Connection API
    @property
    def in_transaction(self) -> bool:
        return self._in_txn

    def cursor(self, factory: Any = None) -> Cursor:
        self._check_open()
        return Cursor(self) if factory is None else factory(self)

    def execute(self, sql: str, parameters: Any = ()) -> Cursor:
        return self.cursor().execute(sql, parameters)

    def executemany(self, sql: str, seq_of_parameters: Iterable[Any]) -> Cursor:
        return self.cursor().executemany(sql, seq_of_parameters)

    def executescript(self, script: str) -> Cursor:
        return self.cursor().executescript(script)

    def commit(self) -> None:
        self._check_open()
        if self._in_txn:
            self._simple("COMMIT")
            self._end_txn()

    def rollback(self) -> None:
        self._check_open()
        if self._in_txn:
            try:
                self._simple("ROLLBACK")
            finally:
                self._end_txn()

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        raw, self._raw = self._raw, None
        broken = self._broken
        if self._in_txn and not broken:
            try:
                raw.execute("ROLLBACK")
            except Exception:
                broken = True
        self._end_txn()
        self._pool.put(raw, broken=broken, dirty=self._dirty)

    def __enter__(self) -> "LegacyConnection":
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> bool:
        if exc_type is None:
            self.commit()
        else:
            try:
                self.rollback()
            except Exception:
                pass
        return False

    def set_trace_callback(self, callback: Optional[Callable[[str], None]]) -> None:
        self._trace = callback

    def create_function(self, *_a: Any, **_k: Any) -> None:
        raise sqlite3.NotSupportedError("create_function is not available on the legacy PostgreSQL adapter")

    def interrupt(self) -> None:
        try:
            self._raw.cancel()
        except Exception:
            pass

    # -- internals
    def _declared_names(self) -> Dict[str, str]:
        return self._catalog.declared_names(self._raw)

    def _check_open(self) -> None:
        if self._closed:
            raise sqlite3.ProgrammingError("Cannot operate on a closed database.")

    def _end_txn(self) -> None:
        self._in_txn = False
        self._locked = False
        self._sp_open = False
        self._user_savepoints = []
        self._outer_savepoint = None

    def _simple(self, sql: str, params: Any = None) -> Any:
        try:
            return self._raw.execute(sql, params)
        except Exception as exc:
            self._note_failure(exc)
            raise map_error(exc, sql) from exc

    def _note_failure(self, exc: BaseException) -> None:
        psycopg = _pg()
        if isinstance(exc, psycopg.OperationalError) or getattr(self._raw, "broken", False):
            self._broken = True
            self._in_txn = False

    def _begin(self, *, lock: bool, repeatable: bool = False) -> None:
        stmt = "BEGIN ISOLATION LEVEL REPEATABLE READ" if repeatable else "BEGIN"
        if lock:
            stmt += "; " + _LOCK_SQL
        self._simple(stmt)
        self._in_txn = True
        self._locked = lock
        self._sp_open = False

    def _bind(self, tr: D.Translation, params: Any) -> Dict[str, Any]:
        if params is None:
            params = ()
        names = tr.param_names
        bound: Dict[str, Any] = {}
        if tr.positional:
            if isinstance(params, Mapping):
                if names:
                    raise sqlite3.ProgrammingError("Binding 1 has no name, but you supplied a dictionary (which has "
                                                   "only names).")
                seq: Sequence[Any] = ()
            else:
                seq = list(params)
            if len(seq) != len(names):
                raise sqlite3.ProgrammingError(f"Incorrect number of bindings supplied. The current statement uses "
                                               f"{len(names)}, and there are {len(seq)} supplied.")
            for k, v in enumerate(seq):
                bound[f"p{k}"] = _adapt_value(v, k + 1)
        else:
            if not isinstance(params, Mapping):
                raise sqlite3.ProgrammingError("named parameters need a mapping")
            for name in names:
                if name not in params:
                    raise sqlite3.ProgrammingError(f"You did not supply a value for binding parameter :{name}.")
                bound[name] = _adapt_value(params[name], name)
        if tr.param_columns and tr.target_table:
            mapping = tr.param_columns
            if any(c.startswith("#") for c in mapping.values()):
                order = [c[0] for c in self._catalog.columns(self._raw, tr.target_table)]
                mapping = {n: (order[int(c[1:])] if c.startswith("#") and int(c[1:]) < len(order) else c)
                           for n, c in mapping.items()}
            _apply_affinity(self._catalog.column_types(self._raw, tr.target_table), tr.target_table,
                            mapping, bound, tr)
        if tr.needs_lastrowid:
            bound["cf_lastrowid"] = self._lastrowid
        if tr.needs_changes:
            bound["cf_changes"] = self._changes
        return bound

    def _run_statement(self, work: Callable[[], Any], sql: str, *, writes: bool, ddl_outside: bool = False) -> Any:
        """Run ``work`` with SQLite's statement atomicity and the legacy write lock."""
        if self._in_txn:
            if writes and not self._locked:
                prefix = _LOCK_SQL + "; "
                self._locked = True
            else:
                prefix = ""
            self._simple(prefix + (f"RELEASE SAVEPOINT {_SP}; SAVEPOINT {_SP}" if self._sp_open
                                   else f"SAVEPOINT {_SP}"))
            self._sp_open = True
            try:
                return work()
            except BaseException as exc:
                if not self._broken:
                    try:
                        self._raw.execute(f"ROLLBACK TO SAVEPOINT {_SP}")
                    except Exception as inner:
                        self._note_failure(inner)
                raise
        if writes:
            # a write outside a transaction: its own transaction, holding the write lock (SQLite autocommit)
            self._simple(f"BEGIN; {_LOCK_SQL}")
            try:
                result = work()
            except BaseException:
                if not self._broken:
                    try:
                        self._raw.execute("ROLLBACK")
                    except Exception as inner:
                        self._note_failure(inner)
                raise
            self._simple("COMMIT")
            return result
        return work()

    def _pg_execute(self, sql: str, bound: Optional[Dict[str, Any]], *, source: str) -> Any:
        try:
            return self._raw.execute(sql, bound if bound is not None else {})
        except Exception as exc:
            self._note_failure(exc)
            raise map_error(exc, source) from exc

    def _execute(self, cur: Cursor, sql: str, params: Any) -> None:
        self._check_open()
        if not isinstance(sql, str):
            raise TypeError("execute() argument 1 must be str")
        if self._trace is not None:
            try:
                self._trace(sql)
            except Exception:
                pass
        tr = D.translate(sql)
        kind = tr.kind
        if kind == "multiple":
            raise sqlite3.ProgrammingError("You can only execute one statement at a time.")
        if kind == "noop":
            cur._set_result(None, None, -1)
            return
        if kind == "txn":
            self._txn(tr.plan)
            cur._set_result(None, None, -1)
            return
        if kind == "pragma":
            names, rows = self._pragma(tr.plan, sql)
            cur._set_result(names, rows, -1)
            return
        bound = self._bind(tr, params)
        if self.isolation_level is not None and not self._in_txn and D.is_implicit_begin_statement(sql):
            self._begin(lock=True)
        if kind == "replace":
            rowcount, last = self._run_statement(lambda: self._replace(tr, bound, sql), sql, writes=True)
            self._after_write(cur, rowcount, last)
            return
        if kind == "trigger":
            self._run_statement(lambda: self._trigger(tr, sql), sql, writes=True)
            self._catalog.clear()
            cur._set_result(None, None, -1)
            return
        if kind == "drop_trigger":
            self._run_statement(lambda: self._drop_trigger(tr, sql), sql, writes=True)
            self._catalog.clear()
            cur._set_result(None, None, -1)
            return
        if kind == "view":
            self._run_statement(lambda: self._view(tr, sql), sql, writes=True)
            cur._set_result(None, None, -1)
            return
        if kind == "ddl" and not bound:
            self._ddl_batch(tr, sql)
            self._catalog.clear()
            if isinstance(tr.plan, D.RetypeMoney):
                self._retype_money(tr.plan.table, sql)
            cur._set_result(None, None, -1)
            return
        pg_sql = tr.sql
        internal_returning = False
        if kind == "dml" and tr.insert_table and not tr.has_returning:
            if self._catalog.has_rowid(self._raw, tr.insert_table):
                pg_sql = pg_sql + f" RETURNING {D.ROWID}"
                internal_returning = True

        def work() -> Tuple[Optional[List[str]], List[tuple], int]:
            pc = self._pg_execute(pg_sql, bound, source=sql)
            if tr.registry:
                self._apply_registry(tr.registry)
            if pc.description is None:
                return None, [], pc.rowcount
            names = [d.name for d in pc.description]
            return names, pc.fetchall(), pc.rowcount

        try:
            names, rows, rowcount = self._run_statement(work, sql, writes=tr.writes)
        except sqlite3.IntegrityError as exc:
            # SQLite's IGNORE skips the row that violates NOT NULL or CHECK too (the savepoint already undid it)
            if not (tr.or_ignore and str(exc).startswith(("NOT NULL constraint failed", "CHECK constraint failed"))):
                raise
            if not self._single_values_row(sql):
                raise D._unsupported("multi-row INSERT OR IGNORE that violates NOT NULL/CHECK", sql) from exc
            names, rows, rowcount = None, [], 0
        if tr.catalog_change:
            self._catalog.clear()
        if kind == "dml":
            if internal_returning:
                last = rows[-1][0] if rows else None
                self._identity_bump(tr)
                self._after_write(cur, len(rows), last)
                return
            self._after_write(cur, rowcount if rowcount is not None else -1, None,
                              insert=tr.insert_table is not None and not tr.has_returning)
            if tr.has_returning:
                cur._set_result(names, rows, rowcount)
            return
        if kind == "query":
            cur._set_result(names, rows, -1)
            return
        cur._set_result(names, rows, -1)

    @staticmethod
    def _single_values_row(sql: str) -> bool:
        toks = D.tokenize(sql)
        for k, t in enumerate(toks):
            if D._is(t, "VALUES") and D._is_op(toks[k + 1] if k + 1 < len(toks) else None, "("):
                end = D._matching(toks, k + 1)
                return not (end + 1 < len(toks) and D._is_op(toks[end + 1], ","))
        return False

    def _after_write(self, cur: Cursor, rowcount: int, last: Optional[int], *, insert: bool = True) -> None:
        cur._set_result(None, None, rowcount)
        if rowcount is not None and rowcount >= 0:
            self._changes = rowcount
            self.total_changes += rowcount
        if last is not None:
            self._lastrowid = int(last)
        if insert:
            cur.lastrowid = self._lastrowid

    def _identity_bump(self, tr: D.Translation) -> None:
        """An explicit value written into an INTEGER PRIMARY KEY moves the identity past it (SQLite: max+1)."""
        if not tr.insert_columns:
            return
        cols = self._catalog.columns(self._raw, tr.insert_table)
        ident = [c[0] for c in cols if c[2]]
        if not ident or ident[0] not in tr.insert_columns:
            return
        col = ident[0]
        self._raw.execute(
            f"SELECT setval(pg_get_serial_sequence(%s, %s), m) FROM (SELECT max({D._qident(col)}) AS m FROM "
            f"{D._qident(tr.insert_table)}) x, pg_sequences s WHERE m IS NOT NULL AND s.schemaname = current_schema() "
            f"AND quote_ident(s.schemaname) || '.' || quote_ident(s.sequencename) = pg_get_serial_sequence(%s, %s) "
            f"AND m > COALESCE(s.last_value, 0)", (tr.insert_table, col, tr.insert_table, col))

    def _executemany(self, cur: Cursor, sql: str, seq: Iterable[Any]) -> None:
        self._check_open()
        if self._trace is not None:
            try:
                self._trace(sql)
            except Exception:
                pass
        tr = D.translate(sql)
        if tr.kind not in ("dml", "replace") or tr.has_returning:
            raise sqlite3.ProgrammingError("executemany() can only execute DML statements.")
        rows = [self._bind(tr, p) for p in seq]
        if self.isolation_level is not None and not self._in_txn and D.is_implicit_begin_statement(sql):
            self._begin(lock=True)
        if not rows:
            cur._set_result(None, None, 0)
            return
        if tr.kind == "replace":
            def work_replace() -> Tuple[int, Optional[int]]:
                total, last = 0, None
                for b in rows:
                    n, last = self._replace(tr, b, sql)
                    total += n
                return total, last

            total, _last = self._run_statement(work_replace, sql, writes=True)
            cur._set_result(None, None, total)
            self._changes = total
            self.total_changes += total
            return

        def work() -> int:
            pc = self._raw.cursor()
            try:
                pc.executemany(tr.sql, rows)
            except Exception as exc:
                self._note_failure(exc)
                raise map_error(exc, sql) from exc
            return pc.rowcount

        total = self._run_statement(work, sql, writes=True)
        cur._set_result(None, None, total)
        if total is not None and total >= 0:
            self._changes = total
            self.total_changes += total

    def _executescript(self, cur: Cursor, script: str) -> None:
        self._check_open()
        if self._in_txn:
            self.commit()
        saved = self.isolation_level
        self.isolation_level = None  # statements of a script run as written (no implicit BEGIN)
        try:
            for stmt in D.split_script(script):
                self._execute(Cursor(self), stmt, ())
        finally:
            self.isolation_level = saved
        cur._set_result(None, None, -1)

    # -- transaction statements
    def _txn(self, plan: D.TxnPlan) -> None:
        a = plan.action
        if a in ("begin", "begin_immediate"):
            if self._in_txn:
                raise sqlite3.OperationalError("cannot start a transaction within a transaction")
            self._begin(lock=(a == "begin_immediate"), repeatable=(a == "begin"))
            return
        if a == "commit":
            if not self._in_txn:
                raise sqlite3.OperationalError("cannot commit - no transaction is active")
            self._simple("COMMIT")
            self._end_txn()
            return
        if a == "rollback":
            if not self._in_txn:
                raise sqlite3.OperationalError("cannot rollback - no transaction is active")
            try:
                self._simple("ROLLBACK")
            finally:
                self._end_txn()
            return
        name = D._qident(plan.name)
        if a == "savepoint":
            if not self._in_txn:
                self._begin(lock=False)
                self._outer_savepoint = plan.name
            if self._sp_open:
                self._simple(f"RELEASE SAVEPOINT {_SP}")
                self._sp_open = False
            self._simple(f"SAVEPOINT {name}")
            self._user_savepoints.append(plan.name)
            return
        if a in ("release", "rollback_to"):
            if plan.name not in self._user_savepoints:
                raise sqlite3.OperationalError(f"no such savepoint: {plan.name}")
            if self._sp_open:
                self._simple(f"RELEASE SAVEPOINT {_SP}")
                self._sp_open = False
            if a == "rollback_to":
                self._simple(f"ROLLBACK TO SAVEPOINT {name}")
                return
            self._simple(f"RELEASE SAVEPOINT {name}")
            idx = len(self._user_savepoints) - 1 - self._user_savepoints[::-1].index(plan.name)
            del self._user_savepoints[idx:]
            if self._outer_savepoint == plan.name and not self._user_savepoints:
                self._simple("COMMIT")
                self._end_txn()
            return
        raise D._unsupported(f"transaction action {a}")

    # -- PRAGMA
    def _pragma(self, plan: D.PragmaPlan, sql: str) -> Tuple[Optional[List[str]], List[tuple]]:
        name, arg, value = plan.name, plan.arg, plan.value
        q = self._query
        if name in ("table_info", "table_xinfo"):
            cols = ["cid", "name", "type", "notnull", "dflt_value", "pk"] + (["hidden"] if name == "table_xinfo"
                                                                              else [])
            rows = q(
                "SELECT (row_number() OVER (ORDER BY a.attnum) - 1)::bigint, a.attname::text, "
                "CASE format_type(a.atttypid, a.atttypmod) WHEN 'bigint' THEN 'INTEGER' WHEN 'integer' THEN "
                "'INTEGER' WHEN 'double precision' THEN 'REAL' WHEN 'numeric(38,18)' THEN 'REAL' WHEN 'numeric' "
                "THEN 'REAL' WHEN 'text' THEN 'TEXT' WHEN 'bytea' THEN 'BLOB' ELSE upper(format_type(a.atttypid, "
                "a.atttypmod)) END, a.attnotnull::int, "
                "CASE WHEN a.attgenerated = '' THEN regexp_replace(pg_get_expr(d.adbin, d.adrelid), "
                "'::(text|bigint|integer|numeric|double precision)', '', 'g') END, "
                "COALESCE(array_position(pk.conkey, a.attnum), 0)::bigint"
                + (", 0::bigint" if name == "table_xinfo" else "") +
                " FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid "
                "LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum "
                "LEFT JOIN pg_constraint pk ON pk.conrelid = c.oid AND pk.contype = 'p' "
                "WHERE c.relname = %s AND pg_table_is_visible(c.oid) AND c.relkind IN ('r', 'p', 'v') "
                "AND a.attnum > 0 AND NOT a.attisdropped AND a.attname <> %s ORDER BY a.attnum",
                (arg, D.ROWID), sql)
            declared = self._declared_names()
            if declared:
                rows = [(r[0], declared.get(r[1], r[1])) + tuple(r[2:]) for r in rows]
            return cols, rows
        if name == "index_list":
            rows = q(
                "SELECT (row_number() OVER (ORDER BY ic.relname) - 1)::bigint, ic.relname::text, "
                "x.indisunique::int, CASE WHEN x.indisprimary THEN 'pk' WHEN con.oid IS NOT NULL THEN 'u' "
                "ELSE 'c' END, (x.indpred IS NOT NULL)::int FROM pg_index x "
                "JOIN pg_class ic ON ic.oid = x.indexrelid JOIN pg_class c ON c.oid = x.indrelid "
                "LEFT JOIN pg_constraint con ON con.conindid = x.indexrelid AND con.contype IN ('u', 'p') "
                "WHERE c.relname = %s AND pg_table_is_visible(c.oid) ORDER BY ic.relname", (arg,), sql)
            return ["seq", "name", "unique", "origin", "partial"], rows
        if name == "index_info":
            rows = q(
                "SELECT (k.i - 1)::bigint, CASE WHEN k.n = 0 THEN -2 ELSE (SELECT count(*) FROM pg_attribute a2 "
                "WHERE a2.attrelid = x.indrelid AND a2.attnum > 0 AND NOT a2.attisdropped AND a2.attnum < k.n "
                "AND a2.attname <> %s) END::bigint, a.attname::text FROM pg_index x "
                "JOIN pg_class ic ON ic.oid = x.indexrelid CROSS JOIN LATERAL unnest(x.indkey::int2[]) WITH ORDINALITY k(n, i) "
                "LEFT JOIN pg_attribute a ON a.attrelid = x.indrelid AND a.attnum = k.n "
                "WHERE ic.relname = %s AND pg_table_is_visible(ic.oid) AND k.i <= x.indnkeyatts ORDER BY k.i",
                (D.ROWID, arg), sql)
            return ["seqno", "cid", "name"], rows
        if name == "user_version":
            if value is None and arg is None:
                rows = q("SELECT COALESCE((SELECT value FROM cf_legacy_meta WHERE key = 'user_version'), '0')"
                         "::bigint", (), sql)
                return ["user_version"], rows
            v = int(value if value is not None else arg)
            self._run_statement(lambda: self._pg_execute(
                "INSERT INTO cf_legacy_meta(key, value) VALUES ('user_version', %(v)s) ON CONFLICT (key) DO UPDATE "
                "SET value = excluded.value", {"v": str(v)}, source=sql), sql, writes=True)
            return None, []
        if name == "journal_mode":
            # PostgreSQL always writes ahead; report what the legacy code asks for and gets on SQLite.
            return ["journal_mode"], [("wal",)]
        if name == "synchronous":
            # synchronous_commit=on (the default): a COMMIT is durable when it returns -- SQLite FULL (2).
            return (["synchronous"], [(2,)]) if value is None and arg is None else (None, [])
        if name == "busy_timeout":
            v = value if value is not None else arg
            if v is None:
                return ["timeout"], [(BUSY_TIMEOUT_MS,)]
            self._simple(f"SET lock_timeout = {int(v)}")
            self._dirty = True
            return ["timeout"], [(int(v),)]
        if name == "foreign_keys":
            v = value if value is not None else arg
            if v is None:
                return ["foreign_keys"], [(0,)]
            if str(v).strip().lower() in ("1", "on", "true", "yes"):
                # Legacy foreign keys are declarative (the legacy runtime never enables them) and are not
                # installed on PostgreSQL, so there is nothing to switch on. The only legacy caller is the user
                # backend's manual SQL migration runner, which re-enables them after a table rebuild.
                logger.warning("[LEGACY_POSTGRES] PRAGMA foreign_keys=ON is a no-op: legacy foreign keys are "
                               "declarative and not installed on PostgreSQL")
            return None, []
        if name == "wal_checkpoint":
            return ["busy", "log", "checkpointed"], [(0, 0, 0)]
        if name == "query_only":
            v = value if value is not None else arg
            if v is None:
                rows = q("SELECT (current_setting('default_transaction_read_only') = 'on')::int", (), sql)
                return ["query_only"], rows
            on = str(v).strip().lower() in ("1", "on", "true", "yes")
            self._simple(f"SET default_transaction_read_only = {'on' if on else 'off'}")
            self._dirty = True
            return None, []
        if name == "page_size":
            return ["page_size"], [(8192,)]
        if name == "page_count":
            return ["page_count"], q("SELECT (pg_database_size(current_database()) / 8192)::bigint", (), sql)
        if name == "freelist_count":
            return ["freelist_count"], [(0,)]
        if name in ("cache_size", "temp_store", "mmap_size", "wal_autocheckpoint", "recursive_triggers",
                    "optimize"):
            return None, []
        raise D._unsupported(f"PRAGMA {name} has no PostgreSQL equivalent here", sql)

    def _query(self, pg_sql: str, params: Sequence[Any], source: str) -> List[tuple]:
        def work() -> List[tuple]:
            try:
                return self._raw.execute(pg_sql, params).fetchall()
            except Exception as exc:
                self._note_failure(exc)
                raise map_error(exc, source) from exc
        return self._run_statement(work, source, writes=False)

    # -- INSERT OR REPLACE
    def _replace(self, tr: D.Translation, bound: Dict[str, Any], sql: str) -> Tuple[int, Optional[int]]:
        plan: D.ReplacePlan = tr.plan
        table = plan.table
        catalog_cols = self._catalog.columns(self._raw, table)
        if not catalog_cols:
            raise sqlite3.OperationalError(f"no such table: {table}")
        cols = list(plan.columns) if plan.columns is not None else [c[0] for c in catalog_cols]
        if plan.rows_are_params is not None:
            rows = [[bound[n] for n in r] for r in plan.rows_are_params]
        elif plan.rows is not None:
            rows = [list(r) for r in self._pg_execute("VALUES " + ", ".join(f"({r})" for r in plan.rows), bound,
                                                      source=sql).fetchall()]
        else:
            rows = [list(r) for r in self._pg_execute(plan.select_sql, bound, source=sql).fetchall()]
        info = {c[0]: c for c in catalog_cols}
        keys = self._catalog.unique_keys(self._raw, table)
        has_rowid = self._catalog.has_rowid(self._raw, table)
        total, last = 0, None
        qt = D._qident(table)
        for values in rows:
            if len(values) != len(cols):
                raise sqlite3.OperationalError(f"table {table} has {len(cols)} columns but {len(values)} values were "
                                               f"supplied")
            named = {f"r{k}": v for k, v in enumerate(values)}
            _apply_affinity(self._catalog.column_types(self._raw, table), table,
                            {f"r{k}": c for k, c in enumerate(cols)}, named, tr)
            values = [named[f"r{k}"] for k in range(len(values))]
            row = dict(zip(cols, values))
            conds: List[str] = []
            params: Dict[str, Any] = {}
            for kidx, key in enumerate(keys):
                parts = []
                skip = False
                for cidx, c in enumerate(key):
                    if c not in row:
                        meta = info.get(c)
                        if meta is not None and (meta[2] or meta[1] is None):
                            skip = True  # identity (a fresh value) or NULL default: never conflicts
                            break
                        raise D._unsupported(f"INSERT OR REPLACE into {table} omits {c}, part of a unique key with a "
                                             f"non-NULL default", sql)
                    if row[c] is None:
                        skip = True  # NULLs never conflict in a UNIQUE constraint
                        break
                    pname = f"k{kidx}_{cidx}"
                    params[pname] = row[c]
                    parts.append(f"{D._qident(c)} = %({pname})s")
                if not skip and parts:
                    conds.append("(" + " AND ".join(parts) + ")")
            if conds:
                self._pg_execute(f"DELETE FROM {qt} WHERE " + " OR ".join(conds), params, source=sql)
            ins_params = {f"v{k}": v for k, v in enumerate(values)}
            ins = (f"INSERT INTO {qt} (" + ", ".join(D._qident(c) for c in cols) + ") VALUES ("
                   + ", ".join(f"%(v{k})s" for k in range(len(values))) + ")")
            if has_rowid:
                ins += f" RETURNING {D.ROWID}"
            pc = self._pg_execute(ins, ins_params, source=sql)
            total += 1
            if has_rowid:
                r = pc.fetchone()
                last = r[0] if r else last
        if plan.columns is not None:
            fake = D.Translation(kind="dml", insert_table=table, insert_columns=tuple(cols))
        else:
            fake = D.Translation(kind="dml", insert_table=table, insert_columns=tuple(cols))
        self._identity_bump(fake)
        return total, last

    # -- triggers, views, registry
    def _trigger(self, tr: D.Translation, sql: str) -> None:
        plan: D.TriggerPlan = tr.plan
        exists = self._raw.execute(
            "SELECT 1 FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid WHERE t.tgname = %s "
            "AND pg_table_is_visible(c.oid) AND NOT t.tgisinternal", (plan.name,)).fetchone()
        if exists:
            if plan.if_not_exists:
                return
            raise sqlite3.OperationalError(f"trigger {plan.name} already exists")
        self._pg_execute(plan.function_sql, {}, source=sql)
        self._pg_execute(plan.trigger_sql, {}, source=sql)
        self._apply_registry(tr.registry)

    def _drop_trigger(self, tr: D.Translation, sql: str) -> None:
        plan: D.DropTriggerPlan = tr.plan
        row = self._raw.execute(
            "SELECT c.relname FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid WHERE t.tgname = %s "
            "AND pg_table_is_visible(c.oid) AND NOT t.tgisinternal", (plan.name,)).fetchone()
        if row is None:
            if plan.if_exists:
                return
            raise sqlite3.OperationalError(f"no such trigger: {plan.name}")
        self._pg_execute(f"DROP TRIGGER {D._qident(plan.name)} ON {D._qident(row[0])}", {}, source=sql)
        self._pg_execute(f"DROP FUNCTION IF EXISTS {D.trigger_function_name(plan.name)}()", {}, source=sql)
        self._apply_registry(tr.registry)

    def _view(self, tr: D.Translation, sql: str) -> None:
        plan: D.ViewPlan = tr.plan
        exists = self._raw.execute("SELECT 1 FROM pg_class c WHERE c.relname = %s AND pg_table_is_visible(c.oid)",
                                   (plan.name,)).fetchone()
        if exists:
            if plan.if_not_exists:
                return
            raise sqlite3.OperationalError(f"table {plan.name} already exists")
        self._pg_execute(plan.sql, {}, source=sql)
        self._apply_registry(tr.registry)

    def _ddl_batch(self, tr: D.Translation, sql: str) -> None:
        """A parameterless DDL statement, its registry rows, the savepoint/lock/transaction bracketing -- one round
        trip (simple query protocol), with SQLite's statement atomicity."""
        body = [tr.sql.replace("%%", "%")] + list(tr.post_sql) + _registry_sql(tr.registry)
        if self._in_txn:
            head = []
            if not self._locked:
                head.append(_LOCK_SQL)
            head.append(f"RELEASE SAVEPOINT {_SP}; SAVEPOINT {_SP}" if self._sp_open else f"SAVEPOINT {_SP}")
            script = "; ".join(head + body)
            try:
                self._raw.execute(script)
            except Exception as exc:
                self._note_failure(exc)
                if not self._broken:
                    try:
                        # the lock statement precedes the savepoint, so a failure after it keeps the lock
                        self._raw.execute(f"ROLLBACK TO SAVEPOINT {_SP}")
                        self._sp_open = True
                    except Exception as inner:
                        self._note_failure(inner)
                raise map_error(exc, sql) from exc
            self._locked = True
            self._sp_open = True
            return
        script = "; ".join(["BEGIN", _LOCK_SQL] + body + ["COMMIT"])
        try:
            self._raw.execute(script)
        except Exception as exc:
            self._note_failure(exc)
            if not self._broken:
                try:
                    self._raw.execute("ROLLBACK")
                except Exception as inner:
                    self._note_failure(inner)
            raise map_error(exc, sql) from exc

    def _retype_money(self, table: str, sql: str) -> None:
        """After ``RENAME TO table``: monetary REAL columns of ``table`` that were created under another name (a
        table rebuild) become NUMERIC. ``float8::text`` is the shortest round-trip decimal (PostgreSQL >= 12), so
        every stored value converts exactly."""
        money = D._money_columns(table)
        if not money:
            return
        rows = self._raw.execute(
            "SELECT a.attname FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid WHERE c.relname = %s "
            "AND pg_table_is_visible(c.oid) AND a.attnum > 0 AND NOT a.attisdropped "
            "AND a.atttypid = 'double precision'::regtype", (table,)).fetchall()
        targets = [r[0] for r in rows if r[0] in money]
        if not targets:
            return
        alter = (f"ALTER TABLE {D._qident(table)} " + ", ".join(
            f"ALTER COLUMN {D._qident(c)} TYPE {D.money_type()} USING {D._qident(c)}::text::numeric" for c in targets))
        tr = D.Translation(kind="ddl", sql=alter.replace("%", "%%"), writes=True)
        self._ddl_batch(tr, sql)
        self._catalog.clear()

    def _apply_registry(self, ops: List[tuple]) -> None:
        for stmt in _registry_sql(ops):
            self._raw.execute(stmt)


def _lit(value: Optional[str]) -> str:
    if value is None:
        return "NULL"
    return "'" + str(value).replace("'", "''") + "'"


def _registry_sql(ops: List[tuple]) -> List[str]:
    """``sqlite_master.sql`` bookkeeping for DDL executed through the adapter (literal SQL, no parameters)."""
    out: List[str] = []
    for op in ops:
        kind = op[0]
        if kind in ("create_table", "create", "upsert"):
            _, typ, name, tbl, text = op
            conflict = ("DO UPDATE SET type = excluded.type, tbl_name = excluded.tbl_name, sql = excluded.sql"
                        if kind == "upsert" else "DO NOTHING")
            out.append(f"INSERT INTO cf_legacy_schema(name, type, tbl_name, sql) VALUES ({_lit(name)}, {_lit(typ)}, "
                       f"{_lit(tbl)}, {_lit(text)}) ON CONFLICT (name) {conflict}")
        elif kind == "add_column":
            _, table, coldef = op
            pattern = r"'\)\s*$'"
            out.append(f"UPDATE cf_legacy_schema SET sql = regexp_replace(sql, {pattern}, {_lit(', ' + coldef + ')')}) "
                       f"WHERE name = {_lit(table)} AND type = 'table'")
        elif kind == "rename_table":
            _, old, new = op
            out.append(f"UPDATE cf_legacy_schema SET name = {_lit(new)} WHERE name = {_lit(old)} AND type = 'table'")
            out.append(f"UPDATE cf_legacy_schema SET tbl_name = {_lit(new)} WHERE tbl_name = {_lit(old)}")
        elif kind == "drop_table":
            out.append(f"DELETE FROM cf_legacy_schema WHERE name = {_lit(op[1])} OR tbl_name = {_lit(op[1])}")
        elif kind == "drop":
            out.append(f"DELETE FROM cf_legacy_schema WHERE name = {_lit(op[1])}")
        # rename_column / drop_column: the stored text keeps the original definition (documented)
    return out


# ═══════════════════════════════════════════════════════════════════════════════════════════════ entry points
def connect(url: str) -> LegacyConnection:
    """A pooled adapter connection to ``url`` (``row_factory`` None, like ``sqlite3.connect``). ``close()``
    returns it to the pool."""
    if not is_postgres_url(url):
        raise ValueError("connect() needs a postgresql:// URL")
    pool = _pool(url)
    raw = pool.get()
    return LegacyConnection(pool, raw)


def create_schema(url: str) -> None:
    """Create (idempotently) the whole legacy schema in the PostgreSQL database at ``url``, through the adapter,
    exactly as the runtime creates it: ``DB`` initialisation, ``migrations.migrate()`` (and every ``ensure_*`` it
    calls), ``production_schema.ensure``, the runtime-ownership lease, and the runtime tables that are created
    lazily outside those paths (safety-engine monitoring, adaptive daily risk decisions, the residual prospective
    tracker, broker auto-trading authority, run tracking columns, deployment consents, the entry-protection
    ledger)."""
    if not is_postgres_url(url):
        raise ValueError("create_schema() needs a postgresql:// URL")
    from shared_lib.persistence.db import DB
    from shared_lib.persistence.migrations import migrate

    db = DB(url=url)
    migrate(db)
    from app.execution import production_schema
    from app.ops.runtime_ownership import ensure_ownership_schema

    production_schema.ensure(db, force=True)
    ensure_ownership_schema(db)
    _lazy_runtime_schema(db)


def _lazy_runtime_schema(db: Any) -> None:
    """Every legacy table/column the runtime creates on FIRST USE (not through migrate/ensure), from the one list
    ``shared_lib.persistence.legacy_first_use.first_use_creators`` the legacy inventory also runs. Only schema
    code runs; no row is written."""
    from shared_lib.persistence.legacy_first_use import first_use_creators

    for _name, _method, creator in first_use_creators():
        with db.connect() as conn:
            creator(db, conn)


class _SchemaOnly:
    """Stands in for ``self`` when a class's schema method is called without constructing the class."""

    def __init__(self, db: Any) -> None:
        self.db = db


__all__ = ["LegacyConnection", "LegacyPostgresConfigError", "Row", "close_pools", "connect", "create_schema",
           "is_postgres_url", "map_error", "pool_stats", "redact"]
