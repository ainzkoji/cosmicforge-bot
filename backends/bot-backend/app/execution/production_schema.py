"""The CATI production tables, created once per database instead of on every call.

Every production module used to run its own ``CREATE TABLE IF NOT EXISTS`` (and
the evidence module its index and triggers) each time it was entered -- about
31 schema statements per account evaluation, several of them three times per
cycle. ``ensure(db)`` runs the whole production DDL exactly once per database
file for the life of the process (keyed by the file's identity, so a database
recreated at the same path is initialised again) and is a no-op afterwards.
The statements are all ``IF NOT EXISTS`` and additive, so running them again
on an existing database -- after a restart, or on a database an older build
created -- is always safe; that is what makes the once-per-process memo
correct rather than merely fast.

File identity is platform dependent, and getting it wrong is silent:

* ``st_ctime`` is the creation time on Windows only. On Linux it is the time
  of the last inode change and moves with every write, so a key built from it
  never matched twice and the whole schema ran again on every call. That was
  the first version of this module: correct on the Windows workstation, a
  no-op on the Linux server it is meant for, and found only by Linux CI.
* The key is therefore ``(path, device, inode)`` plus the creation time where
  the platform reports one (Windows, macOS, BSD). Linux reports none.
* Without a creation time a file deleted and recreated at the same path can
  get the same inode back. Two further signals cover that case without
  touching the database: a file smaller than it was when last seen is treated
  as a different file (SQLite files do not shrink in normal operation), and
  ``missing_table`` drops the memo when a statement fails with "no such
  table", so the next call creates the schema.

Only ``os.stat`` is used. The database file is never opened here: closing any
descriptor of a file drops the process's POSIX locks on it, which SQLite relies
on.
"""
from __future__ import annotations

import os
import sqlite3
import threading

#: identity key -> size in bytes of the file when it was last seen ready
_READY = {}
_LOCK = threading.Lock()

DDL = (
    """CREATE TABLE IF NOT EXISTS cati_production_decisions (
        account_id TEXT NOT NULL, decision_id TEXT NOT NULL, bot_instance_id TEXT,
        observed_at INTEGER NOT NULL, document TEXT NOT NULL,
        PRIMARY KEY(account_id,decision_id))""",
    """CREATE TABLE IF NOT EXISTS cati_production_daily_risk (
        account_id TEXT NOT NULL, day TEXT NOT NULL, opening_wallet REAL NOT NULL,
        loss_latched INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(account_id,day))""",
    """CREATE TABLE IF NOT EXISTS cati_production_fills (
        account_id TEXT NOT NULL, trade_id TEXT NOT NULL, order_id TEXT NOT NULL,
        symbol TEXT NOT NULL, document TEXT NOT NULL, PRIMARY KEY(account_id,symbol,trade_id))""",
    """CREATE TABLE IF NOT EXISTS cati_production_protection (
        account_id TEXT, client_id TEXT, document TEXT NOT NULL, response TEXT,
        PRIMARY KEY(account_id,client_id))""",
    """CREATE TABLE IF NOT EXISTS cati_production_closes (
        account_id TEXT,client_id TEXT,symbol TEXT,identity TEXT,status TEXT,
        document TEXT,PRIMARY KEY(account_id,client_id))""",
    """CREATE TABLE IF NOT EXISTS cati_account_period_risk(account_id TEXT,period TEXT,start_date TEXT,
        peak_equity REAL NOT NULL,transfers REAL NOT NULL DEFAULT 0,PRIMARY KEY(account_id,period,start_date))""",
    """CREATE TABLE IF NOT EXISTS cati_production_state (
        account_id TEXT PRIMARY KEY, user_id TEXT, observed_at INTEGER NOT NULL, document TEXT NOT NULL)""",
)


def birth_ns(st, *, windows: bool):
    """Creation time of the file in nanoseconds, or None when the platform does
    not report one. ``st_ctime`` is used on Windows only (see the module note)."""
    birth = getattr(st, "st_birthtime_ns", None)
    if birth is None and getattr(st, "st_birthtime", None) is not None:
        birth = int(st.st_birthtime * 1_000_000_000)
    if birth is None and windows:
        birth = st.st_ctime_ns
    return birth


def file_key(path, st, *, windows: bool):
    """Identity of a database file that does not change when it is written to."""
    return ("file", path, st.st_dev, st.st_ino, birth_ns(st, windows=windows))


def _identity(db, *, stat=os.stat, windows=None):
    """(key, size). Size is 0 for anything that is not a file on disk."""
    path = getattr(db, "path", None)
    if not path or str(path).startswith(":memory") or getattr(db, "_sqlite_uri", False):
        return ("object", id(db)), 0
    try:
        st = stat(path)
    except OSError:
        return ("path", path), 0
    return file_key(path, st, windows=(os.name == "nt") if windows is None else windows), int(st.st_size)


def _key(db):
    return _identity(db)[0]


def _ready(key, size) -> bool:
    known = _READY.get(key)
    if known is None or size < known:              # unknown, or a smaller (hence different) file
        return False
    if size > known:
        _READY[key] = size                         # the file only grows; remember the high-water mark
    return True


def ensure(db, *, force: bool = False) -> bool:
    """Create every production table once for this database. Returns True when
    the DDL ran in this call."""
    key, size = _identity(db)
    if not force and _ready(key, size):
        return False
    with _LOCK:
        if not force and _ready(key, size):
            return False
        with db.connect() as c:
            for statement in DDL:
                c.execute(statement)
            from app.trading_intelligence.integration.production_evidence import initialize as evaluations
            evaluations(c)
            from app.execution.protection_state import initialize as protection_state
            protection_state(c)
            from app.trading_intelligence.integration.income_ledger import initialize as income
            income(c)
            # The cycle's own recorders: created here once, never inside a cycle.
            from app.observability.account_recorder import initialize as equity_history
            equity_history(c)
            from app.observability.user_events import initialize as user_events
            user_events(c)
        # Identity again: the file may not have existed before the first connection.
        key, size = _identity(db)
        _READY[key] = size
    return True


def missing_table(db, exc) -> bool:
    """Call with an exception raised while using the production tables. When it
    is SQLite reporting a missing table, the memo for this database is dropped
    so the next ``ensure`` creates the schema, and True is returned. This is the
    recovery for a database replaced under a running process on a platform
    whose files carry no creation time."""
    if isinstance(exc, sqlite3.OperationalError) and "no such table" in str(exc).lower():
        forget(db)
        return True
    return False


def forget(db=None) -> None:
    """Drop the memo (tests, or after an operator rebuilt the database)."""
    with _LOCK:
        if db is None:
            _READY.clear()
        else:
            _READY.pop(_key(db), None)


__all__ = ["DDL", "ensure", "forget", "missing_table", "file_key", "birth_ns"]
