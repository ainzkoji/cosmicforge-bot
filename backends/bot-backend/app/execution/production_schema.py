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
"""
from __future__ import annotations

import os
import threading

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


def _key(db):
    path = getattr(db, "path", None)
    if not path or str(path).startswith(":memory") or getattr(db, "_sqlite_uri", False):
        return ("object", id(db))
    try:
        st = os.stat(path)
    except OSError:
        return ("path", path)
    return ("file", path, st.st_ino, st.st_ctime_ns)


def ensure(db, *, force: bool = False) -> bool:
    """Create every production table once for this database. Returns True when
    the DDL ran in this call."""
    key = _key(db)
    if not force and _READY.get(key):
        return False
    with _LOCK:
        if not force and _READY.get(key):
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
        _READY[key] = True
    return True


def forget(db=None) -> None:
    """Drop the memo (tests, or after an operator rebuilt the database)."""
    with _LOCK:
        if db is None:
            _READY.clear()
        else:
            _READY.pop(_key(db), None)


__all__ = ["DDL", "ensure", "forget"]
