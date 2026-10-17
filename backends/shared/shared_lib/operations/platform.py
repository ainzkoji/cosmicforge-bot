"""The platform database as seen by an API process (user-backend admin/public endpoints).

``PLATFORM_DATABASE_URL`` (falling back to ``DATABASE_URL``, as ``Database.from_env`` does) names it; on a
single-server SQLite deployment that is the same file as the legacy database. Migrations are NOT applied here --
they are a deploy step (``python -m shared_lib.persistence.sql migrate``) -- so an API process never changes a
schema. ``ops_schema_ready`` reports whether the operations tables exist, so callers answer 503 instead of
failing obscurely.
"""
from __future__ import annotations

import threading
from typing import Optional

from shared_lib.persistence.sql import Database, DatabaseUnavailable

_lock = threading.Lock()
_db: Optional[Database] = None


class OperationsUnavailable(RuntimeError):
    def __init__(self, code: str, message: str = "") -> None:
        super().__init__(message or code)
        self.code = code


def platform_db() -> Database:
    global _db
    with _lock:
        if _db is None:
            try:
                _db = Database.from_env(pool_size=4)
            except (DatabaseUnavailable, ValueError) as exc:
                raise OperationsUnavailable("PLATFORM_DATABASE_UNAVAILABLE", str(exc)) from exc
        return _db


def reset_platform_db(db: Optional[Database] = None) -> None:
    """Tests (and a reconfigured process) replace the cached database."""
    global _db
    with _lock:
        _db = db


def ops_schema_ready(db: Database) -> bool:
    """True when every operations migration this code knows (30-39 range) is applied."""
    from shared_lib.persistence.sql.operations_schema import MIGRATIONS
    wanted = {m.version for m in MIGRATIONS}
    try:
        with db.transaction(write=False) as conn:
            have = {int(r[0]) for r in conn.all("SELECT version FROM platform_schema_migrations "
                                                 "WHERE version BETWEEN 30 AND 39")}
    except Exception:
        return False
    return wanted <= have
