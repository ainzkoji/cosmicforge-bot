from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from app.core.config import settings


ADMIN_BACKEND_ROOT = Path(__file__).resolve().parents[2]


class AdminDB:
    """Minimal read-oriented SQLite helper for the admin-backend shell.

    This intentionally avoids running shared migrations or table creation during
    the shell phase; health/auth checks should only open the configured DB.
    """

    def __init__(self, database_url: str | None = None, path: str | None = None):
        self.database_url = database_url or settings.DATABASE_URL
        self._path = Path(path) if path else self._resolve_database_url(self.database_url)

    @property
    def path(self) -> Path:
        return self._path

    @staticmethod
    def _resolve_database_url(database_url: str) -> Path:
        if database_url.startswith("sqlite:///"):
            raw_path = database_url[len("sqlite:///") :]
            candidate = Path(raw_path)
            if candidate.is_absolute():
                return candidate
            return (ADMIN_BACKEND_ROOT / candidate).resolve()
        return Path(database_url).resolve()

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(str(self.path), timeout=max(int(settings.SQLITE_BUSY_TIMEOUT_MS) / 1000, 1))
        conn.row_factory = sqlite3.Row
        try:
            conn.execute(f"PRAGMA busy_timeout={int(settings.SQLITE_BUSY_TIMEOUT_MS)}")
            yield conn
        finally:
            conn.close()


def get_db() -> AdminDB:
    return AdminDB()
