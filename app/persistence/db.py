from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Iterator


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class DB:
    def __init__(self, path: str = "data/bot.db"):
        self.path = path
        folder = os.path.dirname(self.path)
        if folder:
            os.makedirs(folder, exist_ok=True)
        self._init()

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.path, timeout=30)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def now_utc(self) -> datetime:
        return datetime.now(timezone.utc)

    # ✅ NEW VERSION (single source of truth)
    def _init(self) -> None:
        conn = sqlite3.connect(self.path, timeout=30)
        try:
            conn.execute(
                """
            CREATE TABLE IF NOT EXISTS runs (
                run_id TEXT PRIMARY KEY,
                started_at TEXT NOT NULL,
                stopped_at TEXT,
                mode TEXT NOT NULL,
                interval_seconds INTEGER NOT NULL,
                max_symbols INTEGER NOT NULL
            )
            """
            )

            conn.execute(
                """
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp_utc TEXT NOT NULL,
                run_id TEXT,
                cycle_id TEXT,
                symbol TEXT,
                event_type TEXT NOT NULL,
                action TEXT,
                details_json TEXT,
                FOREIGN KEY(run_id) REFERENCES runs(run_id)
            )
            """
            )

            # ✅ Daily risk state persistence
            conn.execute(
                """
            CREATE TABLE IF NOT EXISTS daily_state (
                day TEXT PRIMARY KEY,
                realized_pnl REAL NOT NULL,
                kill INTEGER NOT NULL,
                last_updated_at TEXT NOT NULL
            )
            """
            )

            # ✅ Per-symbol trading state persistence
            conn.execute(
                """
            CREATE TABLE IF NOT EXISTS symbol_state (
                symbol TEXT PRIMARY KEY,
                position TEXT NOT NULL,
                entry_price REAL,
                last_signal TEXT NOT NULL,
                last_action TEXT NOT NULL,
                last_checked_ms INTEGER NOT NULL,
                adds INTEGER NOT NULL,
                last_trade_ms INTEGER NOT NULL,
                pending_open TEXT NOT NULL,
                entry_qty REAL NOT NULL,
                last_user_trade_id INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL
            )
            """
            )

            # ✅ Migration safety: if symbol_state existed before last_user_trade_id
            try:
                conn.execute(
                    "ALTER TABLE symbol_state ADD COLUMN last_user_trade_id INTEGER NOT NULL DEFAULT 0"
                )
            except Exception:
                pass

            conn.execute("CREATE INDEX IF NOT EXISTS idx_events_run ON events(run_id)")
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_events_time ON events(timestamp_utc)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_events_symbol ON events(symbol)"
            )

            conn.commit()
        finally:
            conn.close()
