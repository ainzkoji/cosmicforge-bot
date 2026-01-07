from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Iterator


# =========================
# Time helpers
# =========================
def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# =========================
# Database class
# =========================
class DB:
    """
    Single source of truth for SQLite access.
    Default path: data/bot.db
    """

    def __init__(self, path: str = "data/bot.db"):
        self.path = path

        folder = os.path.dirname(self.path)
        if folder:
            os.makedirs(folder, exist_ok=True)

        self._init()

    # -------------------------
    # Connection manager
    # -------------------------
    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.path, timeout=30, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    # -------------------------
    # Init / migrations
    # -------------------------
    def _init(self) -> None:
        conn = sqlite3.connect(self.path, timeout=30)
        try:
            # =========================
            # Runs (one per bot run)
            # =========================
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

            # =========================
            # Events (audit log)
            # =========================
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

            # =========================
            # Daily risk state
            # =========================
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

            # =========================
            # Per-symbol trading state
            # =========================
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
                    last_stop_ms INTEGER NOT NULL DEFAULT 0,
                    reentry_confirm_signal TEXT NOT NULL DEFAULT 'NONE',
                    reentry_confirm_count INTEGER NOT NULL DEFAULT 0,
                    updated_at TEXT NOT NULL
                )
                """
            )

            # =========================
            # Run summary (derived metrics)
            # =========================
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS run_summary (
                    run_id TEXT PRIMARY KEY,
                    cycles INTEGER NOT NULL DEFAULT 0,
                    orders INTEGER NOT NULL DEFAULT 0,
                    opens INTEGER NOT NULL DEFAULT 0,
                    closes INTEGER NOT NULL DEFAULT 0,
                    errors INTEGER NOT NULL DEFAULT 0,
                    realized_pnl REAL,
                    win_trades INTEGER,
                    loss_trades INTEGER,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY(run_id) REFERENCES runs(run_id)
                )
                """
            )

            # =========================
            # Trade fills (for true PnL + win rate)
            # =========================
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS trade_fills (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT,
                    cycle_id TEXT,
                    symbol TEXT NOT NULL,
                    side TEXT NOT NULL,                 -- LONG/SHORT
                    action TEXT NOT NULL,               -- OPEN/CLOSE
                    qty REAL NOT NULL,
                    price REAL NOT NULL,
                    fee REAL,
                    realized_pnl REAL,                  -- only for CLOSE
                    timestamp_utc TEXT NOT NULL
                )
                """
            )

            # =========================
            # Indexes
            # =========================
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

    # =========================
    # Utility helpers
    # =========================
    def now_utc(self) -> datetime:
        return datetime.now(timezone.utc)
