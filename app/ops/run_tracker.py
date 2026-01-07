from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from app.persistence.db import DB


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class RunTracker:
    def __init__(self, db: DB):
        self.db = db

    # ------------------------------------------------------------------
    # 🔒 Schema safety (NO LOGIC CHANGE)
    # ------------------------------------------------------------------
    def _ensure_run_summary_schema(self, conn) -> None:
        cols = {
            row["name"]
            for row in conn.execute("PRAGMA table_info(run_summary)").fetchall()
        }

        # keep existing behavior (add what is missing)
        if "cycles" not in cols:
            conn.execute(
                "ALTER TABLE run_summary ADD COLUMN cycles INTEGER NOT NULL DEFAULT 0"
            )
        if "orders" not in cols:
            conn.execute(
                "ALTER TABLE run_summary ADD COLUMN orders INTEGER NOT NULL DEFAULT 0"
            )
        if "opens" not in cols:
            conn.execute(
                "ALTER TABLE run_summary ADD COLUMN opens INTEGER NOT NULL DEFAULT 0"
            )
        if "closes" not in cols:
            conn.execute(
                "ALTER TABLE run_summary ADD COLUMN closes INTEGER NOT NULL DEFAULT 0"
            )
        if "errors" not in cols:
            conn.execute(
                "ALTER TABLE run_summary ADD COLUMN errors INTEGER NOT NULL DEFAULT 0"
            )
        if "realized_pnl" not in cols:
            conn.execute("ALTER TABLE run_summary ADD COLUMN realized_pnl REAL")
        if "win_trades" not in cols:
            conn.execute("ALTER TABLE run_summary ADD COLUMN win_trades INTEGER")
        if "loss_trades" not in cols:
            conn.execute("ALTER TABLE run_summary ADD COLUMN loss_trades INTEGER")
        if "updated_at" not in cols:
            conn.execute("ALTER TABLE run_summary ADD COLUMN updated_at TEXT")

        # ✅ ADD: optional summary fields some endpoints may read
        if "trades" not in cols:
            conn.execute("ALTER TABLE run_summary ADD COLUMN trades INTEGER")
        if "last_event_at" not in cols:
            conn.execute("ALTER TABLE run_summary ADD COLUMN last_event_at TEXT")

    def _ensure_runs_schema(self, conn) -> None:
        cols = {
            row["name"] for row in conn.execute("PRAGMA table_info(runs)").fetchall()
        }

        if "symbol" not in cols:
            conn.execute("ALTER TABLE runs ADD COLUMN symbol TEXT")

        if "timeframe" not in cols:
            conn.execute("ALTER TABLE runs ADD COLUMN timeframe TEXT")

        if "meta" not in cols:
            conn.execute("ALTER TABLE runs ADD COLUMN meta TEXT")

    # ------------------------------------------------------------------
    # ✅ Timestamp column fallback for events table
    # ------------------------------------------------------------------
    def _get_last_event_at(self, conn, run_id: str) -> Optional[Any]:
        """
        Different DBs name the timestamp column differently.
        Try common columns and fallback safely.
        Returns raw DB value (TEXT/ISO string/etc.) or None.
        """
        last_event_at = None
        for col in ("timestamp_utc", "created_at", "ts", "inserted_at"):
            try:
                row = conn.execute(
                    f"SELECT MAX({col}) AS m FROM events WHERE run_id = ?",
                    (run_id,),
                ).fetchone()
                if row and row["m"]:
                    last_event_at = row["m"]
                    break
            except Exception:
                continue
        return last_event_at

    # ------------------------------------------------------------------
    # Run lifecycle
    # ------------------------------------------------------------------
    def start_run(self, mode: str, interval_seconds: int, max_symbols: int) -> str:
        # ✅ Safety: prevent NOT NULL crash if config passes None
        if interval_seconds is None:
            interval_seconds = 60  # same default your _interval_to_seconds() uses
        if max_symbols is None:
            max_symbols = 10  # harmless default

        run_id = str(uuid.uuid4())

        with self.db.connect() as conn:
            self._ensure_runs_schema(conn)
            self._ensure_run_summary_schema(conn)

            # Keep your existing insert logic for runs table shape
            conn.execute(
                """
                INSERT INTO runs (run_id, started_at, stopped_at, mode, interval_seconds, max_symbols)
                VALUES (?, ?, NULL, ?, ?, ?)
                """,
                (run_id, utc_now_iso(), mode, interval_seconds, max_symbols),
            )

            # Ensure run_summary row exists (safe even if refresh_summary does upsert later)
            try:
                conn.execute(
                    """
                    INSERT INTO run_summary(
                        run_id, cycles, orders, opens, closes, errors,
                        realized_pnl, win_trades, loss_trades, updated_at
                    )
                    VALUES (?, 0, 0, 0, 0, 0, NULL, NULL, NULL, ?)
                    """,
                    (run_id, utc_now_iso()),
                )
            except Exception:
                # If run_summary already exists / constraint differs, ignore.
                # refresh_summary() will upsert anyway.
                pass

        return run_id

    def stop_run(self, run_id: str) -> None:
        with self.db.connect() as conn:
            self._ensure_runs_schema(conn)
            conn.execute(
                "UPDATE runs SET stopped_at = ? WHERE run_id = ?",
                (utc_now_iso(), run_id),
            )
        self.refresh_summary(run_id)

    # ------------------------------------------------------------------
    # refresh_summary (updated per your instructions)
    # ------------------------------------------------------------------
    def refresh_summary(self, run_id: str) -> Dict[str, Any]:
        """
        Robust: counts from events table (always exists),
        PnL/winrate from trade_fills table (if present).
        Never throws 500 because a table is missing.
        """
        with self.db.connect() as conn:
            self._ensure_run_summary_schema(conn)

            # --- cycles from events ---
            cycles_row = conn.execute(
                """
                SELECT COUNT(DISTINCT cycle_id) AS c
                FROM events
                WHERE run_id = ?
                  AND cycle_id IS NOT NULL AND cycle_id != ''
                  AND event_type IN ('CYCLE_START', 'CYCLE_END')
                """,
                (run_id,),
            ).fetchone()
            cycles = int(cycles_row["c"] or 0) if cycles_row else 0

            # --- orders/trade-ish events ---
            # NOTE: your original SQL had conflicting conditions:
            # - action LIKE 'OPEN_%' ... AND action IN ('ORDER_PLACED', ...)
            # That can never be true simultaneously for most values.
            # Below keeps intent: count execution results that represent order/trade actions.
            orders_row = conn.execute(
                """
                SELECT COUNT(*) AS c
                FROM events
                WHERE run_id = ?
                  AND event_type = 'EXECUTION_RESULT'
                  AND action IN (
                      'ORDER_PLACED',
                      'OPEN_LONG', 'OPEN_SHORT',
                      'CLOSE_LONG', 'CLOSE_SHORT',
                      'CLOSED_LONG', 'CLOSED_SHORT',
                      'CLOSED_POSITION',
                      'ADD_LONG', 'ADD_SHORT'
                  )
                """,
                (run_id,),
            ).fetchone()
            orders = int(orders_row["c"] or 0) if orders_row else 0

            # ✅ keep "trades" as "trade actions"
            trades = orders

            opens_row = conn.execute(
                """
                SELECT COUNT(*) AS c
                FROM events
                WHERE run_id = ?
                  AND (action LIKE 'OPEN_%' OR action IN ('OPEN_LONG','OPEN_SHORT'))
                """,
                (run_id,),
            ).fetchone()
            opens = int(opens_row["c"] or 0) if opens_row else 0

            closes_row = conn.execute(
                """
                SELECT COUNT(*) AS c
                FROM events
                WHERE run_id = ?
                  AND (action LIKE 'CLOSE_%' OR action IN ('CLOSE_LONG','CLOSE_SHORT'))
                """,
                (run_id,),
            ).fetchone()
            closes = int(closes_row["c"] or 0) if closes_row else 0

            errors_row = conn.execute(
                """
                SELECT COUNT(*) AS c
                FROM events
                WHERE run_id = ?
                  AND event_type IN ('ERROR', 'EXCEPTION')
                """,
                (run_id,),
            ).fetchone()
            errors = int(errors_row["c"] or 0) if errors_row else 0

            # --- last_event_at (✅ UPDATED: try column, fallback) ---
            last_event_at = self._get_last_event_at(conn, run_id)

            # --- PnL + wins/losses from trade_fills (optional table) ---
            realized_pnl = None
            win_trades = None
            loss_trades = None

            try:
                pnl_row = conn.execute(
                    """
                    SELECT
                        COALESCE(SUM(realized_pnl), 0.0) AS pnl,
                        COALESCE(SUM(CASE WHEN realized_pnl > 0 THEN 1 ELSE 0 END), 0) AS wins,
                        COALESCE(SUM(CASE WHEN realized_pnl < 0 THEN 1 ELSE 0 END), 0) AS losses
                    FROM trade_fills
                    WHERE run_id = ? AND action = 'CLOSE'
                    """,
                    (run_id,),
                ).fetchone()

                realized_pnl = float(pnl_row["pnl"])
                win_trades = int(pnl_row["wins"])
                loss_trades = int(pnl_row["losses"])
            except Exception:
                # trade_fills table not created yet or not used yet
                realized_pnl = None
                win_trades = None
                loss_trades = None

            # upsert summary row (works even if run_summary row doesn't exist)
            conn.execute(
                """
                INSERT INTO run_summary(
                    run_id, cycles, orders, opens, closes, errors,
                    trades, last_event_at,
                    realized_pnl, win_trades, loss_trades, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(run_id) DO UPDATE SET
                    cycles=excluded.cycles,
                    orders=excluded.orders,
                    opens=excluded.opens,
                    closes=excluded.closes,
                    errors=excluded.errors,
                    trades=excluded.trades,
                    last_event_at=excluded.last_event_at,
                    realized_pnl=excluded.realized_pnl,
                    win_trades=excluded.win_trades,
                    loss_trades=excluded.loss_trades,
                    updated_at=excluded.updated_at
                """,
                (
                    run_id,
                    cycles,
                    orders,
                    opens,
                    closes,
                    errors,
                    trades,
                    last_event_at,
                    realized_pnl,
                    win_trades,
                    loss_trades,
                    utc_now_iso(),
                ),
            )

            summary = conn.execute(
                "SELECT * FROM run_summary WHERE run_id=?",
                (run_id,),
            ).fetchone()

        return dict(summary) if summary else {}
