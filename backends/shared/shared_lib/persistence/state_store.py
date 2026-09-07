# app/persistence/state_store.py

from __future__ import annotations

from dataclasses import asdict
from datetime import date
from typing import Dict, Optional

from shared_lib.persistence.db import DB, utc_now_iso
from app.runner.models import SymbolState
from app.risk.state import RiskState, DailyRiskState, PeriodSnapshot, get_week_start, get_month_start


class StateStore:
    def __init__(self, db: DB, bot_instance_id: str = "default"):
        self.db = db
        self.bot_instance_id = bot_instance_id

    # ---------- DAILY ----------
    def load_daily(self, day: date) -> Optional[dict]:
        day_s = str(day)

        if self.bot_instance_id != "default":
             # Route to bot_daily_state
             with self.db.connect() as conn:
                row = conn.execute(
                    "SELECT * FROM bot_daily_state WHERE bot_instance_id = ? AND day = ?",
                    (self.bot_instance_id, day_s),
                ).fetchone()

                if not row:
                     # Create initial
                     conn.execute(
                        """
                        INSERT OR IGNORE INTO bot_daily_state(bot_instance_id, day, realized_pnl, kill, trade_count, last_updated_at)
                        VALUES (?,?,?,?,?,?)
                        """,
                        (self.bot_instance_id, day_s, 0.0, 0, 0, utc_now_iso()),
                     )
                     row = conn.execute(
                        "SELECT * FROM bot_daily_state WHERE bot_instance_id = ? AND day = ?",
                        (self.bot_instance_id, day_s),
                    ).fetchone()

                if not row:
                    return None

                row_keys = row.keys() if hasattr(row, "keys") else []
                return {
                    "day": row["day"],
                    "realized_pnl": float(row["realized_pnl"]),
                    "kill": bool(row["kill"]),
                    "trade_count": int(row["trade_count"] if "trade_count" in row_keys else 0),
                    "consecutive_losses": int(row["consecutive_losses"] if "consecutive_losses" in row_keys else 0),
                    "consec_loss_cooldown_until_ms": int(row["consec_loss_cooldown_until_ms"] if "consec_loss_cooldown_until_ms" in row_keys else 0),
                }

        # Legacy / Global (PaperRunner default)
        with self.db.connect() as conn:
            row = conn.execute(
                "SELECT * FROM daily_state WHERE day = ?",
                (day_s,),
            ).fetchone()

            # ✅ ensure row exists for today (upsert behavior)
            if not row:
                conn.execute(
                    """
                    INSERT OR IGNORE INTO daily_state(day, realized_pnl, kill, last_updated_at)
                    VALUES (?,?,?,?)
                    """,
                    (day_s, 0.0, 0, utc_now_iso()),
                )
                row = conn.execute(
                    "SELECT * FROM daily_state WHERE day = ?",
                    (day_s,),
                ).fetchone()

        if not row:
            return None

        row_keys = row.keys() if hasattr(row, "keys") else []
        return {
            "day": row["day"],
            "realized_pnl": float(row["realized_pnl"]),
            "kill": bool(row["kill"]),
            "trade_count": int(row["trade_count"] if "trade_count" in row_keys else 0),
            "consecutive_losses": int(row["consecutive_losses"] if "consecutive_losses" in row_keys else 0),
            "consec_loss_cooldown_until_ms": int(row["consec_loss_cooldown_until_ms"] if "consec_loss_cooldown_until_ms" in row_keys else 0),
        }

    def save_daily(
        self,
        day: date,
        realized_pnl: float,
        kill: bool,
        trade_count: int = 0,
        consecutive_losses: int = 0,
        consec_loss_cooldown_until_ms: int = 0,
    ) -> None:
        if self.bot_instance_id != "default":
            # Route to bot_daily_state
            with self.db.connect() as conn:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO bot_daily_state(
                        bot_instance_id, day, realized_pnl, kill, trade_count,
                        consecutive_losses, consec_loss_cooldown_until_ms, last_updated_at)
                    VALUES (?,?,?,?,?,?,?,?)
                    """,
                    (
                        self.bot_instance_id, str(day), float(realized_pnl),
                        1 if kill else 0, int(trade_count),
                        int(consecutive_losses), int(consec_loss_cooldown_until_ms),
                        utc_now_iso(),
                    ),
                )
            return

        # Legacy / Global
        with self.db.connect() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO daily_state(
                    day, realized_pnl, kill, trade_count,
                    consecutive_losses, consec_loss_cooldown_until_ms, last_updated_at)
                VALUES (?,?,?,?,?,?,?)
                """,
                (
                    str(day), float(realized_pnl), 1 if kill else 0, int(trade_count),
                    int(consecutive_losses), int(consec_loss_cooldown_until_ms),
                    utc_now_iso(),
                ),
            )

    def get_daily_states(self, start_date: date, end_date: date) -> list[dict]:
        with self.db.connect() as conn:
            rows = conn.execute(
                """
                SELECT day, realized_pnl, kill 
                FROM daily_state 
                WHERE day >= ? AND day <= ? 
                ORDER BY day ASC
                """,
                (str(start_date), str(end_date)),
            ).fetchall()

        return [
            {
                "day": row["day"],
                "realized_pnl": float(row["realized_pnl"]),
                "kill": bool(row["kill"]),
            }
            for row in rows
        ]

    # ---------- SNAPSHOTS ----------
    def load_weekly_snapshot(self, week_start: date) -> Optional[PeriodSnapshot]:
        with self.db.connect() as conn:
            row = conn.execute(
                """
                SELECT * FROM weekly_snapshots
                WHERE week_start_date = ?
                  AND (bot_instance_id = ? OR (bot_instance_id IS NULL AND ? = 'default'))
                """,
                (str(week_start), self.bot_instance_id, self.bot_instance_id),
            ).fetchone()

        if not row:
            return None

        return PeriodSnapshot(
            start_date=week_start,
            start_equity=float(row["start_equity"]),
            peak_equity=float(row["peak_equity"]),
            low_equity=float(row["low_equity"]),
        )

    def save_weekly_snapshot(self, snap: PeriodSnapshot) -> None:
        with self.db.connect() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO weekly_snapshots
                    (bot_instance_id, week_start_date, start_equity, peak_equity, low_equity, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    self.bot_instance_id,
                    str(snap.start_date),
                    snap.start_equity,
                    snap.peak_equity,
                    snap.low_equity,
                    utc_now_iso(),
                ),
            )

    def load_monthly_snapshot(self, month_start: date) -> Optional[PeriodSnapshot]:
        with self.db.connect() as conn:
            row = conn.execute(
                """
                SELECT * FROM monthly_snapshots
                WHERE month_start_date = ?
                  AND (bot_instance_id = ? OR (bot_instance_id IS NULL AND ? = 'default'))
                """,
                (str(month_start), self.bot_instance_id, self.bot_instance_id),
            ).fetchone()

        if not row:
            return None

        return PeriodSnapshot(
            start_date=month_start,
            start_equity=float(row["start_equity"]),
            peak_equity=float(row["peak_equity"]),
            low_equity=float(row["low_equity"]),
        )

    def save_monthly_snapshot(self, snap: PeriodSnapshot) -> None:
        with self.db.connect() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO monthly_snapshots
                    (bot_instance_id, month_start_date, start_equity, peak_equity, low_equity, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    self.bot_instance_id,
                    str(snap.start_date),
                    snap.start_equity,
                    snap.peak_equity,
                    snap.low_equity,
                    utc_now_iso(),
                ),
            )

    # ---------- AGGREGATE RISK STATE ----------
    def load_risk_state(self, day: date) -> RiskState:
        # 1. Daily
        d_dict = self.load_daily(day)
        if not d_dict:
             # Should practically not happen if we init daily properly, but for safety:
             d_state = DailyRiskState(day=day)
        else:
             d_state = DailyRiskState(
                 day=day, 
                 realized_pnl=d_dict["realized_pnl"], 
                 kill=d_dict["kill"],
                 trade_count=d_dict.get("trade_count", 0)
             )
        
        # 2. Weekly
        ws = get_week_start(day)
        w_snap = self.load_weekly_snapshot(ws)
        
        # 3. Monthly
        ms = get_month_start(day)
        m_snap = self.load_monthly_snapshot(ms)
        
        return RiskState(daily=d_state, weekly=w_snap, monthly=m_snap)


    # ---------- SYMBOLS (ROBUST) ----------
    def load_symbols(self) -> Dict[str, SymbolState]:
        """
        Returns typed SymbolState objects (not raw dicts).
        This makes startup reconciliation + debugging consistent.
        """
        # ✅ Route to instance-specific load if needed
        if self.bot_instance_id != "default":
            return self.load_instance_symbols(self.bot_instance_id)

        out: Dict[str, SymbolState] = {}

        with self.db.connect() as conn:
            rows = conn.execute("SELECT * FROM symbol_state").fetchall()

        for r in rows:
            sym = (r["symbol"] or "").upper()
            if not sym:
                continue

            out[sym] = SymbolState(
                position=r["position"],
                entry_price=r["entry_price"],
                last_signal=r["last_signal"],
                last_action=r["last_action"],
                last_checked_ms=int(r["last_checked_ms"] or 0),
                adds=int(r["adds"] or 0),
                last_trade_ms=int(r["last_trade_ms"] or 0),
                last_stop_ms=int(r["last_stop_ms"] or 0),
                pending_open=r["pending_open"],
                entry_qty=float(r["entry_qty"] or 0.0),
                last_user_trade_id=int(r["last_user_trade_id"] or 0),
                reentry_confirm_signal=(r["reentry_confirm_signal"] or "NONE"),
                reentry_confirm_count=int(r["reentry_confirm_count"] or 0),
            )

        return out

    def save_symbol(self, symbol: str, st: SymbolState) -> None:
        """
        UPSERT symbol state (safe across restarts).
        """
        # ✅ Route to instance-specific save if needed
        if self.bot_instance_id != "default":
            return self.save_instance_symbol(self.bot_instance_id, symbol, st)

        if not st.pending_open:
            st.pending_open = "NONE"

        d = asdict(st)
        symbol = symbol.upper()

        with self.db.connect() as conn:
            conn.execute(
                """
                INSERT INTO symbol_state(
                    symbol, position, entry_price, last_signal, last_action,
                    last_checked_ms, adds, last_trade_ms, last_stop_ms, pending_open, entry_qty,
                    last_user_trade_id, reentry_confirm_signal, reentry_confirm_count, updated_at
                )
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(symbol) DO UPDATE SET
                    position=excluded.position,
                    entry_price=excluded.entry_price,
                    last_signal=excluded.last_signal,
                    last_action=excluded.last_action,
                    last_checked_ms=excluded.last_checked_ms,
                    adds=excluded.adds,
                    last_trade_ms=excluded.last_trade_ms,
                    last_stop_ms=excluded.last_stop_ms,
                    pending_open=excluded.pending_open,
                    entry_qty=excluded.entry_qty,
                    last_user_trade_id=excluded.last_user_trade_id,
                    reentry_confirm_signal=excluded.reentry_confirm_signal,
                    reentry_confirm_count=excluded.reentry_confirm_count,
                    updated_at=excluded.updated_at
                """,
                (
                    symbol,
                    d.get("position", "NONE"),
                    d.get("entry_price", None),
                    d.get("last_signal", "HOLD"),
                    d.get("last_action", "NOOP"),
                    int(d.get("last_checked_ms", 0)),
                    int(d.get("adds", 0)),
                    int(d.get("last_trade_ms", 0)),
                    int(d.get("last_stop_ms", 0)),
                    d.get("pending_open", "NONE"),
                    float(d.get("entry_qty", 0.0)),
                    int(d.get("last_user_trade_id", 0)),
                    d.get("reentry_confirm_signal", "NONE"),
                    int(d.get("reentry_confirm_count", 0)),
                    utc_now_iso(),
                ),
            )



    # ---------- BOT INSTANCE SYMBOLS (MULTI-USER) ----------
    def load_instance_symbols(self, bot_instance_id: str) -> Dict[str, SymbolState]:
        """
        Returns typed SymbolState objects for a specific Bot Instance.
        """
        out: Dict[str, SymbolState] = {}

        with self.db.connect() as conn:
            rows = conn.execute(
                "SELECT * FROM bot_symbol_state WHERE bot_instance_id = ?",
                (bot_instance_id,)
            ).fetchall()

        for r in rows:
            sym = (r["symbol"] or "").upper()
            if not sym:
                continue

            # Safely read position_id — column added by Phase 4 migration
            try:
                _pid = r["position_id"]
            except (IndexError, KeyError):
                _pid = None

            out[sym] = SymbolState(
                position=r["position"],
                entry_price=r["entry_price"],
                last_signal=r["last_signal"],
                last_action=r["last_action"],
                last_checked_ms=int(r["last_checked_ms"] or 0),
                adds=int(r["adds"] or 0),
                last_trade_ms=int(r["last_trade_ms"] or 0),
                last_stop_ms=int(r["last_stop_ms"] or 0),
                pending_open=r["pending_open"],
                entry_qty=float(r["entry_qty"] or 0.0),
                last_user_trade_id=int(r["last_user_trade_id"] or 0),
                reentry_confirm_signal=(r["reentry_confirm_signal"] or "NONE"),
                reentry_confirm_count=int(r["reentry_confirm_count"] or 0),
                position_id=_pid,
            )

        return out

    # ---------- POSITION LIFECYCLE STATE ----------

    def save_lifecycle_state(
        self,
        symbol: str,
        lifecycle: dict,
        bot_instance_id: str | None = None,
    ) -> None:
        """
        Persist PositionManager lifecycle state for (bot_instance_id, symbol).

        lifecycle dict keys (all optional, uses defaults when absent):
            phase, original_stop, current_stop, original_tp1, original_tp2,
            is_break_even, tp1_hit, trailing_active,
            highest_since_entry, lowest_since_entry,
            entry_qty_remaining, sl_order_id, tp_order_id,
            position_id, exchange_position_active, reconciliation_status,
            reconciliation_reason, last_reconciled_at
        """
        bid = bot_instance_id or self.bot_instance_id
        symbol = symbol.upper()

        with self.db.connect() as conn:
            conn.execute(
                """
                INSERT INTO position_lifecycle_state(
                    bot_instance_id, symbol, position_id, phase,
                    original_stop, current_stop, original_tp1, original_tp2,
                    is_break_even, tp1_hit, trailing_active,
                    highest_since_entry, lowest_since_entry,
                    entry_qty_remaining, sl_order_id, tp_order_id,
                    exchange_position_active, reconciliation_status, reconciliation_reason, last_reconciled_at,
                    updated_at
                )
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(bot_instance_id, symbol) DO UPDATE SET
                    position_id=COALESCE(excluded.position_id, position_lifecycle_state.position_id),
                    phase=excluded.phase,
                    original_stop=excluded.original_stop,
                    current_stop=excluded.current_stop,
                    original_tp1=excluded.original_tp1,
                    original_tp2=excluded.original_tp2,
                    is_break_even=excluded.is_break_even,
                    tp1_hit=excluded.tp1_hit,
                    trailing_active=excluded.trailing_active,
                    highest_since_entry=excluded.highest_since_entry,
                    lowest_since_entry=excluded.lowest_since_entry,
                    entry_qty_remaining=excluded.entry_qty_remaining,
                    sl_order_id=COALESCE(excluded.sl_order_id, position_lifecycle_state.sl_order_id),
                    tp_order_id=COALESCE(excluded.tp_order_id, position_lifecycle_state.tp_order_id),
                    exchange_position_active=excluded.exchange_position_active,
                    reconciliation_status=excluded.reconciliation_status,
                    reconciliation_reason=excluded.reconciliation_reason,
                    last_reconciled_at=excluded.last_reconciled_at,
                    updated_at=excluded.updated_at
                """,
                (
                    bid,
                    symbol,
                    lifecycle.get("position_id"),
                    lifecycle.get("phase", "SEEKING_TP1"),
                    lifecycle.get("original_stop"),
                    lifecycle.get("current_stop"),
                    lifecycle.get("original_tp1"),
                    lifecycle.get("original_tp2"),
                    1 if lifecycle.get("is_break_even") else 0,
                    1 if lifecycle.get("tp1_hit") else 0,
                    1 if lifecycle.get("trailing_active") else 0,
                    lifecycle.get("highest_since_entry"),
                    lifecycle.get("lowest_since_entry"),
                    lifecycle.get("entry_qty_remaining"),
                    lifecycle.get("sl_order_id"),
                    lifecycle.get("tp_order_id"),
                    lifecycle.get("exchange_position_active"),
                    lifecycle.get("reconciliation_status"),
                    lifecycle.get("reconciliation_reason"),
                    lifecycle.get("last_reconciled_at"),
                    utc_now_iso(),
                ),
            )

    def load_lifecycle_state(
        self,
        symbol: str,
        bot_instance_id: str | None = None,
    ) -> dict | None:
        """
        Load persisted PositionManager lifecycle state for one symbol.
        Returns None if no record exists.
        """
        bid = bot_instance_id or self.bot_instance_id
        symbol = symbol.upper()

        with self.db.connect() as conn:
            row = conn.execute(
                "SELECT * FROM position_lifecycle_state "
                "WHERE bot_instance_id = ? AND symbol = ?",
                (bid, symbol),
            ).fetchone()

        if not row:
            return None

        return {
            "position_id": row["position_id"],
            "phase": row["phase"] or "SEEKING_TP1",
            "original_stop": row["original_stop"],
            "current_stop": row["current_stop"],
            "original_tp1": row["original_tp1"],
            "original_tp2": row["original_tp2"],
            "is_break_even": bool(row["is_break_even"]),
            "tp1_hit": bool(row["tp1_hit"]),
            "trailing_active": bool(row["trailing_active"]),
            "highest_since_entry": row["highest_since_entry"],
            "lowest_since_entry": row["lowest_since_entry"],
            "entry_qty_remaining": row["entry_qty_remaining"],
            "sl_order_id": row["sl_order_id"],
            "tp_order_id": row["tp_order_id"],
            "exchange_position_active": row["exchange_position_active"],
            "reconciliation_status": row["reconciliation_status"],
            "reconciliation_reason": row["reconciliation_reason"],
            "last_reconciled_at": row["last_reconciled_at"],
        }

    def load_all_lifecycle_states(
        self,
        bot_instance_id: str | None = None,
    ) -> dict:
        """
        Bulk-load all persisted lifecycle states for a bot instance.
        Returns Dict[symbol, lifecycle_dict].
        """
        bid = bot_instance_id or self.bot_instance_id

        with self.db.connect() as conn:
            rows = conn.execute(
                "SELECT * FROM position_lifecycle_state WHERE bot_instance_id = ?",
                (bid,),
            ).fetchall()

        out = {}
        for row in rows:
            sym = (row["symbol"] or "").upper()
            if not sym:
                continue
            out[sym] = {
                "position_id": row["position_id"],
                "phase": row["phase"] or "SEEKING_TP1",
                "original_stop": row["original_stop"],
                "current_stop": row["current_stop"],
                "original_tp1": row["original_tp1"],
                "original_tp2": row["original_tp2"],
                "is_break_even": bool(row["is_break_even"]),
                "tp1_hit": bool(row["tp1_hit"]),
                "trailing_active": bool(row["trailing_active"]),
                "highest_since_entry": row["highest_since_entry"],
                "lowest_since_entry": row["lowest_since_entry"],
                "entry_qty_remaining": row["entry_qty_remaining"],
                "sl_order_id": row["sl_order_id"],
                "tp_order_id": row["tp_order_id"],
                "exchange_position_active": row["exchange_position_active"],
                "reconciliation_status": row["reconciliation_status"],
                "reconciliation_reason": row["reconciliation_reason"],
                "last_reconciled_at": row["last_reconciled_at"],
            }
        return out

    def delete_lifecycle_state(
        self,
        symbol: str,
        bot_instance_id: str | None = None,
    ) -> None:
        """
        Delete lifecycle state for a symbol (called when position closes flat).
        """
        bid = bot_instance_id or self.bot_instance_id
        symbol = symbol.upper()

        with self.db.connect() as conn:
            conn.execute(
                "DELETE FROM position_lifecycle_state "
                "WHERE bot_instance_id = ? AND symbol = ?",
                (bid, symbol),
            )

    def mark_lifecycle_flat(
        self,
        symbol: str,
        reason: str,
        bot_instance_id: str | None = None,
    ) -> None:
        """
        Preserve lifecycle history but make the row explicitly inactive.

        Promotion/readiness gates treat FLAT rows as safe; keeping the row makes
        reconciliation decisions auditable instead of silently deleting evidence.
        """
        bid = bot_instance_id or self.bot_instance_id
        symbol = symbol.upper()
        now = utc_now_iso()

        with self.db.connect() as conn:
            conn.execute(
                """
                UPDATE position_lifecycle_state
                SET phase = 'FLAT',
                    exchange_position_active = 0,
                    reconciliation_status = 'FLAT',
                    reconciliation_reason = ?,
                    last_reconciled_at = ?,
                    updated_at = ?
                WHERE bot_instance_id = ? AND symbol = ?
                """,
                (reason, now, now, bid, symbol),
            )

    def update_lifecycle_protection_ids(
        self,
        symbol: str,
        *,
        sl_order_id: str | None = None,
        tp_order_id: str | None = None,
        status: str = "PROTECTED",
        reason: str | None = None,
        bot_instance_id: str | None = None,
    ) -> bool:
        """
        Backfill or refresh exchange protection order IDs on an existing row.
        Returns False when there is no row to update.
        """
        bid = bot_instance_id or self.bot_instance_id
        symbol = symbol.upper()
        now = utc_now_iso()

        assignments = [
            "exchange_position_active = 1",
            "reconciliation_status = ?",
            "reconciliation_reason = ?",
            "last_reconciled_at = ?",
            "updated_at = ?",
        ]
        params: list[object] = [status, reason, now, now]
        if sl_order_id:
            assignments.insert(0, "sl_order_id = ?")
            params.insert(0, str(sl_order_id))
        if tp_order_id:
            insert_at = 1 if sl_order_id else 0
            assignments.insert(insert_at, "tp_order_id = ?")
            params.insert(insert_at, str(tp_order_id))

        params.extend([bid, symbol])
        with self.db.connect() as conn:
            cur = conn.execute(
                f"""
                UPDATE position_lifecycle_state
                SET {", ".join(assignments)}
                WHERE bot_instance_id = ? AND symbol = ?
                """,
                tuple(params),
            )
            return cur.rowcount > 0

    def reconcile_lifecycle_from_fills(
        self,
        bot_instance_id: str | None = None,
    ) -> list[dict]:
        """
        Mark active lifecycle rows FLAT when persisted fills already prove closure.

        This is intentionally DB-only: it never infers exchange flatness unless a
        CLOSE/ALREADY_FLAT record is already persisted for the same position or
        latest symbol lifecycle.
        """
        bid = bot_instance_id or self.bot_instance_id
        now = utc_now_iso()
        updated: list[dict] = []

        with self.db.connect() as conn:
            trade_cols = {
                row["name"]
                for row in conn.execute("PRAGMA table_info(trade_fills)").fetchall()
            }
            time_order_cols = [
                col for col in ("timestamp_utc", "ts", "created_at") if col in trade_cols
            ]
            if len(time_order_cols) > 1:
                time_order = "COALESCE(" + ", ".join(time_order_cols) + ") DESC, id DESC"
            elif len(time_order_cols) == 1:
                time_order = f"{time_order_cols[0]} DESC, id DESC"
            else:
                time_order = "id DESC"
            rows = conn.execute(
                """
                SELECT *
                FROM position_lifecycle_state
                WHERE bot_instance_id = ?
                  AND COALESCE(phase, '') NOT IN ('FLAT','CLOSED','DONE','CANCELLED','CANCELED')
                """,
                (bid,),
            ).fetchall()

            for row in rows:
                symbol = (row["symbol"] or "").upper()
                position_id = row["position_id"]
                reason = None

                if position_id:
                    close = conn.execute(
                        f"""
                        SELECT id, exit_reason, broker_response
                        FROM trade_fills
                        WHERE position_id = ? AND action = 'CLOSE'
                        ORDER BY {time_order}
                        LIMIT 1
                        """,
                        (position_id,),
                    ).fetchone()
                    if close:
                        reason = f"DB_CLOSE_FILL:{close['id']}"

                if not reason:
                    latest_open = conn.execute(
                        f"""
                        SELECT position_id
                        FROM trade_fills
                        WHERE symbol = ? AND action = 'OPEN'
                        ORDER BY {time_order}
                        LIMIT 1
                        """,
                        (symbol,),
                    ).fetchone()
                    if latest_open and latest_open["position_id"]:
                        close = conn.execute(
                            f"""
                            SELECT id, exit_reason, broker_response
                            FROM trade_fills
                            WHERE position_id = ? AND action = 'CLOSE'
                            ORDER BY {time_order}
                            LIMIT 1
                            """,
                            (latest_open["position_id"],),
                        ).fetchone()
                        if close:
                            reason = f"LATEST_POSITION_CLOSED:{close['id']}"

                if not reason:
                    already_flat = conn.execute(
                        f"""
                        SELECT id
                        FROM trade_fills
                        WHERE symbol = ?
                          AND action = 'CLOSE'
                          AND (
                            UPPER(COALESCE(exit_reason, '')) = 'ALREADY_FLAT'
                            OR UPPER(COALESCE(broker_response, '')) LIKE '%ALREADY_FLAT%'
                          )
                        ORDER BY {time_order}
                        LIMIT 1
                        """,
                        (symbol,),
                    ).fetchone()
                    if already_flat:
                        reason = f"DB_ALREADY_FLAT:{already_flat['id']}"

                if not reason:
                    continue

                conn.execute(
                    """
                    UPDATE position_lifecycle_state
                    SET phase = 'FLAT',
                        exchange_position_active = 0,
                        reconciliation_status = 'FLAT',
                        reconciliation_reason = ?,
                        last_reconciled_at = ?,
                        updated_at = ?
                    WHERE bot_instance_id = ? AND symbol = ?
                    """,
                    (reason, now, now, bid, symbol),
                )
                updated.append({"symbol": symbol, "reason": reason})

        return updated

    # ---------- BOT INSTANCE SYMBOLS (MULTI-USER) ----------

    def save_instance_symbol(self, bot_instance_id: str, symbol: str, st: SymbolState) -> None:
        """
        UPSERT bot instance symbol state.
        """
        if not st.pending_open:
            st.pending_open = "NONE"

        d = asdict(st)
        symbol = symbol.upper()

        with self.db.connect() as conn:
            conn.execute(
                """
                INSERT INTO bot_symbol_state(
                    bot_instance_id, symbol, position, entry_price, last_signal, last_action,
                    last_checked_ms, adds, last_trade_ms, last_stop_ms, pending_open, entry_qty,
                    last_user_trade_id, reentry_confirm_signal, reentry_confirm_count,
                    position_id, updated_at
                )
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(bot_instance_id, symbol) DO UPDATE SET
                    position=excluded.position,
                    entry_price=excluded.entry_price,
                    last_signal=excluded.last_signal,
                    last_action=excluded.last_action,
                    last_checked_ms=excluded.last_checked_ms,
                    adds=excluded.adds,
                    last_trade_ms=excluded.last_trade_ms,
                    last_stop_ms=excluded.last_stop_ms,
                    pending_open=excluded.pending_open,
                    entry_qty=excluded.entry_qty,
                    last_user_trade_id=excluded.last_user_trade_id,
                    reentry_confirm_signal=excluded.reentry_confirm_signal,
                    reentry_confirm_count=excluded.reentry_confirm_count,
                    position_id=excluded.position_id,
                    updated_at=excluded.updated_at
                """,
                (
                    bot_instance_id,
                    symbol,
                    d.get("position", "NONE"),
                    d.get("entry_price", None),
                    d.get("last_signal", "HOLD"),
                    d.get("last_action", "NOOP"),
                    int(d.get("last_checked_ms", 0)),
                    int(d.get("adds", 0)),
                    int(d.get("last_trade_ms", 0)),
                    int(d.get("last_stop_ms", 0)),
                    d.get("pending_open", "NONE"),
                    float(d.get("entry_qty", 0.0)),
                    int(d.get("last_user_trade_id", 0)),
                    d.get("reentry_confirm_signal", "NONE"),
                    int(d.get("reentry_confirm_count", 0)),
                    d.get("position_id", None),
                    utc_now_iso(),
                ),
            )
