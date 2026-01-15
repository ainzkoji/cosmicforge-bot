# app/persistence/state_store.py

from __future__ import annotations

from dataclasses import asdict
from datetime import date
from typing import Dict, Optional

from app.persistence.db import DB, utc_now_iso
from app.runner.models import SymbolState
from app.risk.state import RiskState, DailyRiskState, PeriodSnapshot, get_week_start, get_month_start


class StateStore:
    def __init__(self, db: DB):
        self.db = db

    # ---------- DAILY ----------
    def load_daily(self, day: date) -> Optional[dict]:
        day_s = str(day)

        with self.db.connect() as conn:
            row = conn.execute(
                "SELECT day, realized_pnl, kill FROM daily_state WHERE day = ?",
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
                    "SELECT day, realized_pnl, kill FROM daily_state WHERE day = ?",
                    (day_s,),
                ).fetchone()

        if not row:
            return None

        return {
            "day": row["day"],
            "realized_pnl": float(row["realized_pnl"]),
            "kill": bool(row["kill"]),
            "trade_count": int(row["trade_count"] if "trade_count" in row.keys() else 0),
        }

    def save_daily(self, day: date, realized_pnl: float, kill: bool, trade_count: int = 0) -> None:
        with self.db.connect() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO daily_state(day, realized_pnl, kill, trade_count, last_updated_at)
                VALUES (?,?,?,?,?)
                """,
                (str(day), float(realized_pnl), 1 if kill else 0, int(trade_count), utc_now_iso()),
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
                "SELECT * FROM weekly_snapshots WHERE week_start_date = ?",
                (str(week_start),),
            ).fetchone()
            
        if not row:
            return None
            
        return PeriodSnapshot(
            start_date=week_start, # we know it matches
            start_equity=float(row["start_equity"]),
            peak_equity=float(row["peak_equity"]),
            low_equity=float(row["low_equity"]),
        )

    def save_weekly_snapshot(self, snap: PeriodSnapshot) -> None:
        with self.db.connect() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO weekly_snapshots(week_start_date, start_equity, peak_equity, low_equity, updated_at)
                VALUES (?,?,?,?,?)
                """,
                (str(snap.start_date), snap.start_equity, snap.peak_equity, snap.low_equity, utc_now_iso()),
            )

    def load_monthly_snapshot(self, month_start: date) -> Optional[PeriodSnapshot]:
         with self.db.connect() as conn:
            row = conn.execute(
                "SELECT * FROM monthly_snapshots WHERE month_start_date = ?",
                (str(month_start),),
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
                INSERT OR REPLACE INTO monthly_snapshots(month_start_date, start_equity, peak_equity, low_equity, updated_at)
                VALUES (?,?,?,?,?)
                """,
                (str(snap.start_date), snap.start_equity, snap.peak_equity, snap.low_equity, utc_now_iso()),
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
                    last_user_trade_id, updated_at
                )
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
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
                    utc_now_iso(),
                ),
            )
