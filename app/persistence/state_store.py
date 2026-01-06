# app/persistence/state_store.py

from __future__ import annotations

from dataclasses import asdict
from datetime import date
from typing import Dict, Optional

from app.persistence.db import DB, utc_now_iso
from app.runner.models import SymbolState


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
        }

    def save_daily(self, day: date, realized_pnl: float, kill: bool) -> None:
        with self.db.connect() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO daily_state(day, realized_pnl, kill, last_updated_at)
                VALUES (?,?,?,?)
                """,
                (str(day), float(realized_pnl), 1 if kill else 0, utc_now_iso()),
            )

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
