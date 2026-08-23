"""
DB helpers for the Market Reaction Layer (Phase 2).

Tables managed:
  event_market_snapshots  — raw periodic OHLCV/ATR snapshots around events
  market_event_reactions  — computed reaction summaries per event+symbol
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from shared_lib.persistence.db import DB


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# event_market_snapshots
# ---------------------------------------------------------------------------

def insert_snapshot(
    db: DB,
    *,
    event_id: str,
    symbol: str,
    exchange: str,
    timestamp_utc: str,
    window_label: str,
    price: Optional[float],
    volume: Optional[float],
    candle_open: Optional[float] = None,
    candle_high: Optional[float] = None,
    candle_low: Optional[float] = None,
    candle_close: Optional[float] = None,
    atr: Optional[float] = None,
    spread: Optional[float] = None,
    bid_depth: Optional[float] = None,
    ask_depth: Optional[float] = None,
    source: str = "binance",
) -> int:
    now = _now()
    with db.connect() as conn:
        cur = conn.execute(
            """
            INSERT INTO event_market_snapshots
                (event_id, symbol, exchange, timestamp_utc, window_label,
                 price, volume, candle_open, candle_high, candle_low,
                 candle_close, atr, spread, bid_depth, ask_depth,
                 source, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (event_id, symbol, exchange, timestamp_utc, window_label,
             price, volume, candle_open, candle_high, candle_low,
             candle_close, atr, spread, bid_depth, ask_depth,
             source, now),
        )
        return cur.lastrowid  # type: ignore[return-value]


def get_snapshots(
    db: DB,
    event_id: str,
    symbol: str,
    window_labels: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    query = """
        SELECT id, event_id, symbol, exchange, timestamp_utc, window_label,
               price, volume, candle_open, candle_high, candle_low,
               candle_close, atr, spread, bid_depth, ask_depth,
               source, created_at
        FROM event_market_snapshots
        WHERE event_id = ? AND symbol = ?
    """
    params: list = [event_id, symbol]
    if window_labels:
        placeholders = ",".join("?" * len(window_labels))
        query += f" AND window_label IN ({placeholders})"
        params.extend(window_labels)
    query += " ORDER BY timestamp_utc ASC"
    with db.connect() as conn:
        conn.row_factory = __import__("sqlite3").Row
        rows = conn.execute(query, params).fetchall()
        return [dict(r) for r in rows]


def snapshot_exists(db: DB, event_id: str, symbol: str, window_label: str) -> bool:
    with db.connect() as conn:
        row = conn.execute(
            "SELECT 1 FROM event_market_snapshots "
            "WHERE event_id=? AND symbol=? AND window_label=? LIMIT 1",
            (event_id, symbol, window_label),
        ).fetchone()
        return row is not None


# ---------------------------------------------------------------------------
# market_event_reactions
# ---------------------------------------------------------------------------

def upsert_reaction(db: DB, *, event_id: str, symbol: str, **kwargs: Any) -> int:
    """
    Insert or replace a reaction row for (event_id, symbol).
    All metric fields are passed as keyword arguments matching column names.
    """
    now = _now()
    kwargs.setdefault("exchange", "")
    kwargs.setdefault("event_time_utc", now)
    kwargs.setdefault("reaction_type", "NO_REACTION")
    kwargs.setdefault("data_quality", "COMPLETE")

    cols = ["event_id", "symbol"] + list(kwargs.keys()) + ["created_at", "updated_at"]
    vals = [event_id, symbol] + list(kwargs.values()) + [now, now]
    placeholders = ",".join("?" * len(cols))
    col_str = ",".join(cols)

    update_pairs = ",".join(
        f"{c}=excluded.{c}"
        for c in cols
        if c not in ("event_id", "symbol", "created_at")
    )
    with db.connect() as conn:
        cur = conn.execute(
            f"""
            INSERT INTO market_event_reactions ({col_str})
            VALUES ({placeholders})
            ON CONFLICT(event_id, symbol) DO UPDATE SET {update_pairs}
            """,
            vals,
        )
        return cur.lastrowid  # type: ignore[return-value]


def get_reaction(
    db: DB, event_id: str, symbol: str
) -> Optional[Dict[str, Any]]:
    with db.connect() as conn:
        conn.row_factory = __import__("sqlite3").Row
        row = conn.execute(
            "SELECT * FROM market_event_reactions WHERE event_id=? AND symbol=?",
            (event_id, symbol),
        ).fetchone()
        return dict(row) if row else None


def get_reactions_for_event(
    db: DB, event_id: str
) -> List[Dict[str, Any]]:
    with db.connect() as conn:
        conn.row_factory = __import__("sqlite3").Row
        rows = conn.execute(
            "SELECT * FROM market_event_reactions WHERE event_id=? ORDER BY symbol ASC",
            (event_id,),
        ).fetchall()
        return [dict(r) for r in rows]


def get_recent_reactions(
    db: DB,
    days: int = 7,
    limit: int = 100,
) -> List[Dict[str, Any]]:
    from datetime import timedelta
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    with db.connect() as conn:
        conn.row_factory = __import__("sqlite3").Row
        rows = conn.execute(
            """
            SELECT * FROM market_event_reactions
            WHERE event_time_utc >= ?
            ORDER BY event_time_utc DESC
            LIMIT ?
            """,
            (cutoff, limit),
        ).fetchall()
        return [dict(r) for r in rows]


def get_latest_reaction_for_symbol(
    db: DB, symbol: str
) -> Optional[Dict[str, Any]]:
    """Return the most recent reaction row for a symbol (used by risk gate)."""
    with db.connect() as conn:
        conn.row_factory = __import__("sqlite3").Row
        row = conn.execute(
            """
            SELECT * FROM market_event_reactions
            WHERE symbol=?
            ORDER BY event_time_utc DESC
            LIMIT 1
            """,
            (symbol,),
        ).fetchone()
        return dict(row) if row else None
