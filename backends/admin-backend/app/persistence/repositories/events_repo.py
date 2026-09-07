from __future__ import annotations

import json
from datetime import datetime, timezone, timedelta
from typing import Any

from app.persistence.db import AdminDB


def _table_exists(conn, table_name: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    ).fetchone()
    return row is not None


def _bounded_limit(limit: int | None, *, default: int, maximum: int) -> int:
    try:
        parsed = int(limit or default)
    except (TypeError, ValueError):
        parsed = default
    return max(1, min(parsed, maximum))


def get_upcoming_events(
    db: AdminDB,
    *,
    from_utc: str,
    to_utc: str,
    impact_levels: list[str] | None = None,
    limit: int = 200,
) -> list[dict[str, Any]]:
    with db.connect() as conn:
        if not _table_exists(conn, "economic_events"):
            return []
        query = """
            SELECT id, event_id, title, event_type, country_currency, impact_level,
                   scheduled_utc, actual_val, forecast_val, previous_val, source,
                   created_at, updated_at
            FROM economic_events
            WHERE scheduled_utc >= ? AND scheduled_utc <= ?
        """
        params: list[Any] = [from_utc, to_utc]
        if impact_levels:
            placeholders = ",".join("?" * len(impact_levels))
            query += f" AND impact_level IN ({placeholders})"
            params.extend(impact_levels)
        query += " ORDER BY scheduled_utc ASC LIMIT ?"
        params.append(_bounded_limit(limit, default=200, maximum=500))
        rows = conn.execute(query, params).fetchall()
    return [dict(r) for r in rows]


def get_active_blackout_windows(
    db: AdminDB,
    *,
    at_utc: str,
) -> list[dict[str, Any]]:
    with db.connect() as conn:
        if not _table_exists(conn, "event_blackout_windows"):
            return []
        if not _table_exists(conn, "economic_events"):
            return []
        rows = conn.execute(
            """
            SELECT bw.id, bw.event_id, bw.start_utc, bw.end_utc,
                   bw.affected_symbols, bw.is_global, bw.reason,
                   ev.title, ev.event_type, ev.impact_level, ev.country_currency
            FROM event_blackout_windows bw
            JOIN economic_events ev ON ev.id = bw.event_id
            WHERE bw.is_active = 1
              AND bw.start_utc <= ?
              AND bw.end_utc   >= ?
            ORDER BY ev.impact_level DESC, bw.start_utc ASC
            """,
            (at_utc, at_utc),
        ).fetchall()
    return [dict(r) for r in rows]


def get_last_sync_utc(db: AdminDB) -> str | None:
    with db.connect() as conn:
        if not _table_exists(conn, "economic_events"):
            return None
        row = conn.execute(
            "SELECT MAX(updated_at) AS last_sync FROM economic_events"
        ).fetchone()
        return row["last_sync"] if row else None


def count_active_blackouts(db: AdminDB, *, at_utc: str) -> int:
    with db.connect() as conn:
        if not _table_exists(conn, "event_blackout_windows"):
            return 0
        row = conn.execute(
            "SELECT COUNT(*) AS cnt FROM event_blackout_windows "
            "WHERE is_active = 1 AND start_utc <= ? AND end_utc >= ?",
            (at_utc, at_utc),
        ).fetchone()
        return int(row["cnt"] if row else 0)


def get_recent_reactions(
    db: AdminDB,
    *,
    days: int = 7,
    limit: int = 100,
) -> list[dict[str, Any]]:
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    with db.connect() as conn:
        if not _table_exists(conn, "market_event_reactions"):
            return []
        rows = conn.execute(
            """
            SELECT * FROM market_event_reactions
            WHERE event_time_utc >= ?
            ORDER BY event_time_utc DESC
            LIMIT ?
            """,
            (cutoff, _bounded_limit(limit, default=100, maximum=500)),
        ).fetchall()
    return [dict(r) for r in rows]


def get_reactions_for_event(
    db: AdminDB,
    *,
    event_id: str,
) -> list[dict[str, Any]]:
    with db.connect() as conn:
        if not _table_exists(conn, "market_event_reactions"):
            return []
        rows = conn.execute(
            "SELECT * FROM market_event_reactions WHERE event_id=? ORDER BY symbol ASC",
            (event_id,),
        ).fetchall()
    return [dict(r) for r in rows]


def get_single_reaction(
    db: AdminDB,
    *,
    event_id: str,
    symbol: str,
) -> dict[str, Any] | None:
    with db.connect() as conn:
        if not _table_exists(conn, "market_event_reactions"):
            return None
        row = conn.execute(
            "SELECT * FROM market_event_reactions WHERE event_id=? AND symbol=?",
            (event_id, symbol),
        ).fetchone()
    return dict(row) if row else None


def get_event_snapshots(
    db: AdminDB,
    *,
    event_id: str,
    symbol: str,
) -> list[dict[str, Any]]:
    with db.connect() as conn:
        if not _table_exists(conn, "event_market_snapshots"):
            return []
        rows = conn.execute(
            """
            SELECT id, event_id, symbol, exchange, timestamp_utc, window_label,
                   price, volume, candle_open, candle_high, candle_low,
                   candle_close, atr, spread, source, created_at
            FROM event_market_snapshots
            WHERE event_id = ? AND symbol = ?
            ORDER BY timestamp_utc ASC
            """,
            (event_id, symbol),
        ).fetchall()
    return [dict(r) for r in rows]
