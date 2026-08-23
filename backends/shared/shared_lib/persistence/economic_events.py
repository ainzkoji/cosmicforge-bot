"""
DB helpers for economic_events and event_blackout_windows tables.

Read path is hot (called every runner cycle); write path is background-only.
All queries are plain SQLite — no ORM dependency.
"""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import List, Optional, Dict, Any

from shared_lib.persistence.db import DB


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# economic_events
# ---------------------------------------------------------------------------

def insert_event(
    db: DB,
    *,
    title: str,
    event_type: str,
    country_currency: str,
    impact_level: str,
    scheduled_utc: str,
    actual_val: Optional[float] = None,
    forecast_val: Optional[float] = None,
    previous_val: Optional[float] = None,
    source: str = "manual",
    event_id: Optional[str] = None,
) -> int:
    """Insert or replace an economic event. Returns the rowid."""
    eid = event_id or str(uuid.uuid4())
    now = utc_now_iso()
    with db.connect() as conn:
        cur = conn.execute(
            """
            INSERT INTO economic_events
                (event_id, title, event_type, country_currency, impact_level,
                 scheduled_utc, actual_val, forecast_val, previous_val, source,
                 created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(event_id) DO UPDATE SET
                title=excluded.title,
                event_type=excluded.event_type,
                impact_level=excluded.impact_level,
                scheduled_utc=excluded.scheduled_utc,
                actual_val=excluded.actual_val,
                forecast_val=excluded.forecast_val,
                previous_val=excluded.previous_val,
                updated_at=excluded.updated_at
            """,
            (eid, title, event_type, country_currency, impact_level,
             scheduled_utc, actual_val, forecast_val, previous_val, source,
             now, now),
        )
        return cur.lastrowid  # type: ignore[return-value]


def get_upcoming_events(
    db: DB,
    from_utc: str,
    to_utc: str,
    impact_levels: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    """Return events scheduled between from_utc and to_utc (ISO strings)."""
    query = """
        SELECT id, event_id, title, event_type, country_currency, impact_level,
               scheduled_utc, actual_val, forecast_val, previous_val, source,
               created_at, updated_at
        FROM economic_events
        WHERE scheduled_utc >= ? AND scheduled_utc <= ?
    """
    params: list = [from_utc, to_utc]
    if impact_levels:
        placeholders = ",".join("?" * len(impact_levels))
        query += f" AND impact_level IN ({placeholders})"
        params.extend(impact_levels)
    query += " ORDER BY scheduled_utc ASC"
    with db.connect() as conn:
        conn.row_factory = __import__("sqlite3").Row
        rows = conn.execute(query, params).fetchall()
        return [dict(r) for r in rows]


def get_last_sync_utc(db: DB) -> Optional[str]:
    """Return the most recent updated_at across all events (proxy for feed freshness)."""
    with db.connect() as conn:
        row = conn.execute(
            "SELECT MAX(updated_at) AS last_sync FROM economic_events"
        ).fetchone()
        return row[0] if row else None


# ---------------------------------------------------------------------------
# event_blackout_windows
# ---------------------------------------------------------------------------

def upsert_blackout_window(
    db: DB,
    *,
    event_db_id: int,
    start_utc: str,
    end_utc: str,
    reason: str,
    affected_symbols: Optional[List[str]] = None,
    is_global: bool = True,
) -> None:
    """Insert or update a blackout window for an event/range."""
    now = utc_now_iso()
    symbols_json = json.dumps(affected_symbols) if affected_symbols else None
    with db.connect() as conn:
        conn.execute(
            """
            INSERT INTO event_blackout_windows
                (event_id, start_utc, end_utc, affected_symbols, is_global, is_active, reason, created_at)
            VALUES (?, ?, ?, ?, ?, 1, ?, ?)
            ON CONFLICT(event_id, start_utc, end_utc) DO UPDATE SET
                affected_symbols=excluded.affected_symbols,
                is_global=excluded.is_global,
                is_active=1,
                reason=excluded.reason
            """,
            (event_db_id, start_utc, end_utc, symbols_json,
             1 if is_global else 0, reason, now),
        )


def get_active_blackout_windows(
    db: DB,
    at_utc: str,
) -> List[Dict[str, Any]]:
    """Return all windows that are currently active at the given UTC timestamp."""
    with db.connect() as conn:
        conn.row_factory = __import__("sqlite3").Row
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


def deactivate_past_windows(db: DB, before_utc: str) -> int:
    """Mark windows whose end_utc has passed as inactive. Returns count updated."""
    with db.connect() as conn:
        cur = conn.execute(
            "UPDATE event_blackout_windows SET is_active = 0 WHERE end_utc < ? AND is_active = 1",
            (before_utc,),
        )
        return cur.rowcount


def delete_old_windows(db: DB, before_utc: str) -> int:
    """Hard-delete windows older than before_utc to keep the table lean."""
    with db.connect() as conn:
        cur = conn.execute(
            "DELETE FROM event_blackout_windows WHERE end_utc < ?",
            (before_utc,),
        )
        return cur.rowcount
