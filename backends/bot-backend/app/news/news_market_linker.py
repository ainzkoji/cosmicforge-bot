"""
News → Market Linker.

Time-proximity matching between a news cluster and existing
market_event_reactions rows from Phase 2.

Strategy:
  1. Get mapped symbols for the cluster from news_asset_mappings.
  2. For each symbol, query market_event_reactions where
     event_time_utc falls within [first_seen_utc - BEFORE_MIN,
                                   first_seen_utc + AFTER_MIN].
  3. Return the row with the smallest absolute time delta.

This is intentionally greedy: one best-match reaction per symbol.
The returned dict is the raw market_event_reactions row augmented
with latency_minutes (+ = after news, - = before news).
"""
from __future__ import annotations

from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple

from shared_lib.persistence.db import DB

# Default linking windows (configurable via caller kwargs)
DEFAULT_BEFORE_MIN: int = 30   # look back 30 min before cluster first_seen
DEFAULT_AFTER_MIN:  int = 90   # look forward 90 min after cluster first_seen


def _parse_utc(ts: str) -> Optional[datetime]:
    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def find_market_reaction(
    db: DB,
    *,
    cluster_id: int,
    first_seen_utc: str,
    before_minutes: int = DEFAULT_BEFORE_MIN,
    after_minutes:  int = DEFAULT_AFTER_MIN,
) -> List[Dict]:
    """
    Find best-matching market_event_reactions rows for a cluster.

    Returns a list of dicts (one per symbol), each being the raw
    market_event_reactions row plus:
      - 'latency_minutes': float (positive = market reacted after news)
      - 'symbol': str

    Empty list if no match found within the time window.
    """
    first_seen = _parse_utc(first_seen_utc)
    if first_seen is None:
        return []

    window_start = (first_seen - timedelta(minutes=before_minutes)).isoformat()
    window_end   = (first_seen + timedelta(minutes=after_minutes)).isoformat()

    # Fetch mapped symbols for this cluster
    with db.connect() as conn:
        conn.row_factory = __import__("sqlite3").Row
        symbols = [
            row["symbol"]
            for row in conn.execute(
                "SELECT DISTINCT symbol FROM news_asset_mappings "
                "WHERE cluster_id = ? AND symbol IS NOT NULL",
                (cluster_id,),
            ).fetchall()
        ]

    if not symbols:
        return []

    results: List[Dict] = []

    with db.connect() as conn:
        conn.row_factory = __import__("sqlite3").Row
        for symbol in symbols:
            rows = conn.execute(
                """
                SELECT * FROM market_event_reactions
                WHERE symbol = ?
                  AND event_time_utc >= ?
                  AND event_time_utc <= ?
                ORDER BY ABS(JULIANDAY(event_time_utc) - JULIANDAY(?)) ASC
                LIMIT 1
                """,
                (symbol, window_start, window_end, first_seen.isoformat()),
            ).fetchall()

            if rows:
                row = dict(rows[0])
                event_dt = _parse_utc(row["event_time_utc"])
                latency = (
                    (event_dt - first_seen).total_seconds() / 60.0
                    if event_dt else 0.0
                )
                row["latency_minutes"] = round(latency, 2)
                results.append(row)

    return results


def get_symbols_for_cluster(db: DB, cluster_id: int) -> List[str]:
    """Helper: return distinct mapped symbols for a cluster."""
    with db.connect() as conn:
        rows = conn.execute(
            "SELECT DISTINCT symbol FROM news_asset_mappings "
            "WHERE cluster_id = ? AND symbol IS NOT NULL",
            (cluster_id,),
        ).fetchall()
    return [r[0] for r in rows]
