"""
DB helpers for raw_news_items, news_clusters, and news_cluster_items.

Read path is called by the deduplication service each poll cycle.
Write path is called by the ingestion worker.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

from shared_lib.persistence.db import DB


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# raw_news_items
# ---------------------------------------------------------------------------

def insert_raw_item(
    db: DB,
    *,
    provider: str,
    title: str,
    published_utc: str,
    ingested_utc: str,
    external_id: Optional[str] = None,
    source_name: Optional[str] = None,
    source_domain: Optional[str] = None,
    source_url: Optional[str] = None,
    author: Optional[str] = None,
    body_snippet: Optional[str] = None,
    raw_payload_json: Optional[str] = None,
    latency_seconds: Optional[float] = None,
    language: str = "en",
) -> Optional[int]:
    """
    Insert a raw news item. Returns rowid on success, None if duplicate
    (provider+external_id already exists).
    """
    now = _now()
    try:
        with db.connect() as conn:
            cur = conn.execute(
                """
                INSERT OR IGNORE INTO raw_news_items
                    (provider, source_name, source_domain, source_url, external_id,
                     author, title, body_snippet, raw_payload_json,
                     published_utc, ingested_utc, latency_seconds,
                     language, is_duplicate, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?)
                """,
                (provider, source_name, source_domain, source_url, external_id,
                 author, title, body_snippet, raw_payload_json,
                 published_utc, ingested_utc, latency_seconds,
                 language, now),
            )
            return cur.lastrowid if cur.rowcount > 0 else None
    except Exception:
        return None


def mark_item_duplicate(db: DB, item_id: int) -> None:
    with db.connect() as conn:
        conn.execute(
            "UPDATE raw_news_items SET is_duplicate=1 WHERE id=?", (item_id,)
        )


def get_recent_items(
    db: DB,
    since_utc: str,
    provider: Optional[str] = None,
    limit: int = 500,
) -> List[Dict[str, Any]]:
    query = """
        SELECT id, provider, source_name, source_domain, source_url, external_id,
               author, title, body_snippet, published_utc, ingested_utc,
               latency_seconds, language, is_duplicate, created_at
        FROM raw_news_items
        WHERE published_utc >= ?
    """
    params: list = [since_utc]
    if provider:
        query += " AND provider = ?"
        params.append(provider)
    query += " ORDER BY published_utc DESC LIMIT ?"
    params.append(limit)
    with db.connect() as conn:
        conn.row_factory = __import__("sqlite3").Row
        rows = conn.execute(query, params).fetchall()
        return [dict(r) for r in rows]


def get_provider_stats(db: DB, since_utc: str) -> List[Dict[str, Any]]:
    """Count + avg latency per provider since since_utc."""
    with db.connect() as conn:
        conn.row_factory = __import__("sqlite3").Row
        rows = conn.execute(
            """
            SELECT provider,
                   COUNT(*) as item_count,
                   AVG(latency_seconds) as avg_latency_seconds,
                   MAX(ingested_utc) as last_ingested_utc
            FROM raw_news_items
            WHERE ingested_utc >= ?
            GROUP BY provider
            """,
            (since_utc,),
        ).fetchall()
        return [dict(r) for r in rows]


def purge_old_items(db: DB, before_utc: str) -> int:
    with db.connect() as conn:
        cur = conn.execute(
            "DELETE FROM raw_news_items WHERE created_at < ?", (before_utc,)
        )
        return cur.rowcount


# ---------------------------------------------------------------------------
# news_clusters
# ---------------------------------------------------------------------------

def insert_cluster(
    db: DB,
    *,
    canonical_title: str,
    first_seen_utc: str,
    reliability_score: float = 0.0,
    cluster_confidence: float = 0.0,
    summary: Optional[str] = None,
) -> int:
    now = _now()
    with db.connect() as conn:
        cur = conn.execute(
            """
            INSERT INTO news_clusters
                (canonical_title, summary, first_seen_utc, last_seen_utc,
                 source_count, provider_count,
                 highest_reliability_score, cluster_confidence,
                 is_manipulation_suspect, created_at, updated_at)
            VALUES (?, ?, ?, ?, 1, 1, ?, ?, 0, ?, ?)
            """,
            (canonical_title, summary, first_seen_utc, first_seen_utc,
             reliability_score, cluster_confidence, now, now),
        )
        return cur.lastrowid  # type: ignore[return-value]


def update_cluster(
    db: DB,
    cluster_id: int,
    *,
    last_seen_utc: str,
    source_count: int,
    provider_count: int,
    highest_reliability_score: float,
    cluster_confidence: float,
    canonical_title: Optional[str] = None,
    is_manipulation_suspect: bool = False,
    manipulation_reason: Optional[str] = None,
) -> None:
    now = _now()
    with db.connect() as conn:
        if canonical_title:
            conn.execute(
                """UPDATE news_clusters SET
                    canonical_title=?, last_seen_utc=?,
                    source_count=?, provider_count=?,
                    highest_reliability_score=?, cluster_confidence=?,
                    is_manipulation_suspect=?, manipulation_reason=?,
                    updated_at=?
                   WHERE id=?""",
                (canonical_title, last_seen_utc, source_count, provider_count,
                 highest_reliability_score, cluster_confidence,
                 1 if is_manipulation_suspect else 0, manipulation_reason,
                 now, cluster_id),
            )
        else:
            conn.execute(
                """UPDATE news_clusters SET
                    last_seen_utc=?, source_count=?, provider_count=?,
                    highest_reliability_score=?, cluster_confidence=?,
                    is_manipulation_suspect=?, manipulation_reason=?,
                    updated_at=?
                   WHERE id=?""",
                (last_seen_utc, source_count, provider_count,
                 highest_reliability_score, cluster_confidence,
                 1 if is_manipulation_suspect else 0, manipulation_reason,
                 now, cluster_id),
            )


def get_recent_clusters(
    db: DB,
    since_utc: str,
    limit: int = 100,
    include_manipulation: bool = True,
) -> List[Dict[str, Any]]:
    query = """
        SELECT id, canonical_title, summary, first_seen_utc, last_seen_utc,
               source_count, provider_count, highest_reliability_score,
               cluster_confidence, is_manipulation_suspect, manipulation_reason,
               created_at, updated_at
        FROM news_clusters
        WHERE first_seen_utc >= ?
    """
    params: list = [since_utc]
    if not include_manipulation:
        query += " AND is_manipulation_suspect = 0"
    query += " ORDER BY first_seen_utc DESC LIMIT ?"
    params.append(limit)
    with db.connect() as conn:
        conn.row_factory = __import__("sqlite3").Row
        rows = conn.execute(query, params).fetchall()
        return [dict(r) for r in rows]


def get_cluster_by_id(db: DB, cluster_id: int) -> Optional[Dict[str, Any]]:
    with db.connect() as conn:
        conn.row_factory = __import__("sqlite3").Row
        row = conn.execute(
            "SELECT * FROM news_clusters WHERE id=?", (cluster_id,)
        ).fetchone()
        return dict(row) if row else None


# ---------------------------------------------------------------------------
# news_cluster_items
# ---------------------------------------------------------------------------

def link_item_to_cluster(
    db: DB,
    cluster_id: int,
    raw_news_item_id: int,
    similarity_score: float = 1.0,
) -> None:
    now = _now()
    with db.connect() as conn:
        conn.execute(
            """INSERT OR IGNORE INTO news_cluster_items
               (cluster_id, raw_news_item_id, similarity_score, created_at)
               VALUES (?, ?, ?, ?)""",
            (cluster_id, raw_news_item_id, similarity_score, now),
        )


def get_items_for_cluster(
    db: DB, cluster_id: int
) -> List[Dict[str, Any]]:
    with db.connect() as conn:
        conn.row_factory = __import__("sqlite3").Row
        rows = conn.execute(
            """SELECT rni.*, nci.similarity_score
               FROM news_cluster_items nci
               JOIN raw_news_items rni ON rni.id = nci.raw_news_item_id
               WHERE nci.cluster_id = ?
               ORDER BY rni.published_utc DESC""",
            (cluster_id,),
        ).fetchall()
        return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# news_source_reliability
# ---------------------------------------------------------------------------

def get_reliability_for_domain(db: DB, domain: str) -> Optional[Dict[str, Any]]:
    with db.connect() as conn:
        conn.row_factory = __import__("sqlite3").Row
        row = conn.execute(
            "SELECT * FROM news_source_reliability WHERE domain=?",
            (domain,),
        ).fetchone()
        return dict(row) if row else None


def get_all_source_reliabilities(db: DB) -> List[Dict[str, Any]]:
    with db.connect() as conn:
        conn.row_factory = __import__("sqlite3").Row
        rows = conn.execute(
            "SELECT * FROM news_source_reliability ORDER BY reliability_score DESC"
        ).fetchall()
        return [dict(r) for r in rows]
