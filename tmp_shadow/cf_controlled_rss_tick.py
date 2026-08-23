from __future__ import annotations

import json
import os
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
BOT_BACKEND = ROOT / "backends" / "bot-backend"
SHARED = ROOT / "backends" / "shared"
DB_PATH = ROOT / "backends" / "shared" / "shared_lib" / "persistence" / "cosmicforge.db"

sys.path.insert(0, str(BOT_BACKEND))
sys.path.insert(0, str(SHARED))

os.environ.setdefault("DATABASE_URL", f"sqlite:///{DB_PATH}")
os.environ["NEWS_INTELLIGENCE_ENABLED"] = "true"
os.environ["NEWS_REAL_SOURCE_INGESTION_ENABLED"] = "true"
os.environ["NEWS_RSS_INGESTION_ENABLED"] = "true"
os.environ["NEWS_SHADOW_MODE"] = "true"
os.environ["NEWS_TRADING_ENABLED"] = "false"
os.environ["NEWS_SIGNAL_CAN_BLOCK_TRADES"] = "false"
os.environ["REAL_TIME_NEWS_ENABLED"] = "false"
os.environ["REAL_TIME_NEWS_SHADOW_MODE"] = "true"
os.environ["GENERIC_NEWS_API_ENABLED"] = "false"
os.environ["NEWS_API_INGESTION_ENABLED"] = "false"
os.environ["NEWS_MANUAL_IMPORT_ENABLED"] = "false"
os.environ["EVENT_NEWS_MODE_CONTROLLER_ENABLED"] = "false"
os.environ["EVENT_NEWS_AUTO_PROMOTION_ENABLED"] = "false"
os.environ["EVENT_NEWS_AUTO_DEMOTION_ENABLED"] = "false"

from shared_lib.persistence.db import DB  # noqa: E402
from app.news.news_provider_health import ProviderHealthService  # noqa: E402
from app.news.rss_provider_client import RSSProviderClient  # noqa: E402
from app.workers.news_ingestion_worker import NewsIngestionWorker  # noqa: E402


ALLOWED_SOURCE_IDS = (
    "coindesk.com",
    "cointelegraph.com",
    "decrypt.co",
    "bitcoinmagazine.com",
)

class OneShotRegistry:
    def __init__(self, db: DB) -> None:
        self._db = db

    def mark_fetched(self, source_id: str, last_fetch_utc: str, error: str | None = None) -> None:
        now = datetime.now(timezone.utc).isoformat()
        with self._db.connect() as conn:
            if error:
                conn.execute(
                    "UPDATE news_sources SET last_fetch_utc=?, last_error=?, updated_at=? WHERE id=?",
                    (last_fetch_utc, error, now, source_id),
                )
            else:
                conn.execute(
                    "UPDATE news_sources SET last_fetch_utc=?, last_success_utc=?, last_error=NULL, updated_at=? WHERE id=?",
                    (last_fetch_utc, last_fetch_utc, now, source_id),
                )

def connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    return conn


def table_exists(conn: sqlite3.Connection, table: str) -> bool:
    return (
        conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (table,),
        ).fetchone()
        is not None
    )


def count_table(conn: sqlite3.Connection, table: str) -> int | None:
    if not table_exists(conn, table):
        return None
    return int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])


def count_since(conn: sqlite3.Connection, table: str, since_utc: str) -> int | None:
    if not table_exists(conn, table):
        return None
    cols = {r["name"] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    for col in ("created_at", "ingested_utc", "last_checked_utc", "timestamp_utc", "ts_utc"):
        if col in cols:
            return int(conn.execute(f"SELECT COUNT(*) FROM {table} WHERE {col} >= ?", (since_utc,)).fetchone()[0])
    return None


def duplicate_narrative_rows(conn: sqlite3.Connection) -> dict:
    for table in ("news_cluster_narratives", "news_narratives"):
        if not table_exists(conn, table):
            continue
        cols = {r["name"] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}
        cluster_col = "cluster_id" if "cluster_id" in cols else "news_cluster_id" if "news_cluster_id" in cols else None
        type_col = "narrative_type" if "narrative_type" in cols else "type" if "type" in cols else None
        if cluster_col and type_col:
            rows = conn.execute(
                f"""
                SELECT {cluster_col} AS cluster_id, {type_col} AS narrative_type, COUNT(*) AS row_count
                FROM {table}
                GROUP BY {cluster_col}, {type_col}
                HAVING COUNT(*) > 1
                ORDER BY row_count DESC, cluster_id, narrative_type
                """
            ).fetchall()
            return {
                "table": table,
                "count": len(rows),
                "rows": [dict(r) for r in rows[:20]],
            }
    return {"table": None, "count": None, "rows": []}


def unsafe_news_signals(conn: sqlite3.Connection) -> dict:
    if not table_exists(conn, "news_intelligence_signals"):
        return {"count": None, "rows": []}
    cols = {r["name"] for r in conn.execute("PRAGMA table_info(news_intelligence_signals)").fetchall()}
    if "shadow_only" not in cols or "should_affect_trading" not in cols:
        return {"count": None, "rows": []}
    selected = [
        c
        for c in ("id", "cluster_id", "signal_type", "symbol", "shadow_only", "should_affect_trading", "created_at")
        if c in cols
    ]
    rows = conn.execute(
        f"""
        SELECT {", ".join(selected)}
        FROM news_intelligence_signals
        WHERE shadow_only != 1 OR should_affect_trading != 0
        ORDER BY {("created_at" if "created_at" in cols else "id")} DESC
        LIMIT 20
        """
    ).fetchall()
    count = conn.execute(
        "SELECT COUNT(*) FROM news_intelligence_signals WHERE shadow_only != 1 OR should_affect_trading != 0"
    ).fetchone()[0]
    return {"count": int(count), "rows": [dict(r) for r in rows]}


def provider_health(conn: sqlite3.Connection) -> list[dict]:
    if not table_exists(conn, "news_provider_health"):
        return []
    placeholders = ",".join("?" for _ in ALLOWED_SOURCE_IDS)
    return [
        dict(r)
        for r in conn.execute(
            f"""
            WITH latest AS (
              SELECT source_id, MAX(id) AS latest_id
              FROM news_provider_health
              WHERE source_id IN ({placeholders})
              GROUP BY source_id
            )
            SELECT h.*
            FROM news_provider_health h
            JOIN latest l ON l.latest_id = h.id
            ORDER BY h.source_id
            """,
            ALLOWED_SOURCE_IDS,
        ).fetchall()
    ]


def build_allowed_clients(conn: sqlite3.Connection) -> list[RSSProviderClient]:
    placeholders = ",".join("?" for _ in ALLOWED_SOURCE_IDS)
    rows = conn.execute(
        f"""
        SELECT id, source_name, category, rss_url, fetch_interval_seconds
        FROM news_sources
        WHERE id IN ({placeholders})
          AND source_type = 'RSS'
          AND is_enabled = 1
          AND rss_url IS NOT NULL
          AND rss_url != ''
        ORDER BY id
        """,
        ALLOWED_SOURCE_IDS,
    ).fetchall()
    return [
        RSSProviderClient(
            source_id=r["id"],
            source_name=r["source_name"],
            rss_url=r["rss_url"],
            category=r["category"] or "CRYPTO",
            timeout=15,
            max_items=50,
            fetch_interval_seconds=r["fetch_interval_seconds"] or 300,
        )
        for r in rows
    ]


def main() -> None:
    start_utc = datetime.now(timezone.utc).isoformat()
    db = DB(path=str(DB_PATH))
    before = {}
    with connect() as conn:
        clients = build_allowed_clients(conn)
        before = {t: count_table(conn, t) for t in ("raw_news_items", "news_clusters", "news_intelligence_signals", "news_provider_health")}

    worker = NewsIngestionWorker(
        db=db,
        registry=OneShotRegistry(db),
        health_svc=ProviderHealthService(db, stale_minutes=30),
        enabled=True,
    )

    fetched_sources = []
    for client in clients:
        worker._poll_source(client)
        fetched_sources.append(client.source_id)

    end_utc = datetime.now(timezone.utc).isoformat()
    with connect() as conn:
        after = {t: count_table(conn, t) for t in ("raw_news_items", "news_clusters", "news_intelligence_signals", "news_provider_health")}
        report = {
            "db": str(DB_PATH),
            "window_utc": {"start": start_utc, "end": end_utc},
            "allowed_sources": list(ALLOWED_SOURCE_IDS),
            "fetched_sources": fetched_sources,
            "before_counts": before,
            "after_counts": after,
            "delta_counts": {
                k: (after[k] - before[k] if after[k] is not None and before[k] is not None else None)
                for k in after
            },
            "window_counts": {
                "raw_news_items": count_since(conn, "raw_news_items", start_utc),
                "news_clusters": count_since(conn, "news_clusters", start_utc),
                "news_intelligence_signals": count_since(conn, "news_intelligence_signals", start_utc),
                "news_provider_health": count_since(conn, "news_provider_health", start_utc),
            },
            "news_provider_health": provider_health(conn),
            "duplicate_narrative_rows": duplicate_narrative_rows(conn),
            "unsafe_news_signals": unsafe_news_signals(conn),
        }
    print(json.dumps(report, indent=2, default=str))


if __name__ == "__main__":
    main()

