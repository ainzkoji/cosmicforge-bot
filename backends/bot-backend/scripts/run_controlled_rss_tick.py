"""
Run a single controlled RSS ingestion tick (shadow-only).

Purpose: shadow proof / diagnostics runs where we must ingest ONLY an allowlist
of RSS sources (no API providers, no social media) and then report DB deltas.

Usage (from repo root):
  python backends/bot-backend/scripts/run_controlled_rss_tick.py

Or (from backends/bot-backend):
  python scripts/run_controlled_rss_tick.py --db ../shared/shared_lib/persistence/cosmicforge.db
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Tuple

from shared_lib.persistence.db import DB


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _fetch_rss_sources(db: DB, allow_ids: Iterable[str]) -> List[dict]:
    allow = set(allow_ids)
    if not allow:
        return []
    with db.connect() as conn:
        rows = conn.execute(
            """
            SELECT
              id,
              source_name,
              source_type,
              is_enabled,
              rss_url
            FROM news_sources
            WHERE source_type = 'RSS'
              AND is_enabled = 1
              AND rss_url IS NOT NULL
              AND rss_url != ''
            """
        ).fetchall()
    out: List[dict] = []
    for r in rows:
        if r["id"] in allow:
            out.append(
                {
                    "id": r["id"],
                    "source_name": r["source_name"],
                    "rss_url": r["rss_url"],
                }
            )
    return out


def _count_in_window(db: DB, allow_ids: List[str], start_utc: str, end_utc: str) -> Dict[str, int]:
    allow_providers = tuple(f"rss:{sid}" for sid in allow_ids)
    with db.connect() as conn:
        raw_n = conn.execute(
            f"""
            SELECT COUNT(1) AS n
            FROM raw_news_items
            WHERE ingested_utc >= ?
              AND ingested_utc <= ?
              AND provider IN ({",".join(["?"] * len(allow_providers))})
            """,
            (start_utc, end_utc, *allow_providers),
        ).fetchone()["n"]

        cluster_n = conn.execute(
            f"""
            SELECT COUNT(1) AS n
            FROM news_clusters
            WHERE first_seen_utc >= ?
              AND first_seen_utc <= ?
              AND first_seen_provider IN ({",".join(["?"] * len(allow_providers))})
            """,
            (start_utc, end_utc, *allow_providers),
        ).fetchone()["n"]

        signal_n = conn.execute(
            """
            SELECT COUNT(1) AS n
            FROM news_intelligence_signals
            WHERE created_at >= ?
              AND created_at <= ?
            """,
            (start_utc, end_utc),
        ).fetchone()["n"]

    return {
        "raw_news_items": int(raw_n or 0),
        "news_clusters": int(cluster_n or 0),
        "news_intelligence_signals": int(signal_n or 0),
    }


def run_tick(db_path: str, allow_ids: List[str]) -> Tuple[Dict[str, object], List[str]]:
    # Ensure `import app.*` resolves when this script is executed as a file.
    # (python sets sys.path[0] to the script directory, not the repo root)
    bot_backend_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    if bot_backend_dir not in sys.path:
        sys.path.insert(0, bot_backend_dir)

    from app.news.rss_provider_client import RSSProviderClient
    from app.news.news_provider_health import ProviderHealthService
    from app.workers.news_ingestion_worker import NewsIngestionWorker

    db = DB(path=db_path)
    sources = _fetch_rss_sources(db, allow_ids)
    if not sources:
        start_utc = _utc_now_iso()
        end_utc = _utc_now_iso()
        counts = _count_in_window(db, allow_ids=allow_ids, start_utc=start_utc, end_utc=end_utc)
        meta: Dict[str, object] = {"start_utc": start_utc, "end_utc": end_utc, "counts": counts}
        return meta, ["no enabled RSS sources matched allowlist"]

    class _RegistryStub:
        def mark_fetched(self, source_id: str, fetched_utc: str, error: str | None = None) -> None:
            with db.connect() as conn:
                conn.execute(
                    """
                    UPDATE news_sources
                    SET last_fetch_utc = ?,
                        last_success_utc = CASE WHEN ? IS NULL THEN ? ELSE last_success_utc END,
                        last_error = ?
                    WHERE id = ?
                    """,
                    (fetched_utc, error, fetched_utc, error, source_id),
                )

    registry = _RegistryStub()
    health_svc = ProviderHealthService(db)
    worker = NewsIngestionWorker(db=db, registry=registry, health_svc=health_svc, enabled=True)

    warnings: List[str] = []
    start_utc = _utc_now_iso()

    for src in sources:
        client = RSSProviderClient(
            source_id=src["id"],
            source_name=src["source_name"],
            rss_url=src["rss_url"],
        )
        try:
            worker._poll_source(client)
        except Exception as exc:
            warnings.append(f"{src['id']}: poll failed: {exc}")

    end_utc = _utc_now_iso()
    counts = _count_in_window(db, allow_ids=allow_ids, start_utc=start_utc, end_utc=end_utc)
    meta: Dict[str, object] = {"start_utc": start_utc, "end_utc": end_utc, "counts": counts}
    return meta, warnings


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--db",
        default="../shared/shared_lib/persistence/cosmicforge.db",
        help="Path to SQLite DB (cosmicforge.db).",
    )
    ap.add_argument(
        "--sources",
        default="coindesk.com,cointelegraph.com,decrypt.co,bitcoinmagazine.com",
        help="Comma-separated allowlist of news_sources.id to ingest.",
    )
    args = ap.parse_args()

    allow_ids = [s.strip() for s in str(args.sources).split(",") if s.strip()]
    meta, warnings = run_tick(db_path=args.db, allow_ids=allow_ids)
    counts = meta.get("counts", {})

    print(
        "controlled_rss_tick "
        f"start_utc={meta.get('start_utc')} "
        f"end_utc={meta.get('end_utc')} "
        f"raw_news_items={counts.get('raw_news_items')} "
        f"news_clusters={counts.get('news_clusters')} "
        f"news_intelligence_signals={counts.get('news_intelligence_signals')}"
    )
    if warnings:
        print("warnings:")
        for w in warnings:
            print(f"- {w}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
