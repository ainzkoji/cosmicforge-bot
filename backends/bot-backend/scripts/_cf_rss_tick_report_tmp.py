import sqlite3, json

db_path = r"backends\\shared\\shared_lib\\persistence\\cosmicforge.db"
start_utc = "2026-05-03T05:40:19.430153+00:00"
end_utc   = "2026-05-03T05:40:51.553324+00:00"
allow_ids = ["coindesk.com","cointelegraph.com","decrypt.co","bitcoinmagazine.com"]
allow_providers = [f"rss:{sid}" for sid in allow_ids]

conn = sqlite3.connect(db_path)
conn.row_factory = sqlite3.Row
cur = conn.cursor()

def has_table(name:str)->bool:
    return cur.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)).fetchone() is not None

out = {
  "window": {"start_utc": start_utc, "end_utc": end_utc},
  "raw_news_items": [],
  "news_clusters": [],
  "news_intelligence_signals": [],
  "news_provider_health": [],
  "duplicate_narrative_rows": [],
  "unsafe_news_signals": [],
  "notes": []
}

if has_table('raw_news_items'):
    q = """SELECT id, provider, title, published_utc, ingested_utc, source_domain, source_url, is_duplicate
           FROM raw_news_items
           WHERE ingested_utc >= ? AND ingested_utc <= ?
             AND provider IN ({})
           ORDER BY ingested_utc ASC""".format(",".join(["?"]*len(allow_providers)))
    rows = cur.execute(q, [start_utc, end_utc, *allow_providers]).fetchall()
    out["raw_news_items"] = [dict(r) for r in rows]

if has_table('news_clusters'):
    q = """SELECT id, canonical_title, first_seen_utc, last_seen_utc, first_seen_provider,
                  highest_reliability_score, cluster_confidence, is_manipulation_suspect
           FROM news_clusters
           WHERE first_seen_utc >= ? AND first_seen_utc <= ?
             AND first_seen_provider IN ({})
           ORDER BY first_seen_utc ASC""".format(",".join(["?"]*len(allow_providers)))
    rows = cur.execute(q, [start_utc, end_utc, *allow_providers]).fetchall()
    out["news_clusters"] = [dict(r) for r in rows]

if has_table('news_intelligence_signals'):
    rows = cur.execute(
        """SELECT id, cluster_id, symbol, created_at, signal_type, reliability_score,
                  confidence_score, shadow_only, should_affect_trading, suppression_reason
           FROM news_intelligence_signals
           WHERE created_at >= ? AND created_at <= ?
           ORDER BY created_at ASC""",
        [start_utc, end_utc]
    ).fetchall()
    out["news_intelligence_signals"] = [dict(r) for r in rows]
    out["unsafe_news_signals"] = [
        r for r in out["news_intelligence_signals"]
        if (r.get("shadow_only") != 1) or (r.get("should_affect_trading") != 0)
    ]

if has_table('news_provider_health'):
    rows = cur.execute(
        """SELECT id, source_id, status, last_checked_utc, last_success_utc,
                  items_fetched_last_run, duplicate_count_last_run, error_message, created_at
           FROM news_provider_health
           WHERE created_at >= ? AND created_at <= ?
             AND source_id IN ({})
           ORDER BY created_at ASC""".format(",".join(["?"]*len(allow_ids))),
        [start_utc, end_utc, *allow_ids]
    ).fetchall()
    out["news_provider_health"] = [dict(r) for r in rows]

if has_table('news_narratives'):
    rows = cur.execute(
        """SELECT cluster_id, narrative_type, COUNT(1) AS n
           FROM news_narratives
           GROUP BY cluster_id, narrative_type
           HAVING COUNT(1) > 1
           ORDER BY n DESC
           LIMIT 200"""
    ).fetchall()
    out["duplicate_narrative_rows"] = [dict(r) for r in rows]

print(json.dumps(out, indent=2, sort_keys=True))
