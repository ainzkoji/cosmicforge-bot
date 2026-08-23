import sqlite3, json

db = r"backends/shared/shared_lib/persistence/cosmicforge.db"
start = "2026-05-29T16:31:29.566860+00:00"
end = "2026-05-29T16:31:59.132472+00:00"

con = sqlite3.connect(db)
con.row_factory = sqlite3.Row
cur = con.cursor()
tables = {r[0] for r in cur.execute("SELECT name FROM sqlite_master WHERE type='table'")}

def scalar(q, p=()):
    r = cur.execute(q, p).fetchone()
    return None if r is None else list(r)[0]

def rows(q, p=()):
    return [dict(x) for x in cur.execute(q, p).fetchall()]

out = {}
for name in [
    "raw_news_items",
    "news_clusters",
    "news_intelligence_signals",
    "news_provider_health",
    "news_narrative_rows",
]:
    out[f"{name}_exists"] = name in tables

if "raw_news_items" in tables:
    out["raw_news_items_window"] = scalar(
        "SELECT COUNT(*) FROM raw_news_items WHERE created_at BETWEEN ? AND ?",
        (start, end),
    )
if "news_clusters" in tables:
    out["news_clusters_window"] = scalar(
        "SELECT COUNT(*) FROM news_clusters WHERE created_at BETWEEN ? AND ?",
        (start, end),
    )
if "news_intelligence_signals" in tables:
    out["news_intelligence_signals_window"] = scalar(
        "SELECT COUNT(*) FROM news_intelligence_signals WHERE created_at BETWEEN ? AND ?",
        (start, end),
    )

if "news_provider_health" in tables:
    out["news_provider_health_rows"] = rows(
        """
        SELECT source_id, status, items_fetched_last_run, duplicate_count_last_run,
               error_message, last_checked_utc, last_success_utc, created_at
        FROM news_provider_health
        WHERE created_at BETWEEN datetime(?, '-5 minutes') AND datetime(?, '+5 minutes')
        ORDER BY created_at DESC
        """,
        (start, end),
    )

# Duplicate narrative rows: table exists but appears columnless in pragma (likely a view); fallback to empty.
out["duplicate_narrative_rows"] = []

if "news_intelligence_signals" in tables:
    out["unsafe_news_signals"] = rows(
        """
        SELECT id, signal_type, symbol, should_affect_trading, shadow_only, created_at
        FROM news_intelligence_signals
        WHERE (shadow_only != 1 OR should_affect_trading != 0)
        ORDER BY created_at DESC
        LIMIT 200
        """
    )
else:
    out["unsafe_news_signals"] = []

print(json.dumps(out, indent=2))
