import sqlite3, json
from pathlib import Path

db_path = Path(r"backends/shared/shared_lib/persistence/cosmicforge.db")
conn = sqlite3.connect(db_path)
conn.row_factory = sqlite3.Row
cur = conn.cursor()

def table_exists(name: str) -> bool:
    return cur.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)).fetchone() is not None

out = {}

# Duplicate narrative rows: prefer news_cluster_narratives else news_narratives
for narr_table in ("news_cluster_narratives", "news_narratives"):
    if not table_exists(narr_table):
        continue
    cols = [r[1] for r in cur.execute(f"PRAGMA table_info({narr_table})").fetchall()]
    cluster_col = "cluster_id" if "cluster_id" in cols else ("news_cluster_id" if "news_cluster_id" in cols else None)
    type_col = "narrative_type" if "narrative_type" in cols else ("type" if "type" in cols else None)
    if cluster_col and type_col:
        out["duplicate_narrative_rows_all_time"] = cur.execute(
            f"SELECT COUNT(*) FROM (SELECT {cluster_col},{type_col},COUNT(*) n FROM {narr_table} GROUP BY {cluster_col},{type_col} HAVING n>1)"
        ).fetchone()[0]
        out["duplicate_narrative_table"] = narr_table
        out["duplicate_narrative_key"] = [cluster_col, type_col]
        break

# Unsafe news signals: shadow_only != 1 OR should_affect_trading != 0
sig_table = "news_intelligence_signals" if table_exists("news_intelligence_signals") else None
if sig_table:
    cols = [r[1] for r in cur.execute(f"PRAGMA table_info({sig_table})").fetchall()]
    shadow_col = "shadow_only" if "shadow_only" in cols else None
    trade_col = "should_affect_trading" if "should_affect_trading" in cols else None
    if shadow_col and trade_col:
        out["unsafe_signals_all_time"] = cur.execute(
            f"SELECT COUNT(*) FROM {sig_table} WHERE ({shadow_col}!=1 OR {trade_col}!=0)"
        ).fetchone()[0]

print(json.dumps(out, indent=2))
