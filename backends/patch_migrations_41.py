"""
Appends migration block 41 (News Market Validation Layer) to migrations.py.
Run once: python patch_migrations.py
"""
import os

PATH = r"c:\Users\favou\OneDrive\Desktop\cosmicforge-bot\backends\shared\shared_lib\persistence\migrations.py"

BLOCK = (
    "\n"
    "        # ================================================================\n"
    "        # 41) News Market Validation Layer (Phase 3 Extension)\n"
    "        # ================================================================\n"
    "        conn.execute(\n"
    "            \"\"\"\n"
    "            CREATE TABLE IF NOT EXISTS news_market_reactions (\n"
    "                id                         INTEGER PRIMARY KEY AUTOINCREMENT,\n"
    "                cluster_id                 INTEGER NOT NULL\n"
    "                                               REFERENCES news_clusters(id),\n"
    "                symbol                     TEXT NOT NULL,\n"
    "                event_reaction_id          INTEGER,\n"
    "                sentiment_score            REAL,\n"
    "                sentiment_direction        TEXT,\n"
    "                actual_direction           TEXT,\n"
    "                sentiment_accuracy         TEXT NOT NULL DEFAULT 'NEUTRAL',\n"
    "                sentiment_accuracy_score   REAL NOT NULL DEFAULT 0.0,\n"
    "                impact_score               REAL NOT NULL DEFAULT 0.0,\n"
    "                max_price_move_pct         REAL,\n"
    "                volatility_expansion       REAL,\n"
    "                volume_spike               REAL,\n"
    "                reaction_type              TEXT NOT NULL DEFAULT 'NO_REACTION',\n"
    "                reaction_latency_minutes   REAL,\n"
    "                reaction_latency_category  TEXT NOT NULL DEFAULT 'NO_REACTION',\n"
    "                signal_effectiveness_score REAL NOT NULL DEFAULT 0.0,\n"
    "                is_false_signal            INTEGER NOT NULL DEFAULT 0,\n"
    "                false_signal_reason        TEXT,\n"
    "                data_quality_score         REAL NOT NULL DEFAULT 0.0,\n"
    "                reliability_score          REAL NOT NULL DEFAULT 0.0,\n"
    "                created_at                 TEXT NOT NULL,\n"
    "                updated_at                 TEXT NOT NULL\n"
    "            )\n"
    "            \"\"\"\n"
    "        )\n"
    "        for _idx_sql in [\n"
    "            \"CREATE INDEX IF NOT EXISTS idx_nmr_cluster  \"\n"
    "            \"ON news_market_reactions(cluster_id)\",\n"
    "            \"CREATE INDEX IF NOT EXISTS idx_nmr_symbol   \"\n"
    "            \"ON news_market_reactions(symbol)\",\n"
    "            \"CREATE INDEX IF NOT EXISTS idx_nmr_accuracy \"\n"
    "            \"ON news_market_reactions(sentiment_accuracy)\",\n"
    "            \"CREATE INDEX IF NOT EXISTS idx_nmr_false    \"\n"
    "            \"ON news_market_reactions(is_false_signal)\",\n"
    "            \"CREATE INDEX IF NOT EXISTS idx_nmr_created  \"\n"
    "            \"ON news_market_reactions(created_at)\",\n"
    "        ]:\n"
    "            conn.execute(_idx_sql)\n"
    "\n"
    "        conn.execute(\n"
    "            \"\"\"\n"
    "            CREATE TABLE IF NOT EXISTS narrative_effectiveness_scores (\n"
    "                narrative_type          TEXT PRIMARY KEY,\n"
    "                sample_count            INTEGER NOT NULL DEFAULT 0,\n"
    "                avg_impact_score        REAL NOT NULL DEFAULT 0.0,\n"
    "                avg_price_move_pct      REAL NOT NULL DEFAULT 0.0,\n"
    "                correct_sentiment_ratio REAL NOT NULL DEFAULT 0.0,\n"
    "                false_signal_ratio      REAL NOT NULL DEFAULT 0.0,\n"
    "                avg_effectiveness_score REAL NOT NULL DEFAULT 0.0,\n"
    "                last_updated            TEXT NOT NULL\n"
    "            )\n"
    "            \"\"\"\n"
    "        )\n"
)

with open(PATH, "rb") as f:
    raw = f.read()

# The file uses \r\n (Windows). The last real content is `            pass`
# followed by a bare \n (no \r).
MARKER = b"            pass"
idx = raw.rfind(MARKER)
if idx == -1:
    print("ERROR: marker not found")
else:
    new_raw = raw[:idx + len(MARKER)] + BLOCK.encode("utf-8")
    with open(PATH, "wb") as f:
        f.write(new_raw)
    print(f"Done. Total bytes: {len(new_raw)}")
