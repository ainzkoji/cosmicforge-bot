import sys

file_path = r'C:\Users\favou\OneDrive\Desktop\cosmicforge-bot\backends\shared\shared_lib\persistence\migrations.py'

with open(file_path, 'r', encoding='utf-8') as f:
    lines = f.readlines()

# The mangled block starts around line 1805 where it says:
# conn.execute(
#         cluster_id       INTEGER NOT NULL REFERENCES news_clusters(id),

# Let's find the `conn.execute(` that corresponds to `idx_nc_first_seen`
start_idx = -1
end_idx = -1
for i, line in enumerate(lines):
    if "CREATE INDEX IF NOT EXISTS idx_nc_first_seen" in line:
        start_idx = i - 1 # the conn.execute( line before it
        break

for i in range(start_idx, len(lines)):
    if "idx_nci_cluster" in lines[i]:
        end_idx = i - 1 # the conn.execute( line before it
        break

if start_idx != -1 and end_idx != -1:
    correct_block = """        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_nc_first_seen "
            "ON news_clusters(first_seen_utc)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_nc_last_seen "
            "ON news_clusters(last_seen_utc)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_nc_manip "
            "ON news_clusters(is_manipulation_suspect)"
        )

        # Migrate: add new columns to existing news_clusters
        try:
            _nc_cols = {r[1] for r in conn.execute("PRAGMA table_info(news_clusters)").fetchall()}
            for _col, _def in [
                ("spam_score",          "REAL NOT NULL DEFAULT 0.0"),
                ("latency_score",        "REAL NOT NULL DEFAULT 0.0"),
                ("is_valid_signal",      "INTEGER NOT NULL DEFAULT 0"),
                ("manipulation_flag",    "TEXT"),
                ("data_quality_status",  "TEXT NOT NULL DEFAULT 'LOW_CONFIDENCE'"),
            ]:
                if _col not in _nc_cols:
                    conn.execute(f"ALTER TABLE news_clusters ADD COLUMN {_col} {_def}")
        except Exception:
            pass

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_nc_dq_status "
            "ON news_clusters(data_quality_status)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_nc_valid "
            "ON news_clusters(is_valid_signal)"
        )

        conn.execute(\"\"\"
            CREATE TABLE IF NOT EXISTS news_cluster_items (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                cluster_id       INTEGER NOT NULL REFERENCES news_clusters(id),
                raw_news_item_id INTEGER NOT NULL REFERENCES raw_news_items(id),
                similarity_score REAL NOT NULL DEFAULT 1.0,
                created_at       TEXT NOT NULL,
                UNIQUE(cluster_id, raw_news_item_id)
            )
        \"\"\")
"""
    new_lines = lines[:start_idx] + [correct_block] + lines[end_idx:]
    with open(file_path, 'w', encoding='utf-8') as f:
        f.writelines(new_lines)
    print("Fixed!")
else:
    print("Indices not found")
