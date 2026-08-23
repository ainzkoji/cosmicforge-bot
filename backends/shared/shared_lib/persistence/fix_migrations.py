import sys
import re

file_path = r'C:\Users\favou\OneDrive\Desktop\cosmicforge-bot\backends\shared\shared_lib\persistence\migrations.py'

with open(file_path, 'r', encoding='utf-8') as f:
    content = f.read()

# Fix news_clusters
pattern_nc = r'(        conn\.execute\(\n            "CREATE INDEX IF NOT EXISTS idx_nc_dq_status "\n            "ON news_clusters\(data_quality_status\)"\n        \)\n        conn\.execute\(\n            "CREATE INDEX IF NOT EXISTS idx_nc_valid "\n            "ON news_clusters\(is_valid_signal\)"\n        \)\n\n        # Migrate: add new columns to existing news_clusters\n        try:\n            _nc_cols = \{r\[1\] for r in conn\.execute\("PRAGMA table_info\(news_clusters\)"\)\.fetchall\(\)\}\n            for _col, _def in \[\n                \("spam_score",          "REAL NOT NULL DEFAULT 0\.0"\),\n                \("latency_score",        "REAL NOT NULL DEFAULT 0\.0"\),\n                \("is_valid_signal",      "INTEGER NOT NULL DEFAULT 0"\),\n                \("manipulation_flag",    "TEXT"\),\n                \("data_quality_status",  "TEXT NOT NULL DEFAULT \'LOW_CONFIDENCE\'"\),\n            \]:\n                if _col not in _nc_cols:\n                    conn\.execute\(f"ALTER TABLE news_clusters ADD COLUMN \{_col\} \{_def\}"\)\n        except Exception:\n            pass\n)'

replacement_nc = '''        # Migrate: add new columns to existing news_clusters
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
'''

# Fix news_intelligence_signals
pattern_nis = r'(        conn\.execute\(\n            "CREATE INDEX IF NOT EXISTS idx_nis_dq_status "\n            "ON news_intelligence_signals\(data_quality_status\)"\n        \)\n        conn\.execute\(\n            "CREATE INDEX IF NOT EXISTS idx_nis_valid "\n            "ON news_intelligence_signals\(is_valid_signal\)"\n        \)\n\n        # Migrate: add new columns to existing news_intelligence_signals\n        try:\n            _nis_cols = \{r\[1\] for r in conn\.execute\("PRAGMA table_info\(news_intelligence_signals\)"\)\.fetchall\(\)\}\n            for _col, _def in \[\n                \("spam_score",          "REAL NOT NULL DEFAULT 0\.0"\),\n                \("latency_score",        "REAL NOT NULL DEFAULT 0\.0"\),\n                \("is_valid_signal",      "INTEGER NOT NULL DEFAULT 0"\),\n                \("manipulation_flag",    "TEXT"\),\n                \("data_quality_status",  "TEXT NOT NULL DEFAULT \'LOW_CONFIDENCE\'"\),\n            \]:\n                if _col not in _nis_cols:\n                    conn\.execute\(f"ALTER TABLE news_intelligence_signals ADD COLUMN \{_col\} \{_def\}"\)\n        except Exception:\n            pass\n)'

replacement_nis = '''        # Migrate: add new columns to existing news_intelligence_signals
        try:
            _nis_cols = {r[1] for r in conn.execute("PRAGMA table_info(news_intelligence_signals)").fetchall()}
            for _col, _def in [
                ("spam_score",          "REAL NOT NULL DEFAULT 0.0"),
                ("latency_score",        "REAL NOT NULL DEFAULT 0.0"),
                ("is_valid_signal",      "INTEGER NOT NULL DEFAULT 0"),
                ("manipulation_flag",    "TEXT"),
                ("data_quality_status",  "TEXT NOT NULL DEFAULT 'LOW_CONFIDENCE'"),
            ]:
                if _col not in _nis_cols:
                    conn.execute(f"ALTER TABLE news_intelligence_signals ADD COLUMN {_col} {_def}")
        except Exception:
            pass

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_nis_dq_status "
            "ON news_intelligence_signals(data_quality_status)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_nis_valid "
            "ON news_intelligence_signals(is_valid_signal)"
        )
'''

content, count_nc = re.subn(pattern_nc, replacement_nc, content)
content, count_nis = re.subn(pattern_nis, replacement_nis, content)

with open(file_path, 'w', encoding='utf-8') as f:
    f.write(content)

print(f'news_clusters replacements: {count_nc}')
print(f'news_intelligence_signals replacements: {count_nis}')
