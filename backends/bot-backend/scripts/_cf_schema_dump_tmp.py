import sqlite3

db=r"backends\\shared\\shared_lib\\persistence\\cosmicforge.db"
conn=sqlite3.connect(db)
conn.row_factory=sqlite3.Row
cur=conn.cursor()

def exists(t):
    return cur.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (t,)).fetchone() is not None

for t in ['raw_news_items','news_clusters','news_intelligence_signals','news_provider_health','news_narratives','news_provider_health_status']:
    print('TABLE', t, 'exists' if exists(t) else 'missing')
    if exists(t):
        cols=[r['name'] for r in cur.execute(f"PRAGMA table_info({t})").fetchall()]
        print('  cols:', ', '.join(cols))
