"""
Quick diagnostic: print bot credentials stored in DB (masked).
Run from: cosmicforge-bot/backends/bot-backend/
"""
import sqlite3, os, sys

DB_PATH = r"C:\Users\favou\OneDrive\Desktop\cosmicforge-bot\data\bot.db"

if not os.path.exists(DB_PATH):
    print(f"DB not found at {DB_PATH}", file=sys.stderr)
    sys.exit(1)

conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row
cur = conn.cursor()

# List tables
cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
tables = [r[0] for r in cur.fetchall()]
print("Tables:", tables)

# Check for broker-related tables
for t in tables:
    if any(kw in t.lower() for kw in ["broker", "credential", "bot", "config", "user"]):
        print(f"\n--- {t} ---")
        try:
            cur.execute(f"SELECT * FROM {t} LIMIT 5")
            rows = cur.fetchall()
            if rows:
                cols = [d[0] for d in cur.description]
                print("Columns:", cols)
                for row in rows:
                    display = {}
                    for col in cols:
                        val = row[col]
                        # Mask sensitive values
                        if val and isinstance(val, str) and col.lower() in ("api_key", "api_secret", "broker_api_key", "broker_api_secret", "secret"):
                            display[col] = f"{val[:8]}...{val[-4:]}" if len(val) > 12 else "***"
                        else:
                            display[col] = val
                    print(display)
        except Exception as e:
            print(f"  Error: {e}")

conn.close()
