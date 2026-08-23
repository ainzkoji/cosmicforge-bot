import sqlite3
import os

# Target the shared DB
db_path = '../shared/shared_lib/persistence/cosmicforge.db'

print(f"Applying migration to: {db_path}")

if not os.path.exists(db_path):
    print("❌ Database not found!")
    exit(1)

try:
    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS bot_daily_state (
            bot_instance_id TEXT NOT NULL,
            day TEXT NOT NULL,
            realized_pnl REAL DEFAULT 0.0,
            kill INTEGER DEFAULT 0,
            trade_count INTEGER DEFAULT 0,
            last_updated_at TEXT,
            PRIMARY KEY (bot_instance_id, day)
        )
    """)
    conn.commit()
    print("✅ Created table bot_daily_state successfully.")
    conn.close()

except Exception as e:
    print(f"❌ Database error: {e}")
