import sqlite3
from datetime import datetime, timezone

DB_PATH = "data/bot.db"

utc_day = datetime.now(timezone.utc).date().isoformat()

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

cur.execute(
    "UPDATE daily_state SET realized_pnl = 0, kill = 0 WHERE day = ?", (utc_day,)
)

conn.commit()

cur.execute("SELECT day, realized_pnl, kill FROM daily_state WHERE day = ?", (utc_day,))

row = cur.fetchone()
print("RESET RESULT:", row)

conn.close()
