import sqlite3
import json
from datetime import datetime, timedelta

DB_PATH = "data/bot.db"

def main():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # Get events from last 30 mins
    # Assuming timestamp_utc is ISO string
    # We'll just fetch the last 100 items to be safe
    cursor.execute("""
        SELECT * FROM events 
        ORDER BY id DESC 
        LIMIT 100
    """)
    rows = cursor.fetchall()
    conn.close()

    print(f"--- RECENT EVENTS (Last {len(rows)}) ---")
    for row in reversed(rows):
        ts = row["timestamp_utc"]
        evt = row["event_type"]
        act = row["action"]
        sym = row["symbol"] or "---"
        details_str = row["details_json"]
        
        try:
           details = json.loads(details_str)
        except:
           details = details_str

        print(f"[{ts}] {evt} | {sym} | {act}")
        if evt == "ERROR":
             print(f"   >>> ERROR DETAILS: {details}")
        elif act in ["OPEN", "BUY", "SELL", "STEP_SYMBOL_FAILED"]:
             print(f"   >>> DETAILS: {details}")

if __name__ == "__main__":
    main()
