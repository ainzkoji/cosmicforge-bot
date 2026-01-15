import sqlite3
import os

DB_PATH = "data/bot.db"

def migrate():
    print(f"Checking {DB_PATH} for Events timestamp sync...")
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    try:
        c.execute("PRAGMA table_info(events)")
        columns = {row[1] for row in c.fetchall()}
        print(f"Columns: {columns}")
        
        has_ts = "ts" in columns
        has_timestamp_utc = "timestamp_utc" in columns
        
        if has_ts and has_timestamp_utc:
            print("Syncing ts -> timestamp_utc for new records...")
            # If timestamp_utc is null but ts has value, copy it
            conn.execute("UPDATE events SET timestamp_utc = ts WHERE timestamp_utc IS NULL AND ts IS NOT NULL")
            print("Sync complete.")
            
            # Also, we need to make sure the code writes to both if both exist.
            # But we can't change the python code from here comfortably without restarting process.
            # Best fix is to drop the NOT NULL constraint on timestamp_utc, but SQLite ALTER COLUMN is limited.
            # Instead, we will update the Python code to write to both columns.
            
    except Exception as e:
        print(f"Error: {e}")
    finally:
        conn.commit()
        conn.close()

if __name__ == "__main__":
    if os.path.exists(DB_PATH):
        migrate()
