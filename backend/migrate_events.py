import sqlite3
import os

DB_PATH = "data/bot.db"

def migrate():
    print(f"Checking {DB_PATH} for Events schema updates...")
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    try:
        c.execute("PRAGMA table_info(events)")
        columns = [row[1] for row in c.fetchall()]
        print(f"Current columns: {columns}")
        
        # The error is "no column named event_id", but events table usually has 'id'. 
        # Let's check if the code expects 'event_id' or if it's a rename.
        # However, looking at the error, it seems the code is trying to insert into 'event_id' 
        # or similar.
        # Wait, standard practice is 'id' INTEGER PRIMARY KEY. 
        # If the code tries to insert 'event_id', it might be a UUID string column?
        
        if "event_id" not in columns:
            print("Adding 'event_id' column...")
            conn.execute("ALTER TABLE events ADD COLUMN event_id TEXT")

        if "level" not in columns:
            print("Adding 'level' column...")
            conn.execute("ALTER TABLE events ADD COLUMN level TEXT DEFAULT 'INFO'")
            
        print("Migration complete.")
        conn.commit()
    except Exception as e:
        print(f"Error checking/migrating: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    if os.path.exists(DB_PATH):
        migrate()
    else:
        print(f"DB not found at {DB_PATH}")
