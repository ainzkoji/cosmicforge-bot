import sqlite3
import os

DB_PATH = "data/bot.db"

def migrate():
    print(f"Checking {DB_PATH} for schema updates...")
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    # Check current columns in runs table
    try:
        c.execute("PRAGMA table_info(runs)")
        columns = [row[1] for row in c.fetchall()]
        print(f"Current columns: {columns}")
        
        # Add 'environment' if missing
        if "environment" not in columns:
            print("Adding 'environment' column...")
            conn.execute("ALTER TABLE runs ADD COLUMN environment TEXT DEFAULT 'PAPER'")

        # Add 'version' if missing
        if "version" not in columns:
            print("Adding 'version' column...")
            conn.execute("ALTER TABLE runs ADD COLUMN version TEXT DEFAULT '1.0.0'")

        # Add 'config_json' if missing
        if "config_json" not in columns:
            print("Adding 'config_json' column...")
            conn.execute("ALTER TABLE runs ADD COLUMN config_json TEXT")

        # Add 'notes' if missing
        if "notes" not in columns:
            print("Adding 'notes' column...")
            conn.execute("ALTER TABLE runs ADD COLUMN notes TEXT")
            
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
