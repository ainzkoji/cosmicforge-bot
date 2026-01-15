import sqlite3
import time
import os

DB_PATH = "data/bot.db"

def test_lock():
    print(f"Testing lock on {DB_PATH}...")
    if not os.path.exists(DB_PATH):
        print(f"Database file {DB_PATH} does not exist yet.")
        return

    try:
        # Try to connect with a short timeout
        conn = sqlite3.connect(DB_PATH, timeout=2.0)
        
        # Try a write operation
        print("Attempting to write to database...")
        cursor = conn.cursor()
        cursor.execute("CREATE TABLE IF NOT EXISTS _lock_test (id INTEGER PRIMARY KEY)")
        cursor.execute("INSERT INTO _lock_test DEFAULT VALUES")
        conn.commit()
        print("✅ Success: Database is writable (NOT locked).")
        
        # Cleanup
        cursor.execute("DELETE FROM _lock_test")
        conn.commit()
        conn.close()
        
    except sqlite3.OperationalError as e:
        print(f"❌ LOCKED: {e}")
        print("This confirms the database is locked by another process (likely the background runner or OneDrive).")
    except Exception as e:
        print(f"❌ ERROR: {type(e).__name__}: {e}")

if __name__ == "__main__":
    test_lock()
