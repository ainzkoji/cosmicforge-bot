
import sqlite3
import os

# Paths to check
DB_PATHS = [
    r"C:\Users\favou\OneDrive\Desktop\cosmicforge-bot\data\bot.db", # Absolute path
    "data/bot.db",
    "cosmicforge.db"
]

def clear_breakers():
    db_path = None
    for path in DB_PATHS:
        if os.path.exists(path):
            db_path = path
            break
            
    if not db_path:
        print("❌ Could not find database file.")
        return

    print(f"🔌 Connecting to {db_path}...")
    try:
        # Add timeout to handle locking
        conn = sqlite3.connect(db_path, timeout=30.0)
        cursor = conn.cursor()
        
        # Check current state
        cursor.execute("SELECT * FROM order_failures")
        rows = cursor.fetchall()
        print(f"⚠️ Found {len(rows)} circuit breaker entries.")
        for row in rows:
            print(f"   - {row}")

        # Clear them
        print("🧹 Clearing circuit breakers...")
        cursor.execute("DELETE FROM order_failures")
        cursor.execute("UPDATE order_failures SET consecutive_failures = 0, paused_until = NULL") # Just in case we want to keep rows but reset
        # Actually DELETE is cleaner for "reset all"
        
        conn.commit()
        print("✅ Circuit breakers cleared. Bot should resume immediately.")
        conn.close()
        
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    clear_breakers()
