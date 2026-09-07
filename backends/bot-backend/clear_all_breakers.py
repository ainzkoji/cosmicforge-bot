"""Quick script to clear ALL circuit breakers."""
import sqlite3

db_path = r"C:\Users\favou\OneDrive\Desktop\cosmicforge-bot\data\bot.db"

try:
    conn = sqlite3.connect(db_path, timeout=30.0)
    
    # Clear all failures
    rows_deleted = conn.execute("DELETE FROM order_failures").rowcount
    conn.commit()
    conn.close()
    
    print(f"✅ Cleared {rows_deleted} circuit breaker entries")
    print("Bot can now trade all symbols again")
    
except Exception as e:
    print(f"❌ Error: {e}")
