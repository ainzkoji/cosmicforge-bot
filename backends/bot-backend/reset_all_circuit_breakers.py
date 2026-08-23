"""
Reset ALL Circuit Breakers

This script clears all circuit breaker entries from the order_failures table.
Use this after fixing bugs to allow the bot to trade again.
"""

from shared_lib.persistence.db import DB

db = DB()

print("Clearing all circuit breakers...")

with db.connect() as conn:
    # Get count before deletion
    count = conn.execute("SELECT COUNT(*) FROM order_failures").fetchone()[0]
    
    print(f"Found {count} circuit breaker entries")
    
    # Delete ALL entries
    conn.execute("DELETE FROM order_failures")
    conn.commit()
    
    print(f"✅ Cleared {count} circuit breaker entries")
    print("All symbols can now trade again!")
