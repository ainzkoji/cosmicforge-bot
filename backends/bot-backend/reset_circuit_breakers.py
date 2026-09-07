"""
Reset all circuit breakers by clearing the order_failures table.
This should be run with the bot STOPPED to avoid database locking.
"""
import sqlite3
from pathlib import Path

db_path = Path(__file__).parent / "data" / "bot.db"

try:
    with sqlite3.connect(db_path, timeout=10) as conn:
        # Clear all circuit breakers
        result = conn.execute("DELETE FROM order_failures")
        deleted = result.rowcount
        conn.commit()
        print(f"✅ Cleared {deleted} circuit breaker entries")
        print("✅ All symbols can now trade again!")
        print("")
        print("Next steps:")
        print("1. Restart your bot (CTRL+C then restart)")
        print("2. The circuit breaker will start fresh")
        print("3. Only real exchange failures will trigger it now")
except sqlite3.OperationalError as e:
    if "locked" in str(e):
        print("⚠️  Database is locked (bot is running)")
        print("")
        print("To reset circuit breakers:")
        print("1. Stop the bot (CTRL+C)")
        print("2. Run: python reset_circuit_breakers.py")
        print("3. Restart the bot")
    else:
        print(f"❌ Error: {e}")
