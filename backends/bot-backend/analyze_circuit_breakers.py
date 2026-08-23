import sqlite3
from datetime import datetime, timezone

db_path = r"C:\Users\favou\OneDrive\Desktop\cosmicforge-bot\data\bot.db"

print("🔍 Analyzing Circuit Breaker State...")
print("=" * 80)

try:
    conn = sqlite3.connect(db_path, timeout=30.0)
    conn.row_factory = sqlite3.Row
    
    # Get all circuit breaker entries
    rows = conn.execute("""
        SELECT config_id, symbol, consecutive_failures, last_failure_at, paused_until
        FROM order_failures
        WHERE consecutive_failures > 0
        ORDER BY last_failure_at DESC
    """).fetchall()
    
    now = datetime.now(timezone.utc)
    
    print(f"Current Time (UTC): {now.isoformat()}")
    print(f"Total Symbols with Failures: {len(rows)}")
    print("=" * 80)
    
    active_breakers = 0
    expired_breakers = 0
    
    for row in rows:
        paused_until_str = row['paused_until']
        if paused_until_str:
            paused_until = datetime.fromisoformat(paused_until_str)
            is_active = now < paused_until
            time_diff = (paused_until - now).total_seconds() / 60  # minutes
            
            if is_active:
                active_breakers += 1
                print(f"🔴 ACTIVE: {row['symbol']} - {row['consecutive_failures']} failures")
                print(f"   Expires in: {time_diff:.1f} minutes")
            else:
                expired_breakers += 1
                print(f"🟢 EXPIRED: {row['symbol']} - {row['consecutive_failures']} failures")
                print(f"   Expired {abs(time_diff):.1f} minutes ago")
        else:
            print(f"⚪ NO PAUSE: {row['symbol']} - {row['consecutive_failures']} failures")
        
        print(f"   Last Failure: {row['last_failure_at']}")
        print()
    
    print("=" * 80)
    print(f"Summary: {active_breakers} active, {expired_breakers} expired")
    print("=" * 80)
    
    conn.close()
    
except Exception as e:
    print(f"❌ Error: {e}")
