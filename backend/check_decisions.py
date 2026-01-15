import sqlite3
import json
from datetime import datetime, timezone, timedelta

DB_PATH = "data/bot.db"

def check_decisions(minutes=60):
    print(f"--- CHECKING DECISIONS (Last {minutes} minutes) ---")
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    
    # Calculate cutoff time
    cutoff = (datetime.now(timezone.utc) - timedelta(minutes=minutes)).isoformat()
    
    try:
        # 1. Check ANY events (to see if runner is alive)
        cursor = conn.execute("""
            SELECT timestamp_utc, event_type, symbol, action, details_json
            FROM events
            WHERE timestamp_utc > ?
            ORDER BY timestamp_utc DESC
            LIMIT 50
        """, (cutoff,))
        
        rows = cursor.fetchall()
        if not rows:
            print("No DECISION events found in the last hour.")
            print("Possible causes:")
            print("- Runner loop not running? (Use /runner/status)")
            print("- No symbols selected?")
            return

        print(f"Found {len(rows)} recent decisions:")
        for row in rows:
            details = json.loads(row["details_json"])
            sig = details.get("signal", "N/A")
            reason = details.get("reason", "")
            print(f"[{row['timestamp_utc']}] {row['event_type']} | {row['symbol']} -> Action: {row['action']} | Signal: {sig}")
            
            # Highlight rejections
            if row["action"] == "HOLD":
                # Why hold?
                print(f"   Reason: {reason if reason else 'Strategy returned HOLD'}")
                if details.get("cooldown_ok") is False:
                    print("   [!] BLOCKED BY COOLDOWN")
                if details.get("kill_switch") is True:
                    print("   [!] BLOCKED BY KILL SWITCH")
            
            if row["action"] == "STEP_SYMBOL_FAILED":
                 print(f"   ERROR: {details.get('error')}")
            
    except Exception as e:
        print(f"Error querying DB: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    check_decisions()
