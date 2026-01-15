import sqlite3
import json

DB_PATH = "data/bot.db"

def main():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    
    print("--- ERROR EVENTS (Last 20) ---")
    rows = conn.execute("""
        SELECT timestamp_utc, symbol, action, details_json 
        FROM events 
        WHERE event_type = 'ERROR' 
        ORDER BY id DESC 
        LIMIT 20
    """).fetchall()
    
    if not rows:
        print("No ERROR events found. Checking for STEP_SYMBOL_FAILED...")
        rows = conn.execute("""
            SELECT timestamp_utc, symbol, action, details_json 
            FROM events 
            WHERE action = 'STEP_SYMBOL_FAILED' 
            ORDER BY id DESC 
            LIMIT 20
        """).fetchall()
    
    if not rows:
        print("No failures found! Strategy may just be returning HOLD.")
    else:
        for r in rows:
            details = json.loads(r["details_json"] or "{}")
            error = details.get("error", str(details)[:200])
            print(f"[{r['timestamp_utc']}] {r['symbol']} | {r['action']}")
            print(f"   ERROR: {error}")
            print()
    
    conn.close()

if __name__ == "__main__":
    main()
