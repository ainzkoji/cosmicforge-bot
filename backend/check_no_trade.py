"""Check recent NO_TRADE reasons."""
import sqlite3
import json

DB_PATH = "data/bot.db"

def main():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    
    print("--- RECENT EXECUTION RESULTS (Last 20) ---")
    rows = conn.execute("""
        SELECT timestamp_utc, symbol, action, details_json 
        FROM events 
        WHERE event_type = 'EXECUTION_RESULT' 
        ORDER BY id DESC 
        LIMIT 20
    """).fetchall()
    
    for r in rows:
        details = json.loads(r["details_json"] or "{}")
        reason = details.get("reason", "")
        signal = details.get("signal", "")
        decision = details.get("decision", "")
        print(f"[{r['timestamp_utc'][:19]}] {r['symbol']} | {r['action']}")
        print(f"   Signal: {signal}, Decision: {decision}, Reason: {reason}")
        print()
    
    conn.close()

if __name__ == "__main__":
    main()
