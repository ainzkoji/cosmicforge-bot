"""Check which strategy was used to open positions."""
import sqlite3
import json

DB_PATH = "data/bot.db"

def main():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    
    print("=== STRATEGY SIGNALS (Last 20 entries) ===\n")
    rows = conn.execute("""
        SELECT timestamp_utc, symbol, action, details_json 
        FROM events 
        WHERE event_type = 'STRATEGY_SIGNAL'
        ORDER BY id DESC 
        LIMIT 20
    """).fetchall()
    
    for r in rows:
        details = json.loads(r["details_json"] or "{}")
        strategy = details.get("strategy", "unknown")
        confidence = details.get("confidence", 0)
        reason = details.get("reason", "")
        print(f"[{r['timestamp_utc'][:19]}] {r['symbol']} | Signal: {r['action']}")
        print(f"   Strategy: {strategy}, Confidence: {confidence}")
        print(f"   Reason: {reason}")
        print()
    
    print("\n=== TRADES THAT WERE PLACED ===\n")
    trades = conn.execute("""
        SELECT timestamp_utc, symbol, action, details_json 
        FROM events 
        WHERE action = 'ORDER_PLACED'
        ORDER BY id DESC 
        LIMIT 10
    """).fetchall()
    
    if not trades:
        print("No ORDER_PLACED events found in database.")
    else:
        for t in trades:
            details = json.loads(t["details_json"] or "{}")
            print(f"[{t['timestamp_utc'][:19]}] {t['symbol']}")
            print(f"   Signal: {details.get('signal')}, Side: {details.get('side')}")
            print(f"   Qty: {details.get('qty')}, USDT: {details.get('trade_usdt')}")
            print()
    
    conn.close()

if __name__ == "__main__":
    main()
