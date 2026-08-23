import json
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))
from shared_lib.persistence.db import DB

def final_investigation():
    db = DB()
    with db.connect() as conn:
        print("=== BOT INSTANCES ===")
        cursor = conn.execute("SELECT * FROM bot_instances")
        rows = cursor.fetchall()
        for r in rows:
            print(dict(r))
            
        print("\n=== BOT SYMBOL STATE (Open Only) ===")
        cursor = conn.execute("SELECT bot_instance_id, count(*) as cnt FROM bot_symbol_state WHERE position NOT IN ('NONE', 'flat', 'FLAT') GROUP BY bot_instance_id")
        for r in cursor.fetchall():
            print(dict(r))

        print("\n=== SAMPLE OPEN POSITIONS FOR bot_8c4e186bed16 ===")
        cursor = conn.execute("SELECT symbol, position, updated_at FROM bot_symbol_state WHERE bot_instance_id = 'bot_8c4e186bed16' AND position NOT IN ('NONE', 'flat', 'FLAT')")
        for r in cursor.fetchall():
            print(dict(r))

        print("\n=== RECENT DECISION LOGS FOR bot_8c4e186bed16 ===")
        # Note: decision_logs uses config_id. We need to see if bot_8c4e186bed16 is a config_id or instance_id.
        # Based on schema, bot_instances.id is likely the identifier.
        cursor = conn.execute("""
            SELECT created_at, symbol, final_action, risk_gate_decision_json, sizing_decision_json 
            FROM decision_logs 
            WHERE config_id = 'bot_8c4e186bed16' 
            ORDER BY created_at DESC LIMIT 10
        """)
        for r in cursor.fetchall():
            d = dict(r)
            print(f"Time: {d['created_at']}, Symbol: {d['symbol']}, Action: {d['final_action']}")
            if d['risk_gate_decision_json']:
                print(f"  Risk Gate: {d['risk_gate_decision_json']}")

if __name__ == "__main__":
    final_investigation()
