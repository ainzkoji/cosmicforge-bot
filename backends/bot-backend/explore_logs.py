import json
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))
from shared_lib.persistence.db import DB

def explore_logs():
    db = DB()
    with db.connect() as conn:
        print("=== RECENT DECISION LOGS AUDIT ===")
        # Check decision_logs for 'hold' reasons
        cursor = conn.execute("""
            SELECT created_at, symbol, final_action, risk_gate_decision_json, sizing_decision_json, execution_result_json
            FROM decision_logs 
            WHERE final_action = 'hold' 
            ORDER BY created_at DESC LIMIT 50
        """)
        for r in cursor.fetchall():
            d = dict(r)
            time = d['created_at']
            sym = d['symbol']
            
            # Check for MAX_POSITIONS_REACHED in any of the JSON fields
            found = False
            for field in ['risk_gate_decision_json', 'sizing_decision_json', 'execution_result_json']:
                if d[field] and 'MAX_POSITIONS_REACHED' in d[field]:
                    print(f"Time: {time}, Symbol: {sym}, Action: {d['final_action']}, Reason: {field} contains MAX_POSITIONS_REACHED")
                    # print(f"  {field}: {d[field]}")
                    found = True
                    break
            
            if not found:
                # If no explicit limit found, just print what we have
                pass

        print("\n=== SYSTEM CONFIGURATION (via code) ===")
        from app.core.config import settings
        print(f"settings.MAX_OPEN_POSITIONS = {getattr(settings, 'MAX_OPEN_POSITIONS', 'N/A')}")

if __name__ == "__main__":
    explore_logs()
