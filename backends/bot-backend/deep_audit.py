import json
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))
from shared_lib.persistence.db import DB

def audit_9_positions():
    db = DB()
    with db.connect() as conn:
        print("=== BOT INSTANCE CONFIGS ===")
        cursor = conn.execute("SELECT instance_id, status, config_json, created_at FROM bot_instances WHERE status != 'archived'")
        bots = cursor.fetchall()
        for bot in bots:
            cfg = json.loads(bot['config_json'])
            # Check for max_open_positions in various possible locations
            max_pos = cfg.get('max_open_positions') or cfg.get('risk_params', {}).get('max_open_positions') or "NOT SET"
            print(f"Bot ID: {bot['instance_id']}, Status: {bot['status']}, Max Positions: {max_pos}, Created: {bot['created_at']}")
            
            # Count actual open positions
            cursor_pos = conn.execute("SELECT count(*) as cnt FROM bot_symbol_state WHERE bot_instance_id = ? AND position NOT IN ('NONE', 'flat', 'FLAT')", (bot['instance_id'],))
            pos_cnt = cursor_pos.fetchone()['cnt']
            print(f"  Current Open Positions: {pos_cnt}")
            
            # If it's hitting a limit, find out why
            if pos_cnt >= 9:
                print(f"  🔍 Bot appears to be limited at 9 or more. Investigating...")
                
                # Check recent decision logs for 'max positions' reasons
                cursor_logs = conn.execute("""
                    SELECT count(*) as cnt 
                    FROM decision_logs 
                    WHERE bot_instance_id = ? AND final_action = 'hold' 
                    AND (execution_result_json LIKE '%max_open_positions%' OR sizing_decision_json LIKE '%max_open_positions%')
                """, (bot['instance_id'],))
                log_cnt = cursor_logs.fetchone()['cnt']
                print(f"  Recent hold logs due to max positions: {log_cnt}")
                
                if log_cnt > 0:
                    cursor_sample = conn.execute("""
                        SELECT created_at, symbol, final_action, sizing_decision_json, execution_result_json
                        FROM decision_logs
                        WHERE bot_instance_id = ? AND final_action = 'hold'
                        ORDER BY created_at DESC LIMIT 5
                    """, (bot['instance_id'],))
                    for sample in cursor_sample.fetchall():
                        print(f"    - {sample['created_at']} {sample['symbol']}: final_action: {sample['final_action']}")
                        # print(f"      Sizing: {sample['sizing_decision_json']}")
            print("-" * 40)

        # Check for any broker account limits or global circuit breakers
        print("\n=== RECENT ORDER FAILURES (Audit) ===")
        cursor_fail = conn.execute("""
            SELECT id, created_at, symbol, error_message, error_code 
            FROM order_failures 
            ORDER BY created_at DESC LIMIT 10
        """)
        for fail in cursor_fail.fetchall():
            print(f"Fail: {fail['created_at']} {fail['symbol']} | {fail['error_message']} (Code: {fail['error_code']})")

if __name__ == "__main__":
    audit_9_positions()
