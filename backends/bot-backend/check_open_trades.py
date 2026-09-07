"""
Check open trades and allocations from decision_logs and bot_symbol_state
"""
import sys
import os
import json
sys.path.insert(0, os.path.dirname(__file__))

from shared_lib.persistence.db import DB

def check_open_trades():
    db = DB()
    
    print("=" * 80)
    print("CHECKING OPEN TRADES & RECENT ACTIVITY")
    print("=" * 80)
    
    # Check bot_symbol_state for position info
    print("\n📊 OPEN POSITIONS (from bot_symbol_state):\n")
    
    with db.connect() as conn:
        state_query = """
            SELECT 
                bot_instance_id,
                symbol,
                position,
                entry_price,
                entry_qty,
                last_signal,
                last_action,
                updated_at
            FROM bot_symbol_state
            WHERE position IS NOT NULL AND position != 'flat'
        """
        cursor = conn.execute(state_query)
        positions = cursor.fetchall()
    
    if positions:
        print(f"Found {len(positions)} OPEN POSITIONS:\n")
        total_notional = 0
        for pos in positions:
            bot_id, symbol, position, entry_price, qty, last_signal, last_action, updated = pos
            notional = float(qty) * float(entry_price) if qty and entry_price else 0
            total_notional += notional
            
            print(f"  Symbol:      {symbol}")
            print(f"  Position:    {position}")
            print(f"  Quantity:    {qty}")
            print(f"  Entry Price: ${entry_price}")
            print(f"  Notional:    ${notional:.2f}")
            print(f"  Last Signal: {last_signal}")
            print(f"  Last Action: {last_action}")
            print(f"  Updated:     {updated}")
            print()
        
        print("=" * 80)
        print(f"💰 TOTAL NOTIONAL VALUE: ${total_notional:.2f}")
        print("=" * 80)
    else:
        print("✅ NO OPEN POSITIONS FOUND\n")
    
    # Check recent decision logs
    print("\n" + "=" * 80)
    print("RECENT DECISION LOGS (last 10):")
    print("=" * 80 + "\n")
    
    with db.connect() as conn:
        query = """
            SELECT 
                created_at,
                symbol,
                final_action,
                sizing_decision_json,
                execution_result_json
            FROM decision_logs
            ORDER BY created_at DESC
            LIMIT 10
        """
        cursor = conn.execute(query)
        decisions = cursor.fetchall()
    
    if decisions:
        for dec in decisions:
            created_at, symbol, action, sizing_json, exec_json = dec
            
            # Parse JSON if present
            size_usdt = "N/A"
            if sizing_json:
                try:
                    sizing = json.loads(sizing_json)
                    size_usdt = sizing.get('budget_usdt', 'N/A')
                except:
                    pass
            
            exec_status = "N/A"
            if exec_json:
                try:
                    exec_result = json.loads(exec_json)
                    exec_status = exec_result.get('status', 'N/A')
                except:
                    pass
            
            print(f"  {created_at}: {symbol}")
            print(f"    Action: {action}")
            print(f"    Size: ${size_usdt}")
            print(f"    Exec Status: {exec_status}")
            print()
    else:
        print("❌ No decision logs found\n")
    
    # # Check order_failures
    # print("=" * 80)
    # print("RECENT ORDER FAILURES (if any):")
    # print("=" * 80 + "\n")
    
    # with db.connect() as conn:
    #     failures_query = """
    #         SELECT 
    #             created_at,
    #             symbol,
    #             error_reason
    #         FROM order_failures
    #         ORDER BY created_at DESC
    #         LIMIT 5
    #     """
    #     cursor = conn.execute(failures_query)
    #     failures = cursor.fetchall()
    
    # if failures:
    #     print(f"⚠️  Found {len(failures)} recent order failures:\n")
    #     for fail in failures:
    #         timestamp, symbol, reason = fail
    #         print(f"  {timestamp}: {symbol} - {reason}")
    # else:
    #     print("✅ No order failures\n")
    
    print("=" * 80)
    print("✅ ANALYSIS COMPLETE")
    print("=" * 80)

if __name__ == "__main__":
    check_open_trades()
