import sqlite3
import pandas as pd
import json
import os

db_path = 'C:/Users/favou/OneDrive/Desktop/cosmicforge-bot/backends/shared/shared_lib/persistence/cosmicforge.db'

def verify():
    conn = sqlite3.connect(db_path)
    
    print("=== CHECKING FOR RECONCILED TRACES ===")
    try:
        df = pd.read_sql_query("SELECT trace_id, symbol, ml_action, ml_score, ts FROM decision_traces WHERE ml_action='RECONCILED'", conn)
        if len(df) > 0:
            print(f"Found {len(df)} reconciled traces!")
            print(df)
        else:
            print("No reconciled traces found yet. This is expected if the bot hasn't run its first cycle post-patch.")
    except Exception as e:
        print(f"Error checking traces: {e}")

    print("\n=== CHECKING TRADE FILLS VS TRACES JOIN ===")
    try:
        # This simulates the logic in the monitor script
        sql = '''
            SELECT tf.symbol, tf.action, dt.ml_action, dt.ml_score
            FROM trade_fills tf
            JOIN decision_traces dt ON (tf.order_id = dt.order_id OR tf.position_id = dt.order_id OR tf.symbol = dt.symbol)
            WHERE tf.action = 'CLOSE'
        '''
        df = pd.read_sql_query(sql, conn)
        print(f"Total close matches found: {len(df)}")
        print(df.head())
    except Exception as e:
        print(f"Error in join: {e}")

    conn.close()

if __name__ == "__main__":
    verify()
