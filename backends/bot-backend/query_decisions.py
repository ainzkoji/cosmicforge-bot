import sqlite3
import pandas as pd
import glob
import os
print("Looking for bot.db...")
search_path = os.path.abspath(os.path.join(os.getcwd(), "..", ".."))
for db_file in glob.glob(f"{search_path}/**/*bot.db", recursive=True):
    print(f"Found: {db_file}")
    try:
        conn = sqlite3.connect(db_file)
        query = '''SELECT timestamp, symbol, final_action, strategy_signal, sizing_decision, risk_gate_decision 
        FROM bot_decisions 
        ORDER BY timestamp DESC 
        LIMIT 15'''
        df = pd.read_sql_query(query, conn)
        print(f"--- Data from {db_file} ---")
        for _, row in df.iterrows():
            print(f"{row['symbol']}: {row['final_action']} | sig: {row['strategy_signal'][:100]} | size: {row['sizing_decision'][:100]}")
    except Exception as e:
        print(f"Error querying {db_file}: {e}")
