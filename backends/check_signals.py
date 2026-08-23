import sqlite3
import pandas as pd
conn = sqlite3.connect('C:/Users/favou/OneDrive/Desktop/cosmicforge-bot/data/bot.db')
query = '''SELECT timestamp, symbol, signal, confidence, threshold, is_passed 
FROM signal_outcomes 
ORDER BY timestamp DESC 
LIMIT 15'''
df = pd.read_sql_query(query, conn)
print(df)
