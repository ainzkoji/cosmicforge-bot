import sqlite3
import pandas as pd
conn = sqlite3.connect('C:/Users/favou/OneDrive/Desktop/cosmicforge-bot/data/bot.db')
query = '''SELECT name, sql FROM sqlite_master WHERE type='table' AND name LIKE '%signal%';'''
print(pd.read_sql_query(query, conn))
