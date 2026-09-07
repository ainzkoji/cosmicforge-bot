import sqlite3
import pandas as pd
conn = sqlite3.connect('C:/Users/favou/OneDrive/Desktop/cosmicforge-bot/data/bot.db')
query = '''SELECT name FROM sqlite_master WHERE type='table';'''
print(pd.read_sql_query(query, conn))
