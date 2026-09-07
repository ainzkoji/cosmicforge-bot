import sqlite3

db_path = '../../data/bot.db'
conn = sqlite3.connect(db_path)

columns_to_add = [
    ('mfe_pct', 'REAL'),
    ('mae_pct', 'REAL'),
    ('hold_time_minutes', 'REAL')
]

for col_name, col_type in columns_to_add:
    try:
        conn.execute(f"ALTER TABLE trade_fills ADD COLUMN {col_name} {col_type}")
        print(f"Added {col_name}")
    except sqlite3.OperationalError as e:
        print(f"Skipped {col_name}: {e}")

conn.commit()
conn.close()
