import sqlite3

db_path = '../../data/bot.db'
conn = sqlite3.connect(db_path)

columns_to_add = [
    ('adx', 'REAL'),
    ('atr_pct', 'REAL'),
    ('ma_slope', 'REAL'),
    ('compression_ratio', 'REAL'),
    ('breakout_pressure', 'REAL'),
    ('buy_score', 'REAL'),
    ('sell_score', 'REAL'),
    ('threshold', 'REAL'),
    ('active_strategy_count', 'INTEGER'),
    ('htf_opposed', 'INTEGER'),
    ('aggressiveness_score', 'REAL'),
    ('confidence_gate_modifier', 'REAL'),
    ('size_multiplier', 'REAL'),
    ('rolling_win_rate', 'REAL'),
    ('rolling_expectancy', 'REAL'),
    ('loss_streak', 'INTEGER'),
    ('ml_score', 'REAL'),
    ('ml_action', 'TEXT'),
    ('ml_model_version', 'TEXT'),
    ('ml_threshold', 'REAL')
]

for col_name, col_type in columns_to_add:
    try:
        conn.execute(f"ALTER TABLE decision_traces ADD COLUMN {col_name} {col_type}")
        print(f"Added {col_name}")
    except sqlite3.OperationalError as e:
        print(f"Skipped {col_name}: {e}")

conn.commit()
conn.close()
