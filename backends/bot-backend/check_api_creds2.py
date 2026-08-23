"""
Detailed credential check - shows broker_credentials and bot_instances
"""
import sqlite3, json

DB_PATH = r"C:\Users\favou\OneDrive\Desktop\cosmicforge-bot\data\bot.db"
conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row
cur = conn.cursor()

print("=== broker_credentials ===")
cur.execute("SELECT * FROM broker_credentials LIMIT 5")
rows = cur.fetchall()
if rows:
    cols = [d[0] for d in cur.description]
    print("Columns:", cols)
    for row in rows:
        display = {}
        for col in cols:
            val = row[col]
            if val and isinstance(val, str) and col.lower() in ("api_key", "api_secret", "encrypted_credentials", "credentials_json", "secret"):
                display[col] = f"{str(val)[:10]}...{str(val)[-4:]}" if len(str(val)) > 14 else "***"
            else:
                display[col] = val
        print(display)
else:
    print("(empty)")

print("\n=== broker_accounts ===")
cur.execute("SELECT id, user_id, broker_id, status, environment, masked_key, last_error_code, last_error_message FROM broker_accounts LIMIT 10")
rows = cur.fetchall()
cols = [d[0] for d in cur.description]
for row in rows:
    print(dict(zip(cols, row)))

print("\n=== bot_instances ===")
cur.execute("SELECT id, user_id, broker_account_id, status, execution_mode, created_at FROM bot_instances LIMIT 10")
rows = cur.fetchall()
cols = [d[0] for d in cur.description]
for row in rows:
    print(dict(zip(cols, row)))

conn.close()
