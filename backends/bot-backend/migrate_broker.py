import sqlite3

bot_db_path = r"c:\Users\favou\OneDrive\Desktop\cosmicforge-bot\data\bot.db"
master_db_path = r"c:\Users\favou\OneDrive\Desktop\cosmicforge-bot\backends\shared\shared_lib\persistence\cosmicforge.db"

conn_src = sqlite3.connect(bot_db_path)
conn_src.row_factory = sqlite3.Row
cur_src = conn_src.cursor()

conn_dst = sqlite3.connect(master_db_path)
cur_dst = conn_dst.cursor()

target_account = "brk_b42e4a234492"

# 1. Copy broker_accounts
cur_src.execute("SELECT * FROM broker_accounts WHERE id = ?", (target_account,))
account_row = cur_src.fetchone()

if account_row:
    columns = ", ".join(account_row.keys())
    placeholders = ", ".join(["?"] * len(account_row.keys()))
    try:
        cur_dst.execute(
            f"INSERT OR IGNORE INTO broker_accounts ({columns}) VALUES ({placeholders})",
            tuple(account_row)
        )
        print("Migrated broker_accounts row.")
    except Exception as e:
        print(f"Failed to migrate broker_accounts: {e}")
        
# 2. Copy broker_credentials
cur_src.execute("SELECT * FROM broker_credentials WHERE account_id = ?", (target_account,))
cred_row = cur_src.fetchone()

if cred_row:
    columns = ", ".join(cred_row.keys())
    placeholders = ", ".join(["?"] * len(cred_row.keys()))
    try:
        cur_dst.execute(
            f"INSERT OR IGNORE INTO broker_credentials ({columns}) VALUES ({placeholders})",
            tuple(cred_row)
        )
        print("Migrated broker_credentials row.")
    except Exception as e:
        print(f"Failed to migrate broker_credentials: {e}")

conn_dst.commit()
conn_src.close()
conn_dst.close()
print("Migration script completed.")
