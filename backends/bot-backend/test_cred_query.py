import sqlite3

db_path = r"c:\Users\favou\OneDrive\Desktop\cosmicforge-bot\backends\shared\shared_lib\persistence\cosmicforge.db"
conn = sqlite3.connect(db_path)
conn.row_factory = sqlite3.Row
cur = conn.cursor()

cur.execute("SELECT * FROM bot_instances WHERE id = 'bot_54b6ea63f7ce'")
bot = dict(cur.fetchone() or {})
print(f"Bot 54b6ea63f7ce Broker Account ID: {bot.get('broker_account_id')}")

acc_id = bot.get('broker_account_id')
if acc_id:
    cur.execute("SELECT * FROM broker_credentials WHERE account_id = ?", (acc_id,))
    cred = dict(cur.fetchone() or {})
    print(f"Credentials exist: {bool(cred)}")
    if cred:
        print(f"Encrypted blob len: {len(cred.get('encrypted_blob', ''))}")
        
    cur.execute("SELECT * FROM broker_accounts WHERE id = ?", (acc_id,))
    acc = dict(cur.fetchone() or {})
    print(f"Broker exists: {bool(acc)}")
    print(f"Broker status: {acc.get('status')}")

conn.close()
