import sqlite3
from app.core.bot_instance_service import get_bot_instance_service

# Print via service
svc = get_bot_instance_service()
bots = svc.get_active_bot_instances()
print(f"Service returned {len(bots)} active bots:")
for b in bots:
    print(f"  - {b.id} ({b.status})")

# Print via raw query exactly like the service
db_path = r"c:\Users\favou\OneDrive\Desktop\cosmicforge-bot\backends\shared\shared_lib\persistence\cosmicforge.db"
conn = sqlite3.connect(db_path)
conn.row_factory = sqlite3.Row
cur = conn.cursor()

cur.execute("""
    SELECT bi.id, bi.status, ba.broker_id 
    FROM bot_instances bi 
    LEFT JOIN broker_accounts ba ON bi.broker_account_id = ba.id
    WHERE bi.status IN ('active', 'error')
""")
rows = cur.fetchall()
print(f"Raw query returned {len(rows)} active bots:")
for r in rows:
    print(f"  - {r['id']} ({r['status']})")

cur.execute("SELECT id, status FROM bot_instances WHERE id = 'bot_0064a4b6dd86'")
r = cur.fetchone()
print(f"Direct lookup for bot_0064a4b6dd86: {dict(r) if r else 'NOT FOUND'}")

conn.close()
