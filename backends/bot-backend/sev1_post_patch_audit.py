import sqlite3, sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
DB = r'C:\Users\favou\OneDrive\Desktop\cosmicforge-bot\backends\shared\shared_lib\persistence\cosmicforge.db'
conn = sqlite3.connect(DB)
conn.row_factory = sqlite3.Row

print('=== pending_entries (all active/non-terminal) ===')
rows = conn.execute(
    "SELECT symbol, side, state, flat_confirmations, submit_state, updated_at "
    "FROM pending_entries "
    "WHERE state NOT IN ('FAILED', 'CLOSED') "
    "ORDER BY updated_at DESC"
).fetchall()
if not rows:
    print('  (none -- clean!)')
for r in rows:
    print(f'  {r["symbol"]:15s} {r["side"]:5s} state={r["state"]:20s} flat_c={r["flat_confirmations"]} submit={r["submit_state"]} updated={r["updated_at"]}')

print()
print('=== DOGEUSDT rows (all) ===')
doge = conn.execute("SELECT state, flat_confirmations, updated_at FROM pending_entries WHERE symbol='DOGEUSDT'").fetchall()
if not doge:
    print('  (none -- correctly absent)')
for r in doge:
    print(f'  state={r[0]} flat_c={r[1]} updated={r[2]}')

conn.close()
print()
print('AUDIT COMPLETE')
