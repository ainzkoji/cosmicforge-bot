import sqlite3, time, sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
DB = r"C:\Users\favou\OneDrive\Desktop\cosmicforge-bot\backends\shared\shared_lib\persistence\cosmicforge.db"
conn = sqlite3.connect(DB)
conn.row_factory = sqlite3.Row
cur = conn.cursor()
def ts(ms):
    if not ms: return "N/A"
    return time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime(int(ms)/1000))

print("--- BROKER ACCOUNTS ---")
for r in cur.execute("SELECT id, user_id, broker_id, label, status FROM broker_accounts LIMIT 10").fetchall():
    print(f"  id={r['id']} broker_id={r['broker_id']} label={r['label']} status={r['status']}")

print("\n--- DOGE ALL EP EVENTS (chronological) ---")
for r in cur.execute("SELECT * FROM entry_protection_events WHERE symbol='DOGEUSDT' ORDER BY ts_ms ASC LIMIT 30").fetchall():
    print(f"  {ts(r['ts_ms'])} | {r['event_type']} | state={r['state']} | submit={r['submit_state']} | reason={r['reason']}")

print("\n--- APE ALL EP EVENTS (chronological) ---")
for r in cur.execute("SELECT * FROM entry_protection_events WHERE symbol='APEUSDT' ORDER BY ts_ms ASC LIMIT 15").fetchall():
    print(f"  {ts(r['ts_ms'])} | {r['event_type']} | state={r['state']} | submit={r['submit_state']} | reason={r['reason']}")

print("\n--- APE PENDING ENTRY FULL DETAILS ---")
for r in cur.execute("SELECT * FROM pending_entries WHERE symbol='APEUSDT'").fetchall():
    for k in r.keys():
        v = r[k]
        if str(k).endswith("_ms") and v:
            print(f"  {k}: {v} => {ts(v)}")
        else:
            print(f"  {k}: {v}")

print("\n--- DOGE PENDING ENTRY FULL DETAILS ---")
for r in cur.execute("SELECT * FROM pending_entries WHERE symbol='DOGEUSDT'").fetchall():
    for k in r.keys():
        v = r[k]
        if str(k).endswith("_ms") and v:
            print(f"  {k}: {v} => {ts(v)}")
        else:
            print(f"  {k}: {v}")

print("\n--- RECENT RECONCILE/CLOSE EVENTS ---")
for r in cur.execute("""
    SELECT * FROM entry_protection_events
    WHERE event_type IN ('RECONCILE_HELD','MARK_CLOSED','RELEASED','CLOSED')
    ORDER BY ts_ms DESC LIMIT 20
""").fetchall():
    print(f"  {ts(r['ts_ms'])} | {r['symbol']} | {r['event_type']} | state={r['state']} | reason={r['reason']}")

print("\n--- bot_symbol_state for bot_e5fe913972a9 ---")
for r in cur.execute("SELECT * FROM bot_symbol_state WHERE bot_instance_id='bot_e5fe913972a9' ORDER BY symbol").fetchall():
    pos = r['position']
    sym = r['symbol']
    if pos in ("LONG", "SHORT") or sym in ("DOGEUSDT", "APEUSDT", "BTCUSDT", "XRPUSDT"):
        print(f"  {sym}: position={pos} pending_open={r['pending_open']} entry_price={r['entry_price']} qty={r['entry_qty']} updated={r['updated_at']}")

conn.close()
print("\n[DONE]")
