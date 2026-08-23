"""
SEV-1 Supplementary Audit — Bot Instances, DOGE EP events, Reconcile events
Read-only. No writes.
"""
import sqlite3, time, sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

DB = r"C:\Users\favou\OneDrive\Desktop\cosmicforge-bot\backends\shared\shared_lib\persistence\cosmicforge.db"
conn = sqlite3.connect(DB)
conn.row_factory = sqlite3.Row
cur = conn.cursor()

def ts(ms):
    if not ms: return "N/A"
    try:
        return time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime(int(ms)/1000))
    except Exception:
        return str(ms)

# --- Bot instances schema ---
cols = [r[1] for r in cur.execute("PRAGMA table_info(bot_instances)").fetchall()]
print("bot_instances columns:", cols)

# Active bot instances
print("\n--- BOT INSTANCES (last 10) ---")
bots = cur.execute("SELECT id, user_id, broker_account_id, status FROM bot_instances ORDER BY created_at DESC LIMIT 10").fetchall()
for r in bots:
    print(f"  id={r['id']} broker_account_id={r['broker_account_id']} status={r['status']}")

# Broker accounts
print("\n--- BROKER ACCOUNTS ---")
bcols = [r[1] for r in cur.execute("PRAGMA table_info(broker_accounts)").fetchall()]
print("  cols:", bcols)
baccs = cur.execute("SELECT id, user_id, exchange FROM broker_accounts LIMIT 10").fetchall()
for r in baccs:
    print(f"  id={r['id']} exchange={r['exchange']}")

# ── DOGE: Full EP Event Timeline ─────────────────────────────────────────────
print("\n--- DOGE ALL EP EVENTS (chronological) ---")
rows = cur.execute(
    "SELECT * FROM entry_protection_events WHERE symbol='DOGEUSDT' ORDER BY ts_ms ASC LIMIT 50"
).fetchall()
for r in rows:
    print(f"  {ts(r['ts_ms'])} | {r['event_type']} | state={r['state']} | submit={r['submit_state']} | reason={r['reason']}")

# ── All pending entries full context ─────────────────────────────────────────
print("\n--- ALL PENDING ENTRIES (current state) ---")
rows = cur.execute("SELECT * FROM pending_entries ORDER BY symbol").fetchall()
for r in rows:
    print(f"  sym={r['symbol']} state={r['state']} submit={r['submit_state']} "
          f"flat_c={r['flat_confirmations']} last_recon={ts(r['last_reconcile_at_ms'])} "
          f"bot_id={r['bot_id']} created={r['created_at']} updated={r['updated_at']}")
    print(f"    intent_key={r['intent_key']} client_order_id={r['client_order_id']}")
    print(f"    broker_order_id={r['broker_order_id']} sized_qty={r['sized_qty']}")

# ── Recent reconcile/close events ────────────────────────────────────────────
print("\n--- RECENT RECONCILE / CLOSE EP EVENTS ---")
rows = cur.execute("""
    SELECT * FROM entry_protection_events
    WHERE event_type IN ('RECONCILE_HELD','MARK_CLOSED','RELEASED','CLOSED')
    ORDER BY ts_ms DESC LIMIT 20
""").fetchall()
for r in rows:
    print(f"  {ts(r['ts_ms'])} | {r['symbol']} | {r['event_type']} | state={r['state']} | reason={r['reason']}")

# ── APE full EP history (chronological) ──────────────────────────────────────
print("\n--- APE ALL EP EVENTS (chronological) ---")
rows = cur.execute(
    "SELECT * FROM entry_protection_events WHERE symbol='APEUSDT' ORDER BY ts_ms ASC LIMIT 20"
).fetchall()
for r in rows:
    print(f"  {ts(r['ts_ms'])} | {r['event_type']} | state={r['state']} | submit={r['submit_state']} | reason={r['reason']}")

# ── APE pending entry details ─────────────────────────────────────────────────
print("\n--- APE PENDING ENTRY DETAILS ---")
rows = cur.execute("SELECT * FROM pending_entries WHERE symbol='APEUSDT'").fetchall()
for r in rows:
    for k in r.keys():
        v = r[k]
        if str(k).endswith("_ms") and v:
            print(f"  {k}: {v} => {ts(v)}")
        else:
            print(f"  {k}: {v}")

# ── bot_symbol_state for active bot only ──────────────────────────────────────
print("\n--- bot_symbol_state for bot_e5fe913972a9 (LIVE BOT) ---")
rows = cur.execute(
    "SELECT * FROM bot_symbol_state WHERE bot_instance_id='bot_e5fe913972a9' ORDER BY symbol"
).fetchall()
for r in rows:
    pos = r['position']
    sym = r['symbol']
    if pos in ("LONG", "SHORT") or sym in ("DOGEUSDT", "APEUSDT", "BTCUSDT", "XRPUSDT"):
        print(f"  {sym}: position={pos} pending_open={r['pending_open']} entry_price={r['entry_price']} qty={r['entry_qty']} updated={r['updated_at']}")

# ── Check what the FIRST DOGE open time was ──────────────────────────────────
print("\n--- DOGE ORIGINAL OPEN_CONFIRMED event ---")
rows = cur.execute(
    "SELECT * FROM entry_protection_events WHERE symbol='DOGEUSDT' AND event_type='OPEN_CONFIRMED' ORDER BY ts_ms ASC LIMIT 5"
).fetchall()
for r in rows:
    print(f"  {ts(r['ts_ms'])} | state={r['state']} | submit={r['submit_state']}")
    print(f"  filled_notional={r['filled_notional']} sized_notional={r['sized_notional']}")
    print(f"  client_order_id={r['client_order_id']}")

conn.close()
print("\n[DONE]")
