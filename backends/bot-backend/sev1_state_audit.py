"""
SEV-1 Exchange Truth vs Internal State Audit
Read-only. No writes. No patches.
"""
import sqlite3
import os
import sys
import json
import time
from pathlib import Path

# Fix Windows console encoding
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# ─── Resolve DB path (try known live locations in priority order) ─────────────
CANDIDATES = [
    Path(r"C:\Users\favou\OneDrive\Desktop\cosmicforge-bot\backends\shared\shared_lib\persistence\cosmicforge.db"),
    Path(r"C:\Users\favou\OneDrive\Desktop\cosmicforge-bot\backends\bot-backend\data\cosmicforge.db"),
    Path(r"C:\Users\favou\OneDrive\Desktop\cosmicforge-bot\backends\bot-backend\data\bot.db"),
    Path(r"C:\Users\favou\OneDrive\Desktop\cosmicforge-bot\backends\shared_lib\persistence\cosmicforge.db"),
    Path(r"C:\Users\favou\OneDrive\Desktop\cosmicforge-bot\backends\bot.db"),
]
DB_PATH = None
for c in CANDIDATES:
    if c.exists() and c.stat().st_size > 1000:
        DB_PATH = c
        break
if DB_PATH is None:
    print("FATAL: Could not find a non-empty DB. Tried:", [str(c) for c in CANDIDATES])
    sys.exit(1)

print(f"[AUDIT] Using DB: {DB_PATH}")
print(f"[AUDIT] DB exists: {DB_PATH.exists()}, size: {DB_PATH.stat().st_size if DB_PATH.exists() else 'N/A'} bytes")
print("=" * 70)

conn = sqlite3.connect(str(DB_PATH))
conn.row_factory = sqlite3.Row
cur = conn.cursor()

# ─── Helper ───────────────────────────────────────────────────────────────────
def tables():
    return {r[0] for r in cur.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}

def rows_as_dicts(rows):
    return [dict(r) for r in rows]

def ts_to_str(ms):
    if not ms:
        return "N/A"
    try:
        return time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime(int(ms)/1000))
    except Exception:
        return str(ms)

all_tables = tables()
print(f"[AUDIT] Tables found: {sorted(all_tables)}\n")

FOCUS_SYMBOLS = {"DOGEUSDT", "APEUSDT", "BTCUSDT", "XRPUSDT"}

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 2 — INTERNAL BOT STATE SNAPSHOT
# ═══════════════════════════════════════════════════════════════════════════════
print("=" * 70)
print("SECTION 2 — INTERNAL BOT STATE (bot_symbol_state)")
print("=" * 70)

if "bot_symbol_state" in all_tables:
    rows = cur.execute(
        "SELECT * FROM bot_symbol_state ORDER BY bot_instance_id, symbol"
    ).fetchall()
    bss_data = rows_as_dicts(rows)
    if not bss_data:
        print("  [EMPTY] bot_symbol_state — no rows at all!")
    for r in bss_data:
        sym = r.get("symbol", "?")
        pos = r.get("position", "?")
        print(f"\n  ── {sym} ──")
        for k, v in r.items():
            print(f"     {k}: {v}")
else:
    print("  [MISSING] bot_symbol_state table does not exist!")
    bss_data = []

# ─── SYMBOL_STATE (legacy) ───────────────────────────────────────────────────
print("\n" + "─" * 50)
print("SECTION 2b — LEGACY symbol_state")
print("─" * 50)
if "symbol_state" in all_tables:
    rows = cur.execute("SELECT * FROM symbol_state ORDER BY symbol").fetchall()
    ss_data = rows_as_dicts(rows)
    if not ss_data:
        print("  [EMPTY] symbol_state — no rows")
    for r in ss_data:
        sym = r.get("symbol", "?")
        if sym.upper() in FOCUS_SYMBOLS:
            print(f"\n  ── {sym} ──")
            for k, v in r.items():
                print(f"     {k}: {v}")
else:
    print("  [MISSING] symbol_state table does not exist!")
    ss_data = []

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 2c — ENTRY PROTECTION STATE (pending_entries)
# ═══════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("SECTION 2c — ENTRY INTENT / ENTRY LOCK STATE (pending_entries)")
print("=" * 70)

if "pending_entries" in all_tables:
    rows = cur.execute(
        "SELECT * FROM pending_entries ORDER BY bot_id, symbol"
    ).fetchall()
    pe_data = rows_as_dicts(rows)
    if not pe_data:
        print("  [EMPTY] pending_entries — NO stale entry locks! Table is clean.")
    else:
        print(f"  [FOUND] {len(pe_data)} active entry intent row(s):")
        for r in pe_data:
            sym = r.get("symbol", "?")
            print(f"\n  ── {sym} ──")
            for k, v in r.items():
                if k.endswith("_ms") and v:
                    print(f"     {k}: {v} => {ts_to_str(v)}")
                else:
                    print(f"     {k}: {v}")
else:
    print("  [MISSING] pending_entries table does not exist!")
    pe_data = []

# ─── ENTRY PROTECTION EVENTS (recent) ────────────────────────────────────────
print("\n" + "─" * 50)
print("SECTION 2d — ENTRY PROTECTION EVENTS (last 20 for DOGE/APE/BTC/XRP)")
print("─" * 50)

if "entry_protection_events" in all_tables:
    focus_sql = ", ".join([f"'{s}'" for s in FOCUS_SYMBOLS])
    rows = cur.execute(
        f"""SELECT * FROM entry_protection_events
            WHERE symbol IN ({focus_sql})
            ORDER BY ts_ms DESC LIMIT 20"""
    ).fetchall()
    epe_data = rows_as_dicts(rows)
    if not epe_data:
        print("  [EMPTY] No recent EP events for focus symbols")
    for r in epe_data:
        ts_str = ts_to_str(r.get("ts_ms"))
        print(f"  {ts_str} | {r.get('symbol')} | {r.get('event_type')} | state={r.get('state')} | submit={r.get('submit_state')} | reason={r.get('reason')}")
else:
    print("  [MISSING] entry_protection_events table does not exist!")
    epe_data = []

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 2e — POSITION LIFECYCLE STATE
# ═══════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("SECTION 2e — POSITION LIFECYCLE STATE")
print("=" * 70)

if "position_lifecycle_state" in all_tables:
    rows = cur.execute(
        "SELECT * FROM position_lifecycle_state ORDER BY bot_instance_id, symbol"
    ).fetchall()
    pls_data = rows_as_dicts(rows)
    if not pls_data:
        print("  [EMPTY] position_lifecycle_state — no rows")
    for r in pls_data:
        sym = r.get("symbol", "?")
        print(f"\n  ── {sym} (bot={r.get('bot_instance_id', '?')}) ──")
        for k, v in r.items():
            print(f"     {k}: {v}")
else:
    print("  [MISSING] position_lifecycle_state table does not exist!")
    pls_data = []

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 3 — DOGEUSDT MISMATCH AUDIT
# ═══════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("SECTION 3 — DOGEUSDT MISMATCH AUDIT")
print("=" * 70)

doge_pe = [r for r in pe_data if r.get("symbol", "").upper() == "DOGEUSDT"]
doge_bss = [r for r in bss_data if r.get("symbol", "").upper() == "DOGEUSDT"]
doge_pls = [r for r in pls_data if r.get("symbol", "").upper() == "DOGEUSDT"]

print(f"\n[DOGE] pending_entries rows: {len(doge_pe)}")
for r in doge_pe:
    print(f"   state={r.get('state')} submit_state={r.get('submit_state')} flat_confirmations={r.get('flat_confirmations')} bot_id={r.get('bot_id')}")
    print(f"   created_at={r.get('created_at')} updated_at={r.get('updated_at')}")
    print(f"   last_reconcile_at_ms: {ts_to_str(r.get('last_reconcile_at_ms'))}")
    age_s = None
    if r.get("submitted_at_ms"):
        age_s = (time.time() * 1000 - r["submitted_at_ms"]) / 1000
        print(f"   Age since submitted_at_ms: {age_s:.0f}s")

print(f"\n[DOGE] bot_symbol_state rows: {len(doge_bss)}")
for r in doge_bss:
    print(f"   position={r.get('position')} pending_open={r.get('pending_open')} entry_price={r.get('entry_price')} entry_qty={r.get('entry_qty')}")
    print(f"   updated_at={r.get('updated_at')} bot_instance_id={r.get('bot_instance_id')}")

print(f"\n[DOGE] position_lifecycle_state rows: {len(doge_pls)}")
for r in doge_pls:
    print(f"   phase={r.get('phase')} current_stop={r.get('current_stop')} original_tp2={r.get('original_tp2')} updated_at={r.get('updated_at')}")

# EP events for DOGE
print("\n[DOGE] Last 10 EP events:")
if "entry_protection_events" in all_tables:
    rows = cur.execute(
        "SELECT * FROM entry_protection_events WHERE symbol='DOGEUSDT' ORDER BY ts_ms DESC LIMIT 10"
    ).fetchall()
    for r in rows:
        print(f"   {ts_to_str(r['ts_ms'])} | {r['event_type']} | state={r['state']} | submit={r['submit_state']} | reason={r['reason']}")

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 4 — APEUSDT TP/SL MISMATCH AUDIT
# ═══════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("SECTION 4 — APEUSDT TP/SL MISMATCH AUDIT")
print("=" * 70)

ape_pe = [r for r in pe_data if r.get("symbol", "").upper() == "APEUSDT"]
ape_bss = [r for r in bss_data if r.get("symbol", "").upper() == "APEUSDT"]
ape_pls = [r for r in pls_data if r.get("symbol", "").upper() == "APEUSDT"]

print(f"\n[APE] pending_entries rows: {len(ape_pe)}")
for r in ape_pe:
    print(f"   state={r.get('state')} submit_state={r.get('submit_state')} bot_id={r.get('bot_id')}")

print(f"\n[APE] bot_symbol_state rows: {len(ape_bss)}")
for r in ape_bss:
    print(f"   position={r.get('position')} pending_open={r.get('pending_open')}")
    print(f"   bot_instance_id={r.get('bot_instance_id')} updated_at={r.get('updated_at')}")

print(f"\n[APE] position_lifecycle_state rows: {len(ape_pls)}")
for r in ape_pls:
    print(f"   phase={r.get('phase')} current_stop={r.get('current_stop')}")
    print(f"   original_tp1={r.get('original_tp1')} original_tp2={r.get('original_tp2')}")
    print(f"   sl_order_id={r.get('sl_order_id')} tp_order_id={r.get('tp_order_id')}")
    print(f"   updated_at={r.get('updated_at')}")

print("\n[APE] Last 10 EP events:")
if "entry_protection_events" in all_tables:
    rows = cur.execute(
        "SELECT * FROM entry_protection_events WHERE symbol='APEUSDT' ORDER BY ts_ms DESC LIMIT 10"
    ).fetchall()
    for r in rows:
        print(f"   {ts_to_str(r['ts_ms'])} | {r['event_type']} | state={r['state']} | submit={r['submit_state']} | reason={r['reason']}")

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 5 — ACCOUNT CONTEXT CONSISTENCY AUDIT
# ═══════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("SECTION 5 — ACCOUNT / CONTEXT CONSISTENCY AUDIT")
print("=" * 70)

# Get all distinct bot_instance_ids referenced across tables
print("\n[CONTEXT] Distinct bot_instance_ids referenced across system:")
for tbl, col in [
    ("bot_symbol_state", "bot_instance_id"),
    ("pending_entries", "bot_id"),
    ("position_lifecycle_state", "bot_instance_id"),
    ("bot_daily_state", "bot_instance_id"),
]:
    if tbl in all_tables:
        rows = cur.execute(f"SELECT DISTINCT {col} FROM {tbl}").fetchall()
        ids = [r[0] for r in rows]
        print(f"  {tbl}.{col}: {ids}")
    else:
        print(f"  {tbl} — TABLE MISSING")

# Bot instances from bot_instances table
if "bot_instances" in all_tables:
    print("\n[BOT INSTANCES TABLE]:")
    rows = cur.execute(
        "SELECT id, user_id, broker_account_id, status, name FROM bot_instances ORDER BY created_at DESC LIMIT 10"
    ).fetchall()
    for r in rows:
        print(f"  id={r['id']} broker_account_id={r['broker_account_id']} status={r['status']} name={r['name']}")

# Broker accounts
if "broker_accounts" in all_tables:
    print("\n[BROKER ACCOUNTS]:")
    rows = cur.execute(
        "SELECT id, user_id, exchange, label FROM broker_accounts LIMIT 10"
    ).fetchall()
    for r in rows:
        print(f"  id={r['id']} exchange={r['exchange']} label={r['label']}")

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 6 — RECONCILIATION FAILURE CLASSIFICATION (Evidence)
# ═══════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("SECTION 6 — RECONCILIATION FAILURE CLASSIFICATION")
print("=" * 70)

# Check flat_confirmations for all pending entries
print("\n[RECON] Flat confirmation counts (OPEN_CONFIRMED entries):")
for r in pe_data:
    sym = r.get("symbol", "?")
    state = r.get("state", "?")
    flat_c = r.get("flat_confirmations", 0)
    sub = r.get("submit_state", "?")
    last_recon = ts_to_str(r.get("last_reconcile_at_ms"))
    print(f"  {sym}: state={state} submit={sub} flat_confirmations={flat_c} last_reconcile={last_recon}")

# Check if reconcile is being called — look at reconcile events
print("\n[RECON] Recent RECONCILE / CLOSED events (all symbols):")
if "entry_protection_events" in all_tables:
    rows = cur.execute("""
        SELECT * FROM entry_protection_events
        WHERE event_type IN ('RECONCILE_HELD','CLOSED','MARK_CLOSED','RELEASED','OPEN_CONFIRMED')
        ORDER BY ts_ms DESC LIMIT 20
    """).fetchall()
    for r in rows:
        print(f"  {ts_to_str(r['ts_ms'])} | {r['symbol']} | {r['event_type']} | state={r['state']} | flat_conf={r.get('reason')}")

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 7 — CURRENT SAFETY STATUS (from DB perspective)
# ═══════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("SECTION 7 — CURRENT DB SAFETY VIEW OF ALL LIVE POSITIONS")
print("=" * 70)

# Combine bot_symbol_state + pending_entries + position_lifecycle_state
open_positions = [r for r in bss_data if r.get("position") in ("LONG", "SHORT")]
if not open_positions:
    print("  [INFO] No positions tracked as LONG/SHORT in bot_symbol_state")

for r in open_positions:
    sym = r.get("symbol", "?")
    bid = r.get("bot_instance_id", "?")
    pos = r.get("position", "?")
    ep = r.get("entry_price")
    qty = r.get("entry_qty")
    print(f"\n  ── {sym} ({pos}) ──")
    print(f"     bot_instance_id: {bid}")
    print(f"     entry_price: {ep}  qty: {qty}")
    print(f"     pending_open: {r.get('pending_open')}")
    print(f"     position_id: {r.get('position_id')}")
    print(f"     updated_at: {r.get('updated_at')}")

    # Check lifecycle state for SL/TP
    lc = [l for l in pls_data if l.get("symbol", "").upper() == sym.upper() and l.get("bot_instance_id") == bid]
    if lc:
        lc0 = lc[0]
        has_sl_db = lc0.get("current_stop") not in (None, 0, 0.0, "")
        has_tp_db = lc0.get("original_tp2") not in (None, 0, 0.0, "")
        print(f"     [LIFECYCLE] phase={lc0.get('phase')} sl={lc0.get('current_stop')} tp2={lc0.get('original_tp2')}")
        print(f"     [LIFECYCLE] has_sl_in_db={has_sl_db} has_tp_in_db={has_tp_db}")
    else:
        print(f"     [LIFECYCLE] NO lifecycle state row found!")

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 8 — FINAL OUTPUT SUMMARY
# ═══════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("SECTION 8 — FINAL SUMMARY")
print("=" * 70)

# DOGE summary
doge_pe_open = [r for r in doge_pe if r.get("state") in ("OPEN_CONFIRMED", "PENDING_OPEN")]
doge_bss_open = [r for r in doge_bss if r.get("position") in ("LONG", "SHORT")]
print(f"\n[Q1] Is DOGEUSDT actually open in DB?")
print(f"     pending_entries with OPEN/PENDING state: {len(doge_pe_open)} rows")
print(f"     bot_symbol_state with LONG/SHORT: {len(doge_bss_open)} rows")

print(f"\n[Q2] Why does bot think DOGEUSDT is locked/open?")
if doge_pe_open:
    for r in doge_pe_open:
        flat_c = r.get("flat_confirmations", 0)
        sub = r.get("submit_state")
        req_conf = 3  # FLAT_CONFIRMATIONS_REQUIRED
        print(f"     pending_entries has state={r['state']} flat_confirmations={flat_c}/{req_conf}")
        if flat_c < req_conf:
            print(f"     → VERDICT: flat_confirmations ({flat_c}) < threshold (3). Reconcile has NOT cleared it yet.")
        print(f"     → last_reconcile_at: {ts_to_str(r.get('last_reconcile_at_ms'))}")
elif doge_bss_open:
    print(f"     bot_symbol_state shows {doge_bss_open[0].get('position')} but pending_entries empty")
    print(f"     → VERDICT: Stale bot_symbol_state; entry lock was cleared but symbol state not updated")
else:
    print(f"     → VERDICT: No stale data found in either table. Mismatch may have been in-memory only (not persisted).")

# APE summary
ape_pls_sl = None
ape_pls_tp = None
if ape_pls:
    ape_pls_sl = ape_pls[0].get("current_stop")
    ape_pls_tp = ape_pls[0].get("original_tp2")
print(f"\n[Q3] Does APEUSDT have both TP and SL in DB?")
print(f"     lifecycle_state sl (current_stop): {ape_pls_sl}")
print(f"     lifecycle_state tp (original_tp2): {ape_pls_tp}")
has_sl_db = ape_pls_sl not in (None, 0, 0.0, "")
has_tp_db = ape_pls_tp not in (None, 0, 0.0, "")
print(f"     → has_sl_in_db={has_sl_db} has_tp_in_db={has_tp_db}")

print(f"\n[Q4] Why did bot log has_tp=False for APEUSDT?")
print(f"     The protection check in executor.py:check_and_repair_protection():")
print(f"     1. Scans open_orders() → checks for TAKE_PROFIT_MARKET/TAKE_PROFIT types")
print(f"     2. Scans get_algo_orders() → checks for TP / TAKE_PROFIT_MARKET type with triggerPrice")
print(f"     Binance stores exchange-managed TP/SL (Position TP/SL) differently from regular orders.")
print(f"     Position-level TP/SL set via the Binance UI (not via API orders) does NOT appear")
print(f"     in open_orders() OR in /fapi/v1/algoOrders — they live in position risk data only.")
print(f"     → VERDICT: If TP was set via Binance UI as a position TP/SL, the bot cannot see it")
print(f"       via order endpoints. This is a PROTECTION PARSER FALSE NEGATIVE.")

print(f"\n[Q5] Primary root cause classification:")
print(f"     Likely MULTIPLE CAUSES:")
print(f"     A) DOGEUSDT: STALE ENTRY INTENT STATE or RECONCILIATION NOT CLEARING CLOSED POSITIONS")
print(f"     B) APEUSDT: PROTECTION PARSER FALSE NEGATIVE (position-level TP/SL not visible via order API)")

print(f"\n[Q6] Files/functions to patch next:")
print(f"     1. entry_protection.py → reconcile_entry(): flat_confirmations threshold may need dynamic adjustment")
print(f"        FLAT_CONFIRMATIONS_REQUIRED=3 — if reconcile is called infrequently, DOGE can stay stuck")
print(f"     2. executor.py → check_and_repair_protection(): missing position-level TP/SL detection")
print(f"        Need to check positionRisk 'stopPrice' / 'takeProfitOnFill' fields for position-level orders")
print(f"     3. runner.py → _reconcile_entry_protection(): verify it's called on every cycle for all symbols")

conn.close()
print("\n" + "=" * 70)
print("[AUDIT COMPLETE] Read-only. No changes made.")
print("=" * 70)
