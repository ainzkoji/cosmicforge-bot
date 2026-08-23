import sqlite3, json

DB = r"..\shared\shared_lib\persistence\cosmicforge.db"
c = sqlite3.connect(DB)
c.row_factory = sqlite3.Row
MARGIN = 120.0  # configured per-trade margin

# ── 1. Latest restart ─────────────────────────────────────────────────
ev = c.execute(
    "SELECT run_id, MIN(ts) as ft, COUNT(*) as cy FROM events "
    "WHERE event_type='CYCLE_START' GROUP BY run_id ORDER BY ft DESC LIMIT 4"
).fetchall()
print("RESTART HISTORY:")
for r in ev:
    print(f"  run_id={str(r['run_id'])[:16]}... first={r['ft']} cycles={r['cy']}")
since = ev[0]["ft"] if ev else "2026-04-19T00:00:00"
print(f"\nReport window: since {since}\n")

# ── 2. OPEN fills since restart ───────────────────────────────────────
opens = c.execute(
    "SELECT id,symbol,side,qty,price,timestamp_utc,"
    "slippage_pct,entry_price_expected,stop_loss_price,confidence,"
    "run_id,cycle_id,order_id "
    "FROM trade_fills WHERE action='OPEN' AND timestamp_utc>=? ORDER BY timestamp_utc ASC",
    (since,)
).fetchall()
print(f"=== OPEN FILLS SINCE ATR_SAFETY_CAP_ENABLED=false ===")
print(f"Count: {len(opens)}\n")

for r in opens:
    qty = float(r["qty"] or 0)
    px  = float(r["price"] or 0)
    notional = qty * px
    margin_actual = notional / 20.0  # assume leverage 20x as configured default
    linked = "LINKED" if (r["run_id"] and r["cycle_id"]) else "NULL_CONTEXT"

    # Try to join decision_trace
    dt = None
    if r["run_id"] and r["cycle_id"]:
        dt = c.execute(
            "SELECT allocation_mode,base_size,final_size,final_qty,sizing_json,"
            "ml_action,ml_score,intended_action,execution_status,rejection_reason,"
            "equity,confidence "
            "FROM decision_traces WHERE run_id=? AND cycle_id=? AND symbol=? LIMIT 1",
            (r["run_id"], r["cycle_id"], r["symbol"])
        ).fetchone()

    reducer_fired = False
    reducer_reason = "N/A"
    sizing_method = "unknown"
    cap_applied = False
    base_sz = MARGIN
    final_sz = None

    if dt:
        sizing_method = "from_trace"
        base_sz = float(dt["base_size"] or MARGIN)
        final_sz = float(dt["final_size"] or 0)
        try:
            sz = json.loads(dt["sizing_json"] or "{}")
            cap_applied = bool(sz.get("cap_applied"))
            sizing_method = sz.get("sizing_method", "fixed_absolute_margin")
            if cap_applied:
                reducer_fired = True
                reducer_reason = sz.get("cap_reason", "ATR_SAFETY_CAP")
        except Exception:
            pass

    # Infer leverage from notional / margin
    implied_lev = notional / MARGIN if MARGIN > 0 else 0

    print(f"[{linked}] {r['timestamp_utc']} | {r['symbol']}")
    print(f"  configured_margin = {MARGIN:.2f} USDT")
    print(f"  actual_notional   = {notional:.4f} USDT")
    print(f"  actual_qty        = {qty}")
    print(f"  fill_price        = {px}")
    print(f"  implied_leverage  = {implied_lev:.1f}x")
    print(f"  sizing_method     = {sizing_method}")
    print(f"  base_size(trace)  = {base_sz}")
    print(f"  final_size(trace) = {final_sz}")
    print(f"  reducer_fired     = {reducer_fired}")
    print(f"  reducer_reason    = {reducer_reason}")
    if dt:
        print(f"  ml_action         = {dt['ml_action']}  ml_score={dt['ml_score']}")
        print(f"  equity_at_open    = {dt['equity']}")
    print(f"  slippage_pct      = {r['slippage_pct']}")
    print(f"  entry_expected    = {r['entry_price_expected']}")
    print(f"  stop_loss_price   = {r['stop_loss_price']}")
    print()

# ── 3. Decision trace summary ─────────────────────────────────────────
print("=== DECISION TRACE SUMMARY (since restart) ===")
dt_rows = c.execute(
    "SELECT intended_action, ml_action, execution_status, rejection_reason, "
    "       COUNT(*) as n "
    "FROM decision_traces WHERE ts>=? "
    "GROUP BY intended_action, ml_action, execution_status "
    "ORDER BY n DESC LIMIT 30",
    (since,)
).fetchall()

layer_a_passed = 0
opened_count = 0
ml_blocked = 0
already_open_blocked = 0
entry_lock_blocked = 0
sizing_reducer_blocked = 0

for r in dt_rows:
    ia = (r["intended_action"] or "").upper()
    ml = (r["ml_action"] or "").upper()
    ex = (r["execution_status"] or "").upper()
    rr = (r["rejection_reason"] or "").upper()
    n = r["n"]

    # Count Layer A passed = any OPEN_* action
    if "OPEN" in ia:
        layer_a_passed += n
    if "OPEN" in ia and ex in ("SUCCESS", "FILLED", "OPENED"):
        opened_count += n
    if ml == "BLOCK":
        ml_blocked += n
    if "MAX_POSITIONS" in rr or "ALREADY_OPEN" in rr or "POSITION_OPEN" in rr:
        already_open_blocked += n
    if "ENTRY_LOCK" in rr or "LOCK" in rr:
        entry_lock_blocked += n
    if "SIZE" in rr or "NOTIONAL" in rr or "MIN_NOTIONAL" in rr:
        sizing_reducer_blocked += n

    print(f"  action={ia:20s} ml={ml:8s} ex={ex:15s} n={n}  rr={r['rejection_reason'] or ''}")

print(f"\n--- COUNTS ---")
print(f"Layer-A passed (OPEN_* intended_action): {layer_a_passed}")
print(f"Actually opened (OPEN + SUCCESS):         {len(opens)}  (from trade_fills)")
print(f"ML blocked (ml_action=BLOCK):             {ml_blocked}")
print(f"Already-open/orch hold blocked:           {already_open_blocked}")
print(f"Entry lock blocked:                       {entry_lock_blocked}")
print(f"Sizing reducer blocked:                   {sizing_reducer_blocked}")

# Size reducer details from sizing_json
print("\n=== ATR CAP STATUS IN ALL POST-RESTART DECISIONS ===")
cap_rows = c.execute(
    "SELECT symbol, ts, allocation_mode, base_size, final_size, sizing_json "
    "FROM decision_traces WHERE ts>=? AND intended_action LIKE '%OPEN%' "
    "ORDER BY ts DESC LIMIT 40",
    (since,)
).fetchall()
cap_fired = 0
no_cap = 0
for r in cap_rows:
    try:
        sz = json.loads(r["sizing_json"] or "{}")
        capped = bool(sz.get("cap_applied"))
        method = sz.get("sizing_method", "?")
        if capped:
            cap_fired += 1
            print(f"  [CAP FIRED] {r['ts']} {r['symbol']} base={r['base_size']} final={r['final_size']} method={method}")
        else:
            no_cap += 1
    except Exception:
        no_cap += 1
print(f"\n  cap_applied=True: {cap_fired}")
print(f"  cap_applied=False (or no trace): {no_cap}")

c.close()
print("\nDONE")
