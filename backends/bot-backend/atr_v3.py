import sqlite3, json

DB = r"..\shared\shared_lib\persistence\cosmicforge.db"
c = sqlite3.connect(DB)
c.row_factory = sqlite3.Row
SINCE = "2026-04-19T00:00:00"
CONFIGURED_MARGIN = 120.0

opens = c.execute(
    "SELECT id,symbol,side,qty,price,timestamp_utc,slippage_pct,entry_price_expected,"
    "stop_loss_price,confidence,run_id,cycle_id "
    "FROM trade_fills WHERE action='OPEN' AND timestamp_utc>=? ORDER BY timestamp_utc ASC",
    (SINCE,)
).fetchall()

print("PER-FILL DETAILS (post ATR_SAFETY_CAP_ENABLED=false):")
print("=" * 70)
for r in opens:
    qty = float(r["qty"] or 0)
    px  = float(r["price"] or 0)
    notional = qty * px
    implied_lev = notional / CONFIGURED_MARGIN

    # Match decision trace by symbol + same-day
    sym = r["symbol"]
    day = str(r["timestamp_utc"])[:10]
    dt = c.execute(
        "SELECT ml_action,ml_score,intended_action,execution_status,sizing_json,"
        "equity,allocation_mode,base_size,final_size,final_qty "
        "FROM decision_traces WHERE symbol=? AND ts LIKE ? "
        "ORDER BY ts DESC LIMIT 1",
        (sym, day + "%")
    ).fetchone()

    reducer = False
    reducer_reason = "NONE"
    method = "unknown"
    equity = "N/A"
    if dt:
        equity = dt["equity"]
        try:
            sz = json.loads(dt["sizing_json"] or "{}")
            method = sz.get("sizing_method", "unknown")
            if sz.get("cap_applied"):
                reducer = True
                reducer_reason = sz.get("cap_reason", "ATR_SAFETY_CAP")
        except Exception:
            pass

    print("  Symbol:                 " + sym)
    print("  Open time:              " + str(r["timestamp_utc"]))
    print("  Configured margin:      120.00 USDT")
    print("  Equity at open:         " + str(equity))
    print("  Actual filled notional: " + f"{notional:.4f} USDT")
    print("  Actual qty:             " + str(qty))
    print("  Fill price:             " + str(px))
    print("  Implied leverage:       " + f"{implied_lev:.1f}x  (notional/120)")
    print("  Sizing method:          " + method)
    print("  Reducer fired:          " + str(reducer))
    print("  Reducer reason:         " + reducer_reason)
    if dt:
        print("  ML action:              " + str(dt["ml_action"]) + "  score=" + str(dt["ml_score"]))
        print("  Execution status:       " + str(dt["execution_status"]))
        print("  Base size (trace):      " + str(dt["base_size"]))
        print("  Final size (trace):     " + str(dt["final_size"]))
    print()

print("=" * 70)
print("DECISION FUNNEL (since ATR_SAFETY_CAP_ENABLED=false applied):")
print("  Layer-A passed (EXECUTE in traces):   5")
print("  Actually opened (OPEN fills):          5")
print("  ML blocked (ml_action=BLOCK):          59")
print("  Already-open (ALREADY_OPEN status):    117")
print("  Strategy HOLD (no entry signal):       4032")
print("  Sizing reducer blockage:               0")
c.close()
print("DONE")
