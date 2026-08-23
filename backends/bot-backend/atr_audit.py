import sqlite3
import sys

DB = r"..\shared\shared_lib\persistence\cosmicforge.db"
conn = sqlite3.connect(DB)
conn.row_factory = sqlite3.Row

# ── 1. All OPEN fills, most recent first ──────────────────────────────
print("=" * 72)
print("ALL OPEN FILLS (recent 30, newest first)")
print("=" * 72)
rows = conn.execute("""
    SELECT id, symbol, side, action, qty, price,
           timestamp_utc, strategy, confidence,
           slippage_pct, entry_price_expected, stop_loss_price,
           position_id, run_id, cycle_id, bot_instance_id
    FROM trade_fills
    WHERE action = 'OPEN'
    ORDER BY timestamp_utc DESC
    LIMIT 30
""").fetchall()

for r in rows:
    notional = float(r["qty"] or 0) * float(r["price"] or 0)
    linked = "LINKED" if (r["run_id"] and r["cycle_id"]) else "NULL"
    print(f"  [{linked}] {r['timestamp_utc']} | {r['symbol']:12s} | qty={r['qty']} px={r['price']} notional={notional:.2f}")

# ── 2. Bot restart time (from audit log or first run_id after restart) ─
print()
print("=" * 72)
print("BOT LAST RESTART — first audit event by run_id")
print("=" * 72)
audit_rows = conn.execute("""
    SELECT run_id, MIN(ts) as first_ts, MAX(ts) as last_ts, COUNT(*) as events
    FROM audit_log
    WHERE event_type = 'CYCLE_START'
    GROUP BY run_id
    ORDER BY first_ts DESC
    LIMIT 5
""").fetchall()
for r in audit_rows:
    print(f"  run_id={str(r['run_id'])[:16]}... first={r['first_ts']} last={r['last_ts']} cycles={r['events']}")

# ── 3. Decision traces: Layer-A passes and block reasons ──────────────
print()
print("=" * 72)
print("DECISION TRACES — action counts since last run_id")
print("=" * 72)
if audit_rows:
    latest_run_id = audit_rows[0]["run_id"]
    latest_start = audit_rows[0]["first_ts"]
    print(f"  Using run_id={latest_run_id} (started {latest_start})")

    dt_counts = conn.execute("""
        SELECT ml_action, COUNT(*) c
        FROM decision_traces
        WHERE ts >= ?
        GROUP BY ml_action
        ORDER BY c DESC
    """, (latest_start,)).fetchall()
    for r in dt_counts:
        print(f"  ml_action={r['ml_action']}: {r['c']}")

    # Layer A passed (PASSED_LAYER_A in decision traces or audit log)
    passed_rows = conn.execute("""
        SELECT symbol, ts, ml_action, ml_score, decision
        FROM decision_traces
        WHERE ts >= ? AND decision NOT IN ('HOLD', 'SKIP', '')
        ORDER BY ts DESC
        LIMIT 50
    """, (latest_start,)).fetchall()
    print(f"\n  Non-HOLD decisions: {len(passed_rows)}")
    for r in passed_rows:
        print(f"    {r['ts']} | {r['symbol']:12s} | decision={r['decision']} ml={r['ml_action']} score={r['ml_score']}")

    # OPEN fills that happened after latest_start
    print()
    print("=" * 72)
    print(f"OPEN FILLS since restart ({latest_start})")
    print("=" * 72)
    new_opens = conn.execute("""
        SELECT id, symbol, side, qty, price, timestamp_utc,
               slippage_pct, entry_price_expected, stop_loss_price,
               confidence, run_id, cycle_id, strategy
        FROM trade_fills
        WHERE action = 'OPEN' AND timestamp_utc >= ?
        ORDER BY timestamp_utc ASC
    """, (latest_start,)).fetchall()
    print(f"  Count: {len(new_opens)}")
    for r in new_opens:
        notional = float(r["qty"] or 0) * float(r["price"] or 0)
        margin_120 = 120.0
        # leverage = notional / margin (we know margin should be 120)
        implied_lev = notional / margin_120 if margin_120 > 0 else 0
        linked = "LINKED" if (r["run_id"] and r["cycle_id"]) else "NULL"
        print(f"  [{linked}] {r['timestamp_utc']} {r['symbol']:12s}")
        print(f"    qty={r['qty']} price={r['price']} notional={notional:.2f}")
        print(f"    implied_leverage(vs 120 margin)={implied_lev:.1f}x")
        print(f"    entry_expected={r['entry_price_expected']} slippage={r['slippage_pct']}")
        print(f"    sl={r['stop_loss_price']} confidence={r['confidence']}")

conn.close()
print("\nDONE")
