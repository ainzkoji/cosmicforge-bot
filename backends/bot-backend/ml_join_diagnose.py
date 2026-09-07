import sys, io, sqlite3
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
DB = r'c:\Users\favou\OneDrive\Desktop\cosmicforge-bot\backends\shared\shared_lib\persistence\cosmicforge.db'
conn = sqlite3.connect(DB, timeout=30)
conn.row_factory = sqlite3.Row

print("=== LINKAGE FAILURE DIAGNOSIS ===")

r = conn.execute("SELECT COUNT(*) t, SUM(CASE WHEN run_id IS NOT NULL AND cycle_id IS NOT NULL THEN 1 ELSE 0 END) lnk FROM trade_fills WHERE action='OPEN'").fetchone()
print(f"OPEN fills: total={r['t']}, with run+cycle_id={r['lnk']}")

print("DT samples (ml_score IS NOT NULL):")
for row in conn.execute("SELECT run_id, cycle_id, symbol FROM decision_traces WHERE ml_score IS NOT NULL ORDER BY ts DESC LIMIT 3").fetchall():
    print(f"  run={repr(row['run_id'])}  cyc={repr(row['cycle_id'])}  sym={row['symbol']}")

print("TF OPEN samples:")
for row in conn.execute("SELECT run_id, cycle_id, symbol FROM trade_fills WHERE action='OPEN' ORDER BY timestamp_utc DESC LIMIT 3").fetchall():
    print(f"  run={repr(row['run_id'])}  cyc={repr(row['cycle_id'])}  sym={row['symbol']}")

dt_keys = {r[0] for r in conn.execute("SELECT run_id||'|'||cycle_id||'|'||symbol FROM decision_traces WHERE ml_score IS NOT NULL AND run_id IS NOT NULL AND cycle_id IS NOT NULL").fetchall()}
tf_keys = {r[0] for r in conn.execute("SELECT run_id||'|'||cycle_id||'|'||symbol FROM trade_fills WHERE action='OPEN' AND run_id IS NOT NULL AND cycle_id IS NOT NULL").fetchall()}
overlap = dt_keys & tf_keys
print(f"Key overlap: DT={len(dt_keys)}, TF={len(tf_keys)}, overlap={len(overlap)}")
if overlap:
    for k in list(overlap)[:5]:
        print(f"  {k}")

# Timestamp proximity join (within 120s)
ts_j = conn.execute("""
    SELECT COUNT(*) FROM trade_fills tfo
    INNER JOIN trade_fills tfc ON tfo.position_id = tfc.position_id
    INNER JOIN decision_traces dt
        ON dt.symbol = tfo.symbol
       AND dt.ml_score IS NOT NULL
       AND ABS(CAST(strftime('%s', dt.ts) AS REAL) - CAST(strftime('%s', tfo.timestamp_utc) AS REAL)) < 120
    WHERE tfo.action = 'OPEN'
      AND tfc.action = 'CLOSE'
      AND tfc.realized_pnl IS NOT NULL
""").fetchone()[0]
print(f"Timestamp-proximity join (120s): {ts_j}")

# Also try 900s (one 15m candle)
ts_j2 = conn.execute("""
    SELECT COUNT(*) FROM trade_fills tfo
    INNER JOIN trade_fills tfc ON tfo.position_id = tfc.position_id
    INNER JOIN decision_traces dt
        ON dt.symbol = tfo.symbol
       AND dt.ml_score IS NOT NULL
       AND ABS(CAST(strftime('%s', dt.ts) AS REAL) - CAST(strftime('%s', tfo.timestamp_utc) AS REAL)) < 900
    WHERE tfo.action = 'OPEN'
      AND tfc.action = 'CLOSE'
      AND tfc.realized_pnl IS NOT NULL
""").fetchone()[0]
print(f"Timestamp-proximity join (900s): {ts_j2}")

# Check if position_id appears in decision_traces at all
pos_in_dt = conn.execute("SELECT COUNT(*) FROM decision_traces WHERE order_id IS NOT NULL").fetchone()[0]
print(f"decision_traces with order_id populated: {pos_in_dt}")

# Check if trade_fills position_id links to decision_traces.order_id
order_link = conn.execute("""
    SELECT COUNT(*) FROM trade_fills tf
    INNER JOIN decision_traces dt ON dt.order_id = tf.order_id
    WHERE tf.action='CLOSE' AND dt.ml_score IS NOT NULL
""").fetchone()[0]
print(f"Join via order_id (CLOSE): {order_link}")

# Sample the timestamps across both tables to understand overlap
dt_range = conn.execute("SELECT MIN(ts), MAX(ts) FROM decision_traces WHERE ml_score IS NOT NULL").fetchone()
tf_range = conn.execute("SELECT MIN(timestamp_utc), MAX(timestamp_utc) FROM trade_fills WHERE action='OPEN'").fetchone()
print(f"DT date range: {dt_range[0]} to {dt_range[1]}")
print(f"TF OPEN date range: {tf_range[0]} to {tf_range[1]}")

# Is the ML scoring recent compared to the trades?
tf_after_ml_start = conn.execute("""
    SELECT COUNT(*) FROM trade_fills WHERE action='OPEN'
    AND timestamp_utc >= '2026-03-28'
""").fetchone()[0]
print(f"OPEN fills after ML start (2026-03-28): {tf_after_ml_start}")

# Trace 64 position_opened=1 runs -- are their run/cycle IDs present in trade_fills?
executed = conn.execute("""
    SELECT run_id, cycle_id, symbol, ts FROM decision_traces
    WHERE ml_score IS NOT NULL AND gate_allowed=1 AND position_opened=1
    LIMIT 10
""").fetchall()
print(f"\nML-scored + position_opened=1 traces ({len(executed)} sampled):")
for r in executed:
    match_count = conn.execute(
        "SELECT COUNT(*) FROM trade_fills WHERE run_id=? AND cycle_id=? AND symbol=? AND action='OPEN'",
        (r['run_id'], r['cycle_id'], r['symbol'])
    ).fetchone()[0]
    print(f"  run={repr(r['run_id'])} cyc={repr(r['cycle_id'])} sym={r['symbol']} ts={r['ts']} -> tf match={match_count}")

conn.close()
print("\nDONE")
