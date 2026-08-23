import sqlite3, statistics

DB = r"..\shared\shared_lib\persistence\cosmicforge.db"
ACT = "2026-04-18T07:04:00"

conn = sqlite3.connect(DB)
conn.row_factory = sqlite3.Row

print("=== POST-ACTIVATION ML DECISIONS ===")
for r in conn.execute("SELECT ml_action, COUNT(*) c FROM decision_traces WHERE ts>=? AND ml_action IS NOT NULL GROUP BY ml_action", (ACT,)):
    print(f"  {r['ml_action']}: {r['c']}")

print("\n=== SCORE DISTRIBUTION ===")
scores = [float(r[0]) for r in conn.execute("SELECT ml_score FROM decision_traces WHERE ts>=? AND ml_score IS NOT NULL", (ACT,)) if r[0]]
if scores:
    print(f"  n={len(scores)} min={min(scores):.3f} max={max(scores):.3f} mean={statistics.mean(scores):.3f}")
    print(f"  <0.10: {sum(1 for s in scores if s<0.10)} ({sum(1 for s in scores if s<0.10)/len(scores)*100:.1f}%)")
    print(f"  0.10-0.30: {sum(1 for s in scores if 0.10<=s<0.30)} ({sum(1 for s in scores if 0.10<=s<0.30)/len(scores)*100:.1f}%)")
    print(f"  >=0.30: {sum(1 for s in scores if s>=0.30)} ({sum(1 for s in scores if s>=0.30)/len(scores)*100:.1f}%)")

print("\n=== POST-ACTIVATION FILLS (P0 LINKAGE CHECK) ===")
fills = conn.execute("SELECT action, symbol, run_id, cycle_id, timestamp_utc, realized_pnl FROM trade_fills WHERE timestamp_utc>=? ORDER BY timestamp_utc DESC LIMIT 20", (ACT,)).fetchall()
print(f"  Total fills: {len(fills)}")
linked = sum(1 for r in fills if r['run_id'] and r['cycle_id'])
print(f"  With run_id+cycle_id: {linked}/{len(fills)}")
for r in fills:
    tag = "LINKED" if (r['run_id'] and r['cycle_id']) else "NULL"
    print(f"  [{tag}] {r['action']} {r['symbol']} {r['timestamp_utc']} pnl={r['realized_pnl']}")

print("\n=== LINKED JOINS ===")
joins = conn.execute("""
SELECT tf.symbol, tf.action, tf.realized_pnl, dt.ml_score, dt.ml_action
FROM trade_fills tf
JOIN decision_traces dt ON tf.run_id=dt.run_id AND tf.cycle_id=dt.cycle_id AND tf.symbol=dt.symbol
WHERE tf.timestamp_utc>=?
ORDER BY tf.timestamp_utc DESC LIMIT 20
""", (ACT,)).fetchall()
print(f"  Linked rows: {len(joins)}")
for r in joins:
    print(f"  {r['action']} {r['symbol']} score={r['ml_score']} action={r['ml_action']} pnl={r['realized_pnl']}")

print("\n=== PRE-ACTIVATION 7-DAY BASELINE ===")
for r in conn.execute("SELECT ml_action, COUNT(*) c FROM decision_traces WHERE ts>='2026-04-11' AND ts<? AND ml_action IS NOT NULL GROUP BY ml_action", (ACT,)):
    print(f"  {r['ml_action']}: {r['c']}")

conn.close()
