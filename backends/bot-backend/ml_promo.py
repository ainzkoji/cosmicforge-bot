import sqlite3, statistics
DB = r"..\shared\shared_lib\persistence\cosmicforge.db"
ACT = "2026-04-18T07:04:00"
conn = sqlite3.connect(DB)
conn.row_factory = sqlite3.Row

print("POST-ACTIVATION ML DECISIONS:")
for r in conn.execute(
    "SELECT ml_action, COUNT(*) c FROM decision_traces WHERE ts>=? AND ml_action IS NOT NULL GROUP BY ml_action ORDER BY c DESC",
    (ACT,)
):
    print(f"  {r[0]}: {r[1]}")

scores = [float(r[0]) for r in conn.execute(
    "SELECT ml_score FROM decision_traces WHERE ts>=? AND ml_score IS NOT NULL", (ACT,)
) if r[0]]
print(f"SCORES: n={len(scores)}", end="")
if scores:
    print(f" min={min(scores):.3f} max={max(scores):.3f} mean={statistics.mean(scores):.3f}")
    b1 = sum(1 for s in scores if s < 0.10)
    b2 = sum(1 for s in scores if 0.10 <= s < 0.30)
    b3 = sum(1 for s in scores if s >= 0.30)
    print(f"  <0.10={b1}({b1/len(scores)*100:.1f}%)  0.10-0.30={b2}({b2/len(scores)*100:.1f}%)  >=0.30={b3}({b3/len(scores)*100:.1f}%)")
else:
    print()

print("FILLS POST-ACTIVATION:")
fills = conn.execute(
    "SELECT action,symbol,run_id,cycle_id,timestamp_utc,realized_pnl FROM trade_fills WHERE timestamp_utc>=? ORDER BY timestamp_utc DESC LIMIT 15",
    (ACT,)
).fetchall()
linked_count = sum(1 for r in fills if r["run_id"] and r["cycle_id"])
print(f"  total={len(fills)} linked={linked_count}")
for r in fills:
    tag = "LINK" if (r["run_id"] and r["cycle_id"]) else "NULL"
    print(f"  [{tag}] {r['action']} {r['symbol']} {r['timestamp_utc']} pnl={r['realized_pnl']}")

print("JOINS (outcome linkage):")
j = conn.execute("""
SELECT tf.symbol, tf.action, tf.realized_pnl, dt.ml_score, dt.ml_action
FROM trade_fills tf
JOIN decision_traces dt
    ON tf.run_id = dt.run_id
    AND tf.cycle_id = dt.cycle_id
    AND tf.symbol = dt.symbol
WHERE tf.timestamp_utc >= ?
LIMIT 20
""", (ACT,)).fetchall()
print(f"  linked_joins={len(j)}")
for r in j:
    print(f"  {r['action']} {r['symbol']} score={r['ml_score']} action={r['ml_action']} pnl={r['realized_pnl']}")

print("PRE-ACT 7-DAY BASELINE:")
for r in conn.execute(
    "SELECT ml_action, COUNT(*) c FROM decision_traces WHERE ts>='2026-04-11' AND ts<? AND ml_action IS NOT NULL GROUP BY ml_action ORDER BY c DESC",
    (ACT,)
):
    print(f"  {r[0]}: {r[1]}")

conn.close()
print("DONE")
