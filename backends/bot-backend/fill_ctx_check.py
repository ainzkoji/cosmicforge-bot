import sqlite3

DB = r"..\shared\shared_lib\persistence\cosmicforge.db"
c = sqlite3.connect(DB)
c.row_factory = sqlite3.Row

print("OPEN fills run_id / cycle_id / bot_instance_id:")
fills = c.execute(
    "SELECT symbol, timestamp_utc, run_id, cycle_id, bot_instance_id "
    "FROM trade_fills WHERE action='OPEN' ORDER BY timestamp_utc DESC LIMIT 10"
).fetchall()
for r in fills:
    rid = str(r["run_id"])[:20] if r["run_id"] else "NULL"
    cid = str(r["cycle_id"])[:20] if r["cycle_id"] else "NULL"
    bid = str(r["bot_instance_id"])[:20] if r["bot_instance_id"] else "NULL"
    print(f"  {r['timestamp_utc'][:22]} {r['symbol']:12s} run_id={rid}  cycle_id={cid}  bot_inst={bid}")

print("\nBot instances (recent 5):")
bi = c.execute(
    "SELECT id, name, status, bot_instance_id, created_at FROM bot_instances ORDER BY created_at DESC LIMIT 5"
).fetchall()
for r in bi:
    print(f"  id={r['id']} name={r['name']} status={r['status']} bot_instance_id={r['bot_instance_id']}")

print("\nFILL LINKAGE events:")
ev = c.execute(
    "SELECT ts, symbol, event_type, action FROM events "
    "WHERE details_json LIKE '%linkage%' OR details_json LIKE '%LINKAGE%' "
    "ORDER BY ts DESC LIMIT 10"
).fetchall()
print(f"  Found: {len(ev)}")
for r in ev:
    print(f"  {r['ts']} {r['symbol']} {r['event_type']} {r['action']}")

c.close()
