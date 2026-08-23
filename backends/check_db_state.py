import sqlite3
import os

db_path = r"shared\shared_lib\persistence\cosmicforge.db"
conn = sqlite3.connect(db_path)
conn.row_factory = sqlite3.Row

print("=" * 60)
print(f"DB: {os.path.abspath(db_path)}")
print("=" * 60)

tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").fetchall()
print("\nTables:")
for t in tables:
    print(f"  {t[0]}")

print("\nKey counts:")
for tbl in ["decision_traces", "trade_fills"]:
    try:
        n = conn.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
        print(f"  {tbl}: {n}")
    except Exception as e:
        print(f"  {tbl}: ERROR - {e}")

try:
    opens = conn.execute("SELECT COUNT(*) FROM trade_fills WHERE action='OPEN'").fetchone()[0]
    closes = conn.execute("SELECT COUNT(*) FROM trade_fills WHERE action='CLOSE'").fetchone()[0]
    print(f"  trade_fills OPEN: {opens}")
    print(f"  trade_fills CLOSE: {closes}")
except Exception as e:
    print(f"  trade_fills breakdown: {e}")

try:
    cols = conn.execute("PRAGMA table_info(decision_traces)").fetchall()
    print(f"\ndecision_traces columns ({len(cols)}):")
    for c in cols:
        print(f"  {c[1]} ({c[2]})")
except Exception as e:
    print(f"\ndecision_traces schema: ERROR - {e}")

try:
    cols2 = conn.execute("PRAGMA table_info(trade_fills)").fetchall()
    print(f"\ntrade_fills columns ({len(cols2)}):")
    for c in cols2:
        print(f"  {c[1]} ({c[2]})")
except Exception as e:
    print(f"\ntrade_fills schema: ERROR - {e}")

conn.close()
