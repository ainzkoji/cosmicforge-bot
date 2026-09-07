"""Diagnostic + fix script for bot mode and recent trade decisions."""
import sys, json
sys.path.insert(0, ".")
from dotenv import load_dotenv
load_dotenv()

from shared_lib.persistence.db import DB

db = DB()

with db.connect() as conn:
    # ---- Show current state ----
    print("=== CURRENT BOT INSTANCES ===")
    cols = [c[1] for c in conn.execute("PRAGMA table_info(bot_instances)").fetchall()]
    for row in conn.execute("SELECT * FROM bot_instances").fetchall():
        d = dict(zip(cols, row))
        print(f"  id={d['id']}  mode={d['mode']}  strategy={d['strategy_id']}  risk={d['risk_level']}")

    print()

    # ---- Fix mode ----
    result = conn.execute(
        "UPDATE bot_instances SET mode='live' WHERE mode='paper'"
    )
    conn.commit()
    print(f"=== UPDATED {result.rowcount} bot(s) from paper -> live ===")

    print()

    # ---- Confirm ----
    print("=== AFTER UPDATE ===")
    for row in conn.execute("SELECT id, mode FROM bot_instances").fetchall():
        print(f"  id={row[0]}  mode={row[1]}")

    print()

    # ---- Recent decision codes (last 30) ----
    print("=== LAST 30 DECISION EVENTS ===")
    try:
        rows = conn.execute("""
            SELECT symbol, action, details, created_at
            FROM audit_log
            WHERE event_type='DECISION'
            ORDER BY created_at DESC
            LIMIT 30
        """).fetchall()
        for r in rows:
            try:
                det = json.loads(r[2] or "{}")
            except Exception:
                det = {}
            sig = det.get("signal", "?")
            pol = det.get("policy_action", det.get("sub_reason", det.get("reason", "?")))
            print(f"  {str(r[3])[:19]}  {str(r[0]):12s}  {str(r[1]):30s}  sig={sig}  policy={pol}")
    except Exception as e:
        print(f"  audit_log error: {e}")

    print()

    # ---- Recent strategy signals to check if signals generate ----
    print("=== LAST 20 STRATEGY_SIGNAL EVENTS ===")
    try:
        rows = conn.execute("""
            SELECT symbol, action, details, created_at
            FROM audit_log
            WHERE event_type='STRATEGY_SIGNAL'
            ORDER BY created_at DESC
            LIMIT 20
        """).fetchall()
        for r in rows:
            try:
                det = json.loads(r[2] or "{}")
            except Exception:
                det = {}
            conf = det.get("confidence", "?")
            print(f"  {str(r[3])[:19]}  {str(r[0]):12s}  action={r[1]:6s}  conf={conf}")
    except Exception as e:
        print(f"  audit_log error: {e}")

print("\nDone.")
