import sqlite3
import argparse
from datetime import datetime, timezone

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def repair(db_path: str, dry_run: bool):
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        
        # Check if table exists
        try:
            bots = conn.execute("SELECT id, status, broker_account_id FROM bot_instances").fetchall()
        except sqlite3.OperationalError as e:
            print(f"Error accessing DB: {e}")
            return
            
        brokers = conn.execute("SELECT id FROM broker_accounts").fetchall()
        broker_ids = {r["id"] for r in brokers}
        
        orphaned_bots = [b for b in bots if b["broker_account_id"] not in broker_ids and b["status"] != 'deleted']
        
        print(f"Total bots: {len(bots)}")
        print(f"Total brokers: {len(brokers)}")
        print(f"Orphaned, non-deleted bots found: {len(orphaned_bots)}")
        
        for ob in orphaned_bots:
            print(f"  - Bot {ob['id']} relies on missing broker {ob['broker_account_id']} (current status: {ob['status']})")
            
        if dry_run:
            print("\nDry run mode enabled. No changes made. Rerun with --fix to apply.")
            return
            
        if not orphaned_bots:
            print("\nNo repairs needed.")
            return
            
        print("\nApplying repairs...")
        now = utc_now_iso()
        repaired_count = 0
        for ob in orphaned_bots:
            # Check if schema supports explicit block reason columns before inserting them
            cols = {r["name"] for r in conn.execute("PRAGMA table_info(bot_instances)").fetchall()}
            
            if "block_category" in cols:
                conn.execute(
                    """
                    UPDATE bot_instances 
                    SET status = 'deleted', 
                        last_error = 'Linked broker account was permanently deleted',
                        broker_health_status = 'broker_blocked',
                        block_category = 'broker_auth_failure',
                        block_reason_code = 'broker_not_found',
                        updated_at = ?
                    WHERE id = ?
                    """,
                    (now, ob["id"])
                )
            elif "broker_health_status" in cols:
                conn.execute(
                    """
                    UPDATE bot_instances 
                    SET status = 'deleted', 
                        last_error = 'Linked broker account was permanently deleted',
                        broker_health_status = 'broker_blocked',
                        broker_error_code = 'broker_not_found',
                        updated_at = ?
                    WHERE id = ?
                    """,
                    (now, ob["id"])
                )
            else:
                conn.execute(
                    """
                    UPDATE bot_instances 
                    SET status = 'deleted', 
                        last_error = 'Linked broker account was permanently deleted (broker_not_found)',
                        updated_at = ?
                    WHERE id = ?
                    """,
                    (now, ob["id"])
                )
            repaired_count += 1
            
        print(f"Successfully repaired {repaired_count} orphaned bots to 'deleted' state.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Repair bot lifecycle inconsistencies")
    parser.add_argument("--db", required=True, help="Path to cosmicforge.db")
    parser.add_argument("--fix", action="store_true", help="Apply fixes")
    args = parser.parse_args()
    repair(args.db, not args.fix)
