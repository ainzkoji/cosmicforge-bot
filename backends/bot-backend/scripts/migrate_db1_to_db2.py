import os
import sqlite3
import argparse
import sys
from datetime import datetime

def migrate_db(db1_path, db2_path):
    print(f"==================================================")
    print(f" COSMICFORGE DB UNIFICATION MIGRATION SCRIPT")
    print(f"==================================================")
    print(f"Source DB (Runner DB-1): {os.path.abspath(db1_path)}")
    print(f"Target DB (API DB-2):    {os.path.abspath(db2_path)}")
    
    if not os.path.exists(db1_path):
        print(f"❌ Error: Source DB does not exist: {db1_path}")
        return False
        
    if not os.path.exists(db2_path):
        print(f"⚠️ Warning: Target DB does not exist yet. It will be created: {db2_path}")

    # Connect to both DBs
    conn1 = sqlite3.connect(db1_path)
    conn1.row_factory = sqlite3.Row
    
    conn2 = sqlite3.connect(db2_path)
    conn2.row_factory = sqlite3.Row
    
    tables_to_migrate = [
        "decision_traces",
        "trade_fills",
        "bot_symbol_state",
        "position_lifecycle_state",
        "system_audit_log",
        "bot_daily_state"
    ]
    
    print("\n--- Starting Migration ---")
    
    for table_name in tables_to_migrate:
        print(f"\nProcessing table: {table_name}")
        
        # Check if table exists in source
        cur1 = conn1.cursor()
        cur1.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';")
        if not cur1.fetchone():
            print(f"  ⏭️ Skipping: Table '{table_name}' does not exist in source DB.")
            continue
            
        # Get schema from source DB
        cur1.execute(f"PRAGMA table_info({table_name})")
        columns_info = cur1.fetchall()
        columns = [col['name'] for col in columns_info]
        source_cols = set(columns)
        
        # Check if table exists in target DB
        cur2 = conn2.cursor()
        cur2.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';")
        if not cur2.fetchone():
            # Create table in target using source SQL
            cur1.execute(f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{table_name}';")
            create_sql = cur1.fetchone()['sql']
            cur2.execute(create_sql)
            print(f"  🆕 Created table '{table_name}' in target DB.")
            conn2.commit()
            
        # Check columns in target DB
        cur2.execute(f"PRAGMA table_info({table_name})")
        target_cols = set([col['name'] for col in cur2.fetchall()])
        
        # Find intersecting columns and copy data
        common_cols = list(source_cols.intersection(target_cols))
        if not common_cols:
            print(f"  ❌ Error: No common columns for table '{table_name}'. Skipping.")
            continue
            
        col_list = ", ".join(common_cols)
        markers = ", ".join(["?"] * len(common_cols))
        
        # Read from source
        cur1.execute(f"SELECT {col_list} FROM {table_name}")
        rows = cur1.fetchall()
        
        if not rows:
            print(f"  ℹ️ Table '{table_name}' is empty in source. Skipping.")
            continue
            
        print(f"  📦 Read {len(rows)} rows from source.")
        
        # Determine UPSERT strategy based on primary key or unique constraints
        # Since DB1 (runner) has the live data, we want to replace any stale data in DB2
        insert_sql = f"INSERT OR REPLACE INTO {table_name} ({col_list}) VALUES ({markers})"
        
        success = 0
        try:
            # Execute in batches
            for r in rows:
                cur2.execute(insert_sql, [r[col] for col in common_cols])
                success += cur2.rowcount
            conn2.commit()
            print(f"  ✅ Inserted/Updated {success} non-duplicate rows in target.")
        except Exception as e:
            print(f"  ❌ Error migrating table '{table_name}': {e}")
            conn2.rollback()
            
    conn1.close()
    conn2.close()
    print("\n--- Migration Complete ---")
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Migrate data from DB-1 to DB-2")
    parser.add_argument("--source", type=str, required=True, help="Path to source DB (runner DB-1)")
    parser.add_argument("--target", type=str, required=True, help="Path to target DB (API DB-2)")
    
    args = parser.parse_args()
    migrate_db(args.source, args.target)
