import sqlite3

# Connect to database
conn = sqlite3.connect('data/bot.db')
cursor = conn.cursor()

# Get current columns
cursor.execute("PRAGMA table_info(broker_accounts)")
existing_columns = {row[1] for row in cursor.fetchall()}
print(f"Existing columns: {existing_columns}")

# Define required columns based on migrations.py
required_columns = {
    'broker_id': 'TEXT',
    'market_type': 'TEXT',
    'label': 'TEXT',
    'status': 'TEXT',
    'environment': 'TEXT DEFAULT "live"',
    'account_type': 'TEXT',
    'capabilities': 'JSON',
    'masked_key': 'TEXT',
    'last_validated_at': 'TEXT',
    'last_error_code': 'TEXT',
    'last_error_message': 'TEXT',
    'updated_at': 'TEXT'
}

# Add missing columns
added = []
for col_name, col_type in required_columns.items():
    if col_name not in existing_columns:
        print(f"Adding {col_name}...")
        cursor.execute(f"ALTER TABLE broker_accounts ADD COLUMN {col_name} {col_type}")
        added.append(col_name)

if added:
    conn.commit()
    print(f"✓ Added {len(added)} columns: {', '.join(added)}")
else:
    print("✓ All required columns already exist")

conn.close()
print("✓ Database schema fix complete")
