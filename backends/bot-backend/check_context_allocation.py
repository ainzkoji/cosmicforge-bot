"""Check what allocation settings the RUNNING bot has loaded"""
import sys, os
sys.path.insert(0, os.path.dirname(__file__))

from shared_lib.persistence.db import DB
from app.runner.bot_context import BotRunContext

# Load the actual bot instance
db = DB()
with db.connect() as conn:
    cursor = conn.execute("""
        SELECT * FROM bot_instances WHERE id = 'bot_062f90be64b3'
    """)
    row = cursor.fetchone()
    col_names = [desc[0] for desc in cursor.description]

print("=" * 80)
print("DATABASE VALUES")
print("=" * 80)
for i, col in enumerate(col_names):
    if 'alloc' in col.lower() or 'capital' in col.lower():
        print(f"{col}: {row[i]}")
print()

# Map row to mock instance object
class MockInstance:
    pass

instance = MockInstance()
for i, col in enumerate(col_names):
    setattr(instance, col, row[i])

# Ensure lists
if isinstance(instance.symbols, str):
    instance.symbols = instance.symbols.split(",")
if isinstance(instance.timeframes, str):
    instance.timeframes = instance.timeframes.split(",")

# Create context (what bot actually uses)
broker_creds = {
    "api_key": "test",
    "api_secret": "test",
    "broker_type": "binance"
}

context = BotRunContext.from_bot_instance(instance, broker_creds)

print("=" * 80)
print("BOT CONTEXT (What runner loaded)")
print("=" * 80)
print(f"allocation_type: {context.allocation_type}")
print(f"allocation_value: {context.allocation_value}")
print(f"trade_usdt_per_order: {context.trade_usdt_per_order}")
print()

# Get trade settings (what PolicyEngine receives)
mode, val = context.get_trade_amount_settings()

print("=" * 80)
print("POLICY ENGINE INPUT")
print("=" * 80)
print(f"trade_amount_mode: {mode}")
print(f"trade_amount_value: {val}")
print()

print("=" * 80)
print("🔍 ROOT CAUSE CHECK")
print("=" * 80)

if mode == "percent" and val == 50:
    print("❌ BUG CONFIRMED: Bot thinks it should use 50% of balance!")
    print("   Database says: fixed_amount, $121")
    print("   Context loaded: percent, 50%")
    print("\n   WHERE: Check BotRunContext.from_bot_instance() mapping logic")
elif mode == "fixed" and val == 120:
    print("✅ Context is CORRECT")
    print("   Bug must be in PolicyEngine or later in execution chain")
else:
    print(f"⚠️  Unexpected: mode={mode}, val={val}")
