"""Trace the allocation value through the execution flow"""
import sys, os
sys.path.insert(0, os.path.dirname(__file__))

from app.models.bot_instance_models import BotInstanceModel
from app.runner.bot_context import BotRunContext
from app.policy.policy_engine import PolicyContext, compute_budget_usdt
from shared_lib.persistence.db import DB

# 1. Load bot instance from DB
db = DB()
with db.connect() as conn:
    cursor = conn.execute("""
        SELECT * FROM bot_instances WHERE id = 'bot_062f90be64b3'
    """)
    row = cursor.fetchone()

print("=" * 80)
print("STEP 1: Database Bot Instance")
print("=" * 80)
print(f"allocation_type: {row[cursor.description.index(('allocation_type', None))[0]]}")
print(f"allocation_value: {row[cursor.description.index(('allocation_value', None))[0]]}")
print()

# 2. Create BotRunContext
broker_creds = {"api_key": "test", "api_secret": "test", "broker_type": "binance"}

# Mock the instance object
class MockInstance:
    def __init__(self, row_data, cursor):
         # Map row to attributes
        col_names = [desc[0] for desc in cursor.description]
        for i, col in enumerate(col_names):
            setattr(self, col, row_data[i])
        # Ensure lists
        if isinstance(self.symbols, str):
            self.symbols = self.symbols.split(",")
        if isinstance(self.timeframes, str):
            self.timeframes = self.timeframes.split(",")

instance = MockInstance(row, cursor)

context = BotRunContext.from_bot_instance(instance, broker_creds)

print("=" * 80)
print("STEP 2: BotRunContext")
print("=" * 80)
print(f"allocation_type: {context.allocation_type}")
print(f"allocation_value: {context.allocation_value}")
print(f"trade_usdt_per_order: {context.trade_usdt_per_order}")
print()

# 3. Get trade amount settings (what runner uses)
trade_mode, trade_val = context.get_trade_amount_settings()

print("=" * 80)
print("STEP 3: Trade Amount Settings (passed to PolicyContext)")
print("=" * 80)
print(f"trade_amount_mode: {trade_mode}")
print(f"trade_amount_value: {trade_val}")
print()

# 4. Simulate PolicyEngine sizing
print("=" * 80)
print("STEP 4: PolicyEngine Sizing Calculation")
print("=" * 80)

# Simulate compute_budget_usdt call
account_balance = 2400.0  # User's balance
entry_price = 68000.0  # BTC price
atr_notional = 500.0  # Fallback

budget, method, details = compute_budget_usdt(
    mode=trade_mode,
    value=trade_val,
    account_balance=account_balance,
    entry_price=entry_price,
    atr_based_notional=atr_notional,
    min_notional=5.0,
    max_notional=10000.0,
)

print(f"Computed budget: ${budget:.2f}")
print(f"Method: {method}")
print(f"Details: {details}")
print()

print("=" * 80)
print("ANALYSIS")
print("=" * 80)
print(f"Expected: $121")
print(f"Actual: ${budget:.2f}")
print(f"Difference: {budget / 121:.1f}x")
