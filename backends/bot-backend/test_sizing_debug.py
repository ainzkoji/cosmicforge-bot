"""Test the updated sizing function with debug logging"""
import sys, os
sys.path.insert(0, os.path.dirname(__file__))

from app.policy.policy_engine import compute_budget_usdt

print("\n" + "=" * 80)
print("TESTING DEBUG LOGGING - Fixed Amount Mode")
print("=" * 80 + "\n")

# This should trigger debug logs showing mode="fixed", value=120
budget, method, details = compute_budget_usdt(
    mode="fixed",
    value=120.0,
    account_balance=2400.0,
    entry_price=68000.0,
    atr_based_notional=500.0,
    min_notional=5.0,
    max_notional=10000.0,
)

print(f"\n✅ Result: budget=${budget:.2f}, method={method}")
print(f"Expected: $120")
print(f"Match: {abs(budget - 120) < 1}")

print("\n" + "=" * 80)
print("TESTING SAFETY CHECK - Simulating Bug")
print("=" * 80 + "\n")

# This should trigger the 5x safety warning if we somehow got 50% mode
budget2, method2, details2 = compute_budget_usdt(
    mode="percent",
    value=50.0,  # 50%
    account_balance=2400.0,
    entry_price=68000.0,
    atr_based_notional=500.0,
    min_notional=5.0,
    max_notional=10000.0,
)

print(f"\n⚠️  Result: budget=${budget2:.2f}, method={method2}")
print(f"This is what happened with the bug (50% of balance)")
