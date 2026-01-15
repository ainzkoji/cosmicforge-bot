import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from app.persistence.db import DB
from app.core.strategy_service import StrategyService

# Test the service layer directly
db = DB()
service = StrategyService(db)

print("Testing list_strategies with no user_id:")
strategies = service.list_strategies(user_id=None, filters={}, limit=10)
print(f"Found {len(strategies)} strategies")
for s in strategies:
    print(f"  - {s['name']} (visibility: {s['visibility']}, status: {s['status']})")

print("\nTesting list_strategies with dummy user_id:")
strategies = service.list_strategies(user_id="user_123", filters={}, limit=10)
print(f"Found {len(strategies)} strategies")
for s in strategies:
    print(f"  - {s['name']} (visibility: {s['visibility']}, status: {s['status']})")
