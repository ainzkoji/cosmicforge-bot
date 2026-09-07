"""
Test Binance API authentication
"""
import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

from app.core.config import settings
from app.exchange.binance.client import BinanceFuturesClient

print("=" * 80)
print("TESTING BINANCE API AUTHENTICATION")
print("=" * 80)

print(f"\nAPI Key: {settings.BINANCE_API_KEY[:10]}...{settings.BINANCE_API_KEY[-10:]}")
print(f"Base URL: {settings.BINANCE_FAPI_BASE_URL}")
print(f"Environment: {settings.BINANCE_ENV}")

client = BinanceFuturesClient(
    api_key=settings.BINANCE_API_KEY,
    api_secret=settings.BINANCE_API_SECRET,
    base_url=settings.BINANCE_FAPI_BASE_URL
)

# Test 1: Server time (no auth needed)
print("\n" + "=" * 80)
print("TEST 1: Server Time (Public Endpoint)")
print("=" * 80)
try:
    server_time = client.server_time()
    print(f"✅ SUCCESS: Server time = {server_time['serverTime']}")
except Exception as e:
    print(f"❌ FAILED: {e}")

# Test 2: Account info (needs auth)
print("\n" + "=" * 80)
print("TEST 2: Account Info (Authenticated Endpoint)")
print("=" * 80)
try:
    account = client.account()
    print(f"✅ SUCCESS: Account balance = ${account.get('totalWalletBalance', 'N/A')}")
except Exception as e:
    print(f"❌ FAILED: {e}")
    print("\nPossible reasons:")
    print("  1. API key/secret incorrect")
    print("  2. API key doesn't have futures trading permissions")
    print("  3. IP address not whitelisted (if IP whitelist enabled)")
    print("  4. API key expired or deactivated")

# Test 3: Position risk (what we actually need)
print("\n" + "=" * 80)
print("TEST 3: Position Risk (What bot needs)")
print("=" * 80)
try:
    positions = client.position_risk()
    open_pos = [p for p in positions if float(p.get('positionAmt', 0)) != 0]
    print(f"✅ SUCCESS: Found {len(open_pos)} open positions")
    for pos in open_pos:
        print(f"  - {pos['symbol']}: {pos['positionAmt']}")
except Exception as e:
    print(f"❌ FAILED: {e}")

print("\n" + "=" * 80)
