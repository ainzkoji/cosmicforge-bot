"""
Test if client.list_instruments() works
"""
from app.exchange.binance.client import BinanceFuturesClient
from app.core.config import settings

# Create client
client = BinanceFuturesClient(
    api_key=settings.BINANCE_API_KEY or "dummy",
    api_secret=settings.BINANCE_API_SECRET or "dummy",
    base_url=settings.BINANCE_FAPI_BASE_URL
)

print("Testing exchange_info_cached()...")
try:
    ei = client.exchange_info_cached()
    print(f"Exchange info keys: {list(ei.keys())}")
    print(f"Symbols count: {len(ei.get('symbols', []))}")
    print(f"First symbol: {ei.get('symbols', [])[0] if ei.get('symbols') else 'NONE'}")
except Exception as e:
    print(f"ERROR: {e}")
    import traceback
    traceback.print_exc()

print("\nTesting list_instruments()...")
try:
    specs = client.list_instruments()
    print(f"Specs returned: {len(specs)}")
    if specs:
        print(f"First spec: {specs[0]}")
except Exception as e:
    print(f"ERROR: {e}")
    import traceback
    traceback.print_exc()
