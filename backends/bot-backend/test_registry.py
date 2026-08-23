"""
Test if registry cache is properly populated
"""
from app.exchange.registry import get_instrument_registry
from app.exchange.binance.client import BinanceFuturesClient
from app.core.config import settings

# Create client
client = BinanceFuturesClient(
    api_key=settings.BINANCE_API_KEY or "dummy",
    api_secret=settings.BINANCE_API_SECRET or "dummy",
    base_url=settings.BINANCE_FAPI_BASE_URL
)

# Get registry
registry = get_instrument_registry()

# Refresh
print("Refreshing registry...")
registry.refresh(broker_id="binance", client=client, force=True)

#Check cache
print(f"\nCache broker_ids: {list(registry._cache.keys())}")
binance_cache = registry._cache.get("binance", {})
print(f"Binance cache size: {len(binance_cache)}")
print(f"First 10 symbols in cache: {list(binance_cache.keys())[:10]}")

# Test lookup
symbols_to_test = ["BTCUSDT", "ETHUSDT", "XRPUSDT"]
for symbol in symbols_to_test:
    spec = registry.get_spec("binance", symbol)
    print(f"\n{symbol}: {spec is not None}")
    if spec:
        print(f"  - min_qty: {spec.min_qty}")
        print(f"  - min_notional: {spec.min_notional}")
