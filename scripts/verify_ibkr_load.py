import sys
import os

# Add project root to path
sys.path.append(os.getcwd())

try:
    from app.exchange.ibkr.adapter import IBKRAdapter
    from app.exchange.interface import ExchangeClient
    
    print("Successfully imported IBKRAdapter")
    
    adapter = IBKRAdapter(base_url="https://localhost:5000/v1/api")
    
    # Verify inheritance
    assert isinstance(adapter, ExchangeClient)
    print("Instance check passed")
    
    # Verify capabilities
    caps = adapter.capabilities
    print(f"Capabilities loaded: {caps}")
    assert caps.supports_oco is True
    assert caps.supports_hedging is False
    
    # Verify instruments (mock/static)
    instruments = adapter.list_instruments()
    print(f"Loaded {len(instruments)} instruments")
    for i in instruments:
        print(f" - {i.symbol_canonical} (conid: {i.symbol_exchange})")
        
    print("\n✅ Verification Successful: IBKR Adapter loaded and basic checks passed.")
    
except Exception as e:
    print(f"\n❌ Verification Failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
