"""
IBKR Integration Verification Script

Tests:
1. Import verification
2. Capabilities check
3. Instrument loading
4. SessionManager singleton
5. Adapter initialization
"""
import sys
import os

# Add project root to path
sys.path.append(os.getcwd())

try:
    from app.exchange.ibkr.adapter import IBKRAdapter
    from app.exchange.ibkr.session import IBKRSessionManager
    from app.exchange.ibkr.client import IBKRClient
    from app.exchange.ibkr.capabilities import IBKR_CAPABILITIES
    from app.exchange.interface import ExchangeClient
    from app.models.unified_trading import PositionMode, IdempotencyMode
    
    print("✓ Successfully imported all IBKR modules")
    
    # Test 1: Verify capabilities
    print("\n[Test 1] Capabilities:")
    caps = IBKR_CAPABILITIES
    assert caps.position_mode == PositionMode.ONE_WAY
    assert caps.supports_ticket_mode is True
    assert caps.supports_hedging is False
    assert caps.supports_attached_sl_tp is False
    assert caps.supports_separate_protection is True
    assert caps.supports_oco is True
    assert caps.idempotency_mode == IdempotencyMode.NONE
    print(f"  Position Mode: {caps.position_mode}")
    print(f"  Ticket Mode: {caps.supports_ticket_mode}")
    print(f"  OCO: {caps.supports_oco}")
    print("  ✓ All capability flags validated")
    
    # Test 2: SessionManager singleton
    print("\n[Test 2] SessionManager Singleton:")
    sm1 = IBKRSessionManager("https://localhost:5000")
    sm2 = IBKRSessionManager("https://localhost:6000")  # Should be same instance
    assert sm1 is sm2
    print("  ✓ Singleton pattern working correctly")
    
    # Test 3: Adapter initialization
    print("\n[Test 3] Adapter Initialization:")
    adapter = IBKRAdapter(base_url="https://localhost:5000/v1/api")
    assert adapter.capabilities == IBKR_CAPABILITIES
    print(f"  Base URL: {adapter.base_url}")
    print(f"  Capabilities: {adapter.capabilities.position_mode}")
    print("  ✓ Adapter initialized successfully")
    
    # Test 4: Instrument loading
    print("\n[Test 4] Instrument Loading:")
    instruments = adapter.list_instruments()
    print(f"  Loaded {len(instruments)} instruments:")
    for inst in instruments:
        print(f"    - {inst.symbol_canonical} (conid: {inst.symbol_exchange})")
        print(f"      Contract Size: {inst.contract_size}")
        print(f"      Tick Size: {inst.tick_size}")
        print(f"      Asset Class: {inst.asset_class}")
    
    assert len(instruments) == 3
    assert all(inst.contract_size is not None for inst in instruments)
    print("  ✓ Instruments loaded and validated")
    
    # Test 5: Method availability
    print("\n[Test 5] Method Availability:")
    required_methods = [
        'list_instruments', 'get_prices', 'place_order', 
        'get_positions', 'get_balance', 'close_position',
        'cancel_order', 'get_order', 'list_open_orders'
    ]
    
    for method in required_methods:
        assert hasattr(adapter, method), f"Missing method: {method}"
        print(f"  ✓ {method}")
    
    print("\n" + "="*60)
    print("✅ ALL VERIFICATION TESTS PASSED")
    print("="*60)
    print("\nIBKR Adapter is ready for integration.")
    print("\nNext Steps:")
    print("  1. Run IBKR Client Portal Gateway")
    print("  2. Authenticate via Gateway web interface")
    print("  3. Test with live paper trading account")
    
except Exception as e:
    print(f"\n❌ Verification Failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
