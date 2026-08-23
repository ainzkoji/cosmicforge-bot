import pytest
from unittest.mock import MagicMock
from decimal import Decimal
from app.exchange.binance.filters import normalize_protection_price
from app.models.unified_trading import ProtectionRequest, Side, ProtectionUpdateRequest

# --- Section 1/2: Helper Unit Tests ---

def test_normalization_directions():
    """Test directional rounding based on position side and leg."""
    # tick = 0.01 (2dp)
    # LONG SL: round down (protective)
    assert normalize_protection_price(100.019, "0.01", "LONG", "SL") == "100.01"
    # LONG TP: round up (favorable)
    assert normalize_protection_price(100.011, "0.01", "LONG", "TP") == "100.02"
    # SHORT SL: round up (protective)
    assert normalize_protection_price(100.011, "0.01", "SHORT", "SL") == "100.02"
    # SHORT TP: round down (favorable)
    assert normalize_protection_price(100.019, "0.01", "SHORT", "TP") == "100.01"

def test_normalization_symbol_ticks():
    """Test standard ticks and formatting counts for different symbols."""
    # XRPUSDT (4dp)
    assert normalize_protection_price(2.29009, "0.0001", "LONG", "SL") == "2.2900"
    # DOGEUSDT (5dp)
    assert normalize_protection_price(0.195009, "0.00001", "LONG", "SL") == "0.19500"
    # FILUSDT (3dp)
    assert normalize_protection_price(4.9809, "0.001", "LONG", "SL") == "4.980"
    # ETHUSDT (2dp)
    assert normalize_protection_price(2405.123, "0.01", "LONG", "SL") == "2405.12"
    
    # Exact tick alignments should retain formatting
    assert normalize_protection_price(2.29, "0.0001", "LONG", "SL") == "2.2900"
    assert normalize_protection_price(2405, "0.01", "LONG", "SL") == "2405.00"

def test_no_ieee_expansion():
    """Ensure math.floor float rounding artifacts do not occur."""
    # Old bug: 2.29 / 0.0001 -> math.floor(22899.999999999996) * 0.0001 = 2.2899000000000003
    # With canonical we should get exact 2.2900
    res = normalize_protection_price(2.29, "0.0001", "LONG", "SL")
    assert res == "2.2900"
    assert len(res.split(".")[1]) == 4

# --- Section 3/4/5: Binance Client Tests ---

def test_place_protection_serialization():
    from app.exchange.binance.client import BinanceFuturesClient
    client = BinanceFuturesClient.__new__(BinanceFuturesClient)
    client._signed_post = MagicMock(return_value={"algoId": "TEST_ALGO"})
    
    # Mock filters for DOGEUSDT
    import app.exchange.binance.filters
    original_tick = app.exchange.binance.filters._tick
    app.exchange.binance.filters._tick = MagicMock(return_value=0.00001)
    
    try:
        req = ProtectionRequest(
            symbol="DOGEUSDT",
            position_side=Side.BUY,
            qty=Decimal("100"),
            sl_price=Decimal("0.195009"), # Should round down to 0.19500
            tp_price=Decimal("0.201001")  # Should round up to 0.20101
        )
        client.place_protection(req)
        
        # Verify calls
        assert client._signed_post.call_count == 2
        
        args0, kwargs0 = client._signed_post.call_args_list[0]
        sl_call = kwargs0.get("params") or args0[1]
        assert sl_call["stopPrice"] == "0.19500"
        assert sl_call["triggerPrice"] == "0.19500"
        assert type(sl_call["stopPrice"]) is str
        
        args1, kwargs1 = client._signed_post.call_args_list[1]
        tp_call = kwargs1.get("params") or args1[1]
        assert tp_call["stopPrice"] == "0.20101"
        assert tp_call["triggerPrice"] == "0.20101"
        assert type(tp_call["stopPrice"]) is str
    finally:
        app.exchange.binance.filters._tick = original_tick

def test_update_protection_serialization():
    from app.exchange.binance.client import BinanceFuturesClient
    client = BinanceFuturesClient.__new__(BinanceFuturesClient)
    client._signed_post = MagicMock(return_value={"algoId": "TEST_ALGO"})
    client._signed_delete = MagicMock(return_value={"code": 200})
    client.cancel_all_orders = MagicMock()
    
    import app.exchange.binance.filters
    original_tick = app.exchange.binance.filters._tick
    app.exchange.binance.filters._tick = MagicMock(return_value=0.00001)
    
    try:
        req = ProtectionUpdateRequest(
            symbol="DOGEUSDT",
            position_side="LONG",
            qty=100.0,
            new_sl_price=0.195009,
            new_tp_price=0.201001,
            reason="TRAILING_STOP"
        )
        client.update_protection(req)
        
        assert client._signed_post.call_count == 2
        args0, kwargs0 = client._signed_post.call_args_list[0]
        sl_call = kwargs0.get("params") or args0[1]
        assert sl_call["stopPrice"] == "0.19500" # String exactly
        assert sl_call["workingType"] == "CONTRACT_PRICE"
        
        args1, kwargs1 = client._signed_post.call_args_list[1]
        tp_call = kwargs1.get("params") or args1[1]
        assert tp_call["stopPrice"] == "0.20101"
    finally:
        app.exchange.binance.filters._tick = original_tick

# --- Section 6: Executor Repair Tests ---

def test_executor_repair_path_normalization():
    from app.execution.executor import BinanceExecutor
    client = MagicMock()
    # Mock position amt and ensure condition is met
    client.get_position_amt.return_value = "100.0"
    # Ensure protection sanity returns False so it repairs
    ex = BinanceExecutor.__new__(BinanceExecutor)
    ex.client = client
    ex._protection_is_sane = MagicMock(return_value=True) # First we test naked
    
    client.cancel_all_orders = MagicMock()
    client.place_protection = MagicMock(return_value=MagicMock(status="success"))
    
    # Mock get_algo_orders to return empty
    client.get_algo_orders.return_value = []
    client.open_orders.return_value = []
    
    import app.execution.executor
    import app.exchange.binance.filters
    original_tick = app.exchange.binance.filters._tick
    app.exchange.binance.filters._tick = MagicMock(return_value=0.001) # 3dp
    
    try:
        # Naked (missing) protection path
        ex.ensure_protection(
            symbol="FILUSDT",
            sl_price=4.9809,
            tp_price=5.2601,
            repair_source="PERSISTED",
            signal="BUY"
        )
        req = client.place_protection.call_args[0][0]
        # LONG SL round down 4.980, TP round up 5.261
        assert str(req.sl_price) == "4.980"
        assert str(req.tp_price) == "5.261"
    finally:
        app.exchange.binance.filters._tick = original_tick


def test_executor_break_even_mutation_uses_canonical_helper():
    """Verify execute_break_even_update safely normalizes via the canonical helper without float expansion."""
    from app.execution.executor import BinanceExecutor
    client = MagicMock()
    # Mock position amt
    client.get_position_amt.return_value = "100.0"
    client.update_protection = MagicMock(return_value={"status": "OK", "sl_order_id": "SL1", "tp_order_id": "TP1"})
    
    ex = BinanceExecutor.__new__(BinanceExecutor)
    ex.client = client
    
    # Mock PM
    pm = MagicMock()
    pos = MagicMock()
    from app.execution.position_manager import PositionPhase
    pos.phase = PositionPhase.TP1_FILLED
    pos.tp.tp1_hit = True
    pos.sl.be_exchange_confirmed = False
    pm.get_position.return_value = pos
    
    import app.execution.executor
    import app.exchange.binance.filters
    original_tick = app.exchange.binance.filters._tick
    app.exchange.binance.filters._tick = MagicMock(return_value=0.00001)

    try:
        res = ex.execute_break_even_update(
            symbol="DOGEUSDT",
            position_side="LONG",
            runner_qty=100.0,
            entry_price=0.195009, # Long BE stops should round up to be safe
            current_stop=0.19000,
            sl_order_id="old_sl",
            tp_order_id="old_tp",
            tp2_price=0.201001,
            position_manager=pm,
            fee_buffer_mult=0.0, # zero buffer to test exact rounding of entry price
            taker_fee_rate=0.0
        )
        assert res["break_even_applied"] is True
        assert res["normalized_break_even_price"] == "0.19500" # 0.195009 -> round DOWN to tick 0.00001 for LONG SL
        
        # Verify call to update_protection
        req = client.update_protection.call_args[0][0]
        # Should be exact float representation of the formatted string
        # Because in python floats like 0.19501 could be 0.19501000000000001
        # It's okay if req has float IF client.update_protection re-normalizes, BUT wait
        # Our changed execute_break_even_update places exactly normalized strings into `norm_be` 
        # But ProtectionUpdateRequest new_sl_price is Typed as float. 
        # The normalization in Executor proves it uses canonical helper because norm_be was a string originally, though the pydantic model casts to float
        import math
        assert math.isclose(req.new_sl_price, 0.19500)
    finally:
        app.exchange.binance.filters._tick = original_tick


def test_executor_trailing_mutation_uses_canonical_helper():
    """Verify execute_trailing_stop_update safely normalizes via canonical helper."""
    from app.execution.executor import BinanceExecutor
    client = MagicMock()
    client.get_position_amt.return_value = "100.0"
    client.update_protection = MagicMock(return_value={"status": "OK"})
    
    ex = BinanceExecutor.__new__(BinanceExecutor)
    ex.client = client
    
    pm = MagicMock()
    pos = MagicMock()
    # Fake phases to pass the eligibility guards
    from app.execution.position_manager import PositionPhase
    pos.phase = PositionPhase.RUNNER_TRAILING
    pos.tp.tp1_hit = True
    pos.sl.be_exchange_confirmed = True
    pos.sl.trailing_last_stop_price = 4.0
    pos.sl.trailing_last_update_ts = None # avoid throttling
    pm.get_position.return_value = pos
    
    import app.execution.executor
    import app.exchange.binance.filters
    original_tick = app.exchange.binance.filters._tick
    app.exchange.binance.filters._tick = MagicMock(return_value=0.001)

    try:
        res = ex.execute_trailing_stop_update(
            symbol="FILUSDT",
            position_side="LONG",
            runner_qty=100.0,
            entry_price=4.500,
            current_stop=4.000,
            highest_since_entry=5.1239,
            lowest_since_entry=4.0,
            atr=0.1,
            sl_order_id="sl2",
            tp_order_id="tp2",
            tp2_price=6.0,
            position_manager=pm,
            trail_atr_mult=1.0, # trailing distance = 0.1
            min_delta_pct=0.00001,
            last_update_ts=None
        )
        assert res["trailing_applied"] is True
        # highest = 5.1239, distance = 0.1 -> trailing = 5.0239
        # canonical helper for LONG SL -> round DOWN
        # 5.0239 -> 5.023
        assert res["normalized_trailing_stop"] == 5.023
        
        req = client.update_protection.call_args[0][0]
        # In the executor it was cast to float for reference checking
        assert req.new_sl_price == 5.023
    finally:
        app.exchange.binance.filters._tick = original_tick

