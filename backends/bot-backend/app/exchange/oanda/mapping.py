"""
OANDA Symbol and Model Mapping Utilities.

Handles conversion between:
- Canonical symbols (EURUSD) and OANDA format (EUR_USD)
- OANDA instrument metadata to InstrumentSpec
- OANDA order/trade/position models to Unified models
"""
from decimal import Decimal
from typing import Dict, Any
from app.models.unified_trading import (
    InstrumentSpec,
    AssetClass,
    UnifiedOrder,
    UnifiedPosition,
    UnifiedFill,
    Side,
    OrderStatus,
    PositionMode
)


# ==========================================
# SYMBOL MAPPING
# ==========================================

def canonical_to_oanda(symbol: str) -> str:
    """
    Convert canonical symbol to OANDA format.
    Example: EURUSD -> EUR_USD
    """
    if len(symbol) == 6:
        return f"{symbol[:3]}_{symbol[3:]}"
    return symbol


def oanda_to_canonical(oanda_symbol: str) -> str:
    """
    Convert OANDA symbol to canonical format.
    Example: EUR_USD -> EURUSD
    """
    return oanda_symbol.replace("_", "")


# ==========================================
# INSTRUMENT SPEC MAPPING
# ==========================================

def map_oanda_instrument_to_spec(inst: Dict[str, Any]) -> InstrumentSpec:
    """
    Map OANDA instrument metadata to InstrumentSpec.
    
    OANDA Instrument Response Example:
    {
        "name": "EUR_USD",
        "type": "CURRENCY",
        "displayName": "EUR/USD",
        "pipLocation": -4,
        "displayPrecision": 5,
        "tradeUnitsPrecision": 0,
        "minimumTradeSize": "1",
        "maximumTrailingStopDistance": "1.00000",
        "minimumTrailingStopDistance": "0.00050",
        "maximumPositionSize": "0",
        "maximumOrderUnits": "100000000",
        "marginRate": "0.0333"
    }
    
    CRITICAL DESIGN DECISION:
    We use UNITS (not lots) internally to match OANDA API.
    - contract_size = 1 (since we trade in units directly)
    - step_size derived from tradeUnitsPrecision
    - tick_size derived from displayPrecision
    
    Standard FX lot = 100,000 units (preserved in metadata for display/reporting)
    """
    name = inst["name"]
    canonical = oanda_to_canonical(name)
    
    # Extract currencies (EUR_USD -> EUR, USD)
    parts = name.split("_")
    base = parts[0] if len(parts) > 0 else "EUR"
    quote = parts[1] if len(parts) > 1 else "USD"
    
    # Precision
    display_prec = int(inst.get("displayPrecision", 5))
    trade_units_prec = int(inst.get("tradeUnitsPrecision", 0))
    
    # Tick Size: 10^(-displayPrecision)
    tick_size = Decimal(10) ** (-display_prec)
    
    # Step Size: 10^(-tradeUnitsPrecision)
    # If tradeUnitsPrecision=0, step_size=1 (whole units)
    step_size = Decimal(10) ** (-trade_units_prec)
    
    # Min Quantity
    min_trade_size = Decimal(inst.get("minimumTradeSize", "1"))
    
    # Contract Size = 1 (we're trading units, not lots)
    contract_size = Decimal("1")
    
    # Margin Rate (e.g., 0.0333 = ~30:1 leverage)
    margin_rate = Decimal(inst.get("marginRate", "0.02"))
    max_leverage = Decimal("1") / margin_rate if margin_rate > 0 else Decimal("50")
    
    # Asset Class
    inst_type = inst.get("type", "CURRENCY")
    if inst_type == "CURRENCY":
        asset_class = AssetClass.FOREX_SPOT
    elif inst_type == "CFD":
        if "USD" in name or "EUR" in name or "GBP" in name:
            asset_class = AssetClass.FOREX_CFD
        else:
            asset_class = AssetClass.COMMODITY_CFD
    else:
        asset_class = AssetClass.FOREX_CFD
    
    return InstrumentSpec(
        symbol_canonical=canonical,
        symbol_exchange=name,
        asset_class=asset_class,
        base_currency=base,
        quote_currency=quote,
        margin_currency="USD",  # OANDA default account currency
        settlement_currency="USD",
        contract_size=contract_size,
        tick_size=tick_size,
        step_size=step_size,
        min_qty=min_trade_size,
        min_notional=None,  # OANDA doesn't use min notional
        price_precision=display_prec,
        qty_precision=abs(trade_units_prec),
        max_leverage=max_leverage,
        supports_per_order_leverage=False  # OANDA uses account-level margin
    )


# ==========================================
# ORDER MAPPING
# ==========================================

def map_oanda_order_to_unified(order: Dict[str, Any], symbol: str) -> UnifiedOrder:
    """
    Map OANDA order response to UnifiedOrder.
    
    OANDA Order Example:
    {
        "id": "1234",
        "createTime": "2024-01-01T00:00:00.000000000Z",
        "state": "FILLED",
        "type": "MARKET",
        "instrument": "EUR_USD",
        "units": "100000",
        "filledTime": "2024-01-01T00:00:00.000000000Z",
        "averagePrice": "1.10000",
        "fillingTransactionID": "5678"
    }
    """
    order_id = str(order.get("id", ""))
    state = order.get("state", "PENDING")
    
    # Status mapping
    status_map = {
        "PENDING": OrderStatus.NEW,
        "FILLED": OrderStatus.FILLED,
        "TRIGGERED": OrderStatus.PARTIALLY_FILLED,
        "CANCELLED": OrderStatus.CANCELED,
        "TRIGGERED": OrderStatus.NEW,
    }
    status = status_map.get(state, OrderStatus.NEW)
    
    # Side (units can be negative for SELL)
    units = Decimal(order.get("units", "0"))
    side = Side.BUY if units > 0 else Side.SELL
    qty = abs(units)
    
    # Fill info
    avg_price = order.get("averagePrice") or order.get("price")
    filled_qty = qty if status == OrderStatus.FILLED else Decimal("0")
    
    # Timestamp (ISO8601 to ms)
    create_time = order.get("createTime", "")
    timestamp_ms = _parse_oanda_timestamp(create_time)
    
    return UnifiedOrder(
        client_order_id=order.get("clientExtensions", {}).get("id", ""),
        broker_order_id=order_id,
        symbol=symbol,
        side=side,
        type=order.get("type", "MARKET"),
        qty_ordered=qty,
        qty_filled=filled_qty,
        avg_fill_price=Decimal(avg_price) if avg_price else None,
        status=status,
        timestamp=timestamp_ms,
        reduce_only=False
    )


# ==========================================
# POSITION MAPPING
# ==========================================

def map_oanda_trade_to_position(trade: Dict[str, Any]) -> UnifiedPosition:
    """
    Map OANDA trade (ticket) to UnifiedPosition.
    
    OANDA uses ticket-based positions (PositionMode.TICKET).
    
    Trade Example:
    {
        "id": "1234",
        "instrument": "EUR_USD",
        "price": "1.10000",
        "openTime": "2024-01-01T00:00:00.000000000Z",
        "initialUnits": "100000",
        "currentUnits": "100000",
        "realizedPL": "0.0000",
        "unrealizedPL": "50.0000",
        "marginUsed": "3333.33",
        "state": "OPEN"
    }
    """
    trade_id = str(trade.get("id", ""))
    symbol_oanda = trade.get("instrument", "")
    symbol = oanda_to_canonical(symbol_oanda)
    
    # Side
    current_units = Decimal(trade.get("currentUnits", "0"))
    side = Side.BUY if current_units > 0 else Side.SELL
    qty = abs(current_units)
    
    # Prices
    entry_price = Decimal(trade.get("price", "0"))
    current_price = Decimal(trade.get("averageClosePrice", entry_price))
    
    # PnL
    unrealized_pnl = Decimal(trade.get("unrealizedPL", "0"))
    realized_pnl = Decimal(trade.get("realizedPL", "0"))
    
    # Margin
    margin_used = Decimal(trade.get("marginUsed", "0"))
    leverage = (qty * entry_price) / margin_used if margin_used > 0 else Decimal("1")
    
    # Timestamp
    open_time = trade.get("openTime", "")
    timestamp_ms = _parse_oanda_timestamp(open_time)
    
    return UnifiedPosition(
        symbol=symbol,
        broker_id="oanda",
        position_id=trade_id,
        side=side,
        quantity=qty,
        entry_price=entry_price,
        current_price=current_price,
        unrealized_pnl=unrealized_pnl,
        realized_pnl=realized_pnl,
        margin_used=margin_used,
        leverage=leverage,
        mode=PositionMode.TICKET,
        timestamp=timestamp_ms
    )


# ==========================================
# FILLS MAPPING
# ==========================================

def map_oanda_transaction_to_fill(txn: Dict[str, Any]) -> UnifiedFill:
    """
    Map OANDA transaction to UnifiedFill.
    
    Transaction Example:
    {
        "id": "5678",
        "time": "2024-01-01T00:00:00.000000000Z",
        "type": "ORDER_FILL",
        "orderID": "1234",
        "instrument": "EUR_USD",
        "units": "100000",
        "price": "1.10000",
        "pl": "0.0000",
        "financing": "0.0000",
        "commission": "0.0000",
        "accountBalance": "10000.00"
    }
    """
    fill_id = str(txn.get("id", ""))
    order_id = str(txn.get("orderID", ""))
    symbol_oanda = txn.get("instrument", "")
    symbol = oanda_to_canonical(symbol_oanda)
    
    # Side
    units = Decimal(txn.get("units", "0"))
    side = Side.BUY if units > 0 else Side.SELL
    qty = abs(units)
    
    # Price & Commission
    price = Decimal(txn.get("price", "0"))
    commission = abs(Decimal(txn.get("commission", "0")))
    
    # Timestamp
    time_str = txn.get("time", "")
    timestamp_ms = _parse_oanda_timestamp(time_str)
    
    return UnifiedFill(
        fill_id=fill_id,
        order_id=order_id,
        client_order_id=None,
        symbol=symbol,
        side=side,
        qty=qty,
        price=price,
        commission=commission,
        commission_asset="USD",
        timestamp=timestamp_ms,
        is_maker=False  # OANDA doesn't distinguish maker/taker for FX
    )


# ==========================================
# HELPERS
# ==========================================

def _parse_oanda_timestamp(time_str: str) -> int:
    """
    Parse OANDA ISO8601 timestamp to milliseconds.
    Example: "2024-01-01T00:00:00.000000000Z"
    """
    if not time_str:
        return 0
    
    try:
        from datetime import datetime
        # OANDA uses nanoseconds, Python datetime uses microseconds
        # Strip the nanoseconds and parse
        if "." in time_str:
            base, frac = time_str.split(".")
            frac = frac.rstrip("Z")[:6]  # Take first 6 digits (microseconds)
            time_str = f"{base}.{frac}Z"
        
        dt = datetime.fromisoformat(time_str.replace("Z", "+00:00"))
        return int(dt.timestamp() * 1000)
    except Exception:
        return 0
