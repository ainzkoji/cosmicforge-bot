from __future__ import annotations
from dataclasses import dataclass
from decimal import Decimal, ROUND_DOWN
import math

@dataclass(frozen=True)
class SymbolFilters:
    symbol: str
    step_size: Decimal
    min_qty: Decimal
    tick_size: Decimal

def extract_filters(instrument_info: dict) -> SymbolFilters:
    """
    Parse V5 Instrument Info entry.
    """
    # Bybit V5 structure: 
    # result -> list -> [ { symbol: "", lotSizeFilter: { qtyStep: "", minOrderQty: "" }, priceFilter: { tickSize: "" } } ]
    
    symbol = instrument_info.get("symbol", "")
    
    # Lot Size
    lot_filter = instrument_info.get("lotSizeFilter", {})
    qty_step = lot_filter.get("qtyStep", "0.001")
    min_qty = lot_filter.get("minOrderQty", "0.001")
    
    # Price
    price_filter = instrument_info.get("priceFilter", {})
    tick_size = price_filter.get("tickSize", "0.01")
    
    return SymbolFilters(
        symbol=symbol,
        step_size=Decimal(str(qty_step)),
        min_qty=Decimal(str(min_qty)),
        tick_size=Decimal(str(tick_size))
    )

def _to_decimal(x) -> Decimal:
    return x if isinstance(x, Decimal) else Decimal(str(x))

def round_qty(qty: float, step_size) -> Decimal:
    q = Decimal(str(qty))
    step = _to_decimal(step_size)
    return (q / step).to_integral_value(rounding=ROUND_DOWN) * step

def round_price_down(price: float, tick_size) -> float:
    tick = float(tick_size)
    return math.floor(price / tick) * tick
