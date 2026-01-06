from __future__ import annotations

import math
from dataclasses import dataclass
from decimal import Decimal, ROUND_DOWN


@dataclass(frozen=True)
class SymbolFilters:
    symbol: str
    step_size: Decimal
    min_qty: Decimal
    tick_size: Decimal


def _get_filter(symbol_info: dict, filter_type: str) -> dict | None:
    for f in symbol_info.get("filters", []):
        if f.get("filterType") == filter_type:
            return f
    return None


def extract_filters(exchange_info: dict, symbol: str) -> SymbolFilters:
    symbol = symbol.upper()

    for s in exchange_info.get("symbols", []):
        if s.get("symbol") != symbol:
            continue

        # LOT_SIZE → qty rules
        lot = _get_filter(s, "LOT_SIZE")
        if not lot:
            raise ValueError(f"LOT_SIZE filter not found for {symbol}")

        step = Decimal(lot["stepSize"])
        minq = Decimal(lot["minQty"])

        # PRICE_FILTER → price tick rules
        price_filter = _get_filter(s, "PRICE_FILTER")
        if not price_filter:
            raise ValueError(f"PRICE_FILTER not found for {symbol}")

        tick = Decimal(price_filter["tickSize"])

        return SymbolFilters(
            symbol=symbol,
            step_size=step,
            min_qty=minq,
            tick_size=tick,
        )

    raise ValueError(f"Symbol not found in exchangeInfo: {symbol}")


def round_qty(qty: float, step_size: Decimal) -> Decimal:
    """
    Round quantity DOWN to nearest valid stepSize.
    """
    q = Decimal(str(qty))
    return (q / step_size).to_integral_value(rounding=ROUND_DOWN) * step_size


def round_price(px: float, tick_size: Decimal) -> Decimal:
    """
    Round price DOWN to nearest valid tickSize.
    """
    p = Decimal(str(px))
    return (p / tick_size).to_integral_value(rounding=ROUND_DOWN) * tick_size


def _tick(symbol: str) -> float:
    info = get_symbol_info(symbol)
    if not info:
        return 0.01
    f = get_price_filter(info)
    return float(f.get("tickSize", "0.01"))


def round_price_down(symbol: str, price: float) -> float:
    tick = _tick(symbol)
    return math.floor(price / tick) * tick


def round_price_up(symbol: str, price: float) -> float:
    tick = _tick(symbol)
    return math.ceil(price / tick) * tick
