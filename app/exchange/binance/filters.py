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


_EXCHANGE_INFO_CACHE: dict | None = None


def set_exchange_info(exchange_info: dict) -> None:
    """
    Set exchangeInfo once (e.g., on startup). Enables get_symbol_info() for legacy callers.
    """
    global _EXCHANGE_INFO_CACHE
    _EXCHANGE_INFO_CACHE = exchange_info


def get_symbol_info(symbol: str) -> dict | None:
    """
    Legacy helper: return a single symbol info dict from cached exchangeInfo.
    Returns None if cache is not set or symbol not found.
    """
    global _EXCHANGE_INFO_CACHE
    if not _EXCHANGE_INFO_CACHE:
        return None

    sym = (symbol or "").upper()
    for s in _EXCHANGE_INFO_CACHE.get("symbols", []):
        if (s.get("symbol") or "").upper() == sym:
            return s
    return None


def get_price_filter(info: dict) -> dict:
    """
    Extract PRICE_FILTER from Binance symbol info.
    """
    for f in info.get("filters", []):
        if f.get("filterType") == "PRICE_FILTER":
            return f
    raise ValueError("PRICE_FILTER not found in symbol info")


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


def _to_decimal(x) -> Decimal:
    return x if isinstance(x, Decimal) else Decimal(str(x))


def round_qty(qty: float, step_size) -> Decimal:
    """
    Round quantity DOWN to nearest valid stepSize.
    step_size can be Decimal or float/string.
    """
    q = Decimal(str(qty))
    step = _to_decimal(step_size)
    return (q / step).to_integral_value(rounding=ROUND_DOWN) * step


def round_price(px: float, tick_size) -> Decimal:
    """
    Round price DOWN to nearest valid tickSize.
    tick_size can be Decimal or float/string.
    """
    p = Decimal(str(px))
    tick = _to_decimal(tick_size)
    return (p / tick).to_integral_value(rounding=ROUND_DOWN) * tick


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


def _float_quantize(value: Decimal, step) -> float:
    """
    Convert Decimal -> float but quantize to the step's decimal places
    so float division like (out/step)%1 behaves as expected in tests.
    """
    step_d = step if isinstance(step, Decimal) else Decimal(str(step))
    # number of decimal places in step (e.g. 0.1 -> 1, 0.001 -> 3)
    places = max(0, -step_d.as_tuple().exponent)
    return float(value.quantize(Decimal("1").scaleb(-places)))


def round_qty_to_step(qty: float, step_size: float) -> float:
    out_d = round_qty(qty, step_size)  # returns Decimal
    return _float_quantize(out_d, step_size)


def round_price_to_tick(price: float, tick_size: float) -> float:
    out_d = round_price(price, tick_size)  # returns Decimal
    return _float_quantize(out_d, tick_size)
