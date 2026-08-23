# app/symbols/sizing.py
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, ROUND_DOWN, ROUND_CEILING
from typing import Any, Dict, Union, Optional

from app.models.unified_trading import SymbolFilters


def _d(x: Any) -> Decimal:
    return Decimal(str(x))


def _floor_to_step(x: Decimal, step: Decimal) -> Decimal:
    if step <= 0:
        return x
    return (x / step).to_integral_value(rounding=ROUND_DOWN) * step

def _ceil_to_step(x: Decimal, step: Decimal) -> Decimal:
    if step <= 0:
        return x
    return (x / step).to_integral_value(rounding=ROUND_CEILING) * step


def parse_usdt_map(s: Union[str, Dict[str, Any], None]) -> Dict[str, float]:
    """
    Accepts either:
      - CSV string: "BTCUSDT:100,ETHUSDT:100"
      - dict: {"BTCUSDT": 100, "ETHUSDT": 100}
      - None/empty
    Returns: {SYMBOL: usdt_float}
    """
    if not s:
        return {}

    if isinstance(s, dict):
        out: Dict[str, float] = {}
        for k, v in s.items():
            try:
                out[str(k).strip().upper()] = float(v)
            except Exception:
                continue
        return out

    raw = str(s).strip()
    if not raw:
        return {}

    out: Dict[str, float] = {}
    parts = [p.strip() for p in raw.split(",") if p.strip()]
    for part in parts:
        if ":" not in part:
            continue
        k, v = part.split(":", 1)
        key = k.strip().upper()
        try:
            out[key] = float(v.strip())
        except Exception:
            continue
    return out


def usdt_for(symbol: str, m: Dict[str, float], default_usdt: float) -> float:
    return float(m.get(symbol.upper(), default_usdt))


@dataclass
class SizeResult:
    qty: float
    notional: float
    min_notional_required: float
    min_margin_required: float
    reason: str
    details: Dict[str, Any]


def validate_and_adjust_order(
    symbol: str,
    raw_qty: float,
    price: float,
    filters: Union[SymbolFilters, Any],
    budget_usdt: float,
    leverage: int = 1,
    min_notional_override: float = 0.0,
) -> SizeResult:
    """
    Validate and adjust order quantity based on broker filters.
    
    Logic:
      1. Floor raw_qty to step_size
      2. Check min_qty
      3. Check min_notional
      4. If notional < min_notional, attempt to bump qty IF within budget
    """
    px = _d(price)
    lev = max(1, int(leverage))
    budget = _d(budget_usdt)
    
    # Extract filters safely (handle both SymbolFilters obj and generic object/dict)
    if isinstance(filters, SymbolFilters):
        step = filters.step_size
        min_qty = filters.min_qty
        flt_min_notional = filters.min_notional
        contract_size = filters.contract_size
    else:
        # Legacy/Dict fallback
        step = _d(getattr(filters, "step_size", getattr(filters, "stepSize", 0)))
        min_qty = _d(getattr(filters, "min_qty", getattr(filters, "minQty", 0)))
        flt_min_notional = _d(getattr(filters, "min_notional", getattr(filters, "minNotional", 0)))
        contract_size = _d(getattr(filters, "contract_size", 1))

    cfg_min_notional = _d(min_notional_override or 0.0)
    min_notional = max(flt_min_notional, cfg_min_notional)

    if px <= 0:
        return SizeResult(
            qty=0.0,
            notional=0.0,
            min_notional_required=float(min_notional),
            min_margin_required=0.0,
            reason="invalid_price",
            details={"symbol": symbol, "price": float(price)},
        )

    # 1. Round down to step
    # Note: raw_qty is in CONTRACTS (logic units).
    raw_qty_d = _d(raw_qty)
    qty_dec = _floor_to_step(raw_qty_d, step) if step > 0 else raw_qty_d

    # Calculate Notional (Price * Qty * ContractSize)
    # Crypto: CS=1. Forex: CS=100000.
    notional = qty_dec * px * contract_size
    
    # 2. Check Min Qty
    if qty_dec <= 0 or (min_qty > 0 and qty_dec < min_qty):
        # Fail - bumping to min_qty usually risky if it wasn't requested?
        # Actually user logic for min_notional bump implies we might bump min_qty too?
        # But let's stick to min_notional bump for now unless min_qty forces it.
        # If raw_qty is tiny, we probably skip.
        
        # Calculate what min_margin would be needed for min_qty
        min_qty_notional = min_qty * px * contract_size
        min_qty_margin = (min_qty_notional / _d(lev)) if lev > 0 else min_qty_notional
        
        return SizeResult(
            qty=0.0,
            notional=0.0,
            min_notional_required=float(min_notional),
            min_margin_required=float(min_qty_margin),
            reason="qty_below_min_qty",
            details={
                "symbol": symbol,
                "raw_qty": float(raw_qty),
                "qty_rounded": float(qty_dec),
                "min_qty": float(min_qty),
                "step_size": float(step),
            },
        )

    # 3. Check Min Notional & Attempt Bump
    if min_notional > 0 and notional < min_notional:
        # BUMP LOGIC
        # Required Qty to meet min_notional
        # Notional = Qty * Px * CS >= MinNotional
        # Qty >= MinNotional / (Px * CS)
        
        req_raw_qty = min_notional / (px * contract_size)
        # We must ceil to step to ensure we are strictly above
        req_qty = _ceil_to_step(req_raw_qty, step) if step > 0 else req_raw_qty
        
        # Check if req_qty violates budget
        req_notional = req_qty * px * contract_size
        req_margin = (req_notional / _d(lev))
        
        # Allow a 15% tolerance to accommodate exchange step sizes pushing us slightly over budget
        budget_with_tolerance = budget * _d("1.15")
        if req_margin <= budget_with_tolerance:
            # ✅ BUMP SUCCESS
            return SizeResult(
                qty=float(req_qty),
                notional=float(req_notional),
                min_notional_required=float(min_notional),
                min_margin_required=float(req_margin),
                reason="qty_bumped_min_notional",
                details={
                    "symbol": symbol,
                    "original_qty": float(qty_dec),
                    "original_notional": float(notional),
                    "bumped_qty": float(req_qty),
                    "bumped_notional": float(req_notional),
                    "budget_usdt": float(budget),
                    "min_notional": float(min_notional)
                }
            )
        else:
            # ❌ FAIL (Budget exceeded)
            # B-8 Fix: Replace stderr print with structured logger.error so this
            # surfaces in monitoring, audit, and user dashboards instead of only stderr.
            import logging as _log
            _sz_logger = _log.getLogger(__name__)
            min_margin_required = (min_notional / _d(lev))
            _sz_details = {
                "symbol": symbol,
                "price": float(px),
                "budget_margin_usdt": float(budget),
                "leverage": lev,
                "target_notional": float(budget * _d(lev)),
                "min_notional_required": float(min_notional),
                "min_qty": float(min_qty),
                "step_size": float(step),
                "raw_qty": float(raw_qty),
                "rounded_qty": float(qty_dec),
                "calculated_notional": float(notional),
                "required_qty_for_min_notional": float(req_qty),
                "required_notional": float(req_notional),
                "required_margin": float(req_margin),
                "shortfall_usdt": float(req_margin - budget),
                "error_code": "SIZING_FAILURE_MIN_NOTIONAL",
                "user_action": "Increase trade amount to at least 50 USDT per position.",
            }
            _sz_logger.error(
                "SIZING_FAILURE_MIN_NOTIONAL: symbol=%s budget=%.2f leverage=%dx "
                "required_margin=%.2f min_notional=%.2f shortfall=%.2f — "
                "Trade amount too small for exchange minimum. "
                "Increase trade amount to at least 50 USDT per position.",
                symbol, float(budget), lev, float(req_margin),
                float(min_notional), float(req_margin - budget),
            )
            return SizeResult(
                qty=0.0,
                notional=float(notional),
                min_notional_required=float(min_notional),
                min_margin_required=float(min_margin_required),
                reason="below_min_notional_budget_limited",
                details=_sz_details,
            )

    # 4. Success
    margin_cost = (notional / _d(lev))
    return SizeResult(
        qty=float(qty_dec),
        notional=float(notional),
        min_notional_required=float(min_notional),
        min_margin_required=float(margin_cost),
        reason="ok",
        details={
            "symbol": symbol,
            "qty_rounded": float(qty_dec),
            "step_size": float(step),
            "contract_size": float(contract_size),
        },
    )


def size_from_budget(
    *,
    symbol: str,
    price: float,
    usdt_margin: float | None = None,
    budget_usdt: float | None = None,  # <-- alias (backward compatible)
    leverage: int,
    filters: Any,
    min_notional_override: float = 0.0,
    contract_size: float = 1.0, 
) -> SizeResult:
    """
    Universal sizing helper (wrapper around validate_and_adjust_order).
    """
    # Compat logic
    if usdt_margin is None and budget_usdt is not None:
        usdt_margin = budget_usdt
    if usdt_margin is None:
        usdt_margin = 0.0

    px = _d(price)
    lev = max(1, int(leverage))
    budget = _d(usdt_margin)
    
    # Resolve contract size: use filters if available, else override
    c_size = _d(contract_size)
    if isinstance(filters, SymbolFilters):
        c_size = filters.contract_size
        # If override passed (rare), usually trust filters.
        # But if caller passed explicit contract_size != 1, careful.
        # size_from_budget arg 'contract_size' is usually default 1.0
        # We prefer filter's truth.
    
    if px <= 0:
        return SizeResult(0.0, 0.0, 0.0, 0.0, "invalid_price", {})

    target_notional = budget * _d(lev)
    
    # Calculate Raw Qty
    # contracts = notional / (price * contract_size)
    raw_qty = target_notional / (px * c_size)
    
    # DELEGATE TO VALIDATOR
    # Passing the budget as the limit
    return validate_and_adjust_order(
        symbol=symbol,
        raw_qty=float(raw_qty),
        price=price,
        filters=filters,
        budget_usdt=float(budget),
        leverage=leverage,
        min_notional_override=min_notional_override
    )
