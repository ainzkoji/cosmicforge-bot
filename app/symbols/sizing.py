# app/symbols/sizing.py
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, ROUND_DOWN
from typing import Any, Dict, Union


def _d(x: Any) -> Decimal:
    return Decimal(str(x))


def _floor_to_step(x: Decimal, step: Decimal) -> Decimal:
    if step <= 0:
        return x
    return (x / step).to_integral_value(rounding=ROUND_DOWN) * step


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


def size_from_budget(
    *,
    symbol: str,
    price: float,
    usdt_margin: float | None = None,
    budget_usdt: float | None = None,  # <-- alias (backward compatible)
    leverage: int,
    filters: Any,
    min_notional_override: float = 0.0,
) -> SizeResult:
    """
    Universal sizing:
      - Input budget is MARGIN (USDT) for leveraged products.
      - Notional = margin * leverage.
      - For spot/stocks set leverage=1.

    Backward compatible:
      - 'budget_usdt' is accepted as alias for 'usdt_margin'.
    """

    # -----------------------------
    # Backward-compat alias handling
    # -----------------------------
    if usdt_margin is None and budget_usdt is not None:
        usdt_margin = budget_usdt

    if usdt_margin is None:
        usdt_margin = 0.0

    if budget_usdt is not None and usdt_margin is not None:
        # If both were provided and they differ, keep safety strict.
        # (prevents silent mismatches when refactoring callers)
        try:
            if float(budget_usdt) != float(usdt_margin):
                return SizeResult(
                    qty=0.0,
                    notional=0.0,
                    min_notional_required=float(getattr(filters, "min_notional", 0.0)),
                    min_margin_required=0.0,
                    reason="conflicting_budget_args",
                    details={
                        "symbol": symbol,
                        "usdt_margin": float(usdt_margin),
                        "budget_usdt": float(budget_usdt),
                    },
                )
        except Exception:
            pass

    # ---- existing logic continues unchanged below ----

    px = _d(price)
    lev = max(1, int(leverage))
    budget = _d(usdt_margin)

    step = _d(getattr(filters, "step_size", "0"))
    min_qty = _d(getattr(filters, "min_qty", "0"))

    flt_min_notional = _d(getattr(filters, "min_notional", "0"))
    cfg_min_notional = _d(min_notional_override or 0.0)
    min_notional = max(flt_min_notional, cfg_min_notional)

    target_notional = budget * _d(lev)
    if px <= 0:
        return SizeResult(
            qty=0.0,
            notional=0.0,
            min_notional_required=float(min_notional),
            min_margin_required=float(min_notional / _d(lev)) if lev > 0 else 0.0,
            reason="invalid_price",
            details={"symbol": symbol, "price": float(price)},
        )

    raw_qty = target_notional / px
    qty_dec = _floor_to_step(raw_qty, step) if step > 0 else raw_qty

    if qty_dec <= 0 or (min_qty > 0 and qty_dec < min_qty):
        min_qty_notional = min_qty * px
        min_qty_margin = (min_qty_notional / _d(lev)) if lev > 0 else min_qty_notional
        return SizeResult(
            qty=0.0,
            notional=0.0,
            min_notional_required=float(min_notional),
            min_margin_required=float(min_qty_margin),
            reason="qty_below_min_qty",
            details={
                "symbol": symbol,
                "price": float(px),
                "usdt_margin": float(budget),
                "leverage": lev,
                "raw_qty": str(raw_qty),
                "qty_rounded": str(qty_dec),
                "min_qty": str(min_qty),
                "step_size": str(step),
                "min_qty_margin_required": float(min_qty_margin),
            },
        )

    notional = qty_dec * px

    if min_notional > 0 and notional < min_notional:
        min_margin_required = (min_notional / _d(lev)) if lev > 0 else min_notional
        return SizeResult(
            qty=0.0,
            notional=float(notional),
            min_notional_required=float(min_notional),
            min_margin_required=float(min_margin_required),
            reason="below_min_notional",
            details={
                "symbol": symbol,
                "price": float(px),
                "usdt_margin": float(budget),
                "leverage": lev,
                "qty": str(qty_dec),
                "notional": float(notional),
                "min_notional_required": float(min_notional),
                "min_margin_required": float(min_margin_required),
            },
        )

    return SizeResult(
        qty=float(qty_dec),
        notional=float(notional),
        min_notional_required=float(min_notional),
        min_margin_required=(
            float(min_notional / _d(lev)) if lev > 0 else float(min_notional)
        ),
        reason="ok",
        details={
            "symbol": symbol,
            "price": float(px),
            "usdt_margin": float(budget),
            "leverage": lev,
            "target_notional": float(target_notional),
            "raw_qty": str(raw_qty),
            "qty_rounded": str(qty_dec),
            "notional": float(notional),
            "min_qty": str(min_qty),
            "step_size": str(step),
            "min_notional_required": float(min_notional),
        },
    )
