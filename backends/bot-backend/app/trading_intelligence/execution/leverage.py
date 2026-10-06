"""Deterministic leverage resolution for CATI fixed-margin entries.

The user's position margin is authoritative and never changes here. The
user's leverage is a MAXIMUM. CATI's structural stop is authoritative and is
never moved. The resolver returns the highest whole leverage >= 1x that every
applicable ceiling allows:

- the user's effective maximum leverage (EffectiveBotPolicy);
- the system asset-class ceiling (SystemLimits);
- the broker's symbol maximum, when known;
- the selected risk profile's compound-risk limit, applied as
  structural stop fraction x ACTUAL leverage (never a fixed 10x);
- liquidation safety: liquidation distance stays at least twice the stop
  distance (incl. round-trip slippage) after maintenance margin and fees --
  the same invariant the executor enforces before sizing.

No legal leverage >= 1x is an explicit block, never a smaller margin.
"""
from __future__ import annotations

from dataclasses import dataclass, field
import math
from typing import Dict, Optional

#: Maintenance margin + open/close taker fees + funding buffer, as the executor
#: models Binance USDT-M worst-case bounds (executor._size_qty).
LIQUIDATION_DEDUCTION = 0.0061


@dataclass(frozen=True)
class LeverageResolution:
    leverage: Optional[int]
    reason: Optional[str]
    stop_fraction: float
    ceilings: Dict[str, Optional[float]] = field(default_factory=dict)
    binding: Optional[str] = None
    compound_risk: Optional[float] = None

    @property
    def ok(self) -> bool:
        return self.leverage is not None

    def evidence(self) -> dict:
        return {"leverage": self.leverage, "reason": self.reason, "stop_fraction": self.stop_fraction,
                "ceilings": dict(self.ceilings), "binding": self.binding, "compound_risk": self.compound_risk}


def resolve_cati_leverage(*, stop_fraction: float, user_max: float, asset_class_max: float,
                          compound_risk_limit: float, broker_symbol_max: Optional[float] = None,
                          slippage_fraction: float = 0.0) -> LeverageResolution:
    stop = float(stop_fraction)
    if not math.isfinite(stop) or stop <= 0:
        return LeverageResolution(None, "CATI_STRUCTURAL_STOP_INVALID", stop)
    effective_stop = stop + 2.0 * max(0.0, float(slippage_fraction or 0.0))
    ceilings: Dict[str, Optional[float]] = {
        "USER_MAX_LEVERAGE": float(user_max),
        "ASSET_CLASS_MAX_LEVERAGE": float(asset_class_max),
        "BROKER_SYMBOL_MAX_LEVERAGE": float(broker_symbol_max) if broker_symbol_max else None,
        "COMPOUND_RISK_LIMIT": float(compound_risk_limit) / stop,
        "LIQUIDATION_SAFETY": 1.0 / (2.0 * effective_stop + LIQUIDATION_DEDUCTION),
    }
    known = {k: v for k, v in ceilings.items() if v is not None}
    binding = min(known, key=known.get)
    # 1e-9 absorbs float noise such as 0.225/0.075 = 2.9999999999999996.
    leverage = int(math.floor(known[binding] + 1e-9))
    if leverage < 1:
        return LeverageResolution(None, f"CATI_NO_LEGAL_LEVERAGE:{binding}", stop, ceilings, binding)
    return LeverageResolution(leverage, None, stop, ceilings, binding, round(stop * leverage, 10))


__all__ = ["LIQUIDATION_DEDUCTION", "LeverageResolution", "resolve_cati_leverage"]
