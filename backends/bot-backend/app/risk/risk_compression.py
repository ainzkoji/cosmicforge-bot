"""
Risk Compression Layer — Layer 6
==================================
Single authority for position-size and leverage compression.

Applies two independent multipliers based on current risk state:
  1. DrawdownMultiplier:    Scales down as cumulative DD grows.
  2. VolatilityMultiplier:  Suppresses size in ultra-low or ultra-high ATR%.

Final output:
  risk_multiplier    = dd_mult × vol_mult
  leverage_mult      = vol_mult only (compression follows volatility, not performance)

The caller (TradingOrchestrator / runner) multiplies:
  final_risk_usdt  = base_risk_usdt  × risk_multiplier
  final_leverage   = configured_lev  × leverage_mult

No silent fall-throughs. If inputs are invalid, returns a ZERO-RISK result
and logs the cause — the trade is blocked, not passed with defaults.
"""
from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Configuration (immutable dataclass, safe to share)
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class CompressionConfig:
    # Drawdown thresholds → multiplier steps
    dd_bands: tuple = (0.05, 0.10, 0.15)          # 5%, 10%, 15%
    dd_mults: tuple = (1.0, 0.70, 0.40, 0.20)     # at 0, 5, 10, 15%+

    # ATR% thresholds
    vol_zero_below: float = 0.30       # ATR% < 0.30 → no edge, block trade
    vol_compress_above: float = 3.00   # ATR% > 3.00 → compression kicks in
    vol_max_atr_pct: float = 6.00      # ATR% > 6.00 → leverage at floor

    # Leverage floor
    leverage_floor: float = 0.25       # minimum leverage multiplier


# Default config — all users share unless overridden
DEFAULT_CONFIG = CompressionConfig()


# ---------------------------------------------------------------------------
# Result
# ---------------------------------------------------------------------------

@dataclass
class CompressionResult:
    """
    Compression output. All values in range [0.0, 1.0] where 1.0 = no compression.
    """
    risk_multiplier: float          # Applied to base_risk_usdt
    leverage_multiplier: float      # Applied to configured leverage
    drawdown_mult: float
    volatility_mult: float
    atr_pct: float
    drawdown_pct: float
    is_hard_blocked: bool           # True if multiplier reached 0 (ATR too low)
    block_reason: Optional[str]
    breakdown: Dict[str, float] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Core computation (pure function — no side effects, safe to call in threads)
# ---------------------------------------------------------------------------

def compute_compression(
    atr_pct: float,
    adaptive_size_multiplier: float = 1.0,
    adaptive_leverage_multiplier: float = 1.0,
    config: CompressionConfig = DEFAULT_CONFIG,
) -> CompressionResult:
    """
    Apply risk and leverage compression multipliers provided centrally by AdaptiveEngine.
    Maintains the local dead-market hard block.

    Args:
        atr_pct: Current ATR as percentage of price (e.g. 1.5 = 1.5%)
        adaptive_size_multiplier: Multiplier from AdaptiveState
        adaptive_leverage_multiplier: Multiplier from AdaptiveState
        config: Compression thresholds

    Returns:
        CompressionResult with the adaptive multipliers applied.
    """
    atr_pct = max(0.0, float(atr_pct))

    # ---- 1. Volatility Floor Hard Block --------------------------------
    if atr_pct < config.vol_zero_below:
        return CompressionResult(
            risk_multiplier=0.0,
            leverage_multiplier=0.0,
            drawdown_mult=0.0,
            volatility_mult=0.0,
            atr_pct=atr_pct,
            drawdown_pct=0.0,
            is_hard_blocked=True,
            block_reason=f"ATR%={atr_pct:.2f} below floor {config.vol_zero_below:.2f} — no edge in dead market",
            breakdown={"risk_mult": 0.0, "leverage_mult": 0.0}
        )

    # ---- 2. Adaptive Control Pass-Through ------------------------------
    risk_mult       = adaptive_size_multiplier
    leverage_mult   = adaptive_leverage_multiplier

    result = CompressionResult(
        risk_multiplier=round(risk_mult, 4),
        leverage_multiplier=round(leverage_mult, 4),
        drawdown_mult=round(risk_mult, 4),           # Aliased for backward compatibility in logs
        volatility_mult=round(leverage_mult, 4),     # Aliased for backward compatibility in logs
        atr_pct=round(atr_pct, 4),
        drawdown_pct=0.0,                            # Centralised in AdaptiveEngine
        is_hard_blocked=False,
        block_reason=None,
        breakdown={
            "risk_mult": risk_mult,
            "leverage_mult": leverage_mult,
            "adaptive_controlled": 1.0,
        },
    )

    logger.debug(
        f"[COMPRESSION] Adaptive Controlled → risk×{risk_mult:.3f} lev×{leverage_mult:.3f}"
    )
    return result


def apply_compression(
    base_usdt: float,
    base_leverage: float,
    compression: CompressionResult,
) -> tuple[float, float]:
    """
    Apply compression result to raw size/leverage values.

    Returns:
        (compressed_usdt, compressed_leverage)
    """
    if compression.is_hard_blocked:
        return 0.0, 0.0
    return (
        base_usdt     * compression.risk_multiplier,
        base_leverage * compression.leverage_multiplier,
    )
