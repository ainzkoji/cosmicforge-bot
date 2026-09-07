"""
Execution Filter — Layer 7
============================
Final pre-execution gate before an order reaches the exchange.

Checks:
  1. Spread gate         — rejects if bid/ask spread is too wide.
  2. Liquidity gate      — rejects if 15m volume is suspiciously thin.
  3. Volatility spike    — rejects if ATR just expanded > 2.5σ vs recent history
                           (news event / liquidation cascade in progress).

All checks are HARD: a BLOCK from any gate means no trade. No fallback.
Design is stateless; volatility spike detection requires an ATR history
which the caller supplies from klines.
"""
from __future__ import annotations

import logging
import math
import statistics
from dataclasses import dataclass
from typing import List, Optional

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ExecutionFilterConfig:
    max_spread_pct: float        = 0.03     # 0.03% max spread
    min_volume_usdt_15m: float   = 500.0    # Minimum 15m candle volume in USDT
    vol_spike_sigma: float       = 2.5      # ATR expansion z-score threshold
    vol_spike_lookback: int      = 20       # candles of ATR history to use
    min_atr_history_candles: int = 10       # minimum history before spike check runs
    max_stale_data_ms: int       = 30000    # 30 seconds max stale data threshold


DEFAULT_FILTER_CONFIG = ExecutionFilterConfig()


# ---------------------------------------------------------------------------
# Result
# ---------------------------------------------------------------------------

@dataclass
class FilterResult:
    allowed: bool
    block_reason: Optional[str]
    spread_pct: float
    volume_usdt: float
    atr_zscore: Optional[float]   # None if insufficient ATR history
    checks: dict                  # per-gate pass/fail for audit log
    updated_spread_history: List[float] # updated spread history list


# ---------------------------------------------------------------------------
# Core filter
# ---------------------------------------------------------------------------

def check_execution(
    symbol: str,
    current_price: float,
    bid: float,
    ask: float,
    volume_usdt_15m: float,
    atr_history: List[float],    # most recent ATR values (ascending time order)
    data_timestamp_ms: int = 0,  # timestamp of the market data to check staleness
    config: ExecutionFilterConfig = DEFAULT_FILTER_CONFIG,
    spread_history: List[float] = None,
) -> FilterResult:
    """
    Run all pre-execution gates.

    Args:
        symbol:           Trading symbol (for logging).
        current_price:    Last price.
        bid:              Best bid.
        ask:              Best ask.
        volume_usdt_15m:  Approximate USDT volume from most recent 15m candle.
        atr_history:      List of recent ATR values (at least `min_atr_history_candles`).
        config:           Gate thresholds.

    Returns:
        FilterResult — check `allowed` before submitting order.
    """
    import time
    checks = {}

    # ---- 0. Stale data gate --------------------------------------------
    if data_timestamp_ms > 0:
        now_ms = int(time.time() * 1000)
        stale_ms = now_ms - data_timestamp_ms
        if stale_ms > config.max_stale_data_ms:
            reason = f"Data is stale by {stale_ms}ms > allowed {config.max_stale_data_ms}ms"
            logger.warning(f"[EXEC FILTER] {symbol}: BLOCK — {reason}")
            checks["stale_data"] = {"ok": False, "stale_ms": stale_ms, "max": config.max_stale_data_ms}
            return FilterResult(
                allowed=False, block_reason=reason,
                spread_pct=0.0, volume_usdt=volume_usdt_15m,
                atr_zscore=None, checks=checks,
                updated_spread_history=spread_history or [],
            )
        checks["stale_data"] = {"ok": True, "stale_ms": stale_ms, "max": config.max_stale_data_ms}
    else:
        checks["stale_data"] = {"ok": True, "note": "staleness check skipped"}

    # ---- 1. Spread gate (Dynamic) --------------------------------------
    if spread_history is None:
        spread_history = []
        
    spread_pct = _spread_pct(bid, ask, current_price)
    
    # Maintain a rolling window of recent spreads (e.g., last 100 ticks)
    MAX_SPREAD_HISTORY = 100
    spread_history.append(spread_pct)
    if len(spread_history) > MAX_SPREAD_HISTORY:
        spread_history = spread_history[-MAX_SPREAD_HISTORY:]

    # Calculate dynamic spread threshold based on 90th percentile
    dynamic_max_spread = config.max_spread_pct
    if len(spread_history) >= 20:
        sorted_history = sorted(spread_history)
        p90_idx = int(len(sorted_history) * 0.90)
        p90_spread = sorted_history[p90_idx]
        # Allow up to 1.5x the 90th percentile, bounded by an absolute hard max of 0.10%
        dynamic_max_spread = min(max(config.max_spread_pct, p90_spread * 1.5), 0.10)

    spread_ok = spread_pct <= dynamic_max_spread
    checks["spread"] = {"ok": spread_ok, "value": round(spread_pct, 5), "max": round(dynamic_max_spread, 5), "dynamic": len(spread_history) >= 20}
    if not spread_ok:
        reason = f"spread={spread_pct:.4f}% > dynamic_max {dynamic_max_spread:.4f}%"
        logger.warning(f"[EXEC FILTER] {symbol}: BLOCK — {reason}")
        return FilterResult(
            allowed=False, block_reason=reason,
            spread_pct=spread_pct, volume_usdt=volume_usdt_15m,
            atr_zscore=None, checks=checks,
            updated_spread_history=spread_history,
        )

    # ---- 2. Liquidity gate ---------------------------------------------
    liquidity_ok = volume_usdt_15m >= config.min_volume_usdt_15m
    checks["liquidity"] = {"ok": liquidity_ok, "value": round(volume_usdt_15m, 2), "min": config.min_volume_usdt_15m}
    if not liquidity_ok:
        reason = f"volume={volume_usdt_15m:.0f} USDT < min {config.min_volume_usdt_15m:.0f} USDT"
        logger.warning(f"[EXEC FILTER] {symbol}: BLOCK — {reason}")
        return FilterResult(
            allowed=False, block_reason=reason,
            spread_pct=spread_pct, volume_usdt=volume_usdt_15m,
            atr_zscore=None, checks=checks,
            updated_spread_history=spread_history,
        )

    # ---- 3. Volatility spike gate --------------------------------------
    atr_zscore: Optional[float] = None
    if len(atr_history) >= config.min_atr_history_candles:
        history = atr_history[-config.vol_spike_lookback:]
        current_atr = history[-1]
        prior = history[:-1]

        if len(prior) >= 2:
            mean_atr = statistics.mean(prior)
            std_atr  = statistics.stdev(prior)
            if std_atr > 0:
                atr_zscore = (current_atr - mean_atr) / std_atr
                spike_ok = atr_zscore <= config.vol_spike_sigma
                checks["vol_spike"] = {
                    "ok": spike_ok,
                    "zscore": round(atr_zscore, 3),
                    "threshold": config.vol_spike_sigma,
                }
                if not spike_ok:
                    reason = f"ATR spike z={atr_zscore:.2f} > {config.vol_spike_sigma:.1f}σ"
                    logger.warning(f"[EXEC FILTER] {symbol}: BLOCK — {reason}")
                    return FilterResult(
                        allowed=False, block_reason=reason,
                        spread_pct=spread_pct, volume_usdt=volume_usdt_15m,
                        atr_zscore=atr_zscore, checks=checks,
                        updated_spread_history=spread_history,
                    )
            else:
                checks["vol_spike"] = {"ok": True, "note": "std=0, skipped"}
        else:
            checks["vol_spike"] = {"ok": True, "note": "insufficient prior ATR history"}
    else:
        checks["vol_spike"] = {"ok": True, "note": f"warming up ({len(atr_history)}/{config.min_atr_history_candles} candles)"}

    logger.debug(f"[EXEC FILTER] {symbol}: PASS — spread={spread_pct:.4f}% vol={volume_usdt_15m:.0f} atr_z={atr_zscore}")
    return FilterResult(
        allowed=True, block_reason=None,
        spread_pct=spread_pct, volume_usdt=volume_usdt_15m,
        atr_zscore=atr_zscore, checks=checks,
        updated_spread_history=spread_history,
    )


# ---------------------------------------------------------------------------
# Lightweight helpers
# ---------------------------------------------------------------------------

def _spread_pct(bid: float, ask: float, last: float) -> float:
    """Spread as percentage of mid-price."""
    if bid <= 0 or ask <= 0:
        mid = last
        # If no bid/ask, assume worst-case spread of 0.02% for crypto
        return 0.02
    mid = (bid + ask) / 2
    if mid <= 0:
        return 0.0
    return ((ask - bid) / mid) * 100


def build_atr_history_from_klines(klines: list, period: int = 14) -> List[float]:
    """
    Extract per-candle ATR values from a kline list for use in spike detection.
    klines format: [[open_time, open, high, low, close, volume, ...], ...]

    Returns list of ATR values (one per candle from period onward).
    """
    if len(klines) < period + 2:
        return []

    def _tr(k, prev_close):
        high  = float(k[2])
        low   = float(k[3])
        return max(high - low, abs(high - prev_close), abs(low - prev_close))

    trs = [_tr(klines[i], float(klines[i - 1][4])) for i in range(1, len(klines))]
    atrs = []
    for i in range(period, len(trs)):
        atrs.append(sum(trs[i - period:i]) / period)
    return atrs
