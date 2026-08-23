"""
Cross-Symbol Correlation Filter  (Stage 2D)
============================================
Blocks entry when an open position in a highly-correlated symbol is already
held in the same direction, preventing double-exposure to the same underlying
macro factor.

Design principles
-----------------
- Groups are static and defined by known crypto correlation clusters.
  Dynamic covariance matrices are deferred to v2 (need 30+ days of fill data).
- Rule: only 1 open position per correlation group per direction.
  A second same-direction entry in the same group doubles correlated drawdown
  risk without adding independent alpha.
- The filter is ADDITIVE-ONLY: it can block entries but cannot force them.
- If a symbol is not in any group it is treated as uncorrelated (no block).

Configuration
-------------
Set ``CORRELATION_FILTER_ENABLED=false`` env var to disable without code changes.
Set ``CORRELATION_MAX_SAME_DIRECTION=1`` to allow N open positions per group.
"""

from __future__ import annotations

import logging
import os
from typing import Dict, FrozenSet, Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Correlation groups
# ---------------------------------------------------------------------------
# Each group is identified by an integer key.  Symbols should be added here
# as the strategy universe expands.  When two symbols have no historical
# fill data to compute correlation, err on the side of grouping them if they
# share a common underlying risk factor.

_CORRELATION_GROUPS: Dict[int, FrozenSet[str]] = {
    # ── Group 0: BTC + large-caps that track BTC momentum closely
    0: frozenset({
        "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT",
        "AVAXUSDT", "DOTUSDT", "MATICUSDT", "LINKUSDT",
    }),
    # ── Group 1: Mid-cap alts with high BTC beta
    1: frozenset({
        "ADAUSDT", "XRPUSDT", "LTCUSDT", "ATOMUSDT",
        "NEARUSDT", "APTUSDT", "SUIUSDT",
    }),
    # ── Group 2: DeFi tokens (high inter-correlation, sector-rotation risk)
    2: frozenset({
        "UNIUSDT", "AAVEUSDT", "COMPUSDT", "MKRUSDT",
        "CRVUSDT", "SNXUSDT",
    }),
    # ── Group 3: Meme / high-beta
    3: frozenset({
        "DOGEUSDT", "SHIBUSDT", "PEPEUSDT", "WIFUSDT",
        "FLOKIUSDT",
    }),
}

# Reverse lookup: symbol → group_id  (built once at import time)
_SYMBOL_TO_GROUP: Dict[str, int] = {
    symbol: gid
    for gid, symbols in _CORRELATION_GROUPS.items()
    for symbol in symbols
}


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

def _env_bool(name: str, default: bool) -> bool:
    val = os.getenv(name, "").strip().lower()
    if not val:
        return default
    return val in ("true", "1", "yes")


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except (ValueError, TypeError):
        return default


CORRELATION_FILTER_ENABLED: bool = _env_bool("CORRELATION_FILTER_ENABLED", True)
# How many same-direction positions in the same group are allowed before blocking
MAX_SAME_DIRECTION: int = _env_int("CORRELATION_MAX_SAME_DIRECTION", 1)


# ---------------------------------------------------------------------------
# Filter logic
# ---------------------------------------------------------------------------

class CorrelationFilter:
    """
    Stateless filter — evaluates the current symbol-state snapshot each call.
    Designed to be instantiated once and shared across the runner loop.
    """

    def should_block(
        self,
        new_symbol: str,
        new_direction: str,  # "LONG" or "SHORT"
        open_positions: Dict[str, str],  # {symbol: "LONG"|"SHORT"}
    ) -> tuple[bool, str]:
        """
        Decide whether to block the entry of *new_symbol* in *new_direction*.

        Parameters
        ----------
        new_symbol:      Symbol being evaluated for entry.
        new_direction:   "LONG" or "SHORT".
        open_positions:  Snapshot of currently open positions.
                         Keys are symbols, values are their direction.

        Returns
        -------
        (blocked: bool, reason: str)
        """
        if not CORRELATION_FILTER_ENABLED:
            return False, ""

        group_id = _SYMBOL_TO_GROUP.get(new_symbol)
        if group_id is None:
            return False, ""

        group_symbols = _CORRELATION_GROUPS[group_id]

        # Count how many open positions in the same group share the same direction
        same_direction_count = sum(
            1
            for sym, direction in open_positions.items()
            if sym != new_symbol and sym in group_symbols and direction == new_direction
        )

        if same_direction_count >= MAX_SAME_DIRECTION:
            existing = [
                sym for sym, direction in open_positions.items()
                if sym != new_symbol and sym in group_symbols and direction == new_direction
            ]
            reason = (
                f"correlation_block: group={group_id} already has "
                f"{same_direction_count} {new_direction} position(s) "
                f"({', '.join(existing)}); max={MAX_SAME_DIRECTION}"
            )
            logger.info(
                "[CorrelationFilter] BLOCK %s %s — %s",
                new_symbol, new_direction, reason,
            )
            return True, reason

        return False, ""


# ---------------------------------------------------------------------------
# Singleton
# ---------------------------------------------------------------------------

_filter_instance: Optional[CorrelationFilter] = None


def get_correlation_filter() -> CorrelationFilter:
    """Return the shared singleton CorrelationFilter."""
    global _filter_instance
    if _filter_instance is None:
        _filter_instance = CorrelationFilter()
        logger.info(
            "CorrelationFilter initialised: enabled=%s max_same_direction=%s",
            CORRELATION_FILTER_ENABLED,
            MAX_SAME_DIRECTION,
        )
    return _filter_instance
