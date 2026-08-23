from __future__ import annotations

from app.core.config import settings
from app.metrics.performance import update_strategy_performance, record_signal_outcome
from shared_lib.persistence.db import DB


def on_trade_close_update_metrics(
    *,
    strategy: str,
    strategy_version: str,
    symbol: str,
    timeframe: str,
    confidence: float | None,
    realized_pnl: float,
    fees: float = 0.0,
):
    """
    Call this ONLY when a trade is CLOSED and realized PnL is known.
    Works for crypto + forex because it’s attribution-based, not broker-specific.
    """
    # Legacy Note: We used to write to strategy_performance and signal_outcomes here.
    # Phase 3 Hardening: Analytics are now derived directly from trade_fills as the 
    # single execution source of truth. Writing duplicate metrics is disabled.
    pass
