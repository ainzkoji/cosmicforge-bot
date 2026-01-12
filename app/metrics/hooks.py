from __future__ import annotations

from app.core.config import settings
from app.metrics.performance import update_strategy_performance, record_signal_outcome
from app.persistence.db import DB


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
    db = DB()
    outcome_win = realized_pnl > 0

    # (gross_pnl same as realized_pnl for now; expand later if needed)
    update_strategy_performance(
        db,
        strategy=strategy,
        strategy_version=strategy_version,
        symbol=symbol,
        asset_class=settings.ASSET_CLASS,
        broker_id=settings.BROKER_ID,
        account_id=settings.ACCOUNT_ID,
        timeframe=timeframe,
        net_pnl_delta=realized_pnl - fees,
        gross_pnl_delta=realized_pnl,
        fees_delta=fees,
        outcome_win=outcome_win,
        r_multiple=None,  # add later when ATR risk sizing exists
    )

    if confidence is not None:
        record_signal_outcome(
            db,
            strategy=strategy,
            strategy_version=strategy_version,
            symbol=symbol,
            asset_class=settings.ASSET_CLASS,
            broker_id=settings.BROKER_ID,
            account_id=settings.ACCOUNT_ID,
            timeframe=timeframe,
            confidence=float(confidence),
            pnl=float(realized_pnl - fees),
            outcome_win=outcome_win,
        )
