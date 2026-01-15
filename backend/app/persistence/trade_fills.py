from __future__ import annotations

from typing import Any, Optional

from app.persistence.db import DB, utc_now_iso
from app.ops.context import get_run_id, get_cycle_id


def record_fill(
    db: DB,
    symbol: str,
    side: str,  # LONG/SHORT
    action: str,  # OPEN/CLOSE
    qty: float,
    price: float,
    fee: Optional[float] = None,
    realized_pnl: Optional[float] = None,
    pnl: Optional[float] = None,
    notes: Optional[dict[str, Any]] = None,  # ignored here
    *,
    strategy: str = "unknown",
    strategy_version: str = "0",
    broker_id: str = "binance_futures",
    account_id: str = "default",
    asset_class: str = "CRYPTO",
    timeframe: str = "",
    confidence: Optional[float] = None,
) -> None:
    if realized_pnl is None and pnl is not None:
        realized_pnl = pnl

    with db.connect() as conn:
        conn.execute(
            """
            INSERT INTO trade_fills (
                run_id, cycle_id, symbol, side, action, qty, price, fee, realized_pnl,
                strategy, strategy_version, broker_id, account_id, asset_class, timeframe, confidence,
                timestamp_utc
            )
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                get_run_id(),
                get_cycle_id(),
                symbol,
                side,
                action,
                float(qty),
                float(price),
                float(fee) if fee is not None else None,
                float(realized_pnl) if realized_pnl is not None else None,
                strategy,
                strategy_version,
                broker_id,
                account_id,
                asset_class,
                timeframe,
                float(confidence) if confidence is not None else None,
                utc_now_iso(),
            ),
        )
