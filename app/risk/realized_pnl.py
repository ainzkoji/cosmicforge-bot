from __future__ import annotations

from typing import List, Dict


def realized_pnl_from_user_trades(trades: List[Dict]) -> float:
    """
    Binance futures userTrades includes 'realizedPnl' as string per fill.
    Sum it to get realized pnl over the time window.
    """
    total = 0.0
    for t in trades:
        rp = t.get("realizedPnl")
        if rp is None:
            continue
        try:
            total += float(rp)
        except Exception:
            continue
    return total
