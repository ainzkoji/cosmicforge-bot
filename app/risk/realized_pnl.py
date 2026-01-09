from __future__ import annotations

from typing import List, Dict
import time
from app.core.config import settings
from app.runner.models import SymbolState


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


def record_realized_pnl_for_symbol(
    runner,
    symbol: str,
    window_minutes: int = 30,
) -> float:
    """
    Centralized realized PnL recorder.
    - Broker-agnostic
    - Updates daily loss
    - Triggers kill switch
    - Safe to reuse for Forex / other brokers later
    """

    symbol = symbol.upper()

    # symbol state
    st = runner.state.get(symbol)
    if st is None:
        st = SymbolState()
        runner.state[symbol] = st

    end_ms = int(time.time() * 1000)
    start_ms = end_ms - max(1, window_minutes) * 60 * 1000

    # broker adapter (Binance today, others later)
    trades = runner.executor.client.user_trades(
        symbol=symbol,
        start_time_ms=start_ms,
        end_time_ms=end_ms,
        limit=1000,
    )

    new_trades = []
    max_id = st.last_user_trade_id

    for t in trades:
        tid = t.get("id")
        if tid is None:
            continue
        tid = int(tid)
        if tid > st.last_user_trade_id:
            new_trades.append(t)
            max_id = max(max_id, tid)

    pnl_added = realized_pnl_from_user_trades(new_trades)
    st.last_user_trade_id = max_id

    # persist symbol dedup state
    try:
        runner.store.save_symbol(symbol, st)
    except Exception:
        pass

    # daily accounting
    runner.daily.reset_if_new_day()
    runner.daily.realized_pnl += pnl_added

    if runner.daily.realized_pnl <= -settings.DAILY_MAX_LOSS_USDT:
        runner.daily.kill = True

    runner.store.save_daily(
        runner.daily.day,
        runner.daily.realized_pnl,
        runner.daily.kill,
    )

    # audit
    runner.audit.event(
        event_type="REALIZED_PNL",
        run_id=runner.run_id,
        symbol=symbol,
        action="PNL_RECORDED",
        details={
            "window_minutes": window_minutes,
            "pnl_added": pnl_added,
            "daily_realized_pnl": runner.daily.realized_pnl,
            "kill": runner.daily.kill,
        },
    )

    return pnl_added
