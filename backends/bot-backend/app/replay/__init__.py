"""Production-parity historical replay (Phase 13).

The point of this package is that the strategy cannot tell it is in a replay.
It sees the same immutable ``MarketSnapshot`` contract the live runtime builds,
and every component below the snapshot — regime classification, the ensemble,
``TradingDecisionEngine``, risk, execution feasibility, ``EntryProtection``,
``PositionManager`` — is the production one, unmodified.

What replay adds is a *clock*. At replay timestamp ``t`` the system may only
see information that existed at or before ``t``: no future candle, no future
higher-timeframe candle, no future close. That guarantee is enforced by
construction in :mod:`app.replay.historical_provider` rather than by
convention, because look-ahead is the one bug that makes a backtest confidently
wrong instead of merely inaccurate.
"""

from app.replay.historical_provider import (
    HistoricalClock,
    HistoricalMarketDataProvider,
    ReplayDataError,
)

__all__ = [
    "HistoricalClock",
    "HistoricalMarketDataProvider",
    "ReplayDataError",
]
