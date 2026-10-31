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

Section I Batch 7 retired the legacy replay session (``engine.py``, which drove the
V2 ``PaperRunner``) and its fill models (``fill_models.py``): nothing in production
imported them, and CATI replays through its own certification path
(``app.trading_intelligence.research.certification.replay``). What remains here is
shared with that path: the causal historical provider, the canonical cost model and
the replay identity.
"""

from app.replay.cost_model import BINANCE_FUTURES_STANDARD, CostModel
from app.replay.historical_provider import (
    HistoricalClock,
    HistoricalMarketDataProvider,
    ReplayDataError,
    ReplayMarketClient,
)
from app.replay.identity import ReplayIdentity, dataset_hash

__all__ = [
    "BINANCE_FUTURES_STANDARD",
    "CostModel",
    "HistoricalClock",
    "HistoricalMarketDataProvider",
    "ReplayDataError",
    "ReplayIdentity",
    "ReplayMarketClient",
    "dataset_hash",
]
