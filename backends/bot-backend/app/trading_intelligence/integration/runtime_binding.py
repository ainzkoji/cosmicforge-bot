"""Compatibility metadata binding for the existing risk/management container.

This is not an alpha generator or another engine. CATI cycle/controller owns
all analysis; the dispatcher and hard-risk boundary own new entries. Calling
the removed scalar signal interface is always an error, never a fallback.
"""
from app.strategy.strategy_framework import BaseStrategy, StrategyFamily


class CatiRuntimeBinding(BaseStrategy):
    version = "cati-sole-runtime-1"
    snapshot_timeframes = ()

    def __init__(self, client=None, interval="15m", **kwargs):
        super().__init__(strategy_id="cati", family=StrategyFamily.TREND_FOLLOWING,
                         name="CATI", description="CATI runtime / governed entry authority")
        self.client = client
        self.interval = interval

    def analyze(self, *args, **kwargs):
        raise RuntimeError("LEGACY_SCALAR_SIGNAL_PATH_REMOVED_USE_CATI_CONTROLLER")

    def get_signal(self, *args, **kwargs):
        raise RuntimeError("LEGACY_SCALAR_SIGNAL_PATH_REMOVED_USE_CATI_CONTROLLER")
