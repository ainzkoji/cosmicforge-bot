"""Market-data providers of the platform cycle driver (Section H, Step 3.1).

A provider answers one question for a timeframe: "give me the bars up to and including this decision key".
It may hold more than was asked for; the family's market stage cuts the data at the decision key before it
computes anything, so a provider cannot leak a later bar into a signal.

``FrozenPanelBars`` serves a daily panel that already exists in memory: a synthetic market in tests, or the
development period of the frozen research dataset when the pipeline is replayed against the evaluator. The
provider that follows the exchange day by day is ``app.market_data.daily_feed``.
"""
from __future__ import annotations

from typing import Any, Mapping, Protocol, Tuple


class DailyBars(Protocol):
    def panel_until(self, day: int) -> Tuple[Any, Mapping[str, Any]]:
        """``(panel, exchange metadata)`` holding every daily bar up to ``day`` (a day index)."""


class FrozenPanelBars:
    def __init__(self, panel: Any, metadata: Mapping[str, Any]) -> None:
        self._panel, self._metadata = panel, metadata
        self.calls = 0

    def panel_until(self, day: int) -> Tuple[Any, Mapping[str, Any]]:
        self.calls += 1
        return self._panel.truncated(int(day)), self._metadata


__all__ = ["DailyBars", "FrozenPanelBars"]
