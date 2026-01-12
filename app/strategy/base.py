from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Dict, Optional


class Signal(str, Enum):
    BUY = "BUY"
    SELL = "SELL"
    HOLD = "HOLD"


@dataclass
class SignalResult:
    signal: Signal
    confidence: float
    reason: str
    meta: Optional[Dict] = None


class Strategy:
    name: str = "base"
    version: str = "1.0.0"

    def get_signal(self, symbol: str) -> SignalResult:
        """
        symbol must be canonical (e.g. EURUSD, BTCUSDT).
        Broker-specific symbol mapping belongs in broker adapters later.
        """
        raise NotImplementedError
