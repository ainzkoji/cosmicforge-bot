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

    def get_signal(self, symbol: str) -> SignalResult:
        raise NotImplementedError
