# app/models.py
from __future__ import annotations

from dataclasses import dataclass


@dataclass
class SymbolState:
    position: str = "NONE"  # "NONE" | "LONG" | "SHORT"
    entry_price: float | None = None
    last_signal: str = "HOLD"  # "BUY" | "SELL" | "HOLD"
    last_action: str = "NOOP"
    last_checked_ms: int = 0
    adds: int = 0
    last_trade_ms: int = 0
    pending_open: str = "NONE"  # "NONE" | "BUY" | "SELL"
    entry_qty: float = 0.0
    last_user_trade_id: int = 0
    SL_COOLDOWN_MINUTES: int = 60
    last_stop_ms: int = 0
    reentry_confirm_signal: str = "NONE"  # BUY / SELL / NONE
    reentry_confirm_count: int = 0
