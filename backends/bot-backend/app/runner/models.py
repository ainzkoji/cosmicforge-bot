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
    last_regime: str = "UNKNOWN"
    last_regime_confidence: float = 0.0
    last_active_strategies: list | None = None
    last_funding_id: str = ""
    spread_history: list | None = None
    position_id: str | None = None
    current_stop_loss: float | None = None
    tp_price: float = 0.0          # TP2 price at open; passed to D-2 R:R gate
    original_sl_price: float = 0.0  # SL at open; used for PM restore after restart
    original_tp1_price: float = 0.0
    original_tp2_price: float = 0.0
