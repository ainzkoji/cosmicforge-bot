from __future__ import annotations
from dataclasses import dataclass
from datetime import date


@dataclass
class DailyLossState:
    day: date
    realized_pnl: float = 0.0
    kill: bool = False
    trade_count: int = 0

    def reset_if_new_day(self) -> None:
        today = date.today()
        if today != self.day:
            self.day = today
            self.realized_pnl = 0.0
            self.kill = False
            self.trade_count = 0

    def add_pnl(self, pnl: float, max_loss: float) -> None:
        self.realized_pnl += float(pnl)
        if self.realized_pnl <= -abs(max_loss):
            self.kill = True

    def increment_trade(self) -> None:
        self.trade_count += 1
