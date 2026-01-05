from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass
class RiskDecision:
    allowed: bool
    reason: str
    kill: bool
    realized_pnl: float
    max_loss: float


class RiskGate:
    """
    Single source of truth for whether new trades may be opened.

    - OPEN (BUY/SELL) must pass this gate.
    - CLOSE must still be allowed (unless you explicitly block it).
    """

    def __init__(self, *, get_daily_state, max_loss_usdt: float):
        # get_daily_state returns object with: kill, realized_pnl
        self.get_daily_state = get_daily_state
        self.max_loss_usdt = float(max_loss_usdt)

    def can_open(self) -> RiskDecision:
        daily = self.get_daily_state()
        kill = bool(getattr(daily, "kill", False))
        pnl = float(getattr(daily, "realized_pnl", 0.0))

        if kill:
            return RiskDecision(
                allowed=False,
                reason="kill_switch_active",
                kill=True,
                realized_pnl=pnl,
                max_loss=self.max_loss_usdt,
            )

        # extra safety: if pnl <= -max_loss but kill not set yet, block anyway
        if pnl <= -abs(self.max_loss_usdt):
            return RiskDecision(
                allowed=False,
                reason="daily_max_loss_reached_autoblock",
                kill=True,
                realized_pnl=pnl,
                max_loss=self.max_loss_usdt,
            )

        return RiskDecision(
            allowed=True,
            reason="ok",
            kill=False,
            realized_pnl=pnl,
            max_loss=self.max_loss_usdt,
        )
