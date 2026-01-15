from __future__ import annotations
from dataclasses import dataclass
from time import time


@dataclass
class LossGuardState:
    consecutive_losses: int = 0
    cooldown_until: float = 0.0


STATE = LossGuardState()


def on_trade_closed(pnl: float):
    if pnl < 0:
        STATE.consecutive_losses += 1
    else:
        STATE.consecutive_losses = 0


def should_pause(
    now: float | None = None, max_losses: int = 2, cooldown_seconds: int = 1800
) -> bool:
    now = now or time()
    if now < STATE.cooldown_until:
        return True
    if STATE.consecutive_losses >= max_losses:
        STATE.cooldown_until = now + cooldown_seconds
        STATE.consecutive_losses = 0
        return True
    return False
