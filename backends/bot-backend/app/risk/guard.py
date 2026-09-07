from __future__ import annotations

from dataclasses import dataclass
from threading import Lock
from time import time


@dataclass
class LossGuardState:
    consecutive_losses: int = 0
    cooldown_until: float = 0.0


_states: dict[str, LossGuardState] = {}
_lock = Lock()


def _get_state(bot_id: str) -> LossGuardState:
    with _lock:
        if bot_id not in _states:
            _states[bot_id] = LossGuardState()
        return _states[bot_id]


def on_trade_closed(pnl: float, bot_id: str = "default") -> None:
    """Record trade outcome for a specific bot. Resets streak on win."""
    state = _get_state(bot_id)
    if pnl < 0:
        state.consecutive_losses += 1
    else:
        state.consecutive_losses = 0


def should_pause(
    bot_id: str = "default",
    now: float | None = None,
    max_losses: int = 3,
    cooldown_seconds: int = 7200,
) -> bool:
    """Return True if this bot should be paused. State is per-bot — never shared."""
    now = now or time()
    state = _get_state(bot_id)

    if now < state.cooldown_until:
        return True

    if state.consecutive_losses >= max_losses:
        state.cooldown_until = now + cooldown_seconds
        state.consecutive_losses = 0
        return True

    return False


def reset_bot(bot_id: str) -> None:
    """Clear state for a single bot (e.g. on daily reset). Does not affect other bots."""
    with _lock:
        _states.pop(bot_id, None)


def reset_all() -> None:
    """Clear all per-bot state. Use in tests only."""
    with _lock:
        _states.clear()
