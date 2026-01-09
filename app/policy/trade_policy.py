# app/policy/trade_policy.py
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Optional


class Action(str, Enum):
    HOLD = "HOLD"
    OPEN_LONG = "OPEN_LONG"
    OPEN_SHORT = "OPEN_SHORT"
    ADD_LONG = "ADD_LONG"
    ADD_SHORT = "ADD_SHORT"
    CLOSE = "CLOSE"
    FLIP_TO_LONG = "FLIP_TO_LONG"
    FLIP_TO_SHORT = "FLIP_TO_SHORT"


@dataclass
class PolicyInputs:
    # state
    position: str  # "flat" | "long" | "short"
    adds: int
    pending_open: Optional[str]  # None | "BUY" | "SELL"
    reentry_confirm_signal: Optional[str]  # None | "BUY" | "SELL"
    reentry_confirm_count: int
    last_trade_ms: int
    last_stop_ms: int

    # current signal from strategy
    signal: str  # "BUY" | "SELL" | "HOLD"

    # policy settings (passed in so it stays broker-agnostic)
    cooldown_seconds: int
    sl_cooldown_seconds: int
    max_adds: int
    trade_mode: str  # "flip" | "oneway"
    reentry_confirmations: int

    # context
    now_ms: int
    kill_switch: bool


@dataclass
class PolicyResult:
    action: Action
    # state updates (runner applies them)
    pending_open: Optional[str]
    reentry_confirm_signal: Optional[str]
    reentry_confirm_count: int
    reason: str


def _ms(seconds: int) -> int:
    return int(seconds) * 1000


def cooldown_ok(last_trade_ms: int, now_ms: int, cooldown_seconds: int) -> bool:
    if cooldown_seconds <= 0:
        return True
    if last_trade_ms <= 0:
        return True
    return (now_ms - last_trade_ms) >= _ms(cooldown_seconds)


def sl_cooldown_ok(last_stop_ms: int, now_ms: int, sl_cooldown_seconds: int) -> bool:
    if sl_cooldown_seconds <= 0:
        return True
    if last_stop_ms <= 0:
        return True
    return (now_ms - last_stop_ms) >= _ms(sl_cooldown_seconds)


def can_open_or_add(
    kill_switch: bool,
    last_trade_ms: int,
    last_stop_ms: int,
    now_ms: int,
    cooldown_seconds: int,
    sl_cooldown_seconds: int,
) -> bool:
    if kill_switch:
        return False
    if not cooldown_ok(last_trade_ms, now_ms, cooldown_seconds):
        return False
    if not sl_cooldown_ok(last_stop_ms, now_ms, sl_cooldown_seconds):
        return False
    return True


def can_add(position: str, adds: int, max_adds: int) -> bool:
    if position not in ("long", "short"):
        return False
    return int(adds) < int(max_adds)


def _flip_target(signal: str) -> Optional[Action]:
    if signal == "BUY":
        return Action.FLIP_TO_LONG
    if signal == "SELL":
        return Action.FLIP_TO_SHORT
    return None


def decide(inp: PolicyInputs) -> PolicyResult:
    """
    Broker-agnostic policy brain.
    - Enforces kill-switch + cooldowns
    - Enforces max-add rules
    - Supports flip mode with pending-open + confirmation logic

    Runner should:
      - call this with current SymbolState + latest signal
      - execute returned action
      - apply returned state updates
    """
    # default: preserve state
    pending_open = inp.pending_open
    re_sig = inp.reentry_confirm_signal
    re_cnt = int(inp.reentry_confirm_count)

    # Global blocks (no opens/adds)
    open_allowed = can_open_or_add(
        kill_switch=inp.kill_switch,
        last_trade_ms=inp.last_trade_ms,
        last_stop_ms=inp.last_stop_ms,
        now_ms=inp.now_ms,
        cooldown_seconds=inp.cooldown_seconds,
        sl_cooldown_seconds=inp.sl_cooldown_seconds,
    )

    # HOLD always returns hold (but preserves state)
    if inp.signal == "HOLD":
        return PolicyResult(Action.HOLD, pending_open, re_sig, re_cnt, "signal=HOLD")

    # If kill/cooldown block is active, we can still allow CLOSE actions (runner decides close)
    # Here: block any open/add/flip intent.
    if not open_allowed:
        return PolicyResult(
            Action.HOLD, pending_open, re_sig, re_cnt, "blocked_by_kill_or_cooldown"
        )

    # --- Position = FLAT ---
    if inp.position == "flat":
        # If confirmations are required, accumulate
        if inp.reentry_confirmations > 1:
            if re_sig == inp.signal:
                re_cnt += 1
            else:
                re_sig = inp.signal
                re_cnt = 1

            if re_cnt < inp.reentry_confirmations:
                return PolicyResult(
                    Action.HOLD,
                    pending_open,
                    re_sig,
                    re_cnt,
                    "waiting_reentry_confirmations",
                )

            # confirmed → reset confirmation counters on open
            re_sig = None
            re_cnt = 0

        # Open direction
        if inp.signal == "BUY":
            return PolicyResult(Action.OPEN_LONG, None, re_sig, re_cnt, "open_long")
        if inp.signal == "SELL":
            return PolicyResult(Action.OPEN_SHORT, None, re_sig, re_cnt, "open_short")

    # --- Position = LONG ---
    if inp.position == "long":
        if inp.signal == "BUY":
            if can_add(inp.position, inp.adds, inp.max_adds):
                return PolicyResult(
                    Action.ADD_LONG, pending_open, re_sig, re_cnt, "add_long"
                )
            return PolicyResult(
                Action.HOLD, pending_open, re_sig, re_cnt, "max_adds_reached"
            )

        if inp.signal == "SELL":
            # flip logic
            if inp.trade_mode == "flip":
                flip = _flip_target(inp.signal)
                return PolicyResult(flip, "SELL", re_sig, re_cnt, "flip_long_to_short")
            return PolicyResult(
                Action.CLOSE, pending_open, re_sig, re_cnt, "close_long"
            )

    # --- Position = SHORT ---
    if inp.position == "short":
        if inp.signal == "SELL":
            if can_add(inp.position, inp.adds, inp.max_adds):
                return PolicyResult(
                    Action.ADD_SHORT, pending_open, re_sig, re_cnt, "add_short"
                )
            return PolicyResult(
                Action.HOLD, pending_open, re_sig, re_cnt, "max_adds_reached"
            )

        if inp.signal == "BUY":
            if inp.trade_mode == "flip":
                flip = _flip_target(inp.signal)
                return PolicyResult(flip, "BUY", re_sig, re_cnt, "flip_short_to_long")
            return PolicyResult(
                Action.CLOSE, pending_open, re_sig, re_cnt, "close_short"
            )

    # fallback
    return PolicyResult(Action.HOLD, pending_open, re_sig, re_cnt, "no_rule_matched")
