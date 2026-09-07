from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import date
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class DailyLossState:
    day: date
    realized_pnl: float = 0.0
    kill: bool = False
    trade_count: int = 0

    # D-1: Consecutive loss tracking
    # Soft pause: after MAX_CONSECUTIVE_LOSSES_SOFT losses → cooldown for N minutes.
    # Hard pause: after MAX_CONSECUTIVE_LOSSES_HARD losses → rest of day.
    consecutive_losses: int = 0
    consec_loss_cooldown_until_ms: int = 0   # epoch ms; 0 = not in cooldown
    consec_loss_day_paused: bool = False      # True = no new entries until daily reset

    def reset_if_new_day(self) -> None:
        today = date.today()
        if today != self.day:
            self.day = today
            self.realized_pnl = 0.0
            self.kill = False
            self.trade_count = 0
            # D-1: daily reset clears consecutive loss state
            _prev = self.consecutive_losses
            self.consecutive_losses = 0
            self.consec_loss_cooldown_until_ms = 0
            self.consec_loss_day_paused = False
            if _prev > 0:
                logger.info(
                    "CONSECUTIVE_LOSS_COUNTER_RESET_DAILY — reset from %d to 0 at daily rollover.",
                    _prev,
                )

    def add_pnl(self, pnl: float, max_loss: float) -> None:
        self.realized_pnl += float(pnl)
        if self.realized_pnl <= -abs(max_loss):
            self.kill = True

    def increment_trade(self) -> None:
        self.trade_count += 1

    # ------------------------------------------------------------------
    # D-1: Consecutive loss management
    # ------------------------------------------------------------------

    def record_trade_result(
        self,
        is_win: bool,
        *,
        soft_limit: int = 3,
        cooldown_minutes: int = 120,
        hard_limit: int = 5,
        now_ms: Optional[int] = None,
    ) -> None:
        """
        Call after every closed trade.

        - is_win=True  → reset consecutive_losses to 0
        - is_win=False → increment counter; trigger soft or hard pause

        Soft pause: consecutive_losses >= soft_limit → pause for cooldown_minutes.
        Hard pause: consecutive_losses >= hard_limit → pause for rest of trading day.
        """
        _now_ms = now_ms if now_ms is not None else int(time.time() * 1000)

        if is_win:
            if self.consecutive_losses > 0:
                logger.info(
                    "CONSECUTIVE_LOSS_COUNTER_RESET_AFTER_WIN — was %d, reset to 0.",
                    self.consecutive_losses,
                )
            self.consecutive_losses = 0
            self.consec_loss_cooldown_until_ms = 0
            # Note: consec_loss_day_paused is NOT reset by a win — only by daily rollover.
            # This prevents the bot from immediately resuming after a brief win mid-day-pause.
            return

        self.consecutive_losses += 1
        logger.warning(
            "CONSECUTIVE_LOSS_RECORDED — streak now %d (soft=%d hard=%d).",
            self.consecutive_losses, soft_limit, hard_limit,
        )

        if self.consecutive_losses >= hard_limit and not self.consec_loss_day_paused:
            self.consec_loss_day_paused = True
            self.consec_loss_cooldown_until_ms = 0
            logger.error(
                "CONSECUTIVE_LOSS_DAY_PAUSE_STARTED — %d consecutive losses. "
                "All new entries blocked for the remainder of the trading day.",
                self.consecutive_losses,
            )
        elif self.consecutive_losses >= soft_limit and not self.consec_loss_day_paused:
            cooldown_ms = cooldown_minutes * 60 * 1000
            if self.consec_loss_cooldown_until_ms <= _now_ms:
                # Start a new cooldown window
                self.consec_loss_cooldown_until_ms = _now_ms + cooldown_ms
                logger.warning(
                    "CONSECUTIVE_LOSS_COOLDOWN_STARTED — %d losses. "
                    "New entries paused for %d minutes.",
                    self.consecutive_losses, cooldown_minutes,
                )

    def is_entry_paused(self, now_ms: Optional[int] = None) -> tuple[bool, str]:
        """
        Returns (paused, reason_code).
        Only blocks new entries — existing positions may still be managed.
        """
        _now_ms = now_ms if now_ms is not None else int(time.time() * 1000)

        if self.consec_loss_day_paused:
            return True, "TRADE_REJECTED_CONSECUTIVE_LOSS_DAY_PAUSE"

        if self.consec_loss_cooldown_until_ms > _now_ms:
            remaining_s = (self.consec_loss_cooldown_until_ms - _now_ms) // 1000
            logger.debug(
                "CONSECUTIVE_LOSS_TRADE_REJECTED — in cooldown, %ds remaining.", remaining_s
            )
            return True, "TRADE_REJECTED_CONSECUTIVE_LOSS_COOLDOWN"

        return False, ""
