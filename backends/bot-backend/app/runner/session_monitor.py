# app/runner/session_monitor.py
"""
Session Monitor
===============
Handles system-wide time-based session rules, specifically daily position closing.

Responsibilities:
1. Check if current time is within configured "daily close window".
2. If inside window, scan all open positions.
3. If position is profitable (matching threshold), trigger close once via
   _force_market_close (reduce_only market order).

B-1 Fix: DAILY_CLOSE_ENABLED defaults to True. Double-close removed —
_scan_and_close no longer calls executor.execute_signal() before
_force_market_close(). Only one close method is used per position.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Dict, Any, TYPE_CHECKING
try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo

from app.core.config import settings

if TYPE_CHECKING:
    from app.runner.runner import SymbolState
    from app.execution.executor import BinanceExecutor
    from shared_lib.persistence.audit import Audit

logger = logging.getLogger(__name__)


class SessionMonitor:
    def __init__(self, executor: "BinanceExecutor", audit: "Audit", bot_instance_id: str | None = None):
        self.executor = executor
        self.audit = audit
        self.bot_instance_id = bot_instance_id
        self.last_close_check_min = -1  # Prevent spamming checks every cycle

    def check_daily_close(self, state: Dict[str, Any]) -> bool:
        """
        Check if we are in the daily close window and enforce profitable closes.
        Safe to call every cycle - has internal throttling.

        Returns:
            bool: True if actions were taken, requesting a state refresh.
        """
        if not settings.DAILY_CLOSE_ENABLED:
            return False

        logger.debug("DAILY_CLOSE_CHECK_STARTED")

        now_utc = datetime.now(timezone.utc)
        try:
            tz = ZoneInfo(settings.DAILY_CLOSE_TIMEZONE)
            now_local = now_utc.astimezone(tz)
        except Exception as e:
            logger.warning(
                "Timezone %s not found, falling back to UTC. Error: %s",
                settings.DAILY_CLOSE_TIMEZONE, e,
            )
            now_local = now_utc
            settings.DAILY_CLOSE_TIMEZONE = "UTC"

        # Simple throttling: check once per minute
        current_minute = now_local.minute
        if current_minute == self.last_close_check_min:
            return False
        self.last_close_check_min = current_minute

        # Parse window
        try:
            start_h, start_m = map(int, settings.DAILY_CLOSE_WINDOW_START.split(":"))
            end_h, end_m = map(int, settings.DAILY_CLOSE_WINDOW_END.split(":"))
        except ValueError:
            logger.error("Invalid DAILY_CLOSE_WINDOW format. Use HH:MM")
            return False

        # Determine if 'now' is inside the window (supports overnight windows)
        now_mins = now_local.hour * 60 + now_local.minute
        start_mins = start_h * 60 + start_m
        end_mins = end_h * 60 + end_m

        in_window = False
        if start_mins <= end_mins:
            in_window = start_mins <= now_mins < end_mins
        else:
            # Overnight window (e.g. 23:55 to 00:35)
            in_window = (now_mins >= start_mins) or (now_mins < end_mins)

        if not in_window:
            return False

        # Product safety evidence: daily close evaluation fired within window.
        try:
            self.audit.event(
                event_type="DAILY_PROFIT_CLOSE_TRIGGERED",
                details={
                    "bot_instance_id": self.bot_instance_id,
                    "window_start": settings.DAILY_CLOSE_WINDOW_START,
                    "window_end": settings.DAILY_CLOSE_WINDOW_END,
                    "timezone": settings.DAILY_CLOSE_TIMEZONE,
                },
            )
        except Exception:
            pass

        # Scan positions
        return self._scan_and_close(state, now_local)

    def _scan_and_close(self, state: Dict[str, Any], now_local: datetime) -> bool:
        """
        Scan positions and close if profitable.

        B-1 Fix: Only _force_market_close() is called per position.
        The old executor.execute_signal() call has been removed to prevent
        double-closing the same position.

        Returns True if any close was triggered.
        """
        actions_taken = False
        for symbol, st in state.items():
            if st.position not in ("LONG", "SHORT") or st.entry_price <= 0:
                continue

            try:
                pos_info = self.executor.client.get_position(symbol)
                if not pos_info:
                    continue

                amt = float(pos_info.get("positionAmt", 0))
                if amt == 0:
                    continue

                upnl = float(pos_info.get("unRealizedProfit", 0))
                entry_margin = abs(float(pos_info.get("initialMargin", 0)))

                # Profitability check 1: USDT threshold
                usdt_ok = True
                if settings.DAILY_CLOSE_MIN_PROFIT_USDT > 0:
                    usdt_ok = upnl >= settings.DAILY_CLOSE_MIN_PROFIT_USDT
                else:
                    usdt_ok = upnl > 0

                # Profitability check 2: ROI % threshold
                pct_ok = True
                roi_pct = 0.0
                if entry_margin > 0:
                    roi_pct = (upnl / entry_margin) * 100
                if settings.DAILY_CLOSE_MIN_PROFIT_PCT > 0:
                    pct_ok = roi_pct >= settings.DAILY_CLOSE_MIN_PROFIT_PCT

                if not (usdt_ok and pct_ok and upnl > 0):
                    continue

                # Position is eligible for daily close
                logger.info(
                    "DAILY_CLOSE_POSITION_ELIGIBLE: %s PnL=%.2f (%.2f%%) @ %s",
                    symbol, upnl, roi_pct, now_local.strftime("%H:%M"),
                )

                # Determine close side
                side_to_close = "SELL" if amt > 0 else "BUY"
                qty_abs = abs(amt)

                # Cancel open orders first
                try:
                    self.executor.cancel_open_orders(symbol)
                except Exception as cancel_err:
                    logger.warning(
                        "DAILY_CLOSE: cancel_open_orders failed for %s: %s",
                        symbol, cancel_err,
                    )

                # Audit before close attempt
                try:
                    self.audit.event(
                        event_type="DAILY_CLOSE_FORCE_MARKET_CLOSE_ATTEMPTED",
                        symbol=symbol,
                        details={
                            "bot_instance_id": self.bot_instance_id,
                            "pnl": upnl,
                            "roi": roi_pct,
                            "side": side_to_close,
                            "qty": qty_abs,
                            "time_local": now_local.isoformat(),
                        },
                    )
                except Exception:
                    pass

                # B-1 Fix: Use ONLY _force_market_close — no double-close.
                # The old executor.execute_signal() call has been removed.
                logger.info(
                    "DAILY_CLOSE_NO_DOUBLE_CLOSE: using single force-market-close for %s",
                    symbol,
                )
                success = self._force_market_close(symbol, side_to_close, qty_abs)
                if success:
                    actions_taken = True
                    logger.info(
                        "DAILY_CLOSE_POSITION_CLOSE_SUCCESS: %s PnL=%.2f",
                        symbol, upnl,
                    )
                    try:
                        self.audit.event(
                            event_type="DAILY_CLOSE_POSITION_CLOSE_SUCCESS",
                            symbol=symbol,
                            details={
                                "bot_instance_id": self.bot_instance_id,
                                "pnl": upnl,
                                "roi": roi_pct,
                            },
                        )
                    except Exception:
                        pass
                else:
                    logger.error(
                        "DAILY_CLOSE_POSITION_CLOSE_FAILED: %s — force close returned failure",
                        symbol,
                    )

            except Exception as e:
                logger.error("DAILY_CLOSE_POSITION_CLOSE_FAILED: %s — %s", symbol, e)

        return actions_taken

    def _force_market_close(self, symbol: str, side: str, qty: float) -> bool:
        """
        Execute a direct market close order (reduce_only=True).
        Returns True on success, False on failure.
        """
        try:
            self.executor.client.order_market(
                symbol=symbol,
                side=side,
                quantity=qty,
                reduce_only=True,
            )
            return True
        except Exception as e:
            logger.error(
                "DAILY_CLOSE_POSITION_CLOSE_FAILED: _force_market_close for %s raised: %s",
                symbol, e,
            )
            return False
