"""
CosmicForge Safety Startup Validator

Runs before the trading runner starts.
In paper/testnet mode: logs warnings, allows startup.
In live/user-capital mode: fails closed on any critical safety violation.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import List

logger = logging.getLogger(__name__)

_SEPARATOR = "=" * 60


@dataclass
class SafetyCheckResult:
    name: str
    passed: bool
    value: str
    message: str = ""


@dataclass
class StartupSafetyReport:
    checks: List[SafetyCheckResult] = field(default_factory=list)
    all_pass: bool = True

    def add(self, name: str, passed: bool, value: str, message: str = "") -> None:
        self.checks.append(SafetyCheckResult(name=name, passed=passed, value=value, message=message))
        if not passed:
            self.all_pass = False

    def print_report(self) -> None:
        logger.info(_SEPARATOR)
        logger.info("CosmicForge Trading Bot — Safety Startup Check")
        logger.info(_SEPARATOR)
        for c in self.checks:
            status = "PASS" if c.passed else "FAIL"
            msg = f"  [{status}] {c.name}={c.value}"
            if c.message:
                msg += f"  ({c.message})"
            if c.passed:
                logger.info(msg)
            else:
                logger.error(msg)
        logger.info(_SEPARATOR)
        final = "PASS" if self.all_pass else "FAIL — trading blocked in live/user-capital mode"
        logger.info("Final safety status: %s", final)
        logger.info(_SEPARATOR)


def run_startup_safety_check(settings, mode: str = "paper") -> StartupSafetyReport:
    """
    Run all safety checks.

    Args:
        settings: app.core.config.Settings instance
        mode: "paper", "testnet", or "live"

    Returns:
        StartupSafetyReport — check .all_pass before allowing trading.
    """
    report = StartupSafetyReport()

    # 1. Default interval
    interval = getattr(settings, "DEFAULT_INTERVAL", "1m")
    report.add(
        name="DEFAULT_INTERVAL",
        passed=interval != "1m",
        value=interval,
        message="" if interval != "1m" else "1m rejected as signal interval; use 15m+",
    )

    # 2. Position adding disabled
    max_adds = getattr(settings, "MAX_ADDS_PER_POSITION", 100)
    report.add(
        name="MAX_ADDS_PER_POSITION",
        passed=max_adds == 0,
        value=str(max_adds),
        message="" if max_adds == 0 else f"Must be 0 to disable martingale adds; got {max_adds}",
    )

    # 3. Weekly drawdown
    weekly_dd = getattr(settings, "MAX_WEEKLY_DRAWDOWN_PCT", 0.0)
    report.add(
        name="MAX_WEEKLY_DRAWDOWN_PCT",
        passed=weekly_dd > 0,
        value=str(weekly_dd),
        message="" if weekly_dd > 0 else "Weekly drawdown protection disabled",
    )

    # 4. Monthly drawdown
    monthly_dd = getattr(settings, "MAX_MONTHLY_DRAWDOWN_PCT", 0.0)
    report.add(
        name="MAX_MONTHLY_DRAWDOWN_PCT",
        passed=monthly_dd > 0,
        value=str(monthly_dd),
        message="" if monthly_dd > 0 else "Monthly drawdown protection disabled",
    )

    # 5. Daily loss limit
    daily_loss = getattr(settings, "DAILY_MAX_LOSS_USDT", 0.0)
    report.add(
        name="DAILY_MAX_LOSS_USDT",
        passed=daily_loss > 0,
        value=str(daily_loss),
        message="" if daily_loss > 0 else "Daily loss protection disabled",
    )

    # 6. Max trades per day
    max_trades = getattr(settings, "MAX_TRADES_DAILY", 0)
    report.add(
        name="MAX_TRADES_DAILY",
        passed=0 < max_trades <= 20,
        value=str(max_trades),
        message="" if 0 < max_trades <= 20 else f"Should be 1-20; recommend 5 during validation",
    )

    # 7. Max open positions
    max_pos = getattr(settings, "MAX_OPEN_POSITIONS", 0)
    report.add(
        name="MAX_OPEN_POSITIONS",
        passed=0 < max_pos <= 5,
        value=str(max_pos),
        message="" if 0 < max_pos <= 5 else "Should be 1-5; recommend 2 during validation",
    )

    # 8. Minimum confidence threshold
    min_conf = getattr(settings, "MIN_CONFIDENCE_THRESHOLD", 0.0)
    report.add(
        name="MIN_CONFIDENCE_THRESHOLD",
        passed=min_conf >= 0.60,
        value=str(min_conf),
        message="" if min_conf >= 0.60 else f"Too low: {min_conf}. Recommend >= 0.70",
    )

    # 9. Execution mode
    exec_mode = getattr(settings, "EXECUTION_MODE", "paper")
    report.add(
        name="EXECUTION_MODE",
        passed=True,
        value=exec_mode,
        message="paper/testnet mode active" if exec_mode != "live" else "LIVE mode — all safety checks mandatory",
    )

    # 10. Strategy not weak fallback in live mode (A-9)
    strategy = getattr(settings, "STRATEGY_NAME", "sma_cross")
    is_weak = strategy == "sma_cross" and exec_mode == "live"
    report.add(
        name="STRATEGY_NAME",
        passed=not is_weak,
        value=strategy,
        message="" if not is_weak else "sma_cross rejected in live mode — use master_ensemble",
    )

    # 11. External signal safety
    tv_enabled = getattr(settings, "TRADINGVIEW_EXTERNAL_SIGNALS_ENABLED", False)
    tv_testnet = getattr(settings, "TRADINGVIEW_TESTNET_ONLY", True)
    tv_safe = not tv_enabled or tv_testnet or exec_mode != "live"
    report.add(
        name="EXTERNAL_SIGNALS_SAFE",
        passed=tv_safe,
        value=f"enabled={tv_enabled},testnet_only={tv_testnet}",
        message="" if tv_safe else "External signals enabled in live mode without testnet guard",
    )

    # 12. Consecutive loss pause
    max_consec = getattr(settings, "MAX_CONSECUTIVE_LOSSES", 0)
    report.add(
        name="MAX_CONSECUTIVE_LOSSES",
        passed=max_consec > 0,
        value=str(max_consec),
        message="" if max_consec > 0 else "Consecutive loss pause disabled; recommend 3",
    )

    # 13. Kill switch must close positions (A-4)
    ks_close = getattr(settings, "KILL_SWITCH_CLOSE_POSITIONS", False)
    report.add(
        name="KILL_SWITCH_CLOSE_POSITIONS",
        passed=ks_close is True,
        value=str(ks_close),
        message="" if ks_close else "Kill switch will NOT close open positions — open exposure can keep losing",
    )

    # 14. Event filter enabled (A-7)
    event_filter = getattr(settings, "EVENT_FILTER_ENABLED", False)
    report.add(
        name="EVENT_FILTER_ENABLED",
        passed=event_filter is True,
        value=str(event_filter),
        message="" if event_filter else "Event/news blackout filter disabled — bot can trade through high-impact events",
    )

    # 15. Minimum trade amount (A-8)
    trade_usdt = getattr(settings, "TRADE_USDT_PER_ORDER", 0.0)
    report.add(
        name="TRADE_USDT_PER_ORDER",
        passed=trade_usdt >= 50.0,
        value=str(trade_usdt),
        message="" if trade_usdt >= 50.0 else f"Too small: {trade_usdt} USDT. Minimum 50 USDT required to avoid exchange notional failures",
    )

    report.print_report()

    if not report.all_pass and exec_mode == "live":
        logger.error(
            "SAFETY STARTUP FAILED in live mode. "
            "Trading is blocked until all checks pass. "
            "Review the failures above and correct the configuration."
        )

    return report
