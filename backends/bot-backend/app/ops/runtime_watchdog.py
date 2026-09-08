"""Runtime watchdog — prove the bot is actually working, not merely 'active'.

The failure this exists to catch was observed in production: the process ran
continuously for twelve hours, health reported ``WAITING_FOR_SIGNAL /
NO_NEW_CANDLE`` the whole time, and yet a **six-hour window contained zero
candle evaluations** because the host suspended. Nothing in the system noticed.

`active` means "configured to run". It says nothing about whether the strategy
clock is advancing. These two questions are now separate:

    status = active            <- is it supposed to run?
    health = HEALTHY|DEGRADED|ERROR  <- is it actually running?

The strategy-clock check is the important one: for each bot/symbol/timeframe it
compares the latest *available* closed candle against the last one actually
evaluated. Equal is healthy. Behind-but-catching-up is normal. Behind and not
advancing across multiple iterations is ``STRATEGY_CLOCK_STALLED``.
"""
from __future__ import annotations

import logging
import threading
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

logger = logging.getLogger(__name__)

# ── Canonical runtime health reasons (§6) ───────────────────────────────────

STRATEGY_CLOCK_STALLED = "STRATEGY_CLOCK_STALLED"
MARKET_DATA_STALE = "MARKET_DATA_STALE"
MARKET_DATA_UNAVAILABLE = "MARKET_DATA_UNAVAILABLE"
RUNNER_HEARTBEAT_STALE = "RUNNER_HEARTBEAT_STALE"
RUNNER_INITIALIZATION_FAILED = "RUNNER_INITIALIZATION_FAILED"
CANONICAL_EVIDENCE_WRITE_FAILED = "CANONICAL_EVIDENCE_WRITE_FAILED"
RUNTIME_OWNERSHIP_LOST = "RUNTIME_OWNERSHIP_LOST"

RUNTIME_FAULT_REASONS = frozenset({
    STRATEGY_CLOCK_STALLED, MARKET_DATA_STALE, MARKET_DATA_UNAVAILABLE,
    RUNNER_HEARTBEAT_STALE, RUNNER_INITIALIZATION_FAILED,
    CANONICAL_EVIDENCE_WRITE_FAILED, RUNTIME_OWNERSHIP_LOST,
})

HEALTHY = "HEALTHY"
DEGRADED = "DEGRADED"
ERROR = "ERROR"

#: A scheduler tick is ~10s. Beyond this the loop is not running.
SCHEDULER_HEARTBEAT_STALE_SECONDS = 60

#: Market data older than this is stale even if the last fetch "succeeded".
MARKET_DATA_STALE_SECONDS = 300

#: How many consecutive iterations may see an unevaluated newer candle before
#: we call the clock stalled. One or two is normal (the evaluation happens on
#: the next tick); sustained is a fault.
STALL_ITERATION_THRESHOLD = 3

#: Grace after a candle closes before its absence counts against coverage.
CANDLE_EVALUATION_GRACE_SECONDS = 120

TIMEFRAME_MS = {
    "1m": 60_000, "5m": 300_000, "15m": 900_000, "30m": 1_800_000,
    "1h": 3_600_000, "2h": 7_200_000, "4h": 14_400_000, "1d": 86_400_000,
}


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _age_seconds(ts: str | None) -> float | None:
    if not ts:
        return None
    try:
        when = datetime.fromisoformat(ts)
    except Exception:
        return None
    if when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)
    return (_now() - when).total_seconds()


@dataclass
class SymbolClock:
    """What the strategy clock looks like for one bot/symbol/timeframe."""

    symbol: str
    timeframe: str
    latest_available_closed_candle: int | None = None
    last_evaluated_closed_candle: int | None = None
    last_market_data_at: str | None = None
    last_market_data_error: str | None = None
    last_decision_at: str | None = None
    last_decision_reason: str | None = None
    behind_iterations: int = 0

    @property
    def is_behind(self) -> bool:
        if self.latest_available_closed_candle is None:
            return False
        if self.last_evaluated_closed_candle is None:
            return True
        return self.latest_available_closed_candle > self.last_evaluated_closed_candle

    @property
    def evaluation_lag_seconds(self) -> float | None:
        """How far behind the strategy clock is, in wall time."""
        if self.last_evaluated_closed_candle is None:
            return None
        return max(0.0, (_now().timestamp() * 1000 - self.last_evaluated_closed_candle) / 1000.0)

    def to_dict(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "timeframe": self.timeframe,
            "latest_closed_candle_at": _ms_iso(self.latest_available_closed_candle),
            "last_evaluated_closed_candle_at": _ms_iso(self.last_evaluated_closed_candle),
            "is_behind": self.is_behind,
            "behind_iterations": self.behind_iterations,
            "evaluation_lag_seconds": self.evaluation_lag_seconds,
            "last_market_data_at": self.last_market_data_at,
            "last_market_data_error": self.last_market_data_error,
            "last_strategy_decision_at": self.last_decision_at,
            "last_strategy_reason": self.last_decision_reason,
        }


def _ms_iso(ms: int | None) -> str | None:
    if ms is None:
        return None
    try:
        return datetime.fromtimestamp(ms / 1000.0, timezone.utc).isoformat()
    except Exception:
        return None


class RuntimeWatchdog:
    """Tracks liveness signals and derives an honest health verdict.

    Thread-safe: the scheduler writes from its own thread while the API reads.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.started_at = _now().isoformat()
        self.last_scheduler_heartbeat: str | None = None
        self.last_successful_iteration: str | None = None
        self.iteration_count = 0
        self.last_error: str | None = None
        self.ownership_lost_at: str | None = None
        self._bots: dict[str, dict[str, Any]] = {}

    # ── Scheduler-level signals ─────────────────────────────────────────────

    def scheduler_heartbeat(self) -> None:
        with self._lock:
            self.last_scheduler_heartbeat = _now().isoformat()

    def iteration_succeeded(self) -> None:
        with self._lock:
            self.last_successful_iteration = _now().isoformat()
            self.iteration_count += 1

    def record_error(self, message: str) -> None:
        with self._lock:
            self.last_error = f"{_now().isoformat()} {message}"[:500]

    def ownership_lost(self) -> None:
        with self._lock:
            self.ownership_lost_at = _now().isoformat()

    # ── Per-bot signals ─────────────────────────────────────────────────────

    def _bot(self, bot_id: str) -> dict[str, Any]:
        return self._bots.setdefault(bot_id, {
            "last_cycle_at": None,
            "runner_present": False,
            "runner_initialization_status": None,
            "runtime_session_id": None,
            "run_id": None,
            "policy_hash": None,
            "last_execution_attempt_at": None,
            "last_error": None,
            "clocks": {},
        })

    def bot_cycle(
        self, bot_id: str, *, run_id: str | None = None,
        policy_hash: str | None = None, runtime_session_id: str | None = None,
        runner_present: bool = True, initialization_status: str | None = None,
    ) -> None:
        with self._lock:
            bot = self._bot(bot_id)
            bot["last_cycle_at"] = _now().isoformat()
            bot["runner_present"] = runner_present
            bot["run_id"] = run_id or bot["run_id"]
            bot["policy_hash"] = policy_hash or bot["policy_hash"]
            bot["runtime_session_id"] = runtime_session_id or bot["runtime_session_id"]
            bot["runner_initialization_status"] = initialization_status or bot["runner_initialization_status"]

    def market_data(
        self, bot_id: str, symbol: str, timeframe: str,
        *, latest_closed_candle: int | None = None, error: str | None = None,
    ) -> None:
        """Record a market-data outcome. An error is NOT 'no new candle'."""
        with self._lock:
            clock = self._clock(bot_id, symbol, timeframe)
            if error:
                clock.last_market_data_error = str(error)[:300]
                return
            clock.last_market_data_error = None
            clock.last_market_data_at = _now().isoformat()
            if latest_closed_candle is not None:
                clock.latest_available_closed_candle = int(latest_closed_candle)

    def candle_evaluated(
        self, bot_id: str, symbol: str, timeframe: str,
        *, closed_candle: int | None, reason: str | None = None,
    ) -> None:
        with self._lock:
            clock = self._clock(bot_id, symbol, timeframe)
            if closed_candle is not None:
                clock.last_evaluated_closed_candle = int(closed_candle)
            clock.last_decision_at = _now().isoformat()
            clock.last_decision_reason = reason
            clock.behind_iterations = 0

    def observe_clock(self, bot_id: str, symbol: str, timeframe: str) -> None:
        """Called once per iteration to age the stall counter."""
        with self._lock:
            clock = self._clock(bot_id, symbol, timeframe)
            clock.behind_iterations = clock.behind_iterations + 1 if clock.is_behind else 0

    def execution_attempted(self, bot_id: str) -> None:
        with self._lock:
            self._bot(bot_id)["last_execution_attempt_at"] = _now().isoformat()

    def _clock(self, bot_id: str, symbol: str, timeframe: str) -> SymbolClock:
        clocks = self._bot(bot_id)["clocks"]
        key = f"{symbol}:{timeframe}"
        if key not in clocks:
            clocks[key] = SymbolClock(symbol=symbol, timeframe=timeframe)
        return clocks[key]

    # ── Verdicts ────────────────────────────────────────────────────────────

    def scheduler_health(self) -> tuple[str, str | None]:
        age = _age_seconds(self.last_scheduler_heartbeat)
        if self.ownership_lost_at:
            return ERROR, RUNTIME_OWNERSHIP_LOST
        if age is None:
            return DEGRADED, RUNNER_HEARTBEAT_STALE
        if age > SCHEDULER_HEARTBEAT_STALE_SECONDS:
            return ERROR, RUNNER_HEARTBEAT_STALE
        return HEALTHY, None

    def bot_health(self, bot_id: str) -> tuple[str, str | None]:
        """Honest verdict for one bot. Never hides a fault behind 'active'."""
        with self._lock:
            bot = self._bots.get(bot_id)
            clocks = list(bot["clocks"].values()) if bot else []
            init_status = bot.get("runner_initialization_status") if bot else None
            runner_present = bot.get("runner_present") if bot else False

        if bot is None:
            return DEGRADED, RUNNER_HEARTBEAT_STALE
        if init_status == "FAILED_INITIALIZATION" or not runner_present:
            return ERROR, RUNNER_INITIALIZATION_FAILED

        for clock in clocks:
            if clock.last_market_data_error:
                return ERROR, MARKET_DATA_UNAVAILABLE
        for clock in clocks:
            age = _age_seconds(clock.last_market_data_at)
            if age is not None and age > MARKET_DATA_STALE_SECONDS:
                return DEGRADED, MARKET_DATA_STALE
        for clock in clocks:
            if clock.behind_iterations >= STALL_ITERATION_THRESHOLD:
                return ERROR, STRATEGY_CLOCK_STALLED

        return HEALTHY, None

    def snapshot(self) -> dict[str, Any]:
        """Everything §13 asks /runner/status to expose."""
        scheduler_state, scheduler_reason = self.scheduler_health()
        with self._lock:
            bots = {}
            for bot_id, bot in self._bots.items():
                bots[bot_id] = {
                    k: v for k, v in bot.items() if k != "clocks"
                } | {"symbols": {k: c.to_dict() for k, c in bot["clocks"].items()}}
        for bot_id in list(bots):
            state, reason = self.bot_health(bot_id)
            bots[bot_id]["health"] = state
            bots[bot_id]["health_reason"] = reason
        return {
            "started_at": self.started_at,
            "scheduler_heartbeat_at": self.last_scheduler_heartbeat,
            "scheduler_heartbeat_age_seconds": _age_seconds(self.last_scheduler_heartbeat),
            "last_successful_iteration_at": self.last_successful_iteration,
            "iteration_count": self.iteration_count,
            "scheduler_health": scheduler_state,
            "scheduler_health_reason": scheduler_reason,
            "ownership_lost_at": self.ownership_lost_at,
            "last_error": self.last_error,
            "bots": bots,
        }

    def stalled_bots(self) -> list[tuple[str, str]]:
        """(bot_id, reason) for every bot currently in a runtime fault."""
        result = []
        for bot_id in list(self._bots):
            state, reason = self.bot_health(bot_id)
            if state == ERROR and reason:
                result.append((bot_id, reason))
        return result


#: Process-wide watchdog. One scheduler per process, so one watchdog.
_WATCHDOG: RuntimeWatchdog | None = None


def get_watchdog() -> RuntimeWatchdog:
    global _WATCHDOG
    if _WATCHDOG is None:
        _WATCHDOG = RuntimeWatchdog()
    return _WATCHDOG


# ── Candle coverage (§10 / §18) ─────────────────────────────────────────────


def expected_candle_slots(start_ms: int, end_ms: int, timeframe: str) -> list[int]:
    """Every closed-candle close time in [start, end] for this timeframe."""
    step = TIMEFRAME_MS.get(timeframe)
    if not step:
        return []
    first = ((start_ms + 1) // step) * step - 1
    slots = []
    t = first
    while t <= end_ms:
        if t >= start_ms:
            slots.append(t)
        t += step
    return slots


def candle_coverage(
    db: Any, *, bot_instance_id: str, symbols: list[str], timeframe: str,
    since_ms: int, until_ms: int,
) -> dict[str, Any]:
    """Expected closed candles vs actual canonical evaluations (§10).

    A missing slot is a real gap: either the runtime was not running, or it was
    running and failed to evaluate. Both must be visible.
    """
    cutoff = int(_now().timestamp() * 1000) - CANDLE_EVALUATION_GRACE_SECONDS * 1000
    slots = [s for s in expected_candle_slots(since_ms, until_ms, timeframe) if s <= cutoff]

    per_symbol: dict[str, Any] = {}
    with db.connect() as conn:
        for symbol in symbols:
            rows = conn.execute(
                """SELECT DISTINCT closed_candle_close_time FROM trading_decisions
                   WHERE bot_instance_id=? AND symbol=? AND timeframe=?
                     AND primary_reason <> 'NO_NEW_CANDLE'
                     AND closed_candle_close_time BETWEEN ? AND ?""",
                (bot_instance_id, symbol.upper(), timeframe, since_ms, until_ms),
            ).fetchall()
            evaluated = {int(r[0]) for r in rows}
            missing = [s for s in slots if s not in evaluated]
            per_symbol[symbol.upper()] = {
                "expected": len(slots),
                "evaluated": len([s for s in slots if s in evaluated]),
                "missing": len(missing),
                "coverage_pct": round(100.0 * (len(slots) - len(missing)) / len(slots), 2) if slots else None,
                "missing_candle_times": [_ms_iso(s) for s in missing[:50]],
            }

    total_expected = sum(v["expected"] for v in per_symbol.values())
    total_missing = sum(v["missing"] for v in per_symbol.values())
    return {
        "bot_instance_id": bot_instance_id,
        "timeframe": timeframe,
        "window": {"since": _ms_iso(since_ms), "until": _ms_iso(until_ms)},
        "expected_total": total_expected,
        "evaluated_total": total_expected - total_missing,
        "missing_total": total_missing,
        "coverage_pct": round(100.0 * (total_expected - total_missing) / total_expected, 2)
        if total_expected else None,
        "per_symbol": per_symbol,
    }
