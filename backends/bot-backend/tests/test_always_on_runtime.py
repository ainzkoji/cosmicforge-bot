"""Always-on runtime hardening.

The failure this suite exists for was real: the process ran continuously for
twelve hours, health reported ``WAITING_FOR_SIGNAL / NO_NEW_CANDLE``
throughout, and a six-hour window contained zero candle evaluations because the
host suspended. Nothing noticed.

Three properties are pinned here:

* one trading scheduler per **database** (not per port);
* a stalled strategy clock is detected and reported, never disguised as a quiet
  market;
* a market-data failure is never reported as "no new candle".
"""
from __future__ import annotations

import inspect
import os
from datetime import datetime, timedelta, timezone

import pytest

from app.ops.runtime_ownership import (
    LEASE_STALE_SECONDS,
    TRADING_SCHEDULER,
    RuntimeOwnership,
    current_owner,
    ensure_ownership_schema,
)
from app.ops.runtime_watchdog import (
    DEGRADED,
    ERROR,
    HEALTHY,
    MARKET_DATA_STALE,
    MARKET_DATA_UNAVAILABLE,
    RECOVERY_TIER_RUNNER_REBUILD,
    RECOVERY_TIER_SOFT_REFRESH,
    RUNNER_HEARTBEAT_STALE,
    RUNNER_RECOVERY_FAILED,
    RUNTIME_FAULT_REASONS,
    RUNTIME_OWNERSHIP_LOST,
    STALL_ITERATION_THRESHOLD,
    STRATEGY_CLOCK_STALLED,
    RuntimeWatchdog,
    candle_coverage,
    expected_candle_slots,
)
from shared_lib.persistence.db import DB
from shared_lib.persistence.migrations import migrate

DB_PATH = "/runtime/cosmicforge.db"
TF_MS = 900_000


@pytest.fixture
def db():
    database = DB(":memory:")
    migrate(database)
    ensure_ownership_schema(database)
    return database


@pytest.fixture
def watchdog():
    return RuntimeWatchdog()


# ══════════════════════════════════════════════════════════════════════════
# §1/§2 — one canonical runner owner
# ══════════════════════════════════════════════════════════════════════════


def test_the_first_process_acquires_the_lease(db):
    result = RuntimeOwnership(db, database_path=DB_PATH).acquire()
    assert result.acquired is True
    assert result.may_start_scheduler is True
    assert result.runtime_owner_id


def test_a_second_process_is_denied_and_must_not_schedule(db):
    first = RuntimeOwnership(db, database_path=DB_PATH)
    first.acquire()

    second = RuntimeOwnership(db, database_path=DB_PATH)
    result = second.acquire()

    assert result.acquired is False
    assert result.may_start_scheduler is False
    assert result.reason == "RUNTIME_OWNERSHIP_HELD_BY_ANOTHER_PROCESS"
    assert result.holder_pid == os.getpid()


def test_two_simultaneous_startups_yield_exactly_one_owner(db):
    """Both processes try at once; exactly one may schedule."""
    candidates = [RuntimeOwnership(db, database_path=DB_PATH) for _ in range(5)]
    results = [c.acquire() for c in candidates]

    winners = [r for r in results if r.acquired]
    assert len(winners) == 1, "exactly one scheduler owner"
    assert all(r.reason == "RUNTIME_OWNERSHIP_HELD_BY_ANOTHER_PROCESS"
               for r in results if not r.acquired)


def test_the_lease_is_keyed_on_the_database_not_the_port(db):
    """Two backends on different ports, same DB, still collide."""
    a = RuntimeOwnership(db, database_path=DB_PATH)
    b = RuntimeOwnership(db, database_path=DB_PATH)
    assert a.acquire().acquired is True
    assert b.acquire().acquired is False

    # A genuinely different database is independent.
    c = RuntimeOwnership(db, database_path="/runtime/other.db")
    assert c.acquire().acquired is True


def test_a_released_lease_can_be_taken_immediately(db):
    first = RuntimeOwnership(db, database_path=DB_PATH)
    first.acquire()
    first.release(reason="SHUTDOWN")

    assert RuntimeOwnership(db, database_path=DB_PATH).acquire().acquired is True


def test_a_stale_lease_is_taken_over_so_a_crash_recovers(db):
    holder = RuntimeOwnership(db, database_path=DB_PATH)
    holder.acquire()

    stale = (datetime.now(timezone.utc) - timedelta(seconds=LEASE_STALE_SECONDS + 60)).isoformat()
    with db.connect() as conn:
        conn.execute(
            "UPDATE runtime_ownership SET heartbeat_at=?, pid=? WHERE lease_name=?",
            (stale, 999_999, TRADING_SCHEDULER),
        )

    assert RuntimeOwnership(db, database_path=DB_PATH).acquire().acquired is True


def test_a_live_holder_is_never_stolen_from(db):
    holder = RuntimeOwnership(db, database_path=DB_PATH)
    holder.acquire()
    holder.renew()

    assert RuntimeOwnership(db, database_path=DB_PATH).acquire().acquired is False


def test_renewal_fails_once_the_lease_was_taken(db):
    first = RuntimeOwnership(db, database_path=DB_PATH)
    first.acquire()

    with db.connect() as conn:  # simulate a takeover
        conn.execute(
            "UPDATE runtime_ownership SET runtime_owner_id='someone_else' WHERE lease_name=?",
            (TRADING_SCHEDULER,),
        )

    assert first.renew() is False
    assert first.is_owner is False


def test_ownership_is_visible_to_operators(db):
    owner = RuntimeOwnership(db, database_path=DB_PATH, database_role="paper")
    owner.acquire()

    row = current_owner(db, DB_PATH)
    assert row is not None
    assert row["pid"] == os.getpid()
    assert row["database_role"] == "paper"
    assert row["hostname"]


def test_the_scheduler_refuses_to_start_without_the_lease():
    from app.runner.multi_runner import MultiBotRunner

    source = inspect.getsource(MultiBotRunner.run_forever) if hasattr(MultiBotRunner, "run_forever") else ""
    if not source:
        source = inspect.getsource(MultiBotRunner)
    assert "acquire_runtime_ownership" in source
    assert "refusing to start scheduler" in source


def test_losing_the_lease_stops_the_scheduler():
    from app.runner.multi_runner import MultiBotRunner

    source = inspect.getsource(MultiBotRunner)
    assert "lease lost" in source
    assert RUNTIME_OWNERSHIP_LOST in source


# ══════════════════════════════════════════════════════════════════════════
# §4/§5 — scheduler heartbeat and the strategy clock
# ══════════════════════════════════════════════════════════════════════════


def test_a_fresh_heartbeat_is_healthy(watchdog):
    watchdog.scheduler_heartbeat()
    state, reason = watchdog.scheduler_health()
    assert state == HEALTHY
    assert reason is None


def test_a_missing_heartbeat_is_not_healthy(watchdog):
    state, reason = watchdog.scheduler_health()
    assert state == DEGRADED
    assert reason == RUNNER_HEARTBEAT_STALE


def test_a_stale_heartbeat_is_an_error(watchdog):
    """The observed six-hour suspension must surface as a fault."""
    watchdog.last_scheduler_heartbeat = (
        datetime.now(timezone.utc) - timedelta(hours=6)
    ).isoformat()
    state, reason = watchdog.scheduler_health()
    assert state == ERROR
    assert reason == RUNNER_HEARTBEAT_STALE


def test_losing_ownership_is_reported(watchdog):
    watchdog.scheduler_heartbeat()
    watchdog.ownership_lost()
    state, reason = watchdog.scheduler_health()
    assert state == ERROR
    assert reason == RUNTIME_OWNERSHIP_LOST


def test_no_new_candle_between_boundaries_is_healthy(watchdog):
    """Equal clocks: the market is quiet, and that is fine."""
    watchdog.bot_cycle("bot1")
    watchdog.market_data("bot1", "BTCUSDT", "15m", latest_closed_candle=1000)
    watchdog.candle_evaluated("bot1", "BTCUSDT", "15m", closed_candle=1000)

    for _ in range(20):  # many heartbeats on the same candle
        watchdog.observe_clock("bot1", "BTCUSDT", "15m")

    assert watchdog.bot_health("bot1") == (HEALTHY, None)


def test_a_newly_available_candle_is_not_immediately_a_stall(watchdog):
    """One or two iterations behind is normal — evaluation happens next tick."""
    watchdog.bot_cycle("bot1")
    watchdog.candle_evaluated("bot1", "BTCUSDT", "15m", closed_candle=1000)
    watchdog.market_data("bot1", "BTCUSDT", "15m", latest_closed_candle=1000 + TF_MS)

    watchdog.observe_clock("bot1", "BTCUSDT", "15m")
    assert watchdog.bot_health("bot1")[0] == HEALTHY


def test_a_clock_that_stops_advancing_is_detected(watchdog):
    watchdog.bot_cycle("bot1")
    watchdog.candle_evaluated("bot1", "BTCUSDT", "15m", closed_candle=1000)
    watchdog.market_data("bot1", "BTCUSDT", "15m", latest_closed_candle=1000 + TF_MS)

    for _ in range(STALL_ITERATION_THRESHOLD):
        watchdog.observe_clock("bot1", "BTCUSDT", "15m")

    assert watchdog.bot_health("bot1") == (ERROR, STRATEGY_CLOCK_STALLED)


def test_evaluating_the_new_candle_clears_the_stall(watchdog):
    watchdog.bot_cycle("bot1")
    watchdog.candle_evaluated("bot1", "BTCUSDT", "15m", closed_candle=1000)
    watchdog.market_data("bot1", "BTCUSDT", "15m", latest_closed_candle=1000 + TF_MS)
    for _ in range(STALL_ITERATION_THRESHOLD + 2):
        watchdog.observe_clock("bot1", "BTCUSDT", "15m")
    assert watchdog.bot_health("bot1")[1] == STRATEGY_CLOCK_STALLED

    watchdog.candle_evaluated("bot1", "BTCUSDT", "15m", closed_candle=1000 + TF_MS)
    assert watchdog.bot_health("bot1") == (HEALTHY, None)


def test_symbols_have_independent_clocks(watchdog):
    watchdog.bot_cycle("bot1")
    for symbol in ("BTCUSDT", "ETHUSDT"):
        watchdog.candle_evaluated("bot1", symbol, "15m", closed_candle=1000)
        watchdog.market_data("bot1", symbol, "15m", latest_closed_candle=1000 + TF_MS)

    # Only BTC falls behind.
    for _ in range(STALL_ITERATION_THRESHOLD):
        watchdog.observe_clock("bot1", "BTCUSDT", "15m")
    watchdog.candle_evaluated("bot1", "ETHUSDT", "15m", closed_candle=1000 + TF_MS)

    snapshot = watchdog.snapshot()["bots"]["bot1"]["symbols"]
    assert snapshot["BTCUSDT:15m"]["is_behind"] is True
    assert snapshot["ETHUSDT:15m"]["is_behind"] is False
    assert watchdog.bot_health("bot1")[1] == STRATEGY_CLOCK_STALLED


def test_the_stalled_bot_list_drives_recovery(watchdog):
    watchdog.bot_cycle("bot1")
    watchdog.candle_evaluated("bot1", "BTCUSDT", "15m", closed_candle=1000)
    watchdog.market_data("bot1", "BTCUSDT", "15m", latest_closed_candle=1000 + TF_MS)
    for _ in range(STALL_ITERATION_THRESHOLD):
        watchdog.observe_clock("bot1", "BTCUSDT", "15m")

    assert ("bot1", STRATEGY_CLOCK_STALLED) in watchdog.stalled_bots()


def test_recovery_rebuilds_the_runner_without_flattening():
    """§7 — a rebuild is a configuration event, never an exit signal."""
    from app.runner.multi_runner import MultiBotRunner

    source = inspect.getsource(MultiBotRunner._check_strategy_clocks)
    assert "_evict_runner" in source

    # Check executable lines only: the docstring legitimately explains that a
    # rebuild must never flatten anything.
    docstring_end = source.index('"""', source.index('"""') + 3) + 3
    body = "\n".join(
        line for line in source[docstring_end:].splitlines()
        if line.strip() and not line.strip().startswith("#")
    )
    for forbidden in ("close_position", "flatten", "activate_kill_switch"):
        assert forbidden not in body, f"recovery must not {forbidden}"


# ══════════════════════════════════════════════════════════════════════════
# §9 — market-data failure is not "no new candle"
# ══════════════════════════════════════════════════════════════════════════


def test_a_fetch_error_is_reported_as_unavailable_not_quiet(watchdog):
    watchdog.bot_cycle("bot1")
    watchdog.market_data("bot1", "BTCUSDT", "15m", error="NameResolutionError: binance")

    state, reason = watchdog.bot_health("bot1")
    assert state == ERROR
    assert reason == MARKET_DATA_UNAVAILABLE
    assert reason != "NO_NEW_CANDLE"


def test_recovering_market_data_clears_the_error(watchdog):
    watchdog.bot_cycle("bot1")
    watchdog.market_data("bot1", "BTCUSDT", "15m", error="ConnectionError")
    assert watchdog.bot_health("bot1")[1] == MARKET_DATA_UNAVAILABLE

    watchdog.market_data("bot1", "BTCUSDT", "15m", latest_closed_candle=1000)
    watchdog.candle_evaluated("bot1", "BTCUSDT", "15m", closed_candle=1000)
    assert watchdog.bot_health("bot1") == (HEALTHY, None)


def test_old_but_successful_market_data_is_stale_not_unavailable(watchdog):
    watchdog.bot_cycle("bot1")
    watchdog.market_data("bot1", "BTCUSDT", "15m", latest_closed_candle=1000)
    watchdog.candle_evaluated("bot1", "BTCUSDT", "15m", closed_candle=1000)
    clock = watchdog._clock("bot1", "BTCUSDT", "15m")
    clock.last_market_data_at = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()

    assert watchdog.bot_health("bot1") == (DEGRADED, MARKET_DATA_STALE)


def test_the_runner_returns_a_market_data_reason_on_fetch_failure():
    from app.runner.runner import PaperRunner

    source = inspect.getsource(PaperRunner._step_symbol_evaluate)
    assert "MARKET_DATA_UNAVAILABLE" in source
    # The failure branch must not fall through into the candle gate.
    fetch = source.index("fetch failed")
    assert '"reason_code": MARKET_DATA_UNAVAILABLE' in source[fetch:fetch + 600]


# ══════════════════════════════════════════════════════════════════════════
# §10/§18 — candle coverage
# ══════════════════════════════════════════════════════════════════════════


def test_expected_slots_are_the_15m_grid():
    slots = expected_candle_slots(0, TF_MS * 4, "15m")
    assert len(slots) == 4
    assert all((s + 1) % TF_MS == 0 for s in slots)


def test_full_coverage_reports_100_percent(db):
    from app.evidence.decision_recorder import record_decision

    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    since = now_ms - TF_MS * 4
    slots = expected_candle_slots(since, now_ms - TF_MS, "15m")
    for slot in slots:
        for symbol in ("BTCUSDT", "ETHUSDT"):
            with record_decision(
                db, bot_instance_id="bot1", symbol=symbol, timeframe="15m",
                closed_candle_close_time=slot,
            ) as decision:
                decision.hold("NO_OPPORTUNITY")

    report = candle_coverage(
        db, bot_instance_id="bot1", symbols=["BTCUSDT", "ETHUSDT"],
        timeframe="15m", since_ms=since, until_ms=now_ms - TF_MS,
    )
    assert report["missing_total"] == 0
    assert report["coverage_pct"] == 100.0


def test_a_gap_is_detected_and_named(db):
    """The six-hour production gap must be visible, not inferred."""
    from app.evidence.decision_recorder import record_decision

    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    since = now_ms - TF_MS * 6
    slots = expected_candle_slots(since, now_ms - TF_MS, "15m")
    for slot in slots[:2]:  # only the first two evaluated
        with record_decision(
            db, bot_instance_id="bot1", symbol="BTCUSDT", timeframe="15m",
            closed_candle_close_time=slot,
        ) as decision:
            decision.hold("NO_OPPORTUNITY")

    report = candle_coverage(
        db, bot_instance_id="bot1", symbols=["BTCUSDT"],
        timeframe="15m", since_ms=since, until_ms=now_ms - TF_MS,
    )
    assert report["missing_total"] == len(slots) - 2
    assert report["coverage_pct"] < 100.0
    assert report["per_symbol"]["BTCUSDT"]["missing_candle_times"]


def test_heartbeats_do_not_count_as_coverage(db):
    """A NO_NEW_CANDLE tick is not an evaluation."""
    from app.evidence.decision_recorder import record_no_new_candle

    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    since = now_ms - TF_MS * 3
    slots = expected_candle_slots(since, now_ms - TF_MS, "15m")
    for slot in slots:
        record_no_new_candle(
            db, bot_instance_id="bot1", symbol="BTCUSDT", timeframe="15m",
            closed_candle_close_time=slot,
        )

    report = candle_coverage(
        db, bot_instance_id="bot1", symbols=["BTCUSDT"],
        timeframe="15m", since_ms=since, until_ms=now_ms - TF_MS,
    )
    assert report["evaluated_total"] == 0


# ══════════════════════════════════════════════════════════════════════════
# §11 — heartbeat storage is bounded
# ══════════════════════════════════════════════════════════════════════════


def test_heartbeat_ticks_are_counted_not_persisted_as_decisions(db):
    """~17k rows/day is not acceptable; the cycle summary carries the count."""
    from app.evidence.runner_bridge import record_symbol_evaluation

    class Ctx:
        bot_instance_id = "bot1"
        user_id = "u"
        broker_account_id = "b"
        market_type = "CRYPTO"
        broker_environment = "demo"
        effective_policy_hash = "h"

    class Runner:
        def __init__(self, database):
            self.db = database
            self.context = Ctx()
            self.run_id = "r"
            self.cycle_id = "c"
            self.runtime_session_id = "s"
            self.interval = "15m"
            self._symbol_evidence = {}

        def _effective_execution_mode(self):
            return "paper"

    runner = Runner(db)
    for _ in range(50):
        record_symbol_evaluation(
            runner, "BTCUSDT",
            evaluate=lambda s: {"decision": "NO_NEW_CANDLE", "reason_code": "NO_NEW_CANDLE"},
        )

    with db.connect() as conn:
        rows = conn.execute("SELECT COUNT(*) FROM trading_decisions").fetchone()[0]
    assert rows == 0, "heartbeats must not become decision rows"
    assert runner._heartbeat_counts["BTCUSDT"] == 50


def test_a_real_evaluation_still_becomes_a_decision(db):
    from app.evidence.runner_bridge import record_symbol_evaluation

    class Ctx:
        bot_instance_id = "bot1"
        user_id = "u"
        broker_account_id = "b"
        market_type = "CRYPTO"
        broker_environment = "demo"
        effective_policy_hash = "h"

    class Runner:
        def __init__(self, database):
            self.db = database
            self.context = Ctx()
            self.run_id = "r"
            self.cycle_id = "c"
            self.runtime_session_id = "s"
            self.interval = "15m"
            self._symbol_evidence = {}

        def _effective_execution_mode(self):
            return "paper"

    runner = Runner(db)
    record_symbol_evaluation(
        runner, "BTCUSDT",
        evaluate=lambda s: {"decision": "HOLD", "reason": "NO_OPPORTUNITY"},
    )
    with db.connect() as conn:
        rows = conn.execute(
            "SELECT primary_reason FROM trading_decisions"
        ).fetchall()
    assert [r[0] for r in rows] == ["NO_OPPORTUNITY"]


def test_the_cycle_summary_reports_heartbeats_from_the_tally():
    from app.runner.runner import PaperRunner

    source = inspect.getsource(PaperRunner._record_canonical_cycle)
    assert "_heartbeat_counts" in source
    assert "no_new_candle_count" in source


# ══════════════════════════════════════════════════════════════════════════
# §12/§13 — honest health semantics and status freshness
# ══════════════════════════════════════════════════════════════════════════


def test_active_never_hides_a_runtime_fault(watchdog):
    """status=active + broken clock must not read as healthy."""
    watchdog.bot_cycle("bot1")
    watchdog.candle_evaluated("bot1", "BTCUSDT", "15m", closed_candle=1000)
    watchdog.market_data("bot1", "BTCUSDT", "15m", latest_closed_candle=1000 + TF_MS)
    for _ in range(STALL_ITERATION_THRESHOLD):
        watchdog.observe_clock("bot1", "BTCUSDT", "15m")

    bot = watchdog.snapshot()["bots"]["bot1"]
    assert bot["health"] == ERROR
    assert bot["health_reason"] == STRATEGY_CLOCK_STALLED


def test_a_missing_runner_is_an_error_not_merely_quiet(watchdog):
    watchdog.bot_cycle("bot1", runner_present=False)
    assert watchdog.bot_health("bot1")[0] == ERROR


def test_the_status_snapshot_exposes_every_freshness_field(watchdog):
    watchdog.scheduler_heartbeat()
    watchdog.bot_cycle("bot1", run_id="r1", policy_hash="h1", runtime_session_id="s1")
    watchdog.market_data("bot1", "BTCUSDT", "15m", latest_closed_candle=1000)
    watchdog.candle_evaluated("bot1", "BTCUSDT", "15m", closed_candle=1000, reason="NO_OPPORTUNITY")
    watchdog.execution_attempted("bot1")

    snapshot = watchdog.snapshot()
    for key in ("scheduler_heartbeat_at", "scheduler_heartbeat_age_seconds",
                "last_successful_iteration_at", "iteration_count", "scheduler_health"):
        assert key in snapshot

    bot = snapshot["bots"]["bot1"]
    for key in ("runner_present", "runner_initialization_status", "last_cycle_at",
                "run_id", "policy_hash", "runtime_session_id",
                "last_execution_attempt_at", "health", "health_reason"):
        assert key in bot

    clock = bot["symbols"]["BTCUSDT:15m"]
    for key in ("latest_closed_candle_at", "last_evaluated_closed_candle_at",
                "evaluation_lag_seconds", "last_market_data_at",
                "last_strategy_decision_at", "last_strategy_reason"):
        assert key in clock


def test_runner_status_exposes_runtime_health():
    import app.main as main

    source = inspect.getsource(main)
    for field in ("runtime_health", "scheduler_heartbeat_at", "symbol_clocks",
                  "last_execution_attempt_at"):
        assert field in source


# ══════════════════════════════════════════════════════════════════════════
# §3 — the start script
# ══════════════════════════════════════════════════════════════════════════


def _script() -> str:
    from pathlib import Path

    path = Path(__file__).resolve().parents[3] / "scripts" / "start_trading_runtime.ps1"
    assert path.exists(), f"missing {path}"
    return path.read_text(encoding="utf-8")


def test_the_start_script_never_uses_reload():
    """--reload restarts on DB/log writes and kills the runner loop."""
    source = _script()
    # Ignore the comment that explains why; check the actual argument list.
    code = "\n".join(
        line for line in source.splitlines() if not line.strip().startswith("#")
    )
    assert "--reload" not in code
    assert "uvicorn" in code


def test_the_start_script_checks_the_lease_before_starting():
    """The lease probe moved into runtime_probe.py (PowerShell 5.1 mangles
    multi-line here-strings passed to `python -c`), so assert on the call."""
    source = _script()
    assert "runtime_probe.py" in source
    assert "$Probe lease" in source
    assert "Refusing to start a duplicate canonical runner" in source

    # And the probe itself must actually consult the ownership lease.
    from pathlib import Path

    probe = (Path(__file__).resolve().parents[3] / "scripts" / "runtime_probe.py").read_text(
        encoding="utf-8"
    )
    assert "current_owner" in probe
    assert "RuntimeOwnership" in probe
    # It must load the backend .env explicitly, or it resolves the wrong DB.
    assert "load_dotenv" in probe


def test_the_start_script_uses_bounded_backoff():
    source = _script()
    assert "@(5, 10, 30, 60)" in source
    assert "restarting in" in source


def test_the_start_script_distinguishes_operator_shutdown_from_a_crash():
    source = _script()
    assert "operator shutdown" in source
    assert "not restarting" in source
    assert "CRASH" in source


def test_the_start_script_forces_utf8_and_records_the_pid():
    source = _script()
    assert "PYTHONUTF8" in source
    assert "runtime.pid" in source


# ══════════════════════════════════════════════════════════════════════════
# Restart must not look like a stall
# ══════════════════════════════════════════════════════════════════════════


def test_a_restarted_process_is_not_mistaken_for_a_stalled_one(watchdog):
    """Observed on the first supervised restart.

    A fresh process has an empty in-memory clock while the candle marker is
    persisted. Without seeding, latest_available > last_evaluated (None) made a
    perfectly up-to-date bot report STRATEGY_CLOCK_STALLED.
    """
    watchdog.bot_cycle("bot1")
    watchdog.market_data("bot1", "BTCUSDT", "15m", latest_closed_candle=5000)

    # Previous process already evaluated 5000; this one learns that from the DB.
    watchdog.seed_evaluated_candle("bot1", "BTCUSDT", "15m", closed_candle=5000)
    for _ in range(STALL_ITERATION_THRESHOLD + 2):
        watchdog.observe_clock("bot1", "BTCUSDT", "15m")

    assert watchdog.bot_health("bot1") == (HEALTHY, None)


def test_seeding_does_not_mask_a_genuine_stall(watchdog):
    """Seeding an OLD marker must still leave a newer candle looking behind."""
    watchdog.bot_cycle("bot1")
    watchdog.seed_evaluated_candle("bot1", "BTCUSDT", "15m", closed_candle=5000)
    watchdog.market_data("bot1", "BTCUSDT", "15m", latest_closed_candle=5000 + TF_MS)

    for _ in range(STALL_ITERATION_THRESHOLD):
        watchdog.observe_clock("bot1", "BTCUSDT", "15m")

    assert watchdog.bot_health("bot1")[1] == STRATEGY_CLOCK_STALLED


def test_seeding_never_overwrites_a_real_evaluation(watchdog):
    watchdog.bot_cycle("bot1")
    watchdog.candle_evaluated("bot1", "BTCUSDT", "15m", closed_candle=9000)
    watchdog.seed_evaluated_candle("bot1", "BTCUSDT", "15m", closed_candle=1000)

    clock = watchdog.snapshot()["bots"]["bot1"]["symbols"]["BTCUSDT:15m"]
    assert clock["last_evaluated_closed_candle_at"] is not None
    assert watchdog._clock("bot1", "BTCUSDT", "15m").last_evaluated_closed_candle == 9000


def test_the_runner_seeds_the_marker_when_a_candle_was_already_claimed():
    from app.runner.runner import PaperRunner

    source = inspect.getsource(PaperRunner._step_symbol_evaluate)
    assert "seed_evaluated_candle" in source
    assert "last_evaluated_candle" in source


# ── Tiered safe recovery (A7) ───────────────────────────────────────────────


def _stall(watchdog, bot_id="bot1", symbol="BTCUSDT"):
    """Drive one bot into STRATEGY_CLOCK_STALLED."""
    watchdog.bot_cycle(bot_id)
    watchdog.candle_evaluated(bot_id, symbol, "15m", closed_candle=1000)
    watchdog.market_data(bot_id, symbol, "15m", latest_closed_candle=1000 + TF_MS)
    for _ in range(STALL_ITERATION_THRESHOLD):
        watchdog.observe_clock(bot_id, symbol, "15m")


def test_recovery_tries_the_cheap_remedy_before_rebuilding(watchdog):
    """A7 -- a rebuild is the second resort, not the first."""
    _stall(watchdog)
    assert watchdog.next_recovery_tier("bot1") == RECOVERY_TIER_SOFT_REFRESH

    watchdog.recovery_attempted("bot1", RECOVERY_TIER_SOFT_REFRESH)
    assert watchdog.next_recovery_tier("bot1") == RECOVERY_TIER_RUNNER_REBUILD

    watchdog.recovery_attempted("bot1", RECOVERY_TIER_RUNNER_REBUILD)
    assert watchdog.next_recovery_tier("bot1") is None


def test_exhausted_recovery_escalates_instead_of_thrashing(watchdog):
    """A6 -- RUNNER_RECOVERY_FAILED replaces an endless rebuild loop."""
    _stall(watchdog)
    watchdog.recovery_attempted("bot1", RECOVERY_TIER_SOFT_REFRESH)
    watchdog.recovery_attempted("bot1", RECOVERY_TIER_RUNNER_REBUILD)
    watchdog.recovery_failed("bot1", "clock still stalled")

    state, reason = watchdog.bot_health("bot1")
    assert (state, reason) == (ERROR, RUNNER_RECOVERY_FAILED)
    assert RUNNER_RECOVERY_FAILED in RUNTIME_FAULT_REASONS
    # The escalated fault is what recovery reads, so the loop stops retrying.
    assert ("bot1", RUNNER_RECOVERY_FAILED) in watchdog.stalled_bots()


def test_an_advancing_clock_clears_the_escalation(watchdog):
    """Recovery that works must not leave the bot latched in ERROR."""
    _stall(watchdog)
    watchdog.recovery_attempted("bot1", RECOVERY_TIER_SOFT_REFRESH)
    watchdog.recovery_attempted("bot1", RECOVERY_TIER_RUNNER_REBUILD)
    watchdog.recovery_failed("bot1", "clock still stalled")
    assert watchdog.bot_health("bot1")[1] == RUNNER_RECOVERY_FAILED

    watchdog.candle_evaluated("bot1", "BTCUSDT", "15m", closed_candle=1000 + TF_MS)

    assert watchdog.bot_health("bot1") == (HEALTHY, None)
    assert watchdog.next_recovery_tier("bot1") == RECOVERY_TIER_SOFT_REFRESH


def test_soft_refresh_reseeds_the_marker_without_faking_a_decision(watchdog):
    """The marker resync corrects a drifted view; it is not an evaluation."""
    _stall(watchdog)
    before = watchdog.snapshot()["bots"]["bot1"]["symbols"]["BTCUSDT:15m"]

    watchdog.sync_evaluated_marker("bot1", "BTCUSDT", "15m", 1000 + TF_MS)
    after = watchdog.snapshot()["bots"]["bot1"]["symbols"]["BTCUSDT:15m"]

    assert after["is_behind"] is False
    assert after["behind_iterations"] == 0
    # No decision was claimed, so the last-decision timestamp is untouched.
    assert after["last_strategy_decision_at"] == before["last_strategy_decision_at"]


def test_soft_refresh_reads_the_persisted_candle_marker(db, monkeypatch):
    """Tier 1 reloads bot_candle_evaluations -- the table that gates entry."""
    from app.ops import runtime_watchdog as wd_mod
    from app.runner.multi_runner import MultiBotRunner

    watchdog = wd_mod.RuntimeWatchdog()
    monkeypatch.setattr(wd_mod, "_WATCHDOG", watchdog)

    with db.connect() as conn:
        conn.execute(
            """INSERT INTO bot_candle_evaluations
               (bot_instance_id,symbol,timeframe,last_closed_candle_time,updated_at)
               VALUES (?,?,?,?,?)""",
            ("botX", "BTCUSDT", "15m", 5000, datetime.now(timezone.utc).isoformat()),
        )

    _stall(watchdog, bot_id="botX")
    assert watchdog.bot_health("botX")[1] == STRATEGY_CLOCK_STALLED

    runner = MultiBotRunner.__new__(MultiBotRunner)
    runner.db = db
    runner._runners = {}
    assert runner._soft_refresh_bot("botX") is True

    clock = watchdog.snapshot()["bots"]["botX"]["symbols"]["BTCUSDT:15m"]
    assert clock["last_evaluated_closed_candle_at"] is not None
    assert clock["behind_iterations"] == 0


def test_tiered_recovery_never_flattens_at_any_tier():
    """A7 -- no tier of automatic recovery may become an exit signal."""
    from app.runner.multi_runner import MultiBotRunner

    for func in (MultiBotRunner._check_strategy_clocks, MultiBotRunner._soft_refresh_bot):
        source = inspect.getsource(func)
        docstring_end = source.index('"""', source.index('"""') + 3) + 3
        body = "\n".join(
            line for line in source[docstring_end:].splitlines()
            if line.strip() and not line.strip().startswith("#")
        )
        for forbidden in ("close_position", "flatten", "activate_kill_switch"):
            assert forbidden not in body, f"{func.__name__} must not {forbidden}"

    # The rebuild tier is still reached -- recovery is tiered, not weakened.
    assert "_evict_runner" in inspect.getsource(MultiBotRunner._check_strategy_clocks)
