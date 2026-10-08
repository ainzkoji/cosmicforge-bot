"""Step 1.0d -- the real production loop, cycle and collector scheduler, driven
by injected clocks and controlled broker fakes (no real 30-second or one-hour
waits).

Entry points exercised: ``production_runtime.run`` (the asyncio loop),
``broker_cycle`` / ``sync`` / ``sync_account`` (the 30-second broker cycle),
``residual_prospective.schedule`` (the hourly collector's scheduler) and
``production_execution.process_account`` (one account evaluation). Exchange-side
stop read-back, restart with open positions and the fail-safe close are covered
by ``test_protection_read_safety.py`` and ``test_runtime_cost_budget.py`` on the
same entry points.
"""
import asyncio
import json
import sqlite3
import threading
import time
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from test_production_execution import live, profile
from test_production_demo_execution import demo, demo_profile
from test_production_research_separation import fresh, process
from shared_lib.core.production import LiveOrderSubmissionDisabled, order_submission_gate, require_broker_mutation_permission
from shared_lib.persistence.db import DB
from app.core import config
from app.execution import production_schema
from app.trading_intelligence.integration import production_execution as production
from app.trading_intelligence.integration import production_runtime as runtime
from app.trading_intelligence.integration import residual_prospective as collector

__all__ = ["live", "demo", "fresh"]


class Clock:
    """Injected monotonic clock: ``sleep`` advances it and stops the loop after ``limit`` sleeps."""

    def __init__(self, limit):
        self.t, self.sleeps, self.limit = 1_000.0, [], limit

    def monotonic(self):
        return self.t

    async def sleep(self, seconds):
        self.sleeps.append(round(seconds, 6))
        self.t += seconds
        if len(self.sleeps) >= self.limit:
            raise asyncio.CancelledError


@pytest.fixture
def loop(monkeypatch):
    """The real ``run`` loop with its clock, sleep and settings injected."""
    monkeypatch.setattr(runtime, "settings", demo_profile())
    monkeypatch.setattr(runtime, "owner_current", lambda db: True)
    for key in ("loop_started", "cycle_started", "cycle_completed"):
        runtime._progress[key] = None
    runtime._progress.update(cycles=0, cycles_without_lease=0, last_error=None)
    runtime._idle.set()
    calls = {"residual": 0, "forward": 0, "fx": 0}
    monkeypatch.setattr(runtime, "residual_schedule", lambda runner: calls.__setitem__("residual", calls["residual"] + 1))
    monkeypatch.setattr(runtime, "forward_schedule", lambda runner: calls.__setitem__("forward", calls["forward"] + 1))
    from app.trading_intelligence.integration import residual_simulation
    monkeypatch.setattr(residual_simulation, "ensure_fx_watcher", lambda: calls.__setitem__("fx", calls["fx"] + 1))
    from app.execution import demo_boundary_certification, demo_transport_smoke
    monkeypatch.setattr(demo_transport_smoke, "process_local_request", lambda db: None)
    monkeypatch.setattr(demo_boundary_certification, "process_local_request", lambda db: None)

    def start(cycles, sync, work_seconds=2.0):
        clock = Clock(cycles)

        def timed_sync(db):
            clock.t += work_seconds                       # the broker work of one cycle
            return sync(db)
        monkeypatch.setattr(runtime, "sync", timed_sync)
        monkeypatch.setattr(runtime, "time", SimpleNamespace(monotonic=clock.monotonic, time=time.time, perf_counter=time.perf_counter))
        monkeypatch.setattr(runtime, "asyncio", SimpleNamespace(sleep=clock.sleep, to_thread=asyncio.to_thread,
                                                                CancelledError=asyncio.CancelledError))
        with pytest.raises(asyncio.CancelledError):
            asyncio.run(runtime.run("db"))
        return clock
    return SimpleNamespace(start=start, calls=calls)


# ── 1-5: the loop itself ─────────────────────────────────────────────────────

def test_the_loop_starts_and_keeps_a_thirty_second_cadence(loop):
    synced = []
    clock = loop.start(3, synced.append)
    assert synced == ["db", "db", "db"]
    assert clock.sleeps == [28.0, 28.0, 28.0]              # 30 s minus the 2 s of broker work
    progress = runtime.progress()
    assert progress["cycles_completed"] == 3 and progress["cycles_without_lease"] == 0
    assert progress["last_error"] is None and progress["cycle_in_flight"] is False
    assert loop.calls == {"residual": 3, "forward": 3, "fx": 1}   # the FX watcher check is at most once a minute


def test_a_slow_cycle_sleeps_at_least_one_second(loop):
    clock = loop.start(2, lambda db: None, work_seconds=45.0)
    assert clock.sleeps == [1.0, 1.0]


def test_without_the_runtime_lease_the_loop_counts_and_touches_no_broker(loop, monkeypatch):
    monkeypatch.setattr(runtime, "owner_current", lambda db: False)
    synced = []
    loop.start(2, synced.append)
    assert synced == [] and runtime.progress()["cycles_without_lease"] == 2
    assert loop.calls == {"residual": 0, "forward": 0, "fx": 0}


def test_a_database_outage_fails_one_cycle_and_the_next_cycle_recovers(loop):
    attempts = []

    def flaky(db):
        attempts.append(db)
        if len(attempts) == 1:
            raise sqlite3.OperationalError("database is locked")
    loop.start(2, flaky)
    assert len(attempts) == 2 and runtime.progress()["cycles_completed"] == 1
    assert runtime.progress()["last_error"] is None       # the latest cycle's truth, not history


def test_a_failing_auxiliary_step_never_costs_the_broker_cycle(loop, monkeypatch):
    monkeypatch.setattr(runtime, "residual_schedule", lambda runner: 1 / 0)
    synced = []
    loop.start(2, synced.append)
    assert synced == ["db", "db"]
    assert runtime.progress()["last_error"] == "residual_collection_schedule:ZeroDivisionError"


def test_duplicate_cycle_invocations_are_serialised_never_interleaved(monkeypatch):
    from app.execution import demo_boundary_certification, demo_transport_smoke
    monkeypatch.setattr(demo_transport_smoke, "process_local_request", lambda db: None)
    monkeypatch.setattr(demo_boundary_certification, "process_local_request", lambda db: None)
    spans, lock = [], threading.Lock()

    def sync(db):
        start = time.perf_counter()
        time.sleep(0.05)
        with lock:
            spans.append((start, time.perf_counter()))
    monkeypatch.setattr(runtime, "sync", sync)
    threads = [threading.Thread(target=runtime.broker_cycle, args=("db",)) for _ in range(3)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    spans.sort()
    assert len(spans) == 3 and all(spans[i][1] <= spans[i + 1][0] for i in range(2))
    assert runtime.wait_idle(0)


# ── 6-9: account discovery, paused / stopped / no bot, maintenance-only ──────

@pytest.fixture
def accounts_db(tmp_path, monkeypatch):
    """A minimal production database: one connected DEMO account, no bot yet."""
    production_schema.forget()
    from shared_lib.persistence.migrations import migrate
    db = DB(str(tmp_path / "loop.db"))                      # the real, migrated schema; one account row
    migrate(db)
    with db.connect() as c:
        c.execute("INSERT INTO broker_accounts (id,user_id,broker_id,market_type,environment,status,created_at,updated_at) "
                  "VALUES('acct','alice','binance','crypto','demo','connected','2026-10-08','2026-10-08')")
    runtime.initialize(db)
    state = demo_profile()
    monkeypatch.setattr(runtime, "settings", state)
    monkeypatch.setattr(production, "settings", state)
    monkeypatch.setattr(config, "settings", state)
    monkeypatch.setattr(runtime, "owner_current", lambda db: True)
    monkeypatch.setattr(production, "owner_current", lambda db: True)
    from app.activation import account_status
    monkeypatch.setattr(account_status, "refresh_if_stale", lambda *a, **kw: {"status": "SYNCED"})
    monkeypatch.setattr(runtime, "resolve_broker_auth", lambda *a: SimpleNamespace(
        environment="demo", account_id="acct", user_id="alice", broker_type="binance",
        base_url="https://demo-fapi.binance.com", credential_version=1, key_fingerprint="fp"))
    from app.ops import runtime_shutdown
    monkeypatch.setattr(runtime_shutdown, "stop_requested", lambda: False)
    client = Mock()
    client.account.return_value = {"totalWalletBalance": "1000", "totalMarginBalance": "1000", "availableBalance": "1000",
                                   "totalInitialMargin": "0", "totalUnrealizedProfit": "0"}
    client.position_risk.return_value = [{"symbol": "ADAUSDT", "positionAmt": "0", "positionSide": "BOTH"}]
    client.open_orders.return_value = []
    runtime._clients.clear()
    original = runtime.sync_account
    monkeypatch.setattr(runtime, "sync_account", lambda database, account, **kw: original(
        database, account, factory=lambda auth: client, **kw))
    yield SimpleNamespace(db=db, client=client)
    runtime._clients.clear()
    production_schema.forget()


BOT_ROW = ("INSERT INTO bot_instances (id,user_id,broker_account_id,market_type,strategy_id,mode,status,created_at,updated_at) "
           "VALUES(?,'alice','acct','crypto','cati','live',?,'2026-10-08','2026-10-08')")


def state_of(db, account="acct"):
    with db.connect() as c:
        return json.loads(c.execute("SELECT document FROM cati_production_state WHERE account_id=?", (account,)).fetchone()[0])


def test_an_account_without_a_tradable_bot_is_discovered_but_not_evaluated(accounts_db):
    runtime.sync(accounts_db.db)                            # the real cycle, real process_account
    state = state_of(accounts_db.db)
    assert state["status"] == "SYNCED" and state["orders_read"] == "SKIPPED_IDLE_ACCOUNT"
    assert state["execution"]["evaluation_scope"] == "IDLE_NO_TRADABLE_BOT"
    assert state["execution"]["reason"] == "AUTO_TRADING_DISABLED"
    assert state["execution"]["execution_permission"] == "BLOCKED_ACCOUNT"
    assert "risk" not in state["execution"]                 # no account risk, no income, no order book
    accounts_db.client.open_orders.assert_not_called()
    accounts_db.client.income_history.assert_not_called()


def test_a_newly_deployed_running_bot_is_picked_up_on_the_next_cycle(accounts_db, monkeypatch):
    seen = []
    monkeypatch.setattr(production, "process_account", lambda db, account, client, snapshot: seen.append(
        (snapshot["orders_read"], production.idle_account(db, account, snapshot["positions"]))) or
        {"execution_permission": "BLOCKED_ACCOUNT", "reason": "SPY"})
    runtime.sync(accounts_db.db)
    with accounts_db.db.connect() as c:                     # the customer deploys a bot between two cycles
        c.execute(BOT_ROW, ("bot1", "active"))
    runtime.sync(accounts_db.db)
    assert seen == [("SKIPPED_IDLE_ACCOUNT", True), ("ACCOUNT_WIDE", False)]
    assert accounts_db.client.open_orders.call_count == 1    # read once the account had something to evaluate


@pytest.mark.parametrize("status", ["paused", "stopped"])
def test_a_paused_or_stopped_bot_is_not_evaluated_but_its_open_position_is(accounts_db, monkeypatch, status):
    with accounts_db.db.connect() as c:
        c.execute(BOT_ROW, ("bot1", status))
    seen = []
    monkeypatch.setattr(production, "process_account", lambda db, account, client, snapshot: seen.append(
        snapshot["orders_read"]) or {"execution_permission": "BLOCKED_ACCOUNT", "reason": "SPY"})
    runtime.sync(accounts_db.db)
    assert seen == ["SKIPPED_IDLE_ACCOUNT"]                  # no entry evaluation for a paused / stopped bot
    accounts_db.client.position_risk.return_value = [{"symbol": "ADAUSDT", "positionAmt": "3", "positionSide": "BOTH"}]
    runtime.sync(accounts_db.db)
    assert seen == ["SKIPPED_IDLE_ACCOUNT", "ACCOUNT_WIDE"]  # an open position is always maintained


def test_a_broker_outage_marks_the_account_read_failed_and_the_next_cycle_recovers(accounts_db):
    accounts_db.client.account.side_effect = TimeoutError("read timed out")
    accounts_db.client.get_balance.side_effect = TimeoutError("read timed out")
    runtime.sync(accounts_db.db)
    state = state_of(accounts_db.db)
    assert state["status"] == "READ_FAILED" and state["reason"] == "TimeoutError"
    assert state["execution_permission"] == "BLOCKED_ACCOUNT"
    accounts_db.client.account.side_effect = None
    runtime.sync(accounts_db.db)
    assert state_of(accounts_db.db)["status"] == "SYNCED"


def test_one_failing_account_does_not_stop_the_next_one(accounts_db, monkeypatch):
    with accounts_db.db.connect() as c:
        c.execute("INSERT INTO broker_accounts (id,user_id,broker_id,market_type,environment,status,created_at,updated_at) "
                  "VALUES('acct2','bob','binance','crypto','demo','connected','2026-10-08','2026-10-08')")
    original = runtime.sync_account
    monkeypatch.setattr(runtime, "sync_account", lambda db, account, **kw: (_ for _ in ()).throw(RuntimeError("boom"))
                        if account["id"] == "acct" else original(db, account, **kw))
    runtime.sync(accounts_db.db)
    assert state_of(accounts_db.db, "acct")["status"] == "READ_FAILED"
    assert state_of(accounts_db.db, "acct2")["status"] == "SYNCED"


def test_maintenance_only_runs_when_an_auxiliary_sync_step_fails(accounts_db, monkeypatch):
    from app.activation import account_status
    monkeypatch.setattr(account_status, "refresh_if_stale", Mock(side_effect=RuntimeError("discovery unavailable")))
    maintained = Mock()
    monkeypatch.setattr(production, "maintain_only", maintained)
    runtime.sync(accounts_db.db)
    maintained.assert_called_once()
    assert state_of(accounts_db.db)["status"] == "READ_FAILED"


# ── 10-12: the hourly collector's scheduler ─────────────────────────────────

@pytest.fixture
def scheduler(monkeypatch):
    monkeypatch.delenv("COSMICFORGE_TEST_MODE", raising=False)
    monkeypatch.setattr(collector, "owner_current", lambda db: True)
    clock = {"t": 10_000.0}
    monkeypatch.setattr(collector, "time", SimpleNamespace(monotonic=lambda: clock["t"], time=time.time))
    collected = []

    class Pool:
        def submit(self, fn, db):
            collected.append(db)
            future = Mock()
            future.done.return_value = True
            future.add_done_callback = lambda cb: None
            return future
    monkeypatch.setattr(collector, "_pool", Pool())
    monkeypatch.setattr(collector, "_future", None)
    monkeypatch.setattr(collector, "_last", 0.0)
    return SimpleNamespace(clock=clock, collected=collected, runner=SimpleNamespace(db="db"))


def test_the_collector_is_scheduled_at_most_once_a_minute(scheduler):
    s = scheduler
    collector.schedule(s.runner)
    s.clock["t"] += 30
    collector.schedule(s.runner)                            # too early: not repeated
    s.clock["t"] += 31
    collector.schedule(s.runner)
    assert s.collected == ["db", "db"]


def test_a_rate_limit_backs_the_collector_off_for_five_minutes(scheduler):
    s = scheduler
    collector.schedule(s.runner)
    failed = Mock()
    failed.result.side_effect = collector.RateLimited("public reference rate limit")
    collector._completed(failed)
    s.clock["t"] += 200
    collector.schedule(s.runner)                            # inside the backoff
    assert s.collected == ["db"]
    s.clock["t"] += 101
    collector.schedule(s.runner)
    assert s.collected == ["db", "db"]


def test_the_collector_never_runs_without_the_lease(scheduler, monkeypatch):
    monkeypatch.setattr(collector, "owner_current", lambda db: False)
    collector.schedule(scheduler.runner)
    assert scheduler.collected == []


# ── 13-15: kill switch, no live orders, no duplicate orders ─────────────────

def test_the_kill_switch_blocks_the_new_entry_and_nothing_is_sent(fresh):
    boundary = fresh.boundary_for()
    boundary.authority.gov = SimpleNamespace(kill_switch_on=lambda **kw: True)
    result = process(fresh)
    assert result["reason"] == "CATI_NEW_ENTRY_KILL_SWITCH" and result["execution_permission"] == "BLOCKED_RISK"
    assert result["kill_switch"] is True
    fresh.client.place_order.assert_not_called()


def test_two_cycles_on_the_same_decision_place_exactly_one_order(fresh):
    first = process(fresh)
    assert "boundary" in first and fresh.client.place_order.call_count == 1
    second = process(fresh)
    assert fresh.client.place_order.call_count == 1          # the decision was already attempted
    assert second["execution_permission"] in ("WAITING_SIGNAL", "BLOCKED_RISK"), second["reason"]


def test_live_order_submission_stays_impossible_under_the_demo_profile(accounts_db, monkeypatch):
    with accounts_db.db.connect() as c:
        c.execute("UPDATE broker_accounts SET environment='live' WHERE id='acct'")
    monkeypatch.setattr(runtime, "resolve_broker_auth", lambda *a: SimpleNamespace(
        environment="live", account_id="acct", user_id="alice", broker_type="binance",
        base_url="https://fapi.binance.com", credential_version=1, key_fingerprint="fp"))
    runtime.sync(accounts_db.db)
    status = runtime.status(accounts_db.db, user_id="alice")
    account = status["accounts"][0]
    assert account["environment"] == "LIVE" and account["execution_permission"] == "BLOCKED_LIVE_ORDER_GATE"
    assert status["live_order_submission_enabled"] is False and order_submission_gate("live")["enabled"] is False
    with pytest.raises(LiveOrderSubmissionDisabled):
        require_broker_mutation_permission("POST", "/fapi/v1/order", environment="LIVE", broker="binance",
                                           base_url="https://fapi.binance.com", client=accounts_db.client,
                                           payload={"symbol": "ADAUSDT", "side": "BUY"})
    accounts_db.client._signed_post.assert_not_called()
    accounts_db.client.place_order.assert_not_called()
