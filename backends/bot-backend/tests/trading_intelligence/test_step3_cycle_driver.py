"""Step 3.1: the CATI platform cycle driver, run through its real entry point on a synthetic market."""
from __future__ import annotations

import ast
import importlib
import json
import sys
import types
from pathlib import Path

import pytest

import _step2
import _step3 as S
from app.trading_intelligence.families.daily_trend import spec
from app.trading_intelligence.families.daily_trend import targets as T
from app.trading_intelligence.pipeline import contracts as C
from app.trading_intelligence.pipeline import daily_trend as DT
from app.trading_intelligence.pipeline import driver as D
from app.trading_intelligence.pipeline import family_registry as FR
from app.trading_intelligence.pipeline.accounts import (
    CombinedAccounts,
    ConnectedAccounts,
    ModelAccounts,
    model_account_id,
)
from app.trading_intelligence.pipeline.market_data import FrozenPanelBars

EQUITY = spec.INTERPRETATION["initial_equity_usdt"]
START = DT.day_of("2020-09-01")
PIPELINE_DIR = Path(D.__file__).resolve().parent
SHADOW_PATH_MODULES = ("driver", "daily_trend", "shadow_book", "accounts", "market_data", "shadow_compare")


class Setup:
    def __init__(self, tmp_path, *, accounts=None, last="2021-06-30"):
        self.db = S.fresh_db(tmp_path)
        self.research = _step2.new_register(tmp_path / "register.jsonl")
        self.dataset = _step2.SyntheticDataset()
        self.panel = self.dataset._load("2020-01-01", last)
        self.data = FrozenPanelBars(self.panel, self.dataset.metadata)
        self.registry = FR.FamilyRegistry(self.db, research_register=self.research)
        self.registry.register_all(actor_ref="system:test")
        self.accounts = accounts or ModelAccounts(initial_equity=EQUITY, levels=("balanced", "aggressive"),
                                                  policies=(T.MANDATE,))
        self.driver = self.make()

    def make(self, **kw):
        kw.setdefault("accounts", self.accounts)
        kw.setdefault("data", {"1d": self.data})
        kw.setdefault("registry", self.registry)
        return D.PlatformCycleDriver(self.db, **kw)

    def run(self, day, driver=None, **kw):
        return (driver or self.driver).run_cycle("DAILY_TREND", DT.day_text(day), now_ms=DT.close_ms(day) + 60_000, **kw)

    def count(self, table, where="1=1", args=()):
        with self.db.connect() as conn:
            return conn.execute(f"SELECT COUNT(*) FROM {table} WHERE {where}", args).fetchone()[0]

    def rows(self, sql, args=()):
        with self.db.connect() as conn:
            return [tuple(r) for r in conn.execute(sql, args)]

    def book(self, account_id):
        with self.db.connect() as conn:
            return dict(conn.execute("SELECT * FROM cati_shadow_accounts WHERE account_id=?", (account_id,)).fetchone())


def sub(tmp_path, name):
    path = tmp_path / name
    path.mkdir()
    return path


@pytest.fixture
def s(tmp_path):
    return Setup(tmp_path)


# ------------------------------------------------------------------------------------------ the full cycle
def test_a_full_cycle_runs_every_pipeline_stage_in_shadow_and_records_immutable_decisions(s):
    result = s.run(START)
    assert (result.status, result.accounts_total, result.accounts_done, result.mode) == (D.COMPLETED, 2, 2, C.SHADOW)
    with s.db.connect() as conn:
        cycle = dict(conn.execute("SELECT * FROM cati_platform_cycles").fetchone())
        decisions = [dict(r) for r in conn.execute("SELECT * FROM cati_family_decisions ORDER BY account_id")]
    market = json.loads(cycle["detail"])["market"]
    document = json.loads(decisions[0]["document"])
    assert set(market["stages"]) | set(document["stages"]) == set(DT.STAGES)
    assert market["stages"]["MARKET_STATE"]["status"] == "NOT_APPLICABLE"        # stated, not faked
    assert cycle["cycle_id"] == D.cycle_id_of("DAILY_TREND", DT.day_text(START), C.SHADOW)
    assert cycle["rule_hash"] == s.registry.entry("DAILY_TREND").definition.rule_hash and cycle["market_hash"]
    for row in decisions:
        doc = json.loads(row["document"])
        assert row["mode"] == C.SHADOW and row["environment"] == "MODEL" and doc["mode"] == C.SHADOW
        assert row["decision_time"] == DT.close_ms(START) and row["expires_at"] == DT.close_ms(START) + 86_400_000
        assert row["mandate_id"] == "MANDATE_004" and doc["data_snapshot_id"] == market["data_snapshot_id"]
        assert doc["order_authority"]["handoff"] == "NONE" and "FAMILY_NOT_CERTIFIED" in doc["order_authority"]["blockers"]
        assert doc["stages"]["EXECUTION_BOUNDARY"]["status"] == "SHADOW_ONLY"
        for intent in doc["intents"]:                 # what was stored is valid under the trading contracts
            assert C.intent_from_plain(intent).symbol == intent["symbol"]
    assert any(json.loads(r["document"])["intents"] for r in decisions)
    assert s.count("cati_platform_cycle_metrics") >= 4


def test_shadow_operation_touches_no_execution_path(s, monkeypatch):
    """No boundary, no adapter, no client, no reservation: a shadow cycle cannot reach an exchange."""
    from app.exchange.binance.client import BinanceFuturesClient
    from app.trading_intelligence.execution.boundary import CATIExecutionBoundary

    def forbidden(*a, **k):
        raise AssertionError("a shadow cycle reached the execution path")

    monkeypatch.setattr(CATIExecutionBoundary, "__init__", forbidden)
    for name in ("place_order", "cancel_order", "place_market_order", "close_position_market"):
        monkeypatch.setattr(BinanceFuturesClient, name, forbidden, raising=False)
    for day in range(START, START + 12):
        assert s.run(day).status == D.COMPLETED
    for table in ("cati_execution_attempts", "cati_portfolio_reservations", "cati_trade_plans", "pending_entries"):
        assert s.count(table) == 0, table
    assert s.count("cati_shadow_fills") > 0           # the shadow book did trade, on paper only


def test_the_shadow_path_imports_nothing_that_can_send_an_order_and_not_the_legacy_runner():
    forbidden = ("app.runner", "app.exchange", "app.execution", "app.trading_intelligence.execution",
                 "app.trading_intelligence.integration", "app.core.trading_orchestrator")
    for name in SHADOW_PATH_MODULES:
        tree = ast.parse((PIPELINE_DIR / f"{name}.py").read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            modules = [node.module or ""] if isinstance(node, ast.ImportFrom) else \
                [a.name for a in node.names] if isinstance(node, ast.Import) else []
            for module in modules:
                assert not module.startswith(forbidden), f"pipeline/{name}.py imports {module}"


def test_the_driver_keeps_no_state_in_the_process():
    """Everything the driver knows is in the database or in its arguments: no module-level container."""
    for name in SHADOW_PATH_MODULES:
        module = importlib.import_module(f"app.trading_intelligence.pipeline.{name}")
        for key, value in vars(module).items():
            if key.startswith("__") or isinstance(value, (types.ModuleType, type)) or callable(value):
                continue
            assert not isinstance(value, (list, dict, set, bytearray)), f"pipeline/{name}.py has mutable global {key}"


# ---------------------------------------------------------------------------------------- idempotence
def test_a_replayed_cycle_produces_no_second_decision_fill_or_book_move(s):
    for day in range(START, START + 6):
        s.run(day)
    before = (s.count("cati_family_decisions"), s.count("cati_shadow_fills"), s.count("cati_shadow_days"),
              s.book(model_account_id(T.MANDATE, "balanced")))
    replay = s.run(START + 5)
    other_process = s.run(START + 5, driver=s.make(owner="another-worker"))
    assert replay.status == other_process.status == D.COMPLETED and replay.replayed and other_process.replayed
    assert s.data.calls == 6                          # a completed cycle is not recomputed
    after = (s.count("cati_family_decisions"), s.count("cati_shadow_fills"), s.count("cati_shadow_days"),
             s.book(model_account_id(T.MANDATE, "balanced")))
    assert after == before and s.count("cati_platform_cycles") == 6


def test_one_market_stage_serves_every_account(tmp_path):
    s = Setup(tmp_path, accounts=ModelAccounts(initial_equity=EQUITY))       # six accounts
    assert s.run(START).accounts_done == 6 and s.data.calls == 1
    s.run(START + 1)
    assert s.data.calls == 2
    with s.db.connect() as conn:
        snapshots = {json.loads(r[0])["data_snapshot_id"] for r in conn.execute(
            "SELECT document FROM cati_family_decisions WHERE data_cutoff=?", (DT.day_text(START + 1),))}
    assert len(snapshots) == 1


# ----------------------------------------------------------------------------------------- crash recovery
def test_an_account_that_fails_is_isolated_and_retried_without_touching_the_others(s, tmp_path, monkeypatch):
    for day in range(START, START + 4):
        s.run(day)
    reference = Setup(sub(tmp_path, "reference"))
    for day in range(START, START + 5):
        reference.run(day)
    victim, healthy = model_account_id(T.MANDATE, "aggressive"), model_account_id(T.MANDATE, "balanced")
    real = D.PlatformCycleDriver._mark
    state = {"crashed": False}

    def crash_after_the_book_was_written(self, conn, cycle_id, account_id, status, *a, **k):
        # the account's book, fills and decision are already written in this transaction: all of it must roll back
        if account_id == victim and status == D.DONE and not state["crashed"]:
            state["crashed"] = True
            raise RuntimeError("process died before the account committed")
        return real(self, conn, cycle_id, account_id, status, *a, **k)

    monkeypatch.setattr(D.PlatformCycleDriver, "_mark", crash_after_the_book_was_written)
    before = s.book(victim)
    partial = s.run(START + 4)
    assert (partial.status, partial.accounts_done, partial.accounts_failed) == (D.PARTIAL, 1, 1)
    assert partial.account_results[victim]["error"] == "RuntimeError"
    assert s.book(victim) == before                   # nothing of the failed account was kept
    assert s.book(healthy)["last_day"] == START + 4
    assert s.count("cati_family_decisions", "account_id=? AND data_cutoff=?", (victim, DT.day_text(START + 4))) == 0
    assert [k for _, k in s.driver.due(DT.close_ms(START + 4) + 120_000)] == [DT.day_text(START + 4)]
    resumed = s.run(START + 4)
    assert (resumed.status, resumed.accounts_done, resumed.accounts_failed) == (D.COMPLETED, 2, 0)
    assert resumed.account_results[healthy]["replayed"] is True
    for account in (victim, healthy):                 # exactly what an uninterrupted run produces
        a, b = s.book(account), reference.book(account)
        assert (a["equity"], a["pending_orders"], a["last_day"]) == (b["equity"], b["pending_orders"], b["last_day"])
    with s.db.connect() as conn:
        assert conn.execute("SELECT attempt FROM cati_platform_cycles WHERE cycle_key=?", (DT.day_text(START + 4),)).fetchone()[0] == 2


def test_a_cycle_owned_by_a_live_worker_is_not_run_twice_and_a_dead_one_is_taken_over(s):
    with s.db.connect() as conn:
        conn.execute("INSERT INTO cati_platform_cycles (cycle_id, family_id, cycle_key, mode, status, owner, started_at, "
                     "heartbeat_at) VALUES (?,?,?,?,?,?,?,?)",
                     (D.cycle_id_of("DAILY_TREND", DT.day_text(START), C.SHADOW), "DAILY_TREND", DT.day_text(START),
                      C.SHADOW, D.STARTED, "worker-1", 1_000, 1_000))
    alive = s.make(owner="worker-2", clock=lambda: 1_000 + D.HEARTBEAT_STALE_MS - 1)
    assert s.run(START, driver=alive).reason == "CYCLE_RUNNING_ELSEWHERE" and s.count("cati_family_decisions") == 0
    dead = s.make(owner="worker-2", clock=lambda: 1_000 + D.HEARTBEAT_STALE_MS + 1)
    assert s.run(START, driver=dead).status == D.COMPLETED and s.count("cati_family_decisions") == 2


# ------------------------------------------------------------------------------------ data and sequencing
def test_a_day_without_bars_is_waited_for_never_guessed(tmp_path):
    s = Setup(tmp_path, last="2020-09-05")
    missing = DT.day_of("2020-09-06")
    assert s.run(missing - 1).status == D.COMPLETED
    waiting = s.run(missing)
    assert (waiting.status, waiting.reason) == (D.WAITING_FOR_DATA, "DECISION_DAY_HAS_NO_BARS")
    assert s.count("cati_family_decisions", "data_cutoff=?", (DT.day_text(missing),)) == 0
    s.data = FrozenPanelBars(s.dataset._load("2020-01-01", "2020-09-10"), s.dataset.metadata)
    assert s.run(missing, driver=s.make()).status == D.COMPLETED


def test_an_incomplete_decision_day_is_not_used(s):
    import numpy as np

    panel = s.dataset._load("2020-01-01", "2020-09-10")
    day_row = int(np.searchsorted(panel.days, START))
    for field in panel.fields.values():
        field[day_row, 5:] = np.nan                    # most symbols have no bar yet
    driver = s.make(data={"1d": FrozenPanelBars(panel, s.dataset.metadata)})
    result = s.run(START, driver=driver)
    assert (result.status, result.reason) == (D.WAITING_FOR_DATA, "DECISION_DAY_INCOMPLETE")


def test_missed_days_are_made_up_in_order_and_a_book_never_jumps(s):
    assert s.driver.due(DT.close_ms(START) + 1) == [("DAILY_TREND", DT.day_text(START))]   # first time: start now
    s.run(START)
    later = DT.close_ms(START + 3) + 1
    assert [k for _, k in s.driver.due(later)] == [DT.day_text(START + n) for n in (1, 2, 3)]
    skipped = s.run(START + 2)                         # an operator runs the wrong day by hand
    assert skipped.status == D.PARTIAL
    assert {r["error"] for r in skipped.account_results.values()} == {"SHADOW_BOOK_OUT_OF_SEQUENCE"}
    results = s.driver.run_due(later)
    assert [(r.cycle_key, r.status) for r in results] == [(DT.day_text(START + n), D.COMPLETED) for n in (1, 2, 3)]
    assert s.driver.due(later) == []
    assert s.book(model_account_id(T.MANDATE, "balanced"))["last_day"] == START + 3


def test_decisions_are_causal_a_later_bar_changes_nothing(s, tmp_path):
    other = Setup(sub(tmp_path, "short"), last="2020-09-20")      # the same market, ending nine months earlier
    for day in range(START, START + 5):
        s.run(day)
        other.run(day)
    sql = "SELECT account_id, data_cutoff, decision_hash FROM cati_family_decisions ORDER BY 1, 2"
    assert s.rows(sql) == other.rows(sql) and len(s.rows(sql)) == 10


# -------------------------------------------------------------------------------------- controls and mandate
def test_kill_switches_block_entries_and_are_recorded(s):
    from app.trading_intelligence.governance.promotion import PromotionGovernance

    for day in range(START, START + 3):
        s.run(day)
    s.registry.governance.set_kill_switch("DAILY_TREND", True, operator=S.OPERATOR, reason="test")
    family = s.run(START + 3)
    assert {r["blocked"] for r in family.account_results.values()} == {D.FAMILY_KILL}
    s.registry.governance.set_kill_switch("DAILY_TREND", False, operator=S.OPERATOR, reason="test")
    PromotionGovernance(s.db).set_kill_switch(True, reason="test", actor_ref="operator:t")
    platform = s.run(START + 4)
    assert {r["blocked"] for r in platform.account_results.values()} == {D.PLATFORM_KILL}
    with s.db.connect() as conn:
        for row in conn.execute("SELECT document FROM cati_family_decisions WHERE data_cutoff IN (?,?)",
                                (DT.day_text(START + 3), DT.day_text(START + 4))):
            doc = json.loads(row[0])
            assert doc["flags"]["entries_blocked"] is True
            assert not [o for o in doc["orders"] if o["kind"] == T.ENTRY or (o["kind"] == T.ADJUST and o["quantity"] > 0)]


def test_a_family_the_registry_does_not_offer_is_not_evaluated(s):
    s.registry.governance.set_disabled("DAILY_TREND", True, operator=S.OPERATOR, reason="test")
    result = s.run(START)
    assert result.status == D.FAILED and result.reason.startswith("FAMILY_NOT_EVALUABLE") and "FAMILY_DISABLED" in result.reason
    assert s.count("cati_platform_cycles") == 0 and s.driver.due(DT.close_ms(START) + 1) == []
    assert s.run(START, driver=s.make()).status == D.FAILED
    unknown = s.driver.run_cycle("NOT_A_FAMILY", DT.day_text(START), now_ms=1)
    assert unknown.reason == "FAMILY_NOT_EVALUABLE:FAMILY_UNKNOWN"


def test_a_mandate_that_no_longer_verifies_decides_nothing(tmp_path):
    from app.trading_intelligence.research.governance.history import import_historical_hypotheses
    from app.trading_intelligence.research.governance.register import ResearchRegister

    s = Setup(tmp_path)
    bare = ResearchRegister(tmp_path / "bare.jsonl")
    import_historical_hypotheses(bare)                # history only: Mandate 004 is not registered in it
    registry = FR.FamilyRegistry(s.db, research_register=bare,
                                 definitions_source={"DAILY_TREND": FR.definitions()["DAILY_TREND"]})
    result = s.run(START, driver=s.make(registry=registry))
    assert result.status == D.FAILED and result.reason.startswith("MANDATE_VERIFICATION_FAILED")
    assert s.count("cati_family_decisions") == 0


# ------------------------------------------------------------------------------ independent scheduling
def test_daily_and_intraday_families_are_scheduled_independently(s, monkeypatch):
    calls = []

    class Hourly:
        family_id, timeframe = "TEST_HOURLY", "1h"

        def due_key(self, now_ms):
            return f"{int(now_ms) // 3_600_000 - 1:08d}"

        def key_after(self, key):
            return f"{int(key) + 1:08d}"

        def decision_time_ms(self, key):
            return (int(key) + 1) * 3_600_000

        def verify(self, register):
            return {}

        def market(self, data, key):
            calls.append(key)
            raise DT.DataNotFresh("HOURLY_BARS_MISSING")       # this family cannot run; the daily one must still run

        def account(self, *a, **k):
            raise AssertionError("no market stage, no account stage")

    module = types.ModuleType("tests_step3_hourly_adapter")
    module.PIPELINE = Hourly()
    monkeypatch.setitem(sys.modules, "tests_step3_hourly_adapter", module)
    hourly = S.definition("TEST_HOURLY", timeframe="1h", pipeline_adapter="tests_step3_hourly_adapter", mandate_id=None,
                          hypothesis_id=None)
    registry = FR.FamilyRegistry(s.db, research_register=s.research,
                                 definitions_source={"DAILY_TREND": FR.definitions()["DAILY_TREND"], "TEST_HOURLY": hourly})
    registry.register_all(actor_ref="system:test")
    driver = s.make(registry=registry)
    now = DT.close_ms(START) + 5 * 3_600_000 + 1
    due = dict(driver.due(now))
    assert due == {"DAILY_TREND": DT.day_text(START), "TEST_HOURLY": f"{now // 3_600_000 - 1:08d}"}
    results = {r.family_id: r for r in driver.run_due(now)}
    assert results["DAILY_TREND"].status == D.COMPLETED
    assert (results["TEST_HOURLY"].status, results["TEST_HOURLY"].reason) == (D.WAITING_FOR_DATA, "HOURLY_BARS_MISSING")
    assert len(calls) == 1 and [r["family_id"] for r in driver.status()] == ["DAILY_TREND", "TEST_HOURLY"]


# ------------------------------------------------------------------------------------- connected accounts
def test_connected_accounts_are_isolated_shadow_books_sized_with_the_engine_limits(tmp_path):
    s = Setup(tmp_path)
    with s.db.connect() as conn:
        for account, user, level, capital, status in (("acct-a", "user-a", "conservative", 4_000.0, "active"),
                                                      ("acct-b", "user-b", "aggressive", 25_000.0, "active"),
                                                      ("acct-none", "user-c", "balanced", None, "active"),
                                                      ("acct-stopped", "user-d", "balanced", 9_000.0, "stopped")):
            conn.execute("INSERT INTO broker_accounts (id, user_id, broker_id, market_type, label, status, environment, "
                         "created_at, updated_at) VALUES (?,?,?,?,?,?,?,?,?)",
                         (account, user, "binance", "futures", account, "connected", "demo", 1, 1))
            conn.execute("INSERT INTO bot_instances (id, user_id, broker_account_id, market_type, strategy_id, "
                         "strategy_version, symbols, timeframes, allocation_type, allocation_value, mode, status, "
                         "created_at, updated_at, risk_level, capital_allocation, capital_allocation_type) "
                         "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                         (f"bot-{account}", user, account, "futures", "cati", "1", "[]", "[]", "risk_based", 0.5, "live",
                          status, 1, 1, level, capital, "fixed_amount"))
    source = ConnectedAccounts(s.db)
    entry = s.registry.entry("DAILY_TREND")
    found = {a.account_id: a for a in source.accounts(entry)}
    assert set(found) == {"acct-a", "acct-b"}          # no stated capital, or no active bot: left out, nothing assumed
    assert all(a.policy == T.EXECUTABLE and a.environment == "DEMO" for a in found.values())
    assert (found["acct-a"].initial_equity, found["acct-b"].initial_equity) == (4_000.0, 25_000.0)
    driver = s.make(accounts=CombinedAccounts(ModelAccounts(initial_equity=EQUITY, levels=("balanced",), policies=(T.MANDATE,)), source))
    for day in range(START, START + 8):
        assert s.run(day, driver=driver).status == D.COMPLETED
    with s.db.connect() as conn:
        rows = [dict(r) for r in conn.execute("SELECT account_id, user_id, mode, environment, document FROM cati_family_decisions")]
    for row in rows:
        doc = json.loads(row["document"])
        assert row["mode"] == C.SHADOW                 # a connected demo account is still shadow: no order authority
        if row["account_id"].startswith("acct-"):
            assert row["user_id"] == {"acct-a": "user-a", "acct-b": "user-b"}[row["account_id"]]
            assert doc["policy"] == T.EXECUTABLE
            for intent in doc["intents"]:
                if intent["kind"] == C.ENTRY:           # the engine's limits: 0.40 % per trade, entry stop at most 15 %
                    assert intent["risk_fraction"] <= 0.004 * (1 + 1e-9)
                    assert 1 - intent["stop_price"] / intent["reference_price"] <= 0.15 * (1 + 1e-9)
    a, b = s.book("acct-a"), s.book("acct-b")
    assert a["risk_level"] == "conservative" and b["risk_level"] == "aggressive" and a["initial_equity"] == 4_000.0
    assert s.count("cati_execution_attempts") == 0
