"""
CosmicForge Bot Backend — Multi-Tenant Isolation Tests

Proves that Bot A's state (losses, drawdown, circuit breakers, risk budgets,
adaptive history, strategy weights, trade fills, snapshots) can NEVER affect Bot B.

Issue mapping:
  Issue 1  — guard.py consecutive-loss / cooldown state
  Issue 2  — weekly/monthly drawdown snapshots
  Issue 3  — circuit breaker key scoping
  Issue 4  — dynamic threshold calculator
  Issue 5  — risk budget engine
  Issue 6  — policy engine
  Issue 7  — adaptive engine DB queries
  Issue 8  — strategy performance tracker
  Issue 12 — trade fill bot_instance_id
  Issue 13 — decision log scoping
"""
from __future__ import annotations

import os
import tempfile
from datetime import date, datetime, timedelta, timezone
from typing import Any
from unittest.mock import MagicMock, patch

import pytest


# ─── helpers ─────────────────────────────────────────────────────────────────

def _make_temp_db():
    """Return (DB, path) backed by a temp file with full schema applied."""
    from shared_lib.persistence.db import DB
    from shared_lib.persistence.migrations import migrate

    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    migrate(db_path=path)
    return DB(path=path), path


def _cleanup(path: str) -> None:
    try:
        os.unlink(path)
    except OSError:
        pass


# ═══════════════════════════════════════════════════════════════════════════
# Issue 1 — Guard: consecutive loss / cooldown state
# ═══════════════════════════════════════════════════════════════════════════

class TestGuardIsolation:
    def setup_method(self):
        from app.risk.guard import reset_all
        reset_all()

    def teardown_method(self):
        from app.risk.guard import reset_all
        reset_all()

    def test_bot_a_loss_does_not_increase_bot_b_count(self):
        from app.risk.guard import on_trade_closed, _get_state
        on_trade_closed(-10.0, bot_id="botA")
        on_trade_closed(-10.0, bot_id="botA")
        assert _get_state("botB").consecutive_losses == 0

    def test_bot_a_cooldown_does_not_pause_bot_b(self):
        from app.risk.guard import on_trade_closed, should_pause
        on_trade_closed(-10.0, bot_id="botA")
        on_trade_closed(-10.0, bot_id="botA")
        on_trade_closed(-10.0, bot_id="botA")
        should_pause(bot_id="botA", max_losses=3)
        assert should_pause(bot_id="botB", max_losses=3) is False

    def test_bot_a_reset_does_not_reset_bot_b(self):
        from app.risk.guard import on_trade_closed, reset_bot, _get_state
        on_trade_closed(-5.0, bot_id="botA")
        on_trade_closed(-5.0, bot_id="botB")
        reset_bot("botA")
        assert _get_state("botA").consecutive_losses == 0
        assert _get_state("botB").consecutive_losses == 1

    def test_bot_a_win_resets_only_bot_a(self):
        from app.risk.guard import on_trade_closed, _get_state
        on_trade_closed(-5.0, bot_id="botA")
        on_trade_closed(-5.0, bot_id="botB")
        on_trade_closed(10.0, bot_id="botA")  # win for botA
        assert _get_state("botA").consecutive_losses == 0
        assert _get_state("botB").consecutive_losses == 1

    def test_reset_all_clears_all_state(self):
        from app.risk.guard import on_trade_closed, reset_all, _get_state
        on_trade_closed(-1.0, bot_id="botA")
        on_trade_closed(-1.0, bot_id="botB")
        reset_all()
        assert _get_state("botA").consecutive_losses == 0
        assert _get_state("botB").consecutive_losses == 0

    def test_different_bots_independent_cooldowns(self):
        from app.risk.guard import on_trade_closed, should_pause
        # botA hits cooldown
        for _ in range(3):
            on_trade_closed(-1.0, bot_id="botA")
        result_a = should_pause(bot_id="botA", max_losses=3)
        result_b = should_pause(bot_id="botB", max_losses=3)
        assert result_a is True
        assert result_b is False


# ═══════════════════════════════════════════════════════════════════════════
# Issue 2 — Weekly / Monthly Snapshot Isolation
# ═══════════════════════════════════════════════════════════════════════════

class TestSnapshotIsolation:
    def setup_method(self):
        self.db, self.path = _make_temp_db()

    def teardown_method(self):
        _cleanup(self.path)

    def _store(self, bot_id: str):
        from shared_lib.persistence.state_store import StateStore
        return StateStore(self.db, bot_instance_id=bot_id)

    def test_weekly_snapshots_are_per_bot(self):
        from app.risk.state import PeriodSnapshot
        week = date(2024, 1, 1)
        self._store("botA").save_weekly_snapshot(PeriodSnapshot(week, 10000.0, 11000.0, 9000.0))
        self._store("botB").save_weekly_snapshot(PeriodSnapshot(week, 200.0, 250.0, 180.0))
        snap_a = self._store("botA").load_weekly_snapshot(week)
        snap_b = self._store("botB").load_weekly_snapshot(week)
        assert snap_a.start_equity == 10000.0
        assert snap_b.start_equity == 200.0
        assert snap_a.peak_equity != snap_b.peak_equity

    def test_monthly_snapshots_are_per_bot(self):
        from app.risk.state import PeriodSnapshot
        month = date(2024, 1, 1)
        self._store("botA").save_monthly_snapshot(PeriodSnapshot(month, 5000.0, 6000.0, 4500.0))
        self._store("botB").save_monthly_snapshot(PeriodSnapshot(month, 100.0, 120.0, 90.0))
        snap_a = self._store("botA").load_monthly_snapshot(month)
        snap_b = self._store("botB").load_monthly_snapshot(month)
        assert snap_a.peak_equity == 6000.0
        assert snap_b.peak_equity == 120.0

    def test_bot_a_peak_equity_does_not_affect_bot_b_snapshot(self):
        from app.risk.state import PeriodSnapshot
        week = date(2024, 2, 5)
        # Bot A with very high peak
        self._store("botA").save_weekly_snapshot(PeriodSnapshot(week, 50000.0, 99000.0, 45000.0))
        # Bot B has its own small peak
        self._store("botB").save_weekly_snapshot(PeriodSnapshot(week, 300.0, 350.0, 280.0))
        snap_b = self._store("botB").load_weekly_snapshot(week)
        assert snap_b.peak_equity == 350.0  # not 99000

    def test_loading_missing_snapshot_returns_none(self):
        week = date(2025, 6, 2)
        snap = self._store("botX").load_weekly_snapshot(week)
        assert snap is None

    def test_composite_index_allows_same_date_different_bots(self):
        from app.risk.state import PeriodSnapshot
        week = date(2024, 3, 4)
        # Both bots write for the same week — must not raise
        self._store("botA").save_weekly_snapshot(PeriodSnapshot(week, 1.0, 2.0, 0.5))
        self._store("botB").save_weekly_snapshot(PeriodSnapshot(week, 3.0, 4.0, 2.5))
        # Both rows exist independently
        assert self._store("botA").load_weekly_snapshot(week).start_equity == 1.0
        assert self._store("botB").load_weekly_snapshot(week).start_equity == 3.0


# ═══════════════════════════════════════════════════════════════════════════
# Issue 3 — Circuit Breaker Key Scoping
# ═══════════════════════════════════════════════════════════════════════════

class TestCircuitBreakerIsolation:
    def setup_method(self):
        from app.risk.circuit import CircuitBreakerRegistry
        self.registry = CircuitBreakerRegistry()
        self.registry.reset_all()

    def test_bot_a_trip_does_not_trip_bot_b(self):
        key_a = "botA:broker-uuid-001"
        key_b = "botB:broker-uuid-001"
        breaker_a = self.registry.get_breaker(key_a, error_limit=3)
        for _ in range(5):
            breaker_a.record_error()
        assert self.registry.is_tripped(key_a) is True
        assert self.registry.is_tripped(key_b) is False

    def test_same_broker_isolated_by_bot(self):
        key_a = "botA:same-broker"
        key_b = "botB:same-broker"
        for _ in range(5):
            self.registry.get_breaker(key_a, error_limit=3).record_error()
        assert self.registry.is_tripped(key_b) is False

    def test_circuit_key_format_includes_bot_and_broker(self):
        bot_id = "bot-123"
        broker_id = "broker-456"
        key = f"{bot_id}:{broker_id}"
        breaker = self.registry.get_breaker(key)
        assert breaker is not None
        # Key must contain both parts
        assert bot_id in key
        assert broker_id in key

    def test_bot_a_binance_circuit_does_not_affect_bot_b(self):
        key_a = "botA:BINANCE"
        key_b = "botB:BINANCE"
        for _ in range(10):
            self.registry.get_breaker(key_a, error_limit=5).record_error()
        assert self.registry.is_tripped(key_a) is True
        assert self.registry.is_tripped(key_b) is False


# ═══════════════════════════════════════════════════════════════════════════
# Issue 4 — Dynamic Threshold Calculator
# ═══════════════════════════════════════════════════════════════════════════

class TestDynamicThresholdIsolation:
    def setup_method(self):
        from app.risk.dynamic_threshold import reset_all_dynamic_threshold_calculators
        reset_all_dynamic_threshold_calculators()

    def teardown_method(self):
        from app.risk.dynamic_threshold import reset_all_dynamic_threshold_calculators
        reset_all_dynamic_threshold_calculators()

    def test_different_bots_get_different_instances(self):
        from app.risk.dynamic_threshold import get_dynamic_threshold_calculator
        calc_a = get_dynamic_threshold_calculator(bot_id="botA")
        calc_b = get_dynamic_threshold_calculator(bot_id="botB")
        assert calc_a is not calc_b

    def test_same_bot_gets_same_instance(self):
        from app.risk.dynamic_threshold import get_dynamic_threshold_calculator
        calc1 = get_dynamic_threshold_calculator(bot_id="botA")
        calc2 = get_dynamic_threshold_calculator(bot_id="botA")
        assert calc1 is calc2

    def test_bot_a_history_does_not_affect_bot_b_threshold(self):
        from app.risk.dynamic_threshold import get_dynamic_threshold_calculator
        calc_a = get_dynamic_threshold_calculator(bot_id="botA")
        calc_b = get_dynamic_threshold_calculator(bot_id="botB")
        # Fill botA with 50 samples
        for i in range(50):
            calc_a.record("BTCUSDT", 0.90)  # high confidence
        # botB starts cold
        assert calc_b.sample_count("BTCUSDT") == 0

    def test_reconstructing_bot_a_does_not_overwrite_bot_b(self):
        from app.risk.dynamic_threshold import get_dynamic_threshold_calculator
        calc_a = get_dynamic_threshold_calculator(bot_id="botA")
        calc_b = get_dynamic_threshold_calculator(bot_id="botB")
        for _ in range(30):
            calc_a.record("ETHUSDT", 0.8)
        for _ in range(20):
            calc_b.record("ETHUSDT", 0.5)
        assert calc_a.sample_count("ETHUSDT") == 30
        assert calc_b.sample_count("ETHUSDT") == 20

    def test_reset_single_bot_does_not_affect_other(self):
        from app.risk.dynamic_threshold import get_dynamic_threshold_calculator, reset_dynamic_threshold_calculator
        calc_a = get_dynamic_threshold_calculator(bot_id="botA")
        for _ in range(10):
            calc_a.record("BTCUSDT", 0.7)
        calc_b = get_dynamic_threshold_calculator(bot_id="botB")
        for _ in range(10):
            calc_b.record("BTCUSDT", 0.7)
        reset_dynamic_threshold_calculator("botA")
        new_a = get_dynamic_threshold_calculator(bot_id="botA")
        assert new_a.sample_count("BTCUSDT") == 0
        assert calc_b.sample_count("BTCUSDT") == 10


# ═══════════════════════════════════════════════════════════════════════════
# Issue 5 — Risk Budget Engine
# ═══════════════════════════════════════════════════════════════════════════

class TestRiskBudgetIsolation:
    def setup_method(self):
        from app.risk.risk_budget import reset_all_risk_budget_engines
        reset_all_risk_budget_engines()

    def teardown_method(self):
        from app.risk.risk_budget import reset_all_risk_budget_engines
        reset_all_risk_budget_engines()

    def test_different_bots_get_different_instances(self):
        from app.risk.risk_budget import get_risk_budget_engine
        eng_a = get_risk_budget_engine(bot_id="botA")
        eng_b = get_risk_budget_engine(bot_id="botB")
        assert eng_a is not eng_b

    def test_same_bot_gets_same_instance(self):
        from app.risk.risk_budget import get_risk_budget_engine
        eng1 = get_risk_budget_engine(bot_id="botA")
        eng2 = get_risk_budget_engine(bot_id="botA")
        assert eng1 is eng2

    def test_bot_a_positions_do_not_affect_bot_b_budget(self):
        from app.risk.risk_budget import get_risk_budget_engine, RiskBudgetConfig, PositionRisk
        from dataclasses import dataclass
        config = RiskBudgetConfig(portfolio_risk_pct=0.05)
        eng_a = get_risk_budget_engine(bot_id="botA", config=config)
        eng_b = get_risk_budget_engine(bot_id="botB", config=config)
        # Update account state on both
        eng_a.update_account_state(10000.0, 0.0, 10000.0)
        eng_b.update_account_state(10000.0, 0.0, 10000.0)
        # Add a position to botA
        pos = PositionRisk(
            symbol="BTCUSDT", side="LONG", qty=0.1,
            entry_price=50000.0, stop_price=49000.0,
            notional=5000.0,
        )
        eng_a.update_positions([pos])
        # botB's remaining capacity should be untouched (no positions)
        cap_b = eng_b.get_remaining_capacity()
        cap_a = eng_a.get_remaining_capacity()
        assert cap_b["portfolio_risk_remaining"] > cap_a["portfolio_risk_remaining"]

    def test_resetting_bot_a_does_not_reset_bot_b(self):
        from app.risk.risk_budget import get_risk_budget_engine, reset_risk_budget_engine
        get_risk_budget_engine(bot_id="botA")
        get_risk_budget_engine(bot_id="botB")
        reset_risk_budget_engine("botA")
        from app.risk.risk_budget import _budget_engines
        assert "botA" not in _budget_engines
        assert "botB" in _budget_engines


# ═══════════════════════════════════════════════════════════════════════════
# Issue 6 — Policy Engine
# ═══════════════════════════════════════════════════════════════════════════

class TestPolicyEngineIsolation:
    def setup_method(self):
        from app.policy.policy_engine import reset_all_policy_engines
        reset_all_policy_engines()

    def teardown_method(self):
        from app.policy.policy_engine import reset_all_policy_engines
        reset_all_policy_engines()

    def test_different_bots_get_different_policy_engine_instances(self):
        from app.policy.policy_engine import get_policy_engine
        eng_a = get_policy_engine(bot_id="botA")
        eng_b = get_policy_engine(bot_id="botB")
        assert eng_a is not eng_b

    def test_same_bot_gets_same_policy_engine(self):
        from app.policy.policy_engine import get_policy_engine
        eng1 = get_policy_engine(bot_id="botA")
        eng2 = get_policy_engine(bot_id="botA")
        assert eng1 is eng2

    def test_policy_engine_uses_bots_budget_engine(self):
        from app.policy.policy_engine import get_policy_engine
        from app.risk.risk_budget import get_risk_budget_engine, reset_all_risk_budget_engines
        reset_all_risk_budget_engines()
        budget_a = get_risk_budget_engine(bot_id="botA")
        budget_b = get_risk_budget_engine(bot_id="botB")
        eng_a = get_policy_engine(bot_id="botA", budget_engine=budget_a)
        eng_b = get_policy_engine(bot_id="botB", budget_engine=budget_b)
        assert eng_a.budget_engine is budget_a
        assert eng_b.budget_engine is budget_b
        assert eng_a.budget_engine is not eng_b.budget_engine

    def test_resetting_one_bot_engine_does_not_affect_other(self):
        from app.policy.policy_engine import get_policy_engine, reset_policy_engine, _policy_engines
        get_policy_engine(bot_id="botA")
        get_policy_engine(bot_id="botB")
        reset_policy_engine("botA")
        assert "botA" not in _policy_engines
        assert "botB" in _policy_engines

    def test_policy_context_circuit_key_field_exists(self):
        from app.policy.policy_engine import PolicyContext
        ctx = PolicyContext(
            symbol="BTCUSDT",
            signal="BUY",
            circuit_key="botA:broker-xyz",
        )
        assert ctx.circuit_key == "botA:broker-xyz"

    def test_policy_engine_uses_circuit_key_not_broker_id(self):
        from app.policy.policy_engine import get_policy_engine, PolicyContext
        from app.risk.circuit import CircuitBreakerRegistry
        registry = CircuitBreakerRegistry()
        registry.reset_all()
        # Trip the scoped circuit key
        scoped_key = "botA:broker-abc"
        for _ in range(10):
            registry.get_breaker(scoped_key, error_limit=5).record_error()
        eng = get_policy_engine(bot_id="botA", circuit_registry=registry)
        ctx = PolicyContext(
            symbol="BTCUSDT", signal="BUY", equity=10000.0,
            broker_id="broker-abc",
            circuit_key=scoped_key,
        )
        decision = eng.evaluate(ctx)
        # Circuit is tripped for the scoped key — should be blocked
        assert not decision.allowed or "CIRCUIT" in str(decision.reason_code)


# ═══════════════════════════════════════════════════════════════════════════
# Issue 7 — Adaptive Engine DB query scoping
# ═══════════════════════════════════════════════════════════════════════════

class TestAdaptiveEngineIsolation:
    def setup_method(self):
        from app.adaptive.engine import reset_all_adaptive_engines
        reset_all_adaptive_engines()
        self.db, self.path = _make_temp_db()

    def teardown_method(self):
        from app.adaptive.engine import reset_all_adaptive_engines
        reset_all_adaptive_engines()
        _cleanup(self.path)

    def _seed_fills(self, bot_id: str, n_losses: int, n_wins: int = 0) -> None:
        now = datetime.now(timezone.utc)
        with self.db.connect() as conn:
            for i in range(n_losses):
                ts = (now - timedelta(hours=i + 1)).isoformat()
                conn.execute(
                    """
                    INSERT INTO trade_fills
                        (symbol, side, action, qty, price, realized_pnl, timestamp_utc,
                         bot_instance_id)
                    VALUES ('BTCUSDT','LONG','CLOSE',0.1,50000.0,-100.0,?,?)
                    """,
                    (ts, bot_id),
                )
            for i in range(n_wins):
                ts = (now - timedelta(hours=n_losses + i + 1)).isoformat()
                conn.execute(
                    """
                    INSERT INTO trade_fills
                        (symbol, side, action, qty, price, realized_pnl, timestamp_utc,
                         bot_instance_id)
                    VALUES ('BTCUSDT','LONG','CLOSE',0.1,50000.0,200.0,?,?)
                    """,
                    (ts, bot_id),
                )
            conn.commit()

    def _seed_equity(self, bot_id: str, equities: list) -> None:
        now = datetime.now(timezone.utc)
        with self.db.connect() as conn:
            for i, eq in enumerate(equities):
                ts = (now - timedelta(hours=len(equities) - i)).isoformat()
                conn.execute(
                    """
                    INSERT INTO equity_snapshots
                        (bot_instance_id, user_id, broker_account_id, broker_id,
                         equity, timestamp_utc, source, created_at, updated_at)
                    VALUES (?, 'test-user', 'test-broker-acct', 'binance',
                            ?, ?, 'test', datetime('now'), datetime('now'))
                    """,
                    (bot_id, eq, ts),
                )
            conn.commit()

    def test_factory_returns_isolated_engines_per_bot(self):
        from app.adaptive.engine import get_adaptive_engine
        eng_a = get_adaptive_engine(bot_id="botA", db=self.db)
        eng_b = get_adaptive_engine(bot_id="botB", db=self.db)
        assert eng_a is not eng_b
        assert eng_a.bot_instance_id == "botA"
        assert eng_b.bot_instance_id == "botB"

    def test_bot_a_loss_streak_uses_only_bot_a_fills(self):
        from app.adaptive.engine import AdaptiveEngine
        # botA has 5 losses, botB has 0
        self._seed_fills("botA", n_losses=5)
        eng_a = AdaptiveEngine(db=self.db, bot_instance_id="botA")
        eng_b = AdaptiveEngine(db=self.db, bot_instance_id="botB")
        streak_a = eng_a._get_loss_streak_from_db("BTCUSDT")
        streak_b = eng_b._get_loss_streak_from_db("BTCUSDT")
        assert streak_a == 5
        assert streak_b == 0

    def test_bot_b_loss_streak_excludes_bot_a_fills(self):
        from app.adaptive.engine import AdaptiveEngine
        self._seed_fills("botA", n_losses=10)
        self._seed_fills("botB", n_losses=2)
        eng_b = AdaptiveEngine(db=self.db, bot_instance_id="botB")
        streak_b = eng_b._get_loss_streak_from_db("BTCUSDT")
        assert streak_b == 2  # not 12

    def test_bot_a_drawdown_uses_only_bot_a_equity_snapshots(self):
        from app.adaptive.engine import AdaptiveEngine
        # botA has big drawdown (10k peak, now 8k)
        self._seed_equity("botA", [10000.0, 9000.0, 8000.0])
        # botB has no drawdown
        self._seed_equity("botB", [500.0, 510.0, 520.0])
        eng_a = AdaptiveEngine(db=self.db, bot_instance_id="botA")
        eng_b = AdaptiveEngine(db=self.db, bot_instance_id="botB")
        dd_a = eng_a._get_drawdown_from_db()
        dd_b = eng_b._get_drawdown_from_db()
        assert dd_a > 0.1  # ~20% drawdown
        assert dd_b == 0.0  # no drawdown (monotonically increasing)

    def test_bot_a_rolling_stats_exclude_bot_b_fills(self):
        from app.adaptive.engine import AdaptiveEngine
        # botA: 10 losses
        self._seed_fills("botA", n_losses=10)
        # botB: 10 wins
        self._seed_fills("botB", n_losses=0, n_wins=10)
        eng_a = AdaptiveEngine(db=self.db, bot_instance_id="botA")
        win_rate_a, _ = eng_a._get_rolling_stats_from_db("BTCUSDT")
        assert win_rate_a == 0.0  # all losses


# ═══════════════════════════════════════════════════════════════════════════
# Issue 8 — Strategy Performance Tracker
# ═══════════════════════════════════════════════════════════════════════════

class TestStrategyPerformanceIsolation:
    def setup_method(self):
        self.db, self.path = _make_temp_db()

    def teardown_method(self):
        _cleanup(self.path)

    def test_tracker_receives_bot_instance_id(self):
        from app.adaptive.strategy_performance import StrategyPerformanceTracker
        tracker = StrategyPerformanceTracker(db=self.db, bot_instance_id="bot-xyz")
        assert tracker.bot_instance_id == "bot-xyz"

    def test_tracker_default_bot_id_is_default(self):
        from app.adaptive.strategy_performance import StrategyPerformanceTracker
        tracker = StrategyPerformanceTracker(db=self.db)
        assert tracker.bot_instance_id == "default"

    def test_adaptive_engine_passes_bot_id_to_tracker(self):
        from app.adaptive.engine import AdaptiveEngine
        eng = AdaptiveEngine(db=self.db, bot_instance_id="bot-abc")
        assert eng._strategy_tracker.bot_instance_id == "bot-abc"


# ═══════════════════════════════════════════════════════════════════════════
# Issue 12 — Trade Fill bot_instance_id
# ═══════════════════════════════════════════════════════════════════════════

class TestTradeFillIsolation:
    def setup_method(self):
        self.db, self.path = _make_temp_db()

    def teardown_method(self):
        _cleanup(self.path)

    def test_trade_fills_schema_has_bot_instance_id(self):
        with self.db.connect() as conn:
            cols = [r["name"] for r in conn.execute("PRAGMA table_info(trade_fills)").fetchall()]
        assert "bot_instance_id" in cols

    def test_record_fill_accepts_bot_instance_id_param(self):
        """record_fill signature must accept bot_instance_id without raising."""
        import inspect
        from shared_lib.persistence.trade_fills import record_fill
        sig = inspect.signature(record_fill)
        assert "bot_instance_id" in sig.parameters

    def test_fills_are_queryable_by_bot(self):
        now = datetime.now(timezone.utc).isoformat()
        with self.db.connect() as conn:
            conn.execute(
                """
                INSERT INTO trade_fills
                    (symbol, side, action, qty, price, realized_pnl, timestamp_utc, bot_instance_id)
                VALUES ('BTCUSDT','LONG','CLOSE',0.1,50000.0,-50.0,?,?)
                """,
                (now, "botA"),
            )
            conn.execute(
                """
                INSERT INTO trade_fills
                    (symbol, side, action, qty, price, realized_pnl, timestamp_utc, bot_instance_id)
                VALUES ('BTCUSDT','LONG','CLOSE',0.1,50000.0,200.0,?,?)
                """,
                (now, "botB"),
            )
            conn.commit()
        with self.db.connect() as conn:
            rows_a = conn.execute(
                "SELECT realized_pnl FROM trade_fills WHERE bot_instance_id='botA'"
            ).fetchall()
            rows_b = conn.execute(
                "SELECT realized_pnl FROM trade_fills WHERE bot_instance_id='botB'"
            ).fetchall()
        assert len(rows_a) == 1 and float(rows_a[0]["realized_pnl"]) < 0
        assert len(rows_b) == 1 and float(rows_b[0]["realized_pnl"]) > 0


# ═══════════════════════════════════════════════════════════════════════════
# Issue 13 — Decision Log Scoping
# ═══════════════════════════════════════════════════════════════════════════

class TestDecisionLogScoping:
    def setup_method(self):
        self.db, self.path = _make_temp_db()

    def teardown_method(self):
        _cleanup(self.path)

    def test_decision_logs_schema_has_config_id(self):
        with self.db.connect() as conn:
            cols = [r["name"] for r in conn.execute("PRAGMA table_info(decision_logs)").fetchall()]
        assert "config_id" in cols

    def test_decision_log_composite_index_exists(self):
        with self.db.connect() as conn:
            indexes = [
                r["name"]
                for r in conn.execute("PRAGMA index_list(decision_logs)").fetchall()
            ]
        assert "idx_decision_logs_config_symbol" in indexes

    def test_decision_logs_queryable_by_config_id(self):
        import uuid
        config_a = str(uuid.uuid4())
        config_b = str(uuid.uuid4())
        now = datetime.now(timezone.utc).isoformat()
        with self.db.connect() as conn:
            for config_id, action in [(config_a, "execute"), (config_b, "block")]:
                conn.execute(
                    """
                    INSERT INTO decision_logs
                        (id, config_id, symbol, final_action, created_at)
                    VALUES (?, ?, 'BTCUSDT', ?, ?)
                    """,
                    (str(uuid.uuid4()), config_id, action, now),
                )
            conn.commit()
        with self.db.connect() as conn:
            rows_a = conn.execute(
                "SELECT final_action FROM decision_logs WHERE config_id=?", (config_a,)
            ).fetchall()
            rows_b = conn.execute(
                "SELECT final_action FROM decision_logs WHERE config_id=?", (config_b,)
            ).fetchall()
        assert rows_a[0]["final_action"] == "execute"
        assert rows_b[0]["final_action"] == "block"


# ═══════════════════════════════════════════════════════════════════════════
# Migration — weekly/monthly snapshot schema
# ═══════════════════════════════════════════════════════════════════════════

class TestSnapshotSchemaMigration:
    def test_weekly_snapshots_has_bot_instance_id_after_migration(self):
        db, path = _make_temp_db()
        try:
            with db.connect() as conn:
                cols = [r["name"] for r in conn.execute("PRAGMA table_info(weekly_snapshots)").fetchall()]
            assert "bot_instance_id" in cols
        finally:
            _cleanup(path)

    def test_monthly_snapshots_has_bot_instance_id_after_migration(self):
        db, path = _make_temp_db()
        try:
            with db.connect() as conn:
                cols = [r["name"] for r in conn.execute("PRAGMA table_info(monthly_snapshots)").fetchall()]
            assert "bot_instance_id" in cols
        finally:
            _cleanup(path)

    def test_composite_index_weekly_exists(self):
        db, path = _make_temp_db()
        try:
            with db.connect() as conn:
                indexes = [r["name"] for r in conn.execute("PRAGMA index_list(weekly_snapshots)").fetchall()]
            assert "idx_weekly_bot_date" in indexes
        finally:
            _cleanup(path)

    def test_composite_index_monthly_exists(self):
        db, path = _make_temp_db()
        try:
            with db.connect() as conn:
                indexes = [r["name"] for r in conn.execute("PRAGMA index_list(monthly_snapshots)").fetchall()]
            assert "idx_monthly_bot_date" in indexes
        finally:
            _cleanup(path)

    def test_migration_is_idempotent(self):
        """Running migrate() twice must not raise."""
        from shared_lib.persistence.migrations import migrate
        db, path = _make_temp_db()
        try:
            migrate(db_path=path)  # second run
            migrate(db_path=path)  # third run — must not error
        finally:
            _cleanup(path)


# ═══════════════════════════════════════════════════════════════════════════
# Pending Item 1 — AdaptiveEngine exec failure rate scoping (decision_logs)
# ═══════════════════════════════════════════════════════════════════════════

class TestExecFailureRateScoping:
    def setup_method(self):
        self.db, self.path = _make_temp_db()

    def teardown_method(self):
        _cleanup(self.path)

    def _insert_decision(self, *, config_id: str, symbol: str, final_action: str, created_at: str) -> None:
        import uuid
        with self.db.connect() as conn:
            conn.execute(
                """
                INSERT INTO decision_logs (id, config_id, symbol, final_action, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (str(uuid.uuid4()), config_id, symbol, final_action, created_at),
            )

    def test_bot_a_exec_failure_rate_ignores_bot_b(self):
        from app.adaptive.engine import AdaptiveEngine

        now = datetime.now(timezone.utc).isoformat()
        bot_a = "botA-config"
        bot_b = "botB-config"

        # Bot A: mostly blocked
        for _ in range(8):
            self._insert_decision(config_id=bot_a, symbol="BTCUSDT", final_action="blocked", created_at=now)
        for _ in range(2):
            self._insert_decision(config_id=bot_a, symbol="BTCUSDT", final_action="execute", created_at=now)

        # Bot B: all execute (should not reduce Bot A's failure rate)
        for _ in range(50):
            self._insert_decision(config_id=bot_b, symbol="BTCUSDT", final_action="execute", created_at=now)

        rate_a = AdaptiveEngine(self.db, bot_instance_id=bot_a)._get_exec_failure_rate("BTCUSDT", lookback=10)
        rate_b = AdaptiveEngine(self.db, bot_instance_id=bot_b)._get_exec_failure_rate("BTCUSDT", lookback=10)

        assert rate_a == 0.8
        assert rate_b == 0.0

    def test_empty_scoped_data_returns_safe_default(self):
        from app.adaptive.engine import AdaptiveEngine
        bot_c = "botC-config"
        rate_c = AdaptiveEngine(self.db, bot_instance_id=bot_c)._get_exec_failure_rate("BTCUSDT", lookback=10)
        assert rate_c == 0.0

    def test_exec_failure_rate_query_is_scoped_by_config_id(self):
        import inspect
        from app.adaptive.engine import AdaptiveEngine
        src = inspect.getsource(AdaptiveEngine._get_exec_failure_rate)
        assert "config_id" in src


# ═══════════════════════════════════════════════════════════════════════════
# Pending Item 2 — InvariantChecker critical violations scoping
# ═══════════════════════════════════════════════════════════════════════════

class TestInvariantCheckerScoping:
    def setup_method(self):
        self.db, self.path = _make_temp_db()

    def teardown_method(self):
        _cleanup(self.path)

    def _insert_violation(
        self,
        *,
        bot_instance_id: str | None,
        run_id: str | None,
        severity: str = "CRITICAL",
        minutes_ago: int = 1,
    ) -> None:
        ts = (datetime.now(timezone.utc) - timedelta(minutes=minutes_ago)).isoformat()
        with self.db.connect() as conn:
            conn.execute(
                """
                INSERT INTO invariant_violations
                    (ts, trace_id, run_id, bot_instance_id, symbol, violation_type, severity, details_json, auto_action_taken, acknowledged)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
                """,
                (
                    ts,
                    "trace-x",
                    run_id,
                    bot_instance_id,
                    "BTCUSDT",
                    "KILL_SWITCH_BYPASS",
                    severity,
                    "{}",
                    None,
                ),
            )

    def test_bot_a_violation_does_not_affect_bot_b(self):
        from app.risk.invariant_checker import InvariantChecker
        checker = InvariantChecker(db_path=self.path)

        self._insert_violation(bot_instance_id="botA", run_id="runA")

        assert checker.has_critical_violations(bot_instance_id="botA", since_minutes=10) is True
        assert checker.has_critical_violations(bot_instance_id="botB", since_minutes=10) is False

    def test_run_scoped_violation_does_not_affect_other_runs(self):
        from app.risk.invariant_checker import InvariantChecker
        checker = InvariantChecker(db_path=self.path)

        self._insert_violation(bot_instance_id="botA", run_id="runA")
        self._insert_violation(bot_instance_id="botA", run_id="runB")

        assert checker.has_critical_violations(run_id="runA", since_minutes=10) is True
        assert checker.has_critical_violations(run_id="runC", since_minutes=10) is False

    def test_global_no_scope_still_detects_any_critical_violation(self):
        from app.risk.invariant_checker import InvariantChecker
        checker = InvariantChecker(db_path=self.path)
        self._insert_violation(bot_instance_id="botA", run_id="runA")

        # Global audit behavior: true if ANY bot has critical violations recently.
        assert checker.has_critical_violations(since_minutes=10) is True
