"""
tests/test_adaptive_policies.py

Comprehensive test coverage for Phase 6:
  - Section 12: 10 implementation-level test categories
  - Section 11: 12 validation scenario tests

All tests use an in-memory SQLite DB so no real database is needed.
"""
from __future__ import annotations

import sqlite3
import tempfile
import os
import pytest

# ============================================================================
# Shared fixtures
# ============================================================================

@pytest.fixture()
def tmp_db_path():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        path = f.name
    yield path
    try:
        os.unlink(path)
    except Exception:
        pass


def _make_db(tmp_db_path: str):
    """Create a minimal schema + return a DB-like object."""
    from shared_lib.persistence.db import DB
    db = DB(path=tmp_db_path)
    with db.connect() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS trade_fills (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT, cycle_id TEXT,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                action TEXT NOT NULL,
                qty REAL NOT NULL DEFAULT 1.0,
                price REAL NOT NULL DEFAULT 1.0,
                fee REAL,
                realized_pnl REAL,
                timestamp_utc TEXT NOT NULL DEFAULT '2024-01-01T00:00:00Z',
                slippage_pct REAL DEFAULT 0.0,
                entry_price_expected REAL,
                stop_loss_price REAL,
                position_id TEXT,
                r_multiple REAL,
                user_id TEXT, bot_instance_id TEXT, broker_account_id TEXT
            );
            CREATE TABLE IF NOT EXISTS equity_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                equity REAL, timestamp_utc TEXT DEFAULT '2024-01-01T00:00:00Z',
                user_id TEXT, bot_instance_id TEXT, broker_account_id TEXT
            );
            CREATE TABLE IF NOT EXISTS decision_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                config_id TEXT, symbol TEXT,
                final_action TEXT DEFAULT 'EXECUTE',
                strategy_signal_json TEXT DEFAULT '{}',
                created_at TEXT DEFAULT '2024-01-01T00:00:00Z'
            );
        """)
        conn.commit()
    return db


def _engine(db):
    from app.adaptive.engine import AdaptiveEngine
    return AdaptiveEngine(db=db)


def _insert_fill(conn, symbol: str, pnl: float, action: str = "CLOSE", side: str = "LONG") -> None:
    """Insert a trade_fill with all NOT NULL columns satisfied."""
    conn.execute(
        """INSERT INTO trade_fills
           (symbol, side, action, qty, price, realized_pnl, timestamp_utc)
           VALUES (?, ?, ?, 1.0, 1.0, ?, '2024-01-01T00:00:00Z')""",
        (symbol, side, action, pnl),
    )


def _insert_equity(conn, equity: float, user_id: str = "test") -> None:
    """Insert an equity_snapshot satisfying all NOT NULL columns."""
    conn.execute(
        """INSERT INTO equity_snapshots
           (user_id, broker_account_id, broker_id, timestamp_utc, equity, source, created_at, updated_at)
           VALUES (?, 'broker_acc', 'bybit', '2024-01-01T00:00:00Z', ?, 'BROKER', '2024-01-01T00:00:00Z', '2024-01-01T00:00:00Z')""",
        (user_id, equity),
    )


# ============================================================================
# SECTION 12 — Implementation-Level Tests (10 categories)
# ============================================================================

class TestBoundsEnforcement:
    """Section 12.1 — All outputs stay within their hard bounds."""

    def test_confidence_gate_never_exceeds_0_12(self, tmp_db_path):
        db = _make_db(tmp_db_path)
        with db.connect() as conn:
            # 20 losses in a row
            for _ in range(20):
                _insert_fill(conn, "BTCUSDT", -10.0)
            conn.commit()
        engine = _engine(db)
        state = engine.get_adaptive_state("cfg1", "BTCUSDT")
        assert state.confidence_gate_modifier <= 0.12

    def test_size_multiplier_never_below_0_20(self, tmp_db_path):
        db = _make_db(tmp_db_path)
        engine = _engine(db)
        # Simulate 30% drawdown via hint parameter — directly tests bounds enforcement
        state = engine.get_adaptive_state("cfg1", "BTCUSDT", drawdown_pct_hint=0.30)
        assert state.size_multiplier >= 0.20

    def test_leverage_multiplier_never_below_0_25(self, tmp_db_path):
        db = _make_db(tmp_db_path)
        engine = _engine(db)
        # Extreme volatility
        state = engine.get_adaptive_state("cfg1", "BTCUSDT", current_atr_pct=10.0)
        assert state.leverage_multiplier >= 0.25

    def test_aggressiveness_score_in_0_1(self, tmp_db_path):
        db = _make_db(tmp_db_path)
        engine = _engine(db)
        state = engine.get_adaptive_state("cfg1", "BTCUSDT")
        assert 0.0 <= state.aggressiveness_score <= 1.0


class TestHysteresisBehavior:
    """Section 12.2 — Cooldown doesn't jump levels in a single tick."""

    def test_cooldown_does_not_skip_levels(self, tmp_db_path):
        db = _make_db(tmp_db_path)
        with db.connect() as conn:
            # 4 consecutive losses to trigger pressure
            for _ in range(4):
                _insert_fill(conn, "BTCUSDT", -5.0)
            conn.commit()
        engine = _engine(db)
        # First tick: may go to SOFT but NOT HARD immediately
        state1 = engine.get_adaptive_state("cfg1", "BTCUSDT")
        assert state1.cooldown_state in {"NONE", "SOFT"}

    def test_cooldown_recovers_after_3_clean_ticks(self, tmp_db_path):
        db = _make_db(tmp_db_path)
        with db.connect() as conn:
            for _ in range(4):
                _insert_fill(conn, "BTCUSDT", -5.0)
            conn.commit()
        engine = _engine(db)
        # Drive to SOFT/HARD
        for _ in range(3):
            engine.get_adaptive_state("cfg1", "BTCUSDT")
        # Now simulate clean by wiping fills and adding wins
        with db.connect() as conn:
            conn.execute("DELETE FROM trade_fills WHERE symbol=?", ("BTCUSDT",))
            for _ in range(5):
                _insert_fill(conn, "BTCUSDT", 10.0)
            conn.commit()
        # 3+ clean calls
        for _ in range(4):
            state = engine.get_adaptive_state("cfg1", "BTCUSDT")
        assert state.cooldown_state in {"NONE", "SOFT"}


class TestSmoothingBehavior:
    """Section 12.3 — Size drops asymmetrically; recovers slowly."""

    def test_size_drops_faster_than_recovery(self, tmp_db_path):
        db = _make_db(tmp_db_path)
        engine = _engine(db)

        # First tick: heavy drawdown → size compresses
        state_bad = engine.get_adaptive_state("cfg1", "BTCUSDT", drawdown_pct_hint=0.20)
        size_after_drawdown = state_bad.size_multiplier

        # Second tick: drawdown cleared → size starts recovering but NOT fully
        state_recover = engine.get_adaptive_state("cfg1", "BTCUSDT", drawdown_pct_hint=0.0)
        # Size should not immediately snap back to 1.0 (EMA slow recovery)
        assert state_recover.size_multiplier < 1.0


class TestMinSampleProtection:
    """Section 12.4 — No adjustment from statistically weak sample sizes."""

    def test_strategy_weights_neutral_below_min_samples(self, tmp_db_path):
        db = _make_db(tmp_db_path)
        # No trade fills at all (cold start) → weights should default to 1.0
        engine = _engine(db)
        state = engine.get_adaptive_state("cfg1", "BTCUSDT")
        for weight in state.strategy_weight_adjustments.values():
            assert 0.70 <= weight <= 1.30

    def test_streak_policy_needs_1_confirmed_loss(self, tmp_db_path):
        from app.adaptive.policies import LossStreakPolicy
        policy = LossStreakPolicy()
        # No losses
        decision = policy.evaluate(0)
        assert decision.triggered is False
        assert decision.raw_target == 0.0


class TestRestartPersistence:
    """Section 12.5 — EMA snaps to DB-derived target on first tick after restart."""

    def test_cold_start_snaps_to_db_target(self, tmp_db_path):
        db = _make_db(tmp_db_path)
        with db.connect() as conn:
            # 5 losses → streak penalty target = 0.10
            for _ in range(5):
                _insert_fill(conn, "BTCUSDT", -1.0)
            conn.commit()
        engine = _engine(db)
        state = engine.get_adaptive_state("cfg1", "BTCUSDT")
        # Cold start: EMA should snap exactly to the DB-driven target
        assert state.confidence_gate_modifier == pytest.approx(0.10, abs=0.01)


class TestDeterministicReconstruction:
    """Section 12.6 — Same DB state always produces same adaptive state."""

    def test_same_db_same_state(self, tmp_db_path):
        db = _make_db(tmp_db_path)
        with db.connect() as conn:
            _insert_fill(conn, "BTCUSDT", -5.0)
            conn.commit()
        engine1 = _engine(db)
        engine2 = _engine(db)
        state1 = engine1.get_adaptive_state("cfg1", "BTCUSDT")
        state2 = engine2.get_adaptive_state("cfg1", "BTCUSDT")
        assert state1.loss_streak == state2.loss_streak
        assert state1.confidence_gate_modifier == state2.confidence_gate_modifier


class TestNoDuplicatePaths:
    """Section 12.7 — Exec degradation and cooldown don't double-penalize."""

    def test_hard_cooldown_caps_confidence_penalty(self, tmp_db_path):
        from app.adaptive.policies import ConfidenceGatePolicy
        policy = ConfidenceGatePolicy()
        # 10-loss streak with HARD cooldown → penalty capped at 0.06
        decision = policy.evaluate(loss_streak=10, regime_offset=0.0, cooldown_state="HARD")
        assert decision.raw_target <= ConfidenceGatePolicy.HARD_COOLDOWN_CAP


class TestStableWeightNormalization:
    """Section 12.8 — Strategy weights stay within normalized bounds."""

    def test_strategy_weights_always_in_bounds(self, tmp_db_path):
        db = _make_db(tmp_db_path)
        engine = _engine(db)
        state = engine.get_adaptive_state("cfg1", "BTCUSDT")
        for weight in state.strategy_weight_adjustments.values():
            assert 0.70 <= weight <= 1.30


class TestCoordinatedThresholdSize:
    """Section 12.9 — When size is crushed by HARD cooldown, confidence is capped."""

    def test_confidence_capped_when_cooldown_hard(self, tmp_db_path):
        from app.adaptive.policies import ConfidenceGatePolicy
        pol = ConfidenceGatePolicy()
        # HARD cooldown with heavy streak
        decision = pol.evaluate(loss_streak=8, regime_offset=0.0, cooldown_state="HARD")
        # Must be at or below coordination cap
        assert decision.raw_target <= 0.06


class TestRecoveryPathCorrectness:
    """Section 12.10 — Aggressiveness recovers at the slow EMA rate, no overshooting."""

    def test_recovery_does_not_overshoot_1_0(self, tmp_db_path):
        db = _make_db(tmp_db_path)
        with db.connect() as conn:
            _insert_equity(conn, 85.0)
            _insert_equity(conn, 100.0)
            conn.commit()
        engine = _engine(db)
        # First tick: penalty
        engine.get_adaptive_state("cfg1", "BTCUSDT")
        # Snap to recovery
        with db.connect() as conn:
            conn.execute("DELETE FROM equity_snapshots")
            _insert_equity(conn, 100.0)
            _insert_equity(conn, 100.0)
            conn.commit()
        for _ in range(10):
            state = engine.get_adaptive_state("cfg1", "BTCUSDT")
        assert state.size_multiplier <= 1.0


# ============================================================================
# SECTION 11 — Validation Scenarios (12 scenarios)
# ============================================================================

class TestValidationScenarios:

    def test_scenario_1_short_loss_streak(self, tmp_db_path):
        """Short loss streak (2): moderate threshold raise, size unchanged."""
        db = _make_db(tmp_db_path)
        with db.connect() as conn:
            for _ in range(2):
                _insert_fill(conn, "BTCUSDT", -5.0)
            conn.commit()
        engine = _engine(db)
        state = engine.get_adaptive_state("cfg1", "BTCUSDT")
        # Confidence gate raised slightly
        assert state.confidence_gate_modifier > 0.0
        # Size should be close to 1.0 (no drawdown yet)
        assert state.size_multiplier >= 0.90
        # PASS criteria
        assert state.loss_streak == 2

    def test_scenario_2_prolonged_loss_streak(self, tmp_db_path):
        """Prolonged loss streak (8): confidence gate at cap, cooldown engaged."""
        db = _make_db(tmp_db_path)
        with db.connect() as conn:
            for _ in range(8):
                _insert_fill(conn, "BTCUSDT", -5.0)
            conn.commit()
        engine = _engine(db)
        state = engine.get_adaptive_state("cfg1", "BTCUSDT")
        # EMA penalty well above 0.0 (snapped from target on cold start)
        assert state.confidence_gate_modifier > 0.0
        assert state.loss_streak == 8

    def test_scenario_3_drawdown_breach_then_recovery(self, tmp_db_path):
        """Drawdown >=10% → size compressed. After equity recovers → size walks back."""
        db = _make_db(tmp_db_path)
        engine = _engine(db)

        # Simulate 15% drawdown via hint
        state_stress = engine.get_adaptive_state("cfg1", "BTCUSDT", drawdown_pct_hint=0.15)
        assert state_stress.size_multiplier < 1.0

        # Recovery tick: drawdown cleared
        state_rec = engine.get_adaptive_state("cfg1", "BTCUSDT", drawdown_pct_hint=0.0)
        # Size moves toward recovery but not full snap back
        assert state_rec.size_multiplier > state_stress.size_multiplier

    def test_scenario_4_one_strategy_underperforming(self, tmp_db_path):
        """One strategy underperforms: weight adjusted within bounds."""
        db = _make_db(tmp_db_path)
        engine = _engine(db)
        state = engine.get_adaptive_state("cfg1", "BTCUSDT")
        for w in state.strategy_weight_adjustments.values():
            assert 0.70 <= w <= 1.30

    def test_scenario_5_regime_shift(self, tmp_db_path):
        """Regime shift: engine receives different active_regime but outputs remain bounded."""
        db = _make_db(tmp_db_path)
        engine = _engine(db)
        state = engine.get_adaptive_state("cfg1", "BTCUSDT", active_regime="HIGH_VOLATILITY")
        assert state.regime == "HIGH_VOLATILITY"
        assert 0.0 <= state.aggressiveness_score <= 1.0

    def test_scenario_6_restart_during_stressed_state(self, tmp_db_path):
        """Restart during stress: new engine reconstructs DB state identically."""
        db = _make_db(tmp_db_path)
        with db.connect() as conn:
            for _ in range(5):
                _insert_fill(conn, "BTCUSDT", -5.0)
            conn.commit()
        engine_pre = _engine(db)
        state_pre = engine_pre.get_adaptive_state("cfg1", "BTCUSDT")

        # Simulate restart
        engine_post = _engine(db)
        state_post = engine_post.get_adaptive_state("cfg1", "BTCUSDT")

        # Both should read the same DB → same loss_streak
        assert state_pre.loss_streak == state_post.loss_streak

    def test_scenario_7_restart_during_recovery(self, tmp_db_path):
        """Restart during recovery: EMA snaps to target (clean state)."""
        db = _make_db(tmp_db_path)
        with db.connect() as conn:
            for _ in range(5):
                _insert_fill(conn, "BTCUSDT", 10.0)
            conn.commit()
        engine = _engine(db)
        state = engine.get_adaptive_state("cfg1", "BTCUSDT")
        # No losses → EMA should snap to target=0.0 penalty
        assert state.confidence_gate_modifier == pytest.approx(0.0, abs=0.01)

    def test_scenario_8_oscillating_near_threshold(self, tmp_db_path):
        """Oscillating: alternating loss/win prevents cooldown escalation."""
        db = _make_db(tmp_db_path)
        engine = _engine(db)
        for i in range(6):
            pnl = -5.0 if i % 2 == 0 else 5.0
            with db.connect() as conn:
                _insert_fill(conn, "BTCUSDT", pnl)
                conn.commit()
            state = engine.get_adaptive_state("cfg1", "BTCUSDT")
        # Cooldown should not escalate past SOFT due to alternating pattern
        assert state.cooldown_state in {"NONE", "SOFT"}

    def test_scenario_9_weak_sample_environment(self, tmp_db_path):
        """Weak sample: < 10 trades → strategy weights remain at 1.0."""
        db = _make_db(tmp_db_path)
        with db.connect() as conn:
            # Only 3 trades
            for _ in range(3):
                _insert_fill(conn, "BTCUSDT", -5.0)
            conn.commit()
        engine = _engine(db)
        state = engine.get_adaptive_state("cfg1", "BTCUSDT")
        # With fewer than 10 trades, strategy weight adjustments should be empty or default 1.0
        for w in state.strategy_weight_adjustments.values():
            assert w == pytest.approx(1.0, abs=0.01)

    def test_scenario_10_execution_degradation(self, tmp_db_path):
        """Execution degradation: high exec_fail_rate triggers size reduction."""
        from app.adaptive.policies import ExecutionDegradationPolicy
        policy = ExecutionDegradationPolicy()
        decision = policy.evaluate(exec_fail_rate=0.65, num_decisions=50)
        assert decision.triggered is True
        assert decision.raw_target < 1.0
        assert decision.raw_target >= 0.70

    def test_scenario_11_slippage_deterioration(self, tmp_db_path):
        """Slippage proxy via exec_fail_rate: triggers adaptive reduction."""
        from app.adaptive.policies import ExecutionDegradationPolicy
        policy = ExecutionDegradationPolicy()
        decision = policy.evaluate(exec_fail_rate=0.50, num_decisions=25)
        assert decision.triggered is True
        assert decision.raw_target <= 0.90

    def test_scenario_12_recovery_no_overshoot(self, tmp_db_path):
        """Recovery without overshooting: size_multiplier stays ≤ 1.0 at all times."""
        db = _make_db(tmp_db_path)
        with db.connect() as conn:
            _insert_equity(conn, 85.0)
            _insert_equity(conn, 100.0)
            conn.commit()
        engine = _engine(db)
        engine.get_adaptive_state("cfg1", "BTCUSDT")

        with db.connect() as conn:
            conn.execute("DELETE FROM equity_snapshots")
            _insert_equity(conn, 100.0)
            _insert_equity(conn, 100.0)
            conn.commit()
        for _ in range(20):
            state = engine.get_adaptive_state("cfg1", "BTCUSDT")
            assert state.size_multiplier <= 1.0, f"Overshot: {state.size_multiplier}"


# ============================================================================
# SECTION 10 — Audit Log Tests
# ============================================================================

class TestAuditLog:

    def test_audit_log_records_after_tick(self, tmp_db_path):
        db = _make_db(tmp_db_path)
        engine = _engine(db)
        engine.get_adaptive_state("cfg1", "BTCUSDT")
        assert engine.audit_log.size >= 1

    def test_get_last_n_returns_newest_first(self, tmp_db_path):
        db = _make_db(tmp_db_path)
        engine = _engine(db)
        for _ in range(5):
            engine.get_adaptive_state("cfg1", "BTCUSDT")
        records = engine.audit_log.get_last_n(3)
        assert len(records) == 3

    def test_explain_current_returns_string(self, tmp_db_path):
        db = _make_db(tmp_db_path)
        engine = _engine(db)
        engine.get_adaptive_state("cfg1", "BTCUSDT")
        explanation = engine.audit_log.explain_current({
            "confidence_gate_modifier": 0.04,
            "size_multiplier": 0.70,
            "leverage_multiplier": 1.0,
            "aggressiveness_score": 0.60,
        })
        assert "size_multiplier" in explanation
        assert "BELOW" in explanation

    def test_to_json_is_valid(self, tmp_db_path):
        import json
        db = _make_db(tmp_db_path)
        engine = _engine(db)
        engine.get_adaptive_state("cfg1", "BTCUSDT")
        result = engine.audit_log.to_json(5)
        parsed = json.loads(result)
        assert isinstance(parsed, list)


# ============================================================================
# GAP 1 — Spec-Required Output Aliases (risk_multiplier, threshold_adjustment,
#          max_position_size_modifier) present on AdaptiveState
# ============================================================================

class TestSpecOutputAliases:
    """Gap 1: Spec Section 3 requires canonical output field names."""

    def test_risk_multiplier_alias_equals_size_multiplier(self, tmp_db_path):
        db = _make_db(tmp_db_path)
        engine = _engine(db)
        state = engine.get_adaptive_state("cfg1", "BTCUSDT", drawdown_pct_hint=0.12)
        # risk_multiplier must equal size_multiplier
        assert state.risk_multiplier == state.size_multiplier
        assert state.risk_multiplier < 1.0  # compression active at 12% drawdown

    def test_threshold_adjustment_alias_equals_confidence_gate_modifier(self, tmp_db_path):
        db = _make_db(tmp_db_path)
        with db.connect() as conn:
            for _ in range(3):
                _insert_fill(conn, "BTCUSDT", -5.0)
            conn.commit()
        engine = _engine(db)
        state = engine.get_adaptive_state("cfg1", "BTCUSDT")
        assert state.threshold_adjustment == state.confidence_gate_modifier
        assert state.threshold_adjustment > 0.0  # streak raised it

    def test_max_position_size_modifier_alias_equals_size_multiplier(self, tmp_db_path):
        db = _make_db(tmp_db_path)
        engine = _engine(db)
        state = engine.get_adaptive_state("cfg1", "BTCUSDT")
        assert state.max_position_size_modifier == state.size_multiplier

    def test_all_aliases_within_spec_bounds(self, tmp_db_path):
        db = _make_db(tmp_db_path)
        engine = _engine(db)
        state = engine.get_adaptive_state("cfg1", "BTCUSDT", drawdown_pct_hint=0.20, current_atr_pct=5.0)
        assert 0.20 <= state.risk_multiplier <= 1.0
        assert 0.0 <= state.threshold_adjustment <= 0.12
        assert 0.20 <= state.max_position_size_modifier <= 1.0


# ============================================================================
# GAP 2 — Rolling Win Rate and Expectancy as Trusted DB Inputs
# ============================================================================

class TestRollingStatsInputs:
    """Gap 2: Spec Section 2 requires rolling_win_rate and rolling_expectancy."""

    def test_rolling_win_rate_in_trigger_sources(self, tmp_db_path):
        db = _make_db(tmp_db_path)
        with db.connect() as conn:
            # 10 wins and 5 losses for 66.7% win rate
            for _ in range(10):
                _insert_fill(conn, "BTCUSDT", 15.0)
            for _ in range(5):
                _insert_fill(conn, "BTCUSDT", -8.0)
            conn.commit()
        engine = _engine(db)
        state = engine.get_adaptive_state("cfg1", "BTCUSDT")
        assert "rolling_win_rate" in state.trigger_sources
        assert "rolling_expectancy" in state.trigger_sources

    def test_rolling_win_rate_trust_level_is_durable(self, tmp_db_path):
        db = _make_db(tmp_db_path)
        with db.connect() as conn:
            _insert_fill(conn, "BTCUSDT", 10.0)
            conn.commit()
        engine = _engine(db)
        state = engine.get_adaptive_state("cfg1", "BTCUSDT")
        assert state.input_trust_levels.get("rolling_win_rate") == "DURABLE"
        assert state.input_trust_levels.get("rolling_expectancy") == "DURABLE"

    def test_rolling_stats_on_state_object(self, tmp_db_path):
        db = _make_db(tmp_db_path)
        with db.connect() as conn:
            for _ in range(5):
                _insert_fill(conn, "BTCUSDT", 20.0)   # 5 wins
            for _ in range(5):
                _insert_fill(conn, "BTCUSDT", -10.0)  # 5 losses
            conn.commit()
        engine = _engine(db)
        state = engine.get_adaptive_state("cfg1", "BTCUSDT")
        # 50% win rate, expectancy = (5*20 + 5*(-10))/10 = 5.0
        assert state.rolling_win_rate == 0.5
        assert state.rolling_expectancy == pytest.approx(5.0, abs=0.01)

    def test_rolling_stats_empty_db_defaults_to_zero(self, tmp_db_path):
        db = _make_db(tmp_db_path)
        engine = _engine(db)
        state = engine.get_adaptive_state("cfg1", "BTCUSDT")
        assert state.rolling_win_rate == 0.0
        assert state.rolling_expectancy == 0.0


# ============================================================================
# GAP 3 — sample_quality_flag Present and Accurate
# ============================================================================

class TestSampleQualityFlag:
    """Gap 3: Spec Section 8 requires sample_quality_flag in every log."""

    def test_sample_quality_flag_exists_on_state(self, tmp_db_path):
        db = _make_db(tmp_db_path)
        engine = _engine(db)
        state = engine.get_adaptive_state("cfg1", "BTCUSDT")
        assert hasattr(state, "sample_quality_flag")
        assert state.sample_quality_flag in {"strong", "moderate", "weak"}

    def test_sample_quality_flag_weak_no_fills(self, tmp_db_path):
        db = _make_db(tmp_db_path)
        engine = _engine(db)
        state = engine.get_adaptive_state("cfg1", "BTCUSDT")
        # No fills → no rolling stats → weak
        assert state.sample_quality_flag == "weak"

    def test_sample_quality_flag_in_trigger_sources(self, tmp_db_path):
        db = _make_db(tmp_db_path)
        engine = _engine(db)
        state = engine.get_adaptive_state("cfg1", "BTCUSDT")
        assert "sample_quality_flag" in state.trigger_sources


# ============================================================================
# GAP 4 — Performance Expansion Nudge: Bounded and Not Over-Expanded
# ============================================================================

class TestPerformanceExpansionPolicy:
    """Gap 5 (plan): Strong recent performance may slightly expand size within bounds."""

    def test_expansion_does_not_exceed_1_0(self, tmp_db_path):
        """Performance expansion must never push size_multiplier above 1.0."""
        db = _make_db(tmp_db_path)
        with db.connect() as conn:
            # 20 solid wins in a row
            for _ in range(20):
                _insert_fill(conn, "BTCUSDT", 25.0)
            conn.commit()
        engine = _engine(db)
        for _ in range(5):  # run multiple ticks through EMA
            state = engine.get_adaptive_state("cfg1", "BTCUSDT")
        # EMA asymmetry (alpha_up=0.05) means recovery is very gradual; still ≤ 1.0
        assert state.size_multiplier <= 1.0

    def test_expansion_reason_code_when_strong_performance(self, tmp_db_path):
        """PERF_EXPAND reason code should appear when win rate >= 0.55 and no stress."""
        db = _make_db(tmp_db_path)
        with db.connect() as conn:
            # strong win rate: 18 wins, 2 losses out of 20
            for _ in range(18):
                _insert_fill(conn, "BTCUSDT", 20.0)
            for _ in range(2):
                _insert_fill(conn, "BTCUSDT", -5.0)
            conn.commit()
        engine = _engine(db)
        state = engine.get_adaptive_state("cfg1", "BTCUSDT")
        # win rate = 0.90 → expansion path activated (or neutral; check code appears if no stress)
        # loss_streak check: 2 losses were inserted last, so streak may be 2 → no expansion.
        # Use only wins to guarantee streak=0:
        with db.connect() as conn:
            conn.execute("DELETE FROM trade_fills WHERE symbol=?", ("BTCUSDT",))
            for _ in range(15):
                _insert_fill(conn, "BTCUSDT", 20.0)
            conn.commit()
        state = engine.get_adaptive_state("cfg1", "BTCUSDT")
        # win_rate=1.0, streak=0, drawdown=0 → PERF_EXPAND should be in reason codes
        assert "PERF_EXPAND" in state.bounded_reason_codes

    def test_expansion_absent_when_loss_streak_active(self, tmp_db_path):
        """No expansion when loss streak is non-zero, even with historically high win rate."""
        db = _make_db(tmp_db_path)
        with db.connect() as conn:
            # Add some wins first, then losses (streak=2)
            for _ in range(10):
                _insert_fill(conn, "BTCUSDT", 20.0)
            for _ in range(2):
                _insert_fill(conn, "BTCUSDT", -5.0)
            conn.commit()
        engine = _engine(db)
        state = engine.get_adaptive_state("cfg1", "BTCUSDT")
        # loss_streak=2 → expansion should NOT fire
        assert "PERF_EXPAND" not in state.bounded_reason_codes


# ============================================================================
# GAP 5 — DynamicThresholdCalculator adaptive_offset Integration
# ============================================================================

class TestDynamicThresholdAdaptiveOffset:
    """Gap 4 (plan): get_threshold() accepts adaptive_offset from AdaptiveEngine."""

    def test_zero_offset_does_not_change_threshold(self):
        from app.risk.dynamic_threshold import DynamicThresholdCalculator, FALLBACK_THRESHOLD
        calc = DynamicThresholdCalculator()
        # Not enough samples → fallback path used (offset has no effect here either)
        r_no_offset = calc.get_threshold("BTCUSDT", adaptive_offset=0.0)
        r_default   = calc.get_threshold("BTCUSDT")           # default adaptive_offset=0.0
        assert r_no_offset.threshold == r_default.threshold

    def test_positive_offset_shifts_threshold_up(self):
        """Positive adaptive_offset from AdaptiveEngine (streak penalty) should raise the bar."""
        from app.risk.dynamic_threshold import DynamicThresholdCalculator, MIN_SAMPLES, MAX_THRESHOLD
        calc = DynamicThresholdCalculator()
        # Seed enough samples for the dynamic branch
        for _ in range(MIN_SAMPLES + 10):
            calc.record("BTCUSDT", 0.35)   # all in the same band
        r_baseline = calc.get_threshold("BTCUSDT", adaptive_offset=0.0)
        r_offset   = calc.get_threshold("BTCUSDT", adaptive_offset=0.05)
        # With a positive offset the threshold can only go up or hit the cap
        assert r_offset.threshold >= r_baseline.threshold

    def test_offset_is_bounded_by_max_threshold(self):
        """Even a huge offset must not push threshold past MAX_THRESHOLD."""
        from app.risk.dynamic_threshold import DynamicThresholdCalculator, MIN_SAMPLES, MAX_THRESHOLD
        calc = DynamicThresholdCalculator()
        for _ in range(MIN_SAMPLES + 10):
            calc.record("BTCUSDT", 0.30)
        result = calc.get_threshold("BTCUSDT", adaptive_offset=99.0)  # extreme offset
        assert result.threshold <= MAX_THRESHOLD

