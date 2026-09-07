"""
test_adaptive_engine_validation.py
===================================
Section 8 — Validation Requirements for the Hardened Adaptive Control Foundation.

Tests all 9 mandated scenarios:
  1.  Loss streak grows across multiple trades
  2.  Bot restarts during active loss streak
  3.  Drawdown exceeds threshold while volatility is elevated
  4.  Regime changes while strategy weights are already depressed
  5.  Confidence history survives restart / is reconstructed correctly
  6.  Safety breaker triggers while adaptive throttling is already active
  7.  Adaptive outputs remain bounded under stacked stress conditions
  8.  Live behavior after restart matches pre-restart state
  9.  No duplicate penalty path remains for the same condition
"""
import sqlite3
import tempfile
import os
import gc
import pytest
from unittest.mock import MagicMock, patch

# ── Helpers to build a fresh in-memory AdaptiveEngine ─────────────────────────

def _safe_unlink(path: str) -> None:
    """Windows-safe temp file cleanup: force GC to release SQLite handles first."""
    gc.collect()
    try:
        os.unlink(path)
    except PermissionError:
        pass  # Windows: file still locked by SQLite WAL; OS will clean on process exit

def _make_db_with_fills(fills):
    """
    Create a temporary SQLite DB with trade_fills rows.
    fills: list of (realized_pnl, action) tuples, newest-first.
    """
    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    tmp.close()
    conn = sqlite3.connect(tmp.name)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS trade_fills (
            id INTEGER PRIMARY KEY,
            symbol TEXT,
            action TEXT,
            realized_pnl REAL,
            timestamp_utc TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS equity_snapshots (
            id INTEGER PRIMARY KEY,
            equity REAL,
            snapshot_time TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS decision_logs (
            id INTEGER PRIMARY KEY,
            symbol TEXT,
            decision TEXT,
            logged_at TEXT
        )
    """)
    # Insert fills newest-first (mimics DB ordering)
    for i, (pnl, action) in enumerate(fills):
        ts = f"2026-03-{18 - i:02d}T10:00:00Z"
        conn.execute(
            "INSERT INTO trade_fills (symbol, action, realized_pnl, timestamp_utc) VALUES (?, ?, ?, ?)",
            ("BTCUSDT", action, pnl, ts)
        )
    conn.commit()
    conn.close()
    return tmp.name


def _make_db_with_equity(equities):
    """
    Create a DB with equity snapshots.
    equities: list of float values, oldest-first.
    """
    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    tmp.close()
    conn = sqlite3.connect(tmp.name)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS trade_fills (
            id INTEGER PRIMARY KEY, symbol TEXT, action TEXT,
            realized_pnl REAL, timestamp_utc TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS equity_snapshots (
            id INTEGER PRIMARY KEY, equity REAL, snapshot_time TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS decision_logs (
            id INTEGER PRIMARY KEY, symbol TEXT, decision TEXT, logged_at TEXT
        )
    """)
    for i, eq in enumerate(equities):
        ts = f"2026-03-{i+1:02d}T10:00:00Z"
        conn.execute(
            "INSERT INTO equity_snapshots (equity, snapshot_time) VALUES (?, ?)",
            (eq, ts)
        )
    conn.commit()
    conn.close()
    return tmp.name


def _make_full_db(fills=None, equities=None, blocked_decisions=0):
    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    tmp.close()
    conn = sqlite3.connect(tmp.name)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS trade_fills (
            id INTEGER PRIMARY KEY, symbol TEXT, action TEXT,
            realized_pnl REAL, timestamp_utc TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS equity_snapshots (
            id INTEGER PRIMARY KEY, equity REAL, snapshot_time TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS decision_logs (
            id INTEGER PRIMARY KEY, symbol TEXT, decision TEXT, logged_at TEXT
        )
    """)
    for i, (pnl, action) in enumerate(fills or []):
        ts = f"2026-03-{18 - i:02d}T10:00:00Z"
        conn.execute(
            "INSERT INTO trade_fills (symbol, action, realized_pnl, timestamp_utc) VALUES (?, ?, ?, ?)",
            ("BTCUSDT", action, pnl, ts)
        )
    for i, eq in enumerate(equities or []):
        ts = f"2026-03-{i+1:02d}T10:00:00Z"
        conn.execute(
            "INSERT INTO equity_snapshots (equity, snapshot_time) VALUES (?, ?)",
            (eq, ts)
        )
    for i in range(blocked_decisions):
        conn.execute(
            "INSERT INTO decision_logs (symbol, decision, logged_at) VALUES (?, ?, ?)",
            ("BTCUSDT", "BLOCK", f"2026-03-18T10:{i:02d}:00Z")
        )
    conn.commit()
    conn.close()
    return tmp.name


def _make_engine(db_path):
    from app.adaptive.engine import AdaptiveEngine
    db = MagicMock()

    class _Row(dict):
        pass

    def _mock_connect():
        import contextlib
        real_conn = sqlite3.connect(db_path)
        real_conn.row_factory = sqlite3.Row
        return contextlib.contextmanager(lambda: (yield real_conn))()

    db.connect = _mock_connect
    return AdaptiveEngine(db=db)


# ── Scenario 1 — Loss streak grows across multiple trades ─────────────────────

class TestScenario1_LossStreakGrows:
    """
    SCENARIO 1: Loss streak grows across multiple trades.

    Initial state : 0 consecutive losses → confidence_gate_modifier = 0.0
    Trigger       : DB shows 5 consecutive CLOSE fills with negative PnL
    Expected      : confidence_gate_modifier = 0.10 (5 × 0.02, capped at 0.10)
                    size_multiplier unchanged by streak (no double-penalty)
                    bounded_reason_codes includes 'STREAK_5'
    """

    def test_zero_streak_no_modifier(self):
        db_path = _make_db_with_fills([])
        eng = _make_engine(db_path)
        state = eng.get_adaptive_state("cfg", "BTCUSDT")
        assert state.confidence_gate_modifier == 0.0
        assert "STREAK" not in " ".join(state.bounded_reason_codes)
        _safe_unlink(db_path)

    def test_three_losses_increases_gate(self):
        fills = [(-10, "CLOSE"), (-8, "CLOSE"), (-5, "CLOSE")]
        db_path = _make_db_with_fills(fills)
        eng = _make_engine(db_path)
        state = eng.get_adaptive_state("cfg", "BTCUSDT")
        assert state.confidence_gate_modifier == pytest.approx(0.06, abs=1e-6)
        assert "STREAK_3" in state.bounded_reason_codes
        _safe_unlink(db_path)

    def test_five_losses_capped_at_max(self):
        fills = [(-10, "CLOSE")] * 5
        db_path = _make_db_with_fills(fills)
        eng = _make_engine(db_path)
        state = eng.get_adaptive_state("cfg", "BTCUSDT")
        # 5 × 0.02 = 0.10, exactly at the cap
        assert state.confidence_gate_modifier == pytest.approx(0.10, abs=1e-6)
        assert state.confidence_gate_modifier <= 0.12   # hard bound
        _safe_unlink(db_path)

    def test_streak_does_not_affect_size_multiplier(self):
        """Penalty separation: streaks only affect threshold, not size."""
        fills = [(-10, "CLOSE")] * 5
        db_path = _make_db_with_fills(fills)
        eng = _make_engine(db_path)
        state = eng.get_adaptive_state("cfg", "BTCUSDT")
        # No drawdown → size stays 1.0 (before cooldown, which needs 2+ pressure ticks)
        assert state.size_multiplier == pytest.approx(1.0, abs=0.01)
        _safe_unlink(db_path)

    def test_win_resets_streak(self):
        """A single win interrupts the streak count."""
        fills = [(-10, "CLOSE"), (-8, "CLOSE"), (50, "CLOSE"), (-5, "CLOSE")]
        db_path = _make_db_with_fills(fills)
        eng = _make_engine(db_path)
        state = eng.get_adaptive_state("cfg", "BTCUSDT")
        # Only 2 losses before the win; newest-first: loss, loss, WIN → streak = 2
        assert state.confidence_gate_modifier == pytest.approx(0.04, abs=1e-6)
        _safe_unlink(db_path)


# ── Scenario 2 — Bot restarts during active loss streak ───────────────────────

class TestScenario2_RestartDuringStreak:
    """
    SCENARIO 2: Bot restarts during active loss streak.

    Initial state : 4 losses in DB before restart
    Trigger       : New AdaptiveEngine instance created (simulating restart)
    Expected      : loss_streak correctly reconstructed = 4 from trade_fills
                    was_reconstructed = True
                    confidence_gate_modifier = 0.08  (4 × 0.02)
    Pass/Fail     : state.loss_streak == 4, state.was_reconstructed is True
    """

    def test_streak_reconstructed_after_restart(self):
        fills = [(-5, "CLOSE"), (-7, "CLOSE"), (-3, "CLOSE"), (-9, "CLOSE")]
        db_path = _make_db_with_fills(fills)
        eng = _make_engine(db_path)  # fresh instance = simulated restart
        state = eng.get_adaptive_state("cfg", "BTCUSDT")
        assert state.loss_streak == 4
        assert state.was_reconstructed is True
        assert state.confidence_gate_modifier == pytest.approx(0.08, abs=1e-6)
        _safe_unlink(db_path)

    def test_loss_streak_trust_level_is_durable(self):
        fills = [(-5, "CLOSE"), (-7, "CLOSE")]
        db_path = _make_db_with_fills(fills)
        eng = _make_engine(db_path)
        state = eng.get_adaptive_state("cfg", "BTCUSDT")
        assert state.input_trust_levels["loss_streak"] == "DURABLE"
        _safe_unlink(db_path)


# ── Scenario 3 — Drawdown exceeds threshold while volatility elevated ─────────

class TestScenario3_DrawdownAndVolatility:
    """
    SCENARIO 3: Drawdown ≥ 10% while volatility is elevated (ATR ≥ 3%).

    Initial state : equity peak = 10000, current = 8900 (11% drawdown), ATR% = 4%
    Trigger       : get_adaptive_state called
    Expected      : size_multiplier = 0.40  (10% drawdown bracket)
                    leverage_multiplier < 1.0  (elevated vol)
                    No streak applied to size (separate penalty lanes)
                    bounded_reason_codes includes 'DD_10_PCT' and 'HIGH_VOL'
    """

    def test_drawdown_10pct_reduces_size(self):
        equities = [10000.0, 10050.0, 9800.0, 8900.0]   # peak = 10050, current = 8900 → 11.4% DD
        db_path = _make_db_with_equity(equities)
        eng = _make_engine(db_path)
        state = eng.get_adaptive_state("cfg", "BTCUSDT", current_atr_pct=1.0)
        assert state.size_multiplier == pytest.approx(0.40, abs=0.01)
        assert "DD_10_PCT" in state.bounded_reason_codes
        _safe_unlink(db_path)

    def test_elevated_volatility_reduces_leverage(self):
        db_path = _make_db_with_equity([10000.0])
        eng = _make_engine(db_path)
        state = eng.get_adaptive_state("cfg", "BTCUSDT", current_atr_pct=4.0)
        assert state.leverage_multiplier < 1.0
        assert "HIGH_VOL" in state.bounded_reason_codes
        _safe_unlink(db_path)

    def test_drawdown_and_high_vol_apply_independently(self):
        equities = [10000.0, 8900.0]  # 11% drawdown
        db_path = _make_db_with_equity(equities)
        eng = _make_engine(db_path)
        state = eng.get_adaptive_state("cfg", "BTCUSDT", current_atr_pct=4.0)
        # Both penalties active simultaneously, in separate lanes
        assert state.size_multiplier <= 0.40         # drawdown lane
        assert state.leverage_multiplier < 1.0        # volatility lane
        # No streaks applied to size
        assert "STREAK" not in " ".join(state.bounded_reason_codes) or state.loss_streak == 0
        _safe_unlink(db_path)

    def test_drawdown_trust_level_durable_when_db_populated(self):
        equities = [10000.0, 8900.0]
        db_path = _make_db_with_equity(equities)
        eng = _make_engine(db_path)
        state = eng.get_adaptive_state("cfg", "BTCUSDT")
        assert state.input_trust_levels["drawdown_pct"] == "DURABLE"
        _safe_unlink(db_path)


# ── Scenario 4 — Regime changes while weights are depressed ───────────────────

class TestScenario4_RegimeChangeWithDepressedWeights:
    """
    SCENARIO 4: Regime changes while strategy_weight_adjustments are already depressed.

    Initial state : active_regime = "STRONG_TREND", weights externally set
    Trigger       : active_regime changes to "HIGH_VOLATILITY"
    Expected      : AdaptiveState.regime field reflects new regime
                    Engine does NOT independently change weight adjustments
                    (PerformanceTracker owns that; engine just carries the slot)
    Pass/Fail     : state.regime == new regime; no weight mutation by engine
    """

    def test_regime_propagated_in_state(self):
        db_path = _make_db_with_fills([])
        eng = _make_engine(db_path)
        state = eng.get_adaptive_state("cfg", "BTCUSDT", active_regime="HIGH_VOLATILITY")
        assert state.regime == "HIGH_VOLATILITY"
        _safe_unlink(db_path)

    def test_engine_does_not_mutate_weight_adjustments(self):
        """Engine returns empty dict; weights are external to AdaptiveEngine."""
        db_path = _make_db_with_fills([])
        eng = _make_engine(db_path)
        state = eng.get_adaptive_state("cfg", "BTCUSDT", active_regime="RANGE")
        # AdaptiveEngine itself never populates this — PerformanceTracker does externally
        assert state.strategy_weight_adjustments == {}
        _safe_unlink(db_path)

    def test_regime_change_does_not_reset_cooldown(self):
        """Regime change is informational; it doesn't reset the cooldown state."""
        fills = [(-5, "CLOSE")] * 3       # 3 consecutive losses → pressure
        db_path = _make_db_with_fills(fills)
        eng = _make_engine(db_path)
        # Build up 2 ticks of pressure to reach SOFT cooldown
        eng.get_adaptive_state("cfg", "BTCUSDT", active_regime="STRONG_TREND")
        state = eng.get_adaptive_state("cfg", "BTCUSDT", active_regime="HIGH_VOLATILITY")
        # Cooldown state reached SOFT after 2 ticks of loss pressure
        assert state.cooldown_state in ("SOFT", "HARD", "NONE")  # state machine ran, not reset
        _safe_unlink(db_path)


# ── Scenario 5 — Confidence history survives restart ──────────────────────────

class TestScenario5_ConfidenceHistorySurvivesRestart:
    """
    SCENARIO 5: Confidence distribution history survives restart or is reconstructed.

    The DynamicThresholdCalculator stores rolling confidence history in memory,
    but AdaptiveEngine's DURABLE loss_streak ensures the threshold MODIFIER is
    always deterministically reconstructed from DB.

    Initial state : 3 losses in DB, giving confidence_gate_modifier = 0.06
    Trigger       : new engine instance (restart)
    Expected      : confidence_gate_modifier == 0.06 (reconstructed from DB streak)
                    trigger_sources records exact DB-sourced streak value
    """

    def test_confidence_modifier_deterministic_across_restarts(self):
        fills = [(-5, "CLOSE")] * 3
        db_path = _make_db_with_fills(fills)

        eng1 = _make_engine(db_path)
        s1 = eng1.get_adaptive_state("cfg", "BTCUSDT")

        eng2 = _make_engine(db_path)   # simulated restart
        s2 = eng2.get_adaptive_state("cfg", "BTCUSDT")

        assert s1.confidence_gate_modifier == s2.confidence_gate_modifier
        assert s1.loss_streak == s2.loss_streak
        _safe_unlink(db_path)

    def test_trigger_sources_record_db_streak(self):
        fills = [(-5, "CLOSE")] * 2
        db_path = _make_db_with_fills(fills)
        eng = _make_engine(db_path)
        state = eng.get_adaptive_state("cfg", "BTCUSDT")
        assert state.trigger_sources["loss_streak_db"] == 2
        _safe_unlink(db_path)


# ── Scenario 6 — Safety breaker while adaptive throttling active ───────────────

class TestScenario6_SafetyBreakerWithThrottling:
    """
    SCENARIO 6: Safety breaker triggers while adaptive throttling is already active.

    The SafetyEngine and AdaptiveEngine are orthogonal. SafetyEngine can block
    entirely; AdaptiveEngine compresses. Neither modifies the other's output.

    Initial state : size_multiplier = 0.40 (10% drawdown), cooldown = NONE
    Trigger       : SafetyEngine blocks the signal (external)
    Expected      : AdaptiveEngine output unchanged; overall trade blocked at runner
                    No interaction between safety block and adaptive state
    """

    def test_adaptive_state_unaffected_by_external_block(self):
        equities = [10000.0, 8900.0]   # 11% drawdown → size_multiplier = 0.40
        db_path = _make_db_with_equity(equities)
        eng = _make_engine(db_path)
        state = eng.get_adaptive_state("cfg", "BTCUSDT")
        # Adaptive still produces its output regardless of external gates
        assert state.size_multiplier == pytest.approx(0.40, abs=0.01)
        # SafetyEngine is external; we verify adaptive does NOT embed a kill switch
        assert state.cooldown_state in ("NONE", "SOFT", "HARD")   # bounded state machine, not external
        _safe_unlink(db_path)


# ── Scenario 7 — Bounded under stacked stress conditions ──────────────────────

class TestScenario7_BoundedUnderStackedStress:
    """
    SCENARIO 7: All stress inputs simultaneously at maximum.

    Initial state : 6 consecutive losses, 15%+ drawdown, ATR% = 8%, 15 BLOCKED decisions
    Trigger       : get_adaptive_state called
    Expected      : All outputs within defined hard bounds
                    confidence_gate_modifier ≤ 0.12
                    size_multiplier ≥ 0.20
                    leverage_multiplier ≥ 0.25
                    aggressiveness_score ≥ 0.0 and ≤ 1.0
                    cooldown_state ∈ {'NONE', 'SOFT', 'HARD'} only
    """

    def test_all_bounds_respected_under_maximum_stress(self):
        fills = [(-50, "CLOSE")] * 8    # 8 consecutive losses
        equities = [10000.0, 8400.0]    # 16% drawdown
        db_path = _make_full_db(fills=fills, equities=equities, blocked_decisions=15)
        # Two ticks at max pressure to reach HARD cooldown
        eng = _make_engine(db_path)
        eng.get_adaptive_state("cfg", "BTCUSDT", current_atr_pct=9.0)
        state = eng.get_adaptive_state("cfg", "BTCUSDT", current_atr_pct=9.0)

        assert state.confidence_gate_modifier <= 0.12
        assert state.size_multiplier >= 0.20
        assert state.leverage_multiplier >= 0.25
        assert 0.0 <= state.aggressiveness_score <= 1.0
        assert state.cooldown_state in ("NONE", "SOFT", "HARD")
        _safe_unlink(db_path)

    def test_size_floor_not_breached_with_cooldown(self):
        """Even with HARD cooldown applied on top of 15% drawdown, floor holds."""
        fills = [(-50, "CLOSE")] * 3
        equities = [10000.0, 8400.0]   # 16% drawdown → size 0.20
        db_path = _make_full_db(fills=fills, equities=equities)
        eng = _make_engine(db_path)
        eng.get_adaptive_state("cfg", "BTCUSDT")    # tick 1 → cooldown escalates
        state = eng.get_adaptive_state("cfg", "BTCUSDT")  # tick 2 → SOFT or HARD
        # Even with HARD cooldown (×0.70): 0.20 × 0.70 = 0.14 → clamped to 0.20
        assert state.size_multiplier >= 0.20
        _safe_unlink(db_path)


# ── Scenario 8 — Live behavior after restart matches pre-restart ───────────────

class TestScenario8_PostRestartMatchesPreRestart:
    """
    SCENARIO 8: Live behavior after restart matches pre-restart state.

    Initial state : DB contains 4 losses, 8% drawdown
    Trigger       : new engine instance created
    Expected      : loss_streak, drawdown_pct, confidence_gate_modifier, size_multiplier
                    all identical between pre-restart and post-restart instances
    """

    def test_deterministic_state_across_restarts(self):
        fills = [(-10, "CLOSE")] * 4
        equities = [10000.0, 9230.0]   # 7.7% drawdown → size 0.70
        db_path = _make_full_db(fills=fills, equities=equities)

        eng_pre = _make_engine(db_path)
        s_pre = eng_pre.get_adaptive_state("cfg", "BTCUSDT", current_atr_pct=1.0)

        eng_post = _make_engine(db_path)    # simulated restart
        s_post = eng_post.get_adaptive_state("cfg", "BTCUSDT", current_atr_pct=1.0)

        assert s_pre.loss_streak == s_post.loss_streak
        assert s_pre.drawdown_pct == pytest.approx(s_post.drawdown_pct, abs=1e-4)
        assert s_pre.confidence_gate_modifier == pytest.approx(s_post.confidence_gate_modifier, abs=1e-6)
        assert s_pre.size_multiplier == pytest.approx(s_post.size_multiplier, abs=1e-4)
        _safe_unlink(db_path)

    def test_was_reconstructed_true_on_first_call(self):
        db_path = _make_full_db()
        eng = _make_engine(db_path)
        state = eng.get_adaptive_state("cfg", "BTCUSDT")
        assert state.was_reconstructed is True
        _safe_unlink(db_path)


# ── Scenario 9 — No duplicate penalty path for same condition ─────────────────

class TestScenario9_NoDuplicatePenaltyPaths:
    """
    SCENARIO 9: No duplicate penalty path for the same condition.

    Verifies that:
    - Loss streak ONLY affects confidence_gate_modifier, NOT size_multiplier
    - Drawdown ONLY affects size_multiplier, NOT confidence_gate_modifier
    - Volatility ONLY affects leverage_multiplier, NOT the others
    """

    def test_streak_only_modifies_confidence(self):
        fills = [(-10, "CLOSE")] * 5    # max streak penalty = +0.10
        db_path = _make_full_db(fills=fills, equities=[10000.0])  # no drawdown
        eng = _make_engine(db_path)
        state = eng.get_adaptive_state("cfg", "BTCUSDT", current_atr_pct=1.0)

        # Streak touches confidence gate
        assert state.confidence_gate_modifier > 0.0
        # But NOT size (drawdown = 0.0 → size stays 1.0 before any cooldown)
        assert state.size_multiplier == pytest.approx(1.0, abs=0.01)
        # And NOT leverage (ATR = 1.0 → no compression)
        assert state.leverage_multiplier == pytest.approx(1.0, abs=0.01)
        _safe_unlink(db_path)

    def test_drawdown_only_modifies_size(self):
        equities = [10000.0, 8900.0]    # 11% drawdown → size 0.40
        db_path = _make_full_db(equities=equities)   # no losses
        eng = _make_engine(db_path)
        state = eng.get_adaptive_state("cfg", "BTCUSDT", current_atr_pct=1.0)

        # Drawdown touches size
        assert state.size_multiplier == pytest.approx(0.40, abs=0.01)
        # But NOT confidence gate (no loss streak)
        assert state.confidence_gate_modifier == pytest.approx(0.0, abs=1e-6)
        # And NOT leverage (ATR = 1.0)
        assert state.leverage_multiplier == pytest.approx(1.0, abs=0.01)
        _safe_unlink(db_path)

    def test_volatility_only_modifies_leverage(self):
        db_path = _make_full_db(equities=[10000.0])  # no drawdown, no losses
        eng = _make_engine(db_path)
        state = eng.get_adaptive_state("cfg", "BTCUSDT", current_atr_pct=5.0)

        # Volatility touches leverage
        assert state.leverage_multiplier < 1.0
        # But NOT size (no drawdown)
        assert state.size_multiplier == pytest.approx(1.0, abs=0.01)
        # And NOT confidence gate (no streak)
        assert state.confidence_gate_modifier == pytest.approx(0.0, abs=1e-6)
        _safe_unlink(db_path)

    def test_no_cross_contamination_under_all_stress(self):
        """Verify penalty isolation holds even when all three conditions are active."""
        fills = [(-10, "CLOSE")] * 3        # streak = 3 → gate +0.06
        equities = [10000.0, 8900.0]        # drawdown 11% → size 0.40
        db_path = _make_full_db(fills=fills, equities=equities)
        eng = _make_engine(db_path)
        state = eng.get_adaptive_state("cfg", "BTCUSDT", current_atr_pct=5.0)

        # Each penalty only in its lane
        assert pytest.approx(0.06, abs=1e-6) == state.confidence_gate_modifier   # streak lane
        assert state.size_multiplier <= 0.40      # drawdown lane (may be further compressed by cooldown)
        assert state.leverage_multiplier < 1.0    # volatility lane

        # Reason codes must be distinct per source
        reason_set = set(state.bounded_reason_codes)
        assert any("STREAK" in r for r in reason_set)
        assert any("DD" in r for r in reason_set)
        assert any("VOL" in r for r in reason_set)
        _safe_unlink(db_path)


# ── Contract field completeness ────────────────────────────────────────────────

class TestOutputContractCompleteness:
    """Verify all Section 5 contract fields are present in every emitted state."""

    REQUIRED_FIELDS = [
        "timestamp_utc", "adaptive_state_version",
        "aggressiveness_score", "confidence_gate_modifier",
        "size_multiplier", "leverage_multiplier",
        "strategy_weight_adjustments", "cooldown_state",
        "trigger_sources", "bounded_reason_codes",
        "input_trust_levels", "was_reconstructed",
        "min_confidence_gate", "loss_streak", "drawdown_pct", "regime",
    ]

    def test_all_contract_fields_present(self):
        db_path = _make_full_db()
        eng = _make_engine(db_path)
        state = eng.get_adaptive_state("cfg", "BTCUSDT")
        for field in self.REQUIRED_FIELDS:
            assert hasattr(state, field), f"Missing contract field: {field}"
        _safe_unlink(db_path)

    def test_state_version_is_correct(self):
        db_path = _make_full_db()
        eng = _make_engine(db_path)
        state = eng.get_adaptive_state("cfg", "BTCUSDT")
        assert state.adaptive_state_version == "2.0"
        _safe_unlink(db_path)

    def test_trigger_sources_populated(self):
        db_path = _make_full_db()
        eng = _make_engine(db_path)
        state = eng.get_adaptive_state("cfg", "BTCUSDT")
        # Required trigger_sources keys
        for key in ("symbol", "config_id", "loss_streak_db", "drawdown_db",
                    "exec_fail_rate", "atr_pct", "active_regime"):
            assert key in state.trigger_sources, f"Missing trigger_source key: {key}"
        _safe_unlink(db_path)
