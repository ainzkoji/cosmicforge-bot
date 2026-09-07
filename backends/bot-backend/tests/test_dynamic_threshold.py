"""
Unit tests for DynamicThresholdCalculator.

Covers all behavioral scenarios specified in the implementation requirements:
    - Cold start / insufficient history → fallback to 0.5
    - Weak market regime (low confidence) → floor clamping at 0.25
    - Strong market regime (high confidence) → cap clamping at 0.65
    - Normal regime → percentile within bounds
    - Rolling window eviction (old samples drop off)
    - DYNAMIC_THRESHOLD_ENABLED=false → always 0.5
    - Thread safety under concurrent writes
"""
import sys
import os
import threading
import importlib
from unittest.mock import patch

import pytest

# Ensure the bot-backend package root is on sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_calculator(**env_overrides):
    """
    Reimport dynamic_threshold with specified env overrides so module-level
    constants are re-evaluated.
    """
    import app.risk.dynamic_threshold as mod

    env = {
        "DYNAMIC_THRESHOLD_ENABLED": "true",
        "DYNAMIC_THRESHOLD_WINDOW_SIZE": "175",
        "DYNAMIC_THRESHOLD_PERCENTILE": "70",
        "DYNAMIC_THRESHOLD_MIN": "0.25",
        "DYNAMIC_THRESHOLD_MAX": "0.65",
        "DYNAMIC_THRESHOLD_FALLBACK": "0.5",
        "DYNAMIC_THRESHOLD_MIN_SAMPLES": "30",
    }
    env.update(env_overrides)

    with patch.dict(os.environ, env, clear=False):
        importlib.reload(mod)
        calc = mod.DynamicThresholdCalculator()
        return calc, mod


# ---------------------------------------------------------------------------
# Test Suite
# ---------------------------------------------------------------------------

class TestDynamicThresholdCalculator:

    # ------------------------------------------------------------------
    # Scenario C: Cold start / insufficient history
    # ------------------------------------------------------------------

    def test_fallback_zero_samples(self):
        """New symbol with no history → fallback threshold 0.5."""
        calc, mod = _make_calculator()
        result = calc.get_threshold("BTCUSDT")

        assert result.threshold == pytest.approx(0.5)
        assert result.threshold_type == "fallback_static"
        assert result.samples_available == 0

    def test_fallback_insufficient_samples(self):
        """12 samples (< 30 MIN_SAMPLES) → fallback 0.5."""
        calc, mod = _make_calculator()
        for _ in range(12):
            calc.record("SOLUSDT", 0.28)

        result = calc.get_threshold("SOLUSDT")
        assert result.threshold == pytest.approx(0.5)
        assert result.threshold_type == "fallback_static"
        assert result.samples_available == 12

    def test_fallback_exactly_at_boundary(self):
        """29 samples (< 30) still returns fallback; 30 samples activates percentile."""
        calc, mod = _make_calculator()

        # 29 samples — should still be fallback
        for _ in range(29):
            calc.record("XBTUSD", 0.50)
        assert calc.get_threshold("XBTUSD").threshold_type == "fallback_static"

        # 30th sample — should now compute percentile
        calc.record("XBTUSD", 0.50)
        result = calc.get_threshold("XBTUSD")
        # All values are 0.50 → 70th percentile = 0.50 → within bounds
        assert result.threshold_type == "dynamic_percentile"
        assert result.threshold == pytest.approx(0.50, abs=1e-4)

    # ------------------------------------------------------------------
    # Scenario A: Weak market regime → floor clamping
    # ------------------------------------------------------------------

    def test_weak_market_floor_clamped(self):
        """
        Recent confidence scores all very low → raw 70th percentile < 0.25
        → clamped to FLOOR 0.25.
        """
        calc, mod = _make_calculator()
        weak_scores = [0.10, 0.12, 0.14, 0.11, 0.13] * 40  # 200 samples
        for s in weak_scores:
            calc.record("BTCUSDT", s)

        result = calc.get_threshold("BTCUSDT")
        assert result.threshold == pytest.approx(0.25)
        assert result.threshold_type == "dynamic_floor"
        assert result.raw_percentile is not None
        assert result.raw_percentile < 0.25

    # ------------------------------------------------------------------
    # Scenario B: Strong market regime → cap clamping
    # ------------------------------------------------------------------

    def test_strong_market_cap_clamped(self):
        """
        Recent confidence scores all very high → raw 70th percentile > 0.65
        → clamped to CAP 0.65.
        """
        calc, mod = _make_calculator()
        strong_scores = [0.75, 0.82, 0.68, 0.91, 0.77] * 40  # 200 samples
        for s in strong_scores:
            calc.record("ETHUSDT", s)

        result = calc.get_threshold("ETHUSDT")
        assert result.threshold == pytest.approx(0.65)
        assert result.threshold_type == "dynamic_cap"
        assert result.raw_percentile is not None
        assert result.raw_percentile > 0.65

    # ------------------------------------------------------------------
    # Normal market regime → percentile within bounds
    # ------------------------------------------------------------------

    def test_normal_market_within_bounds(self):
        """
        Moderate confidence distribution → 70th percentile falls within
        [0.25, 0.65] and is returned as-is.
        """
        import numpy as np

        calc, mod = _make_calculator()
        scores = [0.35, 0.42, 0.38, 0.45, 0.40] * 40  # 200 samples
        for s in scores:
            calc.record("BNBUSDT", s)

        result = calc.get_threshold("BNBUSDT")
        expected_raw = float(np.percentile(scores[:175], 70))  # window=175, last 175

        # Should be within bounds
        assert 0.25 <= result.threshold <= 0.65
        assert result.threshold_type == "dynamic_percentile"
        assert result.threshold == pytest.approx(result.raw_percentile, abs=1e-6)

    # ------------------------------------------------------------------
    # Rolling window eviction
    # ------------------------------------------------------------------

    def test_rolling_window_evicts_old_samples(self):
        """
        After filling past WINDOW_SIZE, old samples drop off (deque maxlen).
        Inserting all-high scores then all-low scores → threshold adapts down.
        """
        calc, mod = _make_calculator(DYNAMIC_THRESHOLD_WINDOW_SIZE="50",
                                     DYNAMIC_THRESHOLD_MIN_SAMPLES="10")

        # Fill first half with high confidence → high threshold
        for _ in range(50):
            calc.record("ADAUSDT", 0.90)

        result_high = calc.get_threshold("ADAUSDT")
        assert result_high.threshold == pytest.approx(0.65)  # capped

        # Now overwrite entire window with very low confidence
        for _ in range(50):
            calc.record("ADAUSDT", 0.10)

        result_low = calc.get_threshold("ADAUSDT")
        assert result_low.threshold == pytest.approx(0.25)  # floored
        # Old high samples should be fully evicted
        assert result_low.samples_available == 50

    # ------------------------------------------------------------------
    # Disabled via environment variable
    # ------------------------------------------------------------------

    def test_disabled_env_var_returns_fallback(self):
        """DYNAMIC_THRESHOLD_ENABLED=false → always returns static fallback."""
        calc, mod = _make_calculator(DYNAMIC_THRESHOLD_ENABLED="false")

        # Record plenty of samples
        for _ in range(200):
            calc.record("BTCUSDT", 0.90)

        result = calc.get_threshold("BTCUSDT")
        assert result.threshold == pytest.approx(0.5)
        assert result.threshold_type == "fallback_static"

    # ------------------------------------------------------------------
    # Bound label helper
    # ------------------------------------------------------------------

    def test_bound_label_floor(self):
        """ThresholdResult.bound_label returns 'floor' for dynamic_floor."""
        calc, _ = _make_calculator()
        for _ in range(50):
            calc.record("T1", 0.05)
        result = calc.get_threshold("T1")
        assert result.bound_label == "floor"

    def test_bound_label_cap(self):
        """ThresholdResult.bound_label returns 'cap' for dynamic_cap."""
        calc, _ = _make_calculator()
        for _ in range(50):
            calc.record("T2", 0.99)
        result = calc.get_threshold("T2")
        assert result.bound_label == "cap"

    def test_bound_label_fallback(self):
        """ThresholdResult.bound_label returns 'fallback' for cold start."""
        calc, _ = _make_calculator()
        result = calc.get_threshold("T3")
        assert result.bound_label == "fallback"

    # ------------------------------------------------------------------
    # Prune
    # ------------------------------------------------------------------

    def test_prune_removes_inactive_symbols(self):
        """prune() removes deques for symbols not in the active set."""
        calc, _ = _make_calculator()
        for sym in ("BTCUSDT", "ETHUSDT", "XRPUSDT"):
            for _ in range(5):
                calc.record(sym, 0.5)

        calc.prune(active_symbols={"BTCUSDT"})

        assert calc.sample_count("BTCUSDT") == 5
        assert calc.sample_count("ETHUSDT") == 0
        assert calc.sample_count("XRPUSDT") == 0

    # ------------------------------------------------------------------
    # Thread safety
    # ------------------------------------------------------------------

    def test_thread_safety_no_exceptions(self):
        """
        10 threads writing 100 samples each simultaneously must not raise.
        500+ total samples written, no race condition errors.
        """
        calc, _ = _make_calculator()
        errors: list = []

        def worker(thread_id: int):
            try:
                for i in range(100):
                    calc.record("BTCUSDT", float(thread_id * 0.01 + i * 0.001))
                    _ = calc.get_threshold("BTCUSDT")
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors, f"Thread errors occurred: {errors}"
        # At least some samples should be recorded (window is 175, 1000 total written)
        count = calc.sample_count("BTCUSDT")
        assert count == 175  # deque maxlen enforced


# ---------------------------------------------------------------------------
# Integration: SafetyEngine uses dynamic threshold in Gate 3
# ---------------------------------------------------------------------------

class TestSafetyEngineDynamicIntegration:
    """
    Light integration tests verifying dynamic threshold is wired into
    SafetyEngine.check_pre_trade (Gate 3) without changing other layers.
    """

    @pytest.fixture
    def engine(self):
        import tempfile
        from app.risk.safety_engine import SafetyEngine, SafetyConfig
        from app.risk.risk_budget import RiskBudgetEngine, RiskBudgetConfig
        from app.risk.account_protection import AccountProtection
        from shared_lib.persistence.db import DB

        tf = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
        tf.close()
        db = DB(tf.name)
        config = SafetyConfig(min_confidence_hard=0.18, min_confidence_soft=0.05)
        risk_cfg = RiskBudgetConfig(
            portfolio_risk_pct=0.05,
            per_trade_risk_pct=0.01,
            max_margin_usage_pct=0.50,
            base_slots=5,
            max_slots=15,
        )
        rb = RiskBudgetEngine(risk_cfg)
        protection = AccountProtection(db)
        return SafetyEngine(db, rb, protection, config)

    def test_engine_has_dynamic_threshold(self, engine):
        """SafetyEngine should have _dyn_threshold attribute."""
        from app.risk.dynamic_threshold import DynamicThresholdCalculator
        assert hasattr(engine, "_dyn_threshold")
        assert isinstance(engine._dyn_threshold, DynamicThresholdCalculator)

    def test_cold_start_uses_fallback_threshold(self, engine):
        """
        With no history, dynamic threshold falls back to 0.5.
        Confidence 0.6 (> 0.5) should PASS.
        """
        decision = engine.check_pre_trade(
            config_id="test",
            symbol="NEWTOKEN",
            confidence=0.6,
            leverage=5.0,
            current_equity=10000.0,
            open_positions=0,
        )
        assert decision.allowed is True

    def test_cold_start_blocks_below_fallback(self, engine):
        """
        Cold start: confidence 0.10 (< fallback capped at config.min_confidence_hard=0.18)
        → BLOCK.  Confidence=0.30 was previously tested here but correctly PASSES now that
        the cold-start fallback is capped at config.min_confidence_hard (0.18) rather than
        the module default (0.5).
        """
        decision = engine.check_pre_trade(
            config_id="test",
            symbol="NEWTOKEN2",
            confidence=0.10,   # below min_confidence_hard=0.18
            leverage=5.0,
            current_equity=10000.0,
            open_positions=0,
        )
        assert decision.allowed is False
        from app.risk.safety_engine import BlockReason
        assert decision.block_reason == BlockReason.LOW_CONFIDENCE

    def test_soft_threshold_still_honoured(self, engine):
        """
        is_fallback_mode=True must still use soft threshold (0.05), not dynamic.
        Confidence 0.07 should pass soft gate.
        """
        decision = engine.check_pre_trade(
            config_id="test",
            symbol="BTCUSDT",
            confidence=0.07,
            leverage=3.0,
            current_equity=10000.0,
            open_positions=0,
            is_fallback_mode=True,
        )
        assert decision.allowed is True


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
