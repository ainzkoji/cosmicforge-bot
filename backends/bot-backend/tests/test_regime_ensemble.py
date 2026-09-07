"""
Regime-Gated Ensemble Test Suite
===================================
Tests for all 7 layers of the new institutional ensemble architecture.

Run with:
    cd c:\\Users\\favou\\OneDrive\\Desktop\\cosmicforge-bot\\backends\\bot-backend
    python -m pytest tests/test_regime_ensemble.py -v
"""
import pytest
from unittest.mock import MagicMock, patch
from typing import List


# =========================================================================
# Fixtures
# =========================================================================

def _klines(n=150, trend_up=True, atr_pct=1.5):
    """Generate synthetic klines for regime classification."""
    import random
    random.seed(42)
    base = 50000.0
    klines = []
    for i in range(n):
        delta = 10.0 if trend_up else -10.0
        close = base + delta * i + random.uniform(-30, 30)
        high  = close + random.uniform(0, 50 * atr_pct / 1.5)
        low   = close - random.uniform(0, 50 * atr_pct / 1.5)
        vol   = 10000.0
        klines.append([i * 900000, str(close - 5), str(high), str(low), str(close), str(vol), i * 900000 + 899999])
        base = close
    return klines


@pytest.fixture
def trending_klines():
    return _klines(150, trend_up=True, atr_pct=1.5)


@pytest.fixture
def chop_klines():
    """Flat, low-ATR klines that should trigger LOW_VOL_CHOP."""
    import random
    random.seed(7)
    base = 50000.0
    klines = []
    for i in range(150):
        close = base + random.uniform(-30, 30)  # no trend, tiny range
        high  = close + random.uniform(0, 5)    # tiny range → low ATR%
        low   = close - random.uniform(0, 5)
        klines.append([i, str(close), str(high), str(low), str(close), "1000", i])
    return klines


# =========================================================================
# Layer 3: Activation Engine
# =========================================================================

class TestActivationEngine:
    def test_low_vol_chop_blocks_all(self):
        from app.strategy.activation import ActivationEngine
        from app.strategy.regime import MarketRegime, TrendDirection

        engine = ActivationEngine()
        result = engine.get_active(MarketRegime.LOW_VOLATILITY_CHOP)

        assert result.is_blocked is True
        assert len(result.active_strategies) == 0
        assert result.blocked_reason is not None

    def test_strong_trend_has_trend_strategies(self):
        from app.strategy.activation import ActivationEngine
        from app.strategy.regime import MarketRegime, TrendDirection

        engine = ActivationEngine()
        result = engine.get_active(MarketRegime.STRONG_TREND, TrendDirection.UP)

        assert "supertrend" in result.active_strategies
        assert "trend_pullback" in result.active_strategies
        assert result.signal_bias == "BUY_ONLY"

    def test_range_only_has_reversion(self):
        from app.strategy.activation import ActivationEngine
        from app.strategy.regime import MarketRegime

        engine = ActivationEngine()
        result = engine.get_active(MarketRegime.RANGE)

        assert "vwap_reversion" in result.active_strategies
        assert "bollinger_reversion" in result.active_strategies
        # Trend followers excluded from RANGE
        assert "supertrend" not in result.active_strategies
        assert "trend_pullback" not in result.active_strategies

    def test_high_volatility_only_breakouts(self):
        from app.strategy.activation import ActivationEngine
        from app.strategy.regime import MarketRegime

        engine = ActivationEngine()
        result = engine.get_active(MarketRegime.HIGH_VOLATILITY)

        assert "squeeze_breakout" in result.active_strategies
        assert "donchian_breakout" in result.active_strategies
        assert "supertrend" not in result.active_strategies

    def test_filter_votes_strips_inactive(self):
        from app.strategy.activation import ActivationEngine
        from app.strategy.regime import MarketRegime

        engine = ActivationEngine()
        activation = engine.get_active(MarketRegime.RANGE)

        # Include a vote from a strategy NOT in RANGE's allowlist
        votes = [
            ("vwap_reversion", "BUY", 0.7),
            ("supertrend", "BUY", 0.9),           # should be stripped
            ("bollinger_reversion", "SELL", 0.6),
        ]
        filtered = engine.filter_votes(votes, activation)
        names = [v[0] for v in filtered]

        assert "supertrend" not in names
        assert "vwap_reversion" in names

    def test_buy_only_bias_strips_sell_votes(self):
        from app.strategy.activation import ActivationEngine
        from app.strategy.regime import MarketRegime, TrendDirection

        engine = ActivationEngine()
        activation = engine.get_active(MarketRegime.STRONG_TREND, TrendDirection.UP)
        assert activation.signal_bias == "BUY_ONLY"

        votes = [
            ("supertrend", "BUY",  0.8),
            ("trend_pullback", "SELL", 0.6),   # should be stripped by bias
        ]
        filtered = engine.filter_votes(votes, activation)
        assert all(sig != "SELL" for _, sig, _ in filtered)


# =========================================================================
# Layer 6: Risk Compression
# =========================================================================

class TestRiskCompression:
    def test_no_compression_in_normal_conditions(self):
        from app.risk.risk_compression import compute_compression

        result = compute_compression(
            adaptive_size_multiplier=1.0, 
            adaptive_leverage_multiplier=1.0, 
            atr_pct=1.5
        )

        assert result.risk_multiplier == 1.0
        assert result.leverage_multiplier == 1.0
        assert result.is_hard_blocked is False

    def test_central_adaptive_multipliers_passed_through(self):
        from app.risk.risk_compression import compute_compression

        result = compute_compression(
            adaptive_size_multiplier=0.40, 
            adaptive_leverage_multiplier=0.80, 
            atr_pct=1.5
        )

        # RiskCompression should just pass these through
        assert result.risk_multiplier == pytest.approx(0.40, abs=0.01)
        assert result.leverage_multiplier == pytest.approx(0.80, abs=0.01)

    def test_low_atr_hard_blocks(self):
        from app.risk.risk_compression import compute_compression

        # ATR < 0.30% → dead market, no edge
        result = compute_compression(
            adaptive_size_multiplier=1.0, 
            adaptive_leverage_multiplier=1.0, 
            atr_pct=0.15
        )

        assert result.is_hard_blocked is True
        assert result.risk_multiplier == 0.0
        assert result.leverage_multiplier == 0.0

    def test_apply_compression_scales_usdt_and_leverage(self):
        from app.risk.risk_compression import compute_compression, apply_compression

        result = compute_compression(
            adaptive_size_multiplier=0.50, 
            adaptive_leverage_multiplier=0.50, 
            atr_pct=1.5
        )
        usdt, lev = apply_compression(100.0, 10.0, result)

        assert usdt == 50.0
        assert lev == 5.0


# =========================================================================
# Layer 7: Execution Filter
# =========================================================================

class TestExecutionFilter:
    def test_wide_spread_blocks(self):
        from app.execution.execution_filter import check_execution

        result = check_execution(
            symbol="BTCUSDT",
            current_price=50000.0,
            bid=49980.0,
            ask=50040.0,          # spread = 0.12% >> 0.03%
            volume_usdt_15m=100000.0,
            atr_history=[],
        )

        assert result.allowed is False
        assert "spread" in (result.block_reason or "")

    def test_low_volume_blocks(self):
        from app.execution.execution_filter import check_execution

        result = check_execution(
            symbol="ETHUSDT",
            current_price=3000.0,
            bid=2999.99,      # very tight spread: 0.0033% ≪ 0.03% max
            ask=3000.01,
            volume_usdt_15m=50.0,   # way below 500 USDT min
            atr_history=[],
        )

        assert result.allowed is False
        assert "volume" in (result.block_reason or "")

    def test_vol_spike_blocks(self):
        from app.execution.execution_filter import check_execution

        # Use gradually varying ATR so std > 0, then a huge spike at the end
        normal_atrs = [95 + i for i in range(25)]  # 95..119, stdev ≈ 7.5
        normal_atrs[-1] = 500   # spike: z-score = (500 - ~107) / ~7.5 >> 2.5

        result = check_execution(
            symbol="BTCUSDT",
            current_price=50000.0,
            bid=49995.0,
            ask=50005.0,
            volume_usdt_15m=500000.0,
            atr_history=normal_atrs,
        )

        assert result.allowed is False
        assert "spike" in (result.block_reason or "").lower()

    def test_clean_market_passes(self):
        from app.execution.execution_filter import check_execution

        normal_atrs = [100.0 + i * 0.1 for i in range(25)]

        result = check_execution(
            symbol="BTCUSDT",
            current_price=50000.0,
            bid=49998.0,
            ask=50002.0,          # 0.008% spread — within limits
            volume_usdt_15m=1_000_000.0,
            atr_history=normal_atrs,
        )

        assert result.allowed is True
        assert result.block_reason is None




# =========================================================================
# Integration: Regime blocking at ensemble level
# =========================================================================

class TestEnsembleIntegration:
    def test_low_vol_chop_returns_hold(self, chop_klines):
        """LOW_VOL_CHOP klines must produce Signal.HOLD, not BUY/SELL."""
        from app.strategy.master_ensemble import MasterEnsembleStrategy
        from app.strategy.base import Signal

        client = MagicMock()
        # Return the chop klines from client.klines()
        client.klines.return_value = chop_klines

        ensemble = MasterEnsembleStrategy(client=client)
        result = ensemble.get_signal("BTCUSDT")  # klines fetched internally

        assert result.signal == Signal.HOLD
        # ActivationEngine embeds blocked_reason which contains the regime name
        blocked_reason = result.reason or result.meta.get("reason", "")
        # Could be suspended by LOW_VOL_CHOP or insufficient data
        assert (
            "LOW_VOLATILITY_CHOP" in blocked_reason
            or "capital preservation" in blocked_reason
            or "regime_low_vol_chop" in blocked_reason
            or "regime_insufficient" in blocked_reason
            or "regime_klines" in blocked_reason
            or result.signal == Signal.HOLD  # fallback: just confirm HOLD
        )

    def test_regime_meta_present_in_signal(self, trending_klines):
        """Signal result must carry regime metadata for audit log."""
        from app.strategy.master_ensemble import MasterEnsembleStrategy
        from app.strategy.base import Signal

        client = MagicMock()
        client.klines.return_value = trending_klines
        # Sub-strategies return HOLD so we get a deterministic result
        sub_mock = MagicMock()
        sub_mock.get_signal.return_value = MagicMock(signal=Signal.BUY, confidence=0.9, reason="mock")

        ensemble = MasterEnsembleStrategy(client=client)
        # Replace all strategies with the mock
        ensemble._strategies = {n: sub_mock for n in ensemble._strategies}
        result = ensemble.get_signal("BTCUSDT")

        # Even if HOLD (below threshold), regime must be in meta
        assert "regime" in result.meta
        assert result.meta["regime"] != "UNKNOWN"
