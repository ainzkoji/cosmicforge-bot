"""
Tests for Phase 5 Asymmetric EMA smoothing utility.
"""

from app.adaptive.smoothing import AsymmetricEMA


def test_asymmetric_ema_initialization():
    ema = AsymmetricEMA(alpha_up=0.3, alpha_down=0.1)
    assert ema.value is None
    
    # First update should snap exactly to target (deterministic cold start)
    assert ema.update(0.5) == 0.5
    assert ema.value == 0.5


def test_asymmetric_ema_upward_movement():
    ema = AsymmetricEMA(alpha_up=0.5, alpha_down=0.1)
    ema.update(1.0)
    
    # Target goes UP to 2.0 -> should use alpha_up (0.5)
    # EMA = (2.0 * 0.5) + (1.0 * 0.5) = 1.5
    res = ema.update(2.0)
    assert res == 1.5


def test_asymmetric_ema_downward_movement():
    ema = AsymmetricEMA(alpha_up=0.1, alpha_down=0.5)
    ema.update(2.0)
    
    # Target goes DOWN to 1.0 -> should use alpha_down (0.5)
    # EMA = (1.0 * 0.5) + (2.0 * 0.5) = 1.5
    res = ema.update(1.0)
    assert res == 1.5


def test_asymmetric_smoothing_size_logic():
    # Size logic: drops fast (0.5), recovers slow (0.05)
    ema = AsymmetricEMA(alpha_up=0.05, alpha_down=0.5)
    ema.update(1.0)  # Starts at 1.0
    
    # Drawdown hit! Size target drops to 0.40
    # Downward movement uses alpha_down = 0.5
    # EMA = (0.40 * 0.5) + (1.0 * 0.5) = 0.20 + 0.50 = 0.70
    res = ema.update(0.40)
    assert round(res, 2) == 0.70
    
    # Next tick, still in drawdown (target 0.40)
    # Target (0.40) < Current (0.70) -> Downward movement -> alpha_down = 0.5
    # EMA = (0.40 * 0.5) + (0.70 * 0.5) = 0.20 + 0.35 = 0.55
    res2 = ema.update(0.40)
    assert round(res2, 2) == 0.55
    
    # Drawdown recovers! Size target goes back to 1.0
    # Upward movement uses alpha_up = 0.05
    # EMA = (1.0 * 0.05) + (0.55 * 0.95) = 0.05 + 0.5225 = 0.5725
    res3 = ema.update(1.0)
    assert round(res3, 4) == 0.5725
    
    # As we can see, the size drops swiftly from 1.0 to 0.55 in two ticks,
    # but recovers at a snail's pace (0.55 to 0.57) to enforce asymmetry.
