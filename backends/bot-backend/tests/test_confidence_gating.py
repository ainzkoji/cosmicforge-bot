"""
Unit tests for strategy confidence calculations.
"""
import pytest
from unittest.mock import Mock, MagicMock

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.strategy.supertrend import SuperTrendStrategy
from app.strategy.squeeze_breakout import SqueezeBreakoutStrategy
from app.strategy.base import Signal


class TestStrategyConfidence:
    """Test suite for strategy confidence score generation."""
    
    @pytest.fixture
    def mock_client(self):
        return Mock()
    
    def test_supertrend_low_confidence_signal(self, mock_client):
        """Test SuperTrend generates lower confidence signal correctly during weak slope."""
        strategy = SuperTrendStrategy(
            client=mock_client,
            min_confidence=0.3
        )
        
        # 1. Create data that establishes a BEARISH trend first
        closes = [100.0] * 100
        # Drop price to 90 to ensure bearish ST
        for i in range(100, 150):
            closes.append(100.0 - (i-100)*0.2)
        
        # 2. Reversal to BULLISH, but with MODERATE slope
        last_val = closes[-1]
        for i in range(150, 200):
            closes.append(last_val + (i-150)*0.1) # Moderate rise
            
        klines = []
        for c in closes:
            klines.append([0, 0, c+2, c-2, c, 0])
            
        mock_client.klines.return_value = klines
        
        result = strategy.get_signal("BTCUSDT")
        
        if result.signal == Signal.BUY:
            assert result.confidence >= 0.3
            assert result.confidence < 0.8  # Should not be extremely high
        else:
            pytest.fail(f"Signal was {result.signal}, reason: {result.reason}, meta: {result.meta}")

    def test_supertrend_high_confidence_signal(self, mock_client):
        """Test SuperTrend generates high confidence signal correctly during steep slope."""
        strategy = SuperTrendStrategy(
            client=mock_client,
            min_confidence=0.3
        )
        
        closes = [100.0] * 100
        for i in range(100, 150):
            closes.append(100.0 - (i-100)*0.2)
            
        last_val = closes[-1]
        for i in range(150, 200):
            closes.append(last_val + (i-150)*1.0) # Huge rise
            
        klines = []
        for c in closes:
            klines.append([0, 0, c+2, c-2, c, 0])
            
        mock_client.klines.return_value = klines
        
        result = strategy.get_signal("BTCUSDT")
        
        assert result.signal == Signal.BUY
        assert result.confidence >= 0.7  # Should be higher due to steep slope

    def test_supertrend_rejection(self, mock_client):
        """Test SuperTrend REJECTS signal below min_confidence."""
        strategy = SuperTrendStrategy(
            client=mock_client,
            min_confidence=1.0  # Impossible threshold
        )
        
        closes = [100.0] * 100
        for i in range(100, 150):
            closes.append(100.0 - (i-100)*0.2)
        last_val = closes[-1]
        for i in range(150, 200):
            closes.append(last_val + (i-150)*0.1)
            
        klines = []
        for c in closes:
            klines.append([0, 0, c+2, c-2, c, 0])
        mock_client.klines.return_value = klines
        
        result = strategy.get_signal("BTCUSDT")
        
        assert result.signal == Signal.HOLD
        assert result.reason == "gated_low_confidence"

    def test_squeeze_breakout_gating_logic(self, mock_client):
        """Directly test _calculate_squeeze_history and gating logic."""
        strategy = SqueezeBreakoutStrategy(
            client=mock_client,
            min_confidence=0.3
        )
        
        closes = [100.0] * 100
        highs = [100.1] * 100
        lows = [99.9] * 100
        
        # Last 5 candles: EXPLODE
        for i in range(95, 100):
            closes[i] = 100.0 + (i-94)*2.0 # 102, 104...
            highs[i] = closes[i] + 0.1
            lows[i] = closes[i] - 0.1
            
        klines = []
        for i in range(100):
             klines.append([0, 0, highs[i], lows[i], closes[i], 0])
             
        mock_client.klines.return_value = klines
        
        result = strategy.get_signal("BTCUSDT")
        
        assert result.signal == Signal.BUY
        assert result.confidence >= 0.3


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
