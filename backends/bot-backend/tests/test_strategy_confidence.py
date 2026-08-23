
import pytest
from unittest.mock import MagicMock
from app.strategy.sma_cross import SMACrossStrategy
from app.policy.policy_engine import calculate_atr
from app.strategy.bollinger_reversion import BollingerReversionStrategy
from app.strategy.base import SignalResult, Signal

# Mocks
class MockClient:
    def klines(self, symbol, interval, limit):
        return []

@pytest.fixture
def mock_client():
    return MockClient()

# --- SMA Cross Tests ---

def test_atr_calculation():
    # Constant range of 10: high=110, low=100, close=100 or 110 each candle
    # TR for each: max(high-low, |high-prev_close|, |low-prev_close|)
    # h=110, l=100, c=100: TR = max(10, |110-prev|, |100-prev|)
    # With alternating closes 100/110 the TR will vary; simpler: use flat range
    # Use klines: [open, open, high, low, close, vol]
    klines = [[0, 0, 110, 100, 100, 0]] * 7  # 7 candles with range 10
    atr = calculate_atr(klines, period=4)
    # Every TR = 10 (high-low), ATR = 10
    assert atr == pytest.approx(10.0, abs=0.01)
    
    # Only 1 kline -> 0 (needs at least period+1)
    assert calculate_atr([[0, 0, 110, 100, 100, 0]], period=14) == 0.0

def test_sma_cross_confidence_calculation(mock_client):
    strat = SMACrossStrategy(mock_client, fast=2, slow=5, min_confidence=0.1)
    
    # Create synthetic data
    # ATR approx 1.0 (alternating 100, 101)
    # Fast MA (last 2): will be high
    # Slow MA (last 5): will be lower
    closes = [100.0, 101.0] * 50 # 100 elements
    # Last elements: 100, 101, 100, 101
    
    # Create synthetic data for a CROSSOVER
    # Need Fast <= Slow at T-1, and Fast > Slow at T
    # Fast=2, Slow=5
    
    # Base: 100.0 for 50 candles
    closes = [100.0] * 50
    
    # At end, jump up to trigger cross
    # T-1: [..., 100, 100, 100, 100, 100] -> Fast=100, Slow=100 -> Cross condition met (Fast <= Slow)
    # T:   [..., 100, 100, 100, 100, 110] 
    # Fast (2): (100+110)/2 = 105
    # Slow (5): (100+100+100+100+110)/5 = 102
    # Fast > Slow -> BUY Signal
    
    closes[-1] = 110.0
    
    # Mock klines return
    klines = [[0, 0, 0, 0, c, 0] for c in closes]
    mock_client.klines = MagicMock(return_value=klines)
    
    result = strat.get_signal("BTCUSDT")
    
    assert result.signal == Signal.BUY
    # Confidence:
    # ATR approx: 10/14 = 0.71 (last change was 10, rest 0)
    # Separation: 105 - 102 = 3.0
    # Strength: 3.0 / (0.71 * 2) = 2.1 > 1.0 -> 1.0
    # Conf: 0.10 + 0.65 = 0.75
    assert result.confidence > 0.6
    assert "sma_cross" in result.reason  # strategy returns 'sma_cross' for both directions

# --- Bollinger Reversion Tests ---

def test_bollinger_confidence_calculation(mock_client):
    strat = BollingerReversionStrategy(mock_client, min_confidence=0.1)
    
    # Synthetic data for Bullish Reversion
    # Needs:
    # 1. Below lower band (percent_b < 0.05)
    # 2. RSI oversold (< 30)
    # 3. Bullish reversal candle
    
    # Create a sequence where bands are wide and price drops hard then bounces
    # We'll just mock the internal helper results or construct careful data
    # Easier: Construct data where:
    # SMA=100, StdDev=2 -> Lower=96
    # Close=94 -> %B = (94-96)/(104-96) = -2/8 = -0.25 (Extreme!)
    
    # To get this, we need a stable history then a drop
    closes = [100.0] * 50
    closes[-2] = 93.0 # Previous close low
    closes[-1] = 94.0 # Current close higher (reversal)
    
    # High/Low for candle strength
    # Open for reversal check
    opens = [100.0] * 50
    opens[-1] = 93.5 # Open below close (Bullish candle: 93.5 -> 94.0)
    
    highs = [105.0] * 50
    highs[-1] = 94.5
    lows = [95.0] * 50
    lows[-1] = 93.0
    
    klines = []
    for i in range(50):
        klines.append([0, opens[i], highs[i], lows[i], closes[i], 0])
        
    mock_client.klines = MagicMock(return_value=klines)
    
    # Note: bollinger indicators are calc'd inside. 
    # With [100...100, 93, 94], SMA will be slightly < 100. StdDev will be non-zero.
    # Let's hope the math works out to valid %B and RSI.
    # RSI: flat then drop -> RSI will be low.
    
    result = strat.get_signal("BTCUSDT")
    
    # If parameters align, we should get BUY
    # If math is tricky to mock perfectly with simple list, check if we get at least valid result structure
    
    # To get percent_b < 0.05, close must be near/below lower band.
    # To get RSI < 30, price must drop significantly.
    
    # We'll monkeypatch the helper functions for precise control without constructing complex price history
    from app.strategy import bollinger_reversion as br
    
    # Save originals
    orig_bb = br.calculate_bollinger_bands
    orig_rsi = br.calculate_rsi
    orig_pb = br.calculate_bb_percent_b
    
    try:
        # Mock helpers to return "perfect" conditions
        # Upper=110, Mid=100, Lower=90
        # Close=89 (below lower) -> %B < 0
        # RSI=20 (Oversold)
        br.calculate_bollinger_bands = MagicMock(return_value=([110.0]*50, [100.0]*50, [90.0]*50))
        br.calculate_rsi = MagicMock(return_value=[20.0]*50)
        br.calculate_bb_percent_b = MagicMock(return_value=-0.05) # Extreme!
        
        # Mock klines to support candle pattern check
        # Bullish candle: Close > Open
        # Reversal: Close > Prev Close
        
        # Prev Close needs to be lower than current close (94)
        # Prev: Open=94, Close=92 (Bearish)
        # Curr: Open=92, Close=94 (Bullish Engulfing-ish)
        
        klines = []
        for i in range(48):
            klines.append([0, 100, 105, 95, 100, 0])
            
        klines.append([0, 94.0, 95.0, 91.0, 92.0, 0]) # Prev
        klines.append([0, 92.0, 95.0, 88.0, 94.0, 0]) # Curr
        
        mock_client.klines = MagicMock(return_value=klines)
        
        result = strat.get_signal("BTCUSDT")
        
        assert result.signal == Signal.BUY
        assert result.confidence > 0.4 # Should be high with these parameters
        assert "bollinger_reversion" in result.reason
        
    finally:
        # Restore
        br.calculate_bollinger_bands = orig_bb
        br.calculate_rsi = orig_rsi
        br.calculate_bb_percent_b = orig_pb
