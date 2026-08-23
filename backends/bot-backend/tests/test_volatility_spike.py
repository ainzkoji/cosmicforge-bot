import unittest
from unittest.mock import MagicMock
from app.strategy.master_ensemble import MasterEnsembleStrategy
from app.strategy.regime import MarketRegime

class TestRegimeAwareSafety(unittest.TestCase):
    def setUp(self):
        # Initialize strategy with minimal dependencies
        # We only need it to calculate spike detection
        # MasterEnsembleStrategy takes (client, interval, ...)
        mock_client = MagicMock()
        self.strategy = MasterEnsembleStrategy(
            client=mock_client,
            interval="15m"
        )

    def test_spike_range_regime(self):
        """Test strict (2.0x) multiplier in RANGE regime."""
        # 23 base candles (we need 20 for rolling window)
        klines = [
            [0, 100, 110, 100, 105, 0, 0, 0, 0, 0, 0] for _ in range(23)
        ]
        # 3 short-term candles with range of 21 (2.1x avg)
        for _ in range(3):
            klines.append([0, 100, 121, 100, 110, 0, 0, 0, 0, 0, 0])
        
        is_spike, mult = self.strategy._check_volatility_spike("BTCUSDT", klines, MarketRegime.RANGE)
        self.assertTrue(is_spike, f"Expected spike at 2.1x in RANGE (threshold 2.0x). Mult used: {mult}")
        self.assertEqual(mult, 2.0)

    def test_no_spike_high_vol_regime(self):
        """Test permissive (4.5x) multiplier in HIGH_VOLATILITY regime."""
        # 23 base candles (we need 20 for rolling window)
        klines = [
            [0, 100, 110, 100, 105, 0, 0, 0, 0, 0, 0] for _ in range(23)
        ]
        # 3 short-term candles with range of 40 (4.0x avg)
        for _ in range(3):
            klines.append([0, 100, 140, 100, 120, 0, 0, 0, 0, 0, 0])
        
        is_spike, mult = self.strategy._check_volatility_spike("BTCUSDT", klines, MarketRegime.HIGH_VOLATILITY)
        self.assertFalse(is_spike, f"Expected NO spike at 4.0x in HIGH_VOLATILITY (threshold 4.5x). Mult used: {mult}")
        self.assertEqual(mult, 4.5)

    def test_spike_strong_trend_regime(self):
        """Test moderate (3.5x) multiplier in STRONG_TREND regime."""
        klines = [[0, 100, 110, 100, 105, 0, 0, 0, 0, 0, 0]] * 23
        # 3 short-term candles with range of 36 (3.6x avg)
        for _ in range(3):
            klines.append([0, 100, 136, 100, 115, 0, 0, 0, 0, 0, 0])
        
        is_spike, mult = self.strategy._check_volatility_spike("BTCUSDT", klines, MarketRegime.STRONG_TREND)
        self.assertTrue(is_spike, f"Expected spike at 3.6x in STRONG_TREND (threshold 3.5x).")
        self.assertEqual(mult, 3.5)

    def test_insufficient_data(self):
        klines = [[0] * 11] * 25
        is_spike, mult = self.strategy._check_volatility_spike("BTCUSDT", klines, MarketRegime.RANGE)
        self.assertFalse(is_spike)

if __name__ == "__main__":
    unittest.main()
