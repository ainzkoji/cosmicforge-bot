import os
import sys
import math
import random
from collections import Counter
import logging

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from app.strategy.regime import RegimeClassifier, RegimeThresholds, MarketRegime

logging.basicConfig(level=logging.INFO, format="%(message)s")

def generate_chop_klines(count=500, base_price=50000.0, volatility=0.001):
    # Generates a tight, choppy market (no trend, low ATR)
    closes = []
    highs = []
    lows = []
    price = base_price
    random.seed(42) # Deterministic
    
    for i in range(count):
        # Random walk with mean reversion to base_price
        diff = base_price - price
        change = diff * 0.05 + random.uniform(-base_price*volatility, base_price*volatility)
        
        c = price + change
        h = max(price, c) + random.uniform(0, base_price*volatility*0.5)
        l = min(price, c) - random.uniform(0, base_price*volatility*0.5)
        
        closes.append(c)
        highs.append(h)
        lows.append(l)
        price = c
        
    return highs, lows, closes

def main():
    print("Testing LOW_VOLATILITY_CHOP on synthetic sideways data...")
    highs, lows, closes = generate_chop_klines(1000, 50000.0, 0.0008)  # 0.08% candle volatility
    
    # Try with default thresholds
    classifier = RegimeClassifier()
    regimes = []
    
    for i in range(250, len(closes)):
        h = highs[i-250:i]
        l = lows[i-250:i]
        c = closes[i-250:i]
        res = classifier.classify_stable(h, l, c)
        regimes.append(res.regime.value)
        
    counter = Counter(regimes)
    total = sum(counter.values())
    
    print("\n--- Default Thresholds Distribution ---")
    for regime, count in counter.most_common():
        pct = (count / total) * 100
        print(f"{regime:20s}: {count:4d} ({pct:5.1f}%)")
        
    # We want LOW_VOLATILITY_CHOP to be the vast majority here.
    chop_pct = counter.get(MarketRegime.LOW_VOLATILITY_CHOP.value, 0) / total * 100
    if chop_pct >= 80.0:
        print("\n✅ Verification SUCCESS: LOW_VOL_CHOP trips correctly on sideways sessions (>80%)")
    else:
        print("\n❌ Verification FAILED: Not enough LOW_VOL_CHOP trips. Need to calibrate thresholds.")
        
if __name__ == "__main__":
    main()
