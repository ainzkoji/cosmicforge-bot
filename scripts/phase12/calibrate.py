"""Find a deterministic market the real regime gate will actually trade.

The first synthetic series was a clean monotone drift, which the real
RegimeClassifier scored ADX 100 / STRONG_TREND — a blocked regime. Blocking it
was correct behaviour, so the fix is a better market, not a weaker gate.

This runs the *real* classifier over candidate parameter sets and prints what
each one is classified as, so the harness can be pinned to one that the
production gate is willing to trade.
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from scripts.phase12.bootstrap import bootstrap  # noqa: E402

bootstrap(None)

from scripts.phase12.market import build_candles  # noqa: E402

# Regime classification depends on the series shape, not on where it
# sits on the clock, so a fixed anchor keeps this sweep reproducible.
START_MS = 1_700_000_000_000


def classify(rows):
    from app.strategy.master_ensemble import MasterEnsembleStrategy

    ensemble = MasterEnsembleStrategy.__new__(MasterEnsembleStrategy)
    ensemble._regime_classifiers = {}
    ensemble.interval = "15m"
    classifier = ensemble._get_classifier("BTCUSDT")
    highs = [float(r[2]) for r in rows]
    lows = [float(r[3]) for r in rows]
    closes = [float(r[4]) for r in rows]
    return classifier.classify_stable(highs, lows, closes)


def main() -> int:
    print(f"{'drift':>9} {'wave':>7} {'period':>7} | {'regime':<20} {'adx':>7} "
          f"{'atr%':>7} {'slope':>8} {'conf':>6}")
    print("-" * 82)
    best = []
    for drift in (0.0, 0.00005, 0.0001, 0.0002, 0.0004):
        for wave in (0.004, 0.010, 0.020, 0.035):
            for period in (7.0, 11.0, 19.0):
                rows = build_candles(
                    start_ms=START_MS, drift=drift, wave=wave, wave_period=period,
                )
                try:
                    result = classify(rows)
                except Exception as exc:
                    print(f"{drift:9.5f} {wave:7.3f} {period:7.1f} | ERROR {exc}")
                    continue
                regime = result.regime.value
                print(f"{drift:9.5f} {wave:7.3f} {period:7.1f} | {regime:<20} "
                      f"{result.adx:7.1f} {result.atr_percent:7.3f} "
                      f"{result.ma_slope:8.4f} {result.regime_confidence:6.2f}")
                if regime not in ("STRONG_TREND", "UNKNOWN"):
                    best.append((drift, wave, period, regime, result.adx))

    print()
    if best:
        print("Tradeable candidates (regime not blocked):")
        for row in best[:15]:
            print("  drift=%.5f wave=%.3f period=%.1f -> %s (adx=%.1f)" % row)
    else:
        print("No tradeable candidate found in this grid.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
