"""Versioned CATI research generator. No runtime registry or order authority.

Features are rolling closed-bar prefixes. Future paths belong exclusively to
the research evaluator, never to this module.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
import math

import numpy as np
import pandas as pd

from app.trading_intelligence.contracts.setup import SetupCandidate, timeframe_to_ms
from app.trading_intelligence.hashing import stable_hash

GENERATOR_ID = "CATI_ALPHA_CONFIRMED_STRUCTURE_V1"
LABEL_VERSION = "CATI_ALPHA_NEXT_OPEN_GAP_AWARE_V1"


@dataclass(frozen=True)
class AlphaHypothesis:
    family: str
    timeframe: str
    horizon: int
    maximum_cost_R: float = .15
    minimum_risk_fraction: float = .003
    atr_stop_multiple: float = 1.5
    target_atr_multiple: float = 3.
    minimum_target_R: float = 1.25

    @property
    def setup_version(self):
        return f"{self.family}_V1_{self.timeframe}_H{self.horizon}"

    @property
    def policy_hash(self):
        return stable_hash({"generator": GENERATOR_ID, **asdict(self)})


# Two mechanisms, three independently identified time domains, no grid search.
HYPOTHESES = tuple(AlphaHypothesis(family, timeframe, horizon)
                   for timeframe, horizon in (("5m", 72), ("15m", 32), ("1h", 16))
                   for family in ("BREAKOUT_ACCEPTANCE", "TREND_RECLAIM"))


@dataclass(frozen=True)
class AlphaCosts:
    """Inherited audit assumptions, plus conservative funding-stamp rounding.

Historical observations are unavailable: these are modeled assumptions,
not assertions about a user's fee tier or currently obtainable execution.
"""
    fee: float = .0004
    half_spread: float = .0001
    slippage: float = .0002
    funding_per_8h: float = .0001

    def __post_init__(self):
        if any(not math.isfinite(v) or v < 0 for v in asdict(self).values()):
            raise ValueError("cost rates must be finite and nonnegative")

    def fractions(self, hypothesis):
        duration = timeframe_to_ms(hypothesis.timeframe) * hypothesis.horizon
        return dict(fee=2*self.fee, spread=2*self.half_spread,
                    slippage=2*self.slippage,
                    funding=self.funding_per_8h*math.ceil(duration/28_800_000))


def closed_features(rows, timeframe, decision_time):
    """Rows: open_time, open, high, low, close, volume; exact closed timestamps.

    A gap invalidates the next 80-bar warmup, rather than bridging structure
    across missing data. Invalid OHLCV or future input fails closed.
    """
    step = timeframe_to_ms(timeframe)
    if not step or not rows:
        raise ValueError("known timeframe and nonempty closed history required")
    f = pd.DataFrame(rows, columns=("open_time", "open", "high", "low", "close", "volume"))
    a = f.to_numpy(dtype=float)
    if (not np.isfinite(a).all() or (a[:, 1:5] <= 0).any() or (a[:, 5] < 0).any()
            or (f.high < f[["open", "close", "low"]].max(axis=1)).any()
            or (f.low > f[["open", "close", "high"]].min(axis=1)).any()
            or (np.diff(a[:, 0]) <= 0).any()
            or (a[:, 0] % step != 0).any()
            or a[-1, 0] + step - 1 > decision_time):
        raise ValueError("invalid, unordered, unaligned or future OHLCV")
    f["closed_at"] = f.open_time.astype("int64") + step - 1
    prior = f.close.shift(1)
    tr = pd.concat((f.high-f.low, (f.high-prior).abs(), (f.low-prior).abs()), axis=1).max(axis=1)
    f["atr"] = tr.rolling(14).mean()
    f["fast"] = f.close.rolling(12).mean()
    f["slow"] = f.close.rolling(48).mean()
    f["slope"] = (f.slow-f.slow.shift(8))/f.atr
    f["momentum"] = f.close/f.close.shift(12)-1
    f["volume_ratio"] = f.volume/f.volume.shift(1).rolling(24).mean()
    f["prior_high"] = f.high.shift(2).rolling(24).max()
    f["prior_low"] = f.low.shift(2).rolling(24).min()
    f["previous_close"] = f.close.shift(1)
    f["previous_low"] = f.low.shift(1)
    f["previous_high"] = f.high.shift(1)
    f["previous_fast"] = f.fast.shift(1)
    f["swing_low"] = f.low.rolling(8).min()
    f["swing_high"] = f.high.rolling(8).max()
    f["compression"] = tr.shift(2).rolling(8).mean()/tr.shift(10).rolling(24).mean()
    f["displacement"] = (f.close-f.open).abs()/f.atr
    f["continuous"] = f.open_time.diff().eq(step).rolling(79).sum().eq(79)
    return f


def signal_mask(f, hypothesis, btc, eth):
    """Both benchmarks must exist at the exact decision close, no as-of fill."""
    t = pd.Index(f.closed_at)
    b = btc.set_index("closed_at").reindex(t)
    e = eth.set_index("closed_at").reindex(t)
    aligned = b.continuous.to_numpy() == True
    aligned &= e.continuous.to_numpy() == True
    sign = np.where(f.fast > f.slow, 1., -1.)
    common = (f.continuous & (sign*f.slope >= .25) & (sign*f.momentum > 0)
              & (f.volume_ratio >= 1.1) & ((f.close-f.fast).abs() <= 2*f.atr))
    common &= aligned & (sign*b.momentum.to_numpy() >= 0) & (sign*e.momentum.to_numpy() >= 0)
    if hypothesis.family == "BREAKOUT_ACCEPTANCE":
        long = (f.previous_close > f.prior_high) & (f.close > f.prior_high) & (f.low >= f.prior_high-.25*f.atr)
        short = (f.previous_close < f.prior_low) & (f.close < f.prior_low) & (f.high <= f.prior_low+.25*f.atr)
        trigger = np.where(sign > 0, long, short) & (f.compression <= .85) & (f.displacement >= .25)
    elif hypothesis.family == "TREND_RECLAIM":
        long = (f.previous_low <= f.previous_fast) & (f.close > f.previous_high) & (f.close > f.open)
        short = (f.previous_high >= f.previous_fast) & (f.close < f.previous_low) & (f.close < f.open)
        trigger = np.where(sign > 0, long, short) & (f.displacement >= .35)
    else:
        raise ValueError("undeclared family")
    return np.asarray(common & trigger & (f.atr > 0), dtype=bool)


def candidate_at(f, index, hypothesis, instrument, source_hash, costs=AlphaCosts()):
    """Called only for confirmed signals. Structural + ATR stop; reject costs first."""
    r = f.iloc[index]
    sign = 1 if r.fast > r.slow else -1
    entry = float(r.close)
    structural_risk = entry-r.swing_low if sign > 0 else r.swing_high-entry
    risk = max(structural_risk+.25*r.atr, hypothesis.atr_stop_multiple*r.atr)
    target_distance = hypothesis.target_atr_multiple*r.atr
    cost_R = sum(costs.fractions(hypothesis).values())*entry/risk
    if (risk/entry < hypothesis.minimum_risk_fraction or cost_R > hypothesis.maximum_cost_R
            or target_distance/risk < hypothesis.minimum_target_R or entry-sign*risk <= 0
            or entry+sign*target_distance <= 0):
        return None
    return SetupCandidate.build(
        market_state_id=stable_hash({"source": source_hash, "close": int(r.closed_at)}),
        snapshot_id=stable_hash({"source": source_hash, "index": int(index)}), data_hash=source_hash,
        instrument_key=instrument, timeframe=hypothesis.timeframe, decision_time=int(r.closed_at),
        setup_family=hypothesis.family, setup_version=hypothesis.setup_version,
        setup_policy_hash=stable_hash({"hypothesis": hypothesis.policy_hash, "costs": asdict(costs)}),
        side="LONG" if sign > 0 else "SHORT", trigger_reference=entry,
        structural_invalidation=float(entry-sign*risk), target_reference=float(entry+sign*target_distance),
        geometry_features={"estimated_cost_R": float(cost_R), "atr": float(r.atr),
                           "initial_risk_fraction": float(risk/entry), "horizon_bars": hypothesis.horizon},
        evidence_components={name: float(r[name]) for name in
                             ("slope", "momentum", "volume_ratio", "compression", "displacement")},
        validity_bars=1)
