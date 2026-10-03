"""Fixed, research-only four-mechanism alpha budget; no runtime registration."""
from __future__ import annotations
from dataclasses import asdict, dataclass
import numpy as np
import pandas as pd
from app.trading_intelligence.setups.alpha_v1 import AlphaCosts, closed_features as base_features
from app.trading_intelligence.contracts.setup import SetupCandidate
from app.trading_intelligence.hashing import stable_hash

GENERATOR_ID = 'CATI_ALPHA_CAUSAL_DIVERSIFIED_V2'
LABEL_VERSION = 'CATI_ALPHA_NEXT_OPEN_GAP_AWARE_V2'
COST_VERSION = 'CATI_ALPHA_CONSERVATIVE_INHERITED_COST_V2'

@dataclass(frozen=True)
class AlphaHypothesis:
    family: str
    timeframe: str = '15m'
    horizon: int = 48
    maximum_cost_R: float = .15
    minimum_risk_fraction: float = .003
    atr_stop_multiple: float = 1.5
    minimum_target_R: float = 1.25
    @property
    def setup_version(self):
        return self.family+'_V2_15m_H48'
    @property
    def policy_hash(self):
        return stable_hash(dict(generator=GENERATOR_ID, **asdict(self)))

HYPOTHESES = tuple(AlphaHypothesis(f) for f in (
    'CROSS_SECTIONAL_RELATIVE_STRENGTH', 'VOLATILITY_TRANSITION_CONTINUATION',
    'MULTI_TIMEFRAME_TREND_PULLBACK', 'FLOW_CONFIRMED_DIRECTIONAL'))


def aggregate_complete(rows, hours, stop):
    """Only exact complete contiguous 15m groups, closed by the cutoff."""
    step = hours*3600000
    groups = {}
    for r in rows:
        groups.setdefault(r[0]//step*step, []).append(r)
    return [(t, c[0][1], max(r[2] for r in c), min(r[3] for r in c),
             c[-1][4], sum(r[5] for r in c)) for t,c in sorted(groups.items())
            if len(c)==hours*4 and [r[0] for r in c]==[t+i*900000 for i in range(hours*4)]
            and t+step-1<=stop]


def closed_features(rows, timeframe, decision_time):
    f = base_features(rows, timeframe, decision_time)
    f['return_96'] = f.close/f.close.shift(96)-1
    f['continuous_97'] = f.open_time.diff().eq(900000).rolling(96).sum().eq(96)
    f['atr_previous'] = f.atr.shift(1)
    f['atr_baseline'] = f.atr.shift(8).rolling(48).mean()
    f['efficiency'] = (f.close-f.close.shift(12)).abs()/f.close.diff().abs().rolling(12).sum()
    f['body_atr'] = (f.close-f.open)/f.atr
    f['range_high'] = f.high.shift(1).rolling(96).max()
    f['range_low'] = f.low.shift(1).rolling(96).min()
    return f


def htf_context(rows, hours, stop, times):
    derived = aggregate_complete(rows, hours, stop)
    names = ['continuous', 'fast', 'slow', 'slope', 'closed_at']
    if not derived:
        return pd.DataFrame(np.nan, index=range(len(times)), columns=names)
    h = base_features(derived, f'{hours}h', stop)
    indices = np.searchsorted(h.closed_at.to_numpy(), times, side='right')-1
    result = h.iloc[np.maximum(indices,0)][names].reset_index(drop=True).astype(float)
    # Closed-bar asof is valid only until its next expected close. Gaps/stale
    # context fail closed; no indefinite forward fill.
    valid = (indices>=0)&(times-result.closed_at.to_numpy()<hours*3600000)
    result.loc[~valid,:] = np.nan
    return result


def benchmark_align(frame, benchmark):
    return benchmark.set_index('closed_at').reindex(frame.closed_at).reset_index(drop=True)


def signal_mask(f, h, btc, eth, basket, one, four):
    b,e = benchmark_align(f,btc), benchmark_align(f,eth)
    valid = f.continuous_97 & b.continuous_97.eq(True) & e.continuous_97.eq(True)
    sign = np.where(f.momentum>=0,1.,-1.)
    if h.family=='CROSS_SECTIONAL_RELATIVE_STRENGTH':
        valid &= np.isfinite(basket) & (sign*(f.return_96-b.return_96)>.01)
        valid &= (sign*(f.return_96-e.return_96)>.01)&(sign*(f.return_96-basket)>.015)
        trigger = (sign*f.momentum>.002)&(sign*f.body_atr>.35)&(f.volume_ratio>=1.2)
    elif h.family=='VOLATILITY_TRANSITION_CONTINUATION':
        # Volatility regime crossing and efficient direction, no V1 breakout
        # or two-bar acceptance condition.
        trigger = (f.atr_previous<=.85*f.atr_baseline)&(f.atr>1.1*f.atr_baseline)
        trigger &= (f.efficiency>.5)&(sign*f.body_atr>.6)&(f.volume_ratio>=1.5)
    elif h.family=='MULTI_TIMEFRAME_TREND_PULLBACK':
        sign = np.where(f.fast>f.slow,1.,-1.)
        valid &= one.continuous.eq(True)&four.continuous.eq(True)
        valid &= (sign*(one.fast-one.slow)>0)&(sign*(four.fast-four.slow)>0)
        valid &= (sign*one.slope>.25)&(sign*four.slope>.25)
        depth = np.where(sign>0,(f.fast-f.previous_low)/f.atr,(f.previous_high-f.fast)/f.atr)
        reclaim = np.where(sign>0,(f.previous_close<=f.previous_fast)&(f.close>f.fast),
                           (f.previous_close>=f.previous_fast)&(f.close<f.fast))
        trigger = reclaim&(depth>=.25)&(depth<=1.5)&(sign*f.body_atr>.25)&(f.volume_ratio>=1.1)
    elif h.family=='FLOW_CONFIRMED_DIRECTIONAL':
        # Only measured candle volume is available. No fabricated OI/funding,
        # basis, signed trade flow or orderbook confirmations.
        trigger = (f.volume_ratio>=2)&(f.efficiency>.4)&(sign*f.body_atr>.75)
        trigger &= sign*f.momentum>.002
    else:
        raise ValueError('undeclared family')
    return np.asarray(valid & trigger & (f.atr>0),bool), sign


def candidate_at(f,index,h,instrument,source_hash,sign,costs=AlphaCosts()):
    r=f.iloc[index]; entry=float(r.close)
    structural = entry-r.swing_low if sign>0 else r.swing_high-entry
    risk=max(structural+.25*r.atr,h.atr_stop_multiple*r.atr,h.minimum_risk_fraction*entry)
    # Target is a measured prior 96-bar structural extreme; no synthetic
    # reward multiple can manufacture room. Empty room rejects the signal.
    target=float(r.range_high if sign>0 else r.range_low)
    room=sign*(target-entry)
    cost=sum(costs.fractions(h).values())*entry/risk
    if not np.isfinite([risk,target,cost]).all() or cost>h.maximum_cost_R or room/risk<h.minimum_target_R or entry-sign*risk<=0 or target<=0:
        return None
    return SetupCandidate.build(market_state_id=stable_hash(dict(source=source_hash,close=int(r.closed_at))),
        snapshot_id=stable_hash(dict(source=source_hash,index=int(index))),data_hash=source_hash,
        instrument_key=instrument,timeframe=h.timeframe,decision_time=int(r.closed_at),
        setup_family=h.family,setup_version=h.setup_version,
        setup_policy_hash=stable_hash(dict(hypothesis=h.policy_hash,cost_version=COST_VERSION,costs=asdict(costs))),
        side='LONG' if sign>0 else 'SHORT',trigger_reference=entry,
        structural_invalidation=float(entry-sign*risk),target_reference=target,
        geometry_features=dict(estimated_cost_R=float(cost),atr=float(r.atr),initial_risk_fraction=float(risk/entry),horizon_bars=h.horizon),
        evidence_components={n:float(r[n]) for n in ('return_96','momentum','volume_ratio','efficiency','body_atr')},validity_bars=1)

