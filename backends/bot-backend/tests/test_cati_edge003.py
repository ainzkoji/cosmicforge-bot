"""Synthetic tests never consume the registered historical evaluation budget."""
import importlib.util
from pathlib import Path
import sys
import numpy as np
import pytest
ROOT=Path(__file__).resolve().parents[3]
sys.path.insert(0,str(ROOT/'scripts'))
import evaluate_cati_edge003 as edge

@pytest.fixture
def registry():return edge.registry()

def test_frozen_registry_identity_universe_and_budget(registry):
    assert len(registry['source_hashes']['main_native15m_closed_prefix'])==136
    assert len(registry['families'][0]['universe'])==134
    assert len(registry['families'][2]['universe'])==35
    assert registry['maximum_evaluation_runs']==1

def test_complete_hour_rejects_partial_gap_and_future():
    rows=[(i*edge.Q,100,101,99,100,10) for i in range(8)]
    assert len(edge.aggregate_complete(rows,1,8*edge.Q-1))==2
    assert len(edge.aggregate_complete(rows[:2]+rows[3:],1,8*edge.Q-1))==1
    assert len(edge.aggregate_complete(rows,1,4*edge.Q-2))==0

def test_native_future_and_misaligned_rows_rejected():
    a=np.array([(0,100,101,99,100,10),(edge.Q,100,101,99,100,10)],float)
    edge.validate_native(a,2*edge.Q-1)
    with pytest.raises(ValueError):edge.validate_native(a,2*edge.Q-2)
    a[0,0]=1
    with pytest.raises(ValueError):edge.validate_native(a,2*edge.Q-1)

def test_strict_funding_and_staleness():
    a=np.array([(0,.01),(8*edge.H,.02),(16*edge.H,.03),(24*edge.H,.9)])
    result=edge.asof(a,24*edge.H,12*edge.H,strict=True,count=3)
    assert result[:,1].tolist()==[.01,.02,.03]
    assert edge.asof(a,40*edge.H,12*edge.H,strict=True,count=3) is None
    assert edge.asof(a,8*edge.H,12*edge.H,strict=True,count=3) is None

def test_basis_requires_exact_join_and_freshness():
    a=np.array([(edge.H-1,100.),(2*edge.H-1,101.)])
    fa={'X':{'mark_price':a,'index_price':a,'basis_bps':np.array([(edge.H-1,1.),(2*edge.H-1,2.)])}}
    assert edge.current_basis(fa,'X',2*edge.H-1)[0]==2
    fa['X']['basis_bps']=np.array([(edge.H-1,1.)])
    assert edge.current_basis(fa,'X',2*edge.H-1) is None
    assert edge.current_basis(fa,'X',5*edge.H) is None

def test_beta_ols_and_causal_prefix_are_exact():
    rng=np.random.default_rng(3);n=800
    returns=rng.normal(0,.01,(n,3));returns[:,2]=.2+0.6*returns[:,0]-.3*returns[:,1]+rng.normal(0,.002,n)
    close=100*np.exp(np.cumsum(returns,axis=0));times=np.arange(n)*edge.H+edge.H-1
    edge.CURRENT_SYMBOLS=('BTCUSDT','ETHUSDT','X')
    f=edge.rolling_features(close,close*1.01,close*.99,times)
    i=730;rr=np.log(close[1:]/close[:-1])
    window=rr[i-672:i];x=np.column_stack((np.ones(672),window[:,:2]));beta=np.linalg.lstsq(x,window[:,2],rcond=None)[0]
    assert f['beta_btc'][i,2]==pytest.approx(beta[1],abs=1e-9)
    assert f['beta_eth'][i,2]==pytest.approx(beta[2],abs=1e-9)
    residual=window[:,2]-x@beta
    assert f['score'][i,2]==pytest.approx(residual[-24:].sum()/(residual.std(ddof=1)*np.sqrt(24)),abs=1e-9)
    newer=close.copy();newer[i+1:]*=2
    g=edge.rolling_features(newer,newer*1.01,newer*.99,times)
    assert np.allclose(f['score'][:i+1],g['score'][:i+1],equal_nan=True)
    close[100,2]=np.nan
    h=edge.rolling_features(close,close*1.01,close*.99,times)
    assert np.isnan(h['score'][700,2])

def test_fold_purge_entire_horizon(registry):
    f=registry['fold_structure'][0];t=f['end_exclusive_ms']-48*edge.H
    assert edge.fold_for(t,48,registry) is None
    assert edge.fold_for(t-1,48,registry)==1
    assert edge.fold_for(registry['holdout_start_ms'],48,registry) is None

def test_portfolio_ignores_overlapping_attempts():
    rows=[{'label_id':'a','decision_time':0,'entry_time':1,'exit_time':100},
          {'label_id':'b','decision_time':50,'entry_time':51,'exit_time':70},
          {'label_id':'c','decision_time':100,'entry_time':101,'exit_time':150}]
    selected,rejected=edge.portfolio_replay(rows)
    assert [x['label_id'] for x in selected]==['a','c']
    assert rejected==['b']

def test_costs_use_both_legs_exit_turnover_and_full_funding_buffer(registry):
    cost,parts=edge.modeled_cost([100,100],[110,90],[.6,.4],.01,72,registry['cost_policy']['rates'])
    assert parts['funding_buffer']==pytest.approx(.09)
    assert cost==pytest.approx(2.02*.0007/.01+.09)

def candidate():
    return {'family':edge.FAMILIES[0],'decision_time':-1,'legs':['X'],'sign':1,'stop':90.,'target':125.,'risk':10.}

def native():
    return np.array([(i*edge.Q,100.,105.,95.,102.,10.) for i in range(192)])

def test_single_gap_entry_rejection_and_adverse_ambiguity(registry):
    a=native();a[0,1]=89
    assert edge.label_single(candidate(),a,registry['cost_policy']['rates'])[1]=='NON_EXECUTABLE_GAP'
    a=native();a[0,2]=130;a[0,3]=80
    x,reason=edge.label_single(candidate(),a,registry['cost_policy']['rates'])
    assert reason is None and x['terminal']=='STOP' and x['ambiguous_stop']
    assert x['gross_R']==-1

def test_deterministic_synthetic_replay(registry):
    a,b=edge.label_single(candidate(),native(),registry['cost_policy']['rates'])
    c,d=edge.label_single(candidate(),native(),registry['cost_policy']['rates'])
    assert edge.identity(a)==edge.identity(c)

def test_pair_hourly_close_never_infers_intrabar_touch(registry):
    times=np.arange(25)*edge.H+edge.H-1
    hourly={'times':times,'open':np.ones((25,2))*100,'close':np.ones((25,2))*100}
    f={'score':np.tile([0,3.],(25,1))}
    cand={'family':edge.FAMILIES[1],'index':0,'horizon':24,'decision_time':int(times[0]),'legs':['L','S'],'weights':[.5,.5],'risk':.01,'asset_indices':[0,1]}
    row,reason=edge.label_pair(cand,hourly,f,{},registry['cost_policy']['rates'])
    assert row['terminal']=='TIMEOUT' and row['gross_R']==0 and row['largest_one_leg_adverse_R']==0
    # Neither unrelated highs nor lows enter the paired label API.
    hourly['high']=np.ones((25,2))*200;hourly['low']=np.ones((25,2))
    other,_=edge.label_pair(cand,hourly,f,{},registry['cost_policy']['rates'])
    assert other['gross_R']==row['gross_R']

def test_pooled_success_cannot_hide_failed_fold():
    good={'gross_R':.2,'net_lower_95_R':.1}
    folds=[good]*4+[{'gross_R':.2,'net_lower_95_R':-.01}]
    gate,failures=edge.economic_gate(folds,{'net_2x_cost_R':.5})
    assert gate=='FAIL' and failures==['FOLD_5_NET_DAY_CLUSTERED_LCB_NOT_POSITIVE']


def test_carry_cashflows_and_price_basis_decomposition(registry):
    times=np.arange(73)*edge.H+edge.H-1
    hourly={'times':times,'open':np.ones((73,2))*100,'close':np.ones((73,2))*100}
    feature_arrays={}
    for leg,mark,basis,rate in [('L',99.9,-10.,-.001),('S',100.1,10.,.001)]:
        feature_arrays[leg]={'mark_price':np.column_stack((times,np.ones(73)*mark)),
                             'index_price':np.column_stack((times,np.ones(73)*100)),
                             'basis_bps':np.column_stack((times,np.ones(73)*basis)),
                             'funding_rate':np.array([(i*edge.H,rate) for i in range(0,73,8)])}
    cand={'family':edge.FAMILIES[2],'index':0,'horizon':72,'decision_time':int(times[0]),'legs':['L','S'],'weights':[.5,.5],'risk':.01,'asset_indices':[0,1],'entry_basis':[-10.,10.]}
    row,reason=edge.label_pair(cand,hourly,{},feature_arrays,registry['cost_policy']['rates'])
    assert reason is None and row['terminal']=='TIMEOUT'
    assert row['funding_R']==pytest.approx(.9)
    assert row['gross_R']==pytest.approx(row['price_R']+row['basis_R']+row['funding_R'])
    assert row['cost_parts']['funding_buffer']==pytest.approx(.09)
    feature_arrays['L']['mark_price']=feature_arrays['L']['mark_price'][:1]
    assert edge.label_pair(cand,hourly,{},feature_arrays,registry['cost_policy']['rates'])[1]=='BASIS_CONVERGENCE_MISSING'
