"""Alpha V2 causal contracts and immutable economic admission."""
import importlib.util
from pathlib import Path
import numpy as np
import pytest
from app.trading_intelligence.setups.alpha_v2 import aggregate_complete,closed_features,htf_context,signal_mask,HYPOTHESES,GENERATOR_ID,AlphaCosts
PATH=Path(__file__).resolve().parents[4]/'scripts/evaluate_cati_alpha_v2.py'
spec=importlib.util.spec_from_file_location('alpha_v2_evaluation',PATH)
evaluation=importlib.util.module_from_spec(spec);spec.loader.exec_module(evaluation)


def rows(n=1600):
    return [(i*900000,100+i*.01,101+i*.01,99+i*.01,100.1+i*.01,100+i%11) for i in range(n)]


def test_complete_htf_omits_partial_and_missing_groups():
    r=rows(40)
    assert len(aggregate_complete(r,1,40*900000-1))==10
    assert len(aggregate_complete(r[:-1],1,40*900000-1))==9
    assert len(aggregate_complete(r[:5]+r[6:],1,40*900000-1))==9
    assert len(aggregate_complete(r,4,40*900000-1))==2
    assert len(aggregate_complete(r,4,16*900000-2))==0


def test_htf_closed_timestamp_never_exceeds_execution_time():
    r=rows();f=closed_features(r,'15m',1600*900000-1)
    for hours in (1,4):
        h=htf_context(r,hours,1600*900000-1,f.closed_at.to_numpy())
        ix=h.closed_at.notna()
        assert (h.loc[ix,'closed_at'].to_numpy()<=f.loc[ix,'closed_at'].to_numpy()).all()
        assert (f.loc[ix,'closed_at'].to_numpy()-h.loc[ix,'closed_at'].to_numpy()<hours*3600000).all()


def test_four_mechanism_prefix_invariance_and_gap_fail_closed():
    r=rows(); n=1400
    full=closed_features(r,'15m',1600*900000-1);prefix=closed_features(r[:n],'15m',n*900000-1)
    np.testing.assert_allclose(prefix.to_numpy(float),full.iloc[:n].to_numpy(float),equal_nan=True)
    def masks(f,r,stop):
        one=htf_context(r,1,stop,f.closed_at.to_numpy());four=htf_context(r,4,stop,f.closed_at.to_numpy())
        return [signal_mask(f,h,f,f,f.return_96.to_numpy(),one,four)[0] for h in HYPOTHESES]
    for a,b in zip(masks(prefix,r[:n],n*900000-1),masks(full,r,1600*900000-1)):
        np.testing.assert_array_equal(a,b[:n])
    missing=closed_features(r[:1540]+r[1541:],'15m',1600*900000-1)
    assert not missing.continuous_97.iloc[-1]
    assert len(HYPOTHESES)==4 and 'V2' in GENERATOR_ID


def test_registered_economic_gate_cannot_hide_bad_fold_or_costs():
    f=dict(samples=300,days=60,gross_R=.2,net_lower_95_R=.02)
    assert evaluation.viability([f.copy() for _ in range(5)],dict(net_2x_cost_R=.01))
    bad=[f.copy() for _ in range(5)];bad[3]['net_lower_95_R']=-.001
    assert not evaluation.viability(bad,dict(net_2x_cost_R=.5))
    assert not evaluation.viability([f]*5,dict(net_2x_cost_R=0))
    assert AlphaCosts().fractions(HYPOTHESES[0])['funding']==pytest.approx(.0002)
    import json
    registered=json.loads((PATH.parents[1]/'docs/research/cati_alpha_v2_registry.json').read_text())
    assert registered['maximum_evaluation_runs']==1
    assert registered['maximum_mechanisms']==4


def test_query_holdout_rejected_before_sql():
    class RejectSQL:
        def execute(self,*a): raise AssertionError('SQL should not run')
    with pytest.raises(ValueError,match='holdout'):
        evaluation.load_closed(RejectSQL(),'BTCUSDT','15m',0,evaluation.HOLDOUT_START_MS)


def test_v2_geometry_label_identity_gap_ordering_and_cost_reserve():
    from app.trading_intelligence.contracts.instrument import InstrumentKey
    from app.trading_intelligence.setups.alpha_v2 import candidate_at,LABEL_VERSION
    r=rows(200);f=closed_features(r,'15m',200*900000-1)
    last=len(f)-1;entry=float(f.close.iloc[last])
    f.loc[last,'range_high']=entry+8
    key=InstrumentKey('CRYPTO','BTC','USDT','BTC/USDT:PERP','BINANCE_USDM','BTCUSDT')
    c=candidate_at(f,last,HYPOTHESES[0],key,'fixed-source',1)
    assert c is not None and c.room_to_target_R>=1.25
    assert candidate_at(f,last,HYPOTHESES[0],key,'fixed-source',1).setup_candidate_id==c.setup_candidate_id
    assert candidate_at(f,last,HYPOTHESES[0],key,'fixed-source',1,AlphaCosts(fee=.1)) is None
    future=[(c.decision_time+1+i*900000,entry,entry+.1,entry-.1,entry,100) for i in range(48)]
    future[0]=(future[0][0],entry+.2,c.target_reference+.1,c.structural_invalidation-.1,entry,100)
    label=evaluation.label_path(c,future,HYPOTHESES[0],AlphaCosts())
    assert label['terminal']=='STOP' and label['ambiguous_stop']
    assert label['gross_R']<-1
    assert label['net_R']==pytest.approx(label['gross_R']-label['cost_R'])
    assert label['label_id']==evaluation.label_path(c,future,HYPOTHESES[0],AlphaCosts())['label_id']
    assert evaluation.label_path(c,future[:-1],HYPOTHESES[0],AlphaCosts()) is None
    assert LABEL_VERSION.endswith('V2')

