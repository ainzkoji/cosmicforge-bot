import copy,json
from pathlib import Path
import numpy as np
import pytest
from threadpoolctl import threadpool_limits
from app.trading_intelligence.hashing import stable_hash
from app.trading_intelligence.forecast.causal_context import closed_candle_context,align_context
from app.trading_intelligence.forecast.v4_models import FeatureEncoder,V4Probability,ConditionalPayoff,MODES,FEATURE_SCHEMA,probability_gate
from app.trading_intelligence.forecast.artifact import load_library_artifact,LibraryArtifactError


def inputs(n=1600):
    rng=np.random.default_rng(7)
    g=rng.normal(size=(n,4)); c=rng.normal(scale=.01,size=(n,11))
    cats=np.asarray([['A' if i%2 else 'B','LONG','RANGE','MEDIUM','BTC'] for i in range(n)])
    y=np.arange(n)%3!=0
    net=np.where(y,1+.2*g[:,0]**2,-1-.05*abs(g[:,1]))
    target=np.column_stack((y,net,net+.1,abs(g[:,0])+1,abs(g[:,1])+1,np.arange(n)%3))
    return g,c,cats,target


def test_context_exact_alignment_and_appending_future_candles_has_no_effect():
    n=80; t=np.arange(n,dtype=np.int64)*900000+1600000200000
    c=np.linspace(100,105,n)
    closed,x=closed_candle_context(t,c,c+.3,c-.3,c,np.arange(n)+1)
    before=align_context(closed,x,closed[30:40])[0]
    changed=c.copy(); changed[40:]*=10
    _,z=closed_candle_context(t,changed,changed+.3,changed-.3,changed,np.arange(n)+1)
    np.testing.assert_array_equal(before,align_context(closed,z,closed[30:40])[0])
    with pytest.raises(ValueError): align_context(closed,x,closed[30:40]+1)
    with pytest.raises(ValueError): align_context(closed,x,np.array([closed[0]-1]))


@pytest.mark.parametrize('mode',MODES)
def test_nested_encoder_transforms_are_training_only_and_reload_deterministic(mode):
    g,c,cats,target=inputs(500)
    encoder=FeatureEncoder(mode).fit(g,c,cats)
    original=encoder.to_dict(); restored=FeatureEncoder.from_dict(original)
    g2=g*100; encoder.transform(g2,c,cats)
    assert encoder.to_dict()==original
    np.testing.assert_array_equal(encoder.transform(g,c,cats).toarray(),restored.transform(g,c,cats).toarray())
    if mode in (MODES[0],MODES[3]):
        assert len(encoder.mean)>=16
        np.testing.assert_allclose(encoder.knots,np.quantile(g[:,:3],[.25,.5,.75],axis=0).T)


def test_family_deviations_omit_tiny_groups_and_share_broader_slopes():
    g,c,cats,target=inputs(500); cats[0,0]='TINY'
    with threadpool_limits(limits=1): model=V4Probability(MODES[1]).fit(g,c,cats,target[:,0])
    assert 'TINY' not in model.encoder.families
    first=cats[:1].copy(); first[0,0]='TINY'; second=first.copy(); second[0,0]='UNSEEN'
    np.testing.assert_array_equal(model.predict(g[:1],c[:1],first),model.predict(g[:1],c[:1],second))


@pytest.fixture(scope='module')
def trained():
    g,c,cats,target=inputs()
    with threadpool_limits(limits=1):
        probability=V4Probability(MODES[3]).fit(g,c,cats,target[:,0])
        payoff=ConditionalPayoff().fit(g,c,cats,target)
    return g,c,cats,target,probability,payoff


def test_payoff_prediction_uses_only_features_and_binary_probability_and_json_reload(trained):
    g,c,cats,target,model,payoff=trained
    p=model.predict(g[:100],c[:100],cats[:100]); actual=payoff.predict(g[:100],c[:100],cats[:100],p)
    restored=ConditionalPayoff.from_dict(payoff.to_dict()); replay=restored.predict(g[:100],c[:100],cats[:100],p)
    target=target.copy(); target[:100,1:]*=100  # Future outcomes cannot change fitted predictions.
    repeated=payoff.predict(g[:100],c[:100],cats[:100],p)
    for key in ('expected_net_R','conditional_positive_net_R','conditional_loss_net_R','terminal','mfe','mae'):
        np.testing.assert_array_equal(actual[key],replay[key]); np.testing.assert_array_equal(actual[key],repeated[key])
    np.testing.assert_allclose(actual['terminal'].sum(axis=1),1.)
    np.testing.assert_allclose(actual['expected_net_R'],p*actual['conditional_positive_net_R']+(1-p)*actual['conditional_loss_net_R'])
    assert np.all(np.diff(actual['mfe'],axis=1)>=0)


def registry():
    return json.loads((Path(__file__).resolve().parents[4]/'docs/research/cati_v4_research_registry.json').read_text())


def payload(trained):
    _,_,_,_,model,payoff=trained; r=registry()
    pay=dict(expected_net_R=dict(rmse=1.,mae=.8),causal_baseline_net_R=dict(rmse=2.,mae=1.),terminal_multiclass_brier=.4,
        causal_terminal_baseline_brier=.5,maximum_bucket_absolute_bias=.1,maximum_expectancy_quintile_inversion_R=0.,
        quantiles={k:{str(q):dict(coverage_error=0.) for q in (.1,.5,.9)} for k in ('mfe','mae')})
    return dict(role=r['role'],feature_schema=FEATURE_SCHEMA,registry_hash=stable_hash(r),parent_library_hash=r['parent_library_hash'],
        dataset_manifest_hash=r['dataset_manifest_hash'],source_tree_dirty=False,code_revision='1'*40,
        holdout_start_ms=1783876499999,training_label_end=1500000000000,training_rows=1000,holdout_query_count=0,
        runtime_eligible=False,calibration_status='RESEARCH_ONLY',probability_model=model.to_dict(),payoff_model=payoff.to_dict(),
        metrics=dict(samples=1000,brier_skill=.03,ece=.01),folds=[dict(brier_skill=.03,ece=.01,payoff=pay) for _ in range(5)],
        payoff_validation=pay,probability_gate_pass=True,payoff_gate_pass=True,model_ready=True,development_status='DEVELOPMENT_GATE_PASS')


def save(root,d):
    d['library_hash']=stable_hash({k:v for k,v in d.items() if k not in ('library_hash','candidate_id')})
    d['candidate_id']='cati_v4_'+d['library_hash'][:24]
    (root/'v4_model.json').write_text(json.dumps(d))


def test_artifact_identity_runtime_closed_and_canonical_payoff_contract(tmp_path,trained):
    from _helpers import market_state_and_regime,flat_rows,candidate_for
    from app.trading_intelligence.forecast.engine import build_outcome_forecast
    d=payload(trained); save(tmp_path,d)
    lib,_=load_library_artifact(tmp_path,mode='DEVELOPMENT',expected_hash=d['library_hash'])
    ms,regime=market_state_and_regime(flat_rows(0)); candidate=candidate_for(ms)
    context=dict(schema='closed-candle-context-v4-1',instrument=candidate.instrument_key.venue_symbol,
        decision_time=candidate.decision_time,source_close_times=[candidate.decision_time]*3,values=trained[1][0].tolist())
    fc=build_outcome_forecast(candidate,ms,regime,lib,causal_market_context=context)
    assert fc.status=='VALID' and fc.calibration_status=='RESEARCH_ONLY'
    assert fc.expected_net_R is not None and fc.conditional_positive_net_R>=0 and fc.conditional_loss_net_R<=0
    assert 'V4_RESEARCH_ONLY' in fc.reason_codes
    context['source_close_times'][1]+=1
    assert build_outcome_forecast(candidate,ms,regime,lib,causal_market_context=context).status=='INVALID_INPUT'
    with pytest.raises(LibraryArtifactError,match='runtime remains closed'): load_library_artifact(tmp_path,expected_hash=d['library_hash'])
    with pytest.raises(LibraryArtifactError,match='identity'): load_library_artifact(tmp_path,mode='DEVELOPMENT',expected_hash='wrong')


@pytest.mark.parametrize('key,value,message',[
    ('source_tree_dirty',True,'provenance'),('dataset_manifest_hash','wrong','dataset'),
    ('feature_schema','wrong','schema'),('registry_hash','wrong','unregistered'),
    ('probability_gate_pass',False,'calibration'),('payoff_gate_pass',False,'payoff'),
    ('training_label_end',1783876499999,'holdout')])
def test_artifact_semantic_rejection_even_with_recomputed_identity(tmp_path,trained,key,value,message):
    d=payload(trained); d[key]=value; save(tmp_path,d)
    with pytest.raises(LibraryArtifactError,match=message): load_library_artifact(tmp_path,mode='DEVELOPMENT')


def test_pooled_improvement_cannot_hide_late_fold_degradation():
    r=registry(); m=dict(samples=1000,brier_skill=.04,ece=.01)
    folds=[dict(brier_skill=.03,ece=.01) for _ in range(5)]
    assert probability_gate(m,folds,r['probability_gate'])
    folds[4]['brier_skill']=.015
    assert not probability_gate(m,folds,r['probability_gate'])
