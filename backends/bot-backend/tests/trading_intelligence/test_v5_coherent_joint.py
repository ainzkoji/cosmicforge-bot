import copy,gzip,json
from pathlib import Path
import numpy as np
import pytest
from threadpoolctl import threadpool_limits
from app.trading_intelligence.hashing import stable_hash
from app.trading_intelligence.forecast.artifact import load_library_artifact,LibraryArtifactError
from app.trading_intelligence.forecast.v5_models import (
    JointProbability,ConditionalAtoms,STATES,PROFIT,TERMINAL,FEATURE_SCHEMA,
    joint_labels,joint_from_logits,recency_weights,coherence_metrics,coherence_pass,decision_gate)


def registry():
    return json.loads((Path(__file__).resolve().parents[4]/'docs/research/cati_v5_research_registry.json').read_text())


def inputs(n=1800):
    rng=np.random.default_rng(17)
    g=np.column_stack((rng.normal(.4,.3,n),rng.normal(-4.5,.1,n),np.full(n,np.log1p(.1)),rng.normal(-2,.1,n)))
    g[:12,0]=np.log(.05)
    c=rng.normal(0,.01,(n,11)); cats=np.asarray([['A' if i%2 else 'B','LONG','RANGE','MEDIUM','BTC'] for i in range(n)])
    terminal=np.choose(np.arange(n)%4,[0,1,2,2]); cost=np.expm1(g[:,2]); room=np.exp(g[:,0])
    net=np.where(terminal==0,room-cost,np.where(terminal==1,-1-cost,np.where(np.arange(n)%4==2,.2,-.3)))
    net[(terminal==2)&(room<=cost)]=-.03
    target=np.column_stack((net>0,net,net+cost,abs(rng.normal(size=n))+1,abs(rng.normal(size=n))+1,terminal))
    events=rng.integers(1,49,n); events[terminal==2]=48
    times=np.arange(n,dtype=np.int64)*900000+1600000000000
    return g,c,cats,target,events,times


@pytest.fixture(scope='module')
def trained():
    g,c,cats,targets,events,times=inputs(); r=registry()
    with threadpool_limits(limits=1):
        model=JointProbability(r['variants'][0]).fit(g,c,cats,targets,times,int(times[-1])+900000)
        dist=ConditionalAtoms().fit(g,c,cats,targets,events)
    return g,c,cats,targets,events,times,model,dist


def test_all_possible_joint_states_retained_and_stop_profit_is_rejected():
    g,c,cats,targets,events,times=inputs()
    assert set(joint_labels(targets))==set(range(5))
    broken=targets.copy(); broken[1,0]=1; broken[1,1]=.3
    with pytest.raises(ValueError,match='semantics'): joint_labels(broken)
    assert not coherence_pass({})


def test_joint_support_is_generative_not_post_hoc_clipping():
    g,c,cats,targets,events,times=inputs(100)
    joint=joint_from_logits(np.full((100,4),1000.),g)
    np.testing.assert_allclose(joint.sum(axis=1),1.,atol=1e-15)
    assert np.all(joint[:12,0]==0) and np.all(joint[:12,3]==0)
    assert np.all(joint[12:,1]==0)
    np.testing.assert_allclose(joint[:12,1:3],1/3)
    assert np.all(joint>=0) and np.all(joint<=1)


def test_profit_terminal_expected_payoff_identities_and_state_signs(trained):
    g,c,cats,_,_,_,model,dist=trained
    joint=model.predict(g[:200],c[:200],cats[:200]); p=dist.predict(g[:200],c[:200],cats[:200],joint)
    assert coherence_pass(coherence_metrics(p))
    np.testing.assert_allclose(p['p'],joint[:,PROFIT].sum(axis=1),atol=1e-15)
    np.testing.assert_allclose(p['terminal'].sum(axis=1),1.,atol=1e-15)
    assert np.all(p['p']+p['terminal'][:,1]<=1+1e-15)
    np.testing.assert_allclose(p['expected_net_R'],(joint*p['state_net_R_mean']).sum(axis=1),atol=1e-15)
    np.testing.assert_allclose(p['expected_net_R'],p['p']*p['conditional_positive_net_R']+(1-p['p'])*p['conditional_loss_net_R'],atol=1e-15)
    np.testing.assert_allclose(p['expected_gross_R']-p['expected_net_R'],np.expm1(g[:200,2]))
    assert np.all(p['state_net_R_mean'][:,PROFIT]>=0) and np.all(p['state_net_R_mean'][:,~PROFIT]<=0)
    assert np.all(np.diff(p['mfe'],axis=1)>=0) and np.all(np.diff(p['mae'],axis=1)>=0)
    np.testing.assert_allclose(p['joint_time_pmf'].sum(axis=(1,2)),1.,atol=1e-15)
    assert np.all(p['state_time_pmf'][:,3:,:47]==0)


def test_distribution_and_event_time_replay_never_reads_future_labels(trained):
    g,c,cats,targets,events,times,model,dist=trained
    reloaded=JointProbability.from_dict(model.to_dict()); restored=ConditionalAtoms.from_dict(dist.to_dict())
    joint=model.predict(g[:100],c[:100],cats[:100]); np.testing.assert_array_equal(joint,reloaded.predict(g[:100],c[:100],cats[:100]))
    before=dist.to_dict(); expected=dist.predict(g[:100],c[:100],cats[:100],joint)
    targets=targets.copy(); targets[:,1:]*=500; events=events.copy(); events[:]=1
    actual=restored.predict(g[:100],c[:100],cats[:100],joint)
    for key in expected: np.testing.assert_array_equal(expected[key],actual[key])
    assert dist.to_dict()==before
    g2=g.copy(); g2[100:]*=2
    np.testing.assert_array_equal(model.predict(g2[:100],c[:100],cats[:100]),joint)


def test_recency_weights_are_time_only_normalized_and_future_rejected():
    times=np.array([0,180,365],dtype=np.int64)*86400000; cutoff=366*86400000
    w=recency_weights(times,cutoff,180)
    np.testing.assert_allclose(w[1]/w[0],2.)
    assert w[0]<w[1]<w[2] and np.mean(w)==pytest.approx(1.)
    np.testing.assert_array_equal(recency_weights(times,cutoff,None),np.ones(3))
    with pytest.raises(ValueError,match='future'): recency_weights(times,times[-1],180)
    with pytest.raises(ValueError,match='unregistered'): recency_weights(times,cutoff,3)


def test_histogram_joint_export_and_recency_classifier_are_deterministic():
    g,c,cats,targets,events,times=inputs(1600)
    for spec in [registry()['variants'][2],registry()['variants'][4]]:
        with threadpool_limits(limits=1): model=JointProbability(spec).fit(g,c,cats,targets,times,int(times[-1])+900000)
        restored=JointProbability.from_dict(model.to_dict())
        np.testing.assert_array_equal(model.predict(g[:70],c[:70],cats[:70]),restored.predict(g[:70],c[:70],cats[:70]))


def payload(trained):
    *_,model,dist=trained; r=registry()
    coh={k:0 for k in coherence_metrics(dist.predict(trained[0][:20],trained[1][:20],trained[2][:20],model.predict(trained[0][:20],trained[1][:20],trained[2][:20])))}
    buckets=[dict(prediction_mean=i/10,realized_mean=i/10,samples=200) for i in range(1,6)]
    pay=dict(expected_net_R=dict(rmse=1.,mae=.8),causal_baseline_net_R=dict(rmse=2.,mae=1.),terminal_multiclass_brier=.4,
        causal_terminal_baseline_brier=.5,maximum_bucket_absolute_bias=.1,maximum_expectancy_quintile_inversion_R=0.,expected_R_buckets=buckets,
        quantiles={k:{str(q):dict(coverage_error=0.) for q in (.1,.5,.9)} for k in ('mfe','mae')})
    tm=dict(conditional_MAE=1.,causal_terminal_frequency_MAE=2.,conditional_CRPS=1.,causal_terminal_frequency_CRPS=2.)
    metric=dict(samples=1000,brier_skill=.03,ece=.01,coherence=coh,payoff=pay,time_validation=tm)
    return dict(role=r['role'],feature_schema=FEATURE_SCHEMA,registry_hash=stable_hash(r),parent_library_hash=r['parent_library_hash'],
        dataset_manifest_hash=r['dataset_manifest_hash'],source_tree_dirty=False,code_revision='1'*40,holdout_start_ms=1783876499999,
        training_label_end=1500000000000,training_rows=1000,holdout_query_count=0,runtime_eligible=False,calibration_status='RESEARCH_ONLY',
        joint_states=STATES,probability_model=model.to_dict(),conditional_model=dist.to_dict(),metrics=metric,folds=[copy.deepcopy(metric) for _ in range(5)],
        coherence_pass=True,model_ready=True,decision_payoff_ready=True,payoff_gate_pass=True,development_status='DEVELOPMENT_GATE_PASS')


def save(root,d,compressed=False):
    d['library_hash']=stable_hash({k:v for k,v in d.items() if k not in ('library_hash','candidate_id')})
    d['candidate_id']='cati_v5_'+d['library_hash'][:24]
    if compressed:
        with gzip.open(root/'v5_model.json.gz','wt') as f: json.dump(d,f)
    else: (root/'v5_model.json').write_text(json.dumps(d))


@pytest.mark.parametrize('compressed',[False,True])
def test_numeric_artifact_identity_canonical_adapter_and_m0_runtime_denial(tmp_path,trained,compressed):
    from _helpers import market_state_and_regime,flat_rows,candidate_for
    from app.trading_intelligence.forecast.engine import build_outcome_forecast
    d=payload(trained); save(tmp_path,d,compressed)
    lib,_=load_library_artifact(tmp_path,mode='DEVELOPMENT',expected_hash=d['library_hash'])
    ms,regime=market_state_and_regime(flat_rows(0)); candidate=candidate_for(ms)
    context=dict(schema='closed-candle-context-v4-1',instrument=candidate.instrument_key.venue_symbol,
        decision_time=candidate.decision_time,source_close_times=[candidate.decision_time]*3,values=trained[1][0].tolist())
    fc=build_outcome_forecast(candidate,ms,regime,lib,causal_market_context=context)
    assert fc.status=='VALID' and fc.calibration_status=='RESEARCH_ONLY'
    assert sum(fc.joint_outcome_probabilities.values())==pytest.approx(1.)
    assert fc.expected_net_R==pytest.approx(sum(fc.joint_outcome_probabilities[s]*fc.joint_state_net_R_means[s] for s in STATES))
    assert sum(sum(t) for t in fc.joint_event_time_probabilities.values())==pytest.approx(1.)
    assert fc.p_net_profitable_mean+fc.p_stop_before_target<=1+1e-15
    context['source_close_times'][1]+=1
    assert build_outcome_forecast(candidate,ms,regime,lib,causal_market_context=context).status=='INVALID_INPUT'
    context['source_close_times'][1]-=1; context['values'][0]=float('nan')
    assert build_outcome_forecast(candidate,ms,regime,lib,causal_market_context=context).status=='INVALID_INPUT'
    with pytest.raises(LibraryArtifactError,match='runtime remains closed'): load_library_artifact(tmp_path,mode='RUNTIME')
    with pytest.raises(LibraryArtifactError,match='identity'): load_library_artifact(tmp_path,mode='DEVELOPMENT',expected_hash='wrong')


@pytest.mark.parametrize('key,value,message',[
    ('source_tree_dirty',True,'provenance'),('code_revision','z'*40,'provenance'),('dataset_manifest_hash','wrong','dataset'),
    ('feature_schema','wrong','schema'),('registry_hash','wrong','unregistered'),('model_ready',False,'verdict'),
    ('decision_payoff_ready',False,'verdict'),('training_label_end',1783876499999,'holdout'),('holdout_query_count',1,'holdout')])
def test_semantic_artifact_rejection_with_recomputed_identity(tmp_path,trained,key,value,message):
    d=payload(trained); d[key]=value; save(tmp_path,d)
    with pytest.raises(LibraryArtifactError,match=message): load_library_artifact(tmp_path,mode='DEVELOPMENT')


def test_temporal_reversal_and_negative_top_bucket_deny_payoff_even_if_pooled_passes(trained):
    d=payload(trained); r=registry(); m=d['metrics']
    assert decision_gate(m['payoff'],d['folds'],m['time_validation'],r,True)
    d['folds'][4]['payoff']['maximum_expectancy_quintile_inversion_R']=.06
    assert not decision_gate(m['payoff'],d['folds'],m['time_validation'],r,True)
    d=payload(trained); d['metrics']['payoff']['expected_R_buckets'][-1]['realized_mean']=-.01
    assert not decision_gate(d['metrics']['payoff'],d['folds'],d['metrics']['time_validation'],r,True)
