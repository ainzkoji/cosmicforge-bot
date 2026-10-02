import json
from pathlib import Path
import numpy as np
import pytest
from app.trading_intelligence.forecast.information_conditioning import (
    InformationConditioner, causal_features, causal_baseline, matured_indices)
from app.trading_intelligence.forecast.artifact import load_library_artifact, LibraryArtifactError
from app.trading_intelligence.hashing import stable_hash

DIMS=dict(setup_family='BREAKOUT_VOL_EXPANSION_V2',side='LONG',dominant_regime='VOL_EXPANSION',
          volatility_bucket='HIGH',instrument_group='UNKNOWN')


def feature(room=2.,risk=.01,instrument='BTCUSDT'):
    return causal_features(dimensions=DIMS,room=room,risk_fraction=risk,timeframe='15m',horizon=48,instrument=instrument)


def test_causal_geometry_and_cost_no_label_inputs():
    cats,x=feature()
    assert 'instrument_group' not in cats
    assert cats['horizon']=='48' and cats['timeframe']=='15m'
    assert x[2]==pytest.approx(np.log1p(.0015/.01))
    with pytest.raises(ValueError): feature(risk=0.)
    with pytest.raises(ValueError): feature(room=float('nan'))


def test_baseline_strictly_excludes_future_and_maturity_ties():
    t=np.array([1,2,3,4]); end=np.array([3,4,5,6]); y=np.array([1,0,0,1])
    assert matured_indices(t,end,4).tolist()==[0]
    assert causal_baseline(y,t,end,4)==1.
    y[1:]=1-y[1:]
    assert causal_baseline(y,t,end,4)==1.
    with pytest.raises(ValueError): causal_baseline(y,t,end,3)


def test_training_transforms_pool_tiny_instruments_and_roundtrip_determinism():
    features=[feature(room=1.+i/300,instrument='BTCUSDT' if i else 'TINY') for i in range(600)]
    y=np.array([int(i%3==0) for i in range(600)])
    a=InformationConditioner().fit(features,y); b=InformationConditioner().fit(features,y)
    assert 'instrument=TINY' not in a.vocabulary
    assert a.to_dict()==b.to_dict()
    restored=InformationConditioner.from_dict(a.to_dict())
    np.testing.assert_array_equal(a.predict(features),restored.predict(features))
    before=a.mean.copy(); a.predict([feature(room=10000)])
    np.testing.assert_array_equal(a.mean,before)
    assert a.predict([feature(instrument='UNSEEN')])[0]==pytest.approx(a.predict([feature(instrument='TINY')])[0])


def artifact(tmp_path):
    repo=Path(__file__).resolve().parents[4]
    registry=json.loads((repo/'docs/research/cati_v3_research_registry.json').read_text())
    model=InformationConditioner().fit([feature(room=1+i/100) for i in range(100)],np.arange(100)%2)
    from _helpers import build_library
    from app.trading_intelligence.forecast.artifact import row_to_dict
    row=row_to_dict(build_library(n_rows=1).rows[0])
    d=dict(role=registry['role'],feature_schema=registry['feature_schema'],estimator=registry['estimator'],
        registry_hash=stable_hash(registry),dataset_manifest_hash=registry['dataset_manifest_hash'],
        parent_library_hash=registry['parent_library_hash'],code_revision='1'*40,source_tree_dirty=False,
        holdout_start_ms=1783876499999,training_label_end=1783830000000,runtime_eligible=False,
        metrics=dict(samples=500,brier_skill=.001,ece=.02),folds=[dict(brier_skill=.001)]*5,
        calibration_status='RESEARCH_ONLY',model=model.to_dict(),reference_rows=[row])
    return d


def write(tmp_path,d):
    d['library_hash']=stable_hash({k:v for k,v in d.items() if k not in ('library_hash','candidate_id')})
    d['candidate_id']='cati_v3_'+d['library_hash'][:24]
    (tmp_path/'v3_model.json').write_text(json.dumps(d))
    return d['library_hash']


def test_artifact_development_reload_and_runtime_closed(tmp_path):
    d=artifact(tmp_path); h=write(tmp_path,d)
    library,manifest=load_library_artifact(tmp_path,mode='DEVELOPMENT',expected_hash=h)
    from app.trading_intelligence.config import library_scope
    assert library_scope(manifest)==('CRYPTO',)
    assert library.library_hash==h and library.calibration_status=='RESEARCH_ONLY'
    with pytest.raises(LibraryArtifactError,match='runtime format not approved'):
        load_library_artifact(tmp_path,expected_hash=h)
    with pytest.raises(LibraryArtifactError,match='identity mismatch'):
        load_library_artifact(tmp_path,mode='DEVELOPMENT',expected_hash='unknown')


def test_v3_forecast_uses_canonical_contract_and_remains_research_only(tmp_path):
    from _helpers import market_state_and_regime, flat_rows, candidate_for
    from app.trading_intelligence.forecast.engine import build_outcome_forecast
    d=artifact(tmp_path); d['training_label_end']=1500000000000; write(tmp_path,d)
    library,_=load_library_artifact(tmp_path,mode='DEVELOPMENT')
    ms,regime=market_state_and_regime(flat_rows(0)); cand=candidate_for(ms)
    forecast=build_outcome_forecast(cand,ms,regime,library)
    assert forecast.calibration_status=='RESEARCH_ONLY'
    assert 'V3_RESEARCH_ONLY' in forecast.reason_codes
    assert forecast.library_hash==d['library_hash']
    assert 0<forecast.p_net_profitable_mean<1


@pytest.mark.parametrize('key,value,message',[
    ('source_tree_dirty',True,'dirty'),('dataset_manifest_hash','wrong','dataset'),
    ('feature_schema','wrong','schema'),('registry_hash','wrong','unregistered'),
    ('calibration_status','CALIBRATED','calibration'),('training_label_end',1783876499999,'holdout')])
def test_artifact_rejects_semantically_invalid_even_rehashed(tmp_path,key,value,message):
    d=artifact(tmp_path); d[key]=value; write(tmp_path,d)
    with pytest.raises(LibraryArtifactError,match=message): load_library_artifact(tmp_path,mode='DEVELOPMENT')
