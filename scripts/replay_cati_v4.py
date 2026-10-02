"""Replay fixed registered models; no search, source queries, or holdout access."""
from __future__ import annotations
import argparse,json,sys,time
from pathlib import Path
REPO=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(REPO/'scripts'),str(REPO/'backends/bot-backend'),str(REPO/'backends/shared')]
import numpy as np
import psutil
from threadpoolctl import threadpool_limits
from evaluate_cati_v3 import limited
from app.trading_intelligence.hashing import stable_hash
from app.trading_intelligence.forecast.artifact import load_library_artifact,LibraryArtifactError
from app.trading_intelligence.forecast.v4_models import V4Probability,ConditionalPayoff,MODES


def main(a):
    started=time.perf_counter()
    d=json.loads((Path(a.artifact)/'v4_model.json').read_text())
    load_started=time.perf_counter()
    lib,_=load_library_artifact(a.artifact,mode='DEVELOPMENT',expected_hash=d['library_hash'])
    load_seconds=time.perf_counter()-load_started
    try: load_library_artifact(a.artifact,mode='RUNTIME')
    except LibraryArtifactError: runtime_rejected=True
    else: raise AssertionError('research artifact granted runtime access')
    with np.load(Path(a.cache)/'development.npz',allow_pickle=False) as archive:
        cache={k:archive[k] for k in archive.files if k!='label_ids'}
    g,c,cats=cache['geometry'],cache['context'],cache['categories']
    maximum_probability_difference=0.
    for j,mode in enumerate(MODES):
        saved=np.load(Path(a.artifact)/f'family_{j}_outer_predictions.npz',allow_pickle=False)
        offset=0
        for trace,fold in zip([e for e in d['family_effect_drift'] if e['mode']==mode],d['family_results'][mode]['folds']):
            n=fold['samples']; ix=saved['indices'][offset:offset+n]
            model=V4Probability.from_dict(dict(schema=d['feature_schema'],**trace))
            p=model.predict(g[ix],c[ix],cats[ix])
            delta=float(np.max(abs(p-saved['p'][offset:offset+n])))
            assert delta==0.,(mode,fold['fold'],delta)
            maximum_probability_difference=max(maximum_probability_difference,delta); offset+=n
        assert offset==len(saved['indices'])
        saved.close()
    train=np.flatnonzero(cache['label_ends']<=d['training_label_end'])
    ix=limited(train,120000); spec=d['probability_model']
    probability=V4Probability(spec['mode'],spec['C']).fit(g[ix],c[ix],cats[ix],cache['targets'][ix,0])
    assert stable_hash(probability.to_dict())==stable_hash(spec),'fixed probability refit changed'
    ix=limited(train,60000)
    payoff=ConditionalPayoff().fit(g[ix],c[ix],cats[ix],cache['targets'][ix])
    assert stable_hash(payoff.to_dict())==stable_hash(d['payoff_model']),'fixed payoff refit changed'
    # Parameter replay on existing development features, not historical causal
    # forecasts from the final fit. Outer scores above use their own prefix fits.
    probe=limited(train,1000); inference_started=time.perf_counter()
    p=lib.v4_probability.predict(g[probe],c[probe],cats[probe])
    predicted=lib.payoff.predict(g[probe],c[probe],cats[probe],p)
    seconds=time.perf_counter()-inference_started
    for key,value in payoff.predict(g[probe],c[probe],cats[probe],p).items():
        if isinstance(value,np.ndarray): np.testing.assert_array_equal(value,predicted[key])
    m=psutil.Process().memory_info()
    result=dict(candidate_id=d['candidate_id'],outer_probability_replay_max_absolute_difference=maximum_probability_difference,
        probability_refit_identity_equal=True,payoff_refit_identity_equal=True,runtime_load_rejected=runtime_rejected,
        artifact_load_seconds=load_seconds,parameter_replay_rows=len(probe),parameter_replay_seconds=seconds,
        peak_working_set_bytes=getattr(m,'peak_wset',m.rss),elapsed_seconds=time.perf_counter()-started,
        holdout_query_count=0,parallel_fit_workers=1,
        scope='Fixed final refit and all 20 outer probability replays; not a new search or final-fit out-of-sample score.')
    Path(a.output).write_text(json.dumps(result,indent=2)); print(json.dumps(result),flush=True)


if __name__=='__main__':
    p=argparse.ArgumentParser(); p.add_argument('--cache',required=True); p.add_argument('--artifact',required=True); p.add_argument('--output',required=True)
    with threadpool_limits(limits=1): main(p.parse_args())
