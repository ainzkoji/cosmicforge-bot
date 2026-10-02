"""Exact replay of registered outer fits, never a new search or source query."""
import argparse,json,sys,time
from pathlib import Path
REPO=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(REPO/'backends/bot-backend'),str(REPO/'backends/shared')]
import numpy as np
import psutil
from threadpoolctl import threadpool_limits
from app.trading_intelligence.forecast.artifact import load_library_artifact,LibraryArtifactError
from app.trading_intelligence.forecast.v5_artifact import read_numeric_model
from app.trading_intelligence.forecast.v5_models import JointProbability,ConditionalAtoms,coherence_metrics,coherence_pass


def main(a):
    start=time.perf_counter(); lib,d=load_library_artifact(a.artifact,mode='DEVELOPMENT'); load_seconds=time.perf_counter()-start
    try: load_library_artifact(a.artifact,mode='RUNTIME')
    except LibraryArtifactError: pass
    else: raise AssertionError('runtime must reject research model')
    cache=Path(a.cache); arrays={k:np.load(cache/f'{k}.npy',mmap_mode='r',allow_pickle=False) for k in ('geometry','context','categories')}
    g,c,cats=[arrays[k] for k in ('geometry','context','categories')]
    registry=json.loads((REPO/'docs/research/cati_v5_research_registry.json').read_text()); predictions=Path(a.predictions)
    rows=0; maximum_difference=0.; probability_fits=0
    for i,architecture in enumerate(registry['architectures']):
        with np.load(predictions/f'architecture_{i}_outer_predictions.npz',allow_pickle=False) as z:
            indices=z['indices']; stored=z['joint']; offset=0
            for f in d['architecture_results'][architecture]['folds']:
                trace=next(t for t in d['outer_probability_models'] if t['fold']==f['fold'] and t['architecture']==architecture)
                model=JointProbability.from_dict(read_numeric_model(a.artifact,trace['model'])); n=f['samples']; ix=indices[offset:offset+n]
                p=model.predict(g[ix],c[ix],cats[ix]); delta=float(np.max(abs(p-stored[offset:offset+n])))
                assert delta==0.,(architecture,f['fold'],delta); maximum_difference=max(maximum_difference,delta)
                offset+=n; probability_fits+=1
            assert offset==len(indices)
    with np.load(predictions/'outer_predictions.npz',allow_pickle=False) as z:
        keys=('joint','p','terminal','expected_net_R','conditional_positive_net_R','conditional_loss_net_R','expected_gross_R','state_net_R_mean','mfe','mae','net_R_quantiles')
        saved={k:z[k] for k in keys}; indices=z['indices']; offset=0
        for f in d['folds']:
            model=JointProbability.from_dict(read_numeric_model(a.artifact,next(t['model'] for t in d['outer_probability_models'] if t['fold']==f['fold'] and t['architecture']==f['architecture'])))
            distribution=ConditionalAtoms.from_dict(read_numeric_model(a.artifact,next(t['model'] for t in d['outer_conditional_models'] if t['fold']==f['fold'])))
            for pos in range(0,f['samples'],256):
                ix=indices[offset+pos:offset+min(pos+256,f['samples'])]
                joint=model.predict(g[ix],c[ix],cats[ix]); pred=distribution.predict(g[ix],c[ix],cats[ix],joint)
                assert coherence_pass(coherence_metrics(pred))
                for k in keys:
                    np.testing.assert_array_equal(pred[k],saved[k][offset+pos:offset+pos+len(ix)],err_msg=k)
                rows+=len(ix)
            offset+=f['samples']
        assert offset==len(indices)
    attempts=d['attempts']
    assert all(t['training_label_end']<t['training_cutoff']<=t['evaluation_start'] for t in attempts)
    assert all(t['training_label_end']<t['evaluation_start']<d['holdout_start_ms'] for t in d['folds'])
    result=dict(candidate_id=lib.library_id,artifact_load_seconds=load_seconds,outer_probability_models_replayed=probability_fits,
        outer_joint_probability_max_absolute_difference=maximum_difference,selected_joint_payoff_path_rows_replayed=rows,
        exact_payoff_path_replay=True,nested_maturity_checks_passed=True,runtime_load_rejected=True,holdout_query_count=0,
        peak_working_set_bytes=psutil.Process().memory_info().peak_wset,elapsed_seconds=time.perf_counter()-start)
    Path(a.output).write_text(json.dumps(result,indent=2)); print(json.dumps(result),flush=True)


if __name__=='__main__':
    p=argparse.ArgumentParser(); p.add_argument('--artifact',required=True); p.add_argument('--cache',required=True); p.add_argument('--predictions',required=True); p.add_argument('--output',required=True)
    with threadpool_limits(limits=1): main(p.parse_args())
