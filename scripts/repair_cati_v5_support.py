"""Replay fixed registered V5 fits with versioned terminal support; no new fitting."""
import argparse,copy,gc,gzip,hashlib,io,json,shutil,subprocess,sys,time
from pathlib import Path
REPO=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(REPO/'scripts'),str(REPO/'backends/bot-backend'),str(REPO/'backends/shared')]
import numpy as np
import psutil
from threadpoolctl import threadpool_limits
from evaluate_cati_v5 import time_metrics,aggregate
from evaluate_cati_v3 import scores
from evaluate_cati_v4 import payoff_metrics
from app.trading_intelligence.hashing import stable_hash
from app.trading_intelligence.forecast.v4_models import probability_gate,payoff_gate
from app.trading_intelligence.forecast.v5_artifact import read_numeric_model,load_v5_artifact
from app.trading_intelligence.forecast.v5_models import ConditionalAtoms,compact_categories,coherence_metrics,coherence_pass,decision_gate


def main(a):
    started=time.perf_counter(); src=Path(a.artifact); out=Path(a.output); cache=Path(a.cache)
    if subprocess.check_output(['git','status','--porcelain'],cwd=REPO).strip(): raise ValueError('commit clean repair source first')
    _,original=load_v5_artifact(src,mode='DEVELOPMENT'); d=copy.deepcopy(original)
    registry=json.loads((REPO/'docs/research/cati_v5_research_registry.json').read_text())
    out.mkdir(parents=True,exist_ok=False)
    for path in src.glob('*.json.gz'): shutil.copyfile(path,out/path.name)
    g,c,rawcats,targets,events=[np.load(cache/f'{k}.npy',mmap_mode='r',allow_pickle=False) for k in ('geometry','context','categories','targets','event_bars')]
    cats=compact_categories(rawcats); del rawcats
    distributions={}; unchanged=True
    def persist(record,model):
        old=read_numeric_model(src,record); updated=model.to_dict()
        # The policy key is the sole parameter change: all fitted numbers stay fixed.
        assert {k:v for k,v in updated.items() if k!='generation_policy'}=={k:v for k,v in old.items() if k!='generation_policy'}
        path=out/record['artifact_file']
        with path.open('wb') as raw:
            with gzip.GzipFile(fileobj=raw,mode='wb',mtime=0) as compressed:
                with io.TextIOWrapper(compressed,encoding='utf-8') as f: json.dump(updated,f,sort_keys=True,separators=(',',':'))
        return dict(artifact_file=path.name,sha256=hashlib.sha256(path.read_bytes()).hexdigest())
    for trace in d['outer_conditional_models']:
        model=ConditionalAtoms.from_dict(read_numeric_model(src,trace['model'])); model.generation_policy='CAUSAL_TERMINAL_SUPPORT_V5_3'
        trace['model']=persist(trace['model'],model); distributions[trace['fold']]=model
    final=ConditionalAtoms.from_dict(read_numeric_model(src,d['conditional_model'])); final.generation_policy='CAUSAL_TERMINAL_SUPPORT_V5_3'
    d['conditional_model']=persist(d['conditional_model'],final)
    del final; gc.collect()
    def metrics(record):
        ix=record['indices']
        return dict(**scores(record['p'],targets[ix,0],record['baseline']),coherence=coherence_metrics(record),
            payoff=payoff_metrics(record,targets[ix],record['baseline_R'],record['baseline_terminal']),
            time_validation=time_metrics(record['time_cdf'],record['time_baseline_cdf'],events[ix],targets[ix,5]),
            positive_rate=float(targets[ix,0].mean()),prediction_mean=float(record['p'].mean()),prediction_std=float(record['p'].std()))
    changed={}; support_violations=0
    for i,architecture in enumerate(registry['architectures']):
        with np.load(Path(a.predictions)/f'architecture_{i}_outer_predictions.npz',allow_pickle=False) as archive:
            record={k:archive[k] for k in archive.files}
        offset=0; changed[architecture]=0; folds=[]
        for fold in original['architecture_results'][architecture]['folds']:
            n=fold['samples']; model=distributions[fold['fold']]
            for pos in range(0,n,256):
                sl=slice(offset+pos,offset+min(pos+256,n)); ix=record['indices'][sl]
                pred=model.predict(g[ix],c[ix],cats[ix],record['joint'][sl])
                assert coherence_pass(coherence_metrics(pred))
                cost=np.expm1(g[ix,2]); room=np.exp(g[ix,0])
                support_violations+=int(np.sum(pred['net_R_quantiles']>room[:,None]-cost[:,None]+1e-12)+np.sum(pred['net_R_quantiles']<-1-cost[:,None]-1e-12))
                changed[architecture]+=int(np.sum(abs(pred['expected_net_R']-record['expected_net_R'][sl])>1e-12))
                for key in ('joint','p','terminal'): np.testing.assert_array_equal(pred[key],record[key][sl])
                for key in ('expected_net_R','conditional_positive_net_R','conditional_loss_net_R','expected_gross_R','state_net_R_mean','mfe','mae','net_R_quantiles'):
                    record[key][sl]=pred[key]
                from app.trading_intelligence.forecast.v5_models import TERMINAL
                terminal=targets[ix,5].astype(int); masses=np.zeros((len(ix),48))
                for term in range(3):
                    rows=terminal==term
                    masses[rows]=pred['joint_time_pmf'][rows][:,TERMINAL==term,:].sum(axis=1)/pred['terminal'][rows,term,None]
                record['time_cdf'][sl]=np.cumsum(masses,axis=1)
            f=copy.deepcopy(fold); f.update(metrics({k:v[offset:offset+n] for k,v in record.items()})); folds.append(f); offset+=n
        assert offset==len(record['indices'])
        m=metrics(record); coherent=coherence_pass(m['coherence'])
        d['architecture_results'][architecture]=dict(metrics=m,folds=folds,
            model_ready=probability_gate(m,folds,registry['probability_gate']) and coherent,
            decision_payoff_ready=decision_gate(m['payoff'],folds,m['time_validation'],registry,coherent))
        np.savez_compressed(out/f'architecture_{i}_outer_predictions.npz',**record)
        del record; gc.collect()
    selected=[]; selected_folds=[]
    for f in original['folds']:
        i=registry['architectures'].index(f['architecture']); new_f=d['architecture_results'][f['architecture']]['folds'][f['fold']-1]
        with np.load(out/f'architecture_{i}_outer_predictions.npz',allow_pickle=False) as archive:
            start=sum(x['samples'] for x in original['architecture_results'][f['architecture']]['folds'][:f['fold']-1])
            selected.append({k:archive[k][start:start+f['samples']] for k in archive.files})
        selected_folds.append(new_f)
    joined=aggregate(selected); m=metrics(joined); coherent=coherence_pass(m['coherence'])
    assert support_violations==0
    d.update(metrics=m,folds=selected_folds,source_fit_code_revision=original['code_revision'],code_revision=subprocess.check_output(['git','rev-parse','HEAD'],cwd=REPO,text=True).strip(),
        support_repair_from=original['candidate_id'],generation_policy='CAUSAL_TERMINAL_SUPPORT_V5_3',
        coherence_pass=coherent,payoff_gate_pass=payoff_gate(m['payoff'],[f['payoff'] for f in selected_folds],registry['payoff_gate']),
        model_ready=probability_gate(m,selected_folds,registry['probability_gate']) and coherent,
        decision_payoff_ready=decision_gate(m['payoff'],selected_folds,m['time_validation'],registry,coherent))
    d['development_status']='DEVELOPMENT_GATE_PASS' if d['model_ready'] else 'REJECTED_PRE_HOLDOUT'
    d.pop('library_hash'); d.pop('candidate_id'); d['library_hash']=stable_hash(d); d['candidate_id']='cati_v5_'+d['library_hash'][:24]
    with (out/'v5_model.json').open('w') as f: json.dump(d,f,sort_keys=True,indent=2)
    np.savez_compressed(out/'outer_predictions.npz',**joined)
    qa=dict(generation_policy=d['generation_policy'],classifier_parameters_and_probability_vectors_unchanged=True,
        all_fitted_conditional_numbers_unchanged=True,completed_new_model_fits=0,changed_expected_R_rows_by_architecture=changed,
        net_R_quantile_support_violations=support_violations,peak_working_set_bytes=psutil.Process().memory_info().peak_wset,
        elapsed_seconds=time.perf_counter()-started,holdout_query_count=0)
    (out/'support_repair_validation.json').write_text(json.dumps(qa,indent=2))
    print(json.dumps(dict(candidate=d['candidate_id'],metrics=m,validation=qa)),flush=True)


if __name__=='__main__':
    p=argparse.ArgumentParser(); p.add_argument('--artifact',required=True); p.add_argument('--cache',required=True); p.add_argument('--predictions',required=True); p.add_argument('--output',required=True)
    with threadpool_limits(limits=1): main(p.parse_args())
