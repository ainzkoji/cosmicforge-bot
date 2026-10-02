"""Registered bounded joint-state nested evaluation. No source/holdout queries."""
from __future__ import annotations
import argparse,gc,hashlib,json,subprocess,sys,time
from pathlib import Path
REPO=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(REPO/'scripts'),str(REPO/'backends/bot-backend'),str(REPO/'backends/shared')]
import numpy as np
import psutil
from threadpoolctl import threadpool_limits
from evaluate_cati_v3 import limited,scores
from evaluate_cati_v4 import payoff_metrics,decomposition
from cati_v3_residual_analysis import population_stability
from app.trading_intelligence.hashing import stable_hash
from app.trading_intelligence.forecast.v4_models import probability_gate,payoff_gate
from app.trading_intelligence.forecast.v5_models import JointProbability,ConditionalAtoms,FEATURE_SCHEMA,STATES,TERMINAL,joint_labels,coherence_metrics,coherence_pass,time_gate,decision_gate


def time_metrics(cdf,baseline,events,terminal):
    actual=np.arange(1,49)[None,:]>=events[:,None]
    pmf=np.diff(np.column_stack((np.zeros(len(cdf)),cdf)),axis=1)
    bmass=np.diff(np.column_stack((np.zeros(len(cdf)),baseline)),axis=1)
    mean=pmf@np.arange(1,49); bmean=bmass@np.arange(1,49)
    rows=terminal!=2  # TIMEOUT is censoring, not a target/stop event.
    metrics=dict(event_rows=int(rows.sum()),timeout_censoring_rows=int((~rows).sum()),
        conditional_MAE=float(np.mean(abs(mean[rows]-events[rows]))),
        causal_terminal_frequency_MAE=float(np.mean(abs(bmean[rows]-events[rows]))),
        conditional_CRPS=float(np.mean(np.sum((cdf[rows]-actual[rows])**2,axis=1))),
        causal_terminal_frequency_CRPS=float(np.mean(np.sum((baseline[rows]-actual[rows])**2,axis=1))),
        conditional_log_loss=float(-np.mean(np.log(np.maximum(pmf[rows,events[rows]-1],1e-15)))),
        causal_terminal_frequency_log_loss=float(-np.mean(np.log(np.maximum(bmass[rows,events[rows]-1],1e-15)))))
    for q in (.1,.5,.9):
        quantile=np.argmax(cdf>=q,axis=1)+1
        metrics[f'coverage_{q}']=float(np.mean(events[rows]<=quantile[rows]))
    return metrics


def main(a):
    started=time.perf_counter(); root=Path(a.cache); out=Path(a.output)
    registry=json.loads((REPO/'docs/research/cati_v5_research_registry.json').read_text())
    if subprocess.check_output(['git','status','--porcelain'],cwd=REPO).strip(): raise ValueError('commit clean implementation first')
    revision=subprocess.check_output(['git','rev-parse','HEAD'],cwd=REPO,text=True).strip()
    out.mkdir(parents=True,exist_ok=False)
    metadata=json.loads((root/'metadata.json').read_text())
    for k in ('parent_library_hash','dataset_manifest_hash'):
        if metadata[k]!=registry[k]: raise ValueError('wrong dataset identity')
    for name,sha in metadata['array_sha256'].items():
        if hashlib.sha256((root/name).read_bytes()).hexdigest()!=sha: raise ValueError('cache identity mismatch')
    cache={p.stem:np.load(p,mmap_mode='r',allow_pickle=False) for p in root.glob('*.npy')}
    t,end,g,c,cats,targets,events=[cache[k] for k in ('times','label_ends','geometry','context','categories','targets','event_bars')]
    if np.any(end>=metadata['holdout_start_ms']) or np.any(cache['context_source_times']!=t): raise ValueError('holdout/context boundary')
    states=joint_labels(targets)
    by_end=np.argsort(end,kind='stable'); sorted_end=end[by_end]
    cum=np.cumsum(targets[by_end,0]); cumR=np.cumsum(targets[by_end,1]); cumterminal=np.cumsum(np.eye(3)[targets[by_end,5].astype(int)],axis=0)
    boundaries=np.linspace(metadata['start'],metadata['stop'],7,dtype=np.int64)
    architectures=registry['architectures']; variants=registry['variants']; results={k:[] for k in architectures}; outer={k:[] for k in architectures}
    folds=[]; selected=[]; attempts=[]; traces=[]; distributions=[]; drift={}
    try: psutil.Process().nice(psutil.BELOW_NORMAL_PRIORITY_CLASS)
    except (AttributeError,psutil.Error): pass
    def fit(idx,spec,cutoff):
        ii=limited(idx,60000 if spec['architecture']=='JOINT_HISTOGRAM_BOOSTING' else 120000)
        return JointProbability(spec).fit(g[ii],c[ii],cats[ii],targets[ii],t[ii],cutoff)
    def baseline(ii):
        n=np.searchsorted(sorted_end,t[ii],side='left')
        return dict(baseline=cum[n-1]/n,baseline_R=cumR[n-1]/n,baseline_terminal=cumterminal[n-1]/n[:,None])
    def predict(model,distribution,ii,base_time):
        records=[]
        for start in range(0,len(ii),registry['prediction_chunk_rows']):
            ix=ii[start:start+registry['prediction_chunk_rows']]
            joint=model.predict(g[ix],c[ix],cats[ix]); pred=distribution.predict(g[ix],c[ix],cats[ix],joint)
            actual_terminal=targets[ix,5].astype(int)
            time_mass=np.column_stack([np.zeros(len(ix))]*48)
            for term in range(3):
                rows=actual_terminal==term
                mass=pred['joint_time_pmf'][rows][:,TERMINAL==term,:].sum(axis=1)
                time_mass[rows]=mass/pred['terminal'][rows,term,None]
            keep=('joint','p','terminal','expected_net_R','conditional_positive_net_R','conditional_loss_net_R',
                'expected_gross_R','state_net_R_mean','mfe','mae','net_R_quantiles')
            record={k:pred[k] for k in keep}; record.update(indices=ix,time_cdf=np.cumsum(time_mass,axis=1),
                time_baseline_cdf=np.cumsum(base_time[actual_terminal],axis=1),**baseline(ix)); records.append(record)
        return aggregate(records)
    def metric(record):
        ix=record['indices']; coherent=coherence_metrics(record)
        return dict(**scores(record['p'],targets[ix,0],record['baseline']),coherence=coherent,
            payoff=payoff_metrics(record,targets[ix],record['baseline_R'],record['baseline_terminal']),
            time_validation=time_metrics(record['time_cdf'],record['time_baseline_cdf'],events[ix],targets[ix,5]),
            positive_rate=float(targets[ix,0].mean()),prediction_mean=float(record['p'].mean()),prediction_std=float(record['p'].std()))
    for fold in range(1,6):
        cutoff=boundaries[fold]; tr=np.flatnonzero((t<cutoff)&(end<cutoff))
        te=limited(np.flatnonzero((t>=cutoff)&(t<boundaries[fold+1])),12000)
        inner=np.linspace(metadata['start'],cutoff,4,dtype=np.int64); selection=[]
        for spec in variants:
            if 'equivalent_to' in spec:
                other=next(s for s in selection if s[2]['id']==spec['equivalent_to']); selection.append((other[0],variants.index(spec),spec)); continue
            loss=[]
            for f in (1,2):
                train=np.flatnonzero((t<inner[f])&(end<inner[f])); val=limited(np.flatnonzero((t>=inner[f])&(end<inner[f+1])),12000)
                model=fit(train,spec,inner[f]); joint=model.predict(g[val],c[val],cats[val]); p=joint[:,[0,3]].sum(axis=1)
                loss.extend(((p-targets[val,0])**2).tolist())
                attempts.append(dict(outer_fold=fold,inner_fold=f,variant=spec['id'],training_label_end=int(end[train].max()),
                    evaluation_start=int(t[val].min()),training_cutoff=int(inner[f]),**scores(p,targets[val,0],baseline(val)['baseline'])))
                del model; gc.collect()
                (out/'attempts.json').write_text(json.dumps(attempts,indent=2))
            selection.append((float(np.mean(loss)),variants.index(spec),spec))
        chosen=min(selection,key=lambda x:(x[0],x[1]))[2]
        paytrain=limited(tr,60000); distribution=ConditionalAtoms().fit(g[paytrain],c[paytrain],cats[paytrain],targets[paytrain],events[paytrain])
        distributions.append(dict(fold=fold,model=distribution.to_dict()))
        base_time=np.zeros((3,48))
        for term in range(3):
            mask=targets[tr,5]==term
            hist=np.bincount(events[tr][mask].astype(int)-1,minlength=48)+.5
            if term==2: hist=np.zeros(48); hist[-1]=1.
            base_time[term]=hist/hist.sum()
        for architecture in architectures:
            spec=min([s for s in selection if s[2]['architecture']==architecture],key=lambda x:(x[0],x[1]))[2]
            model=fit(tr,spec,cutoff); record=predict(model,distribution,te,base_time); summary=metric(record)
            summary.update(fold=fold,variant=spec['id'],architecture=architecture,training_label_end=int(end[tr].max()),evaluation_start=int(t[te].min()))
            results[architecture].append(summary); outer[architecture].append(record)
            traces.append(dict(fold=fold,architecture=architecture,model=model.to_dict()))
            if spec['id']==chosen['id']:
                folds.append(summary); selected.append(record)
            del model; gc.collect()
        # A duplicate no-decay C choice is resolved by earlier registry A tie.
        assert len(folds)==fold
        drift[str(fold)]={name:population_stability(c[tr,j],c[te,j]) for j,name in enumerate(metadata['context_names'])}
        (out/'outer_folds.json').write_text(json.dumps(folds,indent=2))
        print(json.dumps(dict(stage='outer',fold=fold,variant=chosen['id'],skill=folds[-1]['brier_skill'],payoff_rmse=folds[-1]['payoff']['expected_net_R']['rmse'],peak_bytes=psutil.Process().memory_info().peak_wset)),flush=True)
        del distribution; gc.collect()
    joined=aggregate(selected); pooled=metric(joined); coherent=coherence_pass(pooled['coherence']) and all(coherence_pass(f['coherence']) for f in folds)
    probability_pass=probability_gate(pooled,folds,registry['probability_gate']) and coherent
    paypass=payoff_gate(pooled['payoff'],[f['payoff'] for f in folds],registry['payoff_gate'])
    decision_pass=decision_gate(pooled['payoff'],folds,pooled['time_validation'],registry,coherent)
    family={}
    for architecture in architectures:
        record=aggregate(outer[architecture]); m=metric(record)
        family[architecture]=dict(metrics=m,folds=results[architecture],model_ready=probability_gate(m,results[architecture],registry['probability_gate']) and coherence_pass(m['coherence']),
            decision_payoff_ready=decision_gate(m['payoff'],results[architecture],m['time_validation'],registry,coherence_pass(m['coherence'])))
        np.savez_compressed(out/f'architecture_{architectures.index(architecture)}_outer_predictions.npz',**record)
        del record
    train=np.flatnonzero(end<metadata['stop']); final_spec=next(s for s in variants if s['id']==folds[-1]['variant'])
    final=fit(train,final_spec,metadata['stop']); ix=limited(train,60000)
    final_distribution=ConditionalAtoms().fit(g[ix],c[ix],cats[ix],targets[ix],events[ix])
    v4=json.loads((REPO/'docs/research/artifacts/cati_v4_a71dc63a483d307c3b6cfbeb/v4_model.json').read_text())
    old=np.load(REPO/'data/research/calibration_diagnostics/v4_registered_evaluation_parity/outer_predictions.npz',allow_pickle=False)
    assert np.array_equal(old['indices'],joined['indices'])
    components=dict(v4=decomposition(old['p'],targets[joined['indices'],0]),v5=decomposition(joined['p'],targets[joined['indices'],0]),fitted_calibration_layer_gain=0.)
    old.close()
    payload=dict(role=registry['role'],feature_schema=FEATURE_SCHEMA,registry_hash=stable_hash(registry),code_revision=revision,source_tree_dirty=False,
        parent_library_hash=metadata['parent_library_hash'],dataset_manifest_hash=metadata['dataset_manifest_hash'],cache_metadata=metadata,
        holdout_start_ms=metadata['holdout_start_ms'],training_label_end=int(end[train].max()),training_rows=len(limited(train,120000)),
        holdout_query_count=0,runtime_eligible=False,calibration_status='RESEARCH_ONLY',joint_states=STATES,
        probability_model=final.to_dict(),conditional_model=final_distribution.to_dict(),metrics=pooled,folds=folds,
        architecture_results=family,attempts=attempts,outer_probability_models=traces,outer_conditional_models=distributions,
        feature_drift=drift,brier_gain_components=components,coherence_pass=coherent,payoff_gate_pass=paypass,
        model_ready=probability_pass,decision_payoff_ready=decision_pass,
        development_status='DEVELOPMENT_GATE_PASS' if probability_pass else 'REJECTED_PRE_HOLDOUT',inspection_history=registry['inspection_history'])
    payload['library_hash']=stable_hash(payload); payload['candidate_id']='cati_v5_'+payload['library_hash'][:24]
    (out/'v5_model.json').write_text(json.dumps(payload,sort_keys=True,indent=2)); np.savez_compressed(out/'outer_predictions.npz',**joined)
    performance=dict(peak_working_set_bytes=psutil.Process().memory_info().peak_wset,evaluation_seconds=time.perf_counter()-started,parallel_fit_workers=1)
    (out/'performance.json').write_text(json.dumps(performance,indent=2))
    print(json.dumps(dict(candidate=payload['candidate_id'],metrics=pooled,model_ready=probability_pass,decision_payoff_ready=decision_pass,performance=performance)),flush=True)


def aggregate(records):
    return {k:np.concatenate([r[k] for r in records]) for k in records[0]}


if __name__=='__main__':
    p=argparse.ArgumentParser(); p.add_argument('--cache',required=True); p.add_argument('--output',required=True)
    with threadpool_limits(limits=1): main(p.parse_args())
