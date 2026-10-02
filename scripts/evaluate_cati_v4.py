"""Bounded registered nested V4 probability selection and separate payoff validation."""
from __future__ import annotations
import argparse,csv,hashlib,json,subprocess,sys,time
from pathlib import Path
REPO=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(REPO/'scripts'),str(REPO/'backends/bot-backend'),str(REPO/'backends/shared')]
import numpy as np
import psutil
from threadpoolctl import threadpool_limits
from evaluate_cati_v3 import scores,limited
from cati_v3_residual_analysis import population_stability
from app.trading_intelligence.hashing import stable_hash
from app.trading_intelligence.forecast.v4_models import V4Probability,ConditionalPayoff,FEATURE_SCHEMA,CAT_NAMES,probability_gate as registered_probability_gate,payoff_gate as registered_payoff_gate


def error(y,p):
    return dict(mae=float(np.mean(abs(y-p))),rmse=float(np.sqrt(np.mean((y-p)**2))),bias=float(np.mean(p-y)))


def bucket_calibration(predicted,actual):
    edges=np.unique(np.quantile(predicted,np.linspace(0,1,6)))
    ids=np.minimum(len(edges)-2,np.searchsorted(edges,predicted,side='right')-1)
    rows=[]
    for i in range(len(edges)-1):
        ix=ids==i
        if not np.any(ix): continue
        rows.append(dict(samples=int(np.sum(ix)),prediction_mean=float(np.mean(predicted[ix])),
                         realized_mean=float(np.mean(actual[ix])),**error(actual[ix],predicted[ix])))
    return rows


def payoff_metrics(pred,targets,baseline_net,baseline_terminal):
    y=targets[:,0].astype(bool); net=targets[:,1]
    terminal=np.eye(3)[targets[:,5].astype(int)]
    buckets=bucket_calibration(pred['expected_net_R'],net)
    values=[b['realized_mean'] for b in buckets]
    inv=max((a-b for a,b in zip(values,values[1:])),default=0.)
    qmetrics={}
    for name,j in (('mfe',3),('mae',4)):
        qmetrics[name]={}
        for k,q in enumerate((.1,.5,.9)):
            residual=targets[:,j]-pred[name][:,k]
            qmetrics[name][str(q)]=dict(coverage=float(np.mean(targets[:,j]<=pred[name][:,k])),
                coverage_error=float(abs(np.mean(targets[:,j]<=pred[name][:,k])-q)),
                pinball_loss=float(np.mean(np.maximum(q*residual,(q-1)*residual))))
        qmetrics[name]['interval_10_90_coverage']=float(np.mean((targets[:,j]>=pred[name][:,0])&(targets[:,j]<=pred[name][:,2])))
    return dict(expected_net_R=error(net,pred['expected_net_R']),causal_baseline_net_R=error(net,baseline_net),
        conditional_positive_net_R=error(net[y],pred['conditional_positive_net_R'][y]),
        conditional_loss_net_R=error(net[~y],pred['conditional_loss_net_R'][~y]),
        terminal_multiclass_brier=float(np.mean(np.sum((terminal-pred['terminal'])**2,axis=1))),
        causal_terminal_baseline_brier=float(np.mean(np.sum((terminal-baseline_terminal)**2,axis=1))),
        quantiles=qmetrics,expected_R_buckets=buckets,maximum_bucket_absolute_bias=max(abs(b['bias']) for b in buckets),
        maximum_expectancy_quintile_inversion_R=max(inv,0.),expectancy_monotonic=bool(inv<=0),
        quantile_crossings=pred.get('quantile_crossings',{}))


def probability_gate(m,folds,gate):
    return registered_probability_gate(m,folds,gate)


def payoff_gate(m,folds,gate):
    return registered_payoff_gate(m,folds,gate)


def decomposition(p,y):
    rate=np.mean(y); rel=resolution=0.
    ids=np.minimum((p*10).astype(int),9)
    for i in range(10):
        ix=ids==i
        if not np.any(ix): continue
        weight=np.mean(ix); rel+=weight*(np.mean(p[ix])-np.mean(y[ix]))**2
        resolution+=weight*(np.mean(y[ix])-rate)**2
    uncertainty=rate*(1-rate); brier=np.mean((p-y)**2)
    return dict(reliability=float(rel),resolution=float(resolution),uncertainty=float(uncertainty),
                within_bin_residual=float(brier-(uncertainty-resolution+rel)))


def main(a):
    started=time.perf_counter(); out=Path(a.output); out.mkdir(parents=True,exist_ok=False)
    registry=json.loads((REPO/'docs/research/cati_v4_research_registry.json').read_text())
    if subprocess.check_output(['git','status','--porcelain'],cwd=REPO).strip(): raise ValueError('clean committed research provenance required')
    revision=subprocess.check_output(['git','rev-parse','HEAD'],cwd=REPO,text=True).strip()
    root=Path(a.cache); metadata=json.loads((root/'metadata.json').read_text())
    if hashlib.sha256((root/'development.npz').read_bytes()).hexdigest()!=metadata['cache_sha256']: raise ValueError('cache altered')
    if any(metadata[k]!=registry[k] for k in ('parent_library_hash','dataset_manifest_hash')): raise ValueError('dataset identity')
    with np.load(root/'development.npz',allow_pickle=False) as data: cache={k:data[k] for k in data.files}
    t,end,cats,g,c,targets=[cache[k] for k in ('times','label_ends','categories','geometry','context','targets')]
    if np.any(end>=metadata['holdout_start_ms']) or np.any(cache['context_source_times']!=t): raise ValueError('holdout/alignment violation')
    by_end=np.argsort(end,kind='stable'); sorted_end=end[by_end]
    cumul=np.cumsum(targets[:,0][by_end]); cumR=np.cumsum(targets[:,1][by_end]); cumterminal=np.cumsum(np.eye(3)[targets[:,5].astype(int)[by_end]],axis=0)
    boundaries=np.linspace(metadata['start'],metadata['stop'],7,dtype=np.int64)
    families=registry['families']; folds=[]; attempts=[]; outer=[]; family_outer={f:[] for f in families}; family_folds={f:[] for f in families}
    effects=[]; family_effects=[]; drift={}; peak=0
    def memory():
        nonlocal peak
        m=psutil.Process().memory_info(); peak=max(peak,getattr(m,'peak_wset',m.rss))
    def fit(idx,mode,C):
        ii=limited(idx,registry['maximum_probability_training_rows'])
        model=V4Probability(mode,C).fit(g[ii],c[ii],cats[ii],targets[ii,0]); memory(); return model
    for fold in range(1,6):
        tr=np.flatnonzero((t<boundaries[fold])&(end<boundaries[fold]))
        te=limited(np.flatnonzero((t>=boundaries[fold])&(t<boundaries[fold+1])),12000)
        inner=np.linspace(metadata['start'],boundaries[fold],4,dtype=np.int64)
        selection=[]; local={}
        for mode in families:
            candidate_scores=[]
            for C in registry['C_grid']:
                losses=[]
                for f in (1,2):
                    train=np.flatnonzero((t<inner[f])&(end<inner[f])); val=limited(np.flatnonzero((t>=inner[f])&(end<inner[f+1])),12000)
                    model=fit(train,mode,C); p=model.predict(g[val],c[val],cats[val]); n=np.searchsorted(sorted_end,t[val],side='left'); base=cumul[n-1]/n
                    losses.extend(((p-targets[val,0])**2).tolist())
                    attempts.append(dict(outer_fold=fold,inner_fold=f,mode=mode,C=C,code_revision=revision,
                        training_label_end=int(end[train].max()),validation_start=int(t[val].min()),
                        training_rows=len(limited(train,120000)),**scores(p,targets[val,0],base)))
                    (out/'attempts.json').write_text(json.dumps(attempts,indent=2))
                candidate_scores.append((float(np.mean(losses)),C))
            brier,C=min(candidate_scores); local[mode]=C; selection.append((brier,families.index(mode),C))
        _,chosen,C=min(selection); selected_mode=families[chosen]
        n=np.searchsorted(sorted_end,t[te],side='left'); base=cumul[n-1]/n; baseR=cumR[n-1]/n; baseterm=cumterminal[n-1]/n[:,None]
        paytr=limited(tr,registry['maximum_payoff_training_rows'])
        payoff=ConditionalPayoff().fit(g[paytr],c[paytr],cats[paytr],targets[paytr]); memory()
        for mode in families:
            model=fit(tr,mode,local[mode]); p=model.predict(g[te],c[te],cats[te]); pay=payoff.predict(g[te],c[te],cats[te],p); memory()
            metric=scores(p,targets[te,0],base)
            result=dict(fold=fold,mode=mode,C=local[mode],training_label_end=int(end[tr].max()),evaluation_start=int(t[te].min()),
                positive_rate=float(np.mean(targets[te,0])),prediction_mean=float(np.mean(p)),prediction_std=float(np.std(p)),**metric,
                payoff=payoff_metrics(pay,targets[te],baseR,baseterm))
            family_folds[mode].append(result)
            record=dict(indices=te,p=p,baseline=base,baseline_R=baseR,baseline_terminal=baseterm,**pay)
            family_outer[mode].append(record)
            family_effects.append(dict(fold=fold,mode=mode,C=local[mode],encoder=model.encoder.to_dict(),coefficients=model.coefficients.tolist(),intercept=model.intercept))
            if mode==selected_mode:
                folds.append(result); outer.append(record)
                effects.append(dict(fold=fold,mode=mode,C=local[mode],encoder=model.encoder.to_dict(),coefficients=model.coefficients.tolist(),intercept=model.intercept))
        drift[str(fold)]={name:population_stability(values[tr,j],values[te,j]) for values,names in
            ((g,('log_room','log_risk','log_cost','interaction')),(c,metadata['context_names'])) for j,name in enumerate(names)}
        print(json.dumps(dict(stage='outer',fold=fold,selected=selected_mode,skill=folds[-1]['brier_skill'],payoff_rmse=folds[-1]['payoff']['expected_net_R']['rmse'])),flush=True)
        (out/'outer_folds.json').write_text(json.dumps(folds,indent=2))
    def aggregate(records):
        keys=('indices','p','baseline','baseline_R','baseline_terminal','expected_net_R','conditional_positive_net_R','conditional_loss_net_R','terminal','timeout_gross_R','expected_gross_R','mfe','mae')
        return {k:np.concatenate([r[k] for r in records]) for k in keys}
    joined=aggregate(outer); ix=joined['indices']; metric=scores(joined['p'],targets[ix,0],joined['baseline'])
    paymetric=payoff_metrics(joined,targets[ix],joined['baseline_R'],joined['baseline_terminal'])
    probability_pass=probability_gate(metric,folds,registry['probability_gate'])
    payoff_pass=payoff_gate(paymetric,[f['payoff'] for f in folds],registry['payoff']['gate'])
    family_results={}
    for mode,records in family_outer.items():
        j=aggregate(records); metrics=scores(j['p'],targets[j['indices'],0],j['baseline']); payoff_result=payoff_metrics(j,targets[j['indices']],j['baseline_R'],j['baseline_terminal'])
        family_results[mode]=dict(metrics=metrics,folds=family_folds[mode],payoff=payoff_result,
            probability_gate_pass=probability_gate(metrics,family_folds[mode],registry['probability_gate']))
        family_results[mode]['candidate_id']='cati_v4_family_'+stable_hash(dict(registry_hash=stable_hash(registry),mode=mode,code_revision=revision,metrics=metrics,folds=family_folds[mode]))[:24]
        np.savez_compressed(out/f'family_{families.index(mode)}_outer_predictions.npz',**j)
    final_mode=folds[-1]['mode']; final_C=folds[-1]['C']; train=np.flatnonzero(end<metadata['stop'])
    final=fit(train,final_mode,final_C); paytrain=limited(train,registry['maximum_payoff_training_rows'])
    final_payoff=ConditionalPayoff().fit(g[paytrain],c[paytrain],cats[paytrain],targets[paytrain]); memory()
    v3=np.asarray([float(r['p']) for r in csv.DictReader(open(a.v3_predictions,newline=''))])
    old=decomposition(v3,targets[ix,0]); new=decomposition(joined['p'],targets[ix,0])
    gain=dict(v3=old,v4=new,discrimination_gain_binned_resolution=new['resolution']-old['resolution'],
              calibration_gain_binned_reliability=old['reliability']-new['reliability'],fitted_calibration_layer_gain=0.,
              explanation='Fixed ten-bin empirical decomposition is descriptive; within-bin residual prevents an exact discrimination/calibration attribution.')
    breakdown={}
    for j,name in enumerate(CAT_NAMES):
        breakdown[name]={}
        for key in sorted(set(cats[ix,j])):
            mask=cats[ix,j]==key
            breakdown[name][str(key)]=scores(joined['p'][mask],targets[ix[mask],0],joined['baseline'][mask])
    payload=dict(role=registry['role'],feature_schema=FEATURE_SCHEMA,registry_hash=stable_hash(registry),
        parent_library_hash=metadata['parent_library_hash'],dataset_manifest_hash=metadata['dataset_manifest_hash'],
        cache_metadata=metadata,code_revision=revision,source_tree_dirty=False,holdout_start_ms=metadata['holdout_start_ms'],
        training_label_end=int(end[train].max()),training_rows=len(limited(train,120000)),runtime_eligible=False,
        calibration_status='RESEARCH_ONLY',development_status='DEVELOPMENT_GATE_PASS' if probability_pass else 'REJECTED_PRE_HOLDOUT',
        probability_gate_pass=probability_pass,payoff_gate_pass=payoff_pass,model_ready=bool(probability_pass and payoff_pass),
        probability_model=final.to_dict(),payoff_model=final_payoff.to_dict(),metrics=metric,payoff_validation=paymetric,
        folds=folds,family_results=family_results,attempts=attempts,effect_drift=effects,family_effect_drift=family_effects,feature_drift=drift,
        breakdown=breakdown,brier_gain_components=gain,inspection_history=registry['inspection_history'],holdout_query_count=0)
    payload['library_hash']=stable_hash(payload); payload['candidate_id']='cati_v4_'+payload['library_hash'][:24]
    (out/'v4_model.json').write_text(json.dumps(payload,sort_keys=True,indent=2))
    np.savez_compressed(out/'outer_predictions.npz',**{k:v for k,v in joined.items()})
    performance=dict(peak_working_set_bytes=peak,evaluation_seconds=time.perf_counter()-started,parallel_fit_workers=1)
    (out/'performance.json').write_text(json.dumps(performance,indent=2))
    print(json.dumps(dict(candidate=payload['candidate_id'],metrics=metric,probability_gate=probability_pass,payoff_gate=payoff_pass,performance=performance)),flush=True)


if __name__=='__main__':
    p=argparse.ArgumentParser(); p.add_argument('--cache',required=True); p.add_argument('--v3-predictions',required=True); p.add_argument('--output',required=True)
    with threadpool_limits(limits=1): main(p.parse_args())
