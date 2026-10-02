"""Registered, bounded-memory nested chronological V3 development run.

Reads only the immutable pre-holdout library. No database connection or holdout
query exists in this program. Artifacts require a clean committed source tree.
"""
from __future__ import annotations
import argparse, csv, hashlib, json, subprocess, sys, time
from pathlib import Path
from datetime import datetime, timezone
REPO = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(REPO/'backends/bot-backend'),str(REPO/'backends/shared')]
import numpy as np
import psutil
from sklearn.metrics import roc_auc_score, average_precision_score, log_loss
from app.trading_intelligence.hashing import TextHasher, stable_hash
from app.trading_intelligence.forecast.artifact import manifest_hash_of, row_from_dict
from app.trading_intelligence.forecast.information_conditioning import (
    InformationConditioner, causal_features, matured_indices, FEATURE_SCHEMA, ESTIMATOR)
from app.trading_intelligence.forecast.calibration_report import _ece


def scores(p,y,base):
    b=float(np.mean((p-y)**2)); bb=float(np.mean((base-y)**2))
    top=np.argsort(-p,kind='stable')[:max(1,len(p)//10)]
    return dict(samples=len(y), brier=b, causal_baseline_brier=bb, brier_skill=1-b/bb,
                ece=_ece(list(zip(p.tolist(),y.tolist())),10),
                roc_auc=float(roc_auc_score(y,p)) if len(np.unique(y))==2 else None,
                pr_auc=float(average_precision_score(y,p)), log_loss=float(log_loss(y,p,labels=[0,1])),
                top_decile_lift=float(np.mean(y[top])/np.mean(y)) if np.mean(y) else None)


def limited(idx,n):
    return idx[::max(1,int(np.ceil(len(idx)/n)))]


def main(args):
    started=time.perf_counter(); peak=0
    def memory():
        nonlocal peak
        info=psutil.Process().memory_info()
        peak=max(peak,getattr(info,'peak_wset',info.rss))
    registry=json.loads((REPO/'docs/research/cati_v3_research_registry.json').read_text())
    if subprocess.check_output(['git','status','--porcelain'],cwd=REPO).strip():
        raise RuntimeError('commit implementation before producing governed provenance')
    revision=subprocess.check_output(['git','rev-parse','HEAD'],cwd=REPO,text=True).strip()
    root=Path(args.library); manifest=json.loads((root/'manifest.json').read_text())
    assert manifest_hash_of(manifest)==manifest['manifest_hash']
    assert manifest['library_hash']==registry['parent_library_hash']
    assert manifest['governance']['dataset_manifest_hash']==registry['dataset_manifest_hash']
    out=Path(args.output); out.mkdir(parents=True,exist_ok=False)
    features=[]; times=[]; ends=[]; ys=[]; groups=[]; references={}
    h=TextHasher(); hold=manifest['governance']['holdout_start_ms']
    count=0
    with (root/'rows.jsonl').open(encoding='utf-8',newline='') as f:
        for i,line in enumerate(f):
            h.update(line); count+=1
            if i%12: continue
            d=json.loads(line); l=d['label']; x=d['continuous_features']; dims=d['cohort_dimensions']
            t=l['decision_time']; end=t+l['terminal_horizon_bars']*900000
            if end>=hold: raise RuntimeError('holdout overlap')
            features.append(causal_features(dimensions=dims,room=x['room_to_target_R'],
                risk_fraction=x['initial_risk_fraction'],timeframe=manifest['actual_interval'],
                horizon=l['terminal_horizon_bars'],instrument=l['instrument_key']['venue_symbol']))
            times.append(t); ends.append(end); ys.append(int(l['net_profitable']))
            groups.append({**{k:dims[k] for k in ('setup_family','side','dominant_regime','volatility_bucket')},
                           'year':str(datetime.fromtimestamp(t/1000,timezone.utc).year)})
            # Bounded distribution reference for the existing non-binary forecast fields.
            bucket=references.setdefault(l['setup_family'],[])
            if len(bucket)<512: bucket.append(d)
            if len(times)%10000==0:
                memory(); print(json.dumps({'stage':'stream','sampled':len(times),'rss':peak}),flush=True)
    if count!=manifest['row_count'] or h.hexdigest()!=manifest['rows_sha256']:
        raise RuntimeError('immutable source row identity mismatch')
    t=np.asarray(times); end=np.asarray(ends); y=np.asarray(ys)
    order=np.argsort(t,kind='stable'); t=t[order]; end=end[order]; y=y[order]
    features=[features[i] for i in order]; groups=[groups[i] for i in order]
    start,stop=manifest['start_time'],manifest['end_time']+1
    boundaries=np.linspace(start,stop,7,dtype=np.int64)
    rows=[]; folds=[]; attempts=[]
    def fit(idx,c):
        idx=limited(idx,registry['maximum_training_rows'])
        memory()
        model=InformationConditioner(c,registry['minimum_instrument_support']).fit([features[i] for i in idx],y[idx])
        memory(); return model
    for fold in range(1,6):
        train=matured_indices(t,end,int(boundaries[fold]))
        test=limited(np.flatnonzero((t>=boundaries[fold])&(t<boundaries[fold+1])),12000)
        inner_bounds=np.linspace(start,boundaries[fold],4,dtype=np.int64)
        inner_scores=[]
        for variant in registry['variants']:
            losses=[]
            for inner in (1,2):
                tr=matured_indices(t,end,int(inner_bounds[inner]))
                va=limited(np.flatnonzero((t>=inner_bounds[inner])&(end<inner_bounds[inner+1])),12000)
                model=fit(tr,variant['C']); p=model.predict([features[i] for i in va])
                losses.extend(((p-y[va])**2).tolist())
                by_end=np.argsort(end,kind='stable'); n=np.searchsorted(end[by_end],t[va],side='left')
                baseline=np.cumsum(y[by_end])[n-1]/n
                attempts.append(dict(outer_fold=fold,inner_fold=inner,variant=variant['variant_id'],
                    training_samples=len(limited(tr,120000)),validation_samples=len(va),
                    training_label_end=int(end[tr].max()),validation_start=int(t[va].min()),
                    **scores(p,y[va],baseline)))
                (out/'attempts.json').write_text(json.dumps(attempts,indent=2))
            inner_scores.append((float(np.mean(losses)),variant['C'],variant['variant_id']))
        _,c,variant_id=min(inner_scores)
        model=fit(train,c); p=model.predict([features[i] for i in test])
        # Expanding baseline uses all sampled matured outcomes available at EACH forecast.
        by_end=np.argsort(end,kind='stable'); sorted_end=end[by_end]; cum=np.cumsum(y[by_end])
        n=np.searchsorted(sorted_end,t[test],side='left'); base=cum[n-1]/n
        result=scores(p,y[test],base)
        folds.append(dict(fold=fold,selected_variant=variant_id,training_samples=len(limited(train,120000)),
            training_label_end=int(end[train].max()),evaluation_start=int(t[test].min()),
            evaluation_end=int(t[test].max()),**result))
        for j,i in enumerate(test): rows.append(dict(fold=fold,t=int(t[i]),y=int(y[i]),p=float(p[j]),baseline=float(base[j]),**groups[i]))
        print(json.dumps({'stage':'outer','result':folds[-1]}),flush=True)
        (out/'outer_folds.json').write_text(json.dumps(folds,indent=2))
    p=np.asarray([r['p'] for r in rows]); target=np.asarray([r['y'] for r in rows]); base=np.asarray([r['baseline'] for r in rows])
    metrics=scores(p,target,base); breakdown={}
    for key in ('setup_family','side','dominant_regime','volatility_bucket','year','fold'):
        breakdown[key]={}
        for v in sorted(set(str(r[key]) for r in rows)):
            idx=np.asarray([i for i,r in enumerate(rows) if str(r[key])==v])
            breakdown[key][v]=scores(p[idx],target[idx],base[idx])
    # Select final deployment parameter using inner development scores only.
    c=min((np.mean([a['brier'] for a in attempts if a['variant']==v['variant_id']]),v['C']) for v in registry['variants'])[1]
    final_train=matured_indices(t,end,stop); final_model=fit(final_train,c)
    passed=(metrics['samples']>=300 and metrics['brier_skill']>=.02 and metrics['ece']<=.05
            and all(f['brier_skill']>0 for f in folds))
    payload=dict(role=registry['role'],feature_schema=FEATURE_SCHEMA,estimator=ESTIMATOR,
        registry_hash=stable_hash(registry),dataset_manifest_hash=registry['dataset_manifest_hash'],
        parent_library_hash=manifest['library_hash'],parent_manifest_hash=manifest['manifest_hash'],
        code_revision=revision,source_tree_dirty=False,holdout_start_ms=hold,
        training_label_end=int(end[final_train].max()),runtime_eligible=False,
        calibration_status='CALIBRATED' if passed else 'RESEARCH_ONLY',model=final_model.to_dict(),
        metrics=metrics,folds=folds,attempts=attempts,breakdown=breakdown,
        reference_rows=[d for v in references.values() for d in v],
        reference_distribution='Deterministic capped family reference; ancillary R/terminal statistics are research approximations, not calibrated V3 conditional estimates.')
    payload['library_hash']=stable_hash(payload); payload['candidate_id']='cati_v3_'+payload['library_hash'][:24]
    (out/'v3_model.json').write_text(json.dumps(payload,sort_keys=True,indent=2),encoding='utf-8')
    with (out/'predictions.csv').open('w',newline='') as f:
        writer=csv.DictWriter(f,fieldnames=list(rows[0])); writer.writeheader(); writer.writerows(rows)
    memory()
    from app.trading_intelligence.forecast.v3_artifact import load_v3_artifact
    s=time.perf_counter(); lib,_=load_v3_artifact(out,expected_hash=payload['library_hash'],mode='DEVELOPMENT')
    load_seconds=time.perf_counter()-s
    s=time.perf_counter(); lib.conditioner.predict([features[0]])
    performance=dict(peak_rss_bytes=peak,evaluation_seconds=time.perf_counter()-started,
                     artifact_load_seconds=load_seconds,probability_forecast_seconds=time.perf_counter()-s)
    (out/'performance.json').write_text(json.dumps(performance,indent=2))
    print(json.dumps(dict(candidate_id=payload['candidate_id'],metrics=metrics,gate=passed,performance=performance)),flush=True)


if __name__=='__main__':
    ap=argparse.ArgumentParser(); ap.add_argument('--library',required=True); ap.add_argument('--output',required=True)
    main(ap.parse_args())
