"""Join every V3 outer prediction to its deterministic causal sample; diagnose, never select subsets."""
from __future__ import annotations
import argparse,csv,json,sys
from pathlib import Path
REPO=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(REPO/'scripts'),str(REPO/'backends/bot-backend'),str(REPO/'backends/shared')]
import numpy as np
from evaluate_cati_v3 import scores,limited


def population_stability(train,test):
    edges=np.unique(np.quantile(train,np.linspace(0,1,11))); edges[0]=-np.inf; edges[-1]=np.inf
    a=np.histogram(train,edges)[0]/len(train); b=np.histogram(test,edges)[0]/len(test)
    a=np.maximum(a,1e-6); b=np.maximum(b,1e-6)
    return float(np.sum((b-a)*np.log(b/a)))


def main(a):
    root=Path(a.cache); meta=json.loads((root/'metadata.json').read_text())
    with np.load(root/'development.npz',allow_pickle=False) as data:
        cache={name:data[name] for name in data.files}
    v3=list(csv.DictReader(open(a.predictions,newline='')))
    t=cache['times']; end=cache['label_ends']; boundaries=np.linspace(meta['start'],meta['stop'],7,dtype=np.int64)
    indices=np.concatenate([limited(np.flatnonzero((t>=boundaries[f])&(t<boundaries[f+1])),12000) for f in range(1,6)])
    assert len(indices)==len(v3)
    for i,r in zip(indices,v3):
        assert t[i]==int(r['t']) and cache['targets'][i,0]==int(r['y'])
        assert all(cache['categories'][i,j]==r[k] for j,k in enumerate(('setup_family','side','dominant_regime','volatility_bucket')))
    p=np.asarray([float(r['p']) for r in v3]); y=cache['targets'][indices,0]; base=np.asarray([float(r['baseline']) for r in v3]); residual=y-p
    geometry=cache['geometry'][indices]; cost=np.expm1(geometry[:,2]); folds=np.asarray([int(r['fold']) for r in v3])
    numeric=dict(room_to_target_R=np.exp(geometry[:,0]),initial_risk_fraction=np.exp(geometry[:,1]),modeled_cost_R=cost)
    for j,name in enumerate(meta['context_names']): numeric[name]=cache['context'][indices,j]
    numeric['calendar_time']=t[indices]/86400000
    age=np.empty(len(indices))
    drift={}
    for f in range(1,6):
        tr=np.flatnonzero(end<boundaries[f]); ii=np.flatnonzero(folds==f)
        age[ii]=(t[indices[ii]]-np.mean(t[tr]))/86400000
        drift[str(f)]={name:population_stability(cache['geometry'][tr,j],geometry[ii,j]) for j,name in enumerate(('log_room','log_risk','log_cost','interaction'))}
        drift[str(f)]['regime_distribution']={regime:dict(training=float(np.mean(cache['categories'][tr,2]==regime)),evaluation=float(np.mean(cache['categories'][indices[ii],2]==regime))) for regime in sorted(set(cache['categories'][:,2]))}
    numeric['mean_training_age_days']=age
    diagnostics={}
    def group(ix):
        result=scores(p[ix],y[ix],base[ix]); result.update(residual_mean=float(np.mean(residual[ix])),prediction_mean=float(np.mean(p[ix])),prediction_std=float(np.std(p[ix])),positive_rate=float(np.mean(y[ix])))
        return result
    for key,col in (('family',0),('side',1),('regime',2),('volatility',3),('instrument',4)):
        vals=cache['categories'][indices,col]
        diagnostics[key]={v:group(np.flatnonzero(vals==v)) for v in sorted(set(vals))}
    for name,values in numeric.items():
        edges=np.unique(np.quantile(values,np.linspace(0,1,6)))
        buckets=np.minimum(len(edges)-2,np.searchsorted(edges,values,side='right')-1)
        diagnostics[name]=[{**group(np.flatnonzero(buckets==b)), 'lower':float(edges[b]),'upper':float(edges[b+1])} for b in range(len(edges)-1) if np.any(buckets==b)]
    diagnostics['fold']={str(f):group(np.flatnonzero(folds==f)) for f in range(1,6)}
    diagnostics['early_vs_late']={'folds_1_2':group(np.flatnonzero(folds<=2)),'folds_3_5':group(np.flatnonzero(folds>=3))}
    interactions={}
    for family in sorted(set(cache['categories'][indices,0])):
        ii=np.flatnonzero(cache['categories'][indices,0]==family)
        interactions[family]={}
        for name in ('room_to_target_R','initial_risk_fraction','modeled_cost_R'):
            values=numeric[name][ii]; edges=np.unique(np.quantile(values,np.linspace(0,1,6)))
            buckets=np.minimum(len(edges)-2,np.searchsorted(edges,values,side='right')-1)
            interactions[family][name]=[{**group(ii[buckets==b]),'lower':float(edges[b]),'upper':float(edges[b+1])} for b in range(len(edges)-1) if np.any(buckets==b)]
    result=dict(v3_candidate='cati_v3_f97c1349086b6c1068ee49c0',inspection_history='All pre-holdout data remains inspected DEVELOPMENT; observational residuals do not prove causal attribution.',
        samples=len(p),diagnostics=diagnostics,family_geometry_interactions=interactions,feature_drift=drift,
        failure_mode='MIXED',subset_selection=False,holdout_query_count=0)
    out=Path(a.output); out.mkdir(parents=True,exist_ok=False)
    (out/'residual_summary.json').write_text(json.dumps(result,indent=2))
    with (out/'outer_residuals.csv').open('w',newline='') as f:
        keys=['sample_index','fold','instrument','family','side','regime','volatility','p','y','residual','squared_error',*numeric]
        w=csv.DictWriter(f,fieldnames=keys); w.writeheader()
        for j,i in enumerate(indices): w.writerow(dict(sample_index=int(i),fold=int(folds[j]),instrument=cache['categories'][i,4],family=cache['categories'][i,0],side=cache['categories'][i,1],regime=cache['categories'][i,2],volatility=cache['categories'][i,3],p=p[j],y=y[j],residual=residual[j],squared_error=residual[j]**2,**{k:v[j] for k,v in numeric.items()}))
    print(json.dumps(diagnostics['early_vs_late']),flush=True)


if __name__=='__main__':
    p=argparse.ArgumentParser(); p.add_argument('--cache',required=True); p.add_argument('--predictions',required=True); p.add_argument('--output',required=True)
    main(p.parse_args())
