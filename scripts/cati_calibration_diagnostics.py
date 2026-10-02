"""Read-only, holdout-blind replay of V1's scored probabilities using sufficient statistics.

Never invokes the calibration CLI (which overwrites calibration.json). Outputs only
to a new diagnosis directory; original library and calibration remain immutable.
"""
from __future__ import annotations
import argparse, csv, hashlib, json, math, sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(REPO / 'backends/bot-backend'), str(REPO / 'backends/shared')]
import numpy as np
from app.trading_intelligence.forecast.cohorts import DIMENSION_ORDER, BACKOFF_LEVELS
from app.trading_intelligence.forecast.engine import MIN_USABLE_RAW_SUPPORT, PRIOR_STRENGTH
from app.trading_intelligence.forecast.posterior import beta_binomial_posterior, dirichlet_multinomial_posterior
from app.trading_intelligence.forecast.calibration_report import CalibrationPolicy, CausalCalibrationPolicy, CalibrationReport, build_calibration_record, _ece, _support_bucket
from app.trading_intelligence.forecast.artifact import manifest_hash_of
from app.trading_intelligence.hashing import TextHasher, stable_hash

CLASSES = ('TARGET_BEFORE_STOP', 'STOP_BEFORE_TARGET', 'TIMEOUT')

def score_counts(local, family):
    """Identical arithmetic to V1 engine with uniform weights (ESS = n)."""
    n, wins = float(local[0]), float(local[1])
    fn = float(family[0])
    broad = float(family[1]) / fn if fn else .5
    beta = beta_binomial_posterior(weighted_win_rate=wins/n, ess=n,
        prior_alpha=broad*PRIOR_STRENGTH, prior_beta=(1-broad)*PRIOR_STRENGTH)
    multi = dirichlet_multinomial_posterior(
        weighted_counts={c: float(local[i+2])/n for i,c in enumerate(CLASSES)}, ess=n,
        prior={c: PRIOR_STRENGTH*float(family[i+2])/fn if fn else PRIOR_STRENGTH/3
               for i,c in enumerate(CLASSES)})
    return beta.p_mean, multi

def metrics(records, baseline_key='baseline_v1'):
    if not records:
        return {'samples':0}
    p=[r['p'] for r in records]; y=[r['y'] for r in records]
    b=sum((a-b)**2 for a,b in zip(p,y))/len(y)
    base=sum((r[baseline_key]-r['y'])**2 for r in records)/len(y)
    return {'samples':len(y),'model_brier':b,'baseline_brier':base,
            'brier_skill':1-b/base if base else None,
            'ece':_ece(list(zip(p,y)),10),'positive_rate':sum(y)/len(y),
            'mean_prediction':sum(p)/len(p)}

def grouped(records, key):
    groups={}
    for r in records: groups.setdefault(str(r[key]),[]).append(r)
    return {k:{'legacy':metrics(v),'causal':metrics(v,'baseline_causal'),
               'training_start':min(r['train_start'] for r in v),
               'training_end':max(r['train_end'] for r in v),
               'evaluation_start':min(r['decision_time'] for r in v),
               'evaluation_end':max(r['decision_time'] for r in v)} for k,v in sorted(groups.items())}

def write_csv(path, rows):
    if not rows: return
    with path.open('w',newline='',encoding='utf-8') as f:
        writer=csv.DictWriter(f,fieldnames=list(rows[0]));writer.writeheader();writer.writerows(rows)

def run(library, original_record, output, causal_candidate=False):
    if output.exists(): raise RuntimeError('Refusing to overwrite diagnostic run')
    m=json.loads((library/'manifest.json').read_text()); record=json.loads(original_record.read_text())
    assert manifest_hash_of(m)==m['manifest_hash']
    assert record['library_hash']==(m['governance']['parent_library_hash'] if causal_candidate else m['library_hash'])
    policy=CalibrationPolicy(**{**record['policy'],'support_bucket_edges':tuple(record['policy']['support_bucket_edges'])})
    assert policy.policy_hash==record['policy_hash']==CalibrationPolicy().policy_hash
    n=m['row_count']; hold=m['governance']['holdout_start_ms']; span=900000
    embargo=m['label_horizon_bars']*span
    dtype=np.dtype([('t','i8'),('cid','i4'),('asset','i2'),('y','i1'),('terminal','i1'),
                    ('horizon','i2'),('gross','f8'),('net','f8'),('cost','f8'),('room','f8'),('risk','f8')])
    a=np.empty(n,dtype=dtype); cohorts=[]; cids={}; assets=[]; aids={}; label_ids=[]
    full_counts=[]; violations=Counter(); reason_counts=Counter(); feature_names=Counter()
    hash_rows=TextHasher(); content_hash=hashlib.sha256(b'['); previous=None
    audit_rows=[]
    with (library/'rows.jsonl').open(encoding='utf-8',newline='') as f:
        for i,line in enumerate(f):
            if i>=n: raise RuntimeError('Extra rows')
            hash_rows.update(line); raw=line.rstrip('\n'); d=json.loads(raw); l=d['label']; dims=d['cohort_dimensions']; features=d['continuous_features']
            if i: content_hash.update(b',')
            content_hash.update(raw.encode())
            lid=l['label_id']; assert previous is None or lid>previous,'Duplicate or unordered label ID'
            previous=lid;label_ids.append(lid)
            assert l['decision_time']+l['terminal_horizon_bars']*span<hold,'Holdout boundary violation'
            key=tuple(dims.get(k,'UNKNOWN') for k in DIMENSION_ORDER)
            if key not in cids: cids[key]=len(cohorts);cohorts.append(key);full_counts.append([0,0])
            ci=cids[key];full_counts[ci][0]+=1;full_counts[ci][1]+=int(l['net_profitable'])
            asset=l['instrument_key']['venue_symbol']
            if asset not in aids: aids[asset]=len(assets);assets.append(asset)
            vals=[l[k] for k in ('gross_R','net_R','fee_R','spread_R','slippage_R','funding_R','carry_R','total_cost_R','mfe_R','mae_R')]
            violations['nonfinite']+=int(not all(math.isfinite(v) for v in vals))
            violations['net_math']+=int(not math.isclose(l['net_R'],l['gross_R']-l['total_cost_R'],abs_tol=1e-12))
            violations['cost_sum']+=int(not math.isclose(l['total_cost_R'],sum(l[k] for k in ('fee_R','spread_R','slippage_R','funding_R','carry_R')),abs_tol=1e-12))
            violations['binary_semantics']+=int(l['net_profitable']!=(l['net_R']>0))
            violations['invalid_quality']+=int(l['label_quality']!='VALID')
            violations['stop_R']+=int(l['terminal_outcome']=='STOP_BEFORE_TARGET' and l['gross_R']!=-1)
            violations['target_R']+=int(l['terminal_outcome']=='TARGET_BEFORE_STOP' and not math.isclose(l['gross_R'],features['room_to_target_R'],abs_tol=1e-12))
            violations['horizon']+=int(l['terminal_horizon_bars']!=m['label_horizon_bars'])
            reason_counts.update(l['reason_codes']);feature_names.update(features.keys())
            a[i]=(l['decision_time'],ci,aids[asset],int(l['net_profitable']),CLASSES.index(l['terminal_outcome']),l['terminal_horizon_bars'],l['gross_R'],l['net_R'],l['total_cost_R'],features['room_to_target_R'],features['initial_risk_fraction'])
            if i%max(1,n//64)==0: audit_rows.append(d)
            if i%500000==0: print(f'PROFILE rows={i}/{n}',flush=True)
    assert i+1==n and hash_rows.hexdigest()==m['rows_sha256']
    content_hash.update(b']')
    payload={'source_kind':m['source_kind'],'dataset_source_hash':m['source_data_hash'],
        'candidate_generation_versions':m['setup_family_versions'],'label_policy_version':m['label_policy_version'],
        'cost_model_version':m['cost_model_version'],'feature_bucket_schema_version':m['cohort_schema_version'],
        'schema_version':m['library_version'],'row_label_ids':label_ids,'rows_content_hash':content_hash.hexdigest()}
    assert stable_hash(payload)==m['library_hash'],'Recomputed library hash differs'
    del label_ids,payload
    print('IDENTITY VERIFIED; replaying indexed probabilities',flush=True)
    order=np.lexsort((np.arange(n),a['t']));a=a[order]
    # File ordinal is lexicographic label-ID rank, proving exact tie-breaking.
    mappings=[]; lookup=[]; counts=[]
    for level in BACKOFF_LEVELS:
        keep=[DIMENSION_ORDER.index(d) for d in level]; ids={}; mp=[]
        for c in cohorts:
            key=tuple(c[k] for k in keep)
            if key not in ids: ids[key]=len(ids)
            mp.append(ids[key])
        mappings.append(np.array(mp,dtype=np.int32));lookup.append(ids);counts.append(np.zeros((len(ids),5)))
    stride=max(1,math.ceil(n/policy.max_eval_rows)); cursor=0; trace=[];skipped=[]
    full_n=np.zeros(len(cohorts));full_w=np.zeros(len(cohorts))
    for j in range(0,n,stride):
        t=int(a[j]['t']);end=min(j,int(np.searchsorted(a['t'],t-embargo,side='right')))
        chunk=a[cursor:end]
        for level,mp in enumerate(mappings):
            idx=mp[chunk['cid']];cnt=counts[level];size=len(cnt)
            cnt[:,0]+=np.bincount(idx,minlength=size)
            cnt[:,1]+=np.bincount(idx,weights=chunk['y'],minlength=size)
            for cls in range(3): cnt[:,cls+2]+=np.bincount(idx,weights=chunk['terminal']==cls,minlength=size)
        cursor=end
        if end<policy.min_train_rows: skipped.append({'ordinal':j,'reason':'NO_TRAIN'});continue
        ci=int(a[j]['cid']); selected=None
        for lvl,mp in enumerate(mappings):
            local=counts[lvl][mp[ci]]
            if local[0]>0: selected=(lvl,local)
            if local[0]>=MIN_USABLE_RAW_SUPPORT: break
        if selected is None: skipped.append({'ordinal':j,'reason':'INVALID_FORECAST'});continue
        lvl,local=selected;family=counts[-1][mappings[-1][ci]]
        p,multi=score_counts(local,family); dt=datetime.fromtimestamp(t/1000,timezone.utc);dims=dict(zip(DIMENSION_ORDER,cohorts[ci]))
        fold_width=(m['end_time']-m['start_time']+1)//6
        fold_width-=fold_width%span
        trace.append({'ordinal':j,'row_file_ordinal':int(order[j]),'decision_time':t,'asset':assets[a[j]['asset']],
            'market_family':m['market_type'],'timeframe':m['timeframe'],'horizon':int(a[j]['horizon']),
            **dims,'year':dt.year,'month':dt.strftime('%Y-%m'),'fold':f"FOLD_{min(5,(t-m['start_time'])//fold_width)}",
            'p':p,'y':int(a[j]['y']),'terminal':CLASSES[a[j]['terminal']],
            'backoff':lvl,'raw_support':int(local[0]),'support_bucket':_support_bucket(int(local[0]),policy.support_bucket_edges),
            'baseline_causal':float(counts[-1][:,1].sum()/counts[-1][:,0].sum()),
            'baseline_family_causal':float(family[1]/family[0]),
            'train_start':int(a[0]['t']),'train_end':int(a[end-1]['t']),
            'training_count':end,**{c:multi[c] for c in CLASSES}})
    base=sum(r['y'] for r in trace)/len(trace)
    assert all(math.isfinite(r['p']) and 0<=r['p']<=1 for r in trace)
    for r in trace:r['baseline_v1']=base
    result=metrics(trace);old=record['report']
    differences={k:result[k]-old[v] for k,v in [('model_brier','brier_score'),('baseline_brier','brier_baseline'),('brier_skill','brier_skill'),('ece','ece')]}
    assert result['samples']==old['n_evaluated'] and all(abs(v)<1e-12 for v in differences.values()), differences
    ps=np.array([r['p'] for r in trace]);ys=np.array([r['y'] for r in trace]);bins=[]
    for b in range(10):
        group=[r for r in trace if b/10<=r['p']<(b+1)/10 or (b==9 and r['p']==1)]
        bm=metrics(group) if group else {'samples':0,'model_brier':None,'mean_prediction':None,'positive_rate':None}
        bins.append({'bin':b,**bm,'aggregate_brier_contribution':sum((r['p']-r['y'])**2 for r in group)/len(trace)})
    reliability=sum(b['samples']/len(trace)*(b['mean_prediction']-b['positive_rate'])**2 for b in bins if b['samples'])
    resolution=sum(b['samples']/len(trace)*(b['positive_rate']-base)**2 for b in bins if b['samples'])
    from sklearn.metrics import roc_auc_score,average_precision_score,log_loss,precision_recall_curve,auc
    precision,recall,_=precision_recall_curve(ys,ps)
    deciles=[]
    for k,ix in enumerate(np.array_split(np.argsort(ps,kind='stable'),10)):
        rr=[trace[i] for i in ix];dd=metrics(rr);deciles.append({'decile':k+1,**dd,'lift':dd['positive_rate']/base})
    info={}
    for idx,name in enumerate(DIMENSION_ORDER):
        values={}
        for c,(cn,cw) in zip(cohorts,full_counts):
            v=c[idx]; values.setdefault(v,[0,0,[]]);values[v][0]+=cn;values[v][1]+=cw;values[v][2].append(cn)
        info[name]={'unique_values':len(values),'values':{v:{'rows':ct[0],'share':ct[0]/n,'positive_rate':ct[1]/ct[0],
            'minimum_exact_cohort_size':min(ct[2]),'median_exact_cohort_size':float(np.median(ct[2]))} for v,ct in values.items()}}
    for name,col in [('room_to_target_R','room'),('initial_risk_fraction','risk')]:
        arr=a[col];edges=np.unique(np.quantile(arr,np.linspace(0,1,11)));groups=[]
        for k in range(len(edges)-1):
            mask=(arr>=edges[k]) & ((arr<edges[k+1]) if k<len(edges)-2 else (arr<=edges[k+1]))
            groups.append({'lower':float(edges[k]),'upper':float(edges[k+1]),'rows':int(mask.sum()),'positive_rate':float(a['y'][mask].mean()) if mask.any() else None})
        info[name]={'unique_values':len(np.unique(arr)),'quantiles':dict(zip(['min','p05','p25','p50','p75','p95','max'],map(float,np.quantile(arr,[0,.05,.25,.5,.75,.95,1])))),
                    'used_in_probability':False,'population_bins':groups}
    summary={'library_id':m['library_id'],'library_hash':m['library_hash'],'rows_sha256':m['rows_sha256'],'row_count':n,
        'reproduction':'PASS','differences':differences,'legacy':result,'causal':metrics(trace,'baseline_causal'),
        'family_causal_diagnostic':metrics(trace,'baseline_family_causal'),
        'sampling':{'seed':None,'method':'sorted decision_time, label_id; fixed stride','stride':stride,'selected':len(trace)+len(skipped),'skipped':skipped,
                    'embargo_ms':embargo,'library_start':int(a['t'].min()),'library_end':int(a['t'].max()),'assets':sorted(assets)},
        'prediction_distribution':{'mean':float(ps.mean()),'positive_rate':base,'std':float(ps.std()),'sharpness_variance':float(ps.var()),
            **dict(zip(['min','p05','p25','p50','p75','p95','max'],map(float,np.quantile(ps,[0,.05,.25,.5,.75,.95,1]))))},
        'discrimination':{'roc_auc':float(roc_auc_score(ys,ps)),'pr_auc_trapezoid':float(auc(recall,precision)),
            'average_precision':float(average_precision_score(ys,ps)),'log_loss':float(log_loss(ys,ps)),'deciles':deciles},
        'decomposition':{'uncertainty':base*(1-base),'binned_reliability':reliability,'binned_resolution':resolution,
            'binned_brier':base*(1-base)+reliability-resolution,'within_bin_residual':result['model_brier']-(base*(1-base)+reliability-resolution)},
        'label_checks':dict(violations),'label_reason_counts':dict(reason_counts),'exact_cohorts':len(cohorts),
        'exact_cohort_size_min':min(ct[0] for ct in full_counts),'exact_cohort_size_median':float(np.median([ct[0] for ct in full_counts])),
        'features':info,'breakdowns':{key:grouped(trace,key) for key in ['asset','market_family','timeframe','horizon','setup_family','side','dominant_regime','volatility_bucket','liquidity_bucket','year','month','fold','backoff','support_bucket']},
        'reliability':bins,'code_hash':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        'governance':{'holdout_opened':False,'holdout_inspected':False,'runtime_pin':False,'threshold_changed':False}}
    output.mkdir(parents=True)
    if causal_candidate:
        from dataclasses import asdict
        corrected_policy=CausalCalibrationPolicy()
        updated={**old,'library_hash':m['library_hash'],'policy_hash':corrected_policy.policy_hash,
                 'brier_baseline':summary['causal']['baseline_brier'],'brier_skill':summary['causal']['brier_skill']}
        updated['reliability']=tuple(updated['reliability'])
        corrected=build_calibration_record(CalibrationReport(**updated),corrected_policy)
        summary['corrected_calibration_record']=corrected
        destination=library/'calibration.json'
        assert not destination.exists(),'Refusing to overwrite any calibration record'
        destination.write_text(json.dumps(corrected,indent=2,sort_keys=True)+'\n')
    (output/'summary.json').write_text(json.dumps(summary,indent=2,sort_keys=True)+'\n')
    (output/'label_audit_rows.json').write_text(json.dumps(audit_rows,indent=2)+'\n')
    write_csv(output/'predictions.csv',trace);write_csv(output/'reliability.csv',bins)
    for key,groups in summary['breakdowns'].items():
        write_csv(output/f'by_{key}.csv',[{'group':k,**v['legacy'],**{'causal_'+a:b for a,b in v['causal'].items()},
            **{a:b for a,b in v.items() if a not in ('legacy','causal')}} for k,v in groups.items()])
    print(json.dumps({'reproduction':'PASS','legacy':result,'causal':summary['causal'],'output':str(output)},indent=2),flush=True)
    return summary

if __name__=='__main__':
    ap=argparse.ArgumentParser();ap.add_argument('--library',type=Path,required=True);ap.add_argument('--original-calibration',type=Path,required=True);ap.add_argument('--output',type=Path,required=True);ap.add_argument('--causal-candidate',action='store_true')
    args=ap.parse_args();run(args.library,args.original_calibration,args.output,args.causal_candidate)
