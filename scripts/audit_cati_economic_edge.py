"""Diagnostic-only CATI economics from immutable V5 outer predictions.

No fitting, threshold optimization, source-price queries, or deployment rules.
All fixed tails/thresholds are reported, including empty cells. Candidate returns
are overlapping hypothetical setups, not an executable portfolio backtest.
"""
from __future__ import annotations
import argparse, csv, hashlib, json, math, subprocess, sys, time
from pathlib import Path
import numpy as np
from scipy.stats import spearmanr, kendalltau
from sklearn.metrics import roc_auc_score

REPO = Path(__file__).resolve().parents[1]
FRACTIONS = (.005, .01, .02, .05, .10, .20, .30)
EXPECTANCY_THRESHOLDS = (0., .05, .10, .20)
PROBABILITY_THRESHOLDS = (.50, .55, .60, .65)
COST_NAMES = ('fee_R', 'spread_R', 'slippage_R', 'funding_R', 'carry_R')
TERMINALS = ('TARGET', 'STOP', 'TIMEOUT')
GROUPS = {'family': (0,), 'side': (1,), 'regime': (2,), 'volatility': (3,),
          'family_side': (0, 1), 'family_regime': (0, 2), 'family_volatility': (0, 3)}


def digest(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as f:
        for block in iter(lambda: f.read(8*1024*1024), b''): h.update(block)
    return h.hexdigest()


def fixed_tail(score, fraction):
    """Exact ceil(n*fraction) rows; stable saved order resolves score ties."""
    if not 0 < fraction <= 1: raise ValueError('fraction outside (0,1]')
    if not np.isfinite(score).all(): raise ValueError('nonfinite score')
    return np.argsort(-np.asarray(score), kind='stable')[:math.ceil(len(score)*fraction)]


def fixed_population_bins(values, edges):
    # Tied boundaries can create empty bins, which are retained, not rebalanced.
    return np.searchsorted(np.asarray(edges), values, side='right')


def mean(a):
    return float(np.mean(a)) if len(a) else None


def summary(gross, costs, net, terminal, pred=None, probability=None):
    n = len(net)
    r = dict(samples=n, mean_gross_R=mean(gross), mean_cost_R=mean(costs), mean_net_R=mean(net),
             profitable_rate=mean(net > 0), predicted_mean_R=None if pred is None else mean(pred),
             mean_probability=None if probability is None else mean(probability))
    r.update({name+'_frequency': mean(terminal == i) for i, name in enumerate(TERMINALS)})
    if n:
        r.update(median_gross_R=float(np.median(gross)), median_net_R=float(np.median(net)),
                 std_net_R=float(np.std(net)), **{'p'+str(q).zfill(2):float(np.percentile(net,q)) for q in (5,25,50,75,95)})
        r.update({name+'_mean_net_R':mean(net[terminal == i]) for i,name in enumerate(TERMINALS)})
    return r


def rank_bins(score, realized, count):
    return [dict(bin=i+1, samples=len(ix), predicted_mean=mean(score[ix]), realized_mean=mean(realized[ix]))
            for i,ix in enumerate(np.array_split(np.argsort(score,kind='stable'),count))]


def rank_report(score, realized):
    result = dict(spearman=float(spearmanr(score,realized).statistic),
                  kendall_tau=float(kendalltau(score,realized).statistic),
                  quintiles=rank_bins(score,realized,5), deciles=rank_bins(score,realized,10))
    for key in ('quintiles','deciles'):
        means = [r['realized_mean'] for r in result[key]]
        result[key+'_monotonic'] = bool(np.all(np.diff(means)>=0))
        result[key+'_ordering_breaks'] = [i+1 for i,d in enumerate(np.diff(means)) if d<0]
    result['top_minus_bottom_decile_R'] = result['deciles'][-1]['realized_mean']-result['deciles'][0]['realized_mean']
    result['top_decile_minus_population_R'] = result['deciles'][-1]['realized_mean']-mean(realized)
    result['top_5_percent_minus_population_R'] = mean(realized[fixed_tail(score,.05)])-mean(realized)
    return result


def clustered_mean_interval(net, times):
    """Descriptive normal interval clustered by UTC decision day, not iid rows.

    Adjacent days can still be dependent; this is not independent confirmation.
    """
    _,inv=np.unique(times//86400000,return_inverse=True); k=int(inv.max()+1)
    sums=np.bincount(inv,weights=net-mean(net))
    se=float(np.sqrt(k/(k-1)*np.sum(sums*sums))/len(net)) if k>1 else None
    return dict(utc_day_clusters=k, clustered_standard_error_R=se,
                approximate_95_lower_R=None if se is None else mean(net)-1.96*se,
                approximate_95_upper_R=None if se is None else mean(net)+1.96*se)


def extract_costs(library, v4, cache, output):
    from app.trading_intelligence.hashing import TextHasher
    meta=json.loads((cache/'metadata.json').read_text())
    if digest(v4/'development.npz')!=meta['v4_cache_sha256']: raise ValueError('V4 cache identity')
    with np.load(v4/'development.npz',allow_pickle=False) as data: ids=data['label_ids']
    lookup={key:i for i,key in enumerate(ids)}; costs=np.full((len(ids),5),np.nan)
    manifest=json.loads((library/'manifest.json').read_text()); h=TextHasher(); count=0
    if manifest['library_hash']!=meta['parent_library_hash']: raise ValueError('parent identity')
    if manifest['end_time']>=meta['holdout_start_ms']: raise ValueError('parent overlaps holdout')
    with (library/'rows.jsonl').open(encoding='utf-8',newline='') as f:
        for number,line in enumerate(f):
            h.update(line); count+=1
            if number % meta['sampling_stride']: continue
            label=json.loads(line)['label']; ix=lookup[label['label_id']]
            if label['decision_time']+48*900000>=meta['holdout_start_ms']: raise ValueError('label crosses holdout')
            if np.isfinite(costs[ix]).any(): raise ValueError('duplicate sampled label')
            costs[ix]=[label[k] for k in COST_NAMES]
            if not np.isclose(costs[ix].sum(),label['total_cost_R'],rtol=0,atol=1e-12): raise ValueError('cost components disagree')
    if count!=manifest['row_count'] or h.hexdigest()!=manifest['rows_sha256']: raise ValueError('parent row identity')
    if not np.isfinite(costs).all(): raise ValueError('missing/nonfinite frozen costs')
    np.save(output/'frozen_cost_components.npy',costs,allow_pickle=False)
    return costs,dict(verified_parent_rows=count,rows_sha256=h.hexdigest(),sampled_labels=len(ids),
                      component_source='Exact frozen labels, same stride-12 IDs, no modeled substitutions')


def write_csv(path, rows):
    fields=list(dict.fromkeys(k for row in rows for k in row))
    with path.open('w',newline='',encoding='utf-8') as f:
        writer=csv.DictWriter(f,fieldnames=fields);writer.writeheader();writer.writerows(rows)


def full_parent_raw(library, metadata):
    """Full frozen parent labels, with the same calendar outer fold boundaries.

    Kept separate from sampled outer forecasts: no invented predictions for
    unsampled rows and no changes to the evaluator's sample/selection.
    """
    from app.trading_intelligence.hashing import TextHasher
    manifest=json.loads((library/'manifest.json').read_text()); n=manifest['row_count']
    rows=np.empty((n,10)); times=np.empty(n,dtype=np.int64);h=TextHasher();count=0
    names=('TARGET_BEFORE_STOP','STOP_BEFORE_TARGET','TIMEOUT')
    with (library/'rows.jsonl').open(encoding='utf-8',newline='') as f:
        for i,line in enumerate(f):
            h.update(line);label=json.loads(line)['label'];count+=1
            if label['decision_time']+label['terminal_horizon_bars']*900000>=metadata['holdout_start_ms']: raise ValueError('full label crosses holdout')
            times[i]=label['decision_time'];rows[i]=[label['gross_R'],label['total_cost_R'],label['net_R'],names.index(label['terminal_outcome']),
                label['terminal_horizon_bars'],*[label[k] for k in COST_NAMES]]
    if count!=n or h.hexdigest()!=manifest['rows_sha256']: raise ValueError('full parent identity')
    np.testing.assert_allclose(rows[:,0]-rows[:,1],rows[:,2],rtol=0,atol=1e-12)
    np.testing.assert_allclose(rows[:,5:].sum(axis=1),rows[:,1],rtol=0,atol=1e-12)
    boundaries=np.linspace(metadata['start'],metadata['stop'],7,dtype=np.int64)
    groups={'FULL_PARENT':np.arange(n),'OUTER_CALENDAR_POOLED':np.flatnonzero(times>=boundaries[1])}
    for fold in range(1,6):groups['FOLD_'+str(fold)]=np.flatnonzero((times>=boundaries[fold])&(times<boundaries[fold+1]))
    result={}
    for name,ix in groups.items():
        result[name]=summary(rows[ix,0],rows[ix,1],rows[ix,2],rows[ix,3].astype(int))
        result[name]['cost_components']={k:mean(rows[ix,5+j]) for j,k in enumerate(COST_NAMES)}
        result[name].update(zero_x_cost_net_R=mean(rows[ix,0]),one_x_cost_net_R=mean(rows[ix,2]),two_x_cost_net_R=mean(rows[ix,0]-2*rows[ix,1]))
    return result


def run(a):
    started=time.perf_counter(); out=Path(a.output);out.mkdir(parents=True,exist_ok=False)
    cache=Path(a.cache); metadata=json.loads((cache/'metadata.json').read_text())
    artifact=Path(a.artifact); d=json.loads((artifact/'v5_model.json').read_text())
    if d['candidate_id']!='cati_v5_b131674954a223230581dbbc' or d['holdout_query_count']!=0: raise ValueError('wrong V5')
    if metadata!=d['cache_metadata']: raise ValueError('V5 cache metadata identity')
    sys.path[:0]=[str(REPO/'backends/bot-backend'),str(REPO/'backends/shared')]
    from app.trading_intelligence.forecast.artifact import load_library_artifact
    load_library_artifact(artifact,mode='DEVELOPMENT')
    arrays={}
    for k in ('targets','times','label_ends','geometry','categories','event_bars'):
        p=cache/(k+'.npy')
        if digest(p)!=metadata['array_sha256'][p.name]: raise ValueError('cache identity '+k)
        arrays[k]=np.load(p,mmap_mode='r',allow_pickle=False)
    if np.any(arrays['label_ends']>=metadata['holdout_start_ms']): raise ValueError('holdout overlap')
    with np.load(Path(a.predictions)/'outer_predictions.npz',allow_pickle=False) as data:
        idx=data['indices'];pred=data['expected_net_R'];prob=data['p'];joint=data['joint'];baseline=data['baseline'];state_means=data['state_net_R_mean']
    if len(idx)!=sum(f['samples'] for f in d['folds']) or len(np.unique(idx))!=len(idx): raise ValueError('outer indices')
    if not np.allclose(joint[:,[0,3]].sum(axis=1),prob,rtol=0,atol=1e-12): raise ValueError('joint probability identity')
    np.testing.assert_allclose(np.sum(joint*state_means,axis=1),pred,rtol=0,atol=1e-12)
    targets=arrays['targets'][idx];gross=targets[:,2];net=targets[:,1];terminal=targets[:,5].astype(int)
    if not np.isfinite(pred).all() or not np.isfinite(prob).all(): raise ValueError('nonfinite predictions')
    if not np.isclose(np.mean((prob-targets[:,0])**2),d['metrics']['brier'],rtol=0,atol=1e-14): raise ValueError('V5 Brier replay mismatch')
    if not np.isclose(np.sqrt(np.mean((pred-net)**2)),d['metrics']['payoff']['expected_net_R']['rmse'],rtol=0,atol=1e-14): raise ValueError('V5 payoff replay mismatch')
    if a.cost_cache:
        previous=Path(a.cost_cache); verified=json.loads((previous/'audit.json').read_text())
        manifest=json.loads((Path(a.library)/'manifest.json').read_text())
        if (verified['candidate_id']!=d['candidate_id'] or verified['cost_extraction']['rows_sha256']!=manifest['rows_sha256']
            or verified['reproduction']['cache_array_sha256']!=metadata['array_sha256']
            or digest(previous/'frozen_cost_components.npy')!=verified['reproduction']['frozen_components_sha256']):
            raise ValueError('verified cost cache identity')
        components=np.load(previous/'frozen_cost_components.npy',allow_pickle=False)
        source=verified['cost_extraction'];np.save(out/'frozen_cost_components.npy',components,allow_pickle=False)
    else:
        components,source=extract_costs(Path(a.library),Path(a.v4_cache),cache,out)
    np.testing.assert_allclose(components.sum(axis=1),arrays['targets'][:,2]-arrays['targets'][:,1],rtol=0,atol=1e-12)
    cost=components[idx].sum(axis=1);np.testing.assert_allclose(gross-cost,net,rtol=0,atol=1e-12)
    events=arrays['event_bars'][idx];cats=arrays['categories'][idx];times=arrays['times'][idx]
    geo=arrays['geometry'][idx]; geometry={'room_to_target_R':np.exp(geo[:,0]),'initial_risk_fraction':np.exp(geo[:,1]),'cost_burden_R':cost}
    slices={'POOLED':np.arange(len(idx))};offset=0
    for f in d['folds']:
        rows=np.arange(offset,offset+f['samples'])
        if np.any(times[rows]<f['evaluation_start']) or f['training_label_end']>=f['evaluation_start']: raise ValueError('fold maturity')
        slices['FOLD_'+str(f['fold'])]=rows;offset+=f['samples']
    slices['EARLY_1_2']=np.concatenate([slices['FOLD_1'],slices['FOLD_2']])
    slices['LATE_3_5']=np.concatenate([slices['FOLD_3'],slices['FOLD_4'],slices['FOLD_5']])
    economics={};tails=[];eth=[];pth=[];ranks={};oracle={};group_rows=[];geometry_rows=[];event_rows=[];payoff_bias=[]
    actual_state=np.where(terminal==0,np.where(net>0,0,1),np.where(terminal==1,2,np.where(net>0,3,4)))
    state_names=('TARGET_PROFIT','TARGET_LOSS','STOP_LOSS','TIMEOUT_PROFIT','TIMEOUT_LOSS')
    for name,ii in slices.items():
        economics[name]=summary(gross[ii],cost[ii],net[ii],terminal[ii],pred[ii],prob[ii])
        economics[name].update(cost_components={k:mean(components[idx[ii],j]) for j,k in enumerate(COST_NAMES)},
            zero_x_cost_net_R=mean(gross[ii]),one_x_cost_net_R=mean(net[ii]),two_x_cost_net_R=mean(gross[ii]-2*cost[ii]),
            roc_auc=float(roc_auc_score(net[ii]>0,prob[ii])),
            brier_skill=1-float(np.mean((prob[ii]-(net[ii]>0))**2)/np.mean((baseline[ii]-(net[ii]>0))**2)),
            **clustered_mean_interval(net[ii],times[ii]))
        if name.startswith(('EARLY','LATE')): continue
        for fraction in FRACTIONS:
            jj=ii[fixed_tail(pred[ii],fraction)]
            tails.append(dict(population=name,fraction=fraction,**summary(gross[jj],cost[jj],net[jj],terminal[jj],pred[jj],prob[jj]),
                              **clustered_mean_interval(net[jj],times[jj])))
        for threshold in EXPECTANCY_THRESHOLDS:
            jj=ii[pred[ii]>threshold];eth.append(dict(population=name,threshold=threshold,**summary(gross[jj],cost[jj],net[jj],terminal[jj],pred[jj],prob[jj])))
        for threshold in PROBABILITY_THRESHOLDS:
            jj=ii[prob[ii]>threshold];pth.append(dict(population=name,threshold=threshold,**summary(gross[jj],cost[jj],net[jj],terminal[jj],pred[jj],prob[jj])))
        ranks[name]=dict(expected_R=rank_report(pred[ii],net[ii]), probability_net_R=rank_report(prob[ii],net[ii]),
            expected_R_gross_spearman=float(spearmanr(pred[ii],gross[ii]).statistic),
            expected_R_cost_spearman=float(spearmanr(pred[ii],cost[ii]).statistic),
            probability_binary_spearman=float(spearmanr(prob[ii],net[ii]>0).statistic),
            probability_binary_kendall=float(kendalltau(prob[ii],net[ii]>0).statistic))
        for population,jj in [('ALL',ii),('TOP_20_PERCENT',ii[fixed_tail(pred[ii],.20)])]:
            for state,label in enumerate(state_names):
                observed=actual_state[jj]==state
                predicted_contribution=mean(joint[jj,state]*state_means[jj,state]);realized_contribution=mean(observed*net[jj])
                payoff_bias.append(dict(population=name,ranking_slice=population,state=label,samples=len(jj),
                    mean_predicted_state_probability=mean(joint[jj,state]),actual_state_frequency=mean(observed),
                    predicted_R_contribution=predicted_contribution,realized_R_contribution=realized_contribution,
                    contribution_bias_R=predicted_contribution-realized_contribution))
        oracle[name]={'net_R_gt_'+str(x):mean(net[ii]>x) for x in (0.,.25,.50,1.)}
    for table in (eth,pth):
        for row in table:
            row['fold_coverage']=sum(r['samples']>0 for r in table if r['population'].startswith('FOLD') and r['threshold']==row['threshold'])
    base_slices={k:v for k,v in slices.items() if not k.startswith(('EARLY','LATE'))}
    for group,cols in GROUPS.items():
        keys=np.array([' | '.join(row[list(cols)]) for row in cats]);values=sorted(set(keys))
        for value in values:
            counts=[int(np.sum(keys[slices['FOLD_'+str(f)]]==value)) for f in range(1,6)]
            sufficient=bool(sum(counts)>=300 and min(counts)>=50)
            for name,ii in base_slices.items():
                jj=ii[keys[ii]==value]
                group_rows.append(dict(dimension=group,group=value,population=name,evidence_sufficient=sufficient,
                    **summary(gross[jj],cost[jj],net[jj],terminal[jj],pred[jj],prob[jj])))
    edges={k:np.quantile(values,np.arange(1,10)/10).tolist() for k,values in geometry.items()}
    for feature,values in geometry.items():
        bins=fixed_population_bins(values,edges[feature])
        for name,ii in base_slices.items():
            for b in range(10):
                jj=ii[bins[ii]==b]
                geometry_rows.append(dict(feature=feature,population=name,decile=b+1,value_mean=mean(values[jj]),
                    **summary(gross[jj],cost[jj],net[jj],terminal[jj])))
    event_edges=[1,2,4,8,16,32,48];event_summary={}
    for name,ii in base_slices.items():
        event_summary[name]={}
        for term,label in enumerate(TERMINALS):
            tt=ii[terminal[ii]==term]
            event_summary[name][label]=dict(samples=len(tt),mean_elapsed_bars=mean(events[tt]),
                median_elapsed_bars=float(np.median(events[tt])) if len(tt) else None,
                first_4_bars_fraction=mean(events[tt]<=4),first_8_bars_fraction=mean(events[tt]<=8),
                net_R=mean(net[tt]),funding_R=mean(components[idx[tt],3]))
            lo=0
            for hi in event_edges:
                jj=tt[(events[tt]>lo)&(events[tt]<=hi)]
                event_rows.append(dict(population=name,terminal=label,elapsed_bars_low=lo+1,elapsed_bars_high=hi,
                    administrative_censoring=(term==2),**summary(gross[jj],cost[jj],net[jj],terminal[jj])))
                lo=hi
    development=arrays['targets']; fullsample=summary(development[:,2],components.sum(axis=1),development[:,1],development[:,5].astype(int))
    result=dict(candidate_id=d['candidate_id'],source_code_revision=subprocess.check_output(['git','rev-parse','HEAD'],cwd=REPO,text=True).strip(),
        population_definition='Exact saved nested outer prediction indices; raw returns are hypothetical candidates, not portfolio P&L',
        full_development_sample_economics=fullsample,outer_economics=economics,cost_extraction=source,
        fixed_tails=tails,fixed_expectancy_thresholds=eth,fixed_probability_thresholds=pth,rank_quality=ranks,
        joint_payoff_bias_decomposition=payoff_bias,
        setup_group_economics=group_rows,geometry_population_quantile_edges=edges,geometry_economics=geometry_rows,
        event_time_summary=event_summary,event_time_economics=event_rows,
        NON_DEPLOYABLE_HINDSIGHT_DIAGNOSTIC=oracle,fold_probability_metrics=[{k:f[k] for k in ('fold','samples','roc_auc','brier_skill','ece')} for f in d['folds']],
        feature_drift=d['feature_drift'],DEVELOPMENT_RANGE_ADAPTIVELY_INSPECTED=True,
        boundary=dict(holdout_start_ms=d['holdout_start_ms'],holdout_opened=False,holdout_inspected=False,holdout_query_count=0,
                      fitted_models=0,thresholds_optimized=0,production_rules_created=0,governance='M0',cati_execution='OFF'),
        reproduction=dict(predictions_sha256=digest(Path(a.predictions)/'outer_predictions.npz'),artifact_sha256=digest(artifact/'v5_model.json'),
                          diagnostic_tool_sha256=digest(Path(__file__)),
                          frozen_components_sha256=digest(out/'frozen_cost_components.npy'),cache_array_sha256=metadata['array_sha256']),
        elapsed_seconds=time.perf_counter()-started)
    if a.full_parent_raw:
        result['full_frozen_parent_raw_economics']=full_parent_raw(Path(a.library),metadata)
        result['elapsed_seconds']=time.perf_counter()-started
    (out/'audit.json').write_text(json.dumps(result,indent=2,allow_nan=False)+'\n')
    for name,rows in [('fixed_tails',tails),('expectancy_thresholds',eth),('probability_thresholds',pth),('setup_groups',group_rows),('geometry_deciles',geometry_rows),('event_time_buckets',event_rows),('joint_payoff_bias',payoff_bias)]:write_csv(out/(name+'.csv'),rows)
    print(json.dumps(dict(outer_economics=economics,pooled_tails=[r for r in tails if r['population']=='POOLED'],pooled_rank=ranks['POOLED'],event_time_summary=event_summary['POOLED'],elapsed_seconds=result['elapsed_seconds'])),flush=True)


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--cache',default='data/research/calibration_diagnostics/v5_preparation_elapsed')
    p.add_argument('--v4-cache',default='data/research/calibration_diagnostics/v4_preparation')
    p.add_argument('--artifact',default='docs/research/artifacts/cati_v5_b131674954a223230581dbbc')
    p.add_argument('--predictions',default='data/research/calibration_diagnostics/v5_final_generator_replay')
    p.add_argument('--library',default='data/research/cati_libraries/cati_lib_0ead8264955eb958b20e165e')
    p.add_argument('--cost-cache',help='Reuse SHA-verified cost extraction from an earlier identical audit; never a model fit')
    p.add_argument('--full-parent-raw',action='store_true',help='Also audit all frozen parent labels, without creating predictions for unsampled rows')
    p.add_argument('--output',required=True);run(p.parse_args())
