"""One locked execution of frozen Mandate003. Historical source audit precedes labels.
No runtime registration, orders, holdout, forecast training or adaptive search.
"""
from __future__ import annotations
import argparse
from collections import Counter
from datetime import datetime, timezone
import gzip
import hashlib
import json
import math
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
from types import SimpleNamespace
import numpy as np
import pandas as pd
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'backends/bot-backend'),str(ROOT/'backends/shared'),str(ROOT/'scripts')]
from evaluate_cati_alpha import load_closed
from app.trading_intelligence.setups.alpha_v2 import aggregate_complete
H=3600000
Q=900000
WINDOW=672
HOLDOUT=1783876499999
REG=ROOT/'docs/research/cati_alpha_root_cause/next_edge_registry.json'
OUT=ROOT/'docs/research/cati_edge003'
CACHE=ROOT/'data/research/edge003_inputs'
FAMILIES=('RESIDUAL_MOMENTUM_PORTFOLIO_TOP1','DISPERSION_BREAK_RESIDUAL_RELATIVE_VALUE','SETTLED_FUNDING_BASIS_RELATIVE_CARRY')
HORIZONS=(48,24,72)

def sha(path): return hashlib.sha256(Path(path).read_bytes()).hexdigest()
def identity(value): return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest()
def dump(path,value):
    def cast(v):
        if isinstance(v,dict):return {str(k):cast(x) for k,x in v.items()}
        if isinstance(v,(list,tuple)):return [cast(x) for x in v]
        if isinstance(v,np.integer):return int(v)
        if isinstance(v,(float,np.floating)):return float(v) if np.isfinite(v) else None
        return v
    Path(path).write_text(json.dumps(cast(value),indent=2,allow_nan=False),encoding='utf-8')

def registry():
    r=json.loads(REG.read_text())
    expected=(REG.parent/'next_edge_registry.sha256').read_text().split()[0]
    if sha(REG)!=expected or r['registry_id']!='CATI_NEXT_EDGE_DISCOVERY_MANDATE_003' or r['status']!='FROZEN_PREPARED_NOT_EVALUATED' or r['maximum_major_hypotheses']!=3 or r['maximum_evaluation_runs']!=1:
        raise ValueError('REGISTRY_IDENTITY_OR_BUDGET_MISMATCH')
    if tuple(f['family'] for f in r['families'])!=FAMILIES or r['holdout_start_ms']!=HOLDOUT:raise ValueError('FROZEN_FAMILY_OR_BOUNDARY_MISMATCH')
    for path,digest in r['source_hashes']['code_sha256'].items():
        if sha(ROOT/path)!=digest:raise ValueError('FROZEN_SOURCE_CHANGED:'+path)
    return r

def fold_for(t,hours,r):
    for f in r['fold_structure']:
        if f['start_inclusive_ms']<=t<f['end_exclusive_ms']:
            # Entire horizon, not an early profitable exit, determines admissibility.
            if t+hours*H>=f['end_exclusive_ms'] or t+hours*H>r['development_stop_ms']-1:return None
            return f['fold']
    return None

def validate_native(a,stop):
    if not np.isfinite(a).all() or (a[:,1:5]<=0).any() or (a[:,5]<0).any():raise ValueError('INVALID_NATIVE_VALUE')
    if (np.diff(a[:,0])<=0).any() or (a[:,0]%Q!=0).any() or (a[:,0]+Q-1>stop).any():raise ValueError('NATIVE_TIMESTAMP_DEFECT')
    if (a[:,2]<a[:,[1,3,4]].max(axis=1)).any() or (a[:,3]>a[:,[1,2,4]].min(axis=1)).any():raise ValueError('OHLC_DEFECT')

def asof(a,t,max_age,strict=False,count=1):
    i=np.searchsorted(a[:,0],t,side='left' if strict else 'right')
    if i<count or t-a[i-1,0]>max_age:return None
    return a[i-count:i]

def rolling_features(close,high,low,times):
    returns=np.log(close/pd.DataFrame(close).shift(1).to_numpy())
    returns[1:][np.diff(times)!=H]=np.nan
    y=pd.DataFrame(returns)
    bi=list(CURRENT_SYMBOLS).index('BTCUSDT');ei=list(CURRENT_SYMBOLS).index('ETHUSDT')
    b,e=y[bi],y[ei]
    mb,me=b.rolling(WINDOW).mean(),e.rolling(WINDOW).mean()
    vb=(b*b).rolling(WINDOW).mean()-mb*mb;ve=(e*e).rolling(WINDOW).mean()-me*me
    cov=(b*e).rolling(WINDOW).mean()-mb*me;det=vb*ve-cov*cov
    my=y.rolling(WINDOW).mean()
    cb=y.mul(b,axis=0).rolling(WINDOW).mean()-my.mul(mb,axis=0)
    ce=y.mul(e,axis=0).rolling(WINDOW).mean()-my.mul(me,axis=0)
    bb=(cb.mul(ve,axis=0)-ce.mul(cov,axis=0)).div(det,axis=0)
    be=(ce.mul(vb,axis=0)-cb.mul(cov,axis=0)).div(det,axis=0)
    intercept=my-bb.mul(mb,axis=0)-be.mul(me,axis=0)
    # OLS SSE and 24h residual sum, both reconstructed under CURRENT coefficients.
    variance=(y*y).rolling(WINDOW).mean()-my*my-bb*cb-be*ce
    residual_sd=np.sqrt(variance.clip(lower=0)*WINDOW/(WINDOW-1))
    numerator=y.rolling(24).sum()-24*intercept-bb.mul(b.rolling(24).sum(),axis=0)-be.mul(e.rolling(24).sum(),axis=0)
    score=numerator/(residual_sd*np.sqrt(24))
    full_rank=(det>0)&np.isfinite(det)&(vb>0)&(ve>0)
    factor_values=np.column_stack((b.to_numpy(),e.to_numpy()))
    ranks=np.zeros(len(y),dtype=bool)
    for i in range(WINDOW,len(y)):
        window=factor_values[i-WINDOW+1:i+1]
        if np.isfinite(window).all():ranks[i]=np.linalg.matrix_rank(np.column_stack((np.ones(WINDOW),window)))==3
    full_rank &= ranks
    valid=y.notna().rolling(WINDOW).sum().eq(WINDOW).mul(full_rank,axis=0)
    score=score.where(valid & residual_sd.gt(0))
    # Existing CATI ATR14 convention: simple mean of14 closed-hour true ranges.
    prev=pd.DataFrame(close).shift(1).to_numpy()
    tr=np.maximum(high-low,np.maximum(abs(high-prev),abs(low-prev)))
    atr=pd.DataFrame(tr).rolling(14,min_periods=14).mean().to_numpy()
    return dict(returns=returns,score=score.to_numpy(),residual_sd=residual_sd.to_numpy(),beta_btc=bb.to_numpy(),beta_eth=be.to_numpy(),atr=atr,
                swing_low=pd.DataFrame(low).shift(1).rolling(24).min().to_numpy(),swing_high=pd.DataFrame(high).shift(1).rolling(24).max().to_numpy())

CURRENT_SYMBOLS=()

def source_audit():
    global CURRENT_SYMBOLS
    r=registry();OUT.mkdir(parents=True,exist_ok=True);CACHE.mkdir(parents=True,exist_ok=True)
    if (OUT/'run_lock.json').exists():raise RuntimeError('REGISTERED_RUN_ALREADY_CONSUMED')
    symbols=sorted(r['source_hashes']['main_native15m_closed_prefix']);CURRENT_SYMBOLS=tuple(symbols)
    start=r['development_start_ms']-1280*Q;stop=r['development_stop_ms']-1
    times=np.arange((start//H+1)*H,(stop+1)//H*H,H,dtype=np.int64)+H-1
    matrices={k:np.full((len(times),len(symbols)),np.nan) for k in ('open','high','low','close','volume')}
    fingerprints={};bar_counts={}
    c=sqlite3.connect((ROOT/'backends/shared/shared_lib/persistence/cosmicforge.db').as_uri()+'?mode=ro',uri=True)
    for j,s in enumerate(symbols):
        rows,digest=load_closed(c,s,'15m',start,stop)
        if digest!=r['source_hashes']['main_native15m_closed_prefix'][s]:raise ValueError('PRICE_SOURCE_CHANGED:'+s)
        native=np.array(rows,dtype=np.float64);validate_native(native,stop)
        np.save(CACHE/(s+'.npy'),native,allow_pickle=False)
        aggregated=np.array(aggregate_complete(rows,1,stop),dtype=float)
        indices=np.searchsorted(times,aggregated[:,0].astype(np.int64)+H-1)
        if (indices>=len(times)).any() or not np.array_equal(times[indices],aggregated[:,0].astype(np.int64)+H-1):raise ValueError('DERIVATION_ALIGNMENT_DEFECT')
        for k,n in zip(matrices,range(1,6)):matrices[k][indices,j]=aggregated[:,n]
        fingerprints[s]=digest;bar_counts[s]={'native15m':len(native),'complete1h':len(aggregated),'discarded_partial_or_gap_groups':'No approximation permitted'}
        print('source audit',s,flush=True)
    c.close()
    np.savez(CACHE/'hourly.npz',times=times,symbols=np.array(symbols),**matrices)
    features={};feature_hashes={};quality={}
    c=sqlite3.connect((ROOT/'data/research/crypto_deep_binance.db').as_uri()+'?mode=ro',uri=True)
    for s in r['families'][2]['universe']:
        h=hashlib.sha256();lists={k:[] for k in ('funding_rate','mark_price','index_price','basis_bps')}
        for row in c.execute("SELECT venue,venue_symbol,feature,observed_at,value,status,source,source_version FROM market_feature_observations WHERE venue='binance_usdm' AND venue_symbol=? AND observed_at<? AND feature IN ('funding_rate','mark_price','index_price','basis_bps') ORDER BY feature,observed_at,source",(s,HOLDOUT)):
            h.update(json.dumps(row,separators=(',',':'),allow_nan=False).encode()+b'\n')
            if row[5]!='AVAILABLE' or row[4] is None or not np.isfinite(row[4]):raise ValueError('INVALID_HISTORICAL_FEATURE:'+s)
            lists[row[2]].append((row[3],row[4]))
        if h.hexdigest()!=r['source_hashes']['deep_historical_feature_prefix']['sources'][s]['sha256']:raise ValueError('FEATURE_SOURCE_CHANGED:'+s)
        features[s]={k:np.array(v,dtype=float) for k,v in lists.items()}
        for k,a in features[s].items():
            if not len(a) or (np.diff(a[:,0])<=0).any():raise ValueError('FEATURE_DUPLICATE_OR_ORDER_DEFECT')
            if k!='funding_rate' and (a[:,0]%H!=H-1).any():raise ValueError('FEATURE_NOT_CLOSED_HOUR')
            if k in ('mark_price','index_price') and (a[:,1]<=0).any():raise ValueError('NONPOSITIVE_MARK_INDEX')
            np.save(CACHE/(s+'_'+k+'.npy'),a,allow_pickle=False)
        ma=features[s]['mark_price'];ix=features[s]['index_price'];basis=features[s]['basis_bps']
        mi=np.searchsorted(ma[:,0],basis[:,0]);ii=np.searchsorted(ix[:,0],basis[:,0])
        if (mi>=len(ma)).any() or (ii>=len(ix)).any() or not np.array_equal(ma[mi,0],basis[:,0]) or not np.array_equal(ix[ii,0],basis[:,0]):raise ValueError('BASIS_JOIN_DEFECT')
        if not np.allclose(10000*(ma[mi,1]-ix[ii,1])/ix[ii,1],basis[:,1],atol=1e-7,rtol=1e-7):raise ValueError('BASIS_DERIVATION_DEFECT')
        quality[s]={k:len(v) for k,v in features[s].items()};feature_hashes[s]=h.hexdigest()
    c.close()
    decision_join_coverage={}
    for symbol,p in features.items():
        coverage={'decision_hours':0,'known_three_strictly_prior_funding':0,'fresh_exact_mark_index_basis':0,'causality_violations':0}
        for t in times[(times>=r['development_start_ms'])&(times<r['development_stop_ms'])]:
            coverage['decision_hours']+=1
            settled=asof(p['funding_rate'],t,12*H,strict=True,count=3)
            if settled is not None:
                assert np.all(settled[:,0]<t) and t-settled[-1,0]<=12*H
                coverage['known_three_strictly_prior_funding']+=1
            joins=[asof(p[k],t,H+1) for k in ('mark_price','index_price','basis_bps')]
            if all(v is not None for v in joins) and len({int(v[-1,0]) for v in joins})==1:
                assert all(v[-1,0]<=t and t-v[-1,0]<=H+1 for v in joins)
                coverage['fresh_exact_mark_index_basis']+=1
        decision_join_coverage[symbol]=coverage
    # Prepare causal predictors only. There is deliberately no future-path label call here.
    f=rolling_features(matrices['close'],matrices['high'],matrices['low'],times)
    np.savez(CACHE/'causal_features.npz',**f)
    source_paths=[Path(__file__),ROOT/'scripts/evaluate_cati_alpha.py',ROOT/'backends/bot-backend/app/trading_intelligence/setups/alpha_v2.py',ROOT/'backends/bot-backend/tests/test_cati_edge003.py']
    audit={'SOURCE_CAUSALITY_AUDIT':'PASS','registry_id':r['registry_id'],'registry_hash':sha(REG),'holdout_query_count':0,'outcomes_evaluated':False,
        'native_source_start_ms':start,'native_source_close_stop_ms':stop,'feature_source_exclusive_upper_bound':HOLDOUT,'price_sources':fingerprints,'feature_sources':feature_hashes,'bar_counts':bar_counts,'feature_counts':quality,'decision_join_coverage':decision_join_coverage,
        'checks':['15m positive finite OHLCV, sorted unique aligned opens and closed upper bound','exact complete aligned4x15m hourly aggregation, gaps/partial groups omitted','672 contiguous finite returns required per asset and both factors, full-rank OLS, current-window coefficients only','mark/index/basis exact timestamp join and closed-hour stamp, age <=1h+1ms at decision and convergence exit','funding settlements strictly before decision, three prior settlements, latest age<=12h; realized future cashflow confined to label path','fold purge uses entire registered horizon before any outcome','forward-only observations excluded by DB/source/feature identity'],
        'provenance_limit':'Historical provider closed-hour/settlement timestamps are reconstructed causal availability, not contemporaneous ingestion-vintage proof; late backfill ingestion is explicitly disclosed by the frozen registry.',
        'evaluator_source_sha256':{str(p.relative_to(ROOT)):sha(p) for p in source_paths},'python':sys.version,'numpy':np.__version__,'pandas':pd.__version__,'authorizing_brief_sha256':sha(Path('C:/Users/favou/.codex/attachments/0dfc7d9b-d0cd-4a97-be38-64eae5f3ffcc/Pasted text.txt')),'implementation_choices_fixed_before_outcomes':['Existing CATI simple14-hour ATR; prior24-hour swings exclude current hour.','OLS residual variance uses ddof1; inverse-volatility weights normalize gross notional1.','Prior168-hour dispersion median excludes current hour; sample dispersion ddof1.','Current coefficient estimates reconstruct both672h residual variance and24h residual sum.','Pair stop evaluated before target/convergence on synchronized hourly closes only.','Funding cashflow reference marks use latest causal hourly close; no broker-payment evidence is claimed.','Carry gross=non-basis native-price remainder + historical mark/index basis attribution + funding contribution; basis is never double-counted.','Nonpositive stop/target or next-open gap geometry is non-executable, never resized.'],'audit_at':datetime.now(timezone.utc).isoformat()}
    dump(OUT/'source_causality_audit.json',audit)
    dump(CACHE/'cache_receipt.json',{'registry_hash':sha(REG),'files':{p.name:sha(p) for p in CACHE.iterdir() if p.suffix in ('.npy','.npz')}})
    return audit

def pair_geometry(i,a,b,weights,features):
    rr=features['returns'][i-WINDOW+1:i+1][:,[a,b]]
    if len(rr)!=WINDOW or not np.isfinite(rr).all():return None
    spread=weights[0]*rr[:,0]-weights[1]*rr[:,1]
    std=float(np.std(spread,ddof=1))
    if not np.isfinite(std) or std<=0:return None
    return max(2*std*np.sqrt(24),.003),float(np.corrcoef(rr.T)[0,1]),std

def modeled_cost(entry,exit_prices,weights,risk,hours,rates):
    # Quantities established at actual next-open price: actual exit turnover is retained.
    turnover=sum(w*(1+x/e) for e,x,w in zip(entry,exit_prices,weights))
    parts={k:turnover*rates[k]/risk for k in ('fee','half_spread','slippage')}
    parts['funding_buffer']=sum(weights)*rates['funding_per_8h']*math.ceil(hours/8)/risk
    return sum(parts.values()),parts

def label_single(candidate,native,rates):
    t=candidate['decision_time'];duration=48*H
    a=np.searchsorted(native[:,0],t+1);future=native[a:a+192]
    if len(future)!=192 or not np.array_equal(future[:,0],t+1+np.arange(192)*Q):return None,'FUTURE_GAP'
    entry=float(future[0,1]);sgn=candidate['sign'];stop=candidate['stop'];target=candidate['target'];risk=candidate['risk']
    if sgn*(entry-stop)<=0 or sgn*(target-entry)<=0:return None,'NON_EXECUTABLE_GAP'
    terminal='TIMEOUT';exit_price=float(future[-1,4]);event=191;mfe=mae=0.;ambiguous=False
    for k,bar in enumerate(future):
        op,hi,lo,cl=bar[1:5];st=lo<=stop if sgn>0 else hi>=stop;ta=hi>=target if sgn>0 else lo<=target
        mfe=max(mfe,sgn*((hi if sgn>0 else lo)-entry)/risk);mae=max(mae,-sgn*((lo if sgn>0 else hi)-entry)/risk)
        if st:
            exit_price=float(min(op,stop) if sgn>0 else max(op,stop));terminal='STOP';event=k;ambiguous=bool(ta);break
        if ta:exit_price=float(target);terminal='TARGET';event=k;break
    gross=sgn*(exit_price-entry)/risk
    cost,parts=modeled_cost([entry],[exit_price],[1.],risk/entry,48,rates)
    return dict(candidate,entry_prices=[entry],exit_prices=[exit_price],entry_time=t+1,exit_time=int(future[event,0]+Q-1),terminal=terminal,
                gross_R=gross,price_R=gross,funding_R=0.,basis_R=None,cost_R=cost,net_R=gross-cost,cost_parts=parts,mfe_bound_R=mfe,mae_bound_R=mae,excursion_censored=True,ambiguous_stop=ambiguous),None

def current_basis(feature_arrays,symbol,t):
    p=feature_arrays[symbol]
    rows={k:asof(p[k],t,H+1) for k in ('mark_price','index_price','basis_bps')}
    if any(v is None for v in rows.values()):return None
    stamps=[int(v[-1,0]) for v in rows.values()]
    if len(set(stamps))!=1:return None
    return float(rows['basis_bps'][-1,1]),stamps[0]

def label_pair(candidate,hourly,features,feature_arrays,rates):
    i=candidate['index'];h=candidate['horizon'];t=candidate['decision_time'];legs=candidate['legs'];w=candidate['weights'];risk=candidate['risk']
    js=[candidate['asset_indices'][0],candidate['asset_indices'][1]]
    ts=hourly['times'][i+1:i+1+h]
    if len(ts)!=h or not np.array_equal(ts,t+np.arange(1,h+1)*H):return None,'FUTURE_HOURLY_GAP'
    entry=hourly['open'][i+1,js].astype(float);path=hourly['close'][i+1:i+1+h][:,js]
    if not np.isfinite(entry).all() or not np.isfinite(path).all():return None,'FUTURE_LEG_GAP'
    terminal='TIMEOUT';price=funding=gross=0.;basis_contribution=None;mfe=mae=0.;one_leg=0.;exit_k=h-1
    for k,time_ms in enumerate(ts):
        leg_returns=path[k]/entry-1
        price=(w[0]*leg_returns[0]-w[1]*leg_returns[1])/risk
        funding=0.
        if candidate['family']==FAMILIES[2]:
            for leg,weight,sign,entry_price,exit_price in zip(legs,w,(1,-1),entry,path[k]):
                fa=feature_arrays[leg]['funding_rate'];start=np.searchsorted(fa[:,0],t+1,side='right');stop=np.searchsorted(fa[:,0],time_ms,side='left')
                events=fa[start:stop]
                # Historical funding payment uses mark notional at each settlement.
                for ft,rate in events:
                    mark=asof(feature_arrays[leg]['mark_price'],ft,H+1)
                    if mark is None:return None,'FUNDING_MARK_MISSING'
                    funding+=-sign*weight*mark[-1,1]/entry_price*rate/risk
        gross=price+funding
        mfe=max(mfe,gross);mae=max(mae,-gross);one_leg=max(one_leg,max(-w[0]*leg_returns[0]/risk,w[1]*leg_returns[1]/risk))
        converge=False
        if candidate['family']==FAMILIES[1]:
            z=features['score'][i+1+k,js]
            if not np.isfinite(z).all():return None,'RESIDUAL_CONVERGENCE_MISSING'
            converge=z[1]-z[0]<=.5
        else:
            bl=current_basis(feature_arrays,legs[0],time_ms);bs=current_basis(feature_arrays,legs[1],time_ms)
            if bl is None or bs is None:return None,'BASIS_CONVERGENCE_MISSING'
            spread=bs[0]-bl[0];converge=spread<=2
            # Descriptive decomposition of price return into basis change + remainder.
            basis_contribution=0.
            for leg,weight,sign,entry_price in zip(legs,w,(1,-1),entry):
                m0=asof(feature_arrays[leg]['mark_price'],t,H+1)[-1,1]
                i0=asof(feature_arrays[leg]['index_price'],t,H+1)[-1,1]
                mt=asof(feature_arrays[leg]['mark_price'],time_ms,H+1)[-1,1]
                it=asof(feature_arrays[leg]['index_price'],time_ms,H+1)[-1,1]
                basis_contribution+=sign*weight*(mt-m0*it/i0)/entry_price/risk
        if gross<=-1:terminal='STOP';exit_k=k;break
        if gross>=2:terminal='TARGET';exit_k=k;break
        if converge:terminal='CONVERGENCE';exit_k=k;break
    exit_prices=path[exit_k].astype(float)
    cost,parts=modeled_cost(entry,exit_prices,w,risk,h,rates)
    return dict(candidate,entry_prices=entry.tolist(),exit_prices=exit_prices.tolist(),entry_time=t+1,exit_time=int(ts[exit_k]),terminal=terminal,gross_R=gross,price_R=price if basis_contribution is None else price-basis_contribution,native_price_R=price,funding_R=funding,basis_R=basis_contribution,
                basis_attribution_method="Historical mark change minus index-scaled initial mark, weighted in actual native-entry units; PRICE_R is native-price remainder. Gross=PRICE_R+BASIS_R+FUNDING_R; funding settlement marks use latest causal hourly close, a reference cashflow proxy, not broker receipts.",cost_R=cost,net_R=gross-cost,cost_parts=parts,mfe_bound_R=mfe,mae_bound_R=mae,excursion_censored=False,largest_one_leg_adverse_R=one_leg),None

def portfolio_replay(rows):
    selected=[];rejected=[];busy_until=-1
    for row in sorted(rows,key=lambda x:(x['decision_time'],x['label_id'])):
        if row['entry_time']<=busy_until:rejected.append(row['label_id']);continue
        selected.append(row);busy_until=row['exit_time']
    return selected,rejected

def metrics(rows):
    if not rows:return {'samples':0,'days':0,'gross_R':None,'cost_R':None,'net_R':None,'net_2x_cost_R':None,'net_lower_95_R':None}
    net=np.array([x['net_R'] for x in rows]);gross=np.array([x['gross_R'] for x in rows]);cost=np.array([x['cost_R'] for x in rows]);days=np.array([x['decision_time']//86400000 for x in rows]);unique=np.unique(days)
    scores=np.array([(net[days==d]-net.mean()).sum() for d in unique])
    se=np.sqrt(len(unique)/(len(unique)-1)*sum(scores*scores))/len(net) if len(unique)>1 else None
    return dict(samples=len(rows),days=len(unique),gross_R=float(gross.mean()),cost_R=float(cost.mean()),net_R=float(net.mean()),net_2x_cost_R=float((gross-2*cost).mean()),net_lower_95_R=float(net.mean()-1.96*se) if se is not None else None,
                terminals=dict(Counter(x['terminal'] for x in rows)),profit_rate=float((net>0).mean()),price_R=float(np.mean([x['price_R'] for x in rows])),funding_R=float(np.mean([x['funding_R'] for x in rows])),basis_R=float(np.mean([x['basis_R'] for x in rows if x['basis_R'] is not None])) if any(x['basis_R'] is not None for x in rows) else None)

def economic_gate(folds,pooled):
    failures=[]
    for i,f in enumerate(folds,1):
        if f.get('gross_R') is None or f['gross_R']<=0:failures.append(f'FOLD_{i}_GROSS_NOT_POSITIVE')
        if f.get('net_lower_95_R') is None or f['net_lower_95_R']<=0:failures.append(f'FOLD_{i}_NET_DAY_CLUSTERED_LCB_NOT_POSITIVE')
    if pooled.get('net_2x_cost_R') is None or pooled['net_2x_cost_R']<=0:failures.append('POOLED_2X_COST_NET_NOT_POSITIVE')
    return ('FAIL' if failures else 'PASS'),failures

def run():
    r=registry();audit=json.loads((OUT/'source_causality_audit.json').read_text())
    if audit['SOURCE_CAUSALITY_AUDIT']!='PASS' or audit['registry_hash']!=sha(REG):raise ValueError('SOURCE_AUDIT_REQUIRED')
    for p,h in audit['evaluator_source_sha256'].items():
        if sha(ROOT/p)!=h:raise ValueError('EVALUATOR_CHANGED_AFTER_AUDIT:'+p)
    cache=json.loads((CACHE/'cache_receipt.json').read_text())
    for name,h in cache['files'].items():
        if sha(CACHE/name)!=h:raise ValueError('INPUT_CACHE_CHANGED:'+name)
    capability=json.loads((OUT/'paired_capability_audit.json').read_text())
    fd=os.open(OUT/'run_lock.json',os.O_WRONLY|os.O_CREAT|os.O_EXCL)
    with os.fdopen(fd,'w') as f:json.dump({'registry_hash':sha(REG),'evaluation_runs':1,'started_at':datetime.now(timezone.utc).isoformat(),'source_hashes':audit['evaluator_source_sha256'],'user_authorization':'Execute frozen mandate003 once; authorization supersedes prepared-registry evaluation_authorized=false.'},f,indent=2)
    hourly=dict(np.load(CACHE/'hourly.npz'));features=dict(np.load(CACHE/'causal_features.npz'));symbols=hourly['symbols'].tolist();lookup={s:i for i,s in enumerate(symbols)}
    native={s:np.load(CACHE/(s+'.npy'),mmap_mode='r') for s in symbols}
    fa={s:{k:np.load(CACHE/(s+'_'+k+'.npy'),mmap_mode='r') for k in ('funding_rate','mark_price','index_price','basis_bps')} for s in r['families'][2]['universe']}
    tradable=np.array([lookup[s] for s in r['families'][0]['universe']]);scores=features['score']
    dispersion=pd.DataFrame(scores[:,tradable]).std(axis=1,ddof=1).where(np.isfinite(scores[:,tradable]).sum(axis=1)>=30)
    dispersion_median=dispersion.shift(1).rolling(168,min_periods=168).median().to_numpy()
    results={f:[] for f in FAMILIES};counts={f:Counter() for f in FAMILIES};ranked_labels={f:[] for f in FAMILIES}
    file=gzip.open(OUT/'counterfactual_labels.jsonl.gz','wt',encoding='utf-8')
    def evaluate(cand,chosen):
        family=cand['family'];counts[family]['raw_candidates']+=1
        label,reason=label_single(cand,native[cand['legs'][0]],r['cost_policy']['rates']) if family==FAMILIES[0] else label_pair(cand,hourly,features,fa,r['cost_policy']['rates'])
        if label is None:counts[family][reason]+=1;return
        label['label_id']=identity({'registry':sha(REG),'candidate':cand});label['ranked_top1']=chosen
        file.write(json.dumps(label,separators=(',',':'),allow_nan=False)+'\n')
        results[family].append(label)
        if chosen:ranked_labels[family].append(label)
    for i,t in enumerate(hourly['times']):
        t=int(t)
        if t<r['development_start_ms'] or t>=r['development_stop_ms']:continue
        valid=tradable[np.isfinite(scores[i,tradable])]
        if len(valid)>=30:
            fold=fold_for(t,48,r)
            eligible=[j for j in valid if abs(scores[i,j])>=2]
            eligible.sort(key=lambda j:(-abs(scores[i,j]),symbols[j]))
            counts[FAMILIES[0]]['score_eligible_signals']+=len(eligible)
            if not fold:counts[FAMILIES[0]]['full_horizon_purged_signals']+=len(eligible)
            if fold:
                for rank,j in enumerate(eligible):
                    price=float(hourly['close'][i,j]);sgn=1 if scores[i,j]>0 else -1;atr=float(features['atr'][i,j]);swing=features['swing_low'][i,j] if sgn>0 else features['swing_high'][i,j]
                    risk=max(sgn*(price-swing)+.25*atr,2*atr,.003*price)
                    if not np.isfinite([price,risk,atr]).all() or risk<=0 or price-sgn*risk<=0 or price+sgn*2.5*risk<=0:counts[FAMILIES[0]]['INVALID_GEOMETRY']+=1;continue
                    cand={'family':FAMILIES[0],'index':i,'decision_time':t,'fold':fold,'legs':[symbols[j]],'weights':[1.],'sign':sgn,'side':'LONG' if sgn>0 else 'SHORT','score':float(scores[i,j]),'risk':float(risk),'stop':float(price-sgn*risk),'target':float(price+sgn*2.5*risk),'horizon':48,'beta_btc':float(sgn*features['beta_btc'][i,j]),'beta_eth':float(sgn*features['beta_eth'][i,j]),'gross_exposure':1.,'net_exposure':float(sgn)}
                    evaluate(cand,rank==0)
            fold=fold_for(t,24,r)
            if fold and np.isfinite(dispersion_median[i]) and dispersion_median[i]>0 and dispersion.iloc[i]/dispersion_median[i]>=1.5:
                ordered=sorted(valid,key=lambda j:(scores[i,j],symbols[j]));a=ordered[0];b=sorted(valid,key=lambda j:(-scores[i,j],symbols[j]))[0]
                if scores[i,b]-scores[i,a]>=3:
                    sd=features['residual_sd'][i,[a,b]];weights=1/sd;weights=(weights/weights.sum()).tolist();geo=pair_geometry(i,a,b,weights,features)
                    if geo:
                        risk,corr,std=geo
                        cand={'family':FAMILIES[1],'index':i,'decision_time':t,'fold':fold,'legs':[symbols[a],symbols[b]],'asset_indices':[int(a),int(b)],'weights':weights,'side':'BASKET','risk':risk,'horizon':24,'pair_correlation':corr,'spread_volatility':std,'leg_imbalance':abs(weights[0]-weights[1]),'beta_btc':float(weights[0]*features['beta_btc'][i,a]-weights[1]*features['beta_btc'][i,b]),'beta_eth':float(weights[0]*features['beta_eth'][i,a]-weights[1]*features['beta_eth'][i,b]),'gross_exposure':1.,'net_exposure':weights[0]-weights[1]}
                        evaluate(cand,True)
        fold=fold_for(t,72,r)
        if fold:
            eligible=[]
            for s in r['families'][2]['universe']:
                j=lookup[s]
                if i<WINDOW or not np.isfinite(features['returns'][i-WINDOW+1:i+1,j]).all():continue
                fund=asof(fa[s]['funding_rate'],t,12*H,strict=True,count=3);basis=current_basis(fa,s,t)
                if fund is None or basis is None:counts[FAMILIES[2]]['DECISION_MISSING_OR_STALE_FEATURE']+=1;continue
                eligible.append((s,j,float(fund[:,1].mean()),basis[0]))
            longs=sorted([x for x in eligible if x[3]<=-5],key=lambda x:(x[2],x[0]));shorts=sorted([x for x in eligible if x[3]>=5],key=lambda x:(-x[2],x[0]))
            if longs and shorts:
                lo,sh=longs[0],shorts[0]
                if sh[2]-lo[2]>=.0002 and sh[3]-lo[3]>=10:
                    a,b=lo[1],sh[1];rr=features['returns'][i-WINDOW+1:i+1][:,[a,b]];sd=np.std(rr,axis=0,ddof=1);weights=1/sd;weights=(weights/weights.sum()).tolist();geo=pair_geometry(i,a,b,weights,features)
                    if geo:
                        risk,corr,std=geo
                        cand={'family':FAMILIES[2],'index':i,'decision_time':t,'fold':fold,'legs':[lo[0],sh[0]],'asset_indices':[a,b],'weights':weights,'side':'BASKET','risk':risk,'horizon':72,'entry_basis':[lo[3],sh[3]],'known_mean_funding':[lo[2],sh[2]],'pair_correlation':corr,'spread_volatility':std,'leg_imbalance':abs(weights[0]-weights[1]),'beta_btc':float(weights[0]*features['beta_btc'][i,a]-weights[1]*features['beta_btc'][i,b]),'beta_eth':float(weights[0]*features['beta_eth'][i,a]-weights[1]*features['beta_eth'][i,b]),'gross_exposure':1.,'net_exposure':weights[0]-weights[1]}
                        evaluate(cand,True)
        if i%1000==0:print('registered run hour',i,'of',len(scores),{f:len(results[f]) for f in FAMILIES},flush=True)
    file.close()
    report={'registry_id':r['registry_id'],'registry_hash':sha(REG),'evaluation_runs':1,'holdout_opened':False,'holdout_query_count':0,'source_causality_audit':'PASS','paired_execution_research_capable':capability['PAIRED_EXECUTION_RESEARCH_CAPABLE'],'multiplicity':'Exactly three frozen major hypotheses, all reported separately; no best-of-three promotion or combination. Development range was already adaptively inspected.','families':{}}
    selected_all=[]
    for family in FAMILIES:
        selected,rejected=portfolio_replay(ranked_labels[family]);selected_all+=selected;pooled=metrics(selected);folds=[metrics([x for x in selected if x['fold']==k]) for k in range(1,6)];gate,failures=economic_gate(folds,pooled)
        counterfolds=[metrics([x for x in results[family] if x['fold']==k]) for k in range(1,6)]
        evidence_failures=[]
        for k,m in enumerate(counterfolds,1):
            if m['samples']<300:evidence_failures.append(f'FOLD_{k}_COUNTERFACTUAL_LABELS_LT300')
            if m['days']<60:evidence_failures.append(f'FOLD_{k}_COUNTERFACTUAL_DAYS_LT60')
        raw=metrics(results[family])
        if raw['samples']<1500:evidence_failures.append('POOLED_LABELS_LT1500')
        if raw['days']<300:evidence_failures.append('POOLED_DAYS_LT300')
        def dist(key):
            v=[x[key] for x in selected if key in x and x[key] is not None]
            return {'n':len(v),'mean':float(np.mean(v)) if v else None,'p50':float(np.median(v)) if v else None,'p95':float(np.quantile(v,.95)) if v else None,'max':float(max(v)) if v else None}
        report['families'][family]={'raw_candidates':counts[family]['raw_candidates'],'raw_counterfactual_labels':len(results[family]),'ranked_concurrent_opportunities':len(ranked_labels[family]),'selected_portfolio_trades':len(selected),'unique_decision_timestamps':len({x['decision_time'] for x in results[family]}),'overlapping_position_attempts_rejected':len(rejected),'selection_rejections':{'unselected_concurrent_counterfactuals':len(results[family])-len(ranked_labels[family]),'overlapping_label_ids':rejected},'source_or_execution_skips':dict(counts[family]),'pooled':pooled,'folds':folds,'counterfactual_pooled':raw,'counterfactual_folds':counterfolds,'economic_gate':gate,'failing_clauses':failures,'minimum_evidence_gate':'FAIL' if evidence_failures else 'PASS','minimum_evidence_failing_clauses':evidence_failures,'runtime_capability_blocked':family!=FAMILIES[0] and capability['PAIRED_EXECUTION_RESEARCH_CAPABLE']=='NO','exposures':{k:dist(k) for k in ('beta_btc','beta_eth','gross_exposure','net_exposure','pair_correlation','spread_volatility','leg_imbalance','largest_one_leg_adverse_R')},'maximum_concurrent_positions':1 if selected else 0,'maximum_concurrent_legs':(1 if family==FAMILIES[0] else 2) if selected else 0,'decision_day_clustering':dict(Counter(datetime.fromtimestamp(x['decision_time']/1000,tz=timezone.utc).strftime('%Y-%m-%d') for x in selected))}
    with gzip.open(OUT/'selected_portfolio_labels.jsonl.gz','wt',encoding='utf-8') as f:
        for label in selected_all:f.write(json.dumps(label,separators=(',',':'),allow_nan=False)+'\n')
    report['passing_families']=[f for f,v in report['families'].items() if v['economic_gate']=='PASS' and v['minimum_evidence_gate']=='PASS']
    report['outcome_population_hashes']={p.name:sha(p) for p in (OUT/'counterfactual_labels.jsonl.gz',OUT/'selected_portfolio_labels.jsonl.gz')}
    report['model_training_performed']=False;report['library_built']=False;report['pre_holdout_ready']=False
    dump(OUT/'results.json',report)
    dump(OUT/'run_completion.json',{'evaluation_runs':1,'completed_at':datetime.now(timezone.utc).isoformat(),'results_sha256':sha(OUT/'results.json'),'registry_hash':sha(REG),'source_hashes':audit['evaluator_source_sha256']})
    return report

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--audit',action='store_true');parser.add_argument('--run',action='store_true');args=parser.parse_args()
    if args.audit==args.run:parser.error('Choose exactly one phase')
    result=source_audit() if args.audit else run()
    print(json.dumps({'phase':'SOURCE_AUDIT' if args.audit else 'REGISTERED_RUN','audit':result.get('SOURCE_CAUSALITY_AUDIT'),'passing_families':result.get('passing_families')}))
