"""One streaming library pass and bounded pre-holdout source queries; reusable numeric cache."""
from __future__ import annotations
import argparse, hashlib, json, sqlite3, sys, time
from pathlib import Path
REPO=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(REPO/'backends/bot-backend'),str(REPO/'backends/shared')]
import numpy as np
from app.trading_intelligence.hashing import TextHasher
from app.trading_intelligence.forecast.artifact import manifest_hash_of
from app.trading_intelligence.forecast.information_conditioning import causal_features
from app.trading_intelligence.forecast.causal_context import closed_candle_context,align_context,CONTEXT_NAMES


def main(a):
    root=Path(a.library); m=json.loads((root/'manifest.json').read_text())
    if manifest_hash_of(m)!=m['manifest_hash']: raise ValueError('manifest identity')
    hold=m['governance']['holdout_start_ms']; out=Path(a.output); out.mkdir(parents=True,exist_ok=False)
    cats=[]; raw=[]; targets=[]; times=[]; labels=[]; h=TextHasher(); n=0
    with (root/'rows.jsonl').open(encoding='utf-8',newline='') as f:
        for i,line in enumerate(f):
            h.update(line); n+=1
            if i%12: continue
            d=json.loads(line); l=d['label']; x=d['continuous_features']; dims=d['cohort_dimensions']
            t=l['decision_time']; end=t+l['terminal_horizon_bars']*900000
            if end>=hold or l['terminal_horizon_bars']!=48: raise ValueError('holdout/horizon violation')
            cat,values=causal_features(dimensions=dims,room=x['room_to_target_R'],risk_fraction=x['initial_risk_fraction'],
                timeframe=m['actual_interval'],horizon=48,instrument=l['instrument_key']['venue_symbol'])
            cats.append([cat.get(k,'') for k in ('setup_family','side','dominant_regime','volatility_bucket','instrument')])
            raw.append(values); times.append(t); labels.append(l['label_id'])
            targets.append([int(l['net_profitable']),l['net_R'],l['gross_R'],l['mfe_R'],l['mae_R'],
                ('TARGET_BEFORE_STOP','STOP_BEFORE_TARGET','TIMEOUT').index(l['terminal_outcome'])])
    if n!=m['row_count'] or h.hexdigest()!=m['rows_sha256']: raise ValueError('immutable row identity mismatch')
    times=np.asarray(times,dtype=np.int64); order=np.argsort(times,kind='stable')
    t=times[order]; cats=np.asarray(cats)[order]; raw=np.asarray(raw)[order]; target=np.asarray(targets)[order]
    context=np.empty((len(t),len(CONTEXT_NAMES))); source_times=np.empty(len(t),dtype=np.int64)
    conn=sqlite3.connect(f'file:{Path(a.database).resolve()}?mode=ro',uri=True)
    sources={}; benchmarks={}
    # Query bounds include a fixed causal warmup and end before the holdout.
    lo=int(t.min())-300*900000; hi=int(t.max()); assert hi<hold
    def load(asset):
        rows=list(conn.execute("SELECT open_time,open,high,low,close,volume FROM historical_candles WHERE symbol=? AND interval='15m' AND data_source='binance' AND market_type='crypto' AND open_time>=? AND open_time+900000-1<=? ORDER BY open_time",(asset,lo,hi)))
        h=hashlib.sha256()
        for r in rows: h.update(json.dumps(r,separators=(',',':')).encode()+b'\n')
        a=np.asarray(rows); closed,values=closed_candle_context(a[:,0].astype(np.int64),*a[:,1:].T)
        sources[asset]=dict(rows=len(rows),prefix_hash=h.hexdigest(),first_close=int(closed[0]),last_close=int(closed[-1]),query_end=hi)
        return closed,values
    for asset in ('BTCUSDT','ETHUSDT'): benchmarks[asset]=load(asset)
    for asset in sorted(set(cats[:,4])):
        idx=np.flatnonzero(cats[:,4]==asset); closed,values=benchmarks.get(asset) or load(asset)
        context[idx,:7],source_times[idx]=align_context(closed,values,t[idx])
    for j,asset in enumerate(('BTCUSDT','ETHUSDT')):
        values,_=align_context(*benchmarks[asset],t)
        context[:,7+j*2:9+j*2]=values[:,:2]
    conn.close()
    np.savez_compressed(out/'development.npz',times=t,label_ends=t+48*900000,categories=cats,
        geometry=raw,context=context,context_source_times=source_times,targets=target,label_ids=np.asarray(labels)[order])
    meta=dict(parent_library_hash=m['library_hash'],parent_manifest_hash=m['manifest_hash'],rows_hash=m['rows_sha256'],
        dataset_manifest_hash=m['governance']['dataset_manifest_hash'],holdout_start_ms=hold,
        start=m['start_time'],stop=m['end_time']+1,samples=len(t),sampling_stride=12,
        context_names=CONTEXT_NAMES,context_sources=sources,holdout_query_count=0,
        cache_sha256=hashlib.sha256((out/'development.npz').read_bytes()).hexdigest())
    (out/'metadata.json').write_text(json.dumps(meta,indent=2)); print(json.dumps(dict(samples=len(t),sources=len(sources),context_last_close=int(source_times.max()))),flush=True)


if __name__=='__main__':
    p=argparse.ArgumentParser(); p.add_argument('--library',required=True); p.add_argument('--database',required=True); p.add_argument('--output',required=True)
    main(p.parse_args())
