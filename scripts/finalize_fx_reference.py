"""Sequential FX derivation, QA, gap classification and strict dataset freeze.

Run after the acquisition supervisor has exited. Acquired periods do not prove
gap completeness. All unexpected gaps still prevent the dataset freeze.
"""
from __future__ import annotations
import argparse, hashlib, json, math, sqlite3, subprocess, sys
from pathlib import Path
from datetime import datetime, timezone
REPO=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(REPO/'backends/bot-backend'),str(REPO/'backends/shared')]
from app.market_data.fx_universe import load_fx_universe
from app.market_data.universe import freeze_dataset_payload
from app.market_data.gaps import find_gaps, classify_fx_gap, Gap, summarize, GAP_POLICY_VERSION, EXPECTED_CLOSURES


def final_partition(conn, universe, pair, timeframe):
    steps={'1m':60000,'5m':300000,'15m':900000,'1h':3600000,'4h':14400000}
    s,e=universe['window_start_ms'],universe['window_end_ms']; provider=universe['provider']
    h=hashlib.sha256(); times=[]; invalid=0; count=0
    # Only timestamps are retained; bid/ask bars stream into their content hash.
    columns='open_time,bid_open,bid_high,bid_low,bid_close,ask_open,ask_high,ask_low,ask_close,volume'
    for row in conn.execute(f'SELECT {columns} FROM fx_reference_quotes WHERE provider=? AND pair=? AND timeframe=? AND open_time>=? AND open_time<? ORDER BY open_time',
                            (provider,pair,timeframe,s,e)):
        h.update(json.dumps(row,separators=(',',':')).encode()); h.update(b'\n'); count+=1; times.append(row[0])
        if any(v is None or not math.isfinite(v) for v in row[1:9]) or row[8]<row[4] or row[2]<max(row[1],row[4]) or row[3]>min(row[1],row[4]) or row[6]<max(row[5],row[8]) or row[7]>min(row[5],row[8]): invalid+=1
    statuses={}
    for period,status in conn.execute('SELECT period,status FROM fx_reference_ingest_log WHERE provider=? AND pair=? AND timeframe=?',
                                     (provider,pair,'1h' if timeframe=='1h' else '1m')):
        prior=statuses.get(period)
        statuses[period]=status if prior in (None,status) else 'FAILED'
    step=steps[timeframe]
    gaps=[Gap(a,b,n,classify_fx_gap(a,b,step,ingest_status=statuses,
             period_kind='month' if timeframe=='1h' else 'day')) for a,b,n in find_gaps(times,step,start_ms=s,end_ms=e)]
    report=summarize(gaps)
    return dict(symbol=pair,timeframe=timeframe,source=provider,venue='FX_REFERENCE',
        source_version='bi5-candles-hour-1:v1' if timeframe=='1h' else 'bi5-candles-min-1:v1' if timeframe=='1m' else f'derived-from-1m:{step//60000}:v1',
        status='COMPLETE' if count and not invalid and not report['unexpected_gaps'] else 'INCOMPLETE',
        rows=count,expected_rows=(e-s)//step-sum(g.missing for g in gaps if g.classification in EXPECTED_CLOSURES),
        invalid_rows=invalid,duplicate_rows=sum(a==b for a,b in zip(times,times[1:])),
        out_of_order=sum(t%step!=0 for t in times),partition_hash=h.hexdigest(),
        missing_ranges=[dict(reason=g.classification,start_ms=g.start_ms,end_ms=g.end_ms) for g in gaps],
        gap_classification=report)


def main(args):
    import psutil
    for proc in psutil.process_iter(['pid','cmdline']):
        line=' '.join(proc.info['cmdline'] or [])
        if 'acquire_fx_reference_dataset.py' in line and '--plan' not in line:
            raise RuntimeError('FX acquisition writer still active; finalization refused')
    db=Path(args.db).resolve(); universe=load_fx_universe(args.manifest)
    out=Path(args.output); out.mkdir(parents=True,exist_ok=True)
    python=str(REPO/'backends/venv/Scripts/python.exe')
    plan=subprocess.check_output([python,'scripts/acquire_fx_reference_dataset.py','--db',str(db),'minute','--manifest',args.manifest,'--plan'],cwd=REPO,text=True)
    if json.loads(plan)['remaining_periods']:
        raise RuntimeError('FX acquisition incomplete; finalization refused')
    subprocess.run([python,'scripts/acquire_fx_reference_dataset.py','--db',str(db),'derive','--manifest',args.manifest],cwd=REPO,check=True)
    qa={}
    for tf in ('1m','5m','15m','1h','4h'):
        target=out/f'qa_{tf}.json'
        subprocess.run([python,'scripts/acquire_fx_reference_dataset.py','--db',str(db),'qa','--timeframe',tf,'--out',str(target)],cwd=REPO,check=True)
        qa[tf]=json.loads(target.read_text())
    with sqlite3.connect(f'file:{db}?mode=ro',uri=True) as conn:
        partitions=[final_partition(conn,universe,m['pair'],tf) for m in universe['members'] for tf in ('1m','5m','15m','1h','4h')]
    (out/'partitions.json').write_text(json.dumps(partitions,indent=2))
    failures=[tf for tf,rep in qa.items() if rep['failing_pairs'] or any(rep['bid_ask_quality'][k] for k in ('missing_side','negative_spread','ask_below_bid','invalid_ohlc','mid_inconsistent'))]
    if failures or any(p['status']!='COMPLETE' or p['duplicate_rows'] or p['out_of_order'] for p in partitions):
        (out/'status.json').write_text(json.dumps(dict(status='NOT_FROZEN',qa_failures=failures,
             incomplete_partitions=sum(p['status']!='COMPLETE' for p in partitions),gap_policy=GAP_POLICY_VERSION),indent=2))
        print('NOT_FROZEN: QA or unexpected gaps remain'); return 2
    if subprocess.check_output(['git','status','--porcelain'],cwd=REPO).strip():
        raise RuntimeError('clean committed provenance required for freeze')
    commit=subprocess.check_output(['git','rev-parse','HEAD'],cwd=REPO,text=True).strip()
    payload=freeze_dataset_payload(universe=universe,partitions=partitions,metadata_hash=universe['universe_hash'],
        code_commit=commit,product_type='FX_REFERENCE',base_interval='1m',resampling_policy_version='complete-windows-bid-ask-v1',
        gap_policy_version=GAP_POLICY_VERSION,created_at=datetime.now(timezone.utc).isoformat())
    target=out/'fx_reference.dataset.json'
    if target.exists(): raise RuntimeError('refusing to overwrite frozen dataset')
    target.write_text(json.dumps(payload,indent=2))
    (out/'status.json').write_text(json.dumps(dict(status='FROZEN',manifest_hash=payload['manifest_hash'])))
    return 0


if __name__=='__main__':
    ap=argparse.ArgumentParser(); ap.add_argument('--db',required=True); ap.add_argument('--manifest',required=True); ap.add_argument('--output',required=True)
    raise SystemExit(main(ap.parse_args()))
