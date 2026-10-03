"""Additional repository databases and actual feature provenance; no holdout reads."""
import hashlib
import json
import sqlite3
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
BOUND=1783876499999

def run():
    existing=json.loads((ROOT/'docs/research/cati_alpha_root_cause/data_inventory.json').read_text())
    seen={str((ROOT/x['path']).resolve()) for x in existing['databases']}
    result={'boundary_exclusive':BOUND,'holdout_queries':0,'other_databases':[], 'native_5m_quality':[], 'historical_feature_prefix_hashes':{}}
    for p in sorted(ROOT.rglob('*.db')):
        if any(x in p.parts for x in ('node_modules','venv','.git')) or str(p.resolve()) in seen: continue
        rec={'path':str(p.relative_to(ROOT)), 'bytes':p.stat().st_size,'classification':'NONCANONICAL_STORED_COPY_OR_REPLAY_NOT_INDEPENDENT_EVIDENCE','tables':{}}
        if not rec['bytes']:
            rec['quality']='EMPTY_FILE';result['other_databases'].append(rec);continue
        c=sqlite3.connect(p.as_uri()+'?mode=ro',uri=True);c.row_factory=sqlite3.Row
        tables={r[0] for r in c.execute("select name from sqlite_master where type='table'")}
        rec['table_names']=sorted(tables)
        for table in ('historical_candles','market_candles'):
            if table not in tables: continue
            cols={r[1] for r in c.execute('pragma table_info('+table+')')}
            tf='interval' if table=='historical_candles' else 'timeframe'
            sym='symbol' if table=='historical_candles' else 'venue_symbol'
            src='data_source' if table=='historical_candles' else 'source'
            if not {tf,sym,src,'open_time'}.issubset(cols): continue
            rows=[]
            for frame,step in [('1m',60000),('5m',300000),('15m',900000),('1h',3600000),('4h',14400000)]:
                rows.extend(dict(r)|{'timeframe':frame,'availability':'HISTORICAL_AVAILABLE','causal_usability':'Lineage and quality unverified; no separate evidence credit for copied/replayed rows.'} for r in c.execute(f'SELECT {sym} symbol,{src} source,COUNT(*) rows,MIN(open_time) first_open,MAX(open_time)+?-1 last_close FROM {table} WHERE {tf}=? AND open_time+?-1<? GROUP BY {sym},{src}',(step,frame,step,BOUND)))
            rec['tables'][table]=rows
        c.close();result['other_databases'].append(rec);print(rec['path'],flush=True)
    p=ROOT/'data/research/crypto_deep_binance.db'
    c=sqlite3.connect(p.as_uri()+'?mode=ro',uri=True);c.row_factory=sqlite3.Row
    result['native_5m_quality']=[dict(r) for r in c.execute("SELECT venue,venue_symbol,source,source_version,environment,derived_from,COUNT(*) rows,COUNT(quote_volume) quote_volume_rows,COUNT(trades) trade_count_rows,COUNT(volume) volume_rows,SUM(CASE WHEN open_time%300000!=0 OR close_time!=open_time+299999 THEN 1 ELSE 0 END) timestamp_defects FROM market_candles WHERE timeframe='5m' AND close_time<? GROUP BY venue,venue_symbol,source,source_version,environment,derived_from",(BOUND,))]
    for sym in ('BTCUSDT','ETHUSDT'):
        h=hashlib.sha256();n=0
        for r in c.execute("SELECT venue,venue_symbol,feature,observed_at,value,status,source,source_version FROM market_feature_observations WHERE venue='binance_usdm' AND venue_symbol=? AND observed_at<? AND feature IN ('funding_rate','mark_price','index_price','basis_bps') ORDER BY feature,observed_at,source",(sym,BOUND)):
            h.update(json.dumps(list(r),separators=(',',':'),allow_nan=False).encode()+b'\n');n+=1
        result['historical_feature_prefix_hashes'][sym]={'rows':n,'sha256':h.hexdigest(),'close_exclusive_bound':BOUND,'serialization':'compact JSON tuple + newline ordered feature,observed_at,source'}
    c.close()
    return result

if __name__=='__main__':
    result=run()
    (ROOT/'docs/research/cati_alpha_root_cause/additional_inventory.json').write_text(json.dumps(result,indent=2))
