"""Historical features, event/fill metadata and bounded prefix fingerprints."""
import hashlib
import json
import sqlite3
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
BOUND=1783876499999
OUT=ROOT/'docs/research/cati_alpha_root_cause'

def run():
    result={'boundary_exclusive':BOUND,'holdout_queries':0,'databases':[]}
    paths=list((ROOT/'data/research').glob('*.db'))+[ROOT/'data/bot.db',ROOT/'backends/shared/shared_lib/persistence/cosmicforge.db']
    for p in paths:
        c=sqlite3.connect(p.as_uri()+'?mode=ro',uri=True);c.row_factory=sqlite3.Row
        tables={r[0] for r in c.execute("select name from sqlite_master where type='table'")}
        rec={'path':str(p.relative_to(ROOT)),'profiles':{}}
        if 'market_feature_observations' in tables:
            rec['profiles']['market_feature_observations']=[dict(r) for r in c.execute('SELECT venue,venue_symbol,feature,source,status,unavailable_reason,COUNT(*) rows,MIN(observed_at) first,MAX(observed_at) last,MIN(ingested_at) first_ingest,MAX(ingested_at) last_ingest FROM market_feature_observations WHERE observed_at<? GROUP BY venue,venue_symbol,feature,source,status,unavailable_reason',(BOUND,))]
        for table,stamp,fields in [('event_market_snapshots','timestamp_utc',['spread','bid_depth','ask_depth']),('market_event_reactions','post_window_end_utc',['spread_before','spread_during','spread_after','order_book_depth_change']),('trade_fills','timestamp_utc',['fee','slippage_pct','funding_fees','fees_estimated','slippage_estimated'])]:
            if table not in tables: continue
            cols={r[1] for r in c.execute('pragma table_info('+table+')')}
            fields=[f for f in fields if f in cols]
            query='SELECT COUNT(*) rows,MIN('+stamp+') first,MAX('+stamp+') last'+''.join(',COUNT('+f+') populated_'+f for f in fields)+' FROM '+table+' WHERE CAST((julianday('+stamp+')-2440587.5)*86400000 AS INTEGER)<?'
            rec['profiles'][table]=dict(c.execute(query,(BOUND,)).fetchone())
            rec['profiles'][table]['causal_usability']='Event after-windows are outcomes, not decision features; fills are execution-history diagnostics. Ingest-time/source quality required before research joins.'
        c.close();result['databases'].append(rec)
    (OUT/'supplemental_inventory.json').write_text(json.dumps(result,indent=2))
    c=sqlite3.connect((ROOT/'data/research/crypto_deep_binance.db').as_uri()+'?mode=ro',uri=True)
    hashes={};counts={};times={}
    for row in c.execute("SELECT venue,venue_symbol,feature,observed_at,value,status,source,source_version FROM market_feature_observations WHERE venue='binance_usdm' AND observed_at<? AND feature IN ('funding_rate','mark_price','index_price','basis_bps') ORDER BY venue_symbol,feature,observed_at,source",(BOUND,)):
        sym=row[1]
        hashes.setdefault(sym,hashlib.sha256()).update(json.dumps(row,separators=(',',':'),allow_nan=False).encode()+b'\n')
        counts[sym]=counts.get(sym,0)+1;times.setdefault(sym,[row[3],row[3]])
        times[sym]=[min(times[sym][0],row[3]),max(times[sym][1],row[3])]
    out={s:{'sha256':h.hexdigest(),'rows':counts[s],'first':times[s][0],'last':times[s][1]} for s,h in hashes.items()}
    c.close()
    (OUT/'historical_feature_source_hashes.json').write_text(json.dumps({'exclusive_upper_bound':BOUND,'serialization':'compact JSON tuple + newline; order venue_symbol,feature,observed_at,source','sources':out,'holdout_queries':0},indent=2))

if __name__=='__main__': run()
