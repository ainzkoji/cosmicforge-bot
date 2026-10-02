"""Reconstruct sampled frozen labels from strictly bounded pre-holdout candle reads."""
import json, sqlite3, sys
from dataclasses import asdict
from pathlib import Path
REPO=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(REPO/'backends/bot-backend'),str(REPO/'backends/shared')]
from app.trading_intelligence.forecast.build_library import BuildConfig,_freeze_decision_point
from app.trading_intelligence.forecast.labels import label_candidate
from app.trading_intelligence.forecast.cohorts import derive_cohort_dimensions
from app.trading_intelligence.portfolio.groups import static_group_for
from app.replay.cost_model import BINANCE_FUTURES_STANDARD
from app.replay.historical_provider import HistoricalClock,HistoricalMarketDataProvider
from app.trading_intelligence.regime.policy import default_policy
from app.trading_intelligence.setups.policy import default_policies

def run(library, diagnostic, db):
    m=json.loads((library/'manifest.json').read_text())
    sampled=json.loads((diagnostic/'label_audit_rows.json').read_text());span=900000;hold=m['governance']['holdout_start_ms']
    conn=sqlite3.connect('file:'+db.as_posix()+'?mode=ro',uri=True)
    results=[]
    try:
        for d in sampled:
            label=d['label'];sym=label['instrument_key']['venue_symbol'];t=label['decision_time'];h=m['label_horizon_bars']
            lo=t-300*span;hi=t+h*span
            assert hi<hold,'Refusing a source query that touches holdout'
            raw=conn.execute('SELECT open_time,open,high,low,close,volume,quote_volume,trades,base_currency,quote_currency '
                "FROM historical_candles WHERE symbol=? AND interval=? AND market_type='crypto' AND data_source='binance' "
                'AND open_time BETWEEN ? AND ? ORDER BY open_time',(sym,m['timeframe'],lo,hi)).fetchall()
            candles=[[r[0],*r[1:6],r[0]+span-1,r[6] or 0,r[7] or 0] for r in raw]
            assert all(r[6]<hold for r in candles)
            cfg=BuildConfig(symbols=(sym,),timeframe=m['timeframe'],start_ms=t,end_ms=t,label_horizon_bars=h,source_kind='REAL_MARKET')
            provider=HistoricalMarketDataProvider({sym:{m['timeframe']:candles}},HistoricalClock(t),source=cfg.source_name,source_environment='HISTORICAL')
            frozen=_freeze_decision_point(provider,sym,m['timeframe'],cfg,{'base':raw[0][8],'quote':raw[0][9]},default_policy(),default_policies())
            candidates=frozen[3] if frozen else []
            candidate=next((c for c in candidates if c.setup_candidate_id==label['setup_candidate_id']),None)
            entry={'label_id':label['label_id'],'asset':sym,'time':t,'source_bars':len(candles),'query_max_open_ms':hi,
                   'candidate_reproduced':candidate is not None}
            if candidate:
                rebuilt=label_candidate(candidate,candles,cost_model=BINANCE_FUTURES_STANDARD,horizon_bars=h,cost_model_version=m['cost_model_version'])
                fresh=json.loads(json.dumps(asdict(rebuilt)))
                entry['label_differences']={k:{'old':v,'new':fresh[k]} for k,v in label.items() if v!=fresh[k]}
                dims=derive_cohort_dimensions(setup_family=candidate.setup_family,side=candidate.side,market_state=frozen[1],regime_distribution=frozen[2],instrument_group=static_group_for(sym))
                entry['dimensions_equal']=dims==d['cohort_dimensions']
                entry['entry_reference']=candidate.trigger_reference
                entry['structural_risk']=candidate.initial_structural_risk
                entry['first_future_open_ms']=next(r[0] for r in candles if r[0]>t)
                entry['label_end_ms']=t+h*span
            results.append(entry)
    finally:conn.close()
    out={'sample_count':len(results),'exact_labels':sum(r.get('candidate_reproduced') and not r.get('label_differences') and r.get('dimensions_equal') for r in results),
         'holdout_read':False,'source_db':str(db),'results':results}
    target=diagnostic/'source_label_audit.json'
    if target.exists():raise RuntimeError('Refusing overwrite')
    target.write_text(json.dumps(out,indent=2)+'\n');print(json.dumps({k:v for k,v in out.items() if k!='results'}))

if __name__=='__main__':
    run(REPO/'data/research/cati_libraries/cati_lib_0ead8264955eb958b20e165e',
        REPO/'data/research/calibration_diagnostics/v1_root_cause',REPO/'backends/shared/shared_lib/persistence/cosmicforge.db')
