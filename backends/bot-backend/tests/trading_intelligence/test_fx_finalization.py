import importlib.util
from pathlib import Path
import sqlite3

spec=importlib.util.spec_from_file_location('fx_finalization',Path(__file__).resolve().parents[4]/'scripts/finalize_fx_reference.py')
module=importlib.util.module_from_spec(spec); spec.loader.exec_module(module)


def test_open_session_gap_blocks_fx_freeze_instead_of_becoming_expected():
    c=sqlite3.connect(':memory:')
    c.execute('CREATE TABLE fx_reference_quotes(provider,pair,timeframe,open_time,bid_open,bid_high,bid_low,bid_close,ask_open,ask_high,ask_low,ask_close,volume)')
    c.execute('CREATE TABLE fx_reference_ingest_log(provider,pair,timeframe,period,status)')
    # Monday 2024-08-05 10:00 UTC; no weekend/rollover/holiday exemption.
    start=1722852000000
    u=dict(provider='dukascopy',window_start_ms=start,window_end_ms=start+180000)
    for i in (0,2):
        c.execute('INSERT INTO fx_reference_quotes VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)',
                  ('dukascopy','EURUSD','1m',start+i*60000,1.,1.,1.,1.,1.1,1.1,1.1,1.1,1.))
    r=module.final_partition(c,u,'EURUSD','1m')
    assert r['status']=='INCOMPLETE'
    assert r['rows']==2 and r['expected_rows']==3
    assert r['missing_ranges'][0]['reason']=='UNKNOWN_GAP'
    c.execute('INSERT INTO fx_reference_quotes VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)',
              ('dukascopy','EURUSD','1m',start+60000,1.,1.,1.,1.,1.1,1.1,1.1,1.1,1.))
    r=module.final_partition(c,u,'EURUSD','1m')
    assert r['status']=='COMPLETE' and r['expected_rows']==3
    assert r['gap_classification']['unexpected_gaps']==0
