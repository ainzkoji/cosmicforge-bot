import sqlite3
import pandas as pd
import json

def get_stats(db_path):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    
    report = {}

    schema = {}
    try:
        pragma = conn.execute("PRAGMA table_info(decision_traces)").fetchall()
        cols = [r['name'] for r in pragma]
        schema['has_ml_cols'] = all(c in cols for c in ['ml_score', 'ml_action', 'gate_allowed', 'side', 'adx', 'regime_state'])
        schema['columns'] = cols
        report['schema'] = schema
    except Exception as e:
        report['schema_error'] = str(e)

    try:
        gate_query = 'SELECT * FROM decision_traces'
        gate_df = pd.read_sql_query(gate_query, conn)
        report['GATE_dataset_rows'] = len(gate_df)
        
        if 'gate_allowed' in gate_df.columns:
            report['GATE_gate_allowed_dist'] = gate_df['gate_allowed'].value_counts().to_dict()
        if 'side' in gate_df.columns:
            report['GATE_side_dist'] = gate_df['side'].value_counts().to_dict()
            report['side_is_all_zeros'] = bool((gate_df['side'] == 0).all()) if not gate_df.empty else False
        if 'symbol' in gate_df.columns:
            report['GATE_distinct_symbols'] = int(gate_df['symbol'].nunique()) if not gate_df.empty else 0
        if 'chosen_strategy' in gate_df.columns:
            report['GATE_distinct_strategies'] = int(gate_df['chosen_strategy'].nunique()) if not gate_df.empty else 0
        if 'ts' in gate_df.columns and not gate_df.empty:
            report['GATE_date_range'] = [str(gate_df['ts'].min()), str(gate_df['ts'].max())]
    except Exception as e:
        report['GATE_dataset_error'] = str(e)
    
    try:
        entry_query = '''
            SELECT ut.trace_id, t.position_id, t.timestamp_utc, ut.ts, ut.symbol
            FROM decision_traces ut
            INNER JOIN trade_fills t
                ON ut.run_id = t.run_id AND ut.cycle_id = t.cycle_id AND ut.symbol = t.symbol
            WHERE t.action = 'OPEN' 
        '''
        if 'gate_allowed' in locals().get('cols', []):
            entry_query += " AND ut.gate_allowed = 1"
            
        entry_df = pd.read_sql_query(entry_query, conn)
        report['ENTRY_dataset_rows'] = len(entry_df)
        if 'symbol' in entry_df.columns:
            report['ENTRY_distinct_symbols'] = int(entry_df['symbol'].nunique()) if not entry_df.empty else 0
        if 'ts' in entry_df.columns and not entry_df.empty:
            report['ENTRY_date_range'] = [str(entry_df['ts'].min()), str(entry_df['ts'].max())]
    except Exception as e:
        report['ENTRY_dataset_error'] = str(e)
    
    try:
        sanity = {}
        if 'trace_id' in gate_df.columns:
            sanity['duplicate_traces'] = sum(gate_df.duplicated(subset=['trace_id']))
        else:
            sanity['duplicate_traces'] = 'N/A'
        
        ft_cols = ['regime_state', 'buy_score', 'confidence', 'adx', 'atr_pct']
        for c in ft_cols:
            if c in cols:
                null_q = f"SELECT count(*) as nulls, count(*) as total FROM decision_traces WHERE {c} IS NULL"
                row = conn.execute(null_q).fetchone()
                sanity[f'null_rate_{c}'] = row['nulls'] / max(1, row['total'])
            else:
                sanity[f'null_rate_{c}'] = 'COLUMN_MISSING'

        report['sanity_checks'] = sanity
    except Exception as e:
        report['sanity_checks_error'] = str(e)

    print(json.dumps(report, indent=2))

if __name__ == '__main__':
    get_stats('../shared/shared_lib/persistence/cosmicforge.db')
