import sqlite3
import json
from datetime import datetime, timedelta, timezone

def run_audit():
    db_path = '../shared/shared_lib/persistence/cosmicforge.db'
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    # Time boundaries
    now = datetime.fromisoformat("2026-04-22T18:06:18.495924+00:00")
    t_minus_24h = (now - timedelta(hours=24)).isoformat()
    t_minus_48h = (now - timedelta(hours=48)).isoformat()

    results = {}

    # ---------------------------------------------------------
    # SECTION 1: LAST 24H FUNNEL COUNTS
    # ---------------------------------------------------------
    c.execute("SELECT COUNT(DISTINCT cycle_id) FROM decision_traces WHERE ts >= ?", (t_minus_24h,))
    total_cycles = c.fetchone()[0]

    c.execute("SELECT COUNT(*) FROM decision_traces WHERE ts >= ?", (t_minus_24h,))
    total_symbols_evaluated = c.fetchone()[0]

    c.execute("SELECT COUNT(*) FROM decision_traces WHERE ts >= ? AND UPPER(signal) = 'HOLD'", (t_minus_24h,))
    hold_decisions = c.fetchone()[0]

    c.execute("SELECT COUNT(*) FROM decision_traces WHERE ts >= ? AND UPPER(signal) != 'HOLD'", (t_minus_24h,))
    non_hold_signals = c.fetchone()[0]

    c.execute("SELECT COUNT(*) FROM decision_traces WHERE ts >= ? AND UPPER(signal) != 'HOLD' AND gate_allowed = 1", (t_minus_24h,))
    passed_layer_a = c.fetchone()[0]

    # Gate Reason distributions
    c.execute("SELECT gate_reason, COUNT(*) FROM decision_traces WHERE ts >= ? GROUP BY gate_reason", (t_minus_24h,))
    gate_reason_counts = {row[0]: row[1] for row in c.fetchall()}

    # Execution Status
    c.execute("SELECT execution_status, COUNT(*) FROM decision_traces WHERE ts >= ? AND execution_status IS NOT NULL AND execution_status != '' GROUP BY execution_status", (t_minus_24h,))
    exec_status_counts = {row[0]: row[1] for row in c.fetchall()}
    
    # Final states
    c.execute("SELECT UPPER(final_state_change), COUNT(*) FROM decision_traces WHERE ts >= ? GROUP BY UPPER(final_state_change)", (t_minus_24h,))
    final_states = {row[0]: row[1] for row in c.fetchall()}

    # Events
    c.execute("SELECT action, COUNT(*) FROM events WHERE ts >= ? AND (action LIKE '%MARGIN_AUDIT%' OR action = 'ORDER_PLACED' OR action = 'PLACE_ORDER' OR action LIKE '%REJECT%') GROUP BY action", (t_minus_24h,))
    event_counts = {row[0]: row[1] for row in c.fetchall()}
    
    results['section_1'] = {
        'total_cycles': total_cycles,
        'total_symbols_evaluated': total_symbols_evaluated,
        'hold_decisions': hold_decisions,
        'non_hold_signals': non_hold_signals,
        'passed_layer_a': passed_layer_a,
        'gate_reason_counts': gate_reason_counts,
        'exec_status_counts': exec_status_counts,
        'final_states': final_states,
        'event_counts': event_counts
    }

    # ---------------------------------------------------------
    # SECTION 3: STRATEGY STALL AUDIT (Hold reasons)
    # ---------------------------------------------------------
    c.execute("SELECT reason_codes, COUNT(*) FROM decision_traces WHERE ts >= ? AND UPPER(signal) = 'HOLD' GROUP BY reason_codes", (t_minus_24h,))
    hold_reasons = {row[0]: row[1] for row in c.fetchall()}

    c.execute("SELECT reason_codes, COUNT(*) FROM decision_traces WHERE ts >= ? AND ts < ? AND UPPER(signal) = 'HOLD' GROUP BY reason_codes", (t_minus_48h, t_minus_24h))
    past_hold_reasons = {row[0]: row[1] for row in c.fetchall()}

    results['section_3'] = {
        'current_hold_reasons': hold_reasons,
        'past_hold_reasons': past_hold_reasons
    }

    # ---------------------------------------------------------
    # SECTION 4 & 5: THRESHOLD AND ELIGIBILITY
    # ---------------------------------------------------------
    c.execute("SELECT symbol, gate_reason, gate_details_json, strategy_signals_json, final_state_change FROM decision_traces WHERE ts >= ? AND UPPER(signal) != 'HOLD'", (t_minus_24h,))
    non_hold_details = []
    for r in c.fetchall():
        non_hold_details.append({
            'symbol': r[0],
            'gate_reason': r[1],
            'gate_details_json': r[2],
            'strategy_signals_json': r[3],
            'final_state_change': r[4]
        })
    
    results['non_hold_samples'] = non_hold_details[:20]

    with open('audit_report.json', 'w') as f:
        json.dump(results, f, indent=2)

if __name__ == '__main__':
    run_audit()
