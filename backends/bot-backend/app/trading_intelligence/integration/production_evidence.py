"""Append-only execution evaluations alongside the current-state projection."""
import json
import uuid


def initialize(conn):
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS cati_execution_evaluations (
            evaluation_id TEXT PRIMARY KEY, account_id TEXT NOT NULL, user_id TEXT NOT NULL,
            bot_instance_id TEXT, decision_id TEXT, observed_at INTEGER NOT NULL,
            stage TEXT NOT NULL, reason TEXT, execution_permission TEXT, document TEXT NOT NULL);
        CREATE INDEX IF NOT EXISTS ix_cati_evaluation_account_decision
            ON cati_execution_evaluations(account_id,decision_id,observed_at);
        CREATE TRIGGER IF NOT EXISTS cati_evaluation_no_update BEFORE UPDATE ON cati_execution_evaluations
            BEGIN SELECT RAISE(ABORT,'execution evaluations are append-only'); END;
        CREATE TRIGGER IF NOT EXISTS cati_evaluation_no_delete BEFORE DELETE ON cati_execution_evaluations
            BEGIN SELECT RAISE(ABORT,'execution evaluations are append-only'); END;
    """)


def record(db, account, result, now):
    row = result.get('latest_cati_decision') or {}
    boundary = result.get('boundary') or {}
    attempt = boundary.get('attempt') or {}
    result.setdefault('stage', 'EVALUATED')
    result.update(trade_plan_id=boundary.get('trade_plan_id') or result.get('trade_plan_id'),
                  execution_attempt_id=attempt.get('execution_attempt_id') or boundary.get('detail',{}).get('execution_attempt_id'),
                  broker_order_id=attempt.get('broker_order_id'))
    rid = result.get('reservation_id')
    with db.connect() as c:
        if rid:
            reservation = c.execute('SELECT status FROM cati_portfolio_reservations WHERE reservation_id=? AND broker_account_id=?', (rid,account['id'])).fetchone()
            result['reservation_status'] = reservation[0] if reservation else None
        eid = 'eval_' + uuid.uuid4().hex
        evidence = {**result, 'evaluation_id':eid, 'account_id':account['id'], 'user_id':account['user_id'],
                    'decision_id':row.get('decision_id'), 'observed_at':now,
                    'score':row.get('score'), 'symbol':row.get('selected_symbol'), 'side':row.get('side'),
                    'final_result':boundary.get('status') or result.get('execution_permission')}
        document = json.dumps(evidence, default=str, sort_keys=True)
        c.execute('INSERT INTO cati_execution_evaluations VALUES(?,?,?,?,?,?,?,?,?,?)',
            (eid, account['id'],account['user_id'],result.get('bot_instance_id'),row.get('decision_id'),now,
             result['stage'],result.get('reason'),result.get('execution_permission'),document))
        if row.get('decision_id'):
            c.execute('INSERT OR REPLACE INTO cati_production_decisions VALUES(?,?,?,?,?)',
                (account['id'],row['decision_id'],result.get('bot_instance_id'),now,document))
    result['evaluation_id'] = eid
    return result
