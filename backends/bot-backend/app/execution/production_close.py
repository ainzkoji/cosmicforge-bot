"""Durable reduce-only CLOSE, with broker read-back before any restart action."""
import hashlib
import json
import time


def close_position(client, symbol):
    from shared_lib.core.production import order_submission_gate
    from app.trading_intelligence.integration.residual_prospective import owner_current
    from .demo_transport_smoke import lookup
    db = client._production_db
    account = client._production_account_id
    identity = getattr(client,'_production_intent_identity',None)
    if not identity or not owner_current(db):
        raise ValueError('PRODUCTION_CLOSE_AUTHORITY_REQUIRED')
    if not order_submission_gate(client.broker_environment)['enabled']:
        raise ValueError(order_submission_gate(client.broker_environment)['reason'])
    cid = 'CFCLOSE' + hashlib.sha256((account+identity+symbol).encode()).hexdigest()[:24]
    with db.connect() as c:
        c.execute('''CREATE TABLE IF NOT EXISTS cati_production_closes (
            account_id TEXT,client_id TEXT,symbol TEXT,identity TEXT,status TEXT,
            document TEXT,PRIMARY KEY(account_id,client_id))''')
        prior = c.execute('SELECT * FROM cati_production_closes WHERE account_id=? AND client_id=?',(account,cid)).fetchone()
    if prior:
        order = lookup(client,symbol,cid)
        if not order:
            raise ValueError('CLOSE_SUBMIT_OUTCOME_UNKNOWN')
    else:
        amount = float(client.get_position_amt(symbol))
        if not amount:
            return {'status':'no_position','symbol':symbol}
        request = {'symbol':symbol,'side':'SELL' if amount > 0 else 'BUY','type':'MARKET',
                   'quantity':abs(amount),'reduceOnly':'true','newClientOrderId':cid}
        document = {'request':request,'requested_at':int(time.time()*1000)}
        with db.connect() as c:
            inserted = c.execute("INSERT OR IGNORE INTO cati_production_closes VALUES(?,?,?,?,'PENDING',?)",
                (account,cid,symbol,identity,json.dumps(document))).rowcount
        if not inserted:
            raise ValueError('CLOSE_SUBMIT_OUTCOME_UNKNOWN')
        # Never cancel working native protection before a confirmed flat close.
        client._signed_post('/fapi/v1/order',params=request)
        order = lookup(client,symbol,cid)
        if not order:
            raise ValueError('CLOSE_SUBMIT_OUTCOME_UNKNOWN')
    if order.get('status') != 'FILLED' or float(client.get_position_amt(symbol)) != 0:
        raise ValueError('CLOSE_FILL_OR_FLAT_UNCONFIRMED')
    with db.connect() as c:
        document = json.loads(c.execute('SELECT document FROM cati_production_closes WHERE account_id=? AND client_id=?',(account,cid)).fetchone()[0])
        document.update(order=order,confirmed_at=int(time.time()*1000),final_position=0)
        c.execute("UPDATE cati_production_closes SET status='CLOSED',document=? WHERE account_id=? AND client_id=?",
                  (json.dumps(document),account,cid))
        plan = c.execute('SELECT bot_instance_id,side FROM cati_trade_plans WHERE broker_account_id=? AND trade_plan_id=?',
                         (account,identity.split('|')[0])).fetchone()
    if plan:
        from .entry_protection import get_entry_protection
        get_entry_protection(db).mark_closed(plan['bot_instance_id'],symbol,plan['side'])
    return order
