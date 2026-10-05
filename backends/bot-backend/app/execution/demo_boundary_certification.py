"""Explicit local DEMO certification through the production execution boundary.

No residual decision is created or reused. The immutable plan, attempts and
fills are labelled DEMO_CERTIFICATION and excluded from strategy performance.
Only the canonical owner can process a local operator request; the call-scoped
permit grants signal provenance only, never risk, account or mutation authority.
"""
from contextvars import ContextVar
from dataclasses import asdict
from pathlib import Path
import hashlib
import json
import time

PURPOSE = 'DEMO_CERTIFICATION'
_permit = ContextVar('demo_boundary_certification',default=None)


def permitted(db, plan):
    scope = _permit.get()
    if plan.mode != PURPOSE or plan.setup_family != PURPOSE or plan.environment != 'DEMO' or scope is None:
        return False
    if scope != (str(db.path),plan.broker_account_id,plan.trade_plan_id,plan.trade_plan_hash):
        return False
    from app.trading_intelligence.integration.residual_prospective import owner_current
    if not owner_current(db):
        return False
    with db.connect() as c:
        row = c.execute("SELECT 1 FROM cati_demo_certifications WHERE account_id=? AND plan_id=? AND status='PREPARED'",
                        (plan.broker_account_id,plan.trade_plan_id)).fetchone()
    return bool(row)


def build_plan(account,bot,instrument,reservation_id,run_id,price,now,*,distance=None):
    from app.trading_intelligence.contracts.trade_plan import TradePlan,AllowedEntryZone,TargetZone,ExpectedCosts,ExecutionPreferences
    lineage = {k:run_id for k in ('snapshot_id','market_state_id','regime_distribution_id','source_candidate_id',
        'venue_observation_id','cost_estimate_id','economic_opportunity_id','veto_decision_id',
        'ranking_batch_id','ranked_opportunity_id','portfolio_decision_id')}
    # Deterministic transport geometry, explicitly outside the strategy registry.
    distance = price*.01 if distance is None else distance
    stop,target = price-distance,price+2.5*distance
    return TradePlan.build(**lineage,forecast_id='NOT_APPLICABLE_TRANSPORT_CERTIFICATION',
        portfolio_reservation_id=reservation_id,user_id=account['user_id'],broker_account_id=account['id'],
        bot_instance_id=bot['id'],run_id=run_id,cycle_id=run_id,instrument_key=instrument.to_instrument_key(),
        venue=instrument.to_instrument_key().venue,environment='DEMO',side='LONG',setup_family=PURPOSE,
        setup_version='1',decision_time=now,entry_reference=price,
        allowed_entry_zone=AllowedEntryZone(price,price*.9995,price*1.0005,5.,0.,now+300000),
        structural_invalidation_price=stop,initial_risk_distance=distance,
        target_zones=(TargetZone(run_id,target,target,2.5,'PRIMARY'),),expected_holding_time_ms=300000,
        plan_expiry_time=now+300000,expected_gross_R=0.,expected_net_R=0.,conservative_edge_R=0.,p_net_profitable=0.,
        credible_interval_low=0.,credible_interval_high=0.,raw_support=0,ess=0.,backoff_level=0,
        expected_costs=ExpectedCosts(run_id,run_id,.1,.01,.05,0.,0.,.16,0.,PURPOSE,None,PURPOSE),
        economic_size_assumption=None,
        execution_preferences=ExecutionPreferences('MARKET',(),5.,'NORMAL','ALLOW_PARTIAL_FILL','GTC',10.,30000),
        thesis_conditions=(),invalidation_conditions=(),reason_codes=('TRANSPORT_CERTIFICATION_NOT_STRATEGY',),
        versions=(('execution_purpose',PURPOSE),),mode=PURPOSE,plan_created_at=now)


def run(db, account_id, run_id, *, action='close', symbol='ADAUSDT'):
    from shared_lib.broker.resolver import resolve_broker_auth
    from shared_lib.broker.client_factory import build_client_from_auth
    from shared_lib.broker.environment import normalize_environment,resolve_base_url
    from app.trading_intelligence.integration.residual_prospective import owner_current
    from app.trading_intelligence.integration.production_execution import initialize,account_risk,boundary_for,prepare_submission,reconcile_executions
    from app.trading_intelligence.integration.production_portfolio import execution_portfolio,reconcile_confirmed_intents
    from app.trading_intelligence.trade_plan.evidence_store import TradePlanEvidenceStore
    from shared_lib.broker.auto_trading import authorization
    from shared_lib.core.production import order_submission_gate
    if not owner_current(db):
        raise ValueError('CANONICAL_RUNTIME_LEASE_REQUIRED')
    initialize(db)
    with db.connect() as c:
        c.execute('''CREATE TABLE IF NOT EXISTS cati_demo_certifications (
            run_id TEXT PRIMARY KEY,account_id TEXT NOT NULL,user_id TEXT NOT NULL,
            plan_id TEXT,status TEXT NOT NULL,document TEXT NOT NULL)''')
        account = dict(c.execute('SELECT * FROM broker_accounts WHERE id=?',(account_id,)).fetchone())
        bots = [dict(r) for r in c.execute("SELECT * FROM bot_instances WHERE broker_account_id=? AND status='active'",(account_id,))]
        consent = authorization(c,account,bots)
        prior = c.execute('SELECT * FROM cati_demo_certifications WHERE run_id=?',(run_id,)).fetchone()
    if account['broker_id'] != 'binance' or normalize_environment(account['environment']).value != 'demo':
        raise ValueError('CERTIFICATION_REQUIRES_CONNECTED_BINANCE_DEMO')
    if len(bots) != 1 or bots[0]['user_id'] != account['user_id'] or not consent['enabled']:
        raise ValueError('CERTIFICATION_ACCOUNT_AUTHORIZATION_REQUIRED')
    if not order_submission_gate('DEMO')['enabled']:
        raise ValueError('DEMO_ORDER_SUBMISSION_DISABLED')
    bot = bots[0]
    auth = resolve_broker_auth(account_id,account['user_id'],db)
    client = build_client_from_auth(auth)
    if normalize_environment(client.broker_environment).value != 'demo' or client.base_url.rstrip('/') != resolve_base_url('binance',normalize_environment('demo')):
        raise ValueError('BROKER_ENVIRONMENT_MISMATCH')
    boundary = boundary_for(db,account,bot,client)
    boundary.recover_pending()
    plans = TradePlanEvidenceStore(db)
    now = int(time.time()*1000)
    if prior:
        if prior['account_id'] != account_id or prior['user_id'] != account['user_id']:
            raise ValueError('CERTIFICATION_ACCOUNT_OWNERSHIP_MISMATCH')
        report = json.loads(prior['document'])
        if prior['status']=='COMPLETED':
            return report
        plan = plans.load_plan(account_id,prior['plan_id'])
        if not plan:
            raise ValueError('CERTIFICATION_PLAN_UNAVAILABLE')
        symbol = plan.instrument_key.venue_symbol
    else:
        snapshot = {'positions':client.position_risk(),'orders':client.open_orders()+client.get_algo_orders(symbol,raise_on_error=True)}
        history = reconcile_executions(db,boundary,client,now)
        portfolio = execution_portfolio(db,account,snapshot,history,now)
        if portfolio['active']:
            raise ValueError(portfolio['reason'])
        risk = account_risk(db,account,client,snapshot['positions'],snapshot['orders'],bots,now)
        if risk['reason']:
            raise ValueError(risk['reason'])
        rec = boundary.preflight.catalog.record(boundary.preflight.venue_key,'DEMO',symbol)
        if not boundary.preflight._fresh(rec,now) and boundary.preflight.refresh is not None:
            boundary.preflight.refresh()
            now = int(time.time()*1000)
            rec = boundary.preflight.catalog.record(boundary.preflight.venue_key,'DEMO',symbol)
        if not rec:
            raise ValueError('INSTRUMENT_UNKNOWN')
        instrument = rec['instrument']
        reservation = boundary.reservations.reserve(broker_account_id=account_id,bot_instance_id=bot['id'],cycle_id=run_id,
            selected=[(run_id,instrument.canonical_symbol,boundary.adapter.venue,symbol,'LONG')],now_ms=now,ttl_seconds=300,
            mode='PRODUCTION',production_scope=(account['user_id'],account_id),max_open_positions=1)
        if not reservation.reserved:
            raise ValueError(reservation.conflict_reason)
        rid = reservation.reservation.reservation_id
        try:
            from app.policy.policy_engine import calculate_atr
            bars = client.klines(symbol=symbol,interval='15m',limit=250)
            closed_bars = [b for b in bars if int(b[6]) < int(time.time()*1000)]
            atr = float(calculate_atr(closed_bars,period=14))
            if not atr > 0:
                raise ValueError('CERTIFICATION_ATR_UNAVAILABLE')
            plan = build_plan(account,bot,instrument,rid,run_id,float(client.last_price(symbol)),now,distance=atr)
            plans.append(plan)
            report = {'classification':PURPOSE,'run_id':run_id,'account_id':account_id,'symbol':symbol,
                      'started_at':now,'trade_plan_id':plan.trade_plan_id,'reservation_id':rid,'risk':risk}
            with db.connect() as c:
                c.execute("INSERT INTO cati_demo_certifications VALUES(?,?,?,?,'PREPARED',?)",
                          (run_id,account_id,account['user_id'],plan.trade_plan_id,json.dumps(report)))
            prepared = prepare_submission(db,account,bot,client,boundary,plan,instrument,risk,atr=atr)
            report['risk_controls'] = prepared.pop('controls_evidence')
            token = _permit.set((str(db.path),account_id,plan.trade_plan_id,plan.trade_plan_hash))
            try:
                out = boundary.process_trade_plan(plan,**prepared)
            finally:
                _permit.reset(token)
            report['boundary'] = asdict(out)
            save(db,report,'SUBMITTED' if out.attempt else 'BLOCKED')
            if out.status != 'EXECUTED':
                report.update(status='BLOCKED',reason=out.reason_codes[0] if out.reason_codes else out.status)
                save(db,report,'BLOCKED')
                return report
        finally:
            boundary.reservations.release_unsubmitted(rid,int(time.time()*1000),account_scope=boundary.account_scope,
                                                      bot_instance_id=bot['id'])
    client._production_intent_identity = f'{plan.trade_plan_id}|{plan.trade_plan_hash}'
    history = reconcile_executions(db,boundary,client,int(time.time()*1000))
    entry = next((h for h in history if h['trade_plan_id']==plan.trade_plan_id),None)
    if entry:
        report['entry_reconciliation'] = entry
    entry = entry or report.get('entry_reconciliation')
    if not entry or not entry['order']['answered'] or entry['order']['executed_qty'] <= 0 or not entry['fills']:
        raise ValueError('CERTIFICATION_BROKER_FILL_UNCONFIRMED')
    amount = float(client.get_position_amt(symbol))
    if amount:
        protection = client.get_algo_orders(symbol,raise_on_error=True)
        ids = {str(entry['protection'].get(k)) for k in ('sl_order_id','tp_order_id')} if isinstance(entry['protection'],dict) else set()
        verified = [o for o in protection if str(o.get('algoId')) in ids and o.get('side')=='SELL' and str(o.get('closePosition')).lower()=='true']
        if amount <= 0 or len(verified)!=2:
            raise ValueError('CERTIFICATION_POSITION_OR_PROTECTION_UNCONFIRMED')
        report.update(position_verified=amount,protection_verified=verified,status='PROTECTED')
        save(db,report,'PROTECTED')
        if action=='hold':
            return report
    elif not report.get('position_verified'):
        raise ValueError('CERTIFICATION_POSITION_WAS_NOT_VERIFIED')
    report['close'] = boundary.adapter.submit_exit(symbol,side=plan.side,quantity=abs(amount))
    from .production_protection import cancel_flat_protection
    report['protection_cleanup'] = cancel_flat_protection(client,client._production_intent_identity)
    final = {'positions':client.position_risk(),'orders':client.open_orders()+client.get_algo_orders(symbol,raise_on_error=True)}
    reconcile_confirmed_intents(db,account,final)
    final_history = reconcile_executions(db,boundary,client,int(time.time()*1000))
    report['final_portfolio'] = execution_portfolio(db,account,final,final_history,int(time.time()*1000))
    close_order = report['close'].get('order') or {}
    report['close_fills'] = [f for f in client.user_trades(symbol,start_time_ms=report['started_at'],end_time_ms=int(time.time()*1000),limit=1000)
                             if str(f.get('orderId'))==str(close_order.get('orderId'))]
    if report['final_portfolio']['active'] or not report['close_fills']:
        raise ValueError('CERTIFICATION_FINAL_FLAT_OR_CLOSE_FILL_UNCONFIRMED')
    report.update(status='COMPLETED',completed_at=int(time.time()*1000))
    save(db,report,'COMPLETED')
    return report


def save(db,report,status):
    with db.connect() as c:
        c.execute('UPDATE cati_demo_certifications SET status=?,document=? WHERE run_id=? AND account_id=?',
                  (status,json.dumps(report,default=str),report['run_id'],report['account_id']))


def process_local_request(db):
    request_path = Path('logs/runtime/DEMO_BOUNDARY_CERTIFICATION.json')
    response_path = Path('logs/runtime/DEMO_BOUNDARY_CERTIFICATION_RESULT.json')
    if not request_path.exists():
        return
    try:
        request = json.loads(request_path.read_text(encoding='utf-8-sig'))
        result = run(db,request['account_id'],request['run_id'],action=request.get('action','close'))
    except Exception as exc:
        code = str(exc) if isinstance(exc,ValueError) and str(exc).replace('_','').isalnum() else type(exc).__name__
        result = {'status':'BLOCKED','reason':code}
    response_path.write_text(json.dumps(result,default=str),encoding='utf-8')
    request_path.unlink()
