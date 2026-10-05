"""Frozen research overlap never owns a production account's one execution slot."""
import json
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from types import SimpleNamespace
import pytest
from test_production_demo_execution import demo
from test_production_execution import live
from app.trading_intelligence.integration import production_execution as production
from app.trading_intelligence.integration.residual_prospective import Tracker, H, Q, REGISTRY_HASH
from app.trading_intelligence.integration.production_portfolio import durable_occupancy, execution_portfolio
from app.trading_intelligence.portfolio.reservation_store import CATIReservationStore
from app.trading_intelligence.execution.preflight import SubmissionPreflight
from app.exchange.instruments import InstrumentCatalog
from app.product_safety import execution_safety


@pytest.fixture
def fresh(demo, monkeypatch):
    # The fixture's first real research observation remains OPEN. A subsequent
    # naturally committed synthetic decision is research-overlap rejected.
    tracker = Tracker(demo.db, now_ms=demo.now)
    with demo.db.connect() as c:
        original = dict(c.execute('SELECT * FROM cati_residual_decisions WHERE decision_id=?', (demo.plan.source_candidate_id,)).fetchone())
        c.execute("UPDATE bot_instances SET status='stopped' WHERE broker_account_id=? AND id<>?", (demo.account['id'],demo.plan.bot_instance_id))
        c.execute("UPDATE broker_accounts SET environment='demo' WHERE id=?", (demo.account['id'],))
    demo.reservations.release(demo.plan.portfolio_reservation_id, demo.now)
    decision = original['decision_time'] + H
    did, _ = tracker.commit_decision(decision, json.loads(original['snapshot_json']), decision+100)
    tracker.record_entry(did, decision+1, 100., decision+200)
    demo.now = decision+1000
    row = production.latest_decision(demo.db)
    assert row['decision_id']==did and not row['portfolio_selected']
    assert json.loads(row['risk_state_json'])['overlap_rejected']
    demo.original, demo.row = original, row
    catalog=InstrumentCatalog(demo.db)
    catalog.upsert('binance_usdm','DEMO',[demo.instrument('ADAUSDT')],demo.now-100)
    boundary=demo.demo_boundary()
    boundary.preflight=SubmissionPreflight(catalog=catalog,broker='binance',venue_key='binance_usdm',catalog_environment='DEMO',account_environment='demo')
    boundary.authority.gov=SimpleNamespace(kill_switch_on=lambda **kw:False)
    demo.boundary_for=lambda *a:boundary
    demo.client.account.return_value=dict(totalWalletBalance=5000,totalMarginBalance=5000,availableBalance=5000,totalInitialMargin=0,totalUnrealizedProfit=0)
    demo.client.income_history.return_value=[]
    demo.client.last_price.return_value=100.
    demo.client.klines.return_value=[]
    demo.client.exchange_info_cached.return_value={'symbols':[{'symbol':'ADAUSDT','baseAsset':'ADA','quoteAsset':'USDT','marginAsset':'USDT','contractType':'PERPETUAL','orderTypes':['MARKET','STOP_MARKET','TAKE_PROFIT_MARKET'],'timeInForce':['GTC'],'filters':[{'filterType':'PRICE_FILTER','tickSize':'.01'},{'filterType':'LOT_SIZE','stepSize':'.001','minQty':'.001'}]}]}
    monkeypatch.setattr(production,'persisted_risk_controls',lambda *a:{})
    monkeypatch.setattr(execution_safety,'evaluate_execution_kyc',lambda **kw:SimpleNamespace(allowed=True,state='APPROVED'))
    monkeypatch.setattr(execution_safety,'evaluate_execution_readiness',lambda **kw:SimpleNamespace(allowed=True,state='APPROVED'))
    return demo


def process(fresh, positions=None, orders=None):
    return production.process_account(fresh.db,fresh.account,fresh.client,{'positions':positions or [],'orders':orders or []},now_ms=fresh.now,boundary_factory=fresh.boundary_for)


def test_open_research_observation_does_not_block_fresh_production_candidate(fresh):
    assert production.eligibility(fresh.row,fresh.now) is None
    result=process(fresh)
    assert result['research_portfolio_active'] and result['research_observation_overlap']
    assert 'boundary' in result, result
    assert fresh.client.place_order.call_count==1, result
    with fresh.db.connect() as c:
        assert dict(c.execute('SELECT * FROM cati_residual_decisions WHERE decision_id=?',(fresh.original['decision_id'],)).fetchone())==fresh.original
        assert dict(c.execute('SELECT * FROM cati_residual_decisions WHERE decision_id=?',(fresh.row['decision_id'],)).fetchone())==fresh.row
        assert c.execute('SELECT mode FROM cati_portfolio_reservations WHERE cycle_id=?',(fresh.row['decision_id'],)).fetchone()[0]=='PRODUCTION'
    assert process(fresh)['reason']!='RESIDUAL_PORTFOLIO_OVERLAP'
    assert fresh.client.place_order.call_count==1


@pytest.mark.parametrize('kind',['position','entry','protection'])
def test_broker_exposure_blocks_even_with_research_overlap(fresh,kind):
    positions=[{'symbol':'ETHUSDT','positionAmt':1}] if kind=='position' else []
    orders=[{'symbol':'ETHUSDT','reduceOnly':kind=='protection'}] if kind!='position' else []
    out=process(fresh,positions,orders)
    assert out['execution_permission']=='BLOCKED_RISK'
    assert out['reason'] in {'ACCOUNT_WIDE_POSITION_OR_ORDER_ACTIVE','ACCOUNT_WIDE_ORPHAN_ORDER_ACTIVE'}
    fresh.client.place_order.assert_not_called()


@pytest.mark.parametrize('defect,reason',[
 ('expired','PROSPECTIVE_ENTRY_WINDOW_EXPIRED'),('id','RESIDUAL_DECISION_ID_MISMATCH'),
 ('source','FROZEN_RESIDUAL_PROVENANCE_REQUIRED'),('registry','FROZEN_RESIDUAL_PROVENANCE_REQUIRED'),
 ('content','RESIDUAL_DECISION_CONTENT_MISMATCH'),('gap','NON_EXECUTABLE_GAP'),
 ('entry','NEXT_NATIVE_OPEN_PROVENANCE_REQUIRED'),('future_entry','NEXT_NATIVE_OPEN_PROVENANCE_REQUIRED')])
def test_research_overlap_does_not_bypass_signal_provenance(fresh,defect,reason):
    row=dict(fresh.row);now=fresh.now
    if defect=='expired':now=row['decision_time']+Q
    elif defect=='id':row['decision_id']='forged'
    elif defect=='registry':row['registry_hash']='another'
    elif defect=='source':
        state=json.loads(row['risk_state_json']);state['source']='forged';row['risk_state_json']=json.dumps(state)
    elif defect=='content':row['score']=99
    elif defect=='gap':row['lifecycle']='NON_EXECUTABLE_GAP'
    elif defect=='entry':row['entry_time']+=1
    elif defect=='future_entry':
        detail=json.loads(row['outcome_json']);detail['entry_received_at']=now+1;row['outcome_json']=json.dumps(detail)
    assert production.eligibility(row,now)==reason


def test_unknown_intent_survives_restart_on_flat_account(fresh):
    with fresh.db.connect() as c:
        c.execute("INSERT INTO pending_entries(id,bot_id,symbol,side,state,submit_state,client_order_id,created_at,updated_at) VALUES('unknown',?,'ADAUSDT','LONG','PENDING_OPEN','SUBMIT_UNKNOWN','cid',0,0)",(fresh.plan.bot_instance_id,))
    result=process(fresh)
    assert result['reason']=='ACCOUNT_SUBMIT_OUTCOME_UNRESOLVED'
    assert result['execution_portfolio']['state']=='UNKNOWN'
    from shared_lib.persistence.db import DB
    with DB(fresh.db.path).connect() as c:
        assert durable_occupancy(c,fresh.account['id'],fresh.now)['state']=='UNKNOWN'
    fresh.client.place_order.assert_not_called()


def test_unknown_protection_survives_flat_broker_snapshot(fresh):
    with fresh.db.connect() as c:
        c.execute('CREATE TABLE IF NOT EXISTS cati_production_protection(account_id TEXT,client_id TEXT,document TEXT,response TEXT)')
        c.execute("INSERT INTO cati_production_protection VALUES(?,'protection-unknown',?,NULL)",(fresh.account['id'],json.dumps({'symbol':'ADAUSDT'})))
    assert process(fresh)['reason']=='ACCOUNT_SUBMIT_OUTCOME_UNRESOLVED'
    fresh.client.place_order.assert_not_called()


def test_shadow_selection_without_execution_lineage_does_not_occupy_production(fresh):
    held=CATIReservationStore(fresh.db).reserve(broker_account_id=fresh.account['id'],bot_instance_id=fresh.plan.bot_instance_id,cycle_id='research-only',selected=[('research-only','ETH','BINANCE_USDM','ETHUSDT','LONG')],now_ms=fresh.now,ttl_seconds=900)
    assert held.reserved
    with fresh.db.connect() as c:
        assert not durable_occupancy(c,fresh.account['id'],fresh.now)['active']
    assert 'boundary' in process(fresh)
    assert fresh.client.place_order.call_count==1


def test_two_simultaneous_production_cycles_create_only_once(fresh):
    barrier=Barrier(2)
    def cycle(_):
        barrier.wait()
        return process(fresh)
    with ThreadPoolExecutor(2) as pool:
        results=list(pool.map(cycle,[0,1]))
    assert fresh.client.place_order.call_count==1, results
    assert all(r['reason']!='RESIDUAL_PORTFOLIO_OVERLAP' for r in results)


def test_closed_lineage_is_free_but_unanswered_readback_is_unknown(fresh):
    history=[{'trade_plan_id':'plan','cati_decision_id':'decision','order':{'answered':True,'status':'FILLED'},'position':{'answered':True,'quantity':0}}]
    snapshot={'positions':[],'orders':[]}
    assert not execution_portfolio(fresh.db,fresh.account,snapshot,history,fresh.now)['active']
    history[0]['order']['answered']=False
    assert execution_portfolio(fresh.db,fresh.account,snapshot,history,fresh.now)['state']=='UNKNOWN'


def test_old_confirmed_executor_lock_reconciles_through_existing_lifecycle(fresh):
    from app.execution.entry_protection import get_entry_protection
    protector=get_entry_protection(fresh.db)
    # A broker-confirmed position from a previous bot cycle has since closed.
    with fresh.db.connect() as c:
        c.execute("INSERT INTO pending_entries(id,bot_id,symbol,side,state,submit_state,client_order_id,confirmed_at_ms,submitted_at_ms,created_at,updated_at) VALUES('old-confirmed',?,'DOTUSDT','LONG','OPEN_CONFIRMED','SUBMIT_CONFIRMED','old-cid',1,1,0,0)",(fresh.plan.bot_instance_id,))
    assert 'boundary' in process(fresh)
    assert fresh.client.place_order.call_count==1
    with fresh.db.connect() as c:
        assert not c.execute("SELECT 1 FROM pending_entries WHERE id='old-confirmed'").fetchone()


def test_latest_decision_change_before_create_releases_reservation(fresh,monkeypatch):
    from unittest.mock import Mock
    monkeypatch.setattr(production,'latest_decision',Mock(side_effect=[fresh.row,None]))
    result=process(fresh)
    assert result['reason']=='CURRENT_CATI_DECISION_CHANGED'
    assert not result['execution_portfolio_active']
    fresh.client.place_order.assert_not_called()
    with fresh.db.connect() as c:
        assert c.execute("SELECT status FROM cati_portfolio_reservations WHERE mode='PRODUCTION'").fetchone()[0]=='RELEASED'


def test_production_reservation_account_slot_is_atomic_across_different_symbols(fresh):
    with fresh.db.connect() as c:
        bot=dict(c.execute('SELECT * FROM bot_instances WHERE id=?',(fresh.plan.bot_instance_id,)).fetchone())
        bot['id']='other-production-bot'
        bot['status']='stopped'
        c.execute(f"INSERT INTO bot_instances ({','.join(bot)}) VALUES ({','.join('?' for _ in bot)})",tuple(bot.values()))
    barrier=Barrier(2)
    def reserve(i):
        barrier.wait()
        return CATIReservationStore(fresh.db).reserve(broker_account_id=fresh.account['id'],bot_instance_id=fresh.plan.bot_instance_id if i else bot['id'],cycle_id=str(i),selected=[(str(i),'ADA' if i else 'ETH','BINANCE_USDM','ADAUSDT' if i else 'ETHUSDT','LONG')],now_ms=fresh.now,ttl_seconds=900,mode='PRODUCTION',production_scope=(fresh.account['user_id'],fresh.account['id']),max_open_positions=1)
    with ThreadPoolExecutor(2) as pool:results=list(pool.map(reserve,[0,1]))
    assert sum(r.reserved for r in results)==1
    assert [r.conflict_reason for r in results if not r.reserved]==['EXECUTION_PORTFOLIO_OVERLAP']
    assert process(fresh)['reason']=='EXECUTION_PORTFOLIO_OVERLAP'
    fresh.client.place_order.assert_not_called()
