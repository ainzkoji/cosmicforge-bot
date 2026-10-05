"""Full production boundary certification with a stateful broker transport double."""
import json
from types import SimpleNamespace
import pytest
from test_production_execution import live
from test_production_demo_execution import demo
from test_production_research_separation import fresh
from app.execution import demo_boundary_certification as cert
from app.trading_intelligence.integration import production_execution as production
from app.trading_intelligence.governance.account_authority import AccountExecutionAuthority


@pytest.fixture
def broker(fresh,monkeypatch):
    h=fresh
    monkeypatch.setattr(cert.time,'time',lambda:h.now/1000)
    monkeypatch.setattr('shared_lib.broker.resolver.resolve_broker_auth',lambda *a:object())
    monkeypatch.setattr('shared_lib.broker.client_factory.build_client_from_auth',lambda *a:h.client)
    boundary=h.boundary_for()
    boundary.authority=AccountExecutionAuthority(h.db,(h.account['user_id'],h.account['id']))
    monkeypatch.setattr(production,'boundary_for',lambda *a:boundary)
    h.client.base_url='https://demo-fapi.binance.com'
    h.client._broker_account_id=h.account['id'];h.client._broker_user_id=h.account['user_id']
    state={'qty':0.,'orders':{},'fills':[],'legs':[],'create_count':0}
    h.client.klines.return_value=[[h.now-(251-i)*900000,'100','100.5','99.5','100','1000',h.now-(250-i)*900000-1] for i in range(250)]
    def entry(req):
        from shared_lib.core.production import require_broker_mutation_permission
        require_broker_mutation_permission('POST','/fapi/v1/order',environment='DEMO',broker='binance',base_url=h.client.base_url,
            client=h.client,payload={'symbol':'ADAUSDT','side':'BUY'})
        state['create_count']+=1
        qty=float(req.qty)
        row={'symbol':'ADAUSDT','side':'BUY','orderId':555,'clientOrderId':req.client_order_id,'status':'FILLED',
             'executedQty':str(qty),'origQty':str(qty),'avgPrice':'100','updateTime':h.now}
        state['orders'][req.client_order_id]=row;state['qty']=qty
        state['fills'].append({'id':1,'orderId':555,'symbol':'ADAUSDT','side':'BUY','qty':str(qty),'price':'100','time':h.now})
        return SimpleNamespace(broker_order_id=555,client_order_id=req.client_order_id,qty_filled=qty,avg_fill_price=100.,status='FILLED',model_dump=lambda:row)
    def order(symbol,oid):
        return next(o for o in state['orders'].values() if str(o['orderId'])==str(oid))
    def lookup(symbol,cid):
        if cid not in state['orders']:
            raise RuntimeError('Binance HTTP 400: {"code":-2013}')
        return state['orders'][cid]
    def signed_post(path,params):
        if path.endswith('algoOrder'):
            leg={**params,'algoId':str(len(state['legs'])+1000)}
            state['legs'].append(leg);return leg
        assert params['reduceOnly']=='true' and params['side']=='SELL'
        row={'symbol':'ADAUSDT','side':'SELL','orderId':556,'clientOrderId':params['newClientOrderId'],
             'status':'FILLED','executedQty':str(state['qty']),'avgPrice':'100'}
        state['fills'].append({'id':2,'orderId':556,'symbol':'ADAUSDT','side':'SELL','qty':str(state['qty']),'price':'100','time':h.now})
        state['orders'][params['newClientOrderId']]=row;state['qty']=0.
        return row
    def cancel(path,params):
        state['legs'][:]=[o for o in state['legs'] if str(o['algoId'])!=str(params['algoId'])]
        return {'status':'CANCELED'}
    h.client.place_order.side_effect=entry
    h.client.get_order.side_effect=order
    h.client.get_order_by_client_order_id.side_effect=lookup
    h.client.get_position_info.side_effect=lambda *a:{'positionAmt':str(state['qty']),'entryPrice':'100'}
    h.client.get_position_amt.side_effect=lambda *a:state['qty']
    h.client.position_risk.side_effect=lambda:[{'symbol':'ADAUSDT','positionAmt':str(state['qty'])}]
    h.client.open_orders.return_value=[]
    h.client.get_algo_orders.side_effect=lambda *a,**kw:list(state['legs'])
    h.client.user_trades.side_effect=lambda *a,**kw:list(state['fills'])
    h.client._signed_post.side_effect=signed_post
    h.client._signed_delete.side_effect=cancel
    from app.execution.production_protection import place_native_protection
    h.client.place_protection.side_effect=lambda req:place_native_protection(h.client,req)
    return h,state


def test_certification_fills_protects_restarts_closes_and_never_becomes_strategy(broker):
    h,state=broker
    before=production.latest_decision(h.db)
    report=cert.run(h.db,h.account['id'],'cert-test',action='hold')
    assert report['status']=='PROTECTED',report
    assert state['create_count']==1 and len(state['legs'])==2 and state['qty']>0
    from shared_lib.persistence.db import DB
    restarted=DB(h.db.path)
    report=cert.run(restarted,h.account['id'],'cert-test',action='close')
    assert report['status']=='COMPLETED',report
    assert not report['final_portfolio']['active'] and state['qty']==0 and not state['legs']
    cert.run(restarted,h.account['id'],'cert-test')
    assert state['create_count']==1
    assert production.latest_decision(h.db)==before
    with h.db.connect() as c:
        plan=c.execute('SELECT mode FROM cati_trade_plans WHERE trade_plan_id=?',(report['trade_plan_id'],)).fetchone()
        assert plan['mode']=='DEMO_CERTIFICATION'
        assert c.execute("SELECT status FROM cati_execution_attempts WHERE trade_plan_id=? ORDER BY sequence DESC LIMIT 1",(report['trade_plan_id'],)).fetchone()[0]=='POSITION_CLOSED'
        fills=[json.loads(r[0]) for r in c.execute('SELECT document FROM cati_production_fills')]
        assert {f['_execution']['leg'] for f in fills}=={'ENTRY','EXIT'}
        assert all(f['_execution']['purpose']=='DEMO_CERTIFICATION' for f in fills)


def test_lost_close_ack_is_recovered_by_runtime_without_second_post(broker):
    h,state=broker
    report=cert.run(h.db,h.account['id'],'cert-close-unknown',action='hold')
    original=h.client._signed_post.side_effect
    def lost_ack(path,params):
        original(path,params)
        raise TimeoutError('acknowledgement lost')
    h.client._signed_post.side_effect=lost_ack
    with pytest.raises(TimeoutError):
        cert.run(h.db,h.account['id'],'cert-close-unknown',action='close')
    assert state['qty']==0
    posts=h.client._signed_post.call_count
    with h.db.connect() as c:
        assert c.execute('SELECT status FROM cati_production_closes').fetchone()[0]=='PENDING'
    production.reconcile_executions(h.db,h.boundary_for(),h.client,h.now)
    with h.db.connect() as c:
        assert c.execute('SELECT status FROM cati_production_closes').fetchone()[0]=='CLOSED'
    result=cert.run(h.db,h.account['id'],'cert-close-unknown',action='close')
    assert result['status']=='COMPLETED'
    assert h.client._signed_post.call_count==posts


def test_unknown_close_without_broker_evidence_retains_capacity_and_never_reposts(broker):
    h,state=broker
    cert.run(h.db,h.account['id'],'cert-close-absent',action='hold')
    h.client._signed_post.side_effect=TimeoutError('unknown outcome')
    with pytest.raises(TimeoutError):
        cert.run(h.db,h.account['id'],'cert-close-absent',action='close')
    posts=h.client._signed_post.call_count
    with pytest.raises(ValueError,match='CLOSE_SUBMIT_OUTCOME_UNKNOWN'):
        production.reconcile_executions(h.db,h.boundary_for(),h.client,h.now)
    assert state['qty']>0 and len(state['legs'])==2
    assert h.client._signed_post.call_count==posts
    with h.db.connect() as c:
        assert c.execute('SELECT status FROM cati_production_closes').fetchone()[0]=='PENDING'


def test_certificate_plan_cannot_authorize_itself_outside_local_scope(fresh):
    plan=cert.build_plan(fresh.account,{'id':fresh.plan.bot_instance_id},fresh.instrument('ADAUSDT'),fresh.plan.portfolio_reservation_id,'forged',100.,fresh.now)
    assert not cert.permitted(fresh.db,plan)
    result=fresh.run(fresh.demo_boundary(),plan=plan,atr=1.)
    assert result.reason_codes==('PRODUCTION_REQUIRES_FROZEN_RESIDUAL_DECISION',)
    fresh.client.place_order.assert_not_called()
