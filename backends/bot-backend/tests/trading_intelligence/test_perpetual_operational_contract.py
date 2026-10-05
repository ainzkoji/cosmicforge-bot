from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import Mock
import pytest
from test_production_demo_execution import demo
from test_production_execution import live
from app.execution.perpetual_protection import place_protection
from app.models.unified_trading import ProtectionRequest, Side
from app.exchange.bybit.client import BybitClient
from shared_lib.broker.auto_trading import authorization,set_authorization

@pytest.mark.parametrize('broker',['bybit','bingx'])
def test_durable_protection_recovery(demo,broker):
 client=Mock(_production_db=demo.db,_production_account_id=demo.account['id'],_broker_account_id=demo.account['id'],_production_intent_identity='intent',broker_environment='demo',base_url='https://api-demo.bybit.com' if broker=='bybit' else 'https://open-api-vst.bingx.com')
 client.position_risk.return_value=[{'positionAmt':'1'}];client.get_instrument.return_value=SimpleNamespace(tick_size=Decimal('.01'));client._fmt_qty.return_value='1';client._normalize_symbol.return_value='ADA-USDT'
 book=[]
 def create(method,path,payload):
  oid=str(len(book)+1)
  book.append({'orderId':oid,'symbol':'ADAUSDT','side':'SELL','type':'STOP_MARKET' if not book else 'TAKE_PROFIT_MARKET','stopPrice':payload.get('triggerPrice',payload.get('stopPrice')),'reduceOnly':broker=='bybit','closePosition':broker=='bingx','clientOrderId':payload.get('orderLinkId')})
  return {'retCode':0,'result':{'orderId':oid}} if broker=='bybit' else {'code':0,'data':{'orderId':oid}}
 client._request_v5.side_effect=create;client._request.side_effect=create;client._ok.side_effect=lambda res,what:res['result']
 client.open_orders.side_effect=lambda symbol:list(book);client.get_order.side_effect=lambda symbol,oid:next(o for o in book if o['orderId']==str(oid))
 req=ProtectionRequest(symbol='ADAUSDT',position_side=Side.BUY,qty=1,sl_price='98',tp_price='105')
 assert place_protection(client,req,broker=broker).status=='success'
 assert place_protection(client,req,broker=broker).status=='success'
 assert len(book)==2
 for call in (client._request_v5 if broker=='bybit' else client._request).call_args_list:
  p=call.args[2];assert p.get('reduceOnly') is True or p.get('closePosition')=='true'
  if broker=='bingx':assert 'clientOrderId' not in p and 'reduceOnly' not in p

@pytest.mark.parametrize('broker',['bybit','bingx'])
def test_unknown_protection_never_recreates(demo,broker):
 client=Mock(_production_db=demo.db,_production_account_id=demo.account['id'],_broker_account_id=demo.account['id'],_production_intent_identity='unknown',broker_environment='demo',base_url='https://api-demo.bybit.com' if broker=='bybit' else 'https://open-api-vst.bingx.com')
 client.position_risk.return_value=[{'positionAmt':1}];client.get_instrument.return_value=SimpleNamespace(tick_size=.01);client.open_orders.return_value=[];client._fmt_qty.return_value='1';client._normalize_symbol.return_value='ADA-USDT'
 transport=client._request_v5 if broker=='bybit' else client._request;transport.side_effect=TimeoutError()
 req=ProtectionRequest(symbol='ADAUSDT',position_side=Side.BUY,qty=1,sl_price='98',tp_price='105')
 with pytest.raises(TimeoutError):place_protection(client,req,broker=broker)
 with pytest.raises(ValueError,match='OUTCOME_UNKNOWN'):place_protection(client,req,broker=broker)
 assert transport.call_count==1

def test_bybit_complete_ledger(demo):
 client=BybitClient('fixture','fixture',base_url='https://api-demo.bybit.com')
 client._request_v5=Mock(side_effect=[{'retCode':0,'result':{'list':[{'id':'1','transactionTime':'10','currency':'USDT','cashFlow':'-3','fee':'1','funding':'-.5','change':'-4.5','type':'TRADE'}],'nextPageCursor':'next'}},{'retCode':0,'result':{'list':[{'id':'2','transactionTime':'20','currency':'USDT','cashFlow':'10','change':'10','type':'TRANSFER_IN'}]}}])
 income=client.income_history(0,30)
 assert sum(r['income'] for r in income)==5.5
 assert sum(r['income'] for r in income if r['incomeType']!='TRANSFER')==-4.5
 assert client._request_v5.call_args_list[1].args[2]['cursor']=='next'

def test_bybit_cursor_loop_fails_closed(demo):
 client=BybitClient('fixture','fixture',base_url='https://api-demo.bybit.com');client._request_v5=Mock(return_value={'retCode':0,'result':{'list':[],'nextPageCursor':'loop'}})
 with pytest.raises(ValueError,match='INCOMPLETE'):client.income_history(0,30)


def test_bybit_native_conditional_direction_identifies_take_profit(demo):
 client=BybitClient('fixture','fixture',base_url='https://api-demo.bybit.com')
 raw={'orderId':'tp','orderType':'Market','stopOrderType':'Stop','closeOnTrigger':True,'reduceOnly':True,'side':'Sell','triggerDirection':1,'triggerPrice':'105'}
 assert client._order_view(raw)['type']=='TAKE_PROFIT_MARKET'
 assert client._order_view({**raw,'triggerDirection':2})['type']=='STOP_MARKET'


def test_bingx_real_fill_payload_keeps_fees_time_and_symbol(demo):
 from app.exchange.bingx.client import BingXClient
 client=BingXClient('fixture','fixture',base_url='https://open-api-vst.bingx.com')
 client._request=Mock(return_value={'code':0,'data':[{'tradeId':'fill','orderId':'entry','symbol':'ADA-USDT','qty':'20','price':'.26','fee':'-.003','realizedPnl':'0','time':123,'side':'BUY'}, {'tradeId':'another','orderId':'manual','symbol':'BTC-USDT'}]})
 fills=client.user_trades('ADAUSDT',0,200)
 assert len(fills)==1 and fills[0]['commission']==.003 and fills[0]['time']==123 and fills[0]['symbol']=='ADAUSDT'


def test_bingx_missing_margin_fails_closed(demo):
 from app.exchange.bingx.client import BingXClient
 client=BingXClient('fixture','fixture',base_url='https://open-api-vst.bingx.com')
 client._request=Mock(return_value={'code':0,'data':{'balance':{'equity':100,'balance':100}}})
 with pytest.raises(KeyError):client.account()

def test_live_account_authorization_and_isolation(live):
 account={'id':live.plan.broker_account_id,'user_id':live.plan.user_id,'environment':'live'};bots=[{'id':live.plan.bot_instance_id,'user_id':live.plan.user_id}]
 with live.db.connect() as c:
  assert authorization(c,account,bots)['reason']=='USER_AUTHORIZATION_REQUIRED'
  with pytest.raises(ValueError,match='ACCESS_DENIED'):set_authorization(c,account_id=account['id'],user_id='intruder',bot_instance_id=bots[0]['id'],enabled=True,now='fixture')
  set_authorization(c,account_id=account['id'],user_id=account['user_id'],bot_instance_id=bots[0]['id'],enabled=True,now='fixture')
  assert authorization(c,account,bots)['enabled']
  assert not authorization(c,{**account,'environment':'demo'},bots)['enabled']
  assert authorization(c,account,bots+bots)['reason']=='ACCOUNT_EXECUTION_OWNER_AMBIGUOUS'
  set_authorization(c,account_id=account['id'],user_id=account['user_id'],bot_instance_id=bots[0]['id'],enabled=False,now='fixture')
  assert not authorization(c,account,bots)['enabled']


def test_account_period_drawdown_counts_losses_and_adjusts_cash_transfers(demo):
 from datetime import datetime,timezone
 from app.trading_intelligence.integration.production_execution import account_periods
 now=demo.now;date=datetime.fromtimestamp(now/1000,timezone.utc).date()
 events=[{'time':now-100,'income':100,'incomeType':'TRANSFER'},{'time':now-50,'income':-10,'incomeType':'REALIZED_PNL'}]
 client=Mock();client.income_history.side_effect=lambda start_time_ms,end_time_ms,limit:[r for r in events if start_time_ms<=r['time']<=end_time_ms]
 periods=account_periods(demo.db,demo.account,client,90,90,date,timezone.utc,now)
 assert periods['weekly']['drawdown_pct']==pytest.approx(10)
 assert periods['monthly']['peak_equity']==100
 # A deposit increases capital; it must neither count as profit nor erase loss.
 events.append({'time':now-1,'income':100,'incomeType':'TRANSFER'})
 periods=account_periods(demo.db,demo.account,client,190,190,date,timezone.utc,now)
 assert periods['weekly']['peak_equity']==200
 assert periods['weekly']['drawdown_pct']==pytest.approx(5)


def test_smoke_rejects_live_before_any_broker_mutation(live,monkeypatch):
 from app.execution.demo_transport_smoke import run
 factory=Mock();monkeypatch.setattr('shared_lib.broker.client_factory.build_client_from_auth',factory)
 with pytest.raises(ValueError,match='REQUIRES_BINANCE_DEMO'):run(live.db,live.plan.broker_account_id)
 factory.assert_not_called()


def test_public_health_reports_stale_account_evidence(demo,monkeypatch):
 from app.trading_intelligence.integration import production_runtime as runtime
 runtime.initialize(demo.db)
 monkeypatch.setattr(runtime.time,'time',lambda:demo.now/1000)
 with demo.db.connect() as c:
  c.execute("UPDATE broker_accounts SET status='disconnected' WHERE id<>?",(demo.account['id'],))
  c.execute("UPDATE cati_residual_tracker SET heartbeat_at=?,status='COLLECTING'",(demo.now,))
 runtime.save(demo.db,demo.account['id'],demo.account['user_id'],demo.now,{
  'status':'SYNCED','risk':{'equity':100},'reconciliation_status':'SYNCED','execution':{'execution_permission':'WAITING_SIGNAL'}})
 healthy=runtime.health_summary(demo.db)
 assert healthy['status']=='ok' and healthy['synced_accounts']==1
 assert 'account_id' not in healthy and 'user_id' not in healthy
 monkeypatch.setattr(runtime.time,'time',lambda:demo.now/1000+121)
 stale=runtime.health_summary(demo.db)
 assert stale['status']=='degraded' and stale['synced_accounts']==0 and stale['market_data_status']=='STALE'

def test_smoke_full_lifecycle_is_separate_and_idempotent(demo,monkeypatch):
 from app.execution import demo_transport_smoke as smoke
 from app.models.unified_trading import SymbolFilters,ProtectionResult
 from shared_lib.core.production import require_broker_mutation_permission
 with demo.db.connect() as c:
  c.execute("UPDATE broker_accounts SET environment='demo' WHERE id=?",(demo.account['id'],))
  c.execute("UPDATE bot_instances SET status='stopped' WHERE broker_account_id=?",(demo.account['id'],))
  c.execute("UPDATE bot_instances SET status='active' WHERE id=?",(demo.plan.bot_instance_id,))
  set_authorization(c,account_id=demo.account['id'],user_id=demo.account['user_id'],bot_instance_id=demo.plan.bot_instance_id,enabled=True,now='fixture')
 client=Mock(broker_environment='demo',base_url='https://demo-fapi.binance.com',_broker_account_id=demo.account['id'])
 monkeypatch.setattr('shared_lib.broker.resolver.resolve_broker_auth',lambda *a:None)
 monkeypatch.setattr('shared_lib.broker.client_factory.build_client_from_auth',lambda auth:client)
 monkeypatch.setattr('app.trading_intelligence.integration.production_execution.account_risk',lambda *a:{'reason':None,'loss_latched':False,'free_capital':100,'remaining_daily_risk':2})
 monkeypatch.setattr('app.trading_intelligence.integration.production_execution.persisted_risk_controls',lambda *a:{'kill_switch':False,'consec_loss_day_paused':False,'consec_loss_cooldown_until_ms':0,'max_weekly_drawdown_pct':0,'max_monthly_drawdown_pct':0})
 monkeypatch.setattr('app.trading_intelligence.governance.promotion.PromotionGovernance.kill_switch_on',lambda *a,**kw:False)
 client.position_risk.return_value=[];client.open_orders.return_value=[];client.get_algo_orders.return_value=[]
 client.get_symbol_filters.return_value=SymbolFilters(step_size='.01',min_qty='.01',min_notional=5);client.last_price.return_value=1
 orders={};quantity=[0];book=[]
 def post(path,params):
  require_broker_mutation_permission('POST',path,environment='demo',broker='binance',base_url=client.base_url,client=client,payload=params)
  cid=params['newClientOrderId'];q=float(params['quantity']);quantity[0]=0 if params['side']=='SELL' else q
  order={'orderId':str(len(orders)+1),'status':'FILLED','executedQty':str(q),'avgPrice':'1','clientOrderId':cid};orders[cid]=order
  return order
 client._signed_post.side_effect=post;client.get_order_by_client_order_id.side_effect=lambda symbol,cid:orders.get(cid)
 client.get_position_info.side_effect=lambda symbol:{'positionAmt':quantity[0]};client.get_position_amt.side_effect=lambda symbol:quantity[0]
 client.user_trades.side_effect=lambda *a,**kw:[{'orderId':'1','id':'fill'},{'orderId':'2','id':'closefill'}]
 def protect(req):
  book.extend([{'algoId':'sl','side':'SELL','closePosition':True},{'algoId':'tp','side':'SELL','closePosition':True}])
  return ProtectionResult(status='success',sl_order_id='sl',tp_order_id='tp')
 client.place_protection.side_effect=protect;client.get_algo_orders.side_effect=lambda *a,**kw:list(book)
 client._signed_delete.side_effect=lambda path,params:book.remove(next(o for o in book if o['algoId']==params['algoId']))
 result=smoke.run(demo.db,demo.account['id'])
 assert result['classification']=='TESTNET_SMOKE' and result['status']=='COMPLETED'
 assert result['final_position']==0 and result['final_protection_orders']==0
 assert client._signed_post.call_count==2
 assert smoke.run(demo.db,demo.account['id'])==result
 assert client._signed_post.call_count==2
