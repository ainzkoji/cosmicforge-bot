"""Account authority, reservation cleanup and immutable evaluation evidence."""
import json
import sqlite3
from dataclasses import replace
import pytest
from test_production_execution import live
from test_production_demo_execution import demo
from test_production_research_separation import fresh, process
from app.trading_intelligence.integration import production_execution as production
from app.trading_intelligence.governance.account_authority import AccountExecutionAuthority
from app.trading_intelligence.governance.promotion import StaticAuthority
from app.trading_intelligence.execution.config import CATIExecutionConfig


def test_m0_demo_uses_actual_account_authority(fresh):
    boundary=fresh.boundary_for()
    boundary.authority=AccountExecutionAuthority(fresh.db,(fresh.account['user_id'],fresh.account['id']))
    assert boundary.authority.gov.current_phase()=='M0'
    result=process(fresh)
    assert result.get('boundary',{}).get('status')=='EXECUTED',result
    assert fresh.client.place_order.call_count==1
    assert result['reservation_status']=='CONSUMED'


@pytest.mark.parametrize('reason',['GOVERNANCE_REFUSED','CATI_NEW_ENTRY_KILL_SWITCH'])
def test_definitive_authority_refusal_releases_reservation(demo,reason):
    result=demo.run(demo.demo_boundary(authority=StaticAuthority(False,reason)),atr=1.)
    assert result.reservation_status=='RELEASED'
    demo.client.place_order.assert_not_called()


@pytest.mark.parametrize('kind',['disabled','environment','integrity','preflight','risk'])
def test_precreate_rejections_release_owned_reservation(demo,kind):
    boundary=demo.demo_boundary()
    plan=demo.plan
    args={}
    if kind=='disabled': boundary.config=CATIExecutionConfig(False,False,('DEMO',))
    elif kind=='environment': boundary.config=CATIExecutionConfig(True,False,('LIVE',))
    elif kind=='integrity': plan=replace(plan,trade_plan_hash='forged')
    elif kind=='preflight': boundary.preflight.max_age=-1
    else: args['adaptive_daily_risk']={'daily_risk_state':'HARD_STOP','decision_reason':'DAILY_HARD_LOSS_CAP_REACHED'}
    result=demo.run(boundary,plan=plan,atr=1.,**args)
    assert result.reservation_status=='RELEASED',result
    demo.client.place_order.assert_not_called()


def test_exception_after_reserve_releases_and_preserves_evidence(fresh):
    fresh.client.last_price.side_effect=ValueError('MARKET_PRICE_UNAVAILABLE')
    with pytest.raises(ValueError,match='MARKET_PRICE_UNAVAILABLE'):
        process(fresh)
    with fresh.db.connect() as c:
        assert c.execute("SELECT status FROM cati_portfolio_reservations WHERE mode='PRODUCTION'").fetchone()[0]=='RELEASED'
        evidence=json.loads(c.execute('SELECT document FROM cati_execution_evaluations ORDER BY rowid DESC LIMIT 1').fetchone()[0])
    assert evidence['reason']=='MARKET_PRICE_UNAVAILABLE'
    assert evidence['reservation_status']=='RELEASED'
    fresh.client.place_order.assert_not_called()


def test_no_score_waits_and_history_cannot_be_overwritten(fresh,monkeypatch):
    row=dict(fresh.row)
    state=json.loads(row['risk_state_json']);state['reason']='NO_SCORE_AT_LEAST_2'
    row['risk_state_json']=json.dumps(state)
    monkeypatch.setattr(production,'latest_decision',lambda db:row)
    first=process(fresh)
    assert first['execution_permission']=='WAITING_SIGNAL'
    assert first['reason']=='NO_SCORE_AT_LEAST_2'
    second=process(fresh,positions=[{'symbol':'ETHUSDT','positionAmt':'1'}])
    assert second['execution_permission']=='BLOCKED_RISK'
    with fresh.db.connect() as c:
        rows=c.execute('SELECT evaluation_id,reason FROM cati_execution_evaluations ORDER BY rowid').fetchall()
        assert [r['reason'] for r in rows]==['NO_SCORE_AT_LEAST_2','ACCOUNT_WIDE_POSITION_OR_ORDER_ACTIVE']
        with pytest.raises(sqlite3.IntegrityError,match='append-only'):
            c.execute("UPDATE cati_execution_evaluations SET reason='erased'")
        with pytest.raises(sqlite3.IntegrityError,match='append-only'):
            c.execute('DELETE FROM cati_execution_evaluations')


def test_streak_reconstruction_is_quiet_and_does_not_increment_twice(fresh,caplog):
    production.initialize(fresh.db)
    fresh.client.income_history.return_value=[{'incomeType':'REALIZED_PNL','income':'-1','time':fresh.now,'tranId':123}]
    first=production.account_risk(fresh.db,fresh.account,fresh.client,[],[],[],fresh.now)
    second=production.account_risk(fresh.db,fresh.account,fresh.client,[],[],[],fresh.now)
    assert first['consecutive_losses']==second['consecutive_losses']==1
    assert 'CONSECUTIVE_LOSS_RECORDED' not in caplog.text


def test_account_demo_authority_never_promotes_live(fresh):
    authority=AccountExecutionAuthority(fresh.db,(fresh.account['user_id'],fresh.account['id']))
    live_plan=replace(fresh.plan,environment='LIVE')
    assert authority.authorize_entry(live_plan)==(False,'BROKER_ENVIRONMENT_MISMATCH')
    with fresh.db.connect() as c:
        c.execute("UPDATE broker_accounts SET environment='live' WHERE id=?",(fresh.account['id'],))
        from shared_lib.broker.auto_trading import set_authorization
        set_authorization(c,account_id=fresh.account['id'],user_id=fresh.account['user_id'],bot_instance_id=fresh.plan.bot_instance_id,enabled=True,now=str(fresh.now))
    assert authority.authorize_entry(live_plan)==(False,'LIVE_ORDER_SUBMISSION_DISABLED')


def test_signal_expiring_during_hard_risk_never_reaches_create(fresh,monkeypatch):
    import time
    clock=[time.monotonic()]
    monkeypatch.setattr('app.trading_intelligence.execution.boundary.time.monotonic',lambda:clock[0])
    boundary=fresh.boundary_for()
    original=boundary.orchestrator.process_trade_plan
    def delayed(*args,**kwargs):
        result=original(*args,**kwargs)
        clock[0]+=901
        return result
    monkeypatch.setattr(boundary.orchestrator,'process_trade_plan',delayed)
    result=process(fresh)
    assert result['reason']=='PROSPECTIVE_ENTRY_WINDOW_EXPIRED'
    assert result['reservation_status']=='RELEASED'
    fresh.client.place_order.assert_not_called()


def test_fees_and_funding_are_visible_and_counted_exactly_once(fresh):
    production.initialize(fresh.db)
    fresh.client.income_history.return_value=[
        {'incomeType':kind,'income':str(value),'time':fresh.now,'tranId':i}
        for i,(kind,value) in enumerate([('REALIZED_PNL',-1),('COMMISSION',-.1),('FUNDING_FEE',-.2)])]
    risk=production.account_risk(fresh.db,fresh.account,fresh.client,[],[],[],fresh.now)
    assert risk['realized_pnl']==pytest.approx(-1.3)
    assert risk['daily_loss_usage']==pytest.approx(1.3)
    assert risk['fees']==pytest.approx(.1) and risk['funding']==pytest.approx(-.2)
    assert risk['adaptive_daily_risk']['risk_budget_consumed_usdt']==pytest.approx(1.3)
    assert risk['adaptive_daily_risk']['fees_today']==pytest.approx(.1)


def test_lagging_income_ledger_cannot_hide_a_realized_wallet_loss(fresh):
    # Binance DEMO: the close debits the wallet immediately while the income
    # ledger is still empty. The daily cap must read the wallet, not the lag.
    production.initialize(fresh.db)
    fresh.client.income_history.return_value=[]
    def wallet(value):
        fresh.client.account.return_value=dict(totalWalletBalance=value,totalMarginBalance=value,availableBalance=value,
                                               totalInitialMargin=0,totalUnrealizedProfit=0)
    wallet(426.38)
    assert production.account_risk(fresh.db,fresh.account,fresh.client,[],[],[],fresh.now)['daily_loss_usage']==0
    wallet(418.61)
    risk=production.account_risk(fresh.db,fresh.account,fresh.client,[],[],[],fresh.now)
    assert risk['daily_loss_usage']==pytest.approx(7.77) and risk['realized_pnl']==pytest.approx(-7.77)
    assert risk['adaptive_daily_risk']['risk_budget_consumed_usdt']==pytest.approx(7.77)
    assert not risk['loss_latched'] and risk['reason'] is None
    wallet(415.)
    risk=production.account_risk(fresh.db,fresh.account,fresh.client,[],[],[],fresh.now)
    assert risk['loss_latched'] and risk['reason']=='DAILY_HARD_LOSS_CAP_REACHED'
