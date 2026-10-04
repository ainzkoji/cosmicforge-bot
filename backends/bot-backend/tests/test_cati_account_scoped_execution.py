"""Canonical account discovery, credentials, endpoints and transport gates."""
from types import SimpleNamespace
from unittest.mock import Mock
import sqlite3
import time

import pytest
from app.core import config
from app.core.config import Settings
from app.trading_intelligence.integration import production_runtime as runtime
from shared_lib.broker.environment import normalize_environment, resolve_base_url
from shared_lib.broker.resolver import BrokerAuth
from shared_lib.broker import client_factory
from shared_lib.core.production import LiveOrderSubmissionDisabled, DemoOrderSubmissionDisabled


@pytest.fixture
def operational(monkeypatch):
    settings = Settings(_env_file=None, APP_ENV="PRODUCTION", DATABASE_ROLE="production",
        ENVIRONMENT_NAME="production", EXECUTION_MODE="live", DEMO_ORDER_SUBMISSION_ENABLED=True,
        LIVE_ORDER_SUBMISSION_ENABLED=False)
    monkeypatch.setattr(config,"settings",settings)
    monkeypatch.setattr(runtime,"settings",settings)
    return settings


@pytest.fixture
def accounts(tmp_path):
    class Store:
        path = str(tmp_path/"accounts.db")
        def connect(self):
            c=sqlite3.connect(self.path)
            c.row_factory=sqlite3.Row
            return c
    db=Store()
    with db.connect() as c:
        c.executescript("CREATE TABLE broker_accounts(id,user_id,broker_id,environment,status); CREATE TABLE bot_instances(id,broker_account_id,status);")
        c.executemany("INSERT INTO broker_accounts VALUES(?,?,?,?,?)",[
            ("a-demo","alice","binance","testnet","connected"),
            ("a-demo2","alice","binance","sandbox","active"),
            ("a-live","alice","binance","live","connected"),
            ("b-demo","bob","bybit","demo","connected"),
            ("c-demo","carol","bingx","demo","active"),
            ("inactive","alice","binance","demo","disconnected")])
    return db


def auth_for(account, broker="binance", environment="demo", user="alice"):
    env=normalize_environment(environment)
    return BrokerAuth(account_id=account,user_id=user,broker_type=broker,environment=env,
        base_url=resolve_base_url(broker,env),api_key="isolated-key-"+account,api_secret="isolated-secret",
        credential_version=3,key_fingerprint="test")


@pytest.mark.parametrize("broker",["binance","bybit","bingx"])
@pytest.mark.parametrize("environment",["demo","live"])
def test_resolved_factory_uses_canonical_endpoint_and_isolates_accounts(operational,monkeypatch,broker,environment):
    builder=Mock(side_effect=lambda auth:SimpleNamespace())
    monkeypatch.setattr(client_factory,"_build_"+broker,builder)
    auth_a=auth_for("a",broker,environment)
    auth_b=auth_for("b",broker,environment,"bob")
    a=client_factory.build_client_from_auth(auth_a)
    b=client_factory.build_client_from_auth(auth_b)
    assert a is not b and a._broker_account_id == "a" and b._broker_account_id == "b"
    assert a._broker_user_id == "alice" and b._broker_user_id == "bob"
    assert a.broker_environment == normalize_environment(environment)
    assert builder.call_args_list[0].args[0].base_url == resolve_base_url(broker,normalize_environment(environment))
    assert builder.call_args_list[0].args[0].api_key != builder.call_args_list[1].args[0].api_key
    bad=auth_for("bad",broker,environment)
    from dataclasses import replace
    bad=replace(bad,base_url=resolve_base_url(broker,normalize_environment("live" if environment == "demo" else "demo")))
    with pytest.raises(ValueError,match="BROKER_ENVIRONMENT_MISMATCH"):
        client_factory.build_client_from_auth(bad)
    assert builder.call_count == 2


def test_production_sync_discovers_demo_and_live_and_keeps_users_separate(operational,accounts,monkeypatch):
    from app.activation import account_status
    from app.trading_intelligence.integration import production_execution
    discovered=runtime.execution_accounts(accounts)
    assert len(discovered)==5 and discovered[0]["environment"] == "DEMO"
    clients={}
    def resolve(account,user,db):
        row=next(a for a in discovered if a["id"]==account and a["user_id"]==user)
        return auth_for(account,row["broker_id"],row["environment"],user)
    def factory(auth):
        c=Mock(broker_environment=auth.environment)
        c.get_balance.return_value={"equity":1000+len(clients)}
        c.position_risk.return_value=[]
        c.open_orders.return_value=[]
        clients[auth.account_id]=c
        return c
    monkeypatch.setattr(runtime,"resolve_broker_auth",resolve)
    monkeypatch.setattr(runtime,"build_client_from_auth",factory)
    # sync_account's injectable default is already bound; pass factory through explicitly.
    original=runtime.sync_account
    monkeypatch.setattr(runtime,"sync_account",lambda db,a,**kw:original(db,a,factory=factory,**kw))
    monkeypatch.setattr(runtime,"owner_current",lambda db:True)
    monkeypatch.setattr(account_status,"refresh_if_stale",lambda *a,**kw:{"status":"SYNCED"})
    seen=[]
    def process(db,account,client,snapshot):
        seen.append((account["user_id"],account["id"],client,snapshot["environment"]))
        return {"execution_permission":"WAITING_SIGNAL","reason":"NO_RESIDUAL_DECISION"}
    monkeypatch.setattr(production_execution,"process_account",process)
    runtime.sync(accounts)
    assert len(seen)==5 and len({id(x[2]) for x in seen})==5
    state=runtime.status(accounts,user_id="alice")
    assert [a["account_id"] for a in state["accounts"]]==["a-demo","a-demo2","a-live"]
    assert [a["environment"] for a in state["accounts"]]==["DEMO","DEMO","LIVE"]
    assert state["accounts"][-1]["execution_permission"]=="BLOCKED_LIVE_ORDER_GATE"
    assert state["accounts"][0]["order_submission_gate"]["enabled"] is True
    assert state["broker_execution_scope"]=="ACCOUNT_SCOPED" and state["cati_mode"]=="LIVE"
    for client in clients.values():
        client.place_order.assert_not_called()


@pytest.mark.parametrize("broker",["binance","bybit","bingx"])
def test_real_client_transport_demo_gate_and_live_block_before_network(operational,monkeypatch,broker):
    response=Mock(status_code=200,content=b"{}",headers={},text="{}")
    response.json.return_value={"symbols":[],"code":0,"retCode":0,"data":{},"result":{}}
    monkeypatch.setattr("requests.sessions.Session.get",Mock(return_value=response))
    post=Mock(return_value=response)
    monkeypatch.setattr("requests.post",post)
    monkeypatch.setattr("requests.sessions.Session.post",post)
    demo=client_factory.build_client_from_auth(auth_for("demo",broker))
    live=client_factory.build_client_from_auth(auth_for("live",broker,"live"))
    method={"binance":"_signed_request","bybit":"_request_v5","bingx":"_request"}[broker]
    path={"binance":"/fapi/v1/leverage","bybit":"/v5/position/set-leverage","bingx":"/openApi/swap/v2/trade/leverage"}[broker]
    getattr(demo,method)("POST",path,{})
    assert post.call_count==1
    with pytest.raises(LiveOrderSubmissionDisabled):
        getattr(live,method)("POST",path,{})
    assert post.call_count==1
    monkeypatch.setattr(config,"settings",operational.model_copy(update={"DEMO_ORDER_SUBMISSION_ENABLED":False}))
    with pytest.raises(DemoOrderSubmissionDisabled):
        getattr(demo,method)("POST",path,{})
    assert post.call_count==1


@pytest.mark.parametrize("broker,path",[("binance","/fapi/v1/order"),("bybit","/v5/order/create"),("bingx","/openApi/swap/v2/trade/order")])
def test_enabled_demo_gate_cannot_authorize_contextless_entry(operational,broker,path):
    from shared_lib.core.production import require_broker_mutation_permission
    with pytest.raises(ValueError,match="CATI_ENTRY_AUTHORITY_REQUIRED"):
        require_broker_mutation_permission("POST",path,environment="DEMO",broker=broker,
            base_url=resolve_base_url(broker,normalize_environment("demo")),payload={"symbol":"BTCUSDT","side":"BUY"})
