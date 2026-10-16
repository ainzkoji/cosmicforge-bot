"""Both backend factories use one persisted egress budget before constructing clients."""
from types import SimpleNamespace

import pytest

from shared_lib.broker import client_factory
from shared_lib.broker.environment import BrokerEnvironment, resolve_base_url
from shared_lib.broker.http_budget import BudgetedHTTP
from shared_lib.broker.rate_budget import RateBudget
from shared_lib.broker.resolver import BrokerAuth
from shared_lib.broker.venue_contract import AdapterError
from shared_lib.persistence.db import DB


def test_resolved_factory_clients_share_rate_closure_across_accounts_and_db_objects(tmp_path, monkeypatch):
    path = str(tmp_path / "factory-budget.db")
    db1, db2 = DB(path), DB(path)
    def auth(db, account):
        return BrokerAuth(account, "owner", "bybit", BrokerEnvironment.DEMO,
                          resolve_base_url("bybit", BrokerEnvironment.DEMO), "synthetic", "synthetic",
                          1, "test", database=db)
    captured = []
    def build(broker, resolved, *, http):
        assert isinstance(http, BudgetedHTTP)
        captured.append(http)
        return SimpleNamespace(http=http)
    monkeypatch.setattr(client_factory, "_build_client", build)
    first = client_factory.build_client_from_auth(auth(db1, "one"))
    second = client_factory.build_client_from_auth(auth(db2, "two"))
    calls = []
    def refusal(*args, **kwargs):
        calls.append(1)
        return SimpleNamespace(status_code=429, headers={"Retry-After": "120"})
    for http in captured:
        http.transport.close()
        http.transport = SimpleNamespace(request=refusal, close=lambda: None)
    with pytest.raises(AdapterError) as caught:
        first.http.get("https://api-demo.bybit.com/v5/account/wallet-balance")
    assert caught.value.retry_after_s == 120
    with pytest.raises(AdapterError, match="LOCAL_HTTP_BUDGET_REFUSED"):
        second.http.get("https://api-demo.bybit.com/v5/account/wallet-balance")
    assert len(calls) == 1
    for http in captured:
        http.close()


@pytest.mark.parametrize("status,body", [(403, {}), (200, {"retCode": 10006})])
def test_bybit_ip_ban_and_uid_refusal_have_distinct_persistent_scopes(tmp_path, status, body):
    from shared_lib.persistence.cati_platform_schema import ensure_cati_platform_schema
    db = DB(str(tmp_path / "scopes.db"))
    ensure_cati_platform_schema(db)
    calls = []
    def respond(*args, **kwargs):
        calls.append(1)
        return SimpleNamespace(status_code=status if len(calls) == 1 else 200,
                               headers={"X-Bapi-Limit-Reset-Timestamp": "105000"},
                               json=lambda: body if len(calls) == 1 else {})
    wire = SimpleNamespace(request=respond)
    first = BudgetedHTTP(RateBudget(db), "bybit", account_ref="one", server_address="same-ip", transport=wire, clock=lambda: 100000)
    other = BudgetedHTTP(RateBudget(db), "bybit", account_ref="two", server_address="same-ip", transport=wire, clock=lambda: 100000)
    with pytest.raises(AdapterError) as caught:
        first.get("https://example.invalid/read")
    assert caught.value.retry_after_s == (600 if status == 403 else 5)
    with pytest.raises(AdapterError):
        first.get("https://example.invalid/read")
    if status == 403:
        with pytest.raises(AdapterError):
            other.get("https://example.invalid/read")
        assert len(calls) == 1
    else:
        other.get("https://example.invalid/read")
        assert len(calls) == 2
