"""Exercise the exact credential and validation endpoints used by the UI."""
from fastapi import FastAPI
from fastapi.testclient import TestClient
import pytest
from test_broker_reconnect import _make_db, _insert_account
from shared_lib.broker.resolver import resolve_broker_auth


@pytest.fixture
def flow(monkeypatch):
    from app.core import broker_service as service
    from app.api import brokers
    db = _make_db()
    with db.connect() as c:
        c.execute("ALTER TABLE broker_accounts ADD COLUMN masked_key TEXT")
        c.execute("ALTER TABLE broker_accounts ADD COLUMN last_error_code TEXT")
        _insert_account(c, "demo", "owner", environment="live", status="draft", active_version=None)
        _insert_account(c, "other", "another", environment="demo", status="draft", active_version=None)
        c.execute("CREATE TABLE bot_instances(id TEXT PRIMARY KEY,broker_account_id TEXT,user_id TEXT)")
        c.execute("INSERT INTO bot_instances VALUES('bot','demo','owner')")
    monkeypatch.setattr(service, "get_db", lambda: db)
    monkeypatch.setattr(service, "_test_broker_connection", lambda *a: {"success": True, "capabilities": ["read", "trade"]})
    monkeypatch.setattr(service, "_evaluate_key_permissions", lambda *a: {"decision": "APPROVED", "evidence": {"can_trade": True}})
    app = FastAPI()
    app.include_router(brokers.router)
    app.dependency_overrides[brokers.get_current_user_id] = lambda: "owner"
    return TestClient(app), db


def test_initial_connection_and_reconnect_keep_identity_and_environment(flow):
    client, db = flow
    for version in (1, 2):
        response = client.post("/demo/credentials", json={"environment": "demo", "credentials": {"api_key": "fixture-key", "api_secret": "fixture-secret"}})
        assert response.status_code == 200 and response.json()["version"] == version
        response = client.post("/demo/validate")
        assert response.status_code == 200 and response.json()["success"]
        auth = resolve_broker_auth("demo", "owner", db)
        assert auth.base_url == "https://demo-fapi.binance.com"
        with db.connect() as c:
            assert c.execute("SELECT active_credential_version FROM broker_accounts WHERE id='demo'").fetchone()[0] == version
            assert c.execute("SELECT broker_account_id FROM bot_instances WHERE id='bot'").fetchone()[0] == "demo"
            assert c.execute("SELECT COUNT(*) FROM broker_credentials_v2 WHERE account_id='demo' AND status='active'").fetchone()[0] == 1
            assert c.execute("SELECT last_error_code FROM broker_accounts WHERE id='demo'").fetchone()[0] is None
            c.execute("UPDATE broker_accounts SET last_error_code='CREDENTIAL_RECONNECT_REQUIRED' WHERE id='demo'")
    assert client.post("/demo/credentials", json={"environment": "live", "credentials": {}}).status_code == 400


def test_cross_user_and_malicious_endpoint_cannot_mutate_account(flow):
    client, db = flow
    assert client.post("/other/credentials", json={"credentials": {"api_key": "x"}}).status_code == 404
    assert not client.post("/other/validate").json().get("success")
    assert client.post("/demo/credentials", json={"environment": "demo", "credentials": {"base_url": "https://fapi.binance.com"}}).status_code == 400
    with db.connect() as c:
        assert c.execute("SELECT environment FROM broker_accounts WHERE id='demo'").fetchone()[0] == "live"
        assert c.execute("SELECT COUNT(*) FROM broker_credentials_v2").fetchone()[0] == 0


def test_missing_active_version_never_falls_back_to_another_credential(flow):
    from shared_lib.broker.errors import BrokerResolverError
    from test_broker_reconnect import _make_blob
    client, db = flow
    with db.connect() as c:
        c.execute("UPDATE broker_accounts SET status='connected',active_credential_version=99 WHERE id='demo'")
        c.execute("INSERT INTO broker_credentials VALUES('demo',?,'fixture','fixture')", (_make_blob(),))
    with pytest.raises(BrokerResolverError) as exc:
        resolve_broker_auth("demo", "owner", db)
    assert exc.value.reason_code == BrokerResolverError.REASON_NO_CREDENTIALS
