"""MT4/MT5 bridge test: user-backend -> bot-backend ``POST /api/v1/brokers/test-connection``.

The bot-backend endpoint requires an authenticated user and the body
``{"broker_id", "environment": "paper"|"live", "credentials": {...}}`` and
answers ``{"ok", "error", "details"}``. The caller used to send a flat body
with no Authorization header, so the test could never pass.

No network: the outbound HTTP client is replaced by a fake.
"""
from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "backends" / "shared"))
sys.path.insert(0, str(ROOT / "backends" / "user-backend"))

from app.core import broker_service  # noqa: E402
from app.core.security import decode_token  # noqa: E402

BRIDGE_TOKEN = "bridge-secret-token-123"
CREDENTIALS = {"bridge_url": "https://bridge.example.test:8443", "bridge_token": BRIDGE_TOKEN,
               "api_key": None, "api_secret": None, "base_url": None}


def _http(status_code=200, payload=None):
    """A stand-in for ``httpx.Client`` (used as a context manager)."""
    response = MagicMock()
    response.status_code = status_code
    response.json.return_value = {"ok": True, "details": {"balance": 100.0}} if payload is None else payload
    response.text = str(payload)
    client = MagicMock()
    client.post.return_value = response
    factory = MagicMock()
    factory.return_value.__enter__.return_value = client
    return factory, client


def _run(factory, *, broker="mt5", environment="live", credentials=None, **caller):
    with patch("httpx.Client", factory):
        if caller:
            with broker_service.validation_caller(**caller):
                return broker_service._test_broker_connection(broker, credentials or CREDENTIALS, environment)
        return broker_service._test_broker_connection(broker, credentials or CREDENTIALS, environment)


def test_sends_the_bot_backend_contract_and_the_callers_token(monkeypatch):
    monkeypatch.setenv("BOT_BACKEND_URL", "http://bot.test:9000/")
    factory, client = _http()

    result = _run(factory, user_id="user-1", authorization="Bearer callers-own-token")

    assert result == {"success": True, "details": {"balance": 100.0}}
    (url,), kwargs = client.post.call_args
    assert url == "http://bot.test:9000/api/v1/brokers/test-connection"
    assert kwargs["headers"] == {"Authorization": "Bearer callers-own-token"}
    assert kwargs["json"] == {
        "broker_id": "mt5",
        "environment": "live",
        "credentials": {"bridge_url": CREDENTIALS["bridge_url"], "bridge_token": BRIDGE_TOKEN},
    }


@pytest.mark.parametrize("environment,expected", [("live", "live"), ("demo", "paper")])
def test_environment_is_mapped_to_the_bot_backend_vocabulary(environment, expected):
    factory, client = _http()
    _run(factory, environment=environment, authorization="Bearer t")
    assert client.post.call_args.kwargs["json"]["environment"] == expected


def test_tls_mode_is_passed_through_when_set():
    factory, client = _http()
    _run(factory, credentials={**CREDENTIALS, "tls_mode": "insecure"}, authorization="Bearer t")
    assert client.post.call_args.kwargs["json"]["credentials"]["tls_mode"] == "insecure"


def test_without_the_callers_header_a_short_lived_token_for_that_user_is_used():
    factory, client = _http()

    result = _run(factory, user_id="user-42")

    assert result["success"] is True
    scheme, _, token = client.post.call_args.kwargs["headers"]["Authorization"].partition(" ")
    assert scheme == "Bearer"
    claims = decode_token(token)
    assert claims["sub"] == "user-42" and claims["type"] == "access" and claims["role"] == "user"
    assert claims["exp"] - claims["iat"] <= 60


def test_no_caller_is_an_explicit_failure_and_the_bridge_is_not_contacted():
    factory, client = _http()

    result = _run(factory)

    assert result["success"] is False
    assert result["reason_code"] == broker_service.BRIDGE_VALIDATION_UNAUTHENTICATED
    factory.assert_not_called()
    client.post.assert_not_called()


def test_a_failed_bridge_test_is_never_reported_as_connected():
    factory, _ = _http(200, {"ok": False, "error": "Bridge connection failed: timeout", "details": None})
    result = _run(factory, authorization="Bearer t")
    assert result == {"success": False, "error": "Bridge connection failed: timeout"}


@pytest.mark.parametrize("payload", [{"message": "Mock success"}, {"ok": "yes"}, ["ok"], None])
def test_an_unexpected_answer_is_a_failure(payload):
    factory, client = _http(200, {"placeholder": True})
    client.post.return_value.json.return_value = payload
    result = _run(factory, authorization="Bearer t")
    assert result["success"] is False


def test_rejection_never_echoes_the_bridge_token():
    echo = {"detail": [{"loc": ["body", "credentials"], "msg": "bad", "input": {"bridge_token": BRIDGE_TOKEN}}]}
    factory, client = _http(422, echo)
    client.post.return_value.text = str(echo)

    result = _run(factory, authorization="Bearer t")

    assert result["success"] is False
    assert "HTTP 422" in result["error"]
    assert BRIDGE_TOKEN not in result["error"]


def test_upstream_401_is_a_failure_with_the_reason():
    factory, _ = _http(401, {"detail": "Could not validate credentials"})
    result = _run(factory, authorization="Bearer stale")
    assert result["success"] is False
    assert "HTTP 401" in result["error"] and "Could not validate credentials" in result["error"]


def test_unreachable_bot_service_is_a_failure():
    factory, client = _http()
    client.post.side_effect = ConnectionError("refused: " + BRIDGE_TOKEN)
    result = _run(factory, authorization="Bearer t")
    assert result["success"] is False
    assert BRIDGE_TOKEN not in result["error"]


def test_missing_bridge_credentials_fail_before_any_call():
    factory, client = _http()
    result = _run(factory, credentials={"bridge_url": "", "bridge_token": ""}, authorization="Bearer t")
    assert result["success"] is False
    client.post.assert_not_called()


def test_validate_broker_account_supplies_the_user_for_the_bridge_test():
    """The account validation path knows the user and must make the test runnable."""
    seen = {}

    def fake_test(broker_id, credentials, environment):
        seen["caller"] = broker_service._VALIDATION_CALLER.get()
        return {"success": False, "error": "stop here"}

    from types import SimpleNamespace

    auth = SimpleNamespace(extra={"bridge_url": "https://b.example.test", "bridge_token": "t"},
                           api_key=None, api_secret=None, base_url=None,
                           environment=SimpleNamespace(value="live"), credential_version=1)
    with patch("app.core.broker_service.get_db") as mock_get_db, \
         patch("shared_lib.broker.resolver.resolve_broker_auth", return_value=auth), \
         patch("app.core.broker_service._test_broker_connection", side_effect=fake_test), \
         patch("app.core.broker_service._log_audit_event"):
        conn = MagicMock()
        mock_get_db.return_value.connect.return_value.__enter__.return_value = conn
        conn.execute.return_value.fetchone.return_value = {"broker_id": "mt4"}
        result = broker_service.validate_broker_account("user-7", "acc-7")

    assert result["success"] is False
    assert seen["caller"] == {"user_id": "user-7", "authorization": None}
    assert broker_service._VALIDATION_CALLER.get() is None       # reset afterwards
