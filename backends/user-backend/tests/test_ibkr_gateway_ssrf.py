"""Section J -- a customer-supplied IBKR gateway URL is an outbound request the server makes for them.

In production it may only reach a public https destination (no loopback, private, link-local / metadata or
operator allow-listed gateway), TLS is verified, and a redirect is never followed. Outside production a local
self-signed development gateway keeps working.
"""
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from app.core import broker_service, config
from app.exchange import ibkr_client


def _settings(monkeypatch, *, production, verify="auto", allowed="127.0.0.1:5000"):
    fake = SimpleNamespace(production=production, BROKER_GATEWAY_VERIFY_TLS=verify, BROKER_GATEWAY_ALLOWED_HOSTS=allowed)
    monkeypatch.setattr(config, "settings", fake)
    return fake


@pytest.mark.parametrize("url", [
    "https://localhost:5000", "https://127.0.0.1:5000", "https://169.254.169.254/latest", "https://10.0.0.5:5000",
    "http://203.0.113.10:5000", "https://user:pw@203.0.113.10", "https://203.0.113.10:5000/?x=", "file:///etc/passwd",
])
def test_production_refuses_internal_cleartext_and_malformed_gateways_before_any_request(monkeypatch, url):
    _settings(monkeypatch, production=True)
    post = Mock()
    monkeypatch.setattr(ibkr_client.requests, "post", post)
    out = broker_service._test_broker_connection("ibkr", {"gateway_url": url}, "paper")
    assert out["success"] is False and out["error"].startswith("Destination not allowed")
    post.assert_not_called()


def test_the_operator_allow_list_is_never_offered_to_a_customer(monkeypatch):
    _settings(monkeypatch, production=True, allowed="127.0.0.1:5000")
    assert isinstance(broker_service._checked_gateway("https://127.0.0.1:5000"), dict)


def test_a_public_gateway_is_requested_with_tls_verified_and_no_redirect(monkeypatch):
    _settings(monkeypatch, production=True)
    monkeypatch.setattr("shared_lib.core.security.url_guard._system_resolver", lambda host, port: ["93.184.215.14"])
    response = Mock(status_code=200)
    response.json.return_value = {"authenticated": True, "connected": True}
    post = Mock(return_value=response)
    monkeypatch.setattr(ibkr_client.requests, "post", post)
    out = broker_service._test_broker_connection("ibkr", {"gateway_url": "https://gw.example.com:5000"}, "paper")
    assert out["success"] is True
    kwargs = post.call_args.kwargs
    assert kwargs["verify"] is True and kwargs["allow_redirects"] is False


@pytest.mark.parametrize("production,setting,expected", [
    (True, "auto", True), (False, "auto", False), (False, "true", True), (True, "false", False), (True, "junk", True),
])
def test_tls_verification_follows_the_bot_backend_rule(monkeypatch, production, setting, expected):
    _settings(monkeypatch, production=production, verify=setting)
    assert broker_service.gateway_verify_tls() is expected


def test_a_local_development_gateway_still_works_outside_production(monkeypatch):
    _settings(monkeypatch, production=False)
    assert broker_service._checked_gateway("https://localhost:5000") == "https://localhost:5000"
