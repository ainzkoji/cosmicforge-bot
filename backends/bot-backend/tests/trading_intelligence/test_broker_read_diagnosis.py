"""A failed account read is recorded with a stable, sanitized reason (never a bare class name, never a secret)."""
import json

import pytest
import requests

from shared_lib.broker import venue_contract as C
from shared_lib.broker.errors import BrokerResolverError
from app.trading_intelligence.integration import broker_read_diagnosis as D
from app.trading_intelligence.integration import production_runtime as PR
from app.trading_intelligence.integration import reason_registry as R

SIGNED = "https://demo-fapi.binance.com/fapi/v2/positionRisk?timestamp=1&signature=" + "ab" * 32


@pytest.mark.parametrize("exc,reason", [
    (C.AdapterError(C.RATE_LIMITED, "LOCAL_HTTP_BUDGET_REFUSED", venue="binance", retry_after_s=12.0),
     "BROKER_RATE_LIMITED_LOCAL"),
    (C.AdapterError(C.RATE_LIMITED, "HTTP_BUDGET_VENUE_REFUSAL", venue="binance", http_status=429),
     "BROKER_RATE_LIMITED"),
    (C.AdapterError(C.BANNED, "HTTP_BUDGET_VENUE_REFUSAL", venue="binance", http_status=418), "BROKER_IP_BANNED"),
    (C.AdapterError(C.AUTH_FAILED, venue="binance"), "BROKER_CREDENTIALS_REJECTED"),
    (C.AdapterError(C.TIMESTAMP_OUT_OF_SYNC, venue="binance"), "BROKER_TIMESTAMP_REJECTED"),
    (C.AdapterError(C.UNSUPPORTED, "HTTP_ENDPOINT_WEIGHT_NOT_REGISTERED", venue="binance"),
     "BROKER_ENDPOINT_UNSUPPORTED"),
    (RuntimeError('Binance HTTP 401: {"code":-2015,"msg":"Invalid API-key, IP, or permissions for action."}'),
     "BROKER_CREDENTIALS_REJECTED"),
    (RuntimeError('Binance HTTP 400: {"code":-1022,"msg":"Signature for this request is not valid."}'),
     "BROKER_SIGNATURE_REJECTED"),
    (RuntimeError('Binance HTTP 400: {"code":-1021,"msg":"Timestamp outside recvWindow."}'),
     "BROKER_TIMESTAMP_REJECTED"),
    (RuntimeError("Binance HTTP 503: upstream"), "BROKER_VENUE_ERROR"),
    (requests.ConnectionError(SIGNED), "BROKER_NETWORK_UNAVAILABLE"),
    (BrokerResolverError(BrokerResolverError.REASON_DECRYPT_FAILED, "x"), "BROKER_CREDENTIAL_DECRYPT_FAILED"),
    (BrokerResolverError(BrokerResolverError.REASON_NO_CREDENTIALS, "x"), "BROKER_CREDENTIALS_MISSING"),
    (BrokerResolverError(BrokerResolverError.REASON_REVOKED, "x"), "BROKER_CONNECTION_INVALID"),
    (ValueError("BROKER_READ_SHAPE_INVALID"), "BROKER_READ_SHAPE_INVALID"),
])
def test_each_failure_class_maps_to_a_registered_reason(exc, reason):
    out = D.classify(exc)
    assert out["reason"] == reason
    assert R.known(reason)
    assert R.describe(reason)["severity"] in (R.ATTENTION, R.BLOCKED)


def test_credential_rejection_tells_the_customer_exactly_how_to_reconnect():
    entry = R.describe("BROKER_CREDENTIALS_REJECTED")
    assert entry["severity"] == R.BLOCKED
    assert "Reconnect the exchange" in entry["action"] and "withdrawal" in entry["action"]


@pytest.mark.parametrize("exc", [
    RuntimeError("Binance HTTP 400: " + SIGNED),
    requests.ConnectionError(SIGNED),
    C.AdapterError(C.UNMAPPED_VENUE_ERROR, "free text " + SIGNED, venue="binance"),
    RuntimeError("Binance request failed after retries: GET " + SIGNED),
    KeyError(SIGNED),
])
def test_diagnostic_never_carries_request_text(exc):
    text = json.dumps(D.classify(exc))
    assert "signature" not in text and "abab" not in text and "http" not in text.lower().replace("http_status", "")


def test_a_local_budget_refusal_does_not_rebuild_the_client_but_a_venue_error_does():
    local = C.AdapterError(C.RATE_LIMITED, "LOCAL_HTTP_BUDGET_REFUSED", venue="binance")
    assert D.is_local_budget_refusal(local)
    assert PR.client_doubt(local) is False
    assert PR.client_doubt(C.AdapterError(C.RATE_LIMITED, "HTTP_BUDGET_VENUE_REFUSAL", venue="binance")) is True
    assert PR.client_doubt(RuntimeError("Binance HTTP 401: {}")) is True


def test_read_failure_is_saved_with_the_classified_reason(monkeypatch):
    saved = []
    account = {"id": "brk_t", "user_id": "u", "broker_id": "binance", "environment": "DEMO", "status": "connected"}

    def boom(*_a, **_k):
        raise C.AdapterError(C.RATE_LIMITED, "LOCAL_HTTP_BUDGET_REFUSED", venue="binance", retry_after_s=3.0)

    monkeypatch.setattr(PR, "sync_account", boom)
    monkeypatch.setattr(PR, "save", lambda db, a, u, t, doc: saved.append(doc))
    monkeypatch.setattr(PR, "_round_authority", lambda db, layer: True)
    PR._sync_accounts(object(), [account], layer=None)
    doc = saved[-1]
    assert doc["status"] == "READ_FAILED"
    assert doc["reason"] == doc["execution"]["reason"] == "BROKER_RATE_LIMITED_LOCAL"
    assert doc["read_diagnostic"]["detail"] == "LOCAL_HTTP_BUDGET_REFUSED"
    assert doc["read_diagnostic"]["exception"] == "AdapterError"
