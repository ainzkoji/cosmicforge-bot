"""Physical HTTP tests: shared admission, redirects and Retry-After persistence."""
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from app.exchange.platform_adapter import contract as C
from app.exchange.platform_adapter.http_budget import BudgetedHTTP, priority_context
from app.exchange.platform_adapter.rate_budget import RateBudget, ENTRY, PROTECTION
from shared_lib.persistence.cati_platform_schema import ensure_cati_platform_schema
from shared_lib.persistence.db import DB


@pytest.fixture
def database(tmp_path):
    db = DB(str(tmp_path / "http.db"))
    ensure_cati_platform_schema(db)
    return db


@pytest.fixture
def server():
    calls = []
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            calls.append(self.path)
            self.send_response(429 if self.path == "/limited" else 302 if self.path == "/redirect" else 200)
            self.send_header("Retry-After", "120")
            self.send_header("Location", "/unexpected")
            self.end_headers()
            self.wfile.write(b'{}')

        do_POST = do_GET
        def log_message(self, *_):
            pass
    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{server.server_port}", calls
    server.shutdown()
    server.server_close()
    thread.join()


def http(db, account="one", venue=C.BYBIT):
    return BudgetedHTTP(RateBudget(db), venue, account_ref=account,
                        server_address="shared-egress", clock=lambda: 100_000)


def test_redirect_is_not_followed_and_each_request_is_charged(database, server):
    url, calls = server
    client = http(database)
    assert client.get(url + "/redirect").status_code == 302
    assert client.get(url + "/ok").status_code == 200
    assert calls == ["/redirect", "/ok"]
    assert client.budget.snapshot(C.BYBIT, "shared-egress", 100_000)["used"] == 2
    client.close()


def test_429_closes_other_account_transport_before_network(database, server):
    url, calls = server
    first, second = http(database), http(database, "two")
    with pytest.raises(C.AdapterError) as caught:
        first.get(url + "/limited")
    assert caught.value.retry_after_s == 120
    with pytest.raises(C.AdapterError):
        second.get(url + "/ok")
    assert calls == ["/limited"]
    first.close()
    second.close()


def test_entries_cannot_borrow_emergency_priority(database, server):
    url, _ = server
    client = http(database)
    token = priority_context.set("EMERGENCY")
    try:
        client.post(url + "/mutation", json={"reduceOnly": False})
        client.post(url + "/mutation", json={"reduceOnly": True})
    finally:
        priority_context.reset(token)
    client.post(url + "/mutation", data='{"reduceOnly":true}')
    usage = client.budget.snapshot(C.BYBIT, "shared-egress", 100_000)["used_by_priority"]
    assert usage == {ENTRY: 1, "EMERGENCY": 1, PROTECTION: 1}
    client.close()


def test_unknown_binance_endpoint_fails_before_network(database, server):
    url, calls = server
    with pytest.raises(C.AdapterError, match="HTTP_ENDPOINT_WEIGHT_NOT_REGISTERED"):
        http(database, venue=C.BINANCE).get(url + "/unknown")
    assert calls == []


def test_real_transport_lost_ack_is_sent_once_and_interruption_is_not_sent(server):
    import requests
    from app.exchange.platform_adapter.http_faults import HTTPFaultInjector
    url, calls = server
    faults = HTTPFaultInjector(requests.Session(), C.BYBIT, signing_secret="test-only",
                               external_reduce=lambda *_: None)
    faults.lose_next_acknowledgment()
    faults.request("GET", url + "/preparatory-read")
    with pytest.raises(requests.ConnectionError, match="AFTER_SEND"):
        faults.request("POST", url + "/mutation")
    faults.interrupt_next_request()
    with pytest.raises(requests.ConnectionError, match="BEFORE_SEND"):
        faults.request("GET", url + "/never-sent")
    faults.request("GET", url + "/recovered")
    assert calls == ["/preparatory-read", "/mutation", "/recovered"]
    faults.close()


def test_injected_http_429_flows_through_persistent_budget(database, server):
    import requests
    from app.exchange.platform_adapter.http_faults import HTTPFaultInjector
    url, calls = server
    client = http(database)
    client.transport.close()
    faults = HTTPFaultInjector(requests.Session(), C.BYBIT, signing_secret="test-only",
                               external_reduce=lambda *_: None)
    client.transport = faults
    faults.rate_limit_next_request(90)
    with pytest.raises(C.AdapterError) as caught:
        client.get(url + "/ok")
    assert caught.value.retry_after_s == 90
    with pytest.raises(C.AdapterError):
        http(database, "other").get(url + "/ok")
    assert calls == []
    client.close()


@pytest.mark.parametrize("venue", [C.BINANCE, C.BINGX])
def test_skew_changes_timestamp_and_resigns_real_http_request(server, venue):
    import hashlib
    import hmac
    import requests
    from app.exchange.platform_adapter.http_faults import HTTPFaultInjector
    url, calls = server
    faults = HTTPFaultInjector(requests.Session(), venue, signing_secret="test-only",
                               external_reduce=lambda *_: None)
    faults.skew_clock(60000)
    faults.request("GET", url + "/ok?timestamp=1000&symbol=TEST&signature=old")
    query = "timestamp=61000&symbol=TEST"
    signature = hmac.new(b"test-only", query.encode(), hashlib.sha256).hexdigest()
    assert calls == ["/ok?" + query + "&signature=" + signature]
    faults.skew_clock(0)
    faults.request("GET", url + "/ok?timestamp=1000&signature=original")
    assert calls[-1] == "/ok?timestamp=1000&signature=original"
    faults.close()


def test_factory_injects_budget_before_constructor_and_adapter_does_not_double_charge(database):
    import json
    import requests
    from types import SimpleNamespace
    from app.exchange.platform_adapter.factory import adapter_for
    from app.exchange.platform_adapter.rate_budget import BudgetedVenueAdapter
    from shared_lib.broker.client_factory import build_client_from_auth
    from shared_lib.broker.environment import BrokerEnvironment, resolve_base_url

    calls = []
    class Wire:
        def request(self, method, url, **kwargs):
            calls.append(url)
            response = requests.Response()
            response.status_code = 200
            response._content = json.dumps({"symbols": [], "serverTime": 100_000}).encode()
            return response

    budget = RateBudget(database)
    transport = BudgetedHTTP(budget, C.BINANCE, account_ref="one", server_address="egress",
                             transport=Wire(), clock=lambda: 100_000)
    auth = SimpleNamespace(broker_type=C.BINANCE, environment=BrokerEnvironment.DEMO,
                           base_url=resolve_base_url(C.BINANCE, BrokerEnvironment.DEMO),
                           api_key="synthetic", api_secret="synthetic", account_id="one", user_id="u",
                           key_fingerprint="synthetic")
    client = build_client_from_auth(auth, http=transport)
    assert calls  # The constructor fetched metadata through the injected budget.
    adapter = BudgetedVenueAdapter(adapter_for(client, C.BINANCE, C.DEMO), budget,
                                   account_ref="one", server_address="egress", clock=lambda: 100_000)
    adapter.server_time()
    assert budget.snapshot(C.BINANCE, "egress", 100_000)["used"] == 100 * len(calls)


def test_bybit_clock_fault_resigns_headers_without_mutating_callers_headers():
    import hashlib
    import hmac
    from types import SimpleNamespace
    from app.exchange.platform_adapter.http_faults import HTTPFaultInjector
    captured = []
    wire = SimpleNamespace(request=lambda *a, **kw: captured.append(kw) or SimpleNamespace())
    faults = HTTPFaultInjector(wire, C.BYBIT, signing_secret="test-only", external_reduce=lambda *_: None)
    headers = {"X-BAPI-TIMESTAMP": "1000", "X-BAPI-API-KEY": "synthetic", "X-BAPI-RECV-WINDOW": "5000"}
    faults.skew_clock(60000)
    faults.request("GET", "https://example.invalid/read?symbol=TEST", headers=headers)
    signed = captured[0]["headers"]
    assert signed["X-BAPI-TIMESTAMP"] == "61000"
    assert signed["X-BAPI-SIGN"] == hmac.new(b"test-only", b"61000synthetic5000symbol=TEST", hashlib.sha256).hexdigest()
    assert headers["X-BAPI-TIMESTAMP"] == "1000" and "X-BAPI-SIGN" not in headers

