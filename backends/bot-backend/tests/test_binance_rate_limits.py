"""Rate limits on signed Binance calls, and what a venue error does (not) prove.

* an idempotent signed GET honours Retry-After (bounded) on HTTP 429 and backs
  off locally on a long wait or an HTTP 418 ban;
* that backoff, the used weight and a total sleep budget are kept per venue
  endpoint for the whole process (limits are per IP): they survive a rebuilt
  client and are shared by every account's client;
* a mutating request is NEVER retried, delayed or blocked by any of this;
* only a venue ANSWER of HTTP 4xx with a definitive code proves a request was
  refused -- a timeout, 5xx or unreadable body proves nothing.
"""
from __future__ import annotations

import time

import pytest

from app.exchange.binance import client as module
from app.execution.fill_resolution import (TRANSIENT_NOT_PROCESSED_CODES, client_order_absent, definitive_rejection,
                                           duplicate_client_order_id, venue_error)


class Reply:
    def __init__(self, status=200, body=None, headers=None):
        self.status_code, self._body, self.headers = status, body if body is not None else {}, headers or {}
        self.content, self.text = b"{}", str(body)

    def json(self):
        return self._body

    def raise_for_status(self):
        assert self.status_code < 400


class Wire:
    """A transport that plays back a script of replies; the last one repeats."""

    def __init__(self, *script):
        self.script, self.calls = list(script), []

    def _next(self, method):
        self.calls.append(method)
        step = self.script.pop(0) if len(self.script) > 1 else self.script[0]
        if isinstance(step, Exception):
            raise step
        return step

    def get(self, url, **kw):
        return self._next("GET")

    def post(self, url, **kw):
        return self._next("POST")

    def delete(self, url, **kw):
        return self._next("DELETE")


@pytest.fixture
def slept(monkeypatch):
    naps = []
    monkeypatch.setattr(module.time, "sleep", naps.append)
    return naps


BASE = "https://demo-fapi.binance.com"


@pytest.fixture(autouse=True)
def fresh_rate_state():
    module.reset_rate_limit_state()
    yield
    module.reset_rate_limit_state()


def venue(wire, base_url=BASE):
    client = module.BinanceFuturesClient.__new__(module.BinanceFuturesClient)
    client.api_key, client.api_secret, client.base_url = "k", "s", base_url
    client.recv_window, client._time_offset_ms, client.last_used_weight_1m = 5000, 0, None
    client.session = wire
    return client


LIMITED = {"code": -1003, "msg": "Too many requests."}


def test_a_signed_read_honours_retry_after_and_succeeds(slept):
    wire = Wire(Reply(429, LIMITED, {"Retry-After": "2"}), Reply(200, {"positionAmt": "0"}))
    assert venue(wire)._signed_get("/fapi/v2/positionRisk", {"symbol": "ADAUSDT"}) == {"positionAmt": "0"}
    assert wire.calls == ["GET", "GET"] and slept == [2.0]


def test_signed_read_retries_are_bounded_and_the_error_still_surfaces(slept):
    wire = Wire(Reply(429, LIMITED, {"Retry-After": "1"}))
    client = venue(wire)
    with pytest.raises(RuntimeError, match="Binance HTTP 429"):
        client._signed_get("/fapi/v2/positionRisk")
    assert wire.calls == ["GET"] * (1 + module.RATE_LIMIT_MAX_RETRIES) and slept == [1.0] * module.RATE_LIMIT_MAX_RETRIES
    # Exhausted: later reads back off locally instead of hammering the venue.
    with pytest.raises(RuntimeError, match="rate limit backoff"):
        client._signed_get("/fapi/v2/positionRisk")
    assert len(wire.calls) == 1 + module.RATE_LIMIT_MAX_RETRIES


@pytest.mark.parametrize("reply", [Reply(429, LIMITED, {"Retry-After": "120"}), Reply(418, LIMITED, {"Retry-After": "3"}),
                                   Reply(418, LIMITED)])
def test_a_long_wait_or_a_ban_is_never_slept_through_or_retried(slept, reply):
    wire = Wire(reply)
    client = venue(wire)
    with pytest.raises(RuntimeError, match=f"Binance HTTP {reply.status_code}"):
        client._signed_get("/fapi/v2/account")
    assert wire.calls == ["GET"] and slept == []
    with pytest.raises(RuntimeError, match="rate limit backoff"):
        client._signed_get("/fapi/v2/account")
    assert wire.calls == ["GET"]                            # the venue was not asked again
    # A mutation is never blocked by the read backoff: a close must always be attempted.
    client.session = Wire(Reply(200, {"orderId": 1}))
    assert client._signed_post("/fapi/v1/order", {"symbol": "ADAUSDT", "reduceOnly": "true"}) == {"orderId": 1}


@pytest.mark.parametrize("method", ["POST", "DELETE"])
@pytest.mark.parametrize("status", [429, 418])
def test_a_rate_limited_mutation_is_sent_exactly_once_and_never_slept_on(slept, method, status):
    wire = Wire(Reply(status, LIMITED, {"Retry-After": "1"}))
    client = venue(wire)
    with pytest.raises(RuntimeError, match=f"Binance HTTP {status}"):
        client._signed_request(method, "/fapi/v1/order", {"symbol": "ADAUSDT", "reduceOnly": "true"})
    assert wire.calls == [method] and slept == []
    assert module.rate_limit_backoff_remaining(BASE) == 0.0  # an order limit says nothing about reads


def test_reads_are_paced_when_the_recorded_weight_nears_the_budget(slept):
    threshold = int(module.REQUEST_WEIGHT_LIMIT_1M * module.REQUEST_WEIGHT_PAUSE_FRACTION)
    client = venue(Wire(Reply(200, {}, {"X-MBX-USED-WEIGHT-1M": str(threshold)})))
    client._signed_get("/fapi/v2/account")                  # nothing recorded yet: no pause
    assert slept == [] and client.last_used_weight_1m == threshold
    client._signed_get("/fapi/v2/account")
    assert slept == [module.REQUEST_WEIGHT_PAUSE_SECONDS]
    client._signed_post("/fapi/v1/order", {"symbol": "ADAUSDT", "reduceOnly": "true"})
    assert slept == [module.REQUEST_WEIGHT_PAUSE_SECONDS]   # mutations are never delayed

    # The count is per IP: another account's client paces on the same reading...
    other = venue(Wire(Reply(200, {}, {"X-MBX-USED-WEIGHT-1M": str(threshold - 1)})))
    other._signed_get("/fapi/v2/account")
    assert slept == [module.REQUEST_WEIGHT_PAUSE_SECONDS] * 2
    # ...and stops as soon as any response shows the weight has dropped.
    other._signed_get("/fapi/v2/account")
    client.session = Wire(Reply(200, {}))
    client._signed_get("/fapi/v2/account")
    assert slept == [module.REQUEST_WEIGHT_PAUSE_SECONDS] * 2

    module.reset_rate_limit_state()
    stale = venue(Wire(Reply(200, {})))
    stale._note_weight(Reply(200, {}, {"X-MBX-USED-WEIGHT-1M": str(threshold)}))
    module._rate_state(BASE)["noted_at"] = time.monotonic() - 120
    stale._signed_get("/fapi/v2/account")                   # an old reading is not "now"
    assert slept == [module.REQUEST_WEIGHT_PAUSE_SECONDS] * 2


# ── Process-wide state: per IP, not per client object ───────────────────────

def test_the_backoff_survives_a_rebuilt_client_and_is_shared_across_accounts(slept):
    first = venue(Wire(Reply(418, LIMITED, {"Retry-After": "300"})))
    with pytest.raises(RuntimeError, match="Binance HTTP 418"):
        first._signed_get("/fapi/v2/account")
    assert 299 < module.rate_limit_backoff_remaining(BASE) <= 300
    # The runtime drops its cached client after any error. The NEW client -- or another
    # account's -- must not touch the banned IP again: reads fail closed, without the wire.
    rebuilt_wire = _as_request(Wire(Reply(200, {})))
    rebuilt = venue(rebuilt_wire)
    for read in (lambda: rebuilt._signed_get("/fapi/v2/positionRisk"), lambda: rebuilt.position_risk(),
                 lambda: rebuilt._request("GET", "/fapi/v1/exchangeInfo"), lambda: rebuilt.sync_time()):
        with pytest.raises(module.BinanceRateLimited, match="rate limit backoff"):
            read()
    assert rebuilt_wire.calls == [] and slept == []
    # A close is never held back by it.
    assert rebuilt._signed_post("/fapi/v1/order", {"symbol": "ADAUSDT", "reduceOnly": "true"}) == {}
    assert rebuilt._request("POST", "/fapi/v1/order", params={"symbol": "ADAUSDT", "reduceOnly": "true"}) == {}
    assert rebuilt_wire.calls == ["POST", "POST"] and slept == []
    # Another venue endpoint (another IP limit) is unaffected.
    elsewhere = venue(Wire(Reply(200, {"ok": 1})), base_url="https://fapi.binance.com")
    assert elsewhere._signed_get("/fapi/v2/account") == {"ok": 1}
    # When the venue's Retry-After has passed, reads resume.
    module._rate_state(BASE)["until"] = time.monotonic() - 1
    assert rebuilt._signed_get("/fapi/v2/account") == {}


def test_total_rate_limit_sleep_is_bounded_across_calls_and_clients(slept):
    wait = module.RATE_LIMIT_MAX_SLEEP_SECONDS
    affordable = int(module.RATE_LIMIT_SLEEP_BUDGET_SECONDS // wait)
    limited, answered = Reply(429, LIMITED, {"Retry-After": str(wait)}), Reply(200, {})
    wire = Wire(*[limited, answered] * affordable, limited, limited, answered)
    for _ in range(affordable):                             # several accounts in one cycle
        assert venue(wire)._signed_get("/fapi/v2/account") == {}
    assert slept == [wait] * affordable
    # The budget for this window is spent: the next 429 is NOT slept on. The read fails
    # closed and every later read of any client is refused locally, without sleeping.
    with pytest.raises(RuntimeError, match="Binance HTTP 429"):
        venue(wire)._signed_get("/fapi/v2/account")
    sent = len(wire.calls)
    with pytest.raises(module.BinanceRateLimited):
        venue(wire)._signed_get("/fapi/v2/account")
    assert slept == [wait] * affordable and len(wire.calls) == sent
    assert sum(slept) <= module.RATE_LIMIT_SLEEP_BUDGET_SECONDS < 60
    # A new window (the next cycle) gets a new budget.
    state = module._rate_state(BASE)
    state["window_start"] -= module.RATE_LIMIT_SLEEP_BUDGET_WINDOW_SECONDS
    state["until"] = 0.0
    assert venue(wire)._signed_get("/fapi/v2/account") == {}
    assert slept == [wait] * (affordable + 1)


def test_weight_pacing_can_delay_a_read_but_never_refuses_one(slept):
    threshold = int(module.REQUEST_WEIGHT_LIMIT_1M * module.REQUEST_WEIGHT_PAUSE_FRACTION)
    client = venue(Wire(Reply(200, {"ok": 1}, {"X-MBX-USED-WEIGHT-1M": str(threshold)})))
    reads = int(module.RATE_LIMIT_SLEEP_BUDGET_SECONDS / module.REQUEST_WEIGHT_PAUSE_SECONDS) + 5
    for _ in range(1 + reads):
        assert client._signed_get("/fapi/v2/positionRisk") == {"ok": 1}
    # Paced only within the budget; after that the read is simply sent.
    assert sum(slept) == module.RATE_LIMIT_SLEEP_BUDGET_SECONDS and len(client.session.calls) == 1 + reads


def test_unsigned_reads_share_the_backoff_and_never_sleep_through_a_ban(slept):
    wire = Wire(Reply(418, LIMITED, {"Retry-After": "120"}))
    with pytest.raises(RuntimeError, match="HTTP 418"):
        venue(_as_request(wire))._request("GET", "/fapi/v1/ticker/price")
    assert wire.calls == ["GET"] and slept == []            # used to sleep and retry up to six times
    with pytest.raises(module.BinanceRateLimited):
        venue(_as_request(wire))._signed_get("/fapi/v2/account")
    assert wire.calls == ["GET"]
    module.reset_rate_limit_state()
    long_wait = Wire(Reply(429, LIMITED, {"Retry-After": "45"}))
    with pytest.raises(RuntimeError, match="HTTP 429"):
        venue(_as_request(long_wait))._request("GET", "/fapi/v1/ticker/price")
    assert long_wait.calls == ["GET"] and slept == [] and module.rate_limit_backoff_remaining(BASE) > 40


def _as_request(wire):
    """``_request`` goes through ``session.request``; the signed calls through get/post."""
    wire.request = lambda method, url, **kw: wire._next(method)
    return wire


def test_a_timestamp_refusal_is_not_resent_when_the_clock_cannot_be_read(slept):
    skew = Reply(400, {"code": -1021, "msg": "Timestamp for this request is outside of the recvWindow."})
    skew.text = '{"code":-1021,"msg":"Timestamp for this request is outside of the recvWindow."}'
    wire = Wire(skew)
    client = venue(wire)
    client.sync_time = lambda: (_ for _ in ()).throw(module.BinanceRateLimited("backing off"))
    with pytest.raises(RuntimeError, match="Binance HTTP 400") as refused:
        client._signed_post("/fapi/v1/order", {"symbol": "ADAUSDT", "reduceOnly": "true"})
    # One POST only, and the venue's own answer surfaces: definitively not processed.
    assert wire.calls == ["POST"] and definitive_rejection(refused.value) == -1021


# ── What a venue error proves ────────────────────────────────────────────────

def http(status, body):
    return RuntimeError(f"Binance HTTP {status}: {body}")


@pytest.mark.parametrize("error,code", [
    (http(400, '{"code":-2019,"msg":"Margin is insufficient."}'), -2019),
    (http(400, '{"code":-4164,"msg":"Order\'s notional must be no smaller than 5."}'), -4164),
    (http(400, '{"code":-2022,"msg":"ReduceOnly Order is rejected."}'), -2022),
    (http(400, '{"code":-1021,"msg":"Timestamp for this request is outside of the recvWindow."}'), -1021),
    (http(401, '{"code":-2015,"msg":"Invalid API-key, IP, or permissions for action."}'), -2015),
])
def test_a_4xx_answer_with_a_definitive_code_proves_the_request_was_refused(error, code):
    assert definitive_rejection(error) == code
    # The executor wraps the client's error; the chain is still read.
    try:
        raise ValueError(f"Unexpected exchange error placing order (Protocol): {error}") from error
    except ValueError as wrapped:
        assert definitive_rejection(wrapped) == code


@pytest.mark.parametrize("error", [
    TimeoutError("read timed out"), ConnectionError("connection reset"), RuntimeError("order not found"),
    http(500, '{"code":-1001,"msg":"Internal error."}'), http(503, '{"code":-1008,"msg":"Server overloaded."}'),
    http(408, '{"code":-1007,"msg":"Timeout waiting for response from backend server."}'),
    http(400, '{"code":-1007,"msg":"Send status unknown."}'), http(400, '{"code":-1000,"msg":"Unknown error."}'),
    http(400, '{"code":-1006,"msg":"Unexpected response."}'), http(429, '{"code":-1003,"msg":"Too many requests."}'),
    http(418, '{"code":-1003,"msg":"Banned."}'), http(400, "<html>Bad Gateway</html>"), http(400, '{"msg":"no code"}'),
    http(400, '{"code":"-2019"}'), http(400, '{"code":200}'),
    RuntimeError("Binance request failed after retries: POST /fapi/v1/order (timeout)"),
])
def test_everything_else_leaves_the_outcome_unknown(error):
    assert definitive_rejection(error) is None


def test_only_the_exception_itself_and_its_explicit_cause_are_classified():
    refused = http(400, '{"code":-2021,"msg":"Order would immediately trigger."}')
    # Raised while ANOTHER error was being handled: __context__ is set implicitly and
    # says nothing about this request.
    try:
        try:
            raise refused
        except RuntimeError:
            raise TimeoutError("read timed out")
    except TimeoutError as timeout:
        assert timeout.__context__ is refused and timeout.__cause__ is None
        assert venue_error(timeout) == (None, None) and definitive_rejection(timeout) is None
    # An explicit ``raise ... from`` is a statement about the same request and is read.
    try:
        raise ValueError("order failed") from refused
    except ValueError as wrapped:
        assert venue_error(wrapped) == (400, -2021) and definitive_rejection(wrapped) == -2021


@pytest.mark.parametrize("error", [
    http(400, '{"code":-4116,"msg":"ClientOrderId is duplicated."}'),
    http(400, '{"code":-4111,"msg":"Client tran id is duplicated."}'),
    http(400, '{"code":-2010,"msg":"Duplicate order sent."}'),
])
def test_a_duplicate_client_order_id_refusal_never_proves_that_nothing_exists(error):
    assert duplicate_client_order_id(error) is True and definitive_rejection(error) is None
    assert venue_error(error)[0] == 400                     # it IS a venue answer, just not a terminal one


def test_ordinary_rejections_are_not_mistaken_for_duplicates():
    for error in (http(400, '{"code":-2010,"msg":"Account has insufficient balance for requested action."}'),
                  http(400, '{"code":-2022,"msg":"ReduceOnly Order is rejected."}'), TimeoutError("slow"),
                  http(500, '{"code":-4116,"msg":"ClientOrderId is duplicated."}')):
        assert duplicate_client_order_id(error) is False
    assert definitive_rejection(http(400, '{"code":-2010,"msg":"Account has insufficient balance."}')) == -2010


def test_transient_codes_are_definitive_refusals_of_the_request():
    assert TRANSIENT_NOT_PROCESSED_CODES == {-1003, -1015, -1021}
    for code in TRANSIENT_NOT_PROCESSED_CODES:              # "not processed": a retry is safe...
        assert definitive_rejection(http(400, '{"code":%d,"msg":"x"}' % code)) == code
    # ...but a 429 / 418 carrying the same code proves nothing by itself.
    assert definitive_rejection(http(429, '{"code":-1015,"msg":"Too many new orders."}')) is None


def test_venue_error_reads_only_the_clients_own_error_shape():
    assert venue_error(http(400, '{"code":-2013,"msg":"Order does not exist."}')) == (400, -2013)
    assert venue_error(http(502, "<html></html>")) == (502, None)
    assert venue_error(RuntimeError("HTTP 400: nope")) == (None, None)
    assert venue_error(None) == (None, None)


def test_an_order_is_absent_only_on_the_venues_own_not_found_answer():
    class Lookup:
        def __init__(self, outcome):
            self.outcome, self.asked = outcome, []

        def get_order_by_client_order_id(self, symbol, client_order_id):
            self.asked.append((symbol, client_order_id))
            if isinstance(self.outcome, Exception):
                raise self.outcome
            return self.outcome

    absent = Lookup(http(400, '{"code":-2013,"msg":"Order does not exist."}'))
    assert client_order_absent(absent, "ADAUSDT", "cid-1") is True and absent.asked == [("ADAUSDT", "cid-1")]
    for outcome in ({"status": "FILLED"}, TimeoutError("slow"), http(500, '{"code":-2013}'),
                    http(400, '{"code":-1021,"msg":"timestamp"}'), RuntimeError("order not found")):
        assert client_order_absent(Lookup(outcome), "ADAUSDT", "cid-1") is False
    assert client_order_absent(absent, "ADAUSDT", None) is False
    assert client_order_absent(object(), "ADAUSDT", "cid-1") is False


# ── BingX: the production branch of place_market_order ──────────────────────

def test_bingx_market_order_verifies_one_way_mode_and_changes_no_leverage(monkeypatch):
    from unittest.mock import Mock

    from app.core import config
    from app.core.config import Settings
    from app.exchange.bingx.client import BingXClient

    client = BingXClient("key", "secret")
    monkeypatch.setattr(config, "settings", Settings(
        _env_file=None, APP_ENV="PRODUCTION", DATABASE_ROLE="production", ENVIRONMENT_NAME="production",
        EXECUTION_MODE="live"))
    one_way = {"data": {"dualSidePosition": False}}
    client._request = Mock(side_effect=[one_way, {"data": {"orderId": 77}}])
    order = client.place_market_order("BTC-USDT", "buy", 0.01)
    assert order["orderId"] == "77"
    assert [(c.args[0], c.args[1]) for c in client._request.call_args_list] == [
        ("GET", "/openApi/swap/v1/positionSide/dual"), ("POST", "/openApi/swap/v2/trade/order")]
    # A hedge-mode account is refused before any order is built.
    client._request = Mock(side_effect=[{"data": {"dualSidePosition": True}}])
    with pytest.raises(ValueError, match="NATIVE_PROTECTION_REQUIRES_ONE_WAY_ACCOUNT"):
        client.place_market_order("BTC-USDT", "buy", 0.01)
    assert client._request.call_count == 1
