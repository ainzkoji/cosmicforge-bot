"""Position safety of the production engine: an existing position is always
maintained, and a failed close / protection order is retried -- bounded, and
only when the previous attempt is proven not to exist.

Everything here is about REDUCE-ONLY closes and closePosition protection legs.
The entry invariants (an ambiguous entry CREATE is never re-submitted; one
attempt per plan) are asserted again wherever a path could touch them.
"""
import json
import logging
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from _exec import Harness
from test_production_execution import live, profile
from test_production_demo_execution import demo, demo_profile
from test_production_research_separation import fresh, process
from test_demo_boundary_certification import broker
from app.core import config
from app.execution import demo_boundary_certification as cert
from app.execution import production_protection
from app.execution.production_close import (ABSENT_RESOLUTION_MS, EMERGENCY_IDENTITY_PREFIX, FILLED_NOT_FLAT_ALERT_MS,
                                            MAX_CLOSE_ATTEMPTS, MAX_TRANSIENT_CLOSE_ATTEMPTS,
                                            NAKED_POSITION_OPERATOR_REQUIRED, TRANSIENT_RETRY_BACKOFF_MS,
                                            WORKING_ALERT_MS, close_client_id)
from app.execution.production_protection import MAX_PROTECTION_ATTEMPTS, place_native_protection
from app.trading_intelligence.execution.adapter import BrokerPositionState, OrderState
from app.trading_intelligence.execution.boundary import BoundaryStatus, ENTRY_ABSENT_MIN_MS
from app.trading_intelligence.contracts.execution import ExecutionAttemptStatus as X
from app.trading_intelligence.integration import production_execution as production

__all__ = ["live", "demo", "fresh", "broker"]  # fixtures re-exported for pytest

REJECTED = 'Binance HTTP 400: {"code":-4164,"msg":"Order notional is too small."}'
SKEWED = 'Binance HTTP 400: {"code":-1021,"msg":"Timestamp for this request is outside of the recvWindow."}'
WINDOW = ABSENT_RESOLUTION_MS + 1_000


def close_rows(h):
    with h.db.connect() as c:
        return {r["identity"]: (r["status"], json.loads(r["document"]))
                for r in c.execute("SELECT * FROM cati_production_closes ORDER BY rowid")}


def at(monkeypatch, now):
    """Move the close path's clock (it shares the certification module's ``time``)."""
    monkeypatch.setattr(cert.time, "time", lambda: now / 1000)
    return now


def close_row(h):
    with h.db.connect() as c:
        row = c.execute("SELECT * FROM cati_production_closes").fetchone()
    return row["status"], row["identity"], json.loads(row["document"])


def alerts(db):
    with db.connect() as c:
        c.execute("CREATE TABLE IF NOT EXISTS alerts (id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT, alert_type TEXT,"
                  " severity TEXT, trace_id TEXT, symbol TEXT, message TEXT, details_json TEXT,"
                  " acknowledged INTEGER DEFAULT 0)")
        return [tuple(r) for r in c.execute("SELECT alert_type, severity, symbol FROM alerts WHERE alert_type=?",
                                            (NAKED_POSITION_OPERATOR_REQUIRED,))]


def order_posts(h):
    return [c for c in h.client._signed_post.call_args_list if c.args[0] == "/fapi/v1/order"]


def reconcile(h, now):
    return production.reconcile_executions(h.db, h.boundary_for(), h.client, now)


# ── The fail-safe close ──────────────────────────────────────────────────────

def test_definitively_rejected_close_is_retried_with_a_new_client_id(broker):
    h, state = broker
    cert.run(h.db, h.account["id"], "close-rejected", action="hold")
    working = h.client._signed_post.side_effect

    def rejecting(path, params):
        if path.endswith("algoOrder"):
            return working(path, params)
        raise RuntimeError(REJECTED)

    h.client._signed_post.side_effect = rejecting
    with pytest.raises(RuntimeError, match="-4164"):
        cert.run(h.db, h.account["id"], "close-rejected", action="close")
    status, _, document = close_row(h)
    assert status == "PENDING" and state["qty"] > 0
    assert [a["outcome"] for a in document["attempts"]] == ["REJECTED"]
    assert document["attempts"][0]["venue_code"] == -4164

    # The venue refused it, so the order does not exist: the next cycle may try again.
    h.client._signed_post.side_effect = working
    reconcile(h, h.now)
    status, _, document = close_row(h)
    first, second = document["attempts"]
    assert status == "CLOSED" and state["qty"] == 0 and not state["legs"]
    assert second["client_order_id"] == first["client_order_id"] + "-r1" and len(second["client_order_id"]) <= 36
    assert second["outcome"] == "FILLED"
    posts = order_posts(h)
    assert len(posts) == 2
    # Every close attempt is reduce-only: it can never open or increase a position.
    assert all(p.kwargs["params"]["reduceOnly"] == "true" for p in posts)
    assert state["create_count"] == 1                      # never a second ENTRY


def test_unknown_close_is_retried_only_after_the_broker_proves_it_absent(broker, monkeypatch):
    h, state = broker
    cert.run(h.db, h.account["id"], "close-unknown", action="hold")
    working = h.client._signed_post.side_effect
    h.client._signed_post.side_effect = TimeoutError("unknown outcome")
    with pytest.raises(TimeoutError):
        cert.run(h.db, h.account["id"], "close-unknown", action="close")
    assert len(order_posts(h)) == 1

    # Inside the window the outcome is unknown: fail closed, nothing is re-sent.
    protected = h.client.place_protection.call_count
    with pytest.raises(ValueError, match="CLOSE_SUBMIT_OUTCOME_UNKNOWN"):
        reconcile(h, h.now)
    assert len(order_posts(h)) == 1 and close_row(h)[0] == "PENDING"
    # ...and the pending close did not stop this position's protection maintenance.
    assert h.client.place_protection.call_count == protected + 1 and len(state["legs"]) == 2

    # A read that fails (not an authoritative "no such order") proves nothing, at any age.
    later = h.now + WINDOW
    monkeypatch.setattr(cert.time, "time", lambda: later / 1000)
    lookup = h.client.get_order_by_client_order_id.side_effect
    h.client.get_order_by_client_order_id.side_effect = TimeoutError("venue unreachable")
    with pytest.raises(TimeoutError):
        reconcile(h, later)
    assert len(order_posts(h)) == 1

    # Authoritatively absent AND past the window AND still open: one new attempt.
    h.client.get_order_by_client_order_id.side_effect = lookup
    h.client._signed_post.side_effect = working
    reconcile(h, later)
    status, _, document = close_row(h)
    assert status == "CLOSED" and state["qty"] == 0
    assert [a["outcome"] for a in document["attempts"]] == ["ABSENT", "FILLED"]
    assert document["attempts"][1]["client_order_id"].endswith("-r1")
    assert len(order_posts(h)) == 2 and state["create_count"] == 1


def test_close_attempts_are_bounded_and_then_alert_the_operator(broker, monkeypatch):
    h, state = broker
    cert.run(h.db, h.account["id"], "close-exhausted", action="hold")
    h.client._signed_post.side_effect = TimeoutError("unknown outcome")
    with pytest.raises(TimeoutError):
        cert.run(h.db, h.account["id"], "close-exhausted", action="close")
    now = h.now
    for _ in range(MAX_CLOSE_ATTEMPTS - 1):
        now += WINDOW
        monkeypatch.setattr(cert.time, "time", lambda now=now: now / 1000)
        with pytest.raises(TimeoutError):
            reconcile(h, now)
    assert len(order_posts(h)) == MAX_CLOSE_ATTEMPTS and not alerts(h.db)

    for _ in range(2):                                     # every later cycle: no further order
        now += WINDOW
        monkeypatch.setattr(cert.time, "time", lambda now=now: now / 1000)
        with pytest.raises(ValueError, match="CLOSE_SUBMIT_OUTCOME_UNKNOWN"):
            reconcile(h, now)
    assert len(order_posts(h)) == MAX_CLOSE_ATTEMPTS
    ids = [p.kwargs["params"]["newClientOrderId"] for p in order_posts(h)]
    assert len(set(ids)) == MAX_CLOSE_ATTEMPTS and ids[1:] == [ids[0] + "-r1", ids[0] + "-r2"]
    # One de-duplicated CRITICAL alert, and the position keeps its native protection.
    assert alerts(h.db) == [(NAKED_POSITION_OPERATOR_REQUIRED, "CRITICAL", "ADAUSDT")]
    assert state["qty"] > 0 and len(state["legs"]) == 2 and close_row(h)[0] == "PENDING"

    # Once the operator has flattened it at the venue, the intent retires by itself.
    state["qty"] = 0.
    state["fills"].append({"id": 9, "orderId": 777, "symbol": "ADAUSDT", "side": "SELL",
                           "qty": state["fills"][0]["qty"], "price": "99", "time": h.now})
    reconcile(h, now)
    status, _, document = close_row(h)
    assert status == "CLOSED" and "order" not in document and len(order_posts(h)) == MAX_CLOSE_ATTEMPTS


def test_an_expired_close_order_is_replaced_for_the_remaining_position(broker):
    h, state = broker
    cert.run(h.db, h.account["id"], "close-expired", action="hold")
    working = h.client._signed_post.side_effect

    def expired(path, params):
        state["orders"][params["newClientOrderId"]] = {"symbol": "ADAUSDT", "orderId": 600, "status": "EXPIRED",
                                                       "clientOrderId": params["newClientOrderId"], "executedQty": "0"}
        return {}

    h.client._signed_post.side_effect = expired
    with pytest.raises(ValueError, match="CLOSE_FILL_OR_FLAT_UNCONFIRMED"):
        cert.run(h.db, h.account["id"], "close-expired", action="close")
    assert state["qty"] > 0
    h.client._signed_post.side_effect = working
    reconcile(h, h.now)
    status, _, document = close_row(h)
    assert status == "CLOSED" and state["qty"] == 0
    assert [a["outcome"] for a in document["attempts"]] == ["TERMINAL_UNFILLED", "FILLED"]


# A close is often sent while ANOTHER error is being handled (the fail-safe close
# runs in the ``except`` block of a failed protection call). Only the close's own
# exception may classify its outcome.

PROTECTION_REFUSED = 'Binance HTTP 400: {"code":-2021,"msg":"Order would immediately trigger."}'


def test_a_close_timeout_while_handling_a_venue_error_stays_unknown(broker):
    h, state = broker
    cert.run(h.db, h.account["id"], "close-in-handler", action="hold")
    # Protection repair is refused by the venue; the fail-safe close then times out.
    h.client.place_protection.side_effect = RuntimeError(PROTECTION_REFUSED)
    h.client._signed_post.side_effect = TimeoutError("read timed out")
    with pytest.raises(TimeoutError) as failure:
        reconcile(h, h.now)
    # The situation under test: the timeout was raised inside the handler of the venue error.
    assert "-2021" in str(failure.value.__context__) and failure.value.__cause__ is None
    status, _, document = close_row(h)
    # Not the protection error's -2021: the close's own outcome is unknown.
    assert status == "PENDING" and [a["outcome"] for a in document["attempts"]] == ["UNKNOWN"]
    assert "venue_code" not in document["attempts"][0]
    # So the next cycle reads back and waits; it never re-sends immediately.
    with pytest.raises(ValueError, match="CLOSE_SUBMIT_OUTCOME_UNKNOWN"):
        reconcile(h, h.now)
    assert len(order_posts(h)) == 1 and state["qty"] > 0 and state["create_count"] == 1


def test_a_real_rejection_of_a_close_sent_from_an_error_handler_is_still_recognised(broker):
    h, state = broker
    cert.run(h.db, h.account["id"], "close-in-handler-rejected", action="hold")
    h.client.place_protection.side_effect = TimeoutError("protection read timed out")
    h.client._signed_post.side_effect = RuntimeError(REJECTED)
    with pytest.raises(RuntimeError, match="-4164"):
        reconcile(h, h.now)
    attempt = close_row(h)[2]["attempts"][0]
    assert attempt["outcome"] == "REJECTED" and attempt["venue_code"] == -4164


# ── Transient "request not processed" answers do not burn the attempt budget ─

def test_transient_rejections_are_spaced_out_and_counted_separately(broker, monkeypatch):
    h, state = broker
    cert.run(h.db, h.account["id"], "close-skewed", action="hold")
    working = h.client._signed_post.side_effect

    def skewed(path, params):
        if path.endswith("algoOrder"):
            return working(path, params)
        raise RuntimeError(SKEWED)

    h.client._signed_post.side_effect = skewed
    with pytest.raises(RuntimeError, match="-1021"):
        cert.run(h.db, h.account["id"], "close-skewed", action="close")
    now = h.now
    for sent, wait in enumerate(TRANSIENT_RETRY_BACKOFF_MS, start=1):
        # Inside the backoff nothing is sent, however often the cycle runs.
        at(monkeypatch, now + wait - 1)
        with pytest.raises(ValueError, match="CLOSE_RETRY_BACKOFF"):
            reconcile(h, now + wait - 1)
        assert len(order_posts(h)) == sent
        if sent == len(TRANSIENT_RETRY_BACKOFF_MS):
            break
        now = at(monkeypatch, now + wait)
        with pytest.raises(RuntimeError, match="-1021"):
            reconcile(h, now)
        assert len(order_posts(h)) == sent + 1
    # Three venue refusals so far -- that used to exhaust the intent. It is still alive.
    _, _, document = close_row(h)
    assert len(document["attempts"]) == MAX_CLOSE_ATTEMPTS and not alerts(h.db)
    assert all(a["outcome"] == "REJECTED" and a["transient"] and a["venue_code"] == -1021 for a in document["attempts"])
    h.client._signed_post.side_effect = working
    now = at(monkeypatch, now + TRANSIENT_RETRY_BACKOFF_MS[-1])
    reconcile(h, now)
    status, _, document = close_row(h)
    assert status == "CLOSED" and state["qty"] == 0 and document["attempts"][-1]["outcome"] == "FILLED"
    posts = order_posts(h)
    assert len(posts) == MAX_CLOSE_ATTEMPTS + 1 and all(p.kwargs["params"]["reduceOnly"] == "true" for p in posts)
    assert state["create_count"] == 1


def test_transient_attempts_have_their_own_cap_and_real_rejections_keep_theirs(broker, monkeypatch):
    h, state = broker
    cert.run(h.db, h.account["id"], "close-transient-cap", action="hold")
    answers = [SKEWED] * MAX_TRANSIENT_CLOSE_ATTEMPTS
    working = h.client._signed_post.side_effect

    def refusing(path, params):
        if path.endswith("algoOrder"):
            return working(path, params)
        raise RuntimeError(answers.pop(0))

    h.client._signed_post.side_effect = refusing
    with pytest.raises(RuntimeError):
        cert.run(h.db, h.account["id"], "close-transient-cap", action="close")
    now = h.now
    while answers:
        now = at(monkeypatch, now + TRANSIENT_RETRY_BACKOFF_MS[-1])
        with pytest.raises(RuntimeError):
            reconcile(h, now)
    assert len(order_posts(h)) == MAX_TRANSIENT_CLOSE_ATTEMPTS
    assert len(close_row(h)[2]["attempts"]) == MAX_TRANSIENT_CLOSE_ATTEMPTS > MAX_CLOSE_ATTEMPTS
    for _ in range(2):                                     # the transient cap: bounded, then alert
        now = at(monkeypatch, now + TRANSIENT_RETRY_BACKOFF_MS[-1])
        with pytest.raises(ValueError, match="CLOSE_SUBMIT_OUTCOME_UNKNOWN"):
            reconcile(h, now)
    ids = [p.kwargs["params"]["newClientOrderId"] for p in order_posts(h)]
    assert len(ids) == len(set(ids)) == MAX_TRANSIENT_CLOSE_ATTEMPTS and all(len(i) <= 36 for i in ids)
    assert alerts(h.db) == [(NAKED_POSITION_OPERATOR_REQUIRED, "CRITICAL", "ADAUSDT")]


def test_a_real_rejection_is_never_delayed_and_keeps_the_cap_of_three(broker, monkeypatch):
    h, state = broker
    cert.run(h.db, h.account["id"], "close-mixed", action="hold")
    answers = [SKEWED, REJECTED, REJECTED, REJECTED]
    working = h.client._signed_post.side_effect

    def refusing(path, params):
        if path.endswith("algoOrder"):
            return working(path, params)
        raise RuntimeError(answers.pop(0))

    h.client._signed_post.side_effect = refusing
    with pytest.raises(RuntimeError, match="-1021"):
        cert.run(h.db, h.account["id"], "close-mixed", action="close")
    now = at(monkeypatch, h.now + TRANSIENT_RETRY_BACKOFF_MS[0])
    for _ in range(MAX_CLOSE_ATTEMPTS):                    # real rejections: the very next cycle retries
        with pytest.raises(RuntimeError, match="-4164"):
            reconcile(h, now)
    with pytest.raises(ValueError, match="CLOSE_SUBMIT_OUTCOME_UNKNOWN"):
        reconcile(h, now)
    assert len(order_posts(h)) == 1 + MAX_CLOSE_ATTEMPTS
    assert alerts(h.db) == [(NAKED_POSITION_OPERATOR_REQUIRED, "CRITICAL", "ADAUSDT")]


def test_a_rate_limited_close_stays_unknown_and_counts_as_transient_once_proven_absent(broker, monkeypatch):
    h, state = broker
    cert.run(h.db, h.account["id"], "close-429", action="hold")
    working = h.client._signed_post.side_effect

    def limited(path, params):
        if path.endswith("algoOrder"):
            return working(path, params)
        raise RuntimeError('Binance HTTP 429: {"code":-1015,"msg":"Too many new orders."}')

    h.client._signed_post.side_effect = limited
    with pytest.raises(RuntimeError, match="HTTP 429"):
        cert.run(h.db, h.account["id"], "close-429", action="close")
    # A 429 proves nothing about the order: UNKNOWN, resolved by read-back only.
    assert [a["outcome"] for a in close_row(h)[2]["attempts"]] == ["UNKNOWN"]
    with pytest.raises(ValueError, match="CLOSE_SUBMIT_OUTCOME_UNKNOWN"):
        reconcile(h, h.now)
    assert len(order_posts(h)) == 1
    h.client._signed_post.side_effect = working
    later = at(monkeypatch, h.now + WINDOW)
    reconcile(h, later)
    status, _, document = close_row(h)
    assert status == "CLOSED" and [a["outcome"] for a in document["attempts"]] == ["ABSENT", "FILLED"]
    assert document["attempts"][0]["transient"] is True   # not one of the three real attempts


# ── Timing: the absence window starts at the real send ───────────────────────

def test_a_close_is_stamped_after_its_pre_post_reads(broker, monkeypatch):
    h, state = broker
    state["qty"] = 2.
    clock = [h.now]
    monkeypatch.setattr(cert.time, "time", lambda: clock[0] / 1000)

    def slow_position_read(*a):                            # e.g. waiting out a rate limit
        clock[0] += 20_000
        return state["qty"]

    h.client.get_position_amt.side_effect = slow_position_read
    h.client._signed_post.side_effect = TimeoutError("unknown outcome")
    h.client._production_intent_identity = EMERGENCY_IDENTITY_PREFIX + "stamp"
    from app.execution.production_close import close_position
    with pytest.raises(TimeoutError):
        close_position(h.client, "ADAUSDT")
    _, _, document = close_row(h)
    assert document["attempts"][0]["requested_at"] == h.now + 20_000 == document["requested_at"]


# ── Alerts for closes that are stuck without being exhausted ─────────────────

def test_a_filled_close_that_left_the_position_open_alerts_the_operator(broker, monkeypatch):
    h, state = broker
    cert.run(h.db, h.account["id"], "close-filled-not-flat", action="hold")

    def partial(path, params):
        state["orders"][params["newClientOrderId"]] = {"symbol": "ADAUSDT", "orderId": 601, "status": "FILLED",
                                                       "clientOrderId": params["newClientOrderId"], "executedQty": "1"}
        return {"orderId": 601}

    h.client._signed_post.side_effect = partial
    with pytest.raises(ValueError, match="CLOSE_FILL_OR_FLAT_UNCONFIRMED"):
        cert.run(h.db, h.account["id"], "close-filled-not-flat", action="close")
    with pytest.raises(ValueError, match="CLOSE_FILL_OR_FLAT_UNCONFIRMED"):
        reconcile(h, h.now)
    assert not alerts(h.db)                                # a position read may lag a fill for a moment
    later = at(monkeypatch, h.now + FILLED_NOT_FLAT_ALERT_MS)
    for _ in range(2):
        with pytest.raises(ValueError, match="CLOSE_FILL_OR_FLAT_UNCONFIRMED"):
            reconcile(h, later)
    assert alerts(h.db) == [(NAKED_POSITION_OPERATOR_REQUIRED, "CRITICAL", "ADAUSDT")]   # de-duplicated
    assert len(order_posts(h)) == 1 and state["qty"] > 0


def test_a_close_order_still_working_after_five_minutes_alerts_the_operator(broker, monkeypatch):
    h, state = broker
    cert.run(h.db, h.account["id"], "close-working", action="hold")

    def resting(path, params):
        state["orders"][params["newClientOrderId"]] = {"symbol": "ADAUSDT", "orderId": 602, "status": "NEW",
                                                       "clientOrderId": params["newClientOrderId"], "executedQty": "0"}
        return {"orderId": 602}

    h.client._signed_post.side_effect = resting
    with pytest.raises(ValueError, match="CLOSE_FILL_OR_FLAT_UNCONFIRMED"):
        cert.run(h.db, h.account["id"], "close-working", action="close")
    soon = at(monkeypatch, h.now + WORKING_ALERT_MS - 1)
    with pytest.raises(ValueError, match="CLOSE_FILL_OR_FLAT_UNCONFIRMED"):
        reconcile(h, soon)
    assert not alerts(h.db)
    later = at(monkeypatch, h.now + WORKING_ALERT_MS)
    with pytest.raises(ValueError, match="CLOSE_FILL_OR_FLAT_UNCONFIRMED"):
        reconcile(h, later)
    assert alerts(h.db) == [(NAKED_POSITION_OPERATOR_REQUIRED, "CRITICAL", "ADAUSDT")]
    assert len(order_posts(h)) == 1                        # never a second close while one may be live


# ── Rollout: a close row written before attempts were recorded ───────────────

def test_a_legacy_pending_close_row_waits_a_full_window_from_first_sighting(broker, monkeypatch, caplog):
    h, state = broker
    state["qty"] = 2.
    identity = EMERGENCY_IDENTITY_PREFIX + "legacy"
    cid = close_client_id(h.account["id"], identity, "ADAUSDT")
    legacy = {"request": {"symbol": "ADAUSDT", "side": "SELL", "type": "MARKET", "quantity": 2.0, "reduceOnly": "true",
                          "newClientOrderId": cid}, "requested_at": h.now - 86_400_000}
    with h.db.connect() as c:
        c.execute("CREATE TABLE IF NOT EXISTS cati_production_closes (account_id TEXT,client_id TEXT,symbol TEXT,"
                  "identity TEXT,status TEXT,document TEXT,PRIMARY KEY(account_id,client_id))")
        c.execute("INSERT INTO cati_production_closes VALUES(?,?,?,?,'PENDING',?)",
                  (h.account["id"], cid, "ADAUSDT", identity, json.dumps(legacy)))
    # Its order does not exist at the venue and its timestamp is a day old: on the first
    # cycle after deploy that must NOT read as "proven absent" and fire a market close.
    with caplog.at_level(logging.WARNING, logger="app.execution.production_close"):
        with pytest.raises(ValueError, match="CLOSE_SUBMIT_OUTCOME_UNKNOWN"):
            reconcile(h, h.now)
    assert not order_posts(h) and state["qty"] == 2.
    assert any("legacy close row" in r.getMessage() and cid in r.getMessage() for r in caplog.records)
    status, _, document = close_row(h)
    first = document["attempts"][0]
    assert status == "PENDING" and first["legacy"] and first["requested_at"] == h.now
    assert first["legacy_requested_at"] == h.now - 86_400_000 and first["client_order_id"] == cid
    # Still inside the window, counted from the first sighting: nothing is sent.
    with pytest.raises(ValueError, match="CLOSE_SUBMIT_OUTCOME_UNKNOWN"):
        reconcile(h, at(monkeypatch, h.now + ABSENT_RESOLUTION_MS - 1))
    assert not order_posts(h)
    # After it, the normal rule applies: authoritatively absent -> one bounded retry.
    reconcile(h, at(monkeypatch, h.now + WINDOW))
    status, _, document = close_row(h)
    assert status == "CLOSED" and state["qty"] == 0
    assert [a["outcome"] for a in document["attempts"]] == ["ABSENT", "FILLED"]
    assert [p.kwargs["params"]["newClientOrderId"] for p in order_posts(h)] == [cid + "-r1"]


def test_a_legacy_close_row_whose_order_exists_is_resolved_by_read_back(broker):
    h, state = broker
    identity = EMERGENCY_IDENTITY_PREFIX + "legacy-filled"
    cid = close_client_id(h.account["id"], identity, "ADAUSDT")
    state["orders"][cid] = {"symbol": "ADAUSDT", "orderId": 603, "status": "FILLED", "clientOrderId": cid,
                            "executedQty": "2"}
    with h.db.connect() as c:
        c.execute("CREATE TABLE IF NOT EXISTS cati_production_closes (account_id TEXT,client_id TEXT,symbol TEXT,"
                  "identity TEXT,status TEXT,document TEXT,PRIMARY KEY(account_id,client_id))")
        c.execute("INSERT INTO cati_production_closes VALUES(?,?,?,?,'PENDING',?)",
                  (h.account["id"], cid, "ADAUSDT", identity, json.dumps({"requested_at": h.now - 86_400_000})))
    reconcile(h, h.now)
    assert close_row(h)[0] == "CLOSED" and not order_posts(h)


# ── Native protection legs ───────────────────────────────────────────────────

@pytest.fixture
def leg_client(live, monkeypatch):
    from app.models.unified_trading import ProtectionRequest, Side
    monkeypatch.setattr(config, "settings", profile(True))
    clock = [live.now]
    monkeypatch.setattr(production_protection.time, "time", lambda: clock[0] / 1000)
    assert alerts(live.db) == []                            # the alert store exists and is empty
    book = []
    client = Mock(broker_environment="LIVE", _production_db=live.db, _production_account_id=live.plan.broker_account_id,
                  _production_intent_identity="protection-retry-intent")
    client.get_instrument.return_value = SimpleNamespace(tick_size=.01)
    client.get_algo_orders.side_effect = lambda *a, **k: list(book)
    client._production_protection_readback_sleep = lambda seconds: None

    def create(path, *, params):
        leg = {**params, "algoId": str(len(book) + 1)}
        book.append(leg)
        return leg

    request = ProtectionRequest(symbol="ADAUSDT", position_side=Side.BUY, qty=1, sl_price="98", tp_price="105")
    return SimpleNamespace(client=client, book=book, clock=clock, create=create, request=request, db=live.db)


def leg_ids(c):
    return [call.kwargs["params"]["clientAlgoId"] for call in c.client._signed_post.call_args_list]


def test_definitively_rejected_protection_leg_is_retried_with_a_new_client_id(leg_client):
    c = leg_client
    calls = []

    def first_rejected(path, *, params):
        calls.append(dict(params))
        if len(calls) == 1:
            raise RuntimeError('Binance HTTP 400: {"code":-2021,"msg":"Order would immediately trigger."}')
        return c.create(path, params=params)

    c.client._signed_post.side_effect = first_rejected
    with pytest.raises(RuntimeError, match="-2021"):
        place_native_protection(c.client, c.request)
    assert place_native_protection(c.client, c.request).status == "success"
    ids = leg_ids(c)
    assert len(ids) == 3 and ids[1] == ids[0] + "-r1" and all(len(i) <= 36 for i in ids)
    # Every leg -- first try or retry -- can only close the position.
    assert all(p["closePosition"] == "true" and "quantity" not in p and "_requested_at" not in p for p in calls)
    assert place_native_protection(c.client, c.request).status == "success"
    assert c.client._signed_post.call_count == 3


def test_a_leg_timeout_while_handling_a_venue_error_stays_unknown(leg_client):
    c = leg_client
    c.client._signed_post.side_effect = TimeoutError("read timed out")
    # e.g. under maintain_only: the whole maintenance runs inside an ``except`` block.
    try:
        raise RuntimeError(REJECTED)
    except RuntimeError:
        with pytest.raises(TimeoutError) as failure:
            place_native_protection(c.client, c.request)
    assert "-4164" in str(failure.value.__context__)
    with c.db.connect() as conn:
        assert [r[0] for r in conn.execute("SELECT response FROM cati_production_protection")] == [None]
    with pytest.raises(ValueError, match="PROTECTION_SUBMIT_OUTCOME_UNKNOWN"):
        place_native_protection(c.client, c.request)        # read back only: no immediate -r1
    assert c.client._signed_post.call_count == 1


def test_unknown_protection_leg_waits_for_the_window_then_retries_bounded(leg_client):
    c = leg_client
    c.client._signed_post.side_effect = TimeoutError()
    with pytest.raises(TimeoutError):
        place_native_protection(c.client, c.request)
    with pytest.raises(ValueError, match="PROTECTION_SUBMIT_OUTCOME_UNKNOWN"):
        place_native_protection(c.client, c.request)        # inside the window: read back only
    assert c.client._signed_post.call_count == 1
    for attempt in range(1, MAX_PROTECTION_ATTEMPTS):
        c.clock[0] += WINDOW
        with pytest.raises(TimeoutError):
            place_native_protection(c.client, c.request)
        assert c.client._signed_post.call_count == attempt + 1
    c.clock[0] += WINDOW
    with pytest.raises(ValueError, match="PROTECTION_SUBMIT_OUTCOME_UNKNOWN"):
        place_native_protection(c.client, c.request)        # exhausted: fail closed + alert
    assert c.client._signed_post.call_count == MAX_PROTECTION_ATTEMPTS
    ids = leg_ids(c)
    assert ids == [ids[0], ids[0] + "-r1", ids[0] + "-r2"]
    assert alerts(c.db) == [(NAKED_POSITION_OPERATOR_REQUIRED, "CRITICAL", "ADAUSDT")]


def test_acknowledged_protection_leg_that_vanished_is_never_recreated(leg_client):
    c = leg_client
    c.client._signed_post.side_effect = c.create
    assert place_native_protection(c.client, c.request).status == "success"
    c.book.clear()                                          # triggered or cancelled at the venue
    c.clock[0] += 10 * WINDOW
    with pytest.raises(ValueError, match="PROTECTION_SUBMIT_OUTCOME_UNKNOWN"):
        place_native_protection(c.client, c.request)
    assert c.client._signed_post.call_count == 2


def test_flat_cleanup_resolves_an_unanswered_leg_only_after_the_window(leg_client):
    from app.execution.production_protection import cancel_flat_protection
    c = leg_client
    c.client._signed_post.side_effect = TimeoutError()
    with pytest.raises(TimeoutError):
        place_native_protection(c.client, c.request)
    c.client.get_position_amt.return_value = 0.
    with pytest.raises(ValueError, match="PROTECTION_SUBMIT_OUTCOME_UNKNOWN"):
        cancel_flat_protection(c.client, "protection-retry-intent")
    c.clock[0] += WINDOW
    assert len(cancel_flat_protection(c.client, "protection-retry-intent")) == 1
    c.client._signed_delete.assert_not_called()             # nothing existed to cancel
    with c.db.connect() as conn:
        response = json.loads(conn.execute("SELECT response FROM cati_production_protection WHERE account_id=?",
                                           (c.client._production_account_id,)).fetchone()[0])
    assert response["_absent"] and response["_closed_flat"]


# ── Maintenance runs first, and independently of entry preconditions ────────

def test_existing_position_is_maintained_even_when_account_risk_fails(fresh, monkeypatch):
    assert "boundary" in process(fresh)                     # a real entry: lineage now exists
    maintained = Mock(return_value=[])
    monkeypatch.setattr(production, "reconcile_executions", maintained)
    monkeypatch.setattr(production, "account_risk", Mock(side_effect=ValueError("BROKER_ACCOUNT_RISK_UNAVAILABLE")))
    with pytest.raises(ValueError, match="BROKER_ACCOUNT_RISK_UNAVAILABLE"):
        process(fresh)
    maintained.assert_called_once()
    assert fresh.client.place_order.call_count == 1


def test_existing_position_is_maintained_when_its_bot_is_paused(fresh, monkeypatch):
    assert "boundary" in process(fresh)
    with fresh.db.connect() as c:
        c.execute("UPDATE bot_instances SET status='paused' WHERE id=?", (fresh.plan.bot_instance_id,))
    maintained = Mock(return_value=[])
    monkeypatch.setattr(production, "reconcile_executions", maintained)
    result = process(fresh)
    maintained.assert_called_once()
    # Entry evaluation is gated exactly as before: no active bot, no entry.
    assert result["reason"] == "AUTO_TRADING_DISABLED" and "boundary" not in result
    assert fresh.client.place_order.call_count == 1


def test_an_account_without_lineage_builds_nothing_before_its_entry_gates(fresh, monkeypatch):
    factory = Mock(side_effect=fresh.boundary_for)
    monkeypatch.setattr(production, "account_risk", Mock(side_effect=ValueError("BROKER_ACCOUNT_RISK_UNAVAILABLE")))
    with pytest.raises(ValueError, match="BROKER_ACCOUNT_RISK_UNAVAILABLE"):
        production.process_account(fresh.db, fresh.account, fresh.client, {"positions": [], "orders": []},
                                   now_ms=fresh.now, boundary_factory=factory)
    factory.assert_not_called()
    fresh.client.place_order.assert_not_called()


def test_a_maintenance_failure_fails_the_account_closed(fresh, monkeypatch):
    assert "boundary" in process(fresh)
    monkeypatch.setattr(production, "reconcile_executions",
                        Mock(side_effect=ValueError("CLOSE_SUBMIT_OUTCOME_UNKNOWN")))
    with pytest.raises(ValueError, match="CLOSE_SUBMIT_OUTCOME_UNKNOWN") as failure:
        process(fresh)
    evaluation = failure.value.production_evaluation
    assert evaluation["reason"] == "CLOSE_SUBMIT_OUTCOME_UNKNOWN" and "boundary" not in evaluation
    assert fresh.client.place_order.call_count == 1


# ── Emergency flatten uses the same close path ───────────────────────────────

def flatten(h, request_id="req-1"):
    return production.flatten_account(h.db, h.account, h.client, request_id=request_id, now_ms=h.now,
                                      boundary_factory=h.boundary_for)


def test_flatten_closes_through_the_fail_safe_path_and_cancels_protection(broker):
    h, state = broker
    report = cert.run(h.db, h.account["id"], "flatten-lineage", action="hold")
    assert state["qty"] > 0 and len(state["legs"]) == 2
    results = flatten(h)
    assert [(r["account_id"], r["symbol"], r["status"]) for r in results] == [(h.account["id"], "ADAUSDT", "closed")]
    assert state["qty"] == 0 and not state["legs"] and state["create_count"] == 1
    status, identity, _ = close_row(h)
    # The position's own lineage: this IS the fail-safe close, not a second mechanism.
    assert status == "CLOSED" and identity.startswith(report["trade_plan_id"] + "|")
    assert [p.kwargs["params"]["reduceOnly"] for p in order_posts(h)] == ["true"]
    with h.db.connect() as c:
        assert c.execute("SELECT status FROM cati_execution_attempts WHERE trade_plan_id=? ORDER BY sequence DESC LIMIT 1",
                         (report["trade_plan_id"],)).fetchone()[0] == "POSITION_CLOSED"
    assert [r["status"] for r in flatten(h, "req-2")] == ["no_position"]
    assert len(order_posts(h)) == 1


def test_flatten_closes_a_position_without_lineage_reduce_only(broker):
    h, state = broker
    state["qty"] = 2.
    results = flatten(h, "req-manual")
    assert [r["status"] for r in results] == ["closed"] and state["qty"] == 0
    status, identity, document = close_row(h)
    assert status == "CLOSED" and identity == EMERGENCY_IDENTITY_PREFIX + "req-manual"
    assert document["request"]["reduceOnly"] == "true" and document["request"]["side"] == "SELL"
    assert state["create_count"] == 0


def test_flatten_never_bypasses_the_order_gate_or_the_runtime_lease(broker, monkeypatch):
    h, state = broker
    state["qty"] = 2.
    monkeypatch.setattr(config, "settings", demo_profile(demo=False))
    results = flatten(h)
    assert [(r["status"], r["detail"]) for r in results] == [("failed", "DEMO_ORDER_SUBMISSION_DISABLED")]
    monkeypatch.setattr(config, "settings", demo_profile())
    monkeypatch.setattr(production, "owner_current", lambda db: False)
    with pytest.raises(ValueError, match="CANONICAL_RUNTIME_LEASE_REQUIRED"):
        flatten(h)
    assert state["qty"] == 2. and not order_posts(h)


def test_flatten_reports_an_unknown_close_as_failed_and_the_runtime_resolves_it(broker, monkeypatch):
    h, state = broker
    state["qty"] = 2.
    working = h.client._signed_post.side_effect
    h.client._signed_post.side_effect = TimeoutError("unknown outcome")
    results = flatten(h, "req-unknown")
    assert [(r["status"], r["detail"]) for r in results] == [("failed", "TimeoutError:CLOSE_SUBMIT_OUTCOME_UNKNOWN")]
    # A second request while that close may still be live continues the SAME intent
    # (read-back only): it never stacks another close on top of it.
    again = flatten(h, "req-unknown-2")
    assert [(r["status"], r["detail"]) for r in again] == [
        ("failed", "CLOSE_SUBMIT_OUTCOME_UNKNOWN:PRIOR_ATTEMPT_OUTCOME_UNKNOWN")]
    assert len(order_posts(h)) == 1 and list(close_rows(h)) == [EMERGENCY_IDENTITY_PREFIX + "req-unknown"]
    # The emergency intent is durable: the normal cycle resolves it, bounded as any close.
    later = h.now + WINDOW
    monkeypatch.setattr(cert.time, "time", lambda: later / 1000)
    h.client._signed_post.side_effect = working
    reconcile(h, later)
    assert close_row(h)[0] == "CLOSED" and state["qty"] == 0 and len(order_posts(h)) == 2


def test_flatten_is_not_blocked_by_an_exhausted_automatic_close(broker, monkeypatch):
    h, state = broker
    cert.run(h.db, h.account["id"], "flatten-exhausted", action="hold")
    working = h.client._signed_post.side_effect
    h.client._signed_post.side_effect = TimeoutError("unknown outcome")
    with pytest.raises(TimeoutError):
        cert.run(h.db, h.account["id"], "flatten-exhausted", action="close")
    now = h.now
    for expected in [TimeoutError] * (MAX_CLOSE_ATTEMPTS - 1) + [ValueError]:
        now += WINDOW
        monkeypatch.setattr(cert.time, "time", lambda now=now: now / 1000)
        with pytest.raises(expected):
            reconcile(h, now)
    assert len(order_posts(h)) == MAX_CLOSE_ATTEMPTS and state["qty"] > 0

    # Every automatic attempt is proven absent: the operator's request gets its own intent.
    h.client._signed_post.side_effect = working
    results = production.flatten_account(h.db, h.account, h.client, request_id="req-operator", now_ms=now,
                                         boundary_factory=h.boundary_for)
    assert [r["status"] for r in results] == ["closed"]
    assert state["qty"] == 0 and not state["legs"] and state["create_count"] == 1
    posts = order_posts(h)
    assert len(posts) == MAX_CLOSE_ATTEMPTS + 1 and all(p.kwargs["params"]["reduceOnly"] == "true" for p in posts)
    with h.db.connect() as c:
        rows = {r["identity"].split("|")[0]: r["status"] for r in c.execute("SELECT * FROM cati_production_closes")}
    assert rows.pop("EMERGENCY") == "CLOSED" and list(rows.values()) == ["CLOSED"]   # the exhausted intent retired


def test_flatten_reports_submitted_only_for_an_order_this_request_sent(broker):
    h, state = broker
    cert.run(h.db, h.account["id"], "flatten-partial", action="hold")
    working = h.client._signed_post.side_effect

    def partial(path, params):
        if path.endswith("algoOrder"):
            return working(path, params)
        state["orders"][params["newClientOrderId"]] = {"symbol": "ADAUSDT", "orderId": 610, "status": "FILLED",
                                                       "clientOrderId": params["newClientOrderId"], "executedQty": "1"}
        state["qty"] = 1.                                  # filled, but the position is not flat
        return {"orderId": 610}

    h.client._signed_post.side_effect = partial
    first = flatten(h, "req-a")
    # THIS request sent the order and the venue acknowledged it.
    assert [r["status"] for r in first] == ["submitted"]
    assert first[0]["detail"].startswith(production.SUBMITTED_DETAIL_PREFIX) and len(order_posts(h)) == 1
    # The same state seen by the NEXT request is not "submitted": that request sent nothing
    # for the lineage. Its filled order is terminal, so the operator gets the emergency intent.
    h.client._signed_post.side_effect = working
    second = flatten(h, "req-b")
    assert [r["status"] for r in second] == ["closed"] and state["qty"] == 0
    rows = close_rows(h)
    assert rows[EMERGENCY_IDENTITY_PREFIX + "req-b"][0] == "CLOSED" and len(rows) == 2
    posts = order_posts(h)
    assert len(posts) == 2 and all(p.kwargs["params"]["reduceOnly"] == "true" for p in posts)
    assert state["create_count"] == 1


def test_flatten_never_stacks_a_close_on_one_that_may_still_be_working(broker):
    h, state = broker
    cert.run(h.db, h.account["id"], "flatten-working", action="hold")

    def resting(path, params):
        state["orders"][params["newClientOrderId"]] = {"symbol": "ADAUSDT", "orderId": 611, "status": "NEW",
                                                       "clientOrderId": params["newClientOrderId"], "executedQty": "0"}
        return {"orderId": 611}

    h.client._signed_post.side_effect = resting
    assert [r["status"] for r in flatten(h, "req-a")] == ["submitted"]
    results = flatten(h, "req-b")
    assert [(r["status"], r["detail"]) for r in results] == [
        ("failed", "CLOSE_FILL_OR_FLAT_UNCONFIRMED:PRIOR_CLOSE_ORDER_STILL_WORKING")]
    assert len(order_posts(h)) == 1 and len(close_rows(h)) == 1


def test_flatten_does_not_wait_out_the_automatic_closes_retry_backoff(broker):
    h, state = broker
    cert.run(h.db, h.account["id"], "flatten-backoff", action="hold")
    working = h.client._signed_post.side_effect
    h.client._signed_post.side_effect = lambda path, params: (_ for _ in ()).throw(RuntimeError(SKEWED))
    with pytest.raises(RuntimeError, match="-1021"):
        cert.run(h.db, h.account["id"], "flatten-backoff", action="close")
    h.client._signed_post.side_effect = working
    # The lineage's last attempt was refused by the venue (it does not exist): the operator's
    # request goes ahead under the emergency identity instead of waiting 30 s.
    assert [r["status"] for r in flatten(h, "req-now")] == ["closed"]
    assert state["qty"] == 0 and len(order_posts(h)) == 2
    assert {status for status, _ in close_rows(h).values()} == {"CLOSED"}


def test_one_unreadable_plan_row_does_not_fail_the_accounts_flatten(broker, monkeypatch):
    from app.trading_intelligence.trade_plan.evidence_store import TradePlanEvidenceStore
    h, state = broker
    cert.run(h.db, h.account["id"], "flatten-corrupt", action="hold")
    monkeypatch.setattr(TradePlanEvidenceStore, "load_plan", Mock(side_effect=ValueError("corrupt plan row")))
    results = flatten(h, "req-corrupt")
    assert [r["status"] for r in results] == ["closed"] and state["qty"] == 0
    # Closed under the emergency identity; the later protection clean-up reports its own failure.
    assert list(close_rows(h)) == [EMERGENCY_IDENTITY_PREFIX + "req-corrupt"]
    assert "PROTECTION_CLEANUP_PENDING" in results[0]["detail"]
    assert [p.kwargs["params"]["reduceOnly"] for p in order_posts(h)] == ["true"]


# ── Entries: a definitive rejection is REJECTED, an ambiguous one never retried ─

def test_definitive_venue_rejection_of_an_entry_is_rejected_not_unknown(demo):
    demo.client.place_order.side_effect = RuntimeError(REJECTED)
    b = demo.demo_boundary()
    out = demo.run(b, atr=1.)
    assert out.status == BoundaryStatus.EXECUTION_REJECTED, out
    assert out.attempt.status == X.REJECTED.value and "VENUE_REJECTED_DEFINITIVE" in out.reason_codes
    assert out.reservation_status == "RELEASED"
    # Terminal: the same plan is never submitted again.
    assert demo.run(b, atr=1.).status == BoundaryStatus.DUPLICATE_PLAN
    assert demo.client.place_order.call_count == 1


@pytest.mark.parametrize("failure", [
    RuntimeError('Binance HTTP 503: {"code":-1001,"msg":"Internal error."}'),
    RuntimeError('Binance HTTP 400: {"code":-1007,"msg":"Backend did not respond in time."}'),
    RuntimeError("Binance HTTP 400: <html>bad gateway</html>"),
    ConnectionError("connection reset by peer"),
    # "Duplicate client order id": refused, but an order with that id may exist.
    RuntimeError('Binance HTTP 400: {"code":-4116,"msg":"ClientOrderId is duplicated."}'),
    RuntimeError('Binance HTTP 400: {"code":-2010,"msg":"Duplicate order sent."}'),
])
def test_ambiguous_entry_failure_stays_unknown_and_is_never_resubmitted(demo, failure):
    demo.client.place_order.side_effect = failure
    b = demo.demo_boundary()
    out = demo.run(b, atr=1.)
    assert out.status == BoundaryStatus.SUBMIT_UNKNOWN, out
    assert demo.run(b, atr=1.).status == BoundaryStatus.DUPLICATE_PLAN
    assert demo.client.place_order.call_count == 1


def test_unknown_entry_resolves_to_no_position_only_on_authoritative_absence(demo):
    from app.execution.entry_protection import get_entry_protection
    demo.client.place_order.side_effect = TimeoutError("timeout")
    b = demo.demo_boundary()
    out = demo.run(b, atr=1.)
    assert out.status == BoundaryStatus.SUBMIT_UNKNOWN
    cid = out.attempt.client_order_id
    # The window is measured from when the submit RETURNED (never before the real send),
    # not from the start of the cycle.
    sent = out.attempt.submitted_at
    assert demo.now <= sent < demo.now + 60_000
    b.adapter.query_order = Mock(return_value=OrderState(None, cid, None, 0., 0., False))
    flat = BrokerPositionState("ADAUSDT", "FLAT", 0., None, True)
    b.adapter.reconcile_position = Mock(return_value=flat)
    b.adapter.order_absent = Mock(return_value=True)

    # Too early: the request could still arrive. The venue is not even asked.
    assert b.recover_pending(now_ms=sent + ENTRY_ABSENT_MIN_MS - 1)[0].status == BoundaryStatus.STILL_UNKNOWN
    b.adapter.order_absent.assert_not_called()
    # A position exists: never "no position", whatever the order lookup says.
    b.adapter.reconcile_position.return_value = BrokerPositionState("ADAUSDT", "LONG", 1., 100., True)
    assert b.recover_pending(now_ms=sent + ENTRY_ABSENT_MIN_MS)[0].status == BoundaryStatus.STILL_UNKNOWN
    b.adapter.order_absent.assert_not_called()
    # A lookup that does not authoritatively say "absent" changes nothing.
    b.adapter.reconcile_position.return_value = flat
    b.adapter.order_absent.return_value = False
    assert b.recover_pending(now_ms=sent + ENTRY_ABSENT_MIN_MS)[0].status == BoundaryStatus.STILL_UNKNOWN

    b.adapter.order_absent.return_value = True
    resolved = b.recover_pending(now_ms=sent + ENTRY_ABSENT_MIN_MS)[0]
    assert resolved.status == BoundaryStatus.RECONCILED and resolved.attempt.status == X.RECONCILED_NO_POSITION.value
    assert resolved.reservation_status == "RELEASED"
    b.adapter.order_absent.assert_called_with("ADAUSDT", cid)
    assert get_entry_protection(demo.db).get_entry(demo.plan.bot_instance_id, "ADAUSDT", "LONG") is None
    # Resolved means terminal -- not "free to send again".
    assert demo.run(b, atr=1.).status == BoundaryStatus.DUPLICATE_PLAN
    assert demo.client.place_order.call_count == 1


# ── Stale-data check fails closed in the production profile ─────────────────

class _ProductionView:
    """The executor's own settings, differing only in the production flag."""
    production = True

    def __init__(self, base):
        self._base = base

    def __getattr__(self, name):
        return getattr(self._base, name)


@pytest.mark.parametrize("klines", ["raises", "none"])
def test_unverifiable_market_data_freshness_rejects_the_entry_in_production(tmp_path, monkeypatch, klines):
    from app.execution import executor
    h = Harness(tmp_path)
    monkeypatch.setattr(executor, "settings", _ProductionView(executor.settings))
    if klines == "raises":
        h.client.get_klines.side_effect = RuntimeError("klines unavailable")
    else:
        h.client.get_klines.side_effect = lambda **kw: None
    out = h.run()
    assert out.status == BoundaryStatus.EXECUTION_REJECTED, out
    assert out.reason_codes == ("STALE_DATA_UNVERIFIABLE",) and out.attempt.status == X.REJECTED.value
    h.client.place_order.assert_not_called()
    assert h.reservation_status() == "RELEASED"
