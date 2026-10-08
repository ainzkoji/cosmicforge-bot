"""Step 1.0a -- a failed read of the stop legs never closes a protected position.

Protection verification runs every maintenance cycle for every open position.
Its outcome is one of three states (``app.execution.protection_state``):

* CONFIRMED: both closePosition legs read back; normal maintenance continues.
* ABSENT:    the venue PROVED the stop is gone (two successful reads, a
             definitive refusal, exhausted attempts, wrong geometry); the
             existing durable reduce-only fail-safe close runs.
* UNKNOWN:   the venue did not answer (timeout, reset, 5xx, rate limit,
             malformed body) or a CREATE is still ambiguous; the position is
             preserved, the doubt is recorded, the next cycle retries, the
             operator is alerted after a bounded number of cycles, and the
             account evaluates no new entry.

Everything here runs the real maintenance entry point (``reconcile_executions``)
against the certification broker fake after a real certification fill.
"""
import json
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from test_demo_boundary_certification import broker
from test_production_position_safety import alerts, order_posts
from test_production_execution import live, profile
from test_production_demo_execution import demo, demo_profile
from test_production_research_separation import fresh
from app.core import config
from app.execution import demo_boundary_certification as cert
from app.execution import production_protection, protection_state
from app.execution.production_close import NAKED_POSITION_OPERATOR_REQUIRED
from app.execution.production_protection import place_native_protection
from app.trading_intelligence.integration import production_execution as production
from app.trading_intelligence.integration import production_runtime as runtime

__all__ = ["broker", "live", "demo", "fresh"]

TIMEOUT = TimeoutError("read timed out")
RESET = ConnectionResetError(104, "connection reset by peer")
SERVER_ERROR = RuntimeError('Binance HTTP 503: {"code":-1001,"msg":"Internal error; unable to process your request."}')
RATE_LIMITED = RuntimeError('Binance HTTP 429: {"code":-1003,"msg":"Too many requests; current limit is 2400 request weight per 1 minute."}')
BANNED = RuntimeError('Binance HTTP 418: {"code":-1003,"msg":"Way too much request weight used; IP banned."}')


class _Malformed(Exception):
    """A body without the order list (the Mock returns it instead of raising)."""


def protected(broker, run_id="read-safety"):
    h, state = broker
    report = cert.run(h.db, h.account["id"], run_id, action="hold")
    assert report["status"] == "PROTECTED" and state["qty"] > 0 and len(state["legs"]) == 2
    return h, state, report


def reconcile(h, now=None):
    return production.reconcile_executions(h.db, h.boundary_for(), h.client, h.now if now is None else now)


def uncertainty(h):
    return protection_state.uncertain_positions(h.db, h.account["id"])


def uncertainty_alerts(db):
    with db.connect() as c:
        return [tuple(r) for r in c.execute("SELECT alert_type, severity, symbol FROM alerts WHERE alert_type=?",
                                            (protection_state.UNCERTAINTY_ALERT,))]


def fail_reads(h, failure):
    if isinstance(failure, _Malformed):
        h.client.get_algo_orders.side_effect = lambda *a, **k: {"code": 200, "msg": "ok"}   # no order list
    else:
        h.client.get_algo_orders.side_effect = failure


def restore_reads(h, state):
    h.client.get_algo_orders.side_effect = lambda *a, **kw: list(state["legs"])


# ── A failed read is UNKNOWN: the position is kept ──────────────────────────

@pytest.mark.parametrize("failure", [TIMEOUT, RESET, SERVER_ERROR, RATE_LIMITED, BANNED, _Malformed()],
                         ids=["timeout", "connection-reset", "http-5xx", "rate-limit-429", "ban-418", "malformed-body"])
def test_a_failed_stop_read_keeps_the_protected_position_and_fails_the_account_closed(broker, failure):
    h, state, report = protected(broker)
    legs_before, posts_before = list(state["legs"]), len(order_posts(h))
    fail_reads(h, failure)
    with pytest.raises(ValueError, match=protection_state.STATE_UNKNOWN) as failed:
        reconcile(h)
    # The position is untouched: no close was sent, the legs are untouched, nothing was recreated.
    assert state["qty"] > 0 and state["legs"] == legs_before and len(order_posts(h)) == posts_before
    assert state["create_count"] == 1
    # The item was still recorded, with the uncertainty, before the account failed closed.
    history = failed.value.execution_history
    assert len(history) == 1 and history[0]["protection"]["state"] == protection_state.UNKNOWN
    assert history[0]["protection"]["reason"] == "PROTECTION_READ_UNAVAILABLE"
    assert "fail_safe_close" not in history[0]
    rows = uncertainty(h)
    assert [(r["symbol"], r["cycles"], r["reason"], r["trade_plan_id"]) for r in rows] == \
        [("ADAUSDT", 1, "PROTECTION_READ_UNAVAILABLE", report["trade_plan_id"])]
    assert uncertainty_alerts(h.db) == []                   # one cycle is not yet an operator matter


def test_a_failed_read_during_the_whole_account_cycle_evaluates_no_entry_and_keeps_the_position(broker, monkeypatch):
    """Through ``process_account`` (the runtime's call): maintenance fails closed,
    the evaluation records the reason, and no entry is evaluated."""
    h, state, _ = protected(broker)
    fail_reads(h, TIMEOUT)
    snapshot = {"positions": h.client.position_risk(), "orders": []}
    with pytest.raises(ValueError, match=protection_state.STATE_UNKNOWN) as failed:
        production.process_account(h.db, h.account, h.client, snapshot, now_ms=h.now, boundary_factory=h.boundary_for)
    assert state["qty"] > 0 and state["create_count"] == 1
    evaluation = failed.value.production_evaluation
    assert evaluation["reason"] == protection_state.STATE_UNKNOWN
    assert evaluation["execution_permission"] == "BLOCKED_ACCOUNT"


def test_confirmed_present_after_a_failed_read_clears_the_uncertainty_without_new_legs(broker):
    h, state, _ = protected(broker)
    fail_reads(h, TIMEOUT)
    with pytest.raises(ValueError, match=protection_state.STATE_UNKNOWN):
        reconcile(h)
    assert len(uncertainty(h)) == 1
    restore_reads(h, state)
    history = reconcile(h)
    assert history[0]["protection"]["state"] == protection_state.CONFIRMED
    assert history[0]["protection"]["status"] == "success"
    assert uncertainty(h) == [] and state["qty"] > 0 and len(state["legs"]) == 2
    assert h.client._signed_post.call_count == 2            # only the two original legs; nothing new


def test_repeated_transient_failures_alert_the_operator_once_after_the_bound_and_never_close(broker):
    h, state, _ = protected(broker)
    fail_reads(h, SERVER_ERROR)
    for cycle in range(1, protection_state.UNKNOWN_ALERT_CYCLES + 2):
        with pytest.raises(ValueError, match=protection_state.STATE_UNKNOWN):
            reconcile(h, h.now + cycle * 30_000)
        row = uncertainty(h)[0]
        assert row["cycles"] == cycle and row["first_seen_at"] == h.now + 30_000
        assert row["last_seen_at"] == h.now + cycle * 30_000
        expected = [(protection_state.UNCERTAINTY_ALERT, "CRITICAL", "ADAUSDT")] \
            if cycle >= protection_state.UNKNOWN_ALERT_CYCLES else []
        assert uncertainty_alerts(h.db) == expected         # exactly one alert, not one per cycle
    assert state["qty"] > 0 and len(order_posts(h)) == 0
    assert alerts(h.db) == []                               # not the "automatic close exhausted" alert


def test_a_process_restart_while_protection_is_unknown_resumes_the_verification(broker):
    from shared_lib.persistence.db import DB
    h, state, _ = protected(broker)
    fail_reads(h, TIMEOUT)
    with pytest.raises(ValueError, match=protection_state.STATE_UNKNOWN):
        reconcile(h)
    restarted = DB(h.db.path)                               # a new process: only durable state survives
    assert [r["cycles"] for r in protection_state.uncertain_positions(restarted, h.account["id"])] == [1]
    restore_reads(h, state)
    history = production.reconcile_executions(restarted, h.boundary_for(), h.client, h.now + 30_000)
    assert history[0]["protection"]["state"] == protection_state.CONFIRMED
    assert protection_state.uncertain_positions(restarted, h.account["id"]) == []
    assert state["qty"] > 0 and len(state["legs"]) == 2 and state["create_count"] == 1


def test_the_runtime_status_shows_the_unverified_protection(broker):
    h, state, _ = protected(broker)
    fail_reads(h, TIMEOUT)
    with pytest.raises(ValueError, match=protection_state.STATE_UNKNOWN):
        reconcile(h)
    status = runtime.status(h.db, user_id=h.account["user_id"])
    account = next(a for a in status["accounts"] if a["account_id"] == h.account["id"])
    assert [u["symbol"] for u in account["protection_uncertain"]] == ["ADAUSDT"]
    assert status["protection_uncertain_positions"] == 1
    restore_reads(h, state)
    reconcile(h)
    status = runtime.status(h.db, user_id=h.account["user_id"])
    assert status["protection_uncertain_positions"] == 0


# ── Confirmed absence still triggers the fail-safe close ────────────────────

def test_a_stop_the_venue_proves_gone_still_gets_the_fail_safe_close(broker):
    h, state, report = protected(broker)
    state["legs"].clear()                                   # cancelled or triggered at the venue
    history = reconcile(h, h.now + 30_000)
    item = history[0]
    assert item["protection"] == {"status": "UNCONFIRMED", "state": protection_state.ABSENT,
                                  "reason": "PROTECTION_CONFIRMED_ABSENT"}
    assert item["fail_safe_close"]["status"] == "CLOSED_POSITION"
    assert state["qty"] == 0 and state["create_count"] == 1
    assert [p.kwargs["params"]["reduceOnly"] for p in order_posts(h)] == ["true"]
    assert uncertainty(h) == []


def test_absence_is_confirmed_only_by_a_second_successful_read(broker):
    """One successful read that lacks an acknowledged leg is propagation, not
    proof: the confirming read finds it again and nothing is closed or created."""
    h, state, _ = protected(broker)
    legs = list(state["legs"])
    answers = iter([[], legs])                              # first read empty, second read complete
    h.client.get_algo_orders.side_effect = lambda *a, **k: list(next(answers, legs))
    history = reconcile(h, h.now + 30_000)
    assert history[0]["protection"]["state"] == protection_state.CONFIRMED
    assert state["qty"] > 0 and len(order_posts(h)) == 0 and state["create_count"] == 1
    assert h.client._signed_post.call_count == 2


def test_a_failed_read_after_the_uncertainty_was_recorded_still_closes_once_absence_is_proven(broker):
    h, state, _ = protected(broker)
    fail_reads(h, TIMEOUT)
    with pytest.raises(ValueError, match=protection_state.STATE_UNKNOWN):
        reconcile(h)
    state["legs"].clear()
    restore_reads(h, state)
    history = reconcile(h, h.now + 30_000)
    assert history[0]["protection"]["state"] == protection_state.ABSENT and state["qty"] == 0
    assert uncertainty(h) == []


# ── Emergency flatten is never blocked by uncertain protection ──────────────

def test_an_operator_flatten_closes_while_the_stop_read_keeps_failing(broker):
    h, state, _ = protected(broker)
    fail_reads(h, TIMEOUT)
    with pytest.raises(ValueError, match=protection_state.STATE_UNKNOWN):
        reconcile(h)
    results = production.flatten_account(h.db, h.account, h.client, request_id="req-unknown", now_ms=h.now,
                                         boundary_factory=h.boundary_for)
    assert [r["status"] for r in results] == ["closed"] and state["qty"] == 0
    # The orphaned legs could not be cleaned up yet: said so, not hidden.
    assert "PROTECTION_CLEANUP_PENDING" in results[0]["detail"]


# ── The classification itself ───────────────────────────────────────────────

@pytest.mark.parametrize("exc,state", [
    (TIMEOUT, "UNKNOWN"), (RESET, "UNKNOWN"), (SERVER_ERROR, "UNKNOWN"), (RATE_LIMITED, "UNKNOWN"), (BANNED, "UNKNOWN"),
    (ValueError("PROTECTION_READ_UNAVAILABLE"), "UNKNOWN"),
    (ValueError("PROTECTION_SUBMIT_OUTCOME_UNKNOWN"), "UNKNOWN"),
    (ValueError("PROTECTION_READ_BACK_UNCONFIRMED"), "UNKNOWN"),
    (ValueError("ALGO_ORDERS_RESPONSE_MALFORMED"), "UNKNOWN"),
    (ValueError("CANONICAL_RUNTIME_LEASE_REQUIRED"), "UNKNOWN"),
    (RuntimeError('Binance HTTP 400: {"code":-2021,"msg":"Order would immediately trigger."}'), "ABSENT"),
    (RuntimeError('Binance HTTP 400: {"code":-4164,"msg":"Order notional is too small."}'), "ABSENT"),
    (RuntimeError('Binance HTTP 400: {"code":-1021,"msg":"Timestamp outside of the recvWindow."}'), "UNKNOWN"),
    (ValueError("PROTECTION_CONFIRMED_ABSENT"), "ABSENT"),
    (ValueError("PROTECTION_LEG_ATTEMPTS_EXHAUSTED"), "ABSENT"),
    (ValueError("PROTECTION_READ_BACK_GEOMETRY_MISMATCH"), "ABSENT"),
])
def test_only_positive_venue_evidence_is_absence(exc, state):
    assert protection_state.failure_state(exc) == state


def test_the_binance_client_never_reads_a_malformed_body_as_an_empty_order_list(monkeypatch):
    from app.exchange.binance.client import BinanceFuturesClient
    client = BinanceFuturesClient.__new__(BinanceFuturesClient)
    monkeypatch.setattr(client, "_signed_get", lambda *a, **k: {"code": 200}, raising=False)
    with pytest.raises(ValueError, match="ALGO_ORDERS_RESPONSE_MALFORMED"):
        client.get_algo_orders("ADAUSDT", raise_on_error=True)
    assert client.get_algo_orders("ADAUSDT") == []          # legacy callers keep the lenient answer
    monkeypatch.setattr(client, "_signed_get", lambda *a, **k: {"algoOrders": []}, raising=False)
    assert client.get_algo_orders("ADAUSDT", raise_on_error=True) == []
    monkeypatch.setattr(client, "_signed_get", lambda *a, **k: [{"algoId": "1"}], raising=False)
    assert client.get_algo_orders("ADAUSDT", raise_on_error=True) == [{"algoId": "1"}]


def test_open_protection_legs_wraps_every_transport_failure_as_unavailable():
    client = Mock()
    for failure in (TIMEOUT, RESET, SERVER_ERROR, RATE_LIMITED):
        client.get_algo_orders.side_effect = failure
        with pytest.raises(ValueError, match="PROTECTION_READ_UNAVAILABLE") as err:
            production_protection.open_protection_legs(client, "ADAUSDT")
        assert err.value.__cause__ is failure
    client.get_algo_orders.side_effect = lambda *a, **k: {"unexpected": True}
    with pytest.raises(ValueError, match="PROTECTION_READ_UNAVAILABLE"):
        production_protection.open_protection_legs(client, "ADAUSDT")


# ── The recovered-fill path of the boundary uses the same rule ──────────────

def _recovered_fill(tmp_path, monkeypatch):
    """A SUBMIT_UNKNOWN attempt whose entry the broker then reports FILLED with
    the position open, under the production profile with LIVE submission on."""
    from test_pre_section22_closure import _unknown
    from _exec import _order
    h, b = _unknown(tmp_path)
    monkeypatch.setattr(config, "settings", demo_profile())      # the harness plan is DEMO
    h.client.broker_environment = h.plan.environment
    h.client.get_order.side_effect = lambda s, o: _order("FILLED", "6.0", "100.0")
    h.client.get_position_info.return_value = {"positionAmt": "6.0", "entryPrice": "100.0"}
    exits = []
    b.adapter.submit_exit = lambda *a, **k: exits.append((a, k)) or {"status": "CLOSED_POSITION"}
    return h, b, exits


def test_the_recovered_fill_path_keeps_the_position_when_the_stop_read_fails(tmp_path, monkeypatch):
    """``reconcile_submit_unknown``: a filled entry whose submit outcome was lost
    is recovered from the broker and its protection verified. A failed read
    there used to close the recovered position as well."""
    h, b, exits = _recovered_fill(tmp_path, monkeypatch)
    b.adapter.submit_protection = Mock(side_effect=TIMEOUT)
    with pytest.raises(TimeoutError):
        b.reconcile_submit_unknown(h.plan, now_ms=h.now + 1)
    assert exits == []
    rows = protection_state.uncertain_positions(h.db, h.plan.broker_account_id)
    assert [(r["symbol"], r["reason"], r["trade_plan_id"]) for r in rows] ==         [(h.plan.instrument_key.venue_symbol, "TimeoutError", h.plan.trade_plan_id)]
    assert h.reservation_status() == "RESOLUTION_PENDING"   # still owned: resolved on a later cycle


def test_the_recovered_fill_path_still_closes_on_proven_absence(tmp_path, monkeypatch):
    h, b, exits = _recovered_fill(tmp_path, monkeypatch)
    b.adapter.submit_protection = Mock(side_effect=ValueError("PROTECTION_CONFIRMED_ABSENT"))
    with pytest.raises(ValueError, match="PROTECTION_CONFIRMED_ABSENT"):
        b.reconcile_submit_unknown(h.plan, now_ms=h.now + 1)
    assert len(exits) == 1 and exits[0][1]["quantity"] == 6.0
    assert protection_state.uncertain_positions(h.db, h.plan.broker_account_id) == []
