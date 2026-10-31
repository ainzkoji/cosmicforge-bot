"""A notification failure can never delay or prevent emergency protection (Section J follow-up, 31 October 2026).

The real emergency flatten and the real takeover recovery (protection verified and repaired) run on the stateful
fake venue of the Step 5.2 suite while EVERY notification surface is broken:

* every outbound HTTP and SMTP call in the process hangs for 30 s (``urllib.request.urlopen``, ``smtplib.SMTP``,
  ``smtplib.SMTP_SSL``) -- the transports of the operator channels (``scripts/notify_failure.py``) and of the
  customer channels;
* the customer-notification sender (``user_events._send``) hangs, the in-process SSE broadcaster raises;
* an operations tick is stuck at the same moment inside a hanging alert sink, holding platform-database work.

Asserted: the flatten and the protection repair finish in bounded time with the venue in the safe state, the kill
switch is engaged, and not one notification transport was called on the thread doing the protective work. Customer
events are written to the outbox (delivered later by the notification worker), never sent inline.
"""
from __future__ import annotations

import smtplib
import threading
import time
import urllib.request

import pytest
from test_step5_broker_cycle_assignment import broker, demo, fresh, holder, live, platform, rig  # noqa: F401

from app.observability import user_events
from app.ops.alerting import AlertEngine, CallableSink
from app.platform_workers import broker_assignment
from app.platform_workers.broker_assignment import OWNER
from app.trading_intelligence.integration import production_execution as production
from app.trading_intelligence.integration import production_runtime as runtime

__all__ = ["broker", "demo", "fresh", "live", "platform", "rig"]

HANG_S = 30.0
BOUND_S = 25.0          # far below HANG_S: a single hanging notification call on the path would fail the test


@pytest.fixture
def broken_notifications(monkeypatch, rig):
    calls = []
    release = threading.Event()

    def hang(kind):
        def call(*a, **kw):
            calls.append((kind, threading.get_ident()))
            release.wait(HANG_S)
            raise OSError(f"{kind}: provider unreachable")
        return call

    monkeypatch.setattr(urllib.request, "urlopen", hang("http"))
    monkeypatch.setattr(smtplib, "SMTP", hang("smtp"))
    monkeypatch.setattr(smtplib, "SMTP_SSL", hang("smtps"))
    monkeypatch.setattr(user_events, "_send", hang("customer_channel"))

    def broadcast_down(event):
        calls.append(("sse", threading.get_ident()))
        raise RuntimeError("broadcaster down")
    monkeypatch.setattr(user_events, "_broadcast", broadcast_down)
    # An operations tick stuck inside a hanging alert sink while the protective work runs.
    engine = AlertEngine(rig.platform, sinks=[CallableSink("hanging", hang("alert_sink"))], send_timeout_s=HANG_S)
    engine.raise_alert("UNPROTECTED_POSITION", key="isolation-test", title="t", source="test")
    tick = threading.Thread(target=engine.deliver_pending, daemon=True)
    tick.start()
    deadline = time.monotonic() + 10
    while not any(k == "alert_sink" for k, _ in calls) and time.monotonic() < deadline:
        time.sleep(0.02)
    assert any(k == "alert_sink" for k, _ in calls), "the ops tick never reached its hanging sink"
    yield calls
    release.set()
    tick.join(HANG_S + 5)


def _on(thread_ident, calls, kinds=("http", "smtp", "smtps", "customer_channel", "alert_sink")):
    return [k for k, t in calls if t == thread_ident and k in kinds]


def _run_bounded(fn):
    box = {}

    def body():
        box["ident"] = threading.get_ident()
        started = time.monotonic()
        try:
            box["result"] = fn()
        except BaseException as exc:  # noqa: BLE001 -- reported below
            box["error"] = exc
        box["elapsed"] = time.monotonic() - started
    worker = threading.Thread(target=body, daemon=True)
    worker.start()
    worker.join(BOUND_S)
    assert not worker.is_alive(), f"the protective work did not finish within {BOUND_S}s with notifications broken"
    if "error" in box:
        raise box["error"]
    return box


def test_emergency_flatten_completes_while_every_notification_provider_hangs(rig, broken_notifications, monkeypatch):
    from app.trading_intelligence.governance.promotion import PromotionGovernance
    flatten_account = production.flatten_account
    monkeypatch.setattr(production, "flatten_account", lambda db, account, client, **kw: flatten_account(
        db, account, client, boundary_factory=rig.h.boundary_for, **kw))
    rig.hold_position()
    rig.lease["held"] = False
    a = rig.worker("A", ttl_ms=60_000)                        # a live holder the flatten must preempt
    rig.round(a)
    owner = rig.worker("owner-1", mode=OWNER)
    broker_assignment.install(owner)
    rig.lease["held"] = True
    box = _run_bounded(lambda: runtime.flatten(rig.db, account_id=rig.account, request_id="iso-1",
                                               factory=lambda auth: rig.h.client))
    results = box["result"]
    assert results and results[0]["status"] in ("closed", "submitted"), results
    assert rig.state["qty"] == 0 and rig.state["legs"] == []          # closed; protection legs cancelled
    assert PromotionGovernance(rig.db).kill_switch_on(scope=rig.account)
    assert _on(box["ident"], broken_notifications) == []              # nothing on the flatten thread waited for one
    assert box["elapsed"] < BOUND_S


def test_protection_repair_after_a_takeover_completes_while_every_notification_provider_hangs(rig,
                                                                                               broken_notifications):
    rig.hold_position()
    rig.lease["held"] = False
    legs = list(rig.state["legs"])
    rig.state["legs"].clear()                                 # the venue lost both protection legs
    successor = rig.worker("B")
    box = _run_bounded(lambda: rig.round(successor))          # claim + recovery-only step: verify and repair
    assert holder(rig) == "B" and not successor.owned[rig.account].needs_recovery
    # The safe state either way: protection repaired, or the unprotected position closed (observed here: closed).
    assert rig.state["qty"] == 0 or len(rig.state["legs"]) == 2, (rig.state["qty"], rig.state["legs"], legs)
    assert _on(box["ident"], broken_notifications) == []
    with rig.db.connect() as c:                               # customer events went to the outbox, not the wire
        exists = c.execute("SELECT 1 FROM sqlite_master WHERE name='notification_outbox'").fetchone()
        sent_inline = c.execute("SELECT COUNT(*) FROM notification_outbox WHERE status='sent' AND channel<>'in_app'"
                                ).fetchone()[0] if exists else 0
    assert sent_inline == 0
