"""Alert delivery reliability (master plan Section J follow-up, 31 October 2026) -- on SQLite and PostgreSQL.

What a notification failure may and may not do. It may delay a human hearing of a problem; it must never lose the
alert, count a rejected message as delivered, stop the other evaluators, or stall the tick. (That it can never
delay emergency protection is ``tests/trading_intelligence/test_notification_isolation.py``.)

Covered here: a producer failure is isolated; duplicate signals from two processes make one alert and one delivery
per channel; a missing provider leaves the alert recorded, the delivery pending and both visible; a database outage
while recording fails that tick only; pending deliveries resume after a process restart (including one claimed by a
process that died before sending); escalation and recovery; a provider that answers but accepts nothing is not a
delivery; deliveries that keep failing raise an alert before they are abandoned; the dead-man check of the tick and
the tick's own gap alert; on-call stays unassigned (no person is invented).
"""
from __future__ import annotations

import json
import threading
import time

import pytest

from app.ops import alerting
from app.ops.alerting import (AlertEngine, CallableSink, DeliveryReceipt, ProviderRejected, check_tick,
                              evaluate_alert_delivery, run_cycle, run_tick)
from shared_lib.operations.alerting import DELIVERY_CLAIM_MS, interpret_send_result
from shared_lib.operations.oncall import OPERATIONS_OWNER_REQUIRED, ROTATIONS, OncallDirectory
from shared_lib.operations.tick_lease import TickLease
from shared_lib.persistence.sql import DatabaseUnavailable

T0 = 1_800_000_000_000
MIN = 60_000


class Recorder:
    def __init__(self, name="recorder", fail_times=0, result=None):
        self.name, self.fail_times, self.result, self.sent = name, fail_times, result, []

    def send(self, message):
        if self.fail_times > 0:
            self.fail_times -= 1
            raise RuntimeError("channel down")
        self.sent.append(message)
        return self.result


def staffed(db):
    """Test people (example.test) on every rotation, so a count is not doubled by OPERATIONS_OWNER_REQUIRED."""
    d = OncallDirectory(db)
    for rotation in ROTATIONS:
        for level in (0, 1, 2):
            d.assign(rotation_key=rotation, level=level, person_name=f"Test {rotation} L{level}",
                     contact_channel="EMAIL", contact_address=f"{rotation}.l{level}@example.test", actor="admin-test")
    return db


def _deliveries(engine, alert_id):
    return engine.get(alert_id)["deliveries"]


def _events(engine, alert_id):
    return [e["event"] for e in engine.get(alert_id)["events"]]


# ── silent provider rejection ─────────────────────────────────────────────────────────────────────────────
@pytest.mark.parametrize("answer", [DeliveryReceipt(accepted=0, rejected=2, note="all recipients refused"), False])
def test_a_provider_that_answers_but_accepts_nothing_is_not_a_delivery(platform_db, answer):
    staffed(platform_db)
    sink = Recorder("silent", result=answer)
    e = AlertEngine(platform_db, sinks=[sink])
    a = e.raise_alert("RECONCILIATION_MISMATCH", key="x", title="t", source="test", now_ms=T0)
    stats = e.deliver_pending(now_ms=T0)
    assert stats == {"sent": 0, "retry": 1, "abandoned": 0, "skipped": 0}
    d = _deliveries(e, a.alert_id)[0]
    assert d["status"] == "RETRY" and "PROVIDER_REJECTED" in d["last_error"] and d["sent_at"] is None
    assert "DELIVERY_SUCCEEDED" not in _events(e, a.alert_id)
    assert len(sink.sent) == 1                                   # it WAS called: the answer is what failed


def test_an_accepting_receipt_is_a_delivery_and_is_recorded(platform_db):
    staffed(platform_db)
    sink = Recorder("receipt", result=DeliveryReceipt(accepted=2, provider_reference="webhook,email"))
    e = AlertEngine(platform_db, sinks=[sink])
    a = e.raise_alert("RECONCILIATION_MISMATCH", key="x", title="t", source="test", now_ms=T0)
    assert e.deliver_pending(now_ms=T0)["sent"] == 1
    ok = [ev for ev in e.get(a.alert_id)["events"] if ev["event"] == "DELIVERY_SUCCEEDED"][0]
    assert ok["detail"]["accepted"] == 2 and ok["detail"]["provider_reference"] == "webhook,email"


def test_send_results_are_interpreted_strictly():
    assert interpret_send_result(None) == {} and interpret_send_result(True) == {}
    assert interpret_send_result(DeliveryReceipt(accepted=1))["accepted"] == 1
    for bad in (False, DeliveryReceipt(accepted=0), DeliveryReceipt(accepted=-1), "200 OK", 200):
        with pytest.raises(ProviderRejected):
            interpret_send_result(bad)


class _Answer:
    def __init__(self, body, status=200):
        self.body, self.status = body, status

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def read(self):
        return self.body


@pytest.mark.parametrize("channel,body,delivered", [
    ("webhook", b'{"ok": false, "error": "channel_not_found"}', False),   # Slack-style 200 that dropped it
    ("webhook", b"ok", True),                                              # Slack plain-text acceptance
    ("webhook", b"", True),                                                # Discord: 204, empty body
    ("telegram", b'{"ok": false, "description": "chat not found"}', False),
    ("telegram", b"<html>gateway</html>", False),                          # not an API answer at all
    ("telegram", b'{"ok": true, "result": {"message_id": 7}}', True),
])
def test_notify_failure_counts_a_channel_only_when_the_provider_accepted(monkeypatch, channel, body, delivered):
    notify = alerting._load_notify_module()
    monkeypatch.setattr(notify.urllib.request, "urlopen",
                        lambda request, timeout=None: _Answer(body, 204 if body == b"" else 200))
    config = ({"ALERT_WEBHOOK_URL": "https://hooks.example.test/x"} if channel == "webhook" else
              {"ALERT_TELEGRAM_BOT_TOKEN": "123456789:" + "A" * 35, "ALERT_TELEGRAM_CHAT_ID": "-100"})
    ok, failed = notify.deliver(config, "subject", "text")
    assert (ok, failed) == (([channel], []) if delivered else ([], [channel]))
    sink = alerting.NotifyChannelsSink(env_file=None, environ=config, module=notify)
    if delivered:
        assert sink.send_text("s", "t").accepted == 1
    else:
        with pytest.raises(ProviderRejected):
            sink.send_text("s", "t")


@pytest.mark.parametrize("refused,delivered", [({}, True), ({"b@example.test": (550, b"no")}, True),
                                               ({"a@example.test": (550, b"no"), "b@example.test": (550, b"no")},
                                                False)])
def test_an_smtp_server_that_refuses_every_recipient_is_a_failed_channel(monkeypatch, refused, delivered):
    notify = alerting._load_notify_module()

    class SMTP:
        def __init__(self, *a, **kw):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def starttls(self, context=None):
            pass

        def send_message(self, mail, to_addrs=None):
            return dict(refused)
    monkeypatch.setattr(notify.smtplib, "SMTP", SMTP)
    config = {"ALERT_EMAIL_TO": "a@example.test, b@example.test", "SMTP_HOST": "smtp.example.test"}
    assert notify.deliver(config, "s", "t") == ((["email"], []) if delivered else ([], ["email"]))


# ── missing provider, restart, duplicates ─────────────────────────────────────────────────────────────────
def test_without_any_provider_the_alert_is_recorded_and_the_gap_is_visible(platform_db):
    e = AlertEngine(platform_db, sinks=[])
    a = e.raise_alert("UNPROTECTED_POSITION", key="p", title="t", source="test", now_ms=T0)
    assert a.created and e.get(a.alert_id)["status"] == "OPEN"
    assert "NO_DELIVERY_CHANNEL" in _events(e, a.alert_id)
    health = e.health(now_ms=T0)
    assert health["status"] == "CRITICAL" and any(f["code"] == "NO_DELIVERY_CHANNEL" for f in health["findings"])


def test_a_provider_missing_in_the_delivering_process_stays_pending_visible_and_resumes(platform_db):
    staffed(platform_db)
    queued_by = AlertEngine(platform_db, sinks=[Recorder("notify_channels")])
    a = queued_by.raise_alert("WORKER_OUTAGE", key="w", title="t", source="test", now_ms=T0)
    without = AlertEngine(platform_db, sinks=[])                    # this process has no such channel configured
    assert without.deliver_pending(now_ms=T0)["retry"] == 1
    d = _deliveries(without, a.alert_id)[0]
    assert d["status"] == "RETRY" and "SINK_NOT_CONFIGURED:notify_channels" in d["last_error"]
    assert any(x["alert_id"] == a.alert_id for x in without.open_alerts())
    restored = Recorder("notify_channels")
    later = AlertEngine(platform_db, sinks=[restored])              # the channel is configured again
    assert later.deliver_pending(now_ms=T0 + 30_000)["sent"] == 1 and len(restored.sent) == 1


def test_pending_deliveries_resume_after_a_restart_including_a_claim_left_by_a_dead_process(platform_db):
    staffed(platform_db)
    first = AlertEngine(platform_db, sinks=[Recorder("ch", fail_times=1), Recorder("ch2")])
    a = first.raise_alert("QUEUE_BACKLOG", key="q", title="t", source="test", now_ms=T0)
    first.deliver_pending(now_ms=T0)                                # ch fails once (RETRY), ch2 sent
    b = first.raise_alert("QUEUE_BACKLOG", key="q2", title="t2", source="test", now_ms=T0)
    with platform_db.transaction() as conn:                         # a process claimed b's deliveries, then died
        conn.execute("UPDATE ops_alert_deliveries SET next_attempt_at=? WHERE alert_id=?",
                     (T0 + DELIVERY_CLAIM_MS, b.alert_id))
    del first                                                       # "restart": a fresh engine, nothing in memory
    ch, ch2 = Recorder("ch"), Recorder("ch2")
    second = AlertEngine(platform_db, sinks=[ch, ch2])
    assert second.deliver_pending(now_ms=T0 + 1_000)["sent"] == 0    # retry backoff and the dead claim still run
    assert second.deliver_pending(now_ms=T0 + 30_000)["sent"] == 1    # a's retry
    assert second.deliver_pending(now_ms=T0 + DELIVERY_CLAIM_MS)["sent"] == 2   # b's two, after the claim lapsed
    assert {d["status"] for d in _deliveries(second, a.alert_id) + _deliveries(second, b.alert_id)} == {"SENT"}
    assert len(ch.sent) == 2 and len(ch2.sent) == 1


def test_duplicate_signals_from_two_processes_make_one_alert_and_one_delivery_per_channel(platform_db):
    staffed(platform_db)
    sink = Recorder("ch")
    engines = [AlertEngine(platform_db, sinks=[sink]) for _ in range(4)]
    barrier = threading.Barrier(len(engines))
    errors = []

    def raise_it(engine):
        try:
            barrier.wait(5)
            engine.raise_alert("EXCHANGE_CONNECTIVITY_LOSS", key={"venue": "binance"}, title="t", source="test",
                               now_ms=T0)
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)
    threads = [threading.Thread(target=raise_it, args=(e,)) for e in engines]
    for t in threads:
        t.start()
    for t in threads:
        t.join(30)
    assert not errors
    alerts = [x for x in engines[0].open_alerts() if x["category"] == "EXCHANGE_CONNECTIVITY_LOSS"]
    assert len(alerts) == 1 and alerts[0]["occurrences"] == 4
    for e in engines:
        e.deliver_pending(now_ms=T0)
    assert len(sink.sent) == 1


# ── isolation, database outage ───────────────────────────────────────────────────────────────────────────
def test_a_failing_producer_and_a_database_outage_while_recording_are_isolated(platform_db):
    staffed(platform_db)
    sink = Recorder("ch")
    e = AlertEngine(platform_db, sinks=[sink])
    healthy_ran = []

    def broken_producer():
        raise RuntimeError("producer exploded")

    def outage_while_recording():
        raise DatabaseUnavailable("OperationalError: connection refused")

    def healthy():
        healthy_ran.append(1)
        e.raise_alert("STALE_MARKET_DATA", key="m", title="t", source="test", now_ms=T0)
    out = run_cycle(e, [broken_producer, outage_while_recording, healthy], now_ms=T0)
    assert len(out["evaluator_errors"]) == 2 and out["evaluators"] == 1 and healthy_ran
    assert any("DatabaseUnavailable" in x for x in out["evaluator_errors"])
    assert out["delivery"]["sent"] == 1 and len(sink.sent) == 1


def test_an_alert_store_outage_fails_one_tick_and_the_next_tick_records(platform_db, monkeypatch):
    e = AlertEngine(platform_db, sinks=[Recorder("ch")])
    lease = TickLease(platform_db, owner="host-a:1", ttl_ms=60_000)
    real = e.escalate_due

    def down(**kw):
        raise DatabaseUnavailable("OperationalError: server closed the connection")
    monkeypatch.setattr(e, "escalate_due", down)
    build = lambda state, disk: [lambda: e.raise_alert("STALE_MARKET_DATA", key="m", title="t", source="test")]
    out = run_tick(e, lease, build)
    assert out["ran"] and out["outcome"] == "ERROR" and "DatabaseUnavailable" in out["error"]
    monkeypatch.setattr(e, "escalate_due", real)
    out = run_tick(e, lease, build)
    assert out["outcome"] == "OK"
    assert any(x["category"] == "STALE_MARKET_DATA" for x in e.open_alerts())
    assert lease.status()["last_tick_outcome"] == "OK"


# ── escalation and recovery, on-call stays unassigned ──────────────────────────────────────────────────────
def test_escalation_then_recovery_with_nobody_on_call(platform_db):
    sink = Recorder("ch")
    e = AlertEngine(platform_db, sinks=[sink])
    a = e.raise_alert("UNPROTECTED_POSITION", key="p", title="t", source="test", now_ms=T0)
    assert a.owner_required                                        # no rotation has a person: never invented
    assert OncallDirectory(platform_db).resolve("trading-operations", 0).targets == []
    assert any(x["category"] == OPERATIONS_OWNER_REQUIRED for x in e.open_alerts())
    mine = lambda escalated: [x for x in escalated if x[0] == a.alert_id]  # noqa: E731
    assert mine(e.escalate_due(now_ms=T0 + 4 * MIN)) == []
    assert mine(e.escalate_due(now_ms=T0 + 5 * MIN)) == [(a.alert_id, 1)]
    assert mine(e.escalate_due(now_ms=T0 + 15 * MIN)) == [(a.alert_id, 2)]
    e.deliver_pending(now_ms=T0 + 15 * MIN)
    assert e.resolve_condition("UNPROTECTED_POSITION", key="p", now_ms=T0 + 16 * MIN)
    e.deliver_pending(now_ms=T0 + 16 * MIN)
    reasons = [m.reason for m in sink.sent if m.alert_id == a.alert_id]
    assert reasons == ["RAISED", "ESCALATED", "ESCALATED", "RECOVERED"]
    assert e.health(now_ms=T0)["owner_required"] is True


# ── deliveries that keep failing ───────────────────────────────────────────────────────────────────────────
def test_deliveries_that_keep_failing_alert_before_they_are_abandoned_and_resolve(platform_db):
    staffed(platform_db)
    flaky, other = Recorder("flaky", fail_times=2), Recorder("other")
    e = AlertEngine(platform_db, sinks=[flaky, other])
    e.raise_alert("RECONCILIATION_MISMATCH", key="x", title="t", source="test", now_ms=T0)
    e.deliver_pending(now_ms=T0)
    e.deliver_pending(now_ms=T0 + 30_000)                           # second failure of 'flaky'
    evaluate_alert_delivery(e, platform_db, now_ms=T0 + 31_000)
    failing = [x for x in e.open_alerts() if x["category"] == "ALERT_DELIVERY_FAILURE"]
    assert len(failing) == 1 and failing[0]["detail"]["sink"] == "flaky"
    e.deliver_pending(now_ms=T0 + 31_000)                           # 'other' carries the failure alert to a human
    assert any(m.category == "ALERT_DELIVERY_FAILURE" for m in other.sent)
    e.deliver_pending(now_ms=T0 + 31_000 + 60_000)                  # 'flaky' recovers
    evaluate_alert_delivery(e, platform_db, now_ms=T0 + 100_000)
    assert not [x for x in e.open_alerts() if x["category"] == "ALERT_DELIVERY_FAILURE"]


def test_deliveries_nobody_sends_are_reported_overdue(platform_db):
    staffed(platform_db)
    e = AlertEngine(platform_db, sinks=[Recorder("queue")])
    e.raise_alert("QUEUE_BACKLOG", key="q", title="t", source="test", now_ms=T0)   # queued, never delivered
    observer = AlertEngine(platform_db, sinks=[Recorder("pager")])
    evaluate_alert_delivery(observer, platform_db, now_ms=T0 + 16 * MIN)
    overdue = [x for x in observer.open_alerts() if x["category"] == "ALERT_DELIVERY_FAILURE"]
    assert len(overdue) == 1 and overdue[0]["detail"]["sink"] == "queue" and overdue[0]["detail"]["deliveries"] == 1


# ── the tick's dead-man ────────────────────────────────────────────────────────────────────────────────────
def _now(db):
    with db.transaction(write=False) as conn:
        return conn.now_ms()


def test_dead_man_check_never_ran_ok_stale_and_error(platform_db):
    assert check_tick(platform_db)["verdict"] == "NEVER_RAN"
    e = AlertEngine(platform_db, sinks=[Recorder("ch")])
    lease = TickLease(platform_db, owner="host-a:1", ttl_ms=60_000)
    assert run_tick(e, lease, lambda s, d: [])["outcome"] == "OK"
    assert check_tick(platform_db)["verdict"] == "OK"
    assert check_tick(platform_db, now_ms=_now(platform_db) + 6 * MIN)["verdict"] == "STALE"
    with platform_db.transaction() as conn:
        conn.execute("UPDATE ops_tick_leases SET last_tick_outcome='ERROR'")
    assert check_tick(platform_db)["verdict"] == "ERROR"


def test_dead_man_cli_exit_codes(database_url, monkeypatch, capsys):
    from shared_lib.persistence.sql import Database, migrate
    from shared_lib.persistence.sql.platform_schema import all_migrations
    db = Database(database_url)
    migrate(db, all_migrations())
    monkeypatch.setenv("PLATFORM_DATABASE_URL", database_url)
    assert alerting.main(["--check-tick"]) == 3
    assert json.loads(capsys.readouterr().out.strip().splitlines()[-1])["verdict"] == "NEVER_RAN"
    run_tick(AlertEngine(db, sinks=[Recorder("ch")]), TickLease(db, owner="host-a:1"), lambda s, d: [])
    assert alerting.main(["--check-tick", "--max-tick-age", "300"]) == 0
    assert json.loads(capsys.readouterr().out.strip().splitlines()[-1])["ok"] is True
    db.close()
    monkeypatch.setenv("PLATFORM_DATABASE_URL", "postgresql://nobody@127.0.0.1:1/none")
    assert alerting.main(["--check-tick"]) == 1                      # cannot even read: the unit fails and alerts


def test_a_gap_between_ticks_is_alerted_by_the_next_tick_then_recovers(platform_db):
    staffed(platform_db)
    sink = Recorder("ch")
    e = AlertEngine(platform_db, sinks=[sink])
    lease = TickLease(platform_db, owner="host-a:1", ttl_ms=60_000)
    assert run_tick(e, lease, lambda s, d: [])["tick_gap"] == {"gap_ms": None, "firing": False}
    with platform_db.transaction() as conn:                         # the tick was down for 17 minutes
        conn.execute("UPDATE ops_tick_leases SET last_tick_finished_at=last_tick_finished_at-?", (17 * MIN,))
    out = run_tick(e, lease, lambda s, d: [])
    assert out["tick_gap"]["firing"] is True and out["tick_gap"]["gap_ms"] >= 17 * MIN
    stale = [x for x in e.open_alerts() if x["category"] == "OPERATIONS_TICK_STALE"]
    assert len(stale) == 1 and stale[0]["severity"] == "CRITICAL"
    assert any(m.category == "OPERATIONS_TICK_STALE" for m in sink.sent)      # delivered by the same tick
    run_tick(e, lease, lambda s, d: [])
    assert not [x for x in e.open_alerts() if x["category"] == "OPERATIONS_TICK_STALE"]
    assert [m.reason for m in sink.sent if m.category == "OPERATIONS_TICK_STALE"][-1] == "RECOVERED"


def test_the_tick_alert_and_delivery_failures_can_never_be_silenced(platform_db):
    from shared_lib.operations.alerting import UNSUPPRESSIBLE, AlertError
    assert {"OPERATIONS_TICK_STALE", "ALERT_DELIVERY_FAILURE"} <= UNSUPPRESSIBLE
    e = AlertEngine(platform_db, sinks=[])
    with pytest.raises(AlertError):
        e.add_suppression(category="OPERATIONS_TICK_STALE", ends_at_ms=_now(platform_db) + MIN, reason="r", actor="a")


def test_a_hanging_provider_never_holds_the_tick_beyond_its_timeout(platform_db):
    staffed(platform_db)
    release = threading.Event()
    hanging = CallableSink("hanging", lambda m: release.wait(30))
    quick = Recorder("quick")
    e = AlertEngine(platform_db, sinks=[hanging, quick], send_timeout_s=0.5)
    e.raise_alert("API_OUTAGE", key="a", title="t", source="test", now_ms=T0)
    started = time.monotonic()
    stats = e.deliver_pending(now_ms=T0)
    release.set()
    assert time.monotonic() - started < 5 and stats["sent"] == 1 and stats["retry"] == 1 and len(quick.sent) == 1
