"""Section H Step 8.5: alerting engine, on-call routing and the alert evaluators -- on SQLite and PostgreSQL.

Time is injected (``now_ms``) wherever a decision depends on it, so dedup windows and escalation are tested
exactly; everything else uses the database server's clock. No person here is real: on-call members are created
by the test as an administrator would, with ``example.test`` contacts.
"""
from __future__ import annotations

import json
from collections import namedtuple
from pathlib import Path

import pytest

from app.ops import alerting
from app.ops.alerting import (AlertEngine, AlertError, CallableSink, DiskSample, JobQueueSink, NotifyChannelsSink,
                              evaluate_backup_age, evaluate_database, evaluate_disk_growth, evaluate_lease_takeovers,
                              evaluate_queue_backlog, evaluate_worker_heartbeats, notification_job_handler, run_cycle)
from app.platform_workers.assignments import WorkerAssignments
from app.platform_workers.jobs import NOTIFICATION, JobProcessor, JobQueue
from shared_lib.operations.oncall import OPERATIONS_OWNER_REQUIRED, ROTATIONS, OncallDirectory, OncallError
from shared_lib.persistence.sql.operations_schema import MIGRATIONS
from shared_lib.persistence.sql.platform_schema import all_migrations

T0 = 1_800_000_000_000
MIN = 60_000


class Recorder:
    def __init__(self, name="recorder", fail_times=0):
        self.name = name
        self.sent = []
        self.fail_times = fail_times

    def send(self, message):
        if self.fail_times > 0:
            self.fail_times -= 1
            raise RuntimeError("channel down")
        self.sent.append(message)


def staff(db, rotation="trading-operations", levels=(0, 1, 2)):
    d = OncallDirectory(db)
    for lvl in levels:
        d.assign(rotation_key=rotation, level=lvl, person_name=f"Test Person L{lvl}", contact_channel="EMAIL",
                 contact_address=f"l{lvl}@example.test", actor="admin-test")
    return d


def engine(db, *sinks, **kw):
    return AlertEngine(db, sinks=list(sinks) or [Recorder()], **kw)


# ── schema ─────────────────────────────────────────────────────────────────────────────────────────────────
def test_operations_migrations_live_in_reserved_range_and_are_reversible():
    assert MIGRATIONS and all(30 <= m.version <= 39 for m in MIGRATIONS)
    assert all(m.down for m in MIGRATIONS) and not any(m.irreversible for m in MIGRATIONS)
    assert {m.version for m in MIGRATIONS} <= {m.version for m in all_migrations()}


def test_operations_rollback_and_reapply(database_url):
    from shared_lib.persistence.sql import Database, migrate, rollback
    db = Database(database_url)
    try:
        migrate(db, all_migrations())
        undone = rollback(db, all_migrations(), to_version=29)
        assert set(undone) >= {30, 31, 32}
        assert migrate(db, all_migrations()).final_version >= 32
    finally:
        db.close()


def test_event_log_is_append_only_in_the_database(platform_db):
    e = engine(platform_db)
    r = e.raise_alert("QUEUE_BACKLOG", key="k", title="t", source="test", now_ms=T0)
    for sql in ("UPDATE ops_alert_events SET actor='x' WHERE alert_id=?", "DELETE FROM ops_alert_events WHERE alert_id=?"):
        with pytest.raises(Exception) as err:
            with platform_db.transaction() as conn:
                conn.execute(sql, (r.alert_id,))
        assert "APPEND_ONLY" in str(err.value)


# ── categories ─────────────────────────────────────────────────────────────────────────────────────────────
REQUIRED = ("EXCHANGE_EXECUTION_UNCERTAINTY", "UNPROTECTED_POSITION", "RECONCILIATION_MISMATCH", "KILL_SWITCH_ACTIVATED",
            "WORKER_OUTAGE", "LEASE_TAKEOVER_FAILURE", "DATABASE_UNAVAILABLE", "QUEUE_BACKLOG", "API_OUTAGE",
            "STALE_INSTRUMENT_RULES", "STALE_MARKET_DATA", "AUTHENTICATION_ANOMALY", "BILLING_WEBHOOK_FAILURE",
            "BACKUP_FAILURE", "DISK_GROWTH", "EXCESSIVE_LATENCY", "EXPIRED_EXCHANGE_CERTIFICATION",
            "GOVERNANCE_VIOLATION_ATTEMPT")


def test_every_required_category_has_severity_and_known_routing():
    for c in REQUIRED:
        p = alerting.CATEGORIES[c]
        assert p.severity in alerting.SEVERITIES and p.rotation_key in ROTATIONS
    assert alerting.CATEGORIES["UNPROTECTED_POSITION"].severity == "CRITICAL"
    assert alerting.CATEGORIES["DISK_GROWTH"].severity == "MEDIUM"


def test_unknown_category_refused(platform_db):
    with pytest.raises(AlertError) as err:
        engine(platform_db).raise_alert("NOT_A_THING", key="k", title="t", source="s")
    assert err.value.code == "UNKNOWN_ALERT_CATEGORY"


# ── dedup ──────────────────────────────────────────────────────────────────────────────────────────────────
def test_dedup_within_window_and_reminder_after(platform_db):
    staff(platform_db)
    rec = Recorder()
    e = engine(platform_db, rec, dedup_window_ms=10 * MIN)
    first = e.raise_alert("RECONCILIATION_MISMATCH", key={"account": "a1"}, title="mismatch", source="test",
                          account_ref="a1", now_ms=T0)
    assert first.created and len(first.deliveries) == 1
    again = e.raise_alert("RECONCILIATION_MISMATCH", key={"account": "a1"}, title="mismatch", source="test",
                          account_ref="a1", now_ms=T0 + 3 * MIN)
    assert again.alert_id == first.alert_id and again.deduplicated and not again.reminded and not again.deliveries
    later = e.raise_alert("RECONCILIATION_MISMATCH", key={"account": "a1"}, title="mismatch", source="test",
                          account_ref="a1", now_ms=T0 + 11 * MIN)
    assert later.alert_id == first.alert_id and later.reminded and len(later.deliveries) == 1
    other = e.raise_alert("RECONCILIATION_MISMATCH", key={"account": "a2"}, title="mismatch", source="test",
                          account_ref="a2", now_ms=T0 + 11 * MIN)
    assert other.created and other.alert_id != first.alert_id
    a = e.get(first.alert_id)
    assert a["occurrences"] == 3 and a["scope"] == "ACCOUNT"
    assert [ev["event"] for ev in a["events"]].count("DEDUPLICATED") == 1
    assert [ev["event"] for ev in a["events"]].count("REMINDED") == 1
    with platform_db.transaction(write=False) as conn:
        assert conn.scalar("SELECT COUNT(*) FROM ops_alerts WHERE fingerprint=? AND status<>'RESOLVED'",
                           (alerting.fingerprint("RECONCILIATION_MISMATCH", {"account": "a1"}),)) == 1


def test_resolved_alert_recurs_as_new_alert(platform_db):
    staff(platform_db)
    e = engine(platform_db)
    a = e.raise_alert("KILL_SWITCH_ACTIVATED", key="global", title="kill", source="test", now_ms=T0)
    e.resolve(a.alert_id, actor="admin-test", note="cleared", now_ms=T0 + MIN)
    b = e.raise_alert("KILL_SWITCH_ACTIVATED", key="global", title="kill", source="test", now_ms=T0 + 2 * MIN)
    assert b.created and b.alert_id != a.alert_id


def test_acknowledged_alert_is_not_reminded(platform_db):
    staff(platform_db)
    e = engine(platform_db, dedup_window_ms=MIN)
    a = e.raise_alert("STALE_MARKET_DATA", key="btc", title="stale", source="test", now_ms=T0)
    e.acknowledge(a.alert_id, actor="admin-test", now_ms=T0 + 10)
    again = e.raise_alert("STALE_MARKET_DATA", key="btc", title="stale", source="test", now_ms=T0 + 30 * MIN)
    assert again.deduplicated and not again.reminded


def test_detail_is_redacted_before_storage(platform_db):
    e = engine(platform_db)
    r = e.raise_alert("AUTHENTICATION_ANOMALY", key="x", title="burst api_key=ABCDEF1234567890ZZ", source="test",
                      detail={"api_secret": "s3cr3t-value-123456", "note": "token=abcdefgh12345678"}, now_ms=T0)
    stored = json.dumps(e.get(r.alert_id))
    assert "s3cr3t-value-123456" not in stored and "abcdefgh12345678" not in stored
    assert "ABCDEF1234567890ZZ" not in stored


# ── escalation ─────────────────────────────────────────────────────────────────────────────────────────────
def test_escalation_timing_follows_policy_and_stops_on_ack(platform_db):
    staff(platform_db)
    rec = Recorder()
    e = engine(platform_db, rec)
    a = e.raise_alert("UNPROTECTED_POSITION", key="pos-1", title="naked", source="test", now_ms=T0)
    assert e.escalate_due(now_ms=T0 + 4 * MIN + 59_000) == []
    assert e.escalate_due(now_ms=T0 + 5 * MIN) == [(a.alert_id, 1)]
    assert e.escalate_due(now_ms=T0 + 5 * MIN + 1) == []            # next step is at 15 min, not 5+5
    assert e.escalate_due(now_ms=T0 + 15 * MIN) == [(a.alert_id, 2)]
    assert e.escalate_due(now_ms=T0 + 600 * MIN) == []               # no level beyond the policy
    e.deliver_pending(now_ms=T0 + 15 * MIN)
    levels = [(m.reason, m.escalation_level, [t["person_name"] for t in m.targets]) for m in rec.sent]
    assert levels == [("RAISED", 0, ["Test Person L0"]), ("ESCALATED", 1, ["Test Person L1"]),
                      ("ESCALATED", 2, ["Test Person L2"])]
    b = e.raise_alert("UNPROTECTED_POSITION", key="pos-2", title="naked", source="test", now_ms=T0)
    e.acknowledge(b.alert_id, actor="ops-admin", note="on it", now_ms=T0 + 2 * MIN)
    assert e.escalate_due(now_ms=T0 + 60 * MIN) == []
    got = e.get(b.alert_id)
    assert got["status"] == "ACKNOWLEDGED" and got["acknowledged_by"] == "ops-admin" and got["escalation_level"] == 0


def test_low_severity_never_escalates(platform_db):
    e = engine(platform_db)
    a = e.raise_alert("DISK_GROWTH", key="d", title="disk", source="test", severity="LOW", now_ms=T0)
    assert e.get(a.alert_id)["next_escalation_at"] is None
    assert a.alert_id not in [x for x, _ in e.escalate_due(now_ms=T0 + 10_000 * MIN)]


def test_level_two_without_rotation_owner_goes_to_operations_owner(platform_db):
    staff(platform_db, levels=(0,))
    staff(platform_db, rotation="operations-owner", levels=(0,))
    rec = Recorder()
    e = engine(platform_db, rec)
    a = e.raise_alert("WORKER_OUTAGE", key="w", title="down", source="test", now_ms=T0)
    OncallDirectory(platform_db).assign(rotation_key="platform-infrastructure", level=0, person_name="Infra Primary",
                                        contact_channel="EMAIL", contact_address="infra@example.test", actor="admin")
    e.escalate_due(now_ms=T0 + 5 * MIN)
    e.escalate_due(now_ms=T0 + 15 * MIN)
    e.deliver_pending(now_ms=T0 + 15 * MIN)
    final = [m for m in rec.sent if m.alert_id == a.alert_id and m.escalation_level == 2]
    assert final and [t["person_name"] for t in final[0].targets] == ["Test Person L0"]
    assert final[0].rotation_key == "platform-infrastructure"


# ── ack / resolve ──────────────────────────────────────────────────────────────────────────────────────────
def test_ack_and_resolve_record_actor_and_refuse_invalid_transitions(platform_db):
    e = engine(platform_db)
    a = e.raise_alert("API_OUTAGE", key="api", title="api down", source="test", now_ms=T0)
    with pytest.raises(AlertError) as err:
        e.acknowledge(a.alert_id, actor="", now_ms=T0)
    assert err.value.code == "ACTOR_REQUIRED"
    with pytest.raises(AlertError) as err:
        e.resolve(a.alert_id, actor="ops", note="", now_ms=T0)
    assert err.value.code == "RESOLUTION_NOTE_REQUIRED"
    e.acknowledge(a.alert_id, actor="ops-1", now_ms=T0 + 1)
    with pytest.raises(AlertError) as err:
        e.acknowledge(a.alert_id, actor="ops-1", now_ms=T0 + 2)
    assert err.value.code == "ALERT_NOT_OPEN"
    done = e.resolve(a.alert_id, actor="ops-2", note="restarted api", now_ms=T0 + 3)
    assert (done["status"], done["acknowledged_by"], done["resolved_by"]) == ("RESOLVED", "ops-1", "ops-2")
    assert [ev["event"] for ev in done["events"]][-2:] == ["ACKNOWLEDGED", "RESOLVED"]
    assert [ev["actor"] for ev in done["events"]][-2:] == ["ops-1", "ops-2"]
    with pytest.raises(AlertError) as err:
        e.resolve(a.alert_id, actor="ops", note="again", now_ms=T0 + 4)
    assert err.value.code == "ALERT_ALREADY_RESOLVED"
    with pytest.raises(AlertError):
        e.acknowledge("missing", actor="x")


# ── unassigned routing ─────────────────────────────────────────────────────────────────────────────────────
def test_routing_to_unassigned_rotation_raises_critical_owner_required_finding(platform_db):
    rec = Recorder()
    e = engine(platform_db, rec)
    r = e.raise_alert("EXCHANGE_EXECUTION_UNCERTAINTY", key="ord-1", title="unknown outcome", source="test",
                      account_ref="acct-1", now_ms=T0)
    assert r.owner_required
    alerts = {a["category"]: a for a in e.list_alerts()}
    finding = alerts[OPERATIONS_OWNER_REQUIRED]
    assert finding["severity"] == "CRITICAL" and finding["detail"]["rotation_key"] == "trading-operations"
    assert alerts["EXCHANGE_EXECUTION_UNCERTAINTY"]["owner_required"] is True
    # the original alert still goes out through the channel, marked as reaching nobody
    e.deliver_pending(now_ms=T0)
    texts = [m.text() for m in rec.sent]
    assert any("OPERATIONS_OWNER_REQUIRED" in t for t in texts)
    assert any("[OPERATIONS OWNER REQUIRED]" in m.subject() for m in rec.sent)
    # repeated raises do not multiply the finding (deduplicated by rotation)
    e.raise_alert("EXCHANGE_EXECUTION_UNCERTAINTY", key="ord-2", title="unknown", source="test", account_ref="acct-2",
                  now_ms=T0 + 1)
    assert sum(1 for a in e.list_alerts() if a["category"] == OPERATIONS_OWNER_REQUIRED) == 1
    health = e.health(now_ms=T0)
    assert health["status"] == "CRITICAL" and health["owner_required"] is True
    assert {f["rotation_key"] for f in health["findings"] if f["code"] == OPERATIONS_OWNER_REQUIRED} == set(ROTATIONS)


def test_default_oncall_is_empty_and_never_invents_a_person(platform_db):
    d = OncallDirectory(platform_db)
    view = d.overview()
    assert all(r["members"] == [] and r["status"] == OPERATIONS_OWNER_REQUIRED for r in view["rotations"])
    assert view["owner_assigned"] is False and view["status"] == OPERATIONS_OWNER_REQUIRED
    with pytest.raises(OncallError):
        d.assign(rotation_key="trading-operations", level=0, person_name=" ", contact_channel="EMAIL",
                 contact_address="x@example.test", actor="admin")
    with pytest.raises(OncallError):
        d.assign(rotation_key="made-up", level=0, person_name="A", contact_channel="EMAIL",
                 contact_address="x@example.test", actor="admin")


def test_oncall_assign_remove_and_history(platform_db):
    d = OncallDirectory(platform_db)
    mid = d.assign(rotation_key="operations-owner", level=0, person_name="Owner Example", contact_channel="TELEGRAM",
                   contact_address="@owner_example", actor="admin-1")
    assert d.overview()["owner_assigned"] is True
    assert d.resolve("operations-owner").targets[0].person_name == "Owner Example"
    d.remove(mid, actor="admin-2")
    assert d.resolve("operations-owner").owner_required
    assert [h["action"] for h in d.history()] == ["REMOVED", "ASSIGNED"]
    with pytest.raises(Exception):
        with platform_db.transaction() as conn:
            conn.execute("DELETE FROM ops_oncall_history")


# ── delivery ───────────────────────────────────────────────────────────────────────────────────────────────
def test_failed_delivery_is_retried_then_succeeds(platform_db):
    staff(platform_db)
    flaky = Recorder("flaky", fail_times=2)
    e = engine(platform_db, flaky)
    a = e.raise_alert("RECONCILIATION_MISMATCH", key="x", title="t", source="test", now_ms=T0)
    assert e.deliver_pending(now_ms=T0)["retry"] == 1
    assert e.deliver_pending(now_ms=T0 + 1_000)["sent"] == 0        # not due before the backoff
    assert e.deliver_pending(now_ms=T0 + 30_000)["retry"] == 1
    assert e.deliver_pending(now_ms=T0 + 30_000 + 60_000)["sent"] == 1
    d = e.get(a.alert_id)["deliveries"][0]
    assert (d["status"], d["attempts"]) == ("SENT", 3)
    assert len(flaky.sent) == 1


def test_abandoned_delivery_raises_delivery_failure_alert(platform_db):
    staff(platform_db)
    dead = Recorder("dead", fail_times=99)
    e = engine(platform_db, dead, max_delivery_attempts=2)
    a = e.raise_alert("RECONCILIATION_MISMATCH", key="x", title="t", source="test", now_ms=T0)
    e.deliver_pending(now_ms=T0)
    stats = e.deliver_pending(now_ms=T0 + 10 * MIN)
    assert stats["abandoned"] == 1
    assert e.get(a.alert_id)["deliveries"][0]["status"] == "ABANDONED"
    assert any(x["category"] == "ALERT_DELIVERY_FAILURE" for x in e.list_alerts())
    assert any(f["code"] == "DELIVERIES_ABANDONED" for f in e.health(now_ms=T0)["findings"])


def test_no_sink_is_a_critical_health_finding(platform_db):
    e = AlertEngine(platform_db, sinks=[])
    e.raise_alert("API_OUTAGE", key="a", title="t", source="test", now_ms=T0)
    assert any(f["code"] == "NO_DELIVERY_CHANNEL" for f in e.health()["findings"])


def test_job_queue_sink_enqueues_idempotent_notification_job_and_handler_delivers(platform_db):
    staff(platform_db)
    queue = JobQueue(platform_db)
    e = engine(platform_db, JobQueueSink(queue))
    a = e.raise_alert("UNPROTECTED_POSITION", key="p", title="naked", source="test", now_ms=T0)
    assert e.deliver_pending(now_ms=T0)["sent"] == 1
    with platform_db.transaction(write=False) as conn:
        rows = conn.all("SELECT category, payload FROM platform_jobs")
    assert len(rows) == 1 and rows[0][0] == NOTIFICATION
    payload = json.loads(rows[0][1])
    assert payload["kind"] == "ops_alert" and payload["alert_id"] == a.alert_id
    # the same delivery handed over twice is still one job
    msg = alerting.AlertMessage(a.alert_id, "UNPROTECTED_POSITION", "CRITICAL", "naked", {}, "RAISED", 0,
                                "trading-operations", False, [], e.get(a.alert_id)["deliveries"][0]["delivery_id"], 1)
    with pytest.raises(ValueError):           # same key, different payload text: refused, never a duplicate
        JobQueueSink(queue).send(alerting.AlertMessage(**{**msg.__dict__, "title": "changed"}))

    class Channels:
        def __init__(self, fails):
            self.fails, self.sent = fails, []

        def send_text(self, subject, text):
            if self.fails:
                self.fails -= 1
                raise RuntimeError("smtp down")
            self.sent.append(subject)
    channels = Channels(1)
    proc = JobProcessor(queue, "ops-worker", {NOTIFICATION: notification_job_handler(channels)})
    assert [r[1] for r in proc.run_once()] == ["RETRY"]
    with platform_db.transaction() as conn:
        conn.execute("UPDATE platform_jobs SET next_attempt_at=0")
    assert [r[1] for r in proc.run_once()] == ["SUCCEEDED"]
    assert channels.sent and "UNPROTECTED_POSITION" in channels.sent[0]


def test_notify_channels_sink_uses_notify_failure_and_raises_when_unconfigured():
    calls = []

    class Fake:
        def load_config(self, env_file, environ):
            return dict(environ)

        def configured_channels(self, config):
            return [("webhook", None)] if config.get("ALERT_WEBHOOK_URL") else []

        def deliver(self, config, subject, message):
            calls.append(subject)
            return (["webhook"], []) if config.get("OK") else ([], ["webhook"])
    sink = NotifyChannelsSink(env_file=None, environ={}, module=Fake())
    with pytest.raises(RuntimeError, match="NO_ALERT_CHANNEL_CONFIGURED"):
        sink.send_text("s", "t")
    NotifyChannelsSink(env_file=None, environ={"ALERT_WEBHOOK_URL": "u", "OK": "1"}, module=Fake()).send_text("s", "t")
    with pytest.raises(RuntimeError, match="ALERT_CHANNELS_FAILED"):
        NotifyChannelsSink(env_file=None, environ={"ALERT_WEBHOOK_URL": "u"}, module=Fake()).send_text("s", "t")
    assert calls == ["s", "s"]


def test_real_notify_failure_module_loads():
    sink = NotifyChannelsSink(env_file=None, environ={})
    assert sink.configured() is False


# ── evaluators ─────────────────────────────────────────────────────────────────────────────────────────────
def test_queue_backlog_evaluator_raises_and_resolves(platform_db):
    e = engine(platform_db)
    q = JobQueue(platform_db)
    q.enqueue(NOTIFICATION, "old-job", {"x": 1})
    with platform_db.transaction() as conn:
        conn.execute("UPDATE platform_jobs SET next_attempt_at = next_attempt_at - 3600000")
    overdue, dead = evaluate_queue_backlog(e, q, max_due_age_ms=5 * MIN)
    assert overdue.firing and overdue.result.created and not dead.firing
    job = q.claim("w")[0]
    q.fail(job, "boom", permanent=True)
    overdue, dead = evaluate_queue_backlog(e, q, max_due_age_ms=5 * MIN)
    assert not overdue.firing and overdue.resolved and dead.firing and dead.detail["dead"] == 1


def test_worker_heartbeat_evaluator(platform_db):
    e = engine(platform_db)
    asg = WorkerAssignments(platform_db)
    w = asg.register_worker("worker-a")
    with platform_db.transaction(write=False) as conn:
        now = conn.now_ms()
    [ev] = evaluate_worker_heartbeats(e, platform_db, stale_ms=90_000, now_ms=now + 10_000)
    assert not ev.firing
    [ev] = evaluate_worker_heartbeats(e, platform_db, stale_ms=90_000, now_ms=now + 120_000)
    assert ev.firing and ev.result.created
    asg.set_worker_state(w, "STOPPED")      # a clean stop is not an outage: resolved
    [ev] = evaluate_worker_heartbeats(e, platform_db, stale_ms=90_000, now_ms=now + 10 ** 7)
    assert not ev.firing and ev.resolved


def test_lease_takeover_failure_evaluator(platform_db):
    e = engine(platform_db)
    asg = WorkerAssignments(platform_db, ttl_ms=30_000)
    asg.upsert_accounts([("acct-1", "tenant-1", "binance", "DEMO"), ("acct-2", "tenant-2", "binance", "DEMO")])
    w = asg.register_worker("worker-a")
    asg.claim(w)
    with platform_db.transaction(write=False) as conn:
        now = conn.now_ms()
    assert not evaluate_lease_takeovers(e, platform_db, grace_ms=60_000, now_ms=now).firing
    ev = evaluate_lease_takeovers(e, platform_db, grace_ms=60_000, now_ms=now + 30_000 + 61_000)
    assert ev.firing and ev.detail["orphaned_accounts"] == 2
    asg.register_worker("worker-b")
    asg.claim("worker-b")
    with platform_db.transaction(write=False) as conn:
        now2 = conn.now_ms()
    ev = evaluate_lease_takeovers(e, platform_db, grace_ms=60_000, now_ms=now2)
    assert not ev.firing and ev.resolved


def test_database_evaluator_and_unstored_fallback(platform_db, tmp_path):
    rec = Recorder()
    e = engine(platform_db, rec)
    assert not evaluate_database(e, platform_db).firing

    class Down:
        def health(self):
            return {"ready": False, "dialect": "postgresql", "error": "OperationalError: refused"}
    ev = evaluate_database(e, Down(), name="legacy")
    assert ev.firing and ev.result.created

    class BrokenStore(AlertEngine):
        def raise_alert(self, *a, **k):
            raise RuntimeError("store down")
    ev = evaluate_database(BrokenStore(platform_db, sinks=[rec]), Down(), name="platform")
    assert ev.detail.get("delivered_unstored") is True
    assert rec.sent and rec.sent[-1].reason == "RAISED_UNSTORED"


def test_disk_growth_evaluator(platform_db, tmp_path):
    e = engine(platform_db)
    Usage = namedtuple("Usage", "total used free")
    state = {}
    sizes = iter([1_000, 1_000 + 3 * 1024 ** 3])
    kw = dict(state=state, max_used_fraction=0.85, max_growth_bytes_per_hour=2 * 1024 ** 3,
              size_of=lambda p: next(sizes))
    fill, growth = evaluate_disk_growth(e, tmp_path, usage=lambda p: Usage(100, 50, 50), now_ms=T0, **kw)
    assert not fill.firing and not growth.firing
    fill, growth = evaluate_disk_growth(e, tmp_path, usage=lambda p: Usage(100, 90, 10), now_ms=T0 + 3_600_000, **kw)
    assert fill.firing and fill.result.created and growth.firing
    assert e.get(fill.result.alert_id)["severity"] == "HIGH"
    assert isinstance(state[str(tmp_path)], DiskSample)


def test_backup_age_evaluator(platform_db, tmp_path):
    from datetime import datetime, timedelta, timezone
    e = engine(platform_db)
    now = datetime(2026, 10, 17, 12, tzinfo=timezone.utc)
    ev = evaluate_backup_age(e, tmp_path, now_ms=int(now.timestamp() * 1000))
    assert ev.firing and "No verified" in ev.title

    def manifest(hours_ago, check="ok"):
        at = now - timedelta(hours=hours_ago)
        name = f"cosmicforge-{at.strftime('%Y%m%dT%H%M%SZ')}.db"
        (tmp_path / f"{name}.json").write_text(json.dumps({"file": name, "created_at": at.isoformat(),
                                                           "quick_check": check}))
    manifest(48)
    assert evaluate_backup_age(e, tmp_path, max_age_hours=36, now_ms=int(now.timestamp() * 1000)).firing
    manifest(1, check="corrupt")     # newest is unverified: ignored
    assert evaluate_backup_age(e, tmp_path, max_age_hours=36, now_ms=int(now.timestamp() * 1000)).firing
    manifest(2)
    ev = evaluate_backup_age(e, tmp_path, max_age_hours=36, now_ms=int(now.timestamp() * 1000))
    assert not ev.firing and ev.resolved


def test_run_cycle_isolates_broken_evaluators(platform_db):
    staff(platform_db)
    rec = Recorder()
    e = engine(platform_db, rec)

    def broken():
        raise RuntimeError("signal source down")

    def fine():
        e.raise_alert("STALE_INSTRUMENT_RULES", key="binance", title="rules stale", source="test", now_ms=T0)
    out = run_cycle(e, [broken, fine], now_ms=T0)
    assert out["evaluator_errors"] and out["delivery"]["sent"] >= 1
    assert any(m.category == "STALE_INSTRUMENT_RULES" for m in rec.sent)


def test_cli_tick_reports_owner_required_as_critical(database_url, monkeypatch, capsys, tmp_path):
    from shared_lib.persistence.sql import Database, migrate
    db = Database(database_url)
    migrate(db, all_migrations())
    db.close()
    monkeypatch.setenv("PLATFORM_DATABASE_URL", database_url)
    code = alerting.main(["--backup-dir", str(tmp_path), "--disk-path", str(tmp_path), "--alerts-env", str(tmp_path / "none.env")])
    out = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert code == 2 and out["health"]["owner_required"] is True and not out["evaluator_errors"]
    assert out["delivery"]["sent"] >= 1          # handed to the NOTIFICATION queue
