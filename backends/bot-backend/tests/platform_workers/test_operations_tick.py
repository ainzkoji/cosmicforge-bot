"""Section H Step 8.5 (F2/F3/F6): the operations tick -- one owner across processes and hosts, takeover after a
crash, persisted state and result -- and notification reliability (recovery, suppression windows, dedup evidence,
retries, dead or hanging providers), on SQLite and PostgreSQL.

Lease decisions use injected database-clock times where an exact boundary is tested; the concurrency and CLI
tests use the real database clock. Nothing here contacts a real notification provider.
"""
from __future__ import annotations

import json
import threading
import time
from collections import namedtuple

import pytest

from app.ops import alert_producers as P
from app.ops import alerting
from app.ops.alerting import (AlertEngine, AlertError, evaluate_disk_growth, escalation_policy_from_env, run_loop,
                              run_tick)
from shared_lib.operations.alerting import MAX_SUPPRESSION_MS, UNSUPPRESSIBLE, fingerprint
from shared_lib.operations.oncall import OncallDirectory
from shared_lib.operations.tick_lease import TickLease
from shared_lib.persistence.sql.platform_schema import all_migrations

T0 = 1_800_000_000_000
SEC, MIN = 1_000, 60_000


class Recorder:
    def __init__(self, name="recorder", fail=False, hang_s=0.0):
        self.name, self.sent, self.fail, self.hang_s = name, [], fail, hang_s

    def send(self, message):
        if self.hang_s:
            time.sleep(self.hang_s)
        if self.fail:
            raise ConnectionError("provider unavailable")
        self.sent.append(message)


def staff(db, rotation, levels=(0,)):
    d = OncallDirectory(db)
    for lvl in levels:
        d.assign(rotation_key=rotation, level=lvl, person_name=f"Test Person L{lvl}", contact_channel="EMAIL",
                 contact_address=f"l{lvl}@example.test", actor="admin-test")


# ── tick ownership ─────────────────────────────────────────────────────────────────────────────────────────
def test_two_tickers_one_owner_and_renewal_keeps_the_token(platform_db):
    a = TickLease(platform_db, owner="host-a:1", ttl_ms=3 * MIN)
    b = TickLease(platform_db, owner="host-b:7", ttl_ms=3 * MIN)
    ga = a.acquire(now_ms=T0)
    assert ga.acquired and ga.token == 1 and ga.reason == "ACQUIRED" and not ga.takeover
    gb = b.acquire(now_ms=T0 + SEC)
    assert not gb.acquired and gb.reason == "LEASE_HELD" and gb.holder == "host-a:1"
    again = a.acquire(now_ms=T0 + MIN)
    assert again.acquired and again.token == 1 and again.reason == "RENEWED"
    assert not b.acquire(now_ms=T0 + 3 * MIN + SEC).acquired          # renewed at T0+1min: held until T0+4min


def test_takeover_after_expiry_fences_the_old_owner(platform_db):
    a = TickLease(platform_db, owner="host-a:1", ttl_ms=MIN)
    b = TickLease(platform_db, owner="host-b:7", ttl_ms=MIN)
    ga = a.acquire(now_ms=T0)
    assert a.record(ga.token, started_at_ms=T0, outcome="OK", result={"n": 1}, state={"k": 1}, now_ms=T0 + SEC)
    # host-a freezes (or dies) past its expiry: host-b takes over with the next fencing token
    gb = b.acquire(now_ms=T0 + SEC + MIN + 1)
    assert gb.acquired and gb.reason == "TAKEN_OVER" and gb.takeover and gb.previous_owner == "host-a:1"
    assert gb.token == ga.token + 1 and gb.state == {"k": 1}            # the state follows the lease
    assert a.record(ga.token, started_at_ms=T0, outcome="OK", result={}, state={}, now_ms=T0 + 2 * MIN) is False
    assert a.renew(ga.token, now_ms=T0 + 2 * MIN) is False and a.release(ga.token, now_ms=T0 + 2 * MIN) is False
    assert b.record(gb.token, started_at_ms=T0 + 2 * MIN, outcome="OK", result={"n": 2}, state={"k": 2},
                    now_ms=T0 + 2 * MIN + SEC)
    st = b.status(now_ms=T0 + 2 * MIN + 2 * SEC)
    assert (st["owner"], st["fencing_token"], st["takeovers"], st["last_tick_owner"]) == ("host-b:7", 2, 1, "host-b:7")
    assert st["last_tick_result"] == {"n": 2} and st["held"] is True and st["last_tick_age_ms"] == SEC


def test_clean_release_lets_a_successor_start_at_once(platform_db):
    a = TickLease(platform_db, owner="a", ttl_ms=10 * MIN)
    ga = a.acquire(now_ms=T0)
    assert a.release(ga.token, now_ms=T0 + SEC)
    gb = TickLease(platform_db, owner="b", ttl_ms=10 * MIN).acquire(now_ms=T0 + 2 * SEC)
    assert gb.acquired and gb.reason == "ACQUIRED" and not gb.takeover and gb.token == 2


def test_min_interval_gives_one_tick_per_interval_across_hosts(platform_db):
    a, b = TickLease(platform_db, owner="a"), TickLease(platform_db, owner="b")
    ga = a.acquire(now_ms=T0, min_interval_ms=30 * SEC)
    a.record(ga.token, started_at_ms=T0, outcome="OK", result={}, state={}, now_ms=T0 + SEC)
    a.release(ga.token, now_ms=T0 + SEC)
    early = b.acquire(now_ms=T0 + 10 * SEC, min_interval_ms=30 * SEC)          # timers firing on two hosts
    assert not early.acquired and early.reason == "TICK_NOT_DUE"
    assert b.acquire(now_ms=T0 + 40 * SEC, min_interval_ms=30 * SEC).acquired


def test_concurrent_acquire_yields_exactly_one_owner(platform_db):
    leases = [TickLease(platform_db, owner=f"w{i}", ttl_ms=MIN) for i in range(6)]
    results, start = [], threading.Barrier(len(leases))

    def go(lease):
        start.wait()
        results.append(lease.acquire())
    threads = [threading.Thread(target=go, args=(lease,)) for lease in leases]
    for t in threads:
        t.start()
    for t in threads:
        t.join(30)
    assert len(results) == len(leases)
    assert sum(1 for r in results if r.acquired) == 1
    assert {r.reason for r in results if not r.acquired} == {"LEASE_HELD"}


# ── the tick: persisted state, failures contained ──────────────────────────────────────────────────────────
Usage = namedtuple("Usage", "total used free")


def test_tick_state_survives_a_restart_disk_samples_and_pending_holds(platform_db, tmp_path):
    rec = Recorder()
    engine = AlertEngine(platform_db, sinks=[rec])
    sizes = iter([1_000, 1_000 + 3 * 1024 ** 3])
    clock = {"now": T0}

    def build(state, disk):
        def disk_eval():
            evaluate_disk_growth(engine, tmp_path, state=disk, usage=lambda p: Usage(100, 10, 90),
                                 size_of=lambda p: next(sizes), now_ms=clock["now"])

        def held_eval():
            c = P.ProducerContext(engine, now_ms=clock["now"], state=state)
            P.publish(c, "test_hold", ("STALE_MARKET_DATA",),
                      [P.Signal("STALE_MARKET_DATA", "feed:x", "feed stale", hold_ms=2 * MIN)])
        return [disk_eval, held_eval]
    first = run_tick(engine, TickLease(platform_db, owner="proc-1"), build, release=True, now_ms=clock["now"])
    assert first["ran"] and first["lease"]["recorded"] and first["outcome"] == "OK"
    assert not engine.open_alerts()                         # first sample; the stale signal is still on hold
    clock["now"] = T0 + 3_600_000                            # a new process an hour later (the first one exited)
    second = run_tick(engine, TickLease(platform_db, owner="proc-2"), build, release=True, now_ms=clock["now"])
    assert second["ran"] and second["lease"]["token"] == 2
    cats = {a["category"] for a in engine.open_alerts()}
    assert {"DISK_GROWTH", "STALE_MARKET_DATA"} <= cats      # growth from the persisted sample; hold elapsed
    st = TickLease(platform_db, owner="proc-3").status()
    assert st["last_tick_owner"] == "proc-2" and st["last_tick_outcome"] == "OK"
    assert st["last_tick_result"]["evaluators"] == 2 and st["last_tick_result"]["evaluator_errors"] == []


def test_dead_and_hanging_providers_and_a_broken_evaluator_never_stop_the_tick(platform_db):
    staff(platform_db, "platform-infrastructure")
    dead, slow, good = Recorder("dead", fail=True), Recorder("slow", hang_s=2.0), Recorder("good")
    engine = AlertEngine(platform_db, sinks=[dead, slow, good], send_timeout_s=0.2)

    def broken():
        raise RuntimeError("signal source down")

    def fine():
        engine.raise_alert("QUEUE_BACKLOG", key="q", title="backlog", source="test")
    started = time.monotonic()
    out = run_tick(engine, TickLease(platform_db, owner="t"), lambda state, disk: [broken, fine], release=True)
    assert time.monotonic() - started < 10                   # the hanging provider was not waited for
    assert out["ran"] and out["outcome"] == "DEGRADED" and out["lease"]["recorded"]
    assert out["evaluator_errors"] and "signal source down" in out["evaluator_errors"][0]
    assert out["delivery"] == {"sent": 1, "retry": 2, "abandoned": 0, "skipped": 0}
    [alert] = [a for a in engine.open_alerts() if a["category"] == "QUEUE_BACKLOG"]
    by_sink = {d["sink"]: d for d in engine.get(alert["alert_id"])["deliveries"]}
    assert by_sink["good"]["status"] == "SENT"
    assert by_sink["dead"]["status"] == "RETRY" and "provider unavailable" in by_sink["dead"]["last_error"]
    assert by_sink["slow"]["status"] == "RETRY" and "SINK_TIMEOUT" in by_sink["slow"]["last_error"]


def test_loop_survives_an_unreachable_platform_database_and_releases_on_stop(platform_db, capsys):
    engine = AlertEngine(platform_db, sinks=[Recorder()])
    lease = TickLease(platform_db, owner="looper")
    calls = {"n": 0}
    real_acquire = lease.acquire

    def flaky_acquire(**kw):
        calls["n"] += 1
        if calls["n"] == 1:
            raise ConnectionError("platform database unreachable")
        return real_acquire(**kw)
    lease.acquire = flaky_acquire
    assert run_loop(engine, lease, lambda state, disk: [], interval_s=0.01, max_ticks=3) == 0
    lines = [json.loads(x) for x in capsys.readouterr().out.strip().splitlines()]
    assert lines[0]["ran"] is False and lines[0]["error"] == "ConnectionError"
    assert lines[1]["evaluators"] == 0 and lines[1]["lease"]["grant"] == "ACQUIRED"
    assert lines[2]["lease"]["grant"] == "RENEWED"
    st = lease.status()
    assert st["released_at"] is not None and st["held"] is False        # released for an immediate successor


def test_cli_one_shot_then_a_second_host_waits_its_turn(database_url, monkeypatch, capsys, tmp_path):
    from shared_lib.persistence.sql import Database, migrate
    db = Database(database_url)
    migrate(db, all_migrations())
    db.close()
    monkeypatch.setenv("PLATFORM_DATABASE_URL", database_url)
    args = ["--no-runtime-db", "--alerts-env", str(tmp_path / "none.env")]
    assert alerting.main(args + ["--owner", "host-a:1"]) == 2              # ran; OPERATIONS OWNER REQUIRED
    first = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert first["ran"] and first["lease"]["recorded"] and first["health"]["owner_required"]
    assert alerting.main(args + ["--owner", "host-b:9"]) == 0              # same interval: not this host's turn
    second = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert second == {"ran": False, "reason": "TICK_NOT_DUE", "holder": "host-a:1", "lease_expires_at": None}


def test_cli_loop_runs_the_runtime_producers_from_database_url(database_url, monkeypatch, capsys, tmp_path):
    """The container ``ops-tick`` service: ``--loop`` over the platform database and the runtime database named by
    DATABASE_URL. A kill switch engaged in the runtime database becomes an alert within one tick."""
    from shared_lib.persistence.db import DB
    from shared_lib.persistence.migrations import migrate as legacy_migrate
    from shared_lib.persistence.sql import Database, migrate
    db = Database(database_url)
    migrate(db, all_migrations())
    db.close()
    runtime = tmp_path / "runtime.db"
    rdb = DB(str(runtime))
    legacy_migrate(rdb)
    with rdb.connect() as conn:
        conn.execute("INSERT INTO cati_governance_controls (control_event_id, control, state, scope, recorded_at, "
                     "schema_version, table_version, payload, payload_hash) VALUES (?,?,?,?,?,?,?,?,?)",
                     ("ctrl-cli", "KILL_NEW_CATI_ENTRIES", "ON", "GLOBAL", int(time.time() * 1000), "1", "1",
                      json.dumps({"reason": "cli test"}), "h"))
    monkeypatch.setenv("PLATFORM_DATABASE_URL", database_url)
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{runtime.as_posix()}")
    code = alerting.main(["--loop", "--interval", "5", "--max-ticks", "1", "--owner", "ops-tick-1",
                          "--alerts-env", str(tmp_path / "none.env")])
    assert code == 0
    line = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert line["evaluator_errors"] == [] and line["evaluators"] >= 20 and line["lease"]["recorded"]
    db = Database(database_url)
    try:
        engine = AlertEngine(db)
        assert any(a["fingerprint"] == fingerprint("KILL_SWITCH_ACTIVATED", {"kill_switch": "ctrl-cli"})
                   for a in engine.open_alerts())
        assert TickLease(db, owner="x").status()["held"] is False           # released at the end of the loop
    finally:
        db.close()


# ── notification reliability ───────────────────────────────────────────────────────────────────────────────
def test_recovery_notification_goes_to_who_was_told_and_a_human_resolution_sends_none(platform_db):
    staff(platform_db, "market-data", levels=(0, 1))
    rec = Recorder()
    e = AlertEngine(platform_db, sinks=[rec])
    a = e.raise_alert("STALE_MARKET_DATA", key="feed", title="feed stale", source="test", now_ms=T0)
    e.escalate_due(now_ms=T0 + 15 * MIN)                                   # HIGH: level 1 after 15 min
    assert e.resolve_condition("STALE_MARKET_DATA", key="feed", actor="evaluator:test", now_ms=T0 + 16 * MIN)
    e.deliver_pending(now_ms=T0 + 16 * MIN)
    recovered = [m for m in rec.sent if m.reason == "RECOVERED"]
    assert len(recovered) == 1 and recovered[0].subject().startswith("[RECOVERED] [HIGH] STALE_MARKET_DATA")
    assert recovered[0].escalation_level == 1 and [t["person_name"] for t in recovered[0].targets] == ["Test Person L1"]
    b = e.raise_alert("STALE_MARKET_DATA", key="other", title="t", source="test", now_ms=T0)
    e.resolve(b.alert_id, actor="admin:1", note="fixed by hand", now_ms=T0 + MIN)
    assert not [d for d in e.get(b.alert_id)["deliveries"] if d["reason"] == "RECOVERED"]


def test_maintenance_window_records_but_does_not_deliver_or_escalate(platform_db):
    staff(platform_db, "platform-infrastructure", levels=(0, 1))
    rec = Recorder()
    e = AlertEngine(platform_db, sinks=[rec], dedup_window_ms=10 * MIN)
    w = e.add_suppression(category="DATABASE_UNAVAILABLE", starts_at_ms=T0, ends_at_ms=T0 + HOUR_MS,
                          reason="planned PostgreSQL upgrade", actor="admin:1", now_ms=T0)
    a = e.raise_alert("DATABASE_UNAVAILABLE", key="db", title="db down", source="test", now_ms=T0 + MIN)
    assert a.created and a.deliveries == []
    other = e.raise_alert("QUEUE_BACKLOG", key="q", title="backlog", source="test", now_ms=T0 + MIN)
    assert other.deliveries                                               # another category is not silenced
    assert e.escalate_due(now_ms=T0 + 30 * MIN) == [] or all(x != a.alert_id for x, _ in e.escalate_due(now_ms=T0 + 30 * MIN))
    got = e.get(a.alert_id)
    assert got["status"] == "OPEN" and got["escalation_level"] == 0 and got["deliveries"] == []
    assert "SUPPRESSED" in [ev["event"] for ev in got["events"]]          # recorded, with the window named
    assert [ev["detail"]["suppression_id"] for ev in got["events"] if ev["event"] == "SUPPRESSED"] == [w["suppression_id"]]
    assert e.health(now_ms=T0 + MIN)["active_suppressions"] == 1
    # after the window: the still-open alert is delivered as a reminder, and escalation resumes
    after = e.raise_alert("DATABASE_UNAVAILABLE", key="db", title="db down", source="test", now_ms=T0 + HOUR_MS + MIN)
    assert after.reminded and after.deliveries
    assert any(x == a.alert_id for x, _ in e.escalate_due(now_ms=T0 + HOUR_MS + MIN))


def test_suppression_rules_refuse_unsafe_windows_and_can_be_cancelled(platform_db):
    e = AlertEngine(platform_db, sinks=[Recorder()])
    for category in sorted(UNSUPPRESSIBLE):
        with pytest.raises(AlertError) as err:
            e.add_suppression(category=category, ends_at_ms=T0 + MIN, reason="r", actor="admin", now_ms=T0)
        assert err.value.code == "CATEGORY_NOT_SUPPRESSIBLE"
    for kw, code in ((dict(ends_at_ms=T0 + MAX_SUPPRESSION_MS + 1), "SUPPRESSION_TOO_LONG"),
                     (dict(ends_at_ms=T0 - 1), "INVALID_WINDOW"), (dict(ends_at_ms=T0 + MIN, reason=""), "REASON_REQUIRED"),
                     (dict(ends_at_ms=T0 + MIN, actor=""), "ACTOR_REQUIRED")):
        args = dict(category=None, reason="r", actor="admin", now_ms=T0)
        args.update(kw)
        with pytest.raises(AlertError) as err:
            e.add_suppression(**args)
        assert err.value.code == code
    # a window for every category still delivers the unsuppressible ones
    w = e.add_suppression(category=None, ends_at_ms=T0 + 30 * MIN, reason="host maintenance", actor="admin", now_ms=T0)
    assert e.raise_alert("UNPROTECTED_POSITION", key="p", title="naked", source="t", now_ms=T0 + MIN).deliveries
    assert not e.raise_alert("DISK_GROWTH", key="d", title="disk", source="t", now_ms=T0 + MIN).deliveries
    e.cancel_suppression(w["suppression_id"], actor="admin", now_ms=T0 + 2 * MIN)
    assert e.raise_alert("API_OUTAGE", key="api", title="api", source="t", now_ms=T0 + 3 * MIN).deliveries
    with pytest.raises(AlertError):
        e.cancel_suppression(w["suppression_id"], actor="admin")
    assert e.suppressions()[0]["cancelled_by"] == "admin"


def test_repeat_signals_are_counted_but_logged_once_per_notification(platform_db):
    e = AlertEngine(platform_db, sinks=[Recorder()], dedup_window_ms=10 * MIN)
    a = e.raise_alert("QUEUE_BACKLOG", key="q", title="t", source="t", now_ms=T0)
    for i in range(1, 9):                                                  # an evaluator re-confirming every minute
        e.raise_alert("QUEUE_BACKLOG", key="q", title="t", source="t", now_ms=T0 + i * MIN)
    got = e.get(a.alert_id)
    assert got["occurrences"] == 9 and got["last_seen_at"] == T0 + 8 * MIN
    assert [ev["event"] for ev in got["events"]].count("DEDUPLICATED") == 1
    e.raise_alert("QUEUE_BACKLOG", key="q", title="t", source="t", now_ms=T0 + 11 * MIN)     # reminder
    e.raise_alert("QUEUE_BACKLOG", key="q", title="t", source="t", now_ms=T0 + 12 * MIN)
    events = [ev["event"] for ev in e.get(a.alert_id)["events"]]
    assert events.count("REMINDED") == 1 and events.count("DEDUPLICATED") == 2


def test_event_alerts_raise_once(platform_db):
    e = AlertEngine(platform_db, sinks=[Recorder()])
    first = e.raise_alert("KILL_SWITCH_ACTIVATED", key={"kill_switch": "c1"}, title="t", source="t", once=True, now_ms=T0)
    assert first.created
    assert e.raise_alert("KILL_SWITCH_ACTIVATED", key={"kill_switch": "c1"}, title="t", source="t", once=True,
                         now_ms=T0 + MIN).deduplicated
    e.resolve(first.alert_id, actor="admin", note="intended", now_ms=T0 + 2 * MIN)
    again = e.raise_alert("KILL_SWITCH_ACTIVATED", key={"kill_switch": "c1"}, title="t", source="t", once=True,
                          now_ms=T0 + 3 * MIN)
    assert not again.created and again.alert_id == first.alert_id
    assert not [a for a in e.open_alerts() if a["category"] == "KILL_SWITCH_ACTIVATED"]
    assert e.raise_alert("KILL_SWITCH_ACTIVATED", key={"kill_switch": "c1"}, title="t", source="t",
                         now_ms=T0 + 4 * MIN).created                      # without once: a recurrence is new


def test_a_channel_never_receives_a_secret_or_email_even_from_a_row_written_around_the_engine(platform_db):
    rec = Recorder()
    e = AlertEngine(platform_db, sinks=[rec])
    a = e.raise_alert("API_OUTAGE", key="api", title="api down", source="t", now_ms=T0)
    token = "tok" + "Q" * 30
    email = "someone" + "@" + "example.org"
    with platform_db.transaction() as conn:                                # e.g. an older version or a manual fix
        conn.execute("UPDATE ops_alerts SET title=?, detail=? WHERE alert_id=?",
                     (f"down; refresh_token={token}", json.dumps({"note": f"Authorization: Bearer {token}",
                                                                   "who": email}), a.alert_id))
    e.deliver_pending(now_ms=T0)
    [m] = [m for m in rec.sent if m.alert_id == a.alert_id]
    text = m.subject() + m.text() + json.dumps(m.detail)
    assert token not in text and email not in text


def test_escalation_intervals_are_configurable_defaults_pending_the_owner(platform_db):
    policy = escalation_policy_from_env({"OPS_ESCALATION_MINUTES": json.dumps({"critical": [2, 10]}),
                                         "OPS_DEDUP_WINDOW_MINUTES": "5"})
    assert policy["escalation_minutes"]["CRITICAL"] == (2, 10) and policy["escalation_minutes"]["HIGH"] == (15, 60)
    assert policy["dedup_window_ms"] == 5 * MIN
    staff(platform_db, "trading-operations", levels=(0, 1))
    e = AlertEngine(platform_db, sinks=[Recorder()], **policy)
    a = e.raise_alert("UNPROTECTED_POSITION", key="p", title="naked", source="t", now_ms=T0)
    assert e.escalate_due(now_ms=T0 + 2 * MIN) == [(a.alert_id, 1)]
    assert escalation_policy_from_env({}) == {}
    with pytest.raises(AlertError):
        AlertEngine(platform_db, escalation_minutes={"CRITICAL": (15, 5)})            # must increase


HOUR_MS = 3_600_000


# ── scheduling artifacts ───────────────────────────────────────────────────────────────────────────────────
def test_systemd_timer_and_container_service_schedule_the_tick():
    from pathlib import Path
    import yaml
    root = Path(__file__).resolve().parents[4]
    service = (root / "deploy/systemd/cosmicforge-ops-tick.service").read_text(encoding="utf-8")
    timer = (root / "deploy/systemd/cosmicforge-ops-tick.timer").read_text(encoding="utf-8")
    assert "-m app.ops.alerting" in service and "Type=oneshot" in service and "--loop" not in service
    assert "SuccessExitStatus=2" in service and "OnFailure=cosmicforge-alert@%n.service" in service
    assert "--backup-dir /var/backups/cosmicforge" in service and "User=cosmicforge" in service
    assert "OnCalendar=*:*:00" in timer and "Persistent=true" in timer
    compose = yaml.safe_load((root / "deploy/staging/docker-compose.staging.yml").read_text(encoding="utf-8"))
    tick = compose["services"]["ops-tick"]
    assert tick["command"][:6] == ["python", "-u", "-m", "app.ops.alerting", "--loop", "--interval"]
    assert tick["environment"]["DATABASE_URL"].startswith("sqlite:////var/lib/cosmicforge/")
    assert tick["read_only"] is True and tick["depends_on"]["migrate"]["condition"] == "service_completed_successfully"
    assert tick["stop_signal"] == "SIGTERM" and "profiles" not in tick
