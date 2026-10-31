"""Section H Step 8.5 (F1/F6): every alert producer over durable state, on SQLite and PostgreSQL.

The legacy runtime database is a throwaway SQLite file migrated with the real schema (``migrate`` + the production,
ownership and billing schema steps); rows are written straight into the real tables to stage each condition. The
alert store is the platform database of ``platform_db`` (both dialects). For every producer the test checks the
alert it raises -- category, severity, dedup key (fingerprint), routing (rotation), scope and public visibility --
and that it resolves itself, with a RECOVERED notification, once the condition clears. No real venue, provider or
production database is touched; every identifier here is synthetic.
"""
from __future__ import annotations

import json
import shutil
import sqlite3
import uuid
from types import SimpleNamespace

import pytest

from app.ops import alert_producers as P
from app.ops.alerting import (CATEGORIES, AlertEngine, default_evaluators, evaluate_backup_status, evaluate_database,
                              run_cycle)
from shared_lib.operations.alerting import fingerprint, pseudonym
from shared_lib.operations.status_page import public_status

T0 = 1_800_000_000_000
SEC, MIN, HOUR, DAY = 1_000, 60_000, 3_600_000, 86_400_000
A1, A2, A3 = "broker-acct-0001", "broker-acct-0002", "broker-acct-0003"
TRACKER_DDL = ("CREATE TABLE IF NOT EXISTS cati_residual_tracker (registry_hash TEXT PRIMARY KEY, activated_at INTEGER "
               "NOT NULL, first_decision_time INTEGER NOT NULL, last_decision_time INTEGER, heartbeat_at INTEGER, "
               "status TEXT NOT NULL, detail_json TEXT NOT NULL)")


class Recorder:
    def __init__(self, name="recorder"):
        self.name, self.sent = name, []

    def send(self, message):
        self.sent.append(message)


# ── fixtures ───────────────────────────────────────────────────────────────────────────────────────────────
@pytest.fixture(scope="module")
def runtime_template(tmp_path_factory):
    from shared_lib.billing.schema import ensure_billing_launch_schema, ensure_billing_schema
    from shared_lib.persistence.db import DB
    from shared_lib.persistence.migrations import migrate
    from app.execution import production_schema
    from app.ops.runtime_ownership import ensure_ownership_schema
    path = tmp_path_factory.mktemp("runtime") / "runtime-template.db"
    db = DB(str(path))
    migrate(db)
    production_schema.ensure(db)
    ensure_ownership_schema(db)
    with db.connect() as conn:
        ensure_billing_schema(conn)
        ensure_billing_launch_schema(conn)
        conn.execute(TRACKER_DDL)
    raw = sqlite3.connect(str(path))
    raw.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    raw.close()
    return path


@pytest.fixture
def runtime_db(runtime_template, tmp_path):
    from shared_lib.persistence.db import DB
    path = tmp_path / "runtime.db"
    shutil.copy2(runtime_template, path)
    return DB(str(path))


@pytest.fixture
def rec():
    return Recorder()


@pytest.fixture
def engine(platform_db, rec):
    return AlertEngine(platform_db, sinks=[rec])


def ctx(engine, runtime_db, now, *, platform_db=None, state=None):
    return P.ProducerContext(engine, runtime_db=runtime_db, platform_db=platform_db, now_ms=now,
                             state={} if state is None else state)


def put(db, table, **values):
    """Insert a row into a real table; NOT NULL columns the test does not care about get neutral values."""
    with db.connect() as conn:
        row = dict(values)
        for _, name, ctype, notnull, default, pk in conn.execute(f'PRAGMA table_info("{table}")').fetchall():
            if name in row or not notnull or default is not None or (pk and "INT" in (ctype or "").upper()):
                continue
            t = (ctype or "").upper()
            row[name] = 0 if "INT" in t else (0.0 if ("REAL" in t or "NUM" in t) else f"{name}-{uuid.uuid4().hex[:6]}")
        cur = conn.execute(f'INSERT INTO "{table}" ({", ".join(row)}) VALUES ({", ".join("?" * len(row))})',
                           tuple(row.values()))
        return cur.lastrowid


def sql(db, statement, params=()):
    with db.connect() as conn:
        conn.execute(statement, params)


def open_of(engine, category, source=None):
    return [a for a in engine.open_alerts() if a["category"] == category and (source is None or a["source"] == source)]


def assert_raised(engine, platform_db, category, key, *, source, severity, account_ref=None, public=None):
    found = [a for a in engine.open_alerts() if a["fingerprint"] == fingerprint(category, key)]
    assert len(found) == 1, (category, key, [(a["category"], a["title"]) for a in engine.open_alerts()])
    a = found[0]
    policy = CATEGORIES[category]
    assert (a["category"], a["severity"], a["source"]) == (category, severity, source)
    assert a["rotation_key"] == policy.rotation_key                       # routing is the category's rotation
    assert a["scope"] == ("ACCOUNT" if account_ref else "PLATFORM") and a["account_ref"] == account_ref
    page = public_status(platform_db)
    text = json.dumps(page)
    if public is not None:
        assert policy.component is not None
        assert {c["key"]: c["status"] for c in page["components"]}[policy.component] == public
    if account_ref:                         # no customer identifier in the page, the title or the detail
        assert account_ref not in text and pseudonym(account_ref) not in text
        assert account_ref not in a["title"] and account_ref not in json.dumps(a["detail"])
    return a


def assert_recovered(engine, alert_id, source):
    a = engine.get(alert_id)
    assert a["status"] == "RESOLVED" and a["resolved_by"] == f"evaluator:{source}", a["status"]
    assert any(d["reason"] == "RECOVERED" for d in a["deliveries"])
    return a


def components(platform_db):
    return {c["key"]: c["status"] for c in public_status(platform_db)["components"]}


# ── every producer runs against the real schema ────────────────────────────────────────────────────────────
def test_every_producer_runs_against_the_real_runtime_schema_without_error(engine, platform_db, runtime_db, tmp_path):
    evaluators = default_evaluators(engine, platform_db, backup_dir=tmp_path,          # (disk fill depends on the
                                    runtime_db=runtime_db, runtime_db_path=runtime_db.path,  # host: not checked here)
                                    producer_state={})
    names = [getattr(e, "__name__", "?") for e in evaluators]
    assert len(names) == len(set(names)) and len(evaluators) == 26, names      # 24 + alert delivery + platform dump status
    out = run_cycle(engine, evaluators)
    assert out["evaluator_errors"] == [] and out["evaluators"] == len(evaluators)
    # an empty runtime database raises nothing but the backup alert (no backup in tmp_path)
    assert {a["category"] for a in engine.open_alerts()} <= {"BACKUP_FAILURE", "OPERATIONS_OWNER_REQUIRED"}


# ── 1. worker crash ────────────────────────────────────────────────────────────────────────────────────────
def test_worker_crash_runtime_owner_and_stalled_platform_cycle(engine, platform_db, runtime_db):
    stale = P.ms_iso(T0 - 200 * SEC)
    put(runtime_db, "runtime_ownership", lease_name="trading-runtime", database_path="/srv/rt.db", runtime_owner_id="o1",
        pid=4242, hostname="host-a", started_at=stale, heartbeat_at=stale)
    put(runtime_db, "cati_platform_cycles", cycle_id="c1", family_id="F", cycle_key="k", mode="SHADOW", status="STARTED",
        started_at=T0 - 10 * MIN, heartbeat_at=T0 - 5 * MIN)
    out = P.evaluate_runtime_workers(ctx(engine, runtime_db, T0))
    assert out["firing"] == 2 and out["created"] == 2
    owner = assert_raised(engine, platform_db, "WORKER_OUTAGE", {"runtime_owner": "trading-runtime"},
                          source="runtime_workers", severity="CRITICAL", public="DEGRADED")
    cycle = assert_raised(engine, platform_db, "WORKER_OUTAGE", "platform_cycle:stalled", source="runtime_workers",
                          severity="CRITICAL")
    assert "host-a" not in json.dumps(owner)                                # no host name in the alert
    P.evaluate_runtime_workers(ctx(engine, runtime_db, T0 + MIN))            # dedup: same alerts, counted
    assert len(open_of(engine, "WORKER_OUTAGE")) == 2 and engine.get(owner["alert_id"])["occurrences"] == 2
    sql(runtime_db, "UPDATE runtime_ownership SET heartbeat_at=?", (P.ms_iso(T0 + 2 * MIN),))
    sql(runtime_db, "UPDATE cati_platform_cycles SET status='COMPLETED'")
    assert P.evaluate_runtime_workers(ctx(engine, runtime_db, T0 + 2 * MIN + SEC))["resolved"] == 2
    assert_recovered(engine, owner["alert_id"], "runtime_workers")
    assert_recovered(engine, cycle["alert_id"], "runtime_workers")


# ── 2. failed lease takeover ───────────────────────────────────────────────────────────────────────────────
def test_failed_lease_takeover_on_runtime_account_leases(engine, platform_db, runtime_db):
    put(runtime_db, "cati_account_leases", account_id=A1, owner_id="w-1", purpose="CYCLE", fencing_token=3,
        acquired_at=T0 - 10 * MIN, renewed_at=T0 - 9 * MIN, expires_at=T0 - 5 * MIN)
    put(runtime_db, "cati_account_leases", account_id=A2, owner_id="w-1", purpose="CYCLE", fencing_token=1,
        acquired_at=T0 - MIN, renewed_at=T0 - MIN, expires_at=T0 + MIN)   # held: fine
    P.evaluate_account_leases(ctx(engine, runtime_db, T0))
    a = assert_raised(engine, platform_db, "LEASE_TAKEOVER_FAILURE", "account_leases:abandoned", source="account_leases",
                      severity="HIGH", public="DEGRADED")
    assert a["detail"]["abandoned_leases"] == 1 and a["detail"]["accounts"] == [pseudonym(A1)]
    assert A1 not in json.dumps(a)
    sql(runtime_db, "UPDATE cati_account_leases SET owner_id='w-2', fencing_token=4, expires_at=? WHERE account_id=?",
        (T0 + 5 * MIN, A1))                                                  # taken over
    assert P.evaluate_account_leases(ctx(engine, runtime_db, T0 + MIN))["resolved"] == 1
    assert_recovered(engine, a["alert_id"], "account_leases")


# ── 4. database outage (runtime database) ──────────────────────────────────────────────────────────────────
def test_database_outage_of_the_runtime_database(engine, platform_db, runtime_db, tmp_path):
    ev = evaluate_database(engine, P.RuntimeDatabaseProbe(str(tmp_path / "gone.db")), name="runtime", now_ms=T0)
    assert ev.firing
    a = assert_raised(engine, platform_db, "DATABASE_UNAVAILABLE", {"database": "runtime"}, source="database_health",
                      severity="CRITICAL", public="DEGRADED")
    assert a["detail"]["error"] == "RUNTIME_DATABASE_FILE_MISSING"
    ev = evaluate_database(engine, P.RuntimeDatabaseProbe(runtime_db.path), name="runtime", now_ms=T0 + MIN)
    assert not ev.firing and ev.resolved
    assert_recovered(engine, a["alert_id"], "database_health")


# ── 5. exchange connection loss ────────────────────────────────────────────────────────────────────────────
def test_exchange_connection_loss_accounts_streams_and_venue_bans(engine, platform_db, runtime_db):
    put(runtime_db, "cati_production_state", account_id=A1, user_id="user-1", observed_at=T0,
        document=json.dumps({"status": "READ_FAILED", "reason": "VENUE_TIMEOUT"}))
    put(runtime_db, "cati_user_stream_state", venue="binance", environment="DEMO", account_ref=A2, health="DISCONNECTED",
        health_reason="SOCKET_CLOSED", connects=3, last_frame_at_ms=T0 - 5 * MIN, updated_at_ms=T0 - 5 * MIN)
    put(runtime_db, "cati_user_stream_state", venue="bybit", environment="DEMO", account_ref=A3, health="DISCONNECTED",
        health_reason="NEVER_CONNECTED", connects=0, updated_at_ms=T0)    # never connected: not a loss
    put(runtime_db, "cati_rate_budgets", venue="bybit", scope_key="198.51.100.20", scope="IP", capacity=600.0,
        window_ms=60_000, closed_until_ms=T0 + 30 * SEC, closed_reason="RATE_LIMITED", closed_at_ms=T0 - 10 * SEC,
        updated_at_ms=T0)                                                    # an ordinary short pause: silent
    state: dict = {}
    first = P.evaluate_exchange_connectivity(ctx(engine, runtime_db, T0, state=state))
    assert first["held"] == 2 and not open_of(engine, "EXCHANGE_CONNECTIVITY_LOSS")      # must persist 2 min
    P.evaluate_exchange_connectivity(ctx(engine, runtime_db, T0 + 3 * MIN, state=state))
    acct = assert_raised(engine, platform_db, "EXCHANGE_CONNECTIVITY_LOSS", {"account": A1, "kind": "account_read"},
                         source="exchange_connectivity", severity="HIGH", account_ref=A1, public="OPERATIONAL")
    stream = assert_raised(engine, platform_db, "EXCHANGE_CONNECTIVITY_LOSS",
                           {"account": A2, "kind": "user_stream", "venue": "binance", "env": "DEMO"},
                           source="exchange_connectivity", severity="HIGH", account_ref=A2, public="OPERATIONAL")
    assert stream["detail"]["health"] == "DISCONNECTED"
    # a venue ban is platform-wide: the public "Exchange connectivity" component degrades (no address shown)
    put(runtime_db, "cati_rate_budgets", venue="binance", scope_key="203.0.113.7", scope="IP", capacity=1200.0,
        window_ms=60_000, closed_until_ms=T0 + 20 * MIN, closed_reason="BANNED", closed_status=418,
        closed_at_ms=T0 + 2 * MIN, updated_at_ms=T0 + 2 * MIN)
    P.evaluate_exchange_connectivity(ctx(engine, runtime_db, T0 + 4 * MIN, state=state))
    ban = assert_raised(engine, platform_db, "EXCHANGE_CONNECTIVITY_LOSS",
                        {"rate_closure": "binance", "scope_key": "203.0.113.7"}, source="exchange_connectivity",
                        severity="HIGH", public="DEGRADED")
    assert "203.0.113.7" not in json.dumps(ban)
    sql(runtime_db, "UPDATE cati_production_state SET document=?", (json.dumps({"status": "SYNCED"}),))
    sql(runtime_db, "UPDATE cati_user_stream_state SET health='LIVE', lease_until_ms=? WHERE account_ref=?",
        (T0 + HOUR, A2))
    out = P.evaluate_exchange_connectivity(ctx(engine, runtime_db, T0 + 30 * MIN, state=state))
    assert out["resolved"] == 3 and state.get("pending", {}).get("exchange_connectivity") is None
    for a in (acct, stream, ban):
        assert_recovered(engine, a["alert_id"], "exchange_connectivity")


# ── 6. stale market data ───────────────────────────────────────────────────────────────────────────────────
def test_stale_market_data_collector_and_daily_feed(engine, platform_db, runtime_db):
    sql(runtime_db, "INSERT INTO cati_residual_tracker VALUES (?,?,?,?,?,?,?)",
        ("f3cac769" * 8, T0 - DAY, T0 - DAY, T0 - HOUR, T0 - 10 * MIN, "COLLECTING", "{}"))
    put(runtime_db, "cati_feed_updates", update_id=1, status="COMPLETED", started_at=T0 - 41 * HOUR,
        completed_at=T0 - 40 * HOUR)
    put(runtime_db, "cati_feed_updates", update_id=2, status="FAILED", started_at=T0 - 2 * HOUR,
        completed_at=T0 - 2 * HOUR)
    P.evaluate_market_data(ctx(engine, runtime_db, T0))
    collector = assert_raised(engine, platform_db, "STALE_MARKET_DATA", {"collector": "f3cac769" * 8},
                              source="market_data", severity="HIGH", public="DEGRADED")
    failed = assert_raised(engine, platform_db, "STALE_MARKET_DATA", "daily_feed:last_update_failed",
                           source="market_data", severity="HIGH")
    stale = assert_raised(engine, platform_db, "STALE_MARKET_DATA", "daily_feed:stale", source="market_data",
                          severity="HIGH")
    sql(runtime_db, "UPDATE cati_residual_tracker SET heartbeat_at=?", (T0 + MIN,))
    put(runtime_db, "cati_feed_updates", update_id=3, status="COMPLETED", started_at=T0, completed_at=T0 + MIN)
    assert P.evaluate_market_data(ctx(engine, runtime_db, T0 + 2 * MIN))["resolved"] == 3
    for a in (collector, failed, stale):
        assert_recovered(engine, a["alert_id"], "market_data")


# ── 7. stale instrument rules ──────────────────────────────────────────────────────────────────────────────
def test_stale_and_failing_instrument_rules(engine, platform_db, runtime_db):
    put(runtime_db, "cati_instrument_rule_refreshes", venue="binance", environment="DEMO", last_attempt_ms=T0 - HOUR,
        last_success_ms=T0 - 30 * HOUR, consecutive_failures=5, last_error="RULES_REFRESH_FAILED:TIMEOUT",
        updated_at_ms=T0 - HOUR)
    put(runtime_db, "cati_instrument_rule_refreshes", venue="bybit", environment="DEMO", last_attempt_ms=T0 - MIN,
        last_success_ms=T0 - 2 * HOUR, consecutive_failures=3, last_error="RULES_REFRESH_EMPTY", updated_at_ms=T0)
    put(runtime_db, "cati_instrument_rule_refreshes", venue="bingx", environment="DEMO", last_attempt_ms=T0 - 5 * DAY,
        last_success_ms=T0 - 5 * DAY, updated_at_ms=T0 - 5 * DAY)          # no longer refreshed: not in use
    P.evaluate_instrument_rules(ctx(engine, runtime_db, T0))
    stale = assert_raised(engine, platform_db, "STALE_INSTRUMENT_RULES", {"venue": "binance", "env": "DEMO", "kind": "stale"},
                          source="instrument_rules", severity="HIGH", public="DEGRADED")
    failing = assert_raised(engine, platform_db, "STALE_INSTRUMENT_RULES",
                            {"venue": "bybit", "env": "DEMO", "kind": "failing"}, source="instrument_rules",
                            severity="MEDIUM")
    assert len(open_of(engine, "STALE_INSTRUMENT_RULES")) == 2
    sql(runtime_db, "UPDATE cati_instrument_rule_refreshes SET last_success_ms=?, consecutive_failures=0", (T0,))
    assert P.evaluate_instrument_rules(ctx(engine, runtime_db, T0 + MIN))["resolved"] == 2
    assert_recovered(engine, stale["alert_id"], "instrument_rules")
    assert_recovered(engine, failing["alert_id"], "instrument_rules")


# ── 8. reconciliation mismatch ─────────────────────────────────────────────────────────────────────────────
def test_reconciliation_mismatch_streams_transfers_and_position_corrections(engine, platform_db, runtime_db):
    put(runtime_db, "cati_user_stream_reconciliations", reconcile_id="r1", venue="binance", environment="DEMO",
        account_ref=A1, trigger_reason="PERIODIC", status="OK", started_at_ms=T0 - 10 * MIN,
        completed_at_ms=T0 - 10 * MIN, document=json.dumps({"difference_count": 2, "differences": ["x", "y"]}))
    put(runtime_db, "broker_transfer_reconciliations", broker_account_id=A2, user_id="user-2", status="COMPLETED",
        still_unresolved=1, run_at=P.ms_iso(T0 - HOUR))
    put(runtime_db, "bot_instances", id="bot-9", user_id="user-3", broker_account_id=A3)
    put(runtime_db, "reconciliation_events", event_id="e1", bot_instance_id="bot-9", occurred_at=P.ms_iso(T0 - 5 * MIN),
        scope="BROKER_POSITION", action="CORRECT_LOCAL_POSITION", reason="BROKER_QUANTITY_AUTHORITATIVE",
        result="APPLIED")
    P.evaluate_reconciliation(ctx(engine, runtime_db, T0))
    stream = assert_raised(engine, platform_db, "RECONCILIATION_MISMATCH",
                           {"account": A1, "venue": "binance", "env": "DEMO", "kind": "stream_reconcile"},
                           source="reconciliation", severity="HIGH", account_ref=A1)
    transfer = assert_raised(engine, platform_db, "RECONCILIATION_MISMATCH", {"account": A2, "kind": "transfer_reconcile"},
                             source="reconciliation", severity="HIGH", account_ref=A2)
    # three accounts with open account-scoped trading alerts: the public page degrades in aggregate, naming nobody
    corrections = assert_raised(engine, platform_db, "RECONCILIATION_MISMATCH", {"bot": "bot-9", "kind": "position_corrections"},
                                source="reconciliation", severity="HIGH", account_ref=A3, public="DEGRADED")
    assert corrections["detail"]["position_corrections"] == 1
    put(runtime_db, "cati_user_stream_reconciliations", reconcile_id="r2", venue="binance", environment="DEMO",
        account_ref=A1, trigger_reason="PERIODIC", status="OK", started_at_ms=T0 + MIN, completed_at_ms=T0 + MIN,
        document=json.dumps({"difference_count": 0}))
    put(runtime_db, "broker_transfer_reconciliations", broker_account_id=A2, user_id="user-2", status="COMPLETED",
        still_unresolved=0, run_at=P.ms_iso(T0 + MIN))
    assert P.evaluate_reconciliation(ctx(engine, runtime_db, T0 + 2 * HOUR))["resolved"] == 3
    for a in (stream, transfer, corrections):
        assert_recovered(engine, a["alert_id"], "reconciliation")
    assert components(platform_db)["TRADING_ENGINE"] == "OPERATIONAL"


# ── 9. unknown order outcome ───────────────────────────────────────────────────────────────────────────────
def test_unknown_order_outcomes_from_every_execution_ledger(engine, platform_db, runtime_db):
    from app.platform_workers.assignments import WorkerAssignments
    from app.platform_workers.jobs import OrderIntents
    with platform_db.transaction(write=False) as conn:
        now = conn.now_ms()                       # the platform rows use the database clock; so does this test
    put(runtime_db, "cati_portfolio_reservations", reservation_id="res-1", broker_account_id=A1, bot_instance_id="b1",
        trade_plan_id="tp-1", status="RESOLUTION_PENDING", created_at=now - 10 * MIN, expires_at=now + HOUR,
        updated_at=now - 5 * MIN, pending_since=now - 5 * MIN, resolution_deadline=now + 10 * MIN)
    put(runtime_db, "cati_execution_attempts", record_id="ar-1", execution_attempt_id="att-1", sequence=1,
        trade_plan_id="tp-1", broker_account_id=A1, bot_instance_id="b1", status="SUBMIT_UNKNOWN",
        recorded_at=now - 4 * MIN, payload="{}", payload_hash="h")
    put(runtime_db, "cati_execution_intents", intent_id="int-1", account_id=A2, venue="binance", environment="DEMO",
        family_id="F", decision_id="d1", symbol="BTCUSDT", kind="ENTRY", direction="LONG", sequence=1, role="ENTRY",
        client_order_id="c1", state="UNKNOWN", created_at=now - 3 * MIN, updated_at=now - 3 * MIN)
    WorkerAssignments(platform_db).upsert_accounts([(A3, "tenant-3", "binance", "DEMO")])
    intent = OrderIntents(platform_db).prepare(idempotency_key="k-1", account_id=A3, tenant_id="tenant-3", venue="binance",
                                               symbol="BTCUSDT", side="BUY", quantity="0.001", limit_price="65000")
    with platform_db.transaction() as conn:
        conn.execute("UPDATE platform_order_intents SET status='SUBMITTING', updated_at=? WHERE intent_id=?",
                     (now - 10 * MIN, intent.intent_id))
    c = ctx(engine, runtime_db, None, platform_db=platform_db)
    P.evaluate_execution_uncertainty(c)
    a1 = assert_raised(engine, platform_db, "EXCHANGE_EXECUTION_UNCERTAINTY", {"account": A1},
                       source="execution_uncertainty", severity="CRITICAL", account_ref=A1)
    assert a1["detail"]["by_source"] == {"reservation_resolution_pending": 1, "attempt_submit_unknown": 1}
    a2 = assert_raised(engine, platform_db, "EXCHANGE_EXECUTION_UNCERTAINTY", {"account": A2},
                       source="execution_uncertainty", severity="CRITICAL", account_ref=A2)
    a3 = assert_raised(engine, platform_db, "EXCHANGE_EXECUTION_UNCERTAINTY", {"account": A3},
                       source="execution_uncertainty", severity="CRITICAL", account_ref=A3, public="DEGRADED")
    assert a3["detail"]["by_source"] == {"platform_intent_submitting": 1}
    # outcomes learned: a new attempt row (append-only), the reservation consumed, the intents settled
    put(runtime_db, "cati_execution_attempts", record_id="ar-2", execution_attempt_id="att-1", sequence=2,
        trade_plan_id="tp-1", broker_account_id=A1, bot_instance_id="b1", status="FILLED", recorded_at=now,
        payload="{}", payload_hash="h2")
    sql(runtime_db, "UPDATE cati_portfolio_reservations SET status='CONSUMED'")
    sql(runtime_db, "UPDATE cati_execution_intents SET state='FILLED', evidence=?", (json.dumps({"venue": "filled"}),))
    with platform_db.transaction() as conn:
        conn.execute("UPDATE platform_order_intents SET status='ACKNOWLEDGED'")
    assert P.evaluate_execution_uncertainty(c)["resolved"] == 3
    for a in (a1, a2, a3):
        assert_recovered(engine, a["alert_id"], "execution_uncertainty")


# ── 10. missing protective stop ────────────────────────────────────────────────────────────────────────────
def test_positions_without_confirmed_protection(engine, platform_db, runtime_db):
    put(runtime_db, "cati_production_protection_uncertainty", account_id=A1, symbol="ETHUSDT", reason="STOP_NOT_FOUND",
        first_seen_at=T0 - 5 * MIN, last_seen_at=T0, cycles=3)
    put(runtime_db, "cati_protection_state", account_id=A2, symbol="BTCUSDT", direction="LONG", intended_stop_price=50000.0,
        status="ABSENT", created_at=T0 - HOUR, updated_at=T0 - MIN)
    put(runtime_db, "cati_protection_state", account_id=A2, symbol="SOLUSDT", direction="LONG", intended_stop_price=90.0,
        status="PENDING", created_at=T0 - 10 * SEC, updated_at=T0 - 10 * SEC)   # just placed: not yet a problem
    P.evaluate_protection(ctx(engine, runtime_db, T0))
    a1 = assert_raised(engine, platform_db, "UNPROTECTED_POSITION", {"account": A1}, source="protection",
                       severity="CRITICAL", account_ref=A1)
    a2 = assert_raised(engine, platform_db, "UNPROTECTED_POSITION", {"account": A2}, source="protection",
                       severity="CRITICAL", account_ref=A2)
    assert [p["symbol"] for p in a2["detail"]["positions"]] == ["BTCUSDT"]
    sql(runtime_db, "DELETE FROM cati_production_protection_uncertainty")
    sql(runtime_db, "UPDATE cati_protection_state SET status='PROTECTED'")
    assert P.evaluate_protection(ctx(engine, runtime_db, T0 + MIN))["resolved"] == 2
    assert_recovered(engine, a1["alert_id"], "protection")
    assert_recovered(engine, a2["alert_id"], "protection")


# ── 11. emergency flatten ──────────────────────────────────────────────────────────────────────────────────
def _emergency(db, request_id, *, scope="ALL", account=None, state="REQUESTED", outcome=None, at=T0, heartbeat=None):
    put(db, "cati_emergency_requests", request_id=request_id, scope=scope, account_id=account, action="FLATTEN",
        requested_by="admin:7", reason="operator test", state=state, outcome=outcome, requested_at=at,
        heartbeat_at=heartbeat if heartbeat is not None else at, hold_scope=account or "GLOBAL",
        finished_at=at + SEC if state == "DONE" else None)


def test_emergency_flatten_is_raised_once_per_request_and_resolves_when_complete(engine, platform_db, runtime_db):
    _emergency(runtime_db, "req-active", at=T0 - MIN)
    _emergency(runtime_db, "req-fast", scope="ACCOUNT", account=A1, state="DONE", outcome="COMPLETED", at=T0 - 30 * SEC)
    _emergency(runtime_db, "req-old", state="DONE", outcome="COMPLETED", at=T0 - 3 * DAY)   # before deployment: silent
    P.evaluate_emergency_flattens(ctx(engine, runtime_db, T0))
    active = assert_raised(engine, platform_db, "KILL_SWITCH_ACTIVATED", {"emergency_request": "req-active"},
                           source="emergency_flatten", severity="HIGH", public="DEGRADED")
    fast = assert_raised(engine, platform_db, "KILL_SWITCH_ACTIVATED", {"emergency_request": "req-fast"},
                         source="emergency_flatten", severity="HIGH", account_ref=A1)
    assert not engine.raised_before([fingerprint("KILL_SWITCH_ACTIVATED", {"emergency_request": "req-old"})])
    # next tick: the finished flatten resolves (RECOVERED) and is never raised again
    P.evaluate_emergency_flattens(ctx(engine, runtime_db, T0 + MIN))
    assert_recovered(engine, fast["alert_id"], "emergency_flatten")
    P.evaluate_emergency_flattens(ctx(engine, runtime_db, T0 + 2 * MIN))
    assert sum(1 for a in engine.list_alerts(limit=1000)
               if a["fingerprint"] == fingerprint("KILL_SWITCH_ACTIVATED", {"emergency_request": "req-fast"})) == 1
    # the active one stalls (no heartbeat within the emergency TTL): a CRITICAL alert of its own
    P.evaluate_emergency_flattens(ctx(engine, runtime_db, T0 + 10 * MIN))
    stalled = assert_raised(engine, platform_db, "KILL_SWITCH_ACTIVATED",
                            {"emergency_request": "req-active", "kind": "stalled"}, source="emergency_flatten",
                            severity="CRITICAL")
    # it ends INCOMPLETE: the stall resolves, an incomplete alert stays until a human resolves it, then never returns
    sql(runtime_db, "UPDATE cati_emergency_requests SET state='DONE', outcome='INCOMPLETE', finished_at=? "
                    "WHERE request_id='req-active'", (T0 + 11 * MIN,))
    P.evaluate_emergency_flattens(ctx(engine, runtime_db, T0 + 12 * MIN))
    assert engine.get(stalled["alert_id"])["status"] == "RESOLVED"
    incomplete = assert_raised(engine, platform_db, "KILL_SWITCH_ACTIVATED",
                               {"emergency_request": "req-active", "kind": "incomplete"}, source="emergency_flatten",
                               severity="CRITICAL")
    assert engine.get(active["alert_id"])["status"] == "OPEN"              # not "recovered": it did not complete
    P.evaluate_emergency_flattens(ctx(engine, runtime_db, T0 + 20 * MIN))
    assert engine.get(incomplete["alert_id"])["status"] == "OPEN"
    engine.resolve(incomplete["alert_id"], actor="admin:7", note="positions closed by hand", now_ms=T0 + 21 * MIN)
    P.evaluate_emergency_flattens(ctx(engine, runtime_db, T0 + 22 * MIN))
    assert not [a for a in engine.open_alerts() if a["fingerprint"] == incomplete["fingerprint"]]


# ── 12. platform kill switch ───────────────────────────────────────────────────────────────────────────────
def _control(db, event_id, state, scope, at):
    put(db, "cati_governance_controls", control_event_id=event_id, control="KILL_NEW_CATI_ENTRIES", state=state,
        scope=scope, recorded_at=at, payload=json.dumps({"reason": "incident test", "actor_ref": "admin:7"}))


def test_kill_switch_activations_global_account_family(engine, platform_db, runtime_db):
    _control(runtime_db, "ctrl-g1", "ON", "GLOBAL", T0 - MIN)
    _control(runtime_db, "ctrl-a1", "ON", A1, T0 - 40 * SEC)              # engaged and released between two ticks
    _control(runtime_db, "ctrl-a2", "OFF", A1, T0 - 20 * SEC)
    put(runtime_db, "cati_family_events", event_id="fe-1", family_id="FAM_X", event_type="FAMILY_KILL_SWITCH_ON",
        scope="FAMILY", actor_ref="admin:7", recorded_at=T0 - MIN, payload="{}", prev_hash="0", event_hash="h1")
    P.evaluate_kill_switches(ctx(engine, runtime_db, T0))
    glob = assert_raised(engine, platform_db, "KILL_SWITCH_ACTIVATED", {"kill_switch": "ctrl-g1"}, source="kill_switch",
                         severity="HIGH", public="DEGRADED")
    acct = assert_raised(engine, platform_db, "KILL_SWITCH_ACTIVATED", {"kill_switch": "ctrl-a1"}, source="kill_switch",
                         severity="HIGH", account_ref=A1)
    seq = P.rows(runtime_db, "SELECT seq FROM cati_family_events WHERE event_id='fe-1'")[0][0]
    fam = assert_raised(engine, platform_db, "KILL_SWITCH_ACTIVATED", {"family_kill_switch": seq, "family": "FAM_X"},
                        source="kill_switch", severity="MEDIUM")
    P.evaluate_kill_switches(ctx(engine, runtime_db, T0 + MIN))           # the released account switch recovers
    assert_recovered(engine, acct["alert_id"], "kill_switch")
    assert engine.get(glob["alert_id"])["status"] == "OPEN" and engine.get(fam["alert_id"])["status"] == "OPEN"
    _control(runtime_db, "ctrl-g2", "OFF", "GLOBAL", T0 + 2 * MIN)
    put(runtime_db, "cati_family_events", event_id="fe-2", family_id="FAM_X", event_type="FAMILY_KILL_SWITCH_OFF",
        scope="FAMILY", actor_ref="admin:7", recorded_at=T0 + 2 * MIN, payload="{}", prev_hash="h1", event_hash="h2")
    assert P.evaluate_kill_switches(ctx(engine, runtime_db, T0 + 3 * MIN))["resolved"] == 2
    assert_recovered(engine, glob["alert_id"], "kill_switch")
    assert components(platform_db)["TRADING_ENGINE"] == "OPERATIONAL"
    # a new activation is a new alert; once a human resolved it, it does not come back while the switch stays on
    _control(runtime_db, "ctrl-g3", "ON", "GLOBAL", T0 + 4 * MIN)
    P.evaluate_kill_switches(ctx(engine, runtime_db, T0 + 5 * MIN))
    again = assert_raised(engine, platform_db, "KILL_SWITCH_ACTIVATED", {"kill_switch": "ctrl-g3"}, source="kill_switch",
                          severity="HIGH")
    engine.resolve(again["alert_id"], actor="admin:7", note="intended maintenance", now_ms=T0 + 6 * MIN)
    P.evaluate_kill_switches(ctx(engine, runtime_db, T0 + 7 * MIN))
    assert not open_of(engine, "KILL_SWITCH_ACTIVATED")


# ── 13. risk-brake activation ──────────────────────────────────────────────────────────────────────────────
def test_risk_brake_activation(engine, platform_db, runtime_db):
    put(runtime_db, "cati_brake_state", account_id=A1, scope="account", active_brake="DRAWDOWN_STOP", halted=1,
        reason="drawdown 21% >= 20%", triggered_at=T0 - HOUR, triggered_day="2026-10-28", drawdown=0.21, stop_seq=5,
        updated_at=T0)
    put(runtime_db, "cati_brake_state", account_id=A2, scope="account", active_brake="DAILY_LOSS_PAUSE", halted=0,
        paused=1, reason="daily loss", triggered_at=T0 - MIN, triggered_day="2026-10-29", updated_at=T0)
    P.evaluate_risk_brakes(ctx(engine, runtime_db, T0))
    stop = assert_raised(engine, platform_db, "RISK_BRAKE_ACTIVATED",
                         {"account": A1, "scope": "account", "brake": "DRAWDOWN_STOP", "since": T0 - HOUR},
                         source="risk_brakes", severity="HIGH", account_ref=A1)
    pause = assert_raised(engine, platform_db, "RISK_BRAKE_ACTIVATED",
                          {"account": A2, "scope": "account", "brake": "DAILY_LOSS_PAUSE", "since": T0 - MIN},
                          source="risk_brakes", severity="MEDIUM", account_ref=A2)
    assert all(s == "OPERATIONAL" for s in components(platform_db).values())       # never public
    # a latched stop clears only through an authorized restart record (the database enforces it)
    put(runtime_db, "cati_brake_restarts", restart_id="rs-1", account_id=A1, scope="account", stop_seq=5,
        authorized_by="admin:7", authorized_role="OPERATOR", authorized_at=T0 + HOUR, document="{}")
    sql(runtime_db, "UPDATE cati_brake_state SET restart_pending=1, restart_id='rs-1' WHERE account_id=?", (A1,))
    sql(runtime_db, "UPDATE cati_brake_state SET active_brake='NONE', halted=0, paused=0, triggered_at=NULL")
    assert P.evaluate_risk_brakes(ctx(engine, runtime_db, T0 + DAY))["resolved"] == 2
    assert_recovered(engine, stop["alert_id"], "risk_brakes")
    assert_recovered(engine, pause["alert_id"], "risk_brakes")


# ── 14. family certification expiry ────────────────────────────────────────────────────────────────────────
class _Registry:
    """Stand-in for FamilyRegistry: governance stage per family and the register's certification status now."""

    def __init__(self, stages, statuses):
        self.stages, self.statuses = stages, statuses
        self.governance = SimpleNamespace(families=lambda: sorted(stages),
                                          state=lambda fid: SimpleNamespace(stage=self.stages[fid]))

    def entry(self, fid):
        return SimpleNamespace(certification_status=self.statuses[fid],
                               certification_detail={"reason": f"RESEARCH_REGISTER_STATUS_{self.statuses[fid]}"})


def test_family_certification_expiry_only_for_families_with_order_authority(engine, platform_db, runtime_db):
    registry = _Registry({"FAM_DEMO": "M6", "FAM_LIVE": "M7", "FAM_M0": "M0"},
                         {"FAM_DEMO": "REVOKED", "FAM_LIVE": "CERTIFIED", "FAM_M0": "NOT_CERTIFIED"})
    P.evaluate_family_certification(ctx(engine, runtime_db, T0), registry=registry)
    a = assert_raised(engine, platform_db, "EXPIRED_FAMILY_CERTIFICATION", {"family": "FAM_DEMO", "stage": "M6"},
                      source="family_certification", severity="HIGH")
    assert all(s == "OPERATIONAL" for s in components(platform_db).values())
    assert len(open_of(engine, "EXPIRED_FAMILY_CERTIFICATION")) == 1              # M0 and certified: silent
    registry.statuses["FAM_DEMO"] = "CERTIFIED"
    assert P.evaluate_family_certification(ctx(engine, runtime_db, T0 + MIN), registry=registry)["resolved"] == 1
    assert_recovered(engine, a["alert_id"], "family_certification")


def test_family_certification_against_the_real_registry_raises_nothing_at_m0(engine, runtime_db):
    """The real registry and definitions over a migrated database: every family is at M0, so nothing fires."""
    out = P.evaluate_family_certification(ctx(engine, runtime_db, T0))
    assert out["firing"] == 0 and not open_of(engine, "EXPIRED_FAMILY_CERTIFICATION")


# ── 15. venue certification expiry ─────────────────────────────────────────────────────────────────────────
def test_venue_certification_expired_expiring_and_integrity(engine, platform_db, runtime_db):
    from app.exchange.platform_adapter.certification import CertificationStatus as CS
    statuses = {"binance/DEMO": CS("binance", "DEMO", "EXPIRED", "CERTIFICATION_OLDER_THAN_90_DAYS", run_id="run-1",
                                   expires_at_ms=T0 - DAY),
                "bybit/DEMO": CS("bybit", "DEMO", "CERTIFIED", "", run_id="run-2", expires_at_ms=T0 + 5 * DAY),
                "bingx/DEMO": CS("bingx", "DEMO", "NOT_CERTIFIED", "NO_CERTIFICATION_EVIDENCE"),
                "binance/LIVE": CS("binance", "LIVE", "NOT_CERTIFIED", "REGISTER_INTEGRITY_FAILED:seq=4")}
    P.evaluate_venue_certification(ctx(engine, runtime_db, T0), matrix=lambda db, now: statuses)
    expired = assert_raised(engine, platform_db, "EXPIRED_EXCHANGE_CERTIFICATION",
                            {"venue_certification": "binance/DEMO", "run": "run-1"}, source="venue_certification",
                            severity="HIGH")
    expiring = assert_raised(engine, platform_db, "EXPIRED_EXCHANGE_CERTIFICATION",
                             {"venue_certification": "bybit/DEMO", "run": "run-2", "kind": "expiring"},
                             source="venue_certification", severity="MEDIUM")
    integrity = assert_raised(engine, platform_db, "EXPIRED_EXCHANGE_CERTIFICATION",
                              {"venue_certification": "binance/LIVE", "kind": "integrity"},
                              source="venue_certification", severity="HIGH")
    assert len(open_of(engine, "EXPIRED_EXCHANGE_CERTIFICATION")) == 3           # never certified: silent
    renewed = {k: CS(v.venue, v.environment, "CERTIFIED", "", run_id="run-9", expires_at_ms=T0 + 80 * DAY)
               for k, v in statuses.items()}
    assert P.evaluate_venue_certification(ctx(engine, runtime_db, T0 + MIN),
                                          matrix=lambda db, now: renewed)["resolved"] == 3
    for a in (expired, expiring, integrity):
        assert_recovered(engine, a["alert_id"], "venue_certification")


def test_venue_certification_against_the_real_register_raises_nothing_without_evidence(engine, runtime_db):
    out = P.evaluate_venue_certification(ctx(engine, runtime_db, T0))
    assert out["firing"] == 0


# ── 16. billing failures ───────────────────────────────────────────────────────────────────────────────────
def test_billing_failures(engine, platform_db, runtime_db):
    customer_email = "customer" + "@" + "example.com"
    for i in range(2):
        put(runtime_db, "billing_events", event_id=f"evt_u{i}", event_type="customer.subscription.updated",
            payload_json=json.dumps({"email": customer_email}), processed_at=P.ms_iso(T0 - HOUR),
            created_at=P.ms_iso(T0 - HOUR), result="unmatched:unknown_customer")
    put(runtime_db, "billing_events", event_id="evt_p", event_type="invoice.paid", created_at=P.ms_iso(T0 - HOUR))
    for i in range(5):
        put(runtime_db, "billing_events", event_id=f"evt_f{i}", event_type="invoice.payment_failed",
            processed_at=P.ms_iso(T0 - 2 * HOUR), created_at=P.ms_iso(T0 - 2 * HOUR), result="ok:past_due")
    put(runtime_db, "billing_payment_reviews", review_id="rev-1", kind="DISPUTE", provider="stripe",
        provider_object_id="dp_1", review_status="OPEN", created_at=P.ms_iso(T0 - HOUR), updated_at=P.ms_iso(T0 - HOUR))
    P.evaluate_billing(ctx(engine, runtime_db, T0))
    unmatched = assert_raised(engine, platform_db, "BILLING_WEBHOOK_FAILURE", "billing:unmatched_events",
                              source="billing", severity="HIGH")
    assert unmatched["detail"] == {"unmatched_events": 2, "by_result": {"unmatched:unknown_customer": 2}}
    unprocessed = assert_raised(engine, platform_db, "BILLING_WEBHOOK_FAILURE", "billing:unprocessed_events",
                                source="billing", severity="HIGH")
    reviews = assert_raised(engine, platform_db, "BILLING_WEBHOOK_FAILURE", "billing:payment_reviews_open",
                            source="billing", severity="MEDIUM")
    burst = assert_raised(engine, platform_db, "BILLING_WEBHOOK_FAILURE", "billing:payment_failure_burst",
                          source="billing", severity="MEDIUM")
    assert customer_email not in json.dumps(engine.open_alerts())
    assert all(s == "OPERATIONAL" for s in components(platform_db).values())       # never public
    sql(runtime_db, "UPDATE billing_events SET processed_at=?", (P.ms_iso(T0),))
    sql(runtime_db, "UPDATE billing_payment_reviews SET review_status='RESOLVED_KEEP_ACCESS'")
    assert P.evaluate_billing(ctx(engine, runtime_db, T0 + 2 * DAY))["resolved"] == 4
    for a in (unmatched, unprocessed, reviews, burst):
        assert_recovered(engine, a["alert_id"], "billing")


# ── 17. authentication anomalies ───────────────────────────────────────────────────────────────────────────
def test_authentication_anomalies_report_counts_never_identities(engine, platform_db, runtime_db):
    at = P.ms_iso(T0 - 2 * MIN)
    email = "victim" + "@" + "example.com"
    for event, n in (("refresh_reuse_detected", 1), ("admin_refresh_reuse_detected", 1),
                     ("admin_login_failed_mfa", 3), ("login_failed", 50)):
        for i in range(n):
            put(runtime_db, "auth_audit_log", id=uuid.uuid4().hex, event_type=event, user_id=f"user-{i % 7}",
                email=email, ip="192.0.2.44", created_at=at)
    for key in ("admin:" + email, "u1" + "@" + "example.com", "u2" + "@" + "example.com", "u3" + "@" + "example.com"):
        for _ in range(5):
            put(runtime_db, "login_attempts", id=uuid.uuid4().hex, email=key, ip="192.0.2.44", success=0, attempted_at=at)
    P.evaluate_authentication(ctx(engine, runtime_db, T0))
    expect = {"auth:admin_refresh_reuse": "CRITICAL", "auth:refresh_reuse": "HIGH", "auth:admin_mfa_failures": "HIGH",
              "auth:login_failure_burst": "HIGH", "auth:admin_lockout": "HIGH", "auth:user_lockouts": "HIGH"}
    found = {key: assert_raised(engine, platform_db, "AUTHENTICATION_ANOMALY", key, source="authentication",
                                severity=severity) for key, severity in expect.items()}
    text = json.dumps(engine.open_alerts())
    assert email not in text and "192.0.2.44" not in text and "user-1" not in text
    assert all(s == "OPERATIONAL" for s in components(platform_db).values())       # never public
    assert P.evaluate_authentication(ctx(engine, runtime_db, T0 + HOUR))["resolved"] == len(expect)
    for a in found.values():
        assert_recovered(engine, a["alert_id"], "authentication")


# ── 18. backup failure ─────────────────────────────────────────────────────────────────────────────────────
def test_backup_failure_markers(engine, platform_db, tmp_path):
    from datetime import datetime, timezone
    now = datetime(2026, 10, 29, 12, tzinfo=timezone.utc)
    now_ms = int(now.timestamp() * 1000)
    (tmp_path / "backup-status.json").write_text(json.dumps({"outcome": "FAILED", "exit_code": 2, "message": "FAILED no space",
                                                             "finished_at": now.isoformat(), "last_ok_at": None}))
    (tmp_path / "offsite-status.json").write_text(json.dumps({"outcome": "FAILED", "exit_code": 5,
                                                              "message": "upload failed", "finished_at": now.isoformat(),
                                                              "last_ok_at": "2026-10-26T04:30:00+00:00"}))
    evs = evaluate_backup_status(engine, tmp_path, now_ms=now_ms)
    assert [e.firing for e in evs] == [True, True, True]
    local = assert_raised(engine, platform_db, "BACKUP_FAILURE", "backup:local_run", source="backup_status", severity="HIGH")
    assert_raised(engine, platform_db, "BACKUP_FAILURE", "backup:offsite_run", source="backup_status", severity="HIGH")
    age = assert_raised(engine, platform_db, "BACKUP_FAILURE", "backup:offsite_age", source="backup_status", severity="HIGH")
    assert age["detail"]["age_hours"] > 36
    assert all(s == "OPERATIONAL" for s in components(platform_db).values())       # never public
    for name in ("backup-status.json", "offsite-status.json"):
        (tmp_path / name).write_text(json.dumps({"outcome": "OK", "exit_code": 0, "finished_at": now.isoformat(),
                                                 "last_ok_at": now.isoformat()}))
    evs = evaluate_backup_status(engine, tmp_path, now_ms=now_ms + MIN)
    assert [e.resolved for e in evs] == [True, True, True]
    assert engine.get(local["alert_id"])["status"] == "RESOLVED"
    # never configured off-site (no marker) is not an off-site failure; the age evaluator covers local backups
    (tmp_path / "offsite-status.json").unlink()
    assert not any(e.firing for e in evaluate_backup_status(engine, tmp_path, now_ms=now_ms + 2 * MIN))


# ── 20. unusual execution latency ──────────────────────────────────────────────────────────────────────────
def test_unusual_execution_latency(engine, platform_db, runtime_db):
    for i in range(3):
        before = put(runtime_db, "cati_execution_events", account_id=A1, symbol="BTCUSDT", event_type="VENUE_BEFORE",
                     method="create_order", recorded_at=T0 - 5 * MIN + i * SEC, detail="{}")
        put(runtime_db, "cati_execution_events", account_id=A1, symbol="BTCUSDT", event_type="VENUE_AFTER",
            method="create_order", outcome="OK", recorded_at=T0 - 5 * MIN + i * SEC + 8_000,
            detail=json.dumps({"before_seq": before}))
    put(runtime_db, "pending_entries", id="pe-1", bot_id="b1", symbol="ETHUSDT", side="BUY", client_order_id="c1",
        submitted_at_ms=T0 - 3 * MIN, confirmed_at_ms=T0 - 3 * MIN + 40_000, state="OPEN_CONFIRMED",
        submit_state="SUBMIT_CONFIRMED")
    P.evaluate_execution_latency(ctx(engine, runtime_db, T0))
    venue = assert_raised(engine, platform_db, "EXCESSIVE_LATENCY", {"latency": "venue_calls"},
                          source="execution_latency", severity="MEDIUM")
    assert (venue["detail"]["samples"], venue["detail"]["p95_ms"]) == (3, 8_000)
    entries = assert_raised(engine, platform_db, "EXCESSIVE_LATENCY", {"latency": "entry_confirmations"},
                            source="execution_latency", severity="MEDIUM")
    assert components(platform_db)["PORTAL"] == "OPERATIONAL"                     # MEDIUM: not public
    assert P.evaluate_execution_latency(ctx(engine, runtime_db, T0 + HOUR))["resolved"] == 2
    assert_recovered(engine, venue["alert_id"], "execution_latency")
    assert_recovered(engine, entries["alert_id"], "execution_latency")


# ── redaction: a secret-shaped value in a producer's input never reaches a sink ───────────────────────────
def test_secret_shaped_values_in_producer_detail_never_reach_a_sink(engine, platform_db, runtime_db, rec):
    key_value = "A1b2C3d4" * 3
    bearer = "Z9y8X7w6" * 4
    email = "ops-lead" + "@" + "example.com"
    reason = f"api_key={key_value} Authorization: Bearer {bearer} contact {email}"
    put(runtime_db, "cati_production_state", account_id=A1, user_id="user-1", observed_at=T0,
        document=json.dumps({"status": "READ_FAILED", "reason": reason}))
    state: dict = {}
    P.evaluate_exchange_connectivity(ctx(engine, runtime_db, T0, state=state), hold_ms=0)
    stats = engine.deliver_pending(now_ms=T0)
    assert stats["sent"] >= 1
    sent = [m for m in rec.sent if m.category == "EXCHANGE_CONNECTIVITY_LOSS"]
    assert sent
    for m in sent:
        text = m.subject() + "\n" + m.text() + json.dumps(m.detail)
        for secret in (key_value, bearer, email, A1):
            assert secret not in text, secret
    stored = json.dumps(engine.open_alerts())
    assert key_value not in stored and bearer not in stored and email not in stored


def test_rejected_webhook_deliveries_raise_billing_alerts(engine, platform_db, runtime_db):
    """Section J: the API counts rejected deliveries per minute (shared_lib.billing.webhooks.record_rejection);
    a missing secret alerts at once, invalid signatures alert as a burst. No request data is stored."""
    from shared_lib.billing import webhooks
    now_s = T0 / 1000
    for i in range(12):
        assert webhooks.record_rejection(runtime_db, webhooks.REJECTION_SIGNATURE_INVALID, now_s=now_s - 60 * (i % 3))
    assert webhooks.record_rejection(runtime_db, webhooks.REJECTION_NOT_CONFIGURED, now_s=now_s - 30)
    with runtime_db.connect() as conn:
        assert conn.execute("SELECT COUNT(*), SUM(count) FROM billing_webhook_rejections").fetchone()[:] == (4, 13)
        assert {r[1] for r in conn.execute("PRAGMA table_info(billing_webhook_rejections)")} == {
            "bucket_start", "reason", "count", "last_at"}
    P.evaluate_billing(ctx(engine, runtime_db, T0))
    forged = assert_raised(engine, platform_db, "BILLING_WEBHOOK_FAILURE", "billing:signature_rejections",
                           source="billing", severity="HIGH")
    assert forged["detail"]["rejected_deliveries"] == 12
    missing = assert_raised(engine, platform_db, "BILLING_WEBHOOK_FAILURE", "billing:webhook_not_configured",
                            source="billing", severity="HIGH")
    # An hour later the window is empty and both recover.
    P.evaluate_billing(ctx(engine, runtime_db, T0 + 2 * HOUR))
    for a in (forged, missing):
        assert_recovered(engine, a["alert_id"], "billing")


def test_a_few_invalid_signatures_are_not_an_alert(engine, platform_db, runtime_db):
    from shared_lib.billing import webhooks
    for _ in range(3):
        webhooks.record_rejection(runtime_db, webhooks.REJECTION_SIGNATURE_INVALID, now_s=T0 / 1000)
    assert P.evaluate_billing(ctx(engine, runtime_db, T0))["firing"] == 0


# ── 19. governance violation attempts ──────────────────────────────────────────────────────────────────────
def test_governance_bypass_attempts_alert_and_routine_m0_refusals_do_not(engine, platform_db, runtime_db):
    """Section J: a refusal that only a bypass attempt produces raises GOVERNANCE_VIOLATION_ATTEMPT; the refusals
    every M0 cycle produces (no order authority, not certified, failed research) never do."""
    def gate(account, outcome, at):
        put(runtime_db, "cati_execution_events", account_id=account, event_type="GATE", outcome=outcome,
            recorded_at=at, detail="{}")

    def evaluation(account, reason, at):
        put(runtime_db, "cati_execution_evaluations", evaluation_id=uuid.uuid4().hex, account_id=account,
            user_id="u-" + account, observed_at=at, stage="BOUNDARY_RESULT", reason=reason, document="{}")
    for i in range(3):                                                   # routine under M0
        gate(f"acct-{i}", "FAMILY_STAGE_NO_ORDER_AUTHORITY:M0", T0 - MIN)
        evaluation(f"acct-{i}", "FAMILY_FAILED_RESEARCH", T0 - MIN)
        evaluation(f"acct-{i}", "FAMILY_NOT_CERTIFIED", T0 - MIN)
    assert P.evaluate_governance_violations(ctx(engine, runtime_db, T0))["firing"] == 0
    gate("acct-1", "FAMILY_GATE_NOT_AUTHORITATIVE", T0 - MIN)
    gate("acct-2", "FAMILY_GATE_NOT_AUTHORITATIVE", T0 - 2 * MIN)
    evaluation("acct-1", "DECISION_TAMPERED", T0 - MIN)
    evaluation("acct-3", "GO_LIVE_GATE:3:BUILD_DOES_NOT_PERMIT_LIVE", T0 - MIN)
    gate("acct-9", "TEST_ADAPTER_NOT_ALLOWED", T0 - 3 * HOUR)            # outside the window
    out = P.evaluate_governance_violations(ctx(engine, runtime_db, T0))
    assert out["firing"] == 3
    gate_alert = assert_raised(engine, platform_db, "GOVERNANCE_VIOLATION_ATTEMPT",
                               {"governance": "FAMILY_GATE_NOT_AUTHORITATIVE"}, source="governance", severity="HIGH")
    assert gate_alert["detail"]["refusals"] == 2 and gate_alert["detail"]["accounts"] == 2
    assert "acct-1" not in json.dumps(engine.open_alerts())               # accounts are counted, never named
    tampered = assert_raised(engine, platform_db, "GOVERNANCE_VIOLATION_ATTEMPT", {"governance": "DECISION_TAMPERED"},
                             source="governance", severity="HIGH")
    live = assert_raised(engine, platform_db, "GOVERNANCE_VIOLATION_ATTEMPT", {"governance": "GO_LIVE_GATE"},
                         source="governance", severity="HIGH")
    assert P.evaluate_governance_violations(ctx(engine, runtime_db, T0 + 2 * HOUR))["resolved"] == 3
    for a in (gate_alert, tampered, live):
        assert_recovered(engine, a["alert_id"], "governance")


# ── 20. public API health ──────────────────────────────────────────────────────────────────────────────────
def test_an_api_that_stops_answering_raises_api_outage_after_the_hold(engine, platform_db, runtime_db):
    answers = {"http://user/health": (None, "URLError"), "http://admin/health": (200, None),
               "http://bot/health": (503, None)}
    probes = [("user-backend", "http://user/health"), ("admin-backend", "http://admin/health"),
              ("bot-backend", "http://bot/health")]
    fetch = lambda url, timeout: answers[url]  # noqa: E731
    state = {}
    first = P.evaluate_api_health(ctx(engine, runtime_db, T0, state=state), probes, fetch=fetch)
    assert first["firing"] == 0 and first["held"] == 2                     # one failed probe is not an outage
    out = P.evaluate_api_health(ctx(engine, runtime_db, T0 + 2 * MIN, state=state), probes, fetch=fetch)
    assert out["firing"] == 2
    user = assert_raised(engine, platform_db, "API_OUTAGE", {"api": "user-backend"}, source="api_health",
                         severity="CRITICAL")
    assert user["detail"] == {"api": "user-backend", "status": None, "error": "URLError"}
    bot = assert_raised(engine, platform_db, "API_OUTAGE", {"api": "bot-backend"}, source="api_health",
                        severity="CRITICAL")
    answers.update({"http://user/health": (200, None), "http://bot/health": (200, None)})
    assert P.evaluate_api_health(ctx(engine, runtime_db, T0 + 3 * MIN, state=state), probes, fetch=fetch)["resolved"] == 2
    for a in (user, bot):
        assert_recovered(engine, a["alert_id"], "api_health")


def test_health_probe_configuration_is_validated():
    assert P.parse_health_probes(["a=http://h:1/health", "b=https://x/health,c=http://y/z", ""]) == [
        ("a", "http://h:1/health"), ("b", "https://x/health"), ("c", "http://y/z")]
    for bad in ("nameonly", "=http://h/health", "a=ftp://h/health"):
        with pytest.raises(ValueError):
            P.parse_health_probes([bad])


def test_the_deployed_ticks_probe_every_api():
    from pathlib import Path
    import yaml
    root = Path(__file__).resolve().parents[4]
    service = (root / "deploy/systemd/cosmicforge-ops-tick.service").read_text(encoding="utf-8")
    for probe in ("user-backend=http://127.0.0.1:8000/health", "admin-backend=http://127.0.0.1:8100/health",
                  "bot-backend=http://127.0.0.1:9000/health"):
        assert f"--health-url {probe}" in service
    command = yaml.safe_load((root / "deploy/staging/docker-compose.staging.yml").read_text(encoding="utf-8")
                             )["services"]["ops-tick"]["command"]
    probes = P.parse_health_probes([command[i + 1] for i, v in enumerate(command) if v == "--health-url"])
    assert {name for name, _ in probes} == {"user-backend", "admin-backend", "bot-api", "bot-trading"}
