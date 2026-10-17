"""Step 3.7: the canonical reservation ledger (``pipeline.reservations``) and its schema.

Multi-account, multi-position and concurrent: threads released together by a barrier, each with its own
connections, reserve against one database; the outcome is asserted exactly, whichever thread wins. The legacy
stores are exercised through their own code and compared row for row before and after.
"""
from __future__ import annotations

import contextlib
import dataclasses
import json
import re
import sqlite3
import subprocess
import sys
import threading
from pathlib import Path

import pytest

from app.exchange.platform_adapter.contract import InstrumentRules
from app.trading_intelligence.pipeline import brakes
from app.trading_intelligence.pipeline import portfolio as P
from app.trading_intelligence.pipeline import reservations as R
from app.trading_intelligence.pipeline.brakes import NO_BRAKE
from app.trading_intelligence.pipeline.contracts import (
    LONG, AccountRef, AddIntent, DecisionProvenance, EntryIntent, ExitIntent, IntentBatch,
)
from app.trading_intelligence.portfolio.reservation_store import CATIReservationStore
from shared_lib.persistence import cati_platform_schema as platform_schema
from shared_lib.persistence import cati_portfolio_schema as schema
from shared_lib.persistence.cati_platform_schema import ensure_cati_platform_schema
from shared_lib.persistence.db import DB
from shared_lib.persistence.migrations import migrate

EQUITY = 10_000.0
PRICE = 100.0
NOW = 1_780_000_000_000
DAY_MS = 86_400_000
ACCOUNT, USER = "acct-1", "user-1"
SYMBOLS = tuple(f"{c * 3}USDT" for c in "ABCDEFGHIJKLMNOP")
LEGACY_TABLES = ("cati_portfolio_reservations", "pending_entries", "entry_protection_events", "positions",
                 "cati_production_daily_risk", "cati_account_period_risk")


def rule(symbol: str, **overrides) -> InstrumentRules:
    values = dict(venue="binance", environment="DEMO", symbol=symbol, venue_symbol=symbol, status="TRADING",
                  venue_status="TRADING", base_asset=symbol[:-4], quote_asset="USDT", margin_asset="USDT",
                  price_tick=0.01, quantity_step=0.001, min_quantity=0.001, max_quantity=1_000_000.0, min_notional=5.0,
                  max_leverage=20.0, contract_size=1.0, fetched_at_ms=1)
    values.update(overrides)
    return InstrumentRules(**values)


RULES = {s: rule(s) for s in SYMBOLS}


@pytest.fixture(scope="module")
def migrated(tmp_path_factory):
    """A database as ``migrate()`` leaves it, with the platform schema applied: the template of every test."""
    path = str(tmp_path_factory.mktemp("step3_ledger") / "template.db")
    database = DB(path)
    migrate(database)
    ensure_cati_platform_schema(database)
    return path


def copy_of(template: str, target: str) -> DB:
    with contextlib.closing(sqlite3.connect(template)) as source, contextlib.closing(sqlite3.connect(target)) as copy:
        source.backup(copy)
    return DB(target)


@pytest.fixture
def db(migrated, tmp_path):
    return copy_of(migrated, str(tmp_path / "ledger.db"))


@pytest.fixture
def ledger(db):
    return R.ReservationLedger(db)


def snapshot(*, account=ACCOUNT, equity=EQUITY, positions=(), available=None, as_of=NOW - 1_000):
    return P.AccountSnapshot(account_id=account, equity=equity, wallet_balance=equity,
                             available_margin=equity if available is None else available, positions=tuple(positions),
                             reservations=(), as_of_ms=as_of)


def held(symbol, quantity, *, stop=90.0, leverage=None):
    return P.PortfolioPosition(symbol=symbol, direction=LONG, quantity=quantity, entry_price=PRICE, mark_price=PRICE,
                               stop_price=stop, family_id="daily_trend", leverage=leverage)


def entry(symbol, *, risk=0.001, distance=0.10) -> EntryIntent:
    return EntryIntent(symbol=symbol, direction=LONG, quantity=risk * EQUITY / distance / PRICE, reference_price=PRICE,
                       stop_price=PRICE * (1.0 - distance), risk_fraction=risk)


def scope(decision="dec-1", *, account=ACCOUNT, mode="DEMO", expires=NOW + DAY_MS, user=USER):
    return R.ReservationScope(account_id=account, user_id=user, family_id="daily_trend", decision_id=decision,
                              decision_hash=f"hash-{decision}", mode=mode, expires_at_ms=expires)


def limits(level="conservative", **changes) -> P.AccountLimits:
    return dataclasses.replace(P.AccountLimits.from_profile(level), **changes)


def reserve(ledger, intents, *, decision="dec-1", account=ACCOUNT, mode="DEMO", snap=None, lim=None, brake=NO_BRAKE,
            now=NOW, expires=NOW + DAY_MS, **more):
    return ledger.reserve(intents, snapshot=snap or snapshot(account=account), limits=lim or limits(), rules=RULES,
                          brake_state=brake, now_ms=now, scope=scope(decision, account=account, mode=mode, expires=expires),
                          **more)


def dump(db, table, order="1"):
    with db.connect() as conn:
        return [tuple(r) for r in conn.execute(f"SELECT * FROM {table} ORDER BY {order}").fetchall()]


def ledger_rows(db, where="1=1", *params):
    with db.connect() as conn:
        return R._query(conn, where, params)


def race(count, work):
    """Run ``work(i)`` on ``count`` threads that start together. Returns the results in thread order."""
    barrier, results, failures = threading.Barrier(count), [None] * count, []

    def runner(index):
        try:
            barrier.wait(timeout=30)
            results[index] = work(index)
        except Exception as error:                                   # reported by the assertion below, never lost
            failures.append((index, repr(error)))

    threads = [threading.Thread(target=runner, args=(i,)) for i in range(count)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=120)
    assert failures == [] and not any(thread.is_alive() for thread in threads)
    return results


def ledgers(db, count):
    """One ledger per worker, each on its own ``DB`` object: separate connections, as separate processes have."""
    return [R.ReservationLedger(DB(db.path)) for _ in range(count)]


# ----------------------------------------------------------------------------------------------------- schema
def table_columns(db, tables):
    with db.connect() as conn:
        return {t: [tuple(r)[1:3] for r in conn.execute(f'PRAGMA table_info("{t}")').fetchall()] for t in tables}


def master(db):
    with db.connect() as conn:
        return {(r[0], r[1]): r[2] for r in conn.execute("SELECT type, name, sql FROM sqlite_master").fetchall()}


def add_bot(db, bot="bot-1", account=ACCOUNT, user=USER):
    with db.connect() as conn:
        conn.execute("INSERT OR IGNORE INTO broker_accounts (id, user_id, broker_id, market_type, status, created_at, updated_at)"
                     " VALUES (?,?,?,?,?,?,?)", (account, user, "binance", "crypto", "active", "2026-01-01", "2026-01-01"))
        conn.execute("INSERT OR IGNORE INTO bot_instances (id, user_id, broker_account_id, market_type, strategy_id, mode, status,"
                     " created_at, updated_at) VALUES (?,?,?,?,?,?,?,?,?)",
                     (bot, user, account, "crypto", "cati", "live", "active", "2026-01-01", "2026-01-01"))


def add_pending(db, symbol, *, bot="bot-1", side="LONG", state="PENDING_OPEN", submit="NOT_SUBMITTED", notional=300.0,
                row_id=None):
    with db.connect() as conn:
        conn.execute("INSERT INTO pending_entries (id, bot_id, symbol, side, client_order_id, state, submit_state,"
                     " intended_notional) VALUES (?,?,?,?,?,?,?,?)",
                     (row_id or f"pe-{bot}-{symbol}", bot, symbol, side, f"cid-{bot}-{symbol}", state, submit, notional))


def legacy_production_reservation(db, symbol="SOLUSDT", cycle="cycle-1", bot="bot-1"):
    """A PRODUCTION reservation written by the legacy store's own code."""
    store = CATIReservationStore(db)
    outcome = store.reserve(broker_account_id=ACCOUNT, bot_instance_id=bot, cycle_id=cycle,
                            selected=[(f"cand-{cycle}", f"{symbol[:-4]}/USDT:PERP", "binance", symbol, "LONG")], now_ms=NOW,
                            ttl_seconds=600, max_open_positions=1, mode="PRODUCTION", production_scope=(USER, ACCOUNT))
    assert outcome.reserved, outcome.conflict_reason
    return store, outcome.reservation.reservation_id


MY_TABLES = (schema.LEDGER_TABLE, schema.LEDGER_EVENT_TABLE, schema.BRAKE_STATE_TABLE, schema.BRAKE_EVENT_TABLE,
             schema.BRAKE_RESTART_TABLE)


def mine(objects):
    """The schema objects this step's portfolio module owns (tables, their indexes and triggers)."""
    return {k: v for k, v in objects.items() if re.search(r"cati_(reservation_ledger|brake|rle|brk)", k[1])}


@pytest.fixture
def before_step3(tmp_path):
    """Load the actual baseline schema, not today's migration with one call disabled."""
    import gzip
    import hashlib
    from pathlib import Path

    fixture = Path(__file__).resolve().parents[1] / "fixtures" / "pre_step3_75435cd"
    sql = gzip.decompress(fixture.with_suffix(".sql.gz").read_bytes()).decode("utf-8")
    metadata = json.loads(fixture.with_suffix(".json").read_text())
    assert hashlib.sha256(sql.encode()).hexdigest() == metadata["sql_sha256"]
    def build(name: str) -> DB:
        path = tmp_path / name
        with sqlite3.connect(path) as conn:
            conn.executescript(sql)
            assert conn.execute("PRAGMA foreign_key_check").fetchall() == []
        database = DB(str(path))
        assert mine(master(database)) == {}
        return database

    return build


def test_the_ddl_is_additive_only():
    assert schema.DDL and all(isinstance(s, str) for s in schema.DDL)
    for statement in schema.DDL:
        head = " ".join(statement.split())
        assert re.match(r"CREATE (TABLE|INDEX|UNIQUE INDEX|TRIGGER) IF NOT EXISTS (cati_|idx_cati_|uq_cati_|trg_cati_)", head), head
        assert not re.search(r"\b(DROP|ALTER|RENAME|REPLACE|VACUUM|PRAGMA)\b", head), head
    created = set(re.findall(r"CREATE TABLE IF NOT EXISTS (\w+)", " ".join(schema.DDL)))
    assert created == set(MY_TABLES)
    assert schema.upgrade(None) is None


def test_the_schema_upgrades_a_database_that_already_holds_legacy_reservations_and_can_run_again(before_step3):
    db = before_step3("upgrade.db")
    add_bot(db)
    store, pending_id = legacy_production_reservation(db)
    store.mark_resolution_pending(pending_id, NOW + 5, trade_plan_id="tp-1", execution_attempt_id="ea-1",
                                  resolution_deadline=NOW + 60_000, note="timeout after send")
    add_pending(db, "XRPUSDT", submit="SUBMIT_UNKNOWN")
    before = {table: dump(db, table) for table in LEGACY_TABLES if (("table", table) in master(db))}
    objects_before = master(db)
    assert before["cati_portfolio_reservations"] and before["pending_entries"]

    with pytest.raises(R.LedgerSchemaMissing):
        R.ReservationLedger(db)                                       # the old schema has no ledger: fail closed

    columns_before = table_columns(db, [name for kind, name in objects_before if kind == "table"])
    ensure_cati_platform_schema(db)
    migrate(db)                                                       # the whole migration, as every start does
    first = master(db)
    ensure_cati_platform_schema(db)
    migrate(db)
    migrate(db)
    assert master(db) == first                                        # running it again changes nothing
    # No existing object was touched destructively. Later steps add tables and append columns to existing tables
    # (admin MFA, billing state); an original column is never dropped, renamed or retyped, and every original
    # index and trigger is unchanged.
    after = table_columns(db, list(columns_before))
    for table, cols in columns_before.items():
        assert after[table][:len(cols)] == cols, table
    assert {k: v for k, v in first.items() if k in objects_before and k[0] != "table"} ==         {k: v for k, v in objects_before.items() if k[0] != "table"}
    assert {table: dump(db, table) for table in before} == before     # no legacy row was touched
    assert all(("table", table) in first for table in MY_TABLES)
    assert store.pending_resolutions(ACCOUNT)[0]["reservation_id"] == pending_id         # legacy recovery list intact
    ledger = R.ReservationLedger(db)
    imported = ledger.import_legacy(ACCOUNT, NOW + 10)                # and the new ledger works on the upgraded data
    assert len(imported.created) == 2
    rows_then = {table: dump(db, table) for table in MY_TABLES}
    migrate(db)
    assert {table: dump(db, table) for table in MY_TABLES} == rows_then and master(db) == first


def test_an_interrupted_schema_run_is_completed_by_the_next_one(before_step3, migrated):
    db = before_step3("interrupted.db")
    half = len(schema.DDL) // 2
    with db.connect() as conn:
        for statement in schema.DDL[:half]:                           # the process died half way through
            conn.execute(statement)
        conn.execute(f"INSERT INTO {schema.LEDGER_TABLE} (reservation_id, account_id, book, mode, source, intent_key,"
                     " family_id, decision_id, symbol, direction, kind, state, created_at, updated_at)"
                     " VALUES ('pres_x', 'acct-1', 'SHADOW', 'SHADOW', 'PIPELINE', 'k', 'f', 'd', 'AAAUSDT', 'LONG', 'ENTRY',"
                     " 'RESERVED', 1, 1)")
    assert 0 < len(mine(master(db))) < len(schema.DDL)
    migrate(db)                                                       # the next start runs the migration again
    complete = mine(master(DB(migrated)))                             # a database that was never interrupted
    assert mine(master(db)) == complete and len(complete) >= len(schema.DDL)
    assert [r.reservation_id for r in ledger_rows(db)] == ["pres_x"]  # what was written in between is still there


def test_the_ledger_refuses_to_work_without_its_schema(tmp_path):
    class Bare:
        path = str(tmp_path / "bare.db")

        @contextlib.contextmanager
        def connect(self):
            with contextlib.closing(sqlite3.connect(self.path)) as conn:
                yield conn

    with pytest.raises(R.LedgerSchemaMissing):
        R.ReservationLedger(Bare())


# ---------------------------------------------------------------------------------------------------- reserve
def test_a_reservation_holds_the_slot_risk_and_margin_of_one_authorized_intent(ledger, db):
    outcome = reserve(ledger, [entry("AAAUSDT", risk=0.002)])
    row = outcome.reservation("AAAUSDT")
    assert outcome.created == (row.reservation_id,) and outcome.replayed == () and outcome.refused == ()
    assert (row.account_id, row.user_id, row.book, row.mode, row.source, row.state) == (
        ACCOUNT, USER, R.EXECUTION_BOOK, "DEMO", P.PIPELINE_SOURCE, R.RESERVED)
    assert (row.symbol, row.direction, row.kind, row.quantity, row.stop_price) == ("AAAUSDT", LONG, "ENTRY", 2.0, 90.0)
    assert (row.risk_amount, row.notional, row.margin, row.leverage) == (20.0, 200.0, 200.0, 1.0)
    assert (row.family_id, row.decision_id, row.decision_hash, row.intent_key) == (
        "daily_trend", "dec-1", "hash-dec-1", "dec-1|AAAUSDT|ENTRY")
    assert (row.created_at, row.expires_at, row.awaits_releases) == (NOW, NOW + DAY_MS, ())
    assert row.snapshot_hash == snapshot().snapshot_hash and row.limits_hash == limits().limits_hash
    assert row.authorization_hash == outcome.authorization.authorization_hash
    assert ledger.get(ACCOUNT, row.reservation_id) == row
    assert [(e["from_state"], e["to_state"], e["actor"]) for e in ledger.history(ACCOUNT, row.reservation_id)] == [
        (None, R.RESERVED, "pipeline")]


def test_reserving_the_same_intent_again_returns_the_first_reservation_and_writes_nothing(ledger, db):
    first = reserve(ledger, [entry("AAAUSDT"), entry("BBBUSDT")])
    rows_before, events_before = dump(db, schema.LEDGER_TABLE), dump(db, schema.LEDGER_EVENT_TABLE)
    again = reserve(ledger, [entry("AAAUSDT"), entry("BBBUSDT")], now=NOW + 50)
    assert again.created == () and set(again.replayed) == set(first.created)
    assert again.reservations == first.reservations
    assert dump(db, schema.LEDGER_TABLE) == rows_before and dump(db, schema.LEDGER_EVENT_TABLE) == events_before
    grown = reserve(ledger, [entry("AAAUSDT"), entry("BBBUSDT"), entry("CCCUSDT")], now=NOW + 60)
    assert len(grown.created) == 1 and len(grown.replayed) == 2 and len(ledger_rows(db)) == 3


def test_an_intent_batch_carries_its_own_scope(ledger):
    provenance = DecisionProvenance(family_id="daily_trend", family_version="1.0.0", mandate_id="M004", hypothesis_id="H18",
                                    rule_hash="rule", decision_id="dec-batch", decision_hash="dh", decision_time_ms=NOW - 10,
                                    data_snapshot_id="data-1", timeframe="1d", mode="SHADOW")
    batch = IntentBatch(provenance=provenance, account=AccountRef(USER, ACCOUNT, "binance", "DEMO"),
                        intents=(entry("AAAUSDT"), entry("BBBUSDT")), constraints=limits().constraints(EQUITY),
                        instruments=frozenset(SYMBOLS), created_at_ms=NOW - 5, expires_at_ms=NOW + 60_000)
    outcome = ledger.reserve(batch, snapshot=snapshot(), limits=limits(), rules=RULES, brake_state=NO_BRAKE, now_ms=NOW, ttl_ms=5_000)
    assert len(outcome.created) == 2
    assert {(r.book, r.mode, r.decision_id, r.user_id, r.expires_at) for r in outcome.reservations} == {
        (R.SHADOW_BOOK, "SHADOW", "dec-batch", USER, NOW + 5_000)}
    with pytest.raises(R.LedgerError) as mismatch:
        ledger.reserve(batch, snapshot=snapshot(), limits=limits(), rules=RULES, brake_state=NO_BRAKE, now_ms=NOW, scope=scope("other"))
    assert mismatch.value.code == "SCOPE_MISMATCH"


def test_requests_that_cannot_be_a_reservation_are_rejected_before_anything_is_written(ledger, db):
    cases = (
        (dict(scope=None), "SCOPE_REQUIRED"),
        (dict(scope=scope(account="acct-2")), "ACCOUNT_MISMATCH"),
        (dict(scope=scope(expires=NOW)), "DECISION_EXPIRED"),
        (dict(scope=scope(), ttl_ms=0), "DECISION_EXPIRED"),
    )
    for arguments, code in cases:
        with pytest.raises(R.LedgerError) as error:
            ledger.reserve([entry("AAAUSDT")], snapshot=snapshot(), limits=limits(), rules=RULES, brake_state=NO_BRAKE,
                           now_ms=NOW, **arguments)
        assert error.value.code == code
    with pytest.raises(R.LedgerError) as research:
        scope(mode="RESEARCH_SIMULATION")
    assert research.value.code == "MODE_NOT_RESERVABLE"
    assert R.book_of("LIVE") == R.book_of("DEMO") == R.book_of("DEMO_CERTIFICATION") == R.EXECUTION_BOOK
    assert ledger_rows(db) == []


def test_limits_are_checked_again_against_what_the_ledger_already_holds(ledger):
    lim = limits("conservative")                                      # 4 positions, 1 % open risk
    first = reserve(ledger, [entry(s, risk=0.002) for s in SYMBOLS[:3]], decision="dec-1", lim=lim)
    assert len(first.created) == 3
    stale = snapshot()                                                # the second decision read the same empty account
    assert len(P.authorize(stale, lim, [entry(s, risk=0.002) for s in SYMBOLS[3:6]], RULES, brake_state=NO_BRAKE).accepted) == 3
    second = reserve(ledger, [entry(s, risk=0.002) for s in SYMBOLS[3:6]], decision="dec-2", lim=lim, snap=stale, now=NOW + 5)
    assert [r.symbol for r in second.reservations] == ["DDDUSDT"]
    assert {r.symbol: r.reason for r in second.refused} == {"EEEUSDT": P.POSITION_LIMIT, "FFFUSDT": P.POSITION_LIMIT}
    assert second.authorization.usage_before.positions == 3
    assert second.authorization.usage_before.open_risk_fraction == pytest.approx(0.006)
    assert second.authorization.usage_after.open_risk_fraction == pytest.approx(0.008)


def test_books_do_not_consume_each_others_capacity(ledger):
    lim = limits("conservative")
    shadow = reserve(ledger, [entry(s) for s in SYMBOLS[:5]], decision="dec-shadow", mode="SHADOW", lim=lim)
    executing = reserve(ledger, [entry(s) for s in SYMBOLS[:5]], decision="dec-demo", mode="DEMO", lim=lim)
    assert len(shadow.created) == len(executing.created) == lim.max_positions
    assert {r.book for r in shadow.reservations} == {R.SHADOW_BOOK} and {r.book for r in executing.reservations} == {R.EXECUTION_BOOK}
    assert len(ledger.open_reservations(ACCOUNT, book=R.SHADOW_BOOK, now_ms=NOW)) == lim.max_positions
    live = reserve(ledger, [entry("PPPUSDT")], decision="dec-live", mode="LIVE", lim=lim)
    assert live.refused[0].reason == P.POSITION_LIMIT                 # live and demo are one executing book


# ------------------------------------------------------------------------------------------------ concurrency
def open_totals(db, account=ACCOUNT, book=R.EXECUTION_BOOK):
    rows = [r for r in ledger_rows(db, "account_id=? AND book=?", account, book) if r.open]
    return {"count": len(rows), "risk": sum(r.risk_amount for r in rows), "notional": sum(r.notional for r in rows),
            "margin": sum(r.margin for r in rows), "symbols": sorted(r.symbol for r in rows)}


@pytest.mark.parametrize("case", ["positions", "open_risk", "leverage", "margin"])
def test_concurrent_decisions_cannot_jointly_exceed_a_limit(db, case):
    workers = 10
    setups = {
        # each decision alone fits; together they must stop exactly at the limit
        "positions": dict(lim=limits("conservative"), risk=0.001, distance=0.10, available=EQUITY, expect=4),
        "open_risk": dict(lim=limits("balanced", max_positions=16), risk=0.004, distance=0.10, available=EQUITY, expect=5),
        "leverage": dict(lim=limits("balanced", max_positions=16, max_leverage=1.0), risk=0.004, distance=0.01,
                         available=EQUITY * 9, expect=3),
        "margin": dict(lim=limits("balanced", max_positions=16), risk=0.004, distance=0.10, available=1_000.0, expect=3),
    }
    setup = setups[case]
    lim, snap = setup["lim"], snapshot(available=setup["available"])
    assert all(P.authorize(snap, lim, [entry(SYMBOLS[i], risk=setup["risk"], distance=setup["distance"])], RULES,
                           brake_state=NO_BRAKE).refused == () for i in range(workers))
    books = ledgers(db, workers)
    results = race(workers, lambda i: reserve(books[i], [entry(SYMBOLS[i], risk=setup["risk"], distance=setup["distance"])],
                                              decision=f"dec-{i}", lim=lim, snap=snap))
    totals = open_totals(db)
    assert sum(len(r.created) for r in results) == totals["count"] == setup["expect"]
    assert totals["count"] <= lim.max_positions
    assert totals["risk"] <= lim.max_open_risk * EQUITY * (1 + 1e-9)
    assert totals["notional"] <= lim.max_leverage * EQUITY * (1 + 1e-9)
    assert totals["margin"] <= setup["available"]
    refusals = {r.refused[0].reason for r in results if r.refused}
    assert refusals == {{"positions": P.POSITION_LIMIT, "open_risk": P.OPEN_RISK_CAP, "leverage": P.LEVERAGE_CAP,
                         "margin": P.INSUFFICIENT_MARGIN}[case]}
    assert all(len(r.created) + len(r.refused) == 1 for r in results)


def test_concurrent_entries_on_one_symbol_reserve_it_once(db):
    workers = 8
    books = ledgers(db, workers)
    results = race(workers, lambda i: reserve(books[i], [entry("AAAUSDT")], decision=f"dec-{i}"))
    assert sum(len(r.created) for r in results) == 1 and open_totals(db)["symbols"] == ["AAAUSDT"]
    assert sorted(r.refused[0].reason for r in results if r.refused) == [P.DUPLICATE_RESERVATION] * (workers - 1)


def test_the_same_intent_reserved_concurrently_is_one_reservation(db):
    workers = 8
    books = ledgers(db, workers)
    results = race(workers, lambda i: reserve(books[i], [entry("AAAUSDT"), entry("BBBUSDT")], decision="dec-same"))
    assert sum(len(r.created) for r in results) == 2 and sum(len(r.replayed) for r in results) == 2 * (workers - 1)
    assert len({r.reservations for r in results}) == 1 and all(r.refused == () for r in results)
    assert len(ledger_rows(db)) == 2 and len(dump(db, schema.LEDGER_EVENT_TABLE)) == 2


def test_accounts_reserve_concurrently_without_touching_each_other(db):
    accounts, per_account = ("acct-1", "acct-2", "acct-3"), 6
    lim = limits("conservative")
    jobs = [(account, i) for account in accounts for i in range(per_account)]
    books = ledgers(db, len(jobs))
    results = race(len(jobs), lambda n: reserve(books[n], [entry(SYMBOLS[jobs[n][1]], risk=0.002)], account=jobs[n][0],
                                                decision=f"dec-{jobs[n][1]}", lim=lim))
    for account in accounts:
        totals = open_totals(db, account)
        assert totals["count"] == lim.max_positions and totals["risk"] == pytest.approx(0.008 * EQUITY)
    assert sum(len(r.created) for r in results) == lim.max_positions * len(accounts)
    assert {r.account_id for r in ledger_rows(db)} == set(accounts)
    assert all(row.account_id == account for account in accounts for row in ledger_rows(db, "account_id=?", account))


def test_a_position_added_to_and_a_new_entry_compete_for_the_same_budgets(db):
    lim = limits("balanced", max_positions=16)
    snap = snapshot(positions=(held("AAAUSDT", 1.0),))                # 10 at risk; cap 200
    add = AddIntent(symbol="AAAUSDT", direction=LONG, quantity=3.0, reference_price=PRICE, stop_price=90.0)
    work = [[add]] + [[entry(SYMBOLS[i], risk=0.004)] for i in range(1, 7)]
    books = ledgers(db, len(work))
    race(len(work), lambda i: reserve(books[i], work[i], decision=f"dec-{i}", lim=lim, snap=snap))
    totals = open_totals(db)
    assert totals["risk"] + 10.0 <= lim.max_open_risk * EQUITY * (1 + 1e-9)
    assert totals["risk"] + 10.0 == pytest.approx(lim.max_open_risk * EQUITY, rel=1e-2)       # and the cap is used, not wasted


#: a worker process: it imports this module for its helpers, connects, says so, and reserves when told to
PROCESS_WORKER = '''
import json, sys
sys.path[:0] = sys.argv[1:4]
import test_step3_reservations as T
ledger = T.R.ReservationLedger(T.DB(sys.argv[4]))
print("ready", flush=True)
sys.stdin.readline()
outcome = T.reserve(ledger, [T.entry(T.SYMBOLS[int(sys.argv[5])])], decision="process-" + sys.argv[5],
                    lim=T.limits("conservative", max_positions=2))
print(json.dumps({"created": len(outcome.created), "refused": [r.reason for r in outcome.refused]}), flush=True)
'''


def test_separate_processes_cannot_jointly_exceed_the_position_limit(db):
    tests_dir = Path(__file__).resolve().parent
    bot_backend = tests_dir.parents[1]
    command = [sys.executable, "-c", PROCESS_WORKER, str(bot_backend), str(bot_backend.parent / "shared"), str(tests_dir), db.path]
    workers = [subprocess.Popen(command + [str(i)], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                text=True, cwd=str(bot_backend)) for i in range(4)]
    try:
        ready = [w.stdout.readline().strip() for w in workers]       # each has imported the code and opened the database
        assert ready == ["ready"] * len(workers), [w.communicate(timeout=60)[1] for w in workers]
        for w in workers:                                            # the pipes are the barrier: released together
            w.stdin.write("go\n")
            w.stdin.flush()
        outputs = [w.communicate(timeout=120) for w in workers]
    finally:
        for w in workers:
            if w.poll() is None:
                w.kill()
    assert [w.returncode for w in workers] == [0] * len(workers), [err for _out, err in outputs]
    results = [json.loads(out.strip().splitlines()[-1]) for out, _err in outputs]
    assert sum(r["created"] for r in results) == open_totals(db)["count"] == 2
    assert sorted(reason for r in results for reason in r["refused"]) == [P.POSITION_LIMIT] * 2


# -------------------------------------------------------------------------------------------- tenant isolation
def test_one_account_cannot_read_move_or_use_another_accounts_reservation(ledger, db):
    mine = reserve(ledger, [entry("AAAUSDT")], account="acct-1").created[0]
    theirs = reserve(ledger, [entry("AAAUSDT")], account="acct-2", decision="dec-1").created[0]
    assert mine != theirs
    before = dump(db, schema.LEDGER_TABLE)
    intruder = dict(account_id="acct-2", reservation_id=mine, now_ms=NOW + 10)
    assert ledger.get("acct-2", mine) is None and ledger.history("acct-2", mine) == []
    for attempt in (ledger.mark_submitted(client_order_id="x-1", **intruder),
                    ledger.mark_unknown(resolution_deadline_ms=NOW + 99, **intruder),
                    ledger.mark_filled(filled_quantity=1.0, average_price=PRICE, **intruder),
                    ledger.release(reason="not mine", evidence="forged", **intruder),
                    ledger.resolve_unknown(outcome=R.RELEASED, evidence="forged", **intruder)):
        assert attempt == R.Transition(False, False, None, R.RESERVATION_NOT_FOUND)
    assert dump(db, schema.LEDGER_TABLE) == before
    assert [r.reservation_id for r in ledger.unresolved("acct-2")] == []
    assert {c.reservation_id for c in ledger.open_reservations("acct-2", book=R.EXECUTION_BOOK, now_ms=NOW)} == {theirs}


def test_one_accounts_reservations_never_count_against_another_account(ledger):
    lim = limits("conservative")
    assert len(reserve(ledger, [entry(s) for s in SYMBOLS[:4]], account="acct-1", lim=lim).created) == 4
    other = reserve(ledger, [entry(s) for s in SYMBOLS[:4]], account="acct-2", lim=lim)
    assert len(other.created) == 4 and other.authorization.usage_before.positions == 0
    assert ledger.expire_stale(NOW + 2 * DAY_MS, account_id="acct-2") == 4
    assert len(ledger.open_reservations("acct-1", book=R.EXECUTION_BOOK, now_ms=NOW)) == 4


# -------------------------------------------------------------------------------------------------- lifecycle
def test_the_normal_lifecycle_is_recorded_step_by_step(ledger, db):
    rid = reserve(ledger, [entry("AAAUSDT", risk=0.002)]).created[0]
    key = dict(account_id=ACCOUNT, reservation_id=rid)
    assert ledger.mark_submitted(client_order_id="cf-1", now_ms=NOW + 10, **key) == R.Transition(True, True, R.SUBMITTED)
    assert ledger.mark_submitted(client_order_id="cf-1", now_ms=NOW + 11, **key) == R.Transition(True, False, R.SUBMITTED)
    assert ledger.mark_submitted(client_order_id="cf-2", now_ms=NOW + 12, **key).reason == R.CONFLICTING_REPLAY
    assert ledger.mark_filled(filled_quantity=1.5, average_price=100.2, now_ms=NOW + 20, evidence="order 77 FILLED", **key).ok
    assert ledger.mark_filled(filled_quantity=1.5, average_price=100.2, now_ms=NOW + 21, **key) == R.Transition(True, False, R.FILLED)
    row = ledger.get(ACCOUNT, rid)
    assert (row.state, row.client_order_id, row.submitted_at, row.filled_at, row.filled_quantity, row.filled_price) == (
        R.FILLED, "cf-1", NOW + 10, NOW + 20, 1.5, 100.2)
    assert [(e["from_state"], e["to_state"], e["recorded_at"]) for e in ledger.history(ACCOUNT, rid)] == [
        (None, R.RESERVED, NOW), (R.RESERVED, R.SUBMITTED, NOW + 10), (R.SUBMITTED, R.FILLED, NOW + 20)]
    for late in (ledger.release(reason="late", evidence="x", now_ms=NOW + 30, **key),
                 ledger.mark_unknown(resolution_deadline_ms=NOW + 99, now_ms=NOW + 30, **key),
                 ledger.mark_submitted(client_order_id="cf-1", now_ms=NOW + 30, **key)):
        assert not late.ok and late.state == R.FILLED
    with pytest.raises(R.LedgerError):
        ledger.mark_filled(filled_quantity=0.0, average_price=PRICE, now_ms=NOW, **key)
    with pytest.raises(R.LedgerError):
        ledger.mark_submitted(client_order_id=" ", now_ms=NOW, **key)


def test_the_event_history_is_append_only_and_written_by_the_database(ledger, db):
    rid = reserve(ledger, [entry("AAAUSDT")]).created[0]
    with db.connect() as conn:                                        # a state change made behind the module's back
        conn.execute(f"UPDATE {schema.LEDGER_TABLE} SET state='RELEASED', updated_at=?, last_actor='manual' WHERE reservation_id=?",
                     (NOW + 1, rid))
    assert [(e["to_state"], e["actor"]) for e in ledger.history(ACCOUNT, rid)] == [(R.RESERVED, "pipeline"), (R.RELEASED, "manual")]
    for statement in (f"UPDATE {schema.LEDGER_EVENT_TABLE} SET to_state='FILLED'", f"DELETE FROM {schema.LEDGER_EVENT_TABLE}"):
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            with db.connect() as conn:
                conn.execute(statement)
    assert len(ledger.history(ACCOUNT, rid)) == 2


def test_an_unsent_reservation_expires_with_its_decision_and_frees_its_capacity(ledger, db):
    lim = limits("conservative")
    first = reserve(ledger, [entry(s) for s in SYMBOLS[:4]], lim=lim, expires=NOW + 1_000)
    assert reserve(ledger, [entry("EEEUSDT")], decision="dec-2", lim=lim, now=NOW + 999).refused[0].reason == P.POSITION_LIMIT
    late = ledger.mark_submitted(account_id=ACCOUNT, reservation_id=first.created[0], client_order_id="cf-1", now_ms=NOW + 1_000)
    assert late == R.Transition(False, True, R.EXPIRED, R.RESERVATION_EXPIRED)        # too late: the order must not be sent
    after = reserve(ledger, [entry("EEEUSDT")], decision="dec-3", lim=lim, now=NOW + 1_000)
    assert len(after.created) == 1 and after.authorization.usage_before.positions == 0
    assert {r.state for r in ledger_rows(db, "decision_id='dec-1'")} == {R.EXPIRED}
    assert ledger.expire_stale(NOW + 5_000) == 0                      # the sweep is idempotent
    assert {(e["to_state"], e["detail"]) for r in first.created for e in ledger.history(ACCOUNT, r)} >= {(R.EXPIRED, "TTL")}


def test_an_intent_whose_reservation_ended_is_not_reserved_again(ledger):
    rid = reserve(ledger, [entry("AAAUSDT")]).created[0]
    assert ledger.release(account_id=ACCOUNT, reservation_id=rid, now_ms=NOW + 5, reason="hard risk refused the plan").ok
    assert ledger.release(account_id=ACCOUNT, reservation_id=rid, now_ms=NOW + 6, reason="again") == R.Transition(True, False, R.RELEASED)
    again = reserve(ledger, [entry("AAAUSDT")], now=NOW + 10)
    assert again.created == () and again.reservations == () and again.refused[0].reason == R.INTENT_ALREADY_FINAL
    assert len(reserve(ledger, [entry("AAAUSDT")], decision="dec-2", now=NOW + 20).created) == 1       # a new decision may


# ------------------------------------------------------------------------------------------ submitted / unknown
def test_a_submitted_order_never_expires_and_is_released_only_with_evidence(ledger, db):
    lim = limits("conservative", max_positions=1)
    rid = reserve(ledger, [entry("AAAUSDT")], lim=lim, expires=NOW + 1_000).created[0]
    key = dict(account_id=ACCOUNT, reservation_id=rid)
    assert ledger.mark_submitted(client_order_id="cf-1", now_ms=NOW + 10, **key).ok
    assert ledger.expire_stale(NOW + 10 * DAY_MS) == 0 and ledger.get(ACCOUNT, rid).state == R.SUBMITTED
    assert reserve(ledger, [entry("BBBUSDT")], decision="dec-2", lim=lim, now=NOW + 10 * DAY_MS,
                   expires=NOW + 11 * DAY_MS).refused[0].reason == P.POSITION_LIMIT
    assert ledger.release(reason="gave up", now_ms=NOW + 20, **key) == R.Transition(False, False, R.SUBMITTED, R.EVIDENCE_REQUIRED)
    for forged in ("state='RELEASED'", "state='EXPIRED'", "state='RESERVED'"):
        with pytest.raises(sqlite3.IntegrityError, match="evidence"):
            with db.connect() as conn:
                conn.execute(f"UPDATE {schema.LEDGER_TABLE} SET {forged} WHERE reservation_id=?", (rid,))
    assert [r.reservation_id for r in ledger.unresolved(ACCOUNT)] == [rid]            # what a restarted process must look up
    assert ledger.release(reason="rejected", evidence="venue: -2019 margin is insufficient, no order", now_ms=NOW + 30, **key).ok
    assert ledger.get(ACCOUNT, rid).resolution_evidence.startswith("venue: -2019") and ledger.unresolved(ACCOUNT) == []


def test_an_unknown_outcome_is_never_released_or_expired_until_reconciliation_resolves_it(ledger, db):
    lim = limits("conservative", max_positions=1)
    rid = reserve(ledger, [entry("AAAUSDT")], lim=lim, expires=NOW + 1_000).created[0]
    key = dict(account_id=ACCOUNT, reservation_id=rid)
    ledger.mark_submitted(client_order_id="cf-1", now_ms=NOW + 10, **key)
    assert ledger.mark_unknown(resolution_deadline_ms=NOW + 500, detail="timeout after send", now_ms=NOW + 20, **key).ok
    assert ledger.mark_unknown(resolution_deadline_ms=NOW + 500, now_ms=NOW + 21, **key) == R.Transition(True, False, R.UNKNOWN)
    far = NOW + 400 * DAY_MS                                          # long past its expiry and its deadline
    assert ledger.release(reason="no answer", evidence="silence", now_ms=far, **key) == R.Transition(
        False, False, R.UNKNOWN, R.UNKNOWN_OUTCOME_NOT_RELEASABLE)
    assert ledger.expire_stale(far) == 0
    assert ledger.mark_filled(filled_quantity=1.0, average_price=PRICE, now_ms=far, **key).reason == "NOT_ALLOWED_FROM_UNKNOWN"
    assert ledger.resolve_unknown(outcome=R.RELEASED, evidence="   ", now_ms=far, **key).reason == R.EVIDENCE_REQUIRED
    for forged in ("state='RELEASED'", "state='EXPIRED'", "state='RESERVED'", "state='SUBMITTED'",
                   "state='RELEASED', resolution_evidence=''", "state='RELEASED', resolution_evidence='  '", "state='FILLED'"):
        with pytest.raises(sqlite3.IntegrityError, match="evidence"):
            with db.connect() as conn:
                conn.execute(f"UPDATE {schema.LEDGER_TABLE} SET {forged} WHERE reservation_id=?", (rid,))
    with pytest.raises(sqlite3.IntegrityError, match="never deleted"):
        with db.connect() as conn:
            conn.execute(f"DELETE FROM {schema.LEDGER_TABLE} WHERE reservation_id=?", (rid,))
    row = ledger.get(ACCOUNT, rid)
    assert (row.state, row.unknown_since, row.resolution_deadline) == (R.UNKNOWN, NOW + 20, NOW + 500)
    held_back = reserve(ledger, [entry("BBBUSDT")], decision="dec-2", lim=lim, now=far, expires=far + DAY_MS)
    assert held_back.refused[0].reason == P.POSITION_LIMIT            # the capacity is still held
    assert [r.reservation_id for r in ledger.unresolved()] == [rid]
    assert [f.code for f in R.reconcile_stores(db, ACCOUNT, now_ms=far).findings] == ["UNKNOWN_PAST_DEADLINE"]

    resolved = ledger.resolve_unknown(outcome=R.RELEASED, evidence="order lookup by cf-1: not found; no fills since", now_ms=far + 5, **key)
    assert resolved == R.Transition(True, True, R.RELEASED)
    assert ledger.resolve_unknown(outcome=R.RELEASED, evidence="again", now_ms=far + 6, **key) == R.Transition(True, False, R.RELEASED)
    assert len(reserve(ledger, [entry("BBBUSDT")], decision="dec-3", lim=lim, now=far + 10, expires=far + DAY_MS).created) == 1
    assert [e["to_state"] for e in ledger.history(ACCOUNT, rid)] == [R.RESERVED, R.SUBMITTED, R.UNKNOWN, R.RELEASED]


def test_an_unknown_outcome_that_turns_out_filled_is_resolved_with_the_fill(ledger):
    rid = reserve(ledger, [entry("AAAUSDT")]).created[0]
    key = dict(account_id=ACCOUNT, reservation_id=rid)
    assert ledger.mark_unknown(resolution_deadline_ms=NOW + 500, now_ms=NOW + 5, **key).ok        # straight from RESERVED
    with pytest.raises(R.LedgerError):
        ledger.resolve_unknown(outcome=R.FILLED, evidence="found", now_ms=NOW + 6, **key)
    with pytest.raises(R.LedgerError):
        ledger.resolve_unknown(outcome=R.EXPIRED, evidence="found", now_ms=NOW + 6, **key)
    assert ledger.resolve_unknown(outcome=R.FILLED, evidence="order 91 FILLED 1.0 @ 100.1", filled_quantity=1.0,
                                  average_price=100.1, now_ms=NOW + 7, **key).ok
    row = ledger.get(ACCOUNT, rid)
    assert (row.state, row.filled_quantity, row.filled_price, row.filled_at) == (R.FILLED, 1.0, 100.1, NOW + 7)
    assert "order 91" in row.resolution_evidence


def test_final_states_do_not_change_and_identity_is_immutable(ledger, db):
    rid = reserve(ledger, [entry("AAAUSDT")]).created[0]
    ledger.release(account_id=ACCOUNT, reservation_id=rid, now_ms=NOW + 5, reason="unused")
    with pytest.raises(sqlite3.IntegrityError, match="final"):
        with db.connect() as conn:
            conn.execute(f"UPDATE {schema.LEDGER_TABLE} SET state='RESERVED' WHERE reservation_id=?", (rid,))
    with pytest.raises(sqlite3.IntegrityError, match="immutable"):
        with db.connect() as conn:
            conn.execute(f"UPDATE {schema.LEDGER_TABLE} SET account_id='acct-2' WHERE reservation_id=?", (rid,))
    with pytest.raises(sqlite3.IntegrityError):
        with db.connect() as conn:
            conn.execute(f"UPDATE {schema.LEDGER_TABLE} SET state='PARKED' WHERE reservation_id=?", (rid,))
    assert ledger.get(ACCOUNT, rid).state == R.RELEASED


def test_evidence_is_stored_without_credential_like_content(ledger):
    rid = reserve(ledger, [entry("AAAUSDT")]).created[0]
    key = dict(account_id=ACCOUNT, reservation_id=rid)
    ledger.mark_submitted(client_order_id="cf-1", now_ms=NOW + 1, **key)
    ledger.release(reason="rejected", evidence="rejected; request had apiKey=AbCdEf0123456789AbCdEf0123456789 signature=deadbeef",
                   now_ms=NOW + 2, **key)
    stored = ledger.get(ACCOUNT, rid).resolution_evidence + json.dumps(ledger.history(ACCOUNT, rid))
    assert "AbCdEf0123456789" not in stored and "deadbeef" not in stored and "rejected" in stored


# ------------------------------------------------------------------------------- fills, releases, the brake latch
def test_a_fill_holds_its_capacity_until_a_snapshot_taken_after_it_shows_the_account(ledger):
    lim = limits("conservative", max_positions=1)
    rid = reserve(ledger, [entry("AAAUSDT", risk=0.002)], lim=lim).created[0]
    ledger.mark_submitted(account_id=ACCOUNT, reservation_id=rid, client_order_id="cf-1", now_ms=NOW + 10)
    ledger.mark_filled(account_id=ACCOUNT, reservation_id=rid, filled_quantity=2.0, average_price=PRICE, now_ms=NOW + 20)
    older = ledger.open_reservations(ACCOUNT, book=R.EXECUTION_BOOK, now_ms=NOW + 30, as_of_ms=NOW - 1_000)
    assert [(c.symbol, c.state) for c in older] == [("AAAUSDT", R.FILLED)]
    assert ledger.open_reservations(ACCOUNT, book=R.EXECUTION_BOOK, now_ms=NOW + 30, as_of_ms=NOW + 25) == ()
    blind = reserve(ledger, [entry("BBBUSDT")], decision="dec-2", lim=lim, now=NOW + 30, snap=snapshot(as_of=NOW - 1_000))
    assert blind.refused[0].reason == P.POSITION_LIMIT               # that snapshot cannot show the new position
    seeing = snapshot(as_of=NOW + 25, positions=(held("AAAUSDT", 2.0),))
    counted_once = reserve(ledger, [entry("BBBUSDT")], decision="dec-3", lim=lim, now=NOW + 30, snap=seeing)
    assert counted_once.refused[0].reason == P.POSITION_LIMIT and counted_once.authorization.usage_before.positions == 1
    assert counted_once.authorization.usage_before.open_risk_amount == 20.0
    closed_since = reserve(ledger, [entry("BBBUSDT")], decision="dec-4", lim=lim, now=NOW + 30, snap=snapshot(as_of=NOW + 25))
    assert len(closed_since.created) == 1


def test_an_entry_that_stands_on_an_exit_is_not_submitted_before_the_exit_is_visible(ledger):
    lim = limits("conservative")
    full = snapshot(positions=tuple(held(s, 1.0) for s in SYMBOLS[:4]))
    outcome = reserve(ledger, [ExitIntent(symbol="AAAUSDT", direction=LONG, reason="SIGNAL"), entry("EEEUSDT")], lim=lim, snap=full)
    assert [(a.symbol, a.kind) for a in outcome.authorization.accepted] == [("AAAUSDT", "EXIT"), ("EEEUSDT", "ENTRY")]
    row = outcome.reservation("EEEUSDT")
    assert outcome.created == (row.reservation_id,) and row.awaits_releases == (P.Release("AAAUSDT", 0.0),)
    key = dict(account_id=ACCOUNT, reservation_id=row.reservation_id, client_order_id="cf-1")
    not_yet = R.Transition(False, False, R.RESERVED, R.RELEASE_NOT_CONFIRMED)
    assert ledger.mark_submitted(now_ms=NOW + 10, **key) == not_yet                               # no evidence at all
    assert ledger.mark_submitted(now_ms=NOW + 10, snapshot=full, **key) == not_yet                # the snapshot it was made from
    still_held = snapshot(as_of=NOW + 5, positions=tuple(held(s, 1.0) for s in SYMBOLS[:4]))
    assert ledger.mark_submitted(now_ms=NOW + 10, snapshot=still_held, **key) == not_yet          # the exit has not happened
    other_account = snapshot(account="acct-2", as_of=NOW + 5)
    assert ledger.mark_submitted(now_ms=NOW + 10, snapshot=other_account, **key) == not_yet
    exited = snapshot(as_of=NOW + 5, positions=tuple(held(s, 1.0) for s in SYMBOLS[1:4]))
    assert ledger.mark_submitted(now_ms=NOW + 10, snapshot=exited, **key) == R.Transition(True, True, R.SUBMITTED)
    plain = reserve(ledger, [entry("FFFUSDT")], decision="dec-2", lim=limits("balanced"), snap=exited, now=NOW + 20).reservation("FFFUSDT")
    assert plain.awaits_releases == () and ledger.mark_submitted(account_id=ACCOUNT, reservation_id=plain.reservation_id,
                                                                  client_order_id="cf-2", now_ms=NOW + 30).ok


def test_a_terminal_stop_latched_in_the_database_overrides_a_stale_brake_state(ledger, db):
    thresholds = brakes.BrakeThresholds.from_profile("conservative")

    def stop(account, brake_scope):
        return brakes.evaluate(db, account_id=account, scope=brake_scope, risk_day="2026-10-08", day_definition=brakes.UTC_DAY,
                               closing_equity=EQUITY * 0.5, thresholds=thresholds, now_ms=NOW, initial_equity=EQUITY)

    assert stop("acct-1", brakes.ACCOUNT_SCOPE).halted and stop("acct-2", "family:daily_trend").halted
    refused = reserve(ledger, [entry("AAAUSDT")], account="acct-1", brake=NO_BRAKE)      # the caller's flags are out of date
    assert refused.created == () and refused.refused[0].reason == P.BRAKE_ACTIVE and refused.authorization.close_all_positions
    exits_pass = reserve(ledger, [ExitIntent(symbol="AAAUSDT", direction=LONG, reason="BRAKE")], account="acct-1", decision="dec-x",
                         snap=snapshot(positions=(held("AAAUSDT", 1.0),)))
    assert [a.kind for a in exits_pass.authorization.accepted] == ["EXIT"]
    scoped = reserve(ledger, [entry("AAAUSDT")], account="acct-2", brake_scope="family:daily_trend")
    assert scoped.refused[0].reason == P.BRAKE_ACTIVE
    other_family = reserve(ledger, [entry("AAAUSDT")], account="acct-2", decision="dec-2", brake_scope="family:other")
    assert len(other_family.created) == 1
    assert len(reserve(ledger, [entry("AAAUSDT")], account="acct-3").created) == 1
    assert reserve(ledger, [entry("BBBUSDT")], account="acct-3", decision="dec-2", brake=None).refused[0].reason == P.BRAKE_STATE_UNKNOWN
    # a shadow book has its own equity and brake state: the executing account's stop is not the shadow book's
    assert len(reserve(ledger, [entry("CCCUSDT")], account="acct-1", decision="dec-shadow", mode="SHADOW").created) == 1
    named = reserve(ledger, [entry("DDDUSDT")], account="acct-1", decision="dec-shadow-2", mode="SHADOW",
                    brake_scope=brakes.ACCOUNT_SCOPE)
    assert named.refused[0].reason == P.BRAKE_ACTIVE


# ---------------------------------------------------------------------------------------------- legacy stores
def test_open_legacy_reservations_are_imported_once_and_the_legacy_rows_are_left_alone(ledger, db):
    add_bot(db)
    add_bot(db, bot="bot-2")
    add_bot(db, bot="bot-9", account="acct-2", user="user-2")
    store, pending_id = legacy_production_reservation(db, "SOLUSDT")
    store.mark_resolution_pending(pending_id, NOW + 5, trade_plan_id="tp-1", execution_attempt_id="ea-1",
                                  resolution_deadline=NOW + 60_000, note="timeout after send")
    shadow = store.reserve(broker_account_id=ACCOUNT, bot_instance_id="bot-2", cycle_id="cycle-shadow",
                           selected=[("cand-s", "ADA/USDT:PERP", "binance", "ADAUSDT", "LONG")], now_ms=NOW, ttl_seconds=600)
    assert shadow.reserved
    add_pending(db, "XRPUSDT", bot="bot-2", submit="SUBMIT_UNKNOWN", notional=250.0)
    add_pending(db, "DOGEUSDT", bot="bot-2", submit="SUBMIT_CONFIRMED", notional=120.0)
    add_pending(db, "LTCUSDT", bot="bot-2", state="OPEN_FAILED")
    add_pending(db, "BNBUSDT", bot="bot-9", notional=500.0)           # another account's
    legacy_before = {t: dump(db, t) for t in ("cati_portfolio_reservations", "pending_entries")}

    first = ledger.import_legacy(ACCOUNT, NOW + 100)
    assert len(first.created) == 3 and first.updated == () and first.held_unknown == ()
    mirrors = {m.symbol: m for m in ledger_rows(db, "account_id=? AND source<>'PIPELINE'", ACCOUNT)}
    assert {s: (m.source, m.state, m.book) for s, m in mirrors.items()} == {
        "SOLUSDT": (R.LEGACY_CATI_RESERVATION, R.UNKNOWN, R.EXECUTION_BOOK),
        "XRPUSDT": (R.LEGACY_PENDING_ENTRY, R.UNKNOWN, R.EXECUTION_BOOK),
        "DOGEUSDT": (R.LEGACY_PENDING_ENTRY, R.SUBMITTED, R.EXECUTION_BOOK)}
    assert (mirrors["XRPUSDT"].notional, mirrors["SOLUSDT"].notional, mirrors["SOLUSDT"].risk_amount) == (250.0, None, None)
    assert ledger_rows(db, "account_id='acct-2'") == []               # nothing crossed to the other account
    rows_after_first, events_after_first = dump(db, schema.LEDGER_TABLE), dump(db, schema.LEDGER_EVENT_TABLE)

    second = R.import_legacy_reservations(db, ACCOUNT, now_ms=NOW + 200)
    assert (second.created, second.updated, second.unchanged) == ((), (), 3)
    assert dump(db, schema.LEDGER_TABLE) == rows_after_first and dump(db, schema.LEDGER_EVENT_TABLE) == events_after_first
    assert {t: dump(db, t) for t in legacy_before} == legacy_before   # no legacy row was written, moved or deleted
    assert R.reconcile_stores(db, ACCOUNT, now_ms=NOW + 200).consistent
    assert len(ledger.import_legacy("acct-2", NOW + 300).created) == 1


def test_legacy_claims_count_against_the_account_inside_the_reserve_transaction(ledger, db):
    add_bot(db)
    lim = limits("conservative")
    legacy_production_reservation(db, "SOLUSDT")                      # a legacy claim that states no size
    blocked = reserve(ledger, [entry("AAAUSDT")], lim=lim, now=NOW + 10)
    assert blocked.created == () and blocked.refused[0].reason == P.LEGACY_CLAIM_UNSIZED
    assert blocked.authorization.usage_before.positions == 1 and blocked.authorization.usage_before.unsized == ("SOLUSDT",)
    add_pending(db, "SOLUSDT", notional=300.0)                        # its entry lock now states the notional
    sized = reserve(ledger, [entry(s, risk=0.0025) for s in SYMBOLS[:4]], decision="dec-2", lim=lim, now=NOW + 20)
    before = sized.authorization.usage_before
    assert (before.positions, before.gross_notional, before.reserved_margin) == (1, 300.0, 300.0)
    assert before.open_risk_amount == lim.per_trade_risk * EQUITY     # one legacy entry may risk the engine ceiling
    assert len(sized.created) == 3 and sized.refused[0].reason == P.POSITION_LIMIT       # 3 slots were left
    assert sized.authorization.usage_after.open_risk_fraction <= lim.max_open_risk * (1 + 1e-9)
    realised = snapshot(positions=(held("SOLUSDT", 3.0),), as_of=NOW + 25)
    later = reserve(ledger, [entry("PPPUSDT", risk=0.0005)], decision="dec-3", lim=limits("balanced"), snap=realised, now=NOW + 30)
    assert later.authorization.usage_before.symbols.count("SOLUSDT") == 1 and len(later.created) == 1


def test_a_mirror_follows_its_legacy_store_and_is_never_driven_from_the_ledger(ledger, db):
    add_bot(db)
    store, legacy_id = legacy_production_reservation(db, "SOLUSDT")
    mirror = ledger.import_legacy(ACCOUNT, NOW + 10).created[0]
    key = dict(account_id=ACCOUNT, reservation_id=mirror)
    follows = R.Transition(False, False, R.RESERVED, R.LEGACY_ROW_FOLLOWS_ITS_STORE)
    assert ledger.release(reason="not ours", now_ms=NOW + 20, **key) == follows
    assert ledger.mark_submitted(client_order_id="x", now_ms=NOW + 20, **key) == follows
    assert ledger.unresolved(ACCOUNT) == []                           # the legacy store owns its own recovery

    store.mark_resolution_pending(legacy_id, NOW + 30, trade_plan_id="tp-1", execution_attempt_id="ea-1",
                                  resolution_deadline=NOW + 60_000, note="timeout")
    assert [f.code for f in R.reconcile_stores(db, ACCOUNT, now_ms=NOW + 35).findings] == ["MIRROR_STATE_DIFFERS"]
    assert ledger.import_legacy(ACCOUNT, NOW + 40).updated == (mirror,) and ledger.get(ACCOUNT, mirror).state == R.UNKNOWN
    assert ledger.resolve_unknown(outcome=R.RELEASED, evidence="guess", now_ms=NOW + 45, **key).reason == R.LEGACY_ROW_FOLLOWS_ITS_STORE
    assert [r["reservation_id"] for r in store.pending_resolutions(ACCOUNT)] == [legacy_id]      # legacy recovery list intact
    assert store.overdue_pending(NOW + 70_000)[0]["reservation_id"] == legacy_id

    assert store.resolve_pending(legacy_id, "RELEASED", NOW + 50, note="broker shows no order")  # the legacy reconciliation
    assert ledger.import_legacy(ACCOUNT, NOW + 60).updated == (mirror,)
    row = ledger.get(ACCOUNT, mirror)
    assert row.state == R.RELEASED and "broker shows no order" in row.resolution_evidence
    assert [e["to_state"] for e in ledger.history(ACCOUNT, mirror)] == [R.RESERVED, R.UNKNOWN, R.RELEASED]
    assert R.reconcile_stores(db, ACCOUNT, now_ms=NOW + 70).consistent
    assert len(reserve(ledger, [entry("AAAUSDT")], now=NOW + 80).created) == 1


def test_pending_entries_are_followed_through_their_own_lifecycle(ledger, db):
    add_bot(db)
    add_pending(db, "XRPUSDT", notional=250.0)

    def legacy(**changes):
        with db.connect() as conn:
            for column, value in changes.items():
                conn.execute(f"UPDATE pending_entries SET {column}=? WHERE symbol='XRPUSDT'", (value,))

    mirror = ledger.import_legacy(ACCOUNT, NOW + 1).created[0]
    assert ledger.get(ACCOUNT, mirror).state == R.RESERVED and ledger.get(ACCOUNT, mirror).expires_at is None
    assert ledger.expire_stale(NOW + 400 * DAY_MS) == 0               # a mirror is never expired from here
    legacy(submit_state="SUBMIT_CONFIRMED", submitted_notional=260.0)
    ledger.import_legacy(ACCOUNT, NOW + 2)
    assert (ledger.get(ACCOUNT, mirror).state, ledger.get(ACCOUNT, mirror).notional) == (R.SUBMITTED, 260.0)
    legacy(state="OPEN_CONFIRMED", filled_notional=255.0, confirmed_at_ms=NOW + 3)
    ledger.import_legacy(ACCOUNT, NOW + 4)
    row = ledger.get(ACCOUNT, mirror)
    assert (row.state, row.notional, row.filled_at) == (R.FILLED, 255.0, NOW + 3)
    with db.connect() as conn:                                        # the position closed: the legacy lock is removed
        conn.execute("DELETE FROM pending_entries WHERE symbol='XRPUSDT'")
    again = ledger.import_legacy(ACCOUNT, NOW + 5)
    assert (again.created, again.updated) == ((), ()) and R.reconcile_stores(db, ACCOUNT, now_ms=NOW + 6).consistent

    add_pending(db, "XRPUSDT", notional=100.0)                        # the same lock key is taken again later
    reborn = ledger.import_legacy(ACCOUNT, NOW + 7).created[0]
    assert reborn != mirror and ledger.get(ACCOUNT, reborn).legacy_generation == 1
    with db.connect() as conn:
        conn.execute("DELETE FROM pending_entries WHERE symbol='XRPUSDT'")
    assert ledger.import_legacy(ACCOUNT, NOW + 8).updated == (reborn,) and ledger.get(ACCOUNT, reborn).state == R.RELEASED


def test_a_vanished_legacy_row_never_releases_an_unknown_outcome(ledger, db):
    add_bot(db)
    lim = limits("conservative", max_positions=1)
    add_pending(db, "XRPUSDT", submit="SUBMIT_UNKNOWN", notional=250.0)
    mirror = ledger.import_legacy(ACCOUNT, NOW + 1).created[0]
    with db.connect() as conn:                                        # e.g. EntryProtection.clear_all_for_bot
        conn.execute("DELETE FROM pending_entries")
    swept = ledger.import_legacy(ACCOUNT, NOW + 2)
    assert swept.held_unknown == (mirror,) and ledger.get(ACCOUNT, mirror).state == R.UNKNOWN
    assert reserve(ledger, [entry("AAAUSDT")], lim=lim, now=NOW + 3).refused[0].reason == P.POSITION_LIMIT
    findings = R.reconcile_stores(db, ACCOUNT, now_ms=NOW + 4).findings
    assert [(f.code, f.store) for f in findings] == [("MIRROR_WITHOUT_LEGACY_ROW", R.LEGACY_PENDING_ENTRY)]
    key = dict(account_id=ACCOUNT, reservation_id=mirror)
    assert ledger.release(reason="row is gone", evidence="x", now_ms=NOW + 5, **key).reason == R.LEGACY_ROW_FOLLOWS_ITS_STORE
    assert ledger.resolve_unknown(outcome=R.RELEASED, evidence="", now_ms=NOW + 5, **key).reason == R.EVIDENCE_REQUIRED
    assert ledger.resolve_unknown(outcome=R.RELEASED, evidence="venue: no order for cid-bot-1-XRPUSDT, position flat",
                                  now_ms=NOW + 6, **key).ok
    assert len(reserve(ledger, [entry("AAAUSDT")], decision="dec-2", lim=lim, now=NOW + 7).created) == 1
    assert R.reconcile_stores(db, ACCOUNT, now_ms=NOW + 8).consistent


def test_reconcile_stores_reports_every_disagreement_and_changes_nothing(ledger, db):
    add_bot(db)
    clean = R.reconcile_stores(db, ACCOUNT, now_ms=NOW)
    assert clean.consistent and clean.findings == () and clean.not_inspected == R.VOLATILE_STORES
    assert dict(clean.counts) == {"ledger_open_pipeline": 0, "ledger_open_mirrors": 0, "legacy_open_claims": 0}

    reserve(ledger, [entry("AAAUSDT")], expires=NOW + 1_000)
    store, legacy_id = legacy_production_reservation(db, "SOLUSDT", cycle="cycle-2")
    add_pending(db, "AAAUSDT", notional=100.0)                        # a legacy entry on the symbol the pipeline reserved
    tables = (schema.LEDGER_TABLE, schema.LEDGER_EVENT_TABLE, "cati_portfolio_reservations", "pending_entries")
    before = {t: dump(db, t) for t in tables}
    report = R.reconcile_stores(db, ACCOUNT, now_ms=NOW + 5_000)
    assert {t: dump(db, t) for t in tables} == before                 # read only
    assert not report.consistent
    assert sorted((f.code, f.store) for f in report.findings) == [
        ("LEGACY_CLAIM_NOT_MIRRORED", R.LEGACY_CATI_RESERVATION), ("LEGACY_CLAIM_NOT_MIRRORED", R.LEGACY_PENDING_ENTRY),
        ("RESERVED_PAST_EXPIRY", P.PIPELINE_SOURCE), ("SYMBOL_CLAIMED_TWICE", R.LEGACY_PENDING_ENTRY)]
    assert dict(report.counts) == {"ledger_open_pipeline": 1, "ledger_open_mirrors": 0, "legacy_open_claims": 2}

    ledger.import_legacy(ACCOUNT, NOW + 6_000)
    ledger.expire_stale(NOW + 6_000)
    assert R.reconcile_stores(db, ACCOUNT, now_ms=NOW + 6_000).consistent
    assert store.release(legacy_id, NOW + 7_000)                      # the legacy store moves on by itself
    drift = R.reconcile_stores(db, ACCOUNT, now_ms=NOW + 7_000)
    assert [(f.code, f.store) for f in drift.findings] == [("MIRROR_STATE_DIFFERS", R.LEGACY_CATI_RESERVATION)]
    ledger.import_legacy(ACCOUNT, NOW + 8_000)
    assert R.reconcile_stores(db, ACCOUNT, now_ms=NOW + 8_000).consistent
