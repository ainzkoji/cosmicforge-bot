"""Step 3.14: the retention policy, controlled removal of raw operational detail, and database growth.

Every test runs on a temporary database and a temporary archive directory. Nothing here is evidence about a
strategy; the synthetic market and the fake broker only produce rows to count, keep and remove.
"""
from __future__ import annotations

import ast
import hashlib
import json
import os
import re
import shutil
import sqlite3
import time
from pathlib import Path
from types import SimpleNamespace

import pytest
from test_demo_boundary_certification import broker
from test_production_demo_execution import demo, demo_profile
from test_production_execution import live, profile
from test_production_research_separation import fresh

import _step2
import _step3 as S
from app.observability import account_recorder
from app.trading_intelligence.families.daily_trend import spec
from app.trading_intelligence.families.daily_trend import targets as T
from app.trading_intelligence.governance.promotion import PromotionGovernance
from app.trading_intelligence.integration import production_execution as production
from app.trading_intelligence.integration import production_runtime as runtime
from app.trading_intelligence.pipeline import coordination as K
from app.trading_intelligence.pipeline import daily_trend as DT
from app.trading_intelligence.pipeline import driver as D
from app.trading_intelligence.pipeline import family_registry as FR
from app.trading_intelligence.pipeline import retention as R
from app.trading_intelligence.pipeline.accounts import CombinedAccounts, ConnectedAccounts, ModelAccounts
from app.trading_intelligence.pipeline.market_data import FrozenPanelBars

__all__ = ["broker", "live", "demo", "fresh", "profile", "demo_profile"]

DAY = R.DAY_MS
NOW = 1_800_000_000_000                       # a fixed "now" for the synthetic rows (2027-01-15)
START = DT.day_of("2020-09-01")
EQUITY = spec.INTERPRETATION["initial_equity_usdt"]
BACKENDS = Path(runtime.__file__).resolve().parents[4]
METRICS = "cati_platform_cycle_metrics"
SNAPSHOTS = "account_equity_snapshots"
#: The owner's "do not delete" list, as tables. Each must be in a protected class.
DO_NOT_DELETE = {
    "exchange execution audit evidence": ("cati_execution_attempts", "cati_trade_plans", "cati_execution_evaluations",
                                          "cati_production_fills", "cati_risk_decisions", "cati_execution_intents",
                                          "cati_user_stream_fills", "cati_family_decisions", "events", "trade_fills"),
    "certification records": ("cati_certification_runs", "cati_certification_stage_results", "cati_adapter_cert_events",
                              "cati_adapter_cert_checks", "cati_demo_certifications"),
    "hypothesis history": ("cati_experiment_registry", "cati_residual_decisions", "cati_shadow_comparisons"),
    "holdout records": ("cati_holdout_registry",),
    "governance promotion history": ("cati_promotion_phase_history", "cati_promotion_scopes", "cati_governance_controls",
                                     "cati_family_events"),
    "mandatory financial records": ("cati_account_income", "broker_transfer_events", "account_equity_daily",
                                    "billing_events", "commission_ledger", "invoices"),
    "unsettled or unknown execution reconciliation": ("cati_production_closes", "cati_portfolio_reservations",
                                                      "cati_production_protection", "cati_production_protection_uncertainty",
                                                      "cati_protection_state", "pending_entries", "cati_production_state",
                                                      "cati_reservation_ledger"),
}


def full_database(tmp_path):
    """Everything ``migrate`` and the production runtime create, plus the tables created on first use."""
    db = S.fresh_db(tmp_path)
    runtime.initialize(db)
    from app.ops.runtime_ownership import ensure_ownership_schema

    ensure_ownership_schema(db)
    return db


@pytest.fixture
def db(tmp_path):
    return full_database(tmp_path)


def count(db, table, where="1=1", args=()):
    with db.connect() as conn:
        return conn.execute(f"SELECT COUNT(*) FROM {table} WHERE {where}", args).fetchone()[0]


def fingerprint(db, skip=()):
    """Rows and a content hash of every table: what a retention run may not change shows up here."""
    out = {}
    with db.connect() as conn:
        for table in R.database_tables(conn):
            if table in skip:
                continue
            digest, rows = hashlib.sha256(), 0
            for row in conn.execute(f"SELECT * FROM {table} ORDER BY rowid"):
                digest.update(repr(tuple(row)).encode("utf-8"))
                rows += 1
            out[table] = (rows, digest.hexdigest())
    return out


def triggers(db):
    with db.connect() as conn:
        return [tuple(r) for r in conn.execute("SELECT name, tbl_name, sql FROM sqlite_master WHERE type='trigger' ORDER BY name")]


def add_cycle(conn, cycle_id, status="COMPLETED", at=NOW):
    conn.execute("INSERT INTO cati_platform_cycles (cycle_id, family_id, cycle_key, mode, status, owner, started_at, heartbeat_at) "
                 "VALUES (?,?,?,?,?,?,?,?)", (cycle_id, "DAILY_TREND", cycle_id, "SHADOW", status, "w", at, at))


def add_metric(conn, cycle_id, stage, age_days, *, account="", duration=10.0, items=0):
    conn.execute(f"INSERT INTO {METRICS} (cycle_id, stage, account_id, duration_ms, items, recorded_at) VALUES (?,?,?,?,?,?)",
                 (cycle_id, stage, account, duration, items, NOW - int(age_days * DAY)))


def seed_metrics(db, old_cycles=6, fresh_cycles=2):
    """``old_cycles`` finished cycles 40+ days old (4 stage rows each), ``fresh_cycles`` recent ones, and one
    old cycle that never finished."""
    with db.connect() as conn:
        for n in range(old_cycles):
            add_cycle(conn, f"old-{n}")
            for stage, ms in (("VERIFY_MANDATE", 2.0), ("MARKET_STAGE", 12.0), ("ACCOUNT_LIST", 1.0), ("ACCOUNT_STAGE", 300.0 + n)):
                add_metric(conn, f"old-{n}", stage, 40 + n, duration=ms, items=50 if stage == "ACCOUNT_STAGE" else 0)
        for n in range(fresh_cycles):
            add_cycle(conn, f"fresh-{n}")
            for stage in ("MARKET_STAGE", "ACCOUNT_STAGE"):
                add_metric(conn, f"fresh-{n}", stage, 3 + n)
        add_cycle(conn, "unfinished", status="PARTIAL")
        for stage in ("MARKET_STAGE", "ACCOUNT_STAGE"):
            add_metric(conn, "unfinished", stage, 60)


# ═══════════════════════════════════════════════════════════════════════════════════════ the policy
def test_every_table_of_the_database_is_classified(db):
    """Fails the day a table exists that the policy does not name: it must be given a class in
    ``retention.POLICY`` (and a PruneRule if, and only if, it is removable raw detail)."""
    assert R.unclassified_tables(db) == []
    report = R.policy_report(db)
    assert report["unclassified"] == [] and sum(report["by_class_in_database"].values()) == report["tables_in_database"]
    assert set(report["by_class_in_database"]) == set(R.CLASS_RULES) and report["misclassified_guarded"] == []
    with pytest.raises(R.UnclassifiedTable):
        R.classify("a_table_nobody_classified")
    with db.connect() as conn:
        conn.execute("CREATE TABLE a_table_nobody_classified (x)")
    assert R.unclassified_tables(db) == ["a_table_nobody_classified"]           # this is what fails the build


def test_every_table_the_source_can_create_is_classified():
    """Tables created on first use are not in a fresh database. The same rule applies to them: every table
    name a CREATE TABLE statement of the application can produce is in the policy."""
    literal = re.compile(r"CREATE\s+TABLE(?:\s+IF\s+NOT\s+EXISTS)?\s+([a-z][a-z0-9_]*)\s*\(", re.IGNORECASE)
    templated = re.compile(r"CREATE\s+TABLE(?:\s+IF\s+NOT\s+EXISTS)?\s+\{([A-Za-z_][A-Za-z0-9_]*)\}", re.IGNORECASE)
    found = {}
    for root in (BACKENDS / "bot-backend" / "app", BACKENDS / "shared" / "shared_lib", BACKENDS / "user-backend" / "app"):
        for path in (root.rglob("*.py") if root.is_dir() else ()):
            text = path.read_text(encoding="utf-8", errors="replace")
            names = {m.lower() for m in literal.findall(text) if m.islower()}
            for constant in set(templated.findall(text)):
                value = re.search(rf"^{constant}\s*=\s*[\"']([a-z][a-z0-9_]*)[\"']", text, re.MULTILINE)
                if value:
                    names.add(value.group(1))
            for name in names:
                found.setdefault(name, path.name)
    assert len(found) > 200                                                      # the scan really reads the schema
    missing = {name: where for name, where in found.items() if name not in R.POLICY}
    assert not missing, f"tables without a retention class: {missing}"


def test_the_policy_is_one_class_per_table_and_the_do_not_delete_list_is_protected():
    listed = [t for _, tables in R._BY_CLASS for t in tables]
    assert len(listed) == len(set(listed)) == len(R.POLICY) and set(c for c, _ in R._BY_CLASS) == set(R.CLASS_RULES)
    for what, tables in DO_NOT_DELETE.items():
        for table in tables:
            assert R.is_protected(table), f"{what}: {table} is not protected"
            assert table not in R.PRUNE_RULES
    assert set(R.PROTECTED_CLASSES) == set(R.CLASS_RULES) - {R.RAW_OPERATIONAL_DETAIL, R.CACHE}
    for table, rule in R.PRUNE_RULES.items():                                    # a rule exists only for raw detail
        assert R.POLICY[table] == R.RAW_OPERATIONAL_DETAIL and rule.table == table
        assert 0 < rule.minimum_retention_days <= rule.default_retention_days
    assert sorted(R.PRUNE_RULES) == [SNAPSHOTS, METRICS]
    assert R.PRUNE_RULES[SNAPSHOTS].default_retention_days == account_recorder.RETENTION_DAYS


def test_no_table_the_database_guards_is_prunable(db):
    with db.connect() as conn:
        guarded = R.guarded_tables(conn)
    assert len(guarded) > 40 and "cati_execution_evaluations" in guarded and "cati_trade_plans" in guarded
    assert not set(guarded) & set(R.PRUNE_RULES)
    for table in guarded:                                                        # every guarded table is protected evidence
        assert R.is_protected(table), table
    assert R.POLICY["cati_execution_evaluations"] == R.EXECUTION_AUDIT


def test_no_retention_function_can_select_a_protected_table(db, tmp_path):
    seed_metrics(db)
    before, guards = fingerprint(db), triggers(db)
    with db.connect() as conn:
        tables = R.database_tables(conn)
    refused = 0
    for table in tables:
        if table in R.PRUNE_RULES:
            continue
        for call in (lambda: R.run_retention(db, table=table, archive_dir=tmp_path / "archive", now_ms=NOW + 10_000 * DAY),
                     lambda: R.plan_retention(db, table=table, now_ms=NOW + 10_000 * DAY)):
            with pytest.raises(R.RetentionRefused):
                call()
        with pytest.raises(R.RetentionRefused):
            R.retention_days_for(table)
        refused += 1
    assert refused == len(tables) - len(R.PRUNE_RULES) > 240
    for name in ("users; DROP TABLE users", "sqlite_master", "no_such_table", "", None, "cati_execution_evaluations --"):
        with pytest.raises(R.RetentionRefused):
            R.run_retention(db, table=name, archive_dir=tmp_path / "archive", now_ms=NOW)
    assert fingerprint(db) == before and triggers(db) == guards
    assert not (tmp_path / "archive").exists()                                   # not even an export was attempted


def test_a_guarded_table_is_refused_even_if_the_policy_called_it_prunable(db, tmp_path):
    seed_metrics(db)
    with db.connect() as conn:                       # the database says append-only; the policy still says prunable
        conn.execute(f"CREATE TRIGGER guard_metrics BEFORE DELETE ON {METRICS} BEGIN SELECT RAISE(ABORT, 'append-only'); END")
    before = count(db, METRICS)
    with pytest.raises(R.RetentionRefused, match="GUARDED_BY_TRIGGER"):
        R.run_retention(db, table=METRICS, archive_dir=tmp_path / "archive", now_ms=NOW)
    assert count(db, METRICS) == before and any(name == "guard_metrics" for name, _, _ in triggers(db))


def test_retention_reaches_nothing_of_the_research_register():
    """Operational storage retention and research governance are separate: the register is a file under
    ``research/``; this module imports nothing of research and names no path of it."""
    source = Path(R.__file__).read_text(encoding="utf-8")
    for node in ast.walk(ast.parse(source)):
        modules = [node.module or ""] if isinstance(node, ast.ImportFrom) else \
            [a.name for a in node.names] if isinstance(node, ast.Import) else []
        for module in modules:
            assert "research" not in module and "families" not in module, module
    code = "\n".join(line for line in source.split('"""')[2::2])                 # everything but the docstrings
    assert "register.jsonl" not in code and "docs/research" not in code and "shutil" not in code
    assert code.count(".unlink(") == 1                                           # its own unfinished .tmp export, only


# ═══════════════════════════════════════════════════════════════════════════════════ a retention run
def test_expired_raw_detail_is_summarised_exported_and_removed(db, tmp_path):
    seed_metrics(db)
    archive = tmp_path / "archive"
    plan = R.plan_retention(db, table=METRICS, now_ms=NOW)
    assert (plan["rows"], plan["retention_days"], plan["cutoff_ms"]) == (24, 30, NOW - 30 * DAY)
    assert count(db, METRICS) == 30 and not archive.exists()                     # a plan reads, it removes nothing
    with db.connect() as conn:
        expected = [tuple(r) for r in conn.execute(f"SELECT rowid, * FROM {METRICS} WHERE cycle_id LIKE 'old-%' ORDER BY rowid")]
    others = fingerprint(db, skip=(METRICS, "cati_retention_runs", "cati_retention_summaries"))

    result = R.run_retention(db, table=METRICS, archive_dir=archive, now_ms=NOW)
    assert (result["status"], result["rows_exported"], result["rows_removed"], result["more"]) == ("COMPLETED", 24, 24, False)
    # what is left: the recent cycles and the cycle that never finished, whatever its age
    with db.connect() as conn:
        left = {r[0] for r in conn.execute(f"SELECT DISTINCT cycle_id FROM {METRICS}")}
        run = dict(conn.execute("SELECT * FROM cati_retention_runs").fetchone())
        summaries = [dict(r) for r in conn.execute("SELECT * FROM cati_retention_summaries ORDER BY bucket")]
    assert left == {"fresh-0", "fresh-1", "unfinished"} and count(db, METRICS) == 6
    # the archive holds exactly the removed rows and its hash is the one that was recorded
    path = archive / result["archive_path"]
    assert run["archive_path"] == result["archive_path"] == f"{METRICS}/{result['run_id']}.jsonl.gz"
    assert run["archive_sha256"] == hashlib.sha256(path.read_bytes()).hexdigest() == result["archive_sha256"]
    header, rows = R.read_archive(path)
    assert [tuple(r) for r in rows] == expected and header["columns"][0] == "rowid" and header["table"] == METRICS
    assert R.verify_archive(path, sha256=run["archive_sha256"])["rows"] == 24
    assert (run["status"], run["rows_exported"], run["rows_removed"], run["cutoff_ms"], run["table_name"]) == (
        "COMPLETED", 24, 24, NOW - 30 * DAY, METRICS)
    # the summary keeps the totals of what went: rows per day, counts per stage, sums and extremes of the measures
    assert sum(s["rows"] for s in summaries) == 24 and len(summaries) == 6
    stages, account_stage = {}, []
    for row in summaries:
        document = json.loads(row["summary"])
        for stage, n in document["by"]["stage"].items():
            stages[stage] = stages.get(stage, 0) + n
        assert document["measures"]["duration_ms"]["count"] == 4 and document["measures"]["items"]["sum"] == 50.0
        account_stage.append(document["measures"]["duration_ms"]["max"])
    assert stages == {"VERIFY_MANDATE": 6, "MARKET_STAGE": 6, "ACCOUNT_LIST": 6, "ACCOUNT_STAGE": 6}
    assert sorted(account_stage) == [300.0 + n for n in range(6)]
    assert fingerprint(db, skip=(METRICS, "cati_retention_runs", "cati_retention_summaries")) == others


def test_compaction_is_idempotent_and_never_counts_a_row_twice(db, tmp_path):
    seed_metrics(db)
    first = R.run_retention(db, table=METRICS, archive_dir=tmp_path / "a", now_ms=NOW)
    state = fingerprint(db)
    for _ in range(3):
        again = R.run_retention(db, table=METRICS, archive_dir=tmp_path / "a", now_ms=NOW)
        assert (again["status"], again["rows_removed"]) == ("NOOP", 0)
    assert fingerprint(db) == state and count(db, "cati_retention_runs") == 1
    assert first["rows_removed"] == 24 == sum(r[0] for r in _rows(db, "SELECT rows FROM cati_retention_summaries"))
    assert [r["status"] for r in R.run_all(db, archive_dir=tmp_path / "a", now_ms=NOW)] == ["NOOP", "NOOP"]


def _rows(db, sql, args=()):
    with db.connect() as conn:
        return [tuple(r) for r in conn.execute(sql, args)]


def test_raw_detail_retention_is_configurable_but_never_below_its_minimum(db, tmp_path):
    seed_metrics(db)
    assert R.retention_days_for(METRICS) == 30 and R.retention_days_for(METRICS, {METRICS: 45}) == 45
    for days in (6, 0, -1):
        with pytest.raises(R.RetentionRefused, match="BELOW_MINIMUM"):
            R.run_retention(db, table=METRICS, archive_dir=tmp_path / "a", now_ms=NOW, retention_days=days)
    assert count(db, METRICS) == 30
    kept_longer = R.run_retention(db, table=METRICS, archive_dir=tmp_path / "a", now_ms=NOW, retention_days=43)
    assert kept_longer["rows_removed"] == 8                                      # only the cycles 44 and 45 days old
    shorter = R.run_retention(db, table=METRICS, archive_dir=tmp_path / "a", now_ms=NOW, retention_days=7)
    assert shorter["rows_removed"] == 16 and count(db, METRICS) == 6             # nothing younger than a week goes


def test_a_large_backlog_is_removed_in_bounded_batches(db, tmp_path):
    seed_metrics(db)
    results = R.run_all(db, archive_dir=tmp_path / "a", now_ms=NOW, max_rows=10)
    metrics = [r for r in results if r["table"] == METRICS]
    assert [(r["rows_removed"], r["more"]) for r in metrics] == [(10, True), (10, True), (4, False)]
    assert len({r["run_id"] for r in metrics}) == 3 and count(db, "cati_retention_runs") == 3 and count(db, METRICS) == 6
    assert R.recover(db, tmp_path / "a")["verified"] == sorted(r["run_id"] for r in metrics)


# ════════════════════════════════════════════════════════════════════════ interrupted cleanup
class Crash(RuntimeError):
    pass


def crash_at(stage):
    def fault(reached):
        if reached == stage:
            raise Crash(stage)
    return fault


@pytest.mark.parametrize("stage", ["after_export", "after_summary", "after_delete"])
def test_a_crash_before_the_commit_leaves_every_row_and_the_next_run_completes(tmp_path, stage):
    interrupted, reference = full_database(sub(tmp_path, "interrupted")), full_database(sub(tmp_path, "reference"))
    for database in (interrupted, reference):
        seed_metrics(database)
    before = fingerprint(interrupted)
    with pytest.raises(Crash):
        R.run_retention(interrupted, table=METRICS, archive_dir=tmp_path / "ai", now_ms=NOW, fault=crash_at(stage))
    assert fingerprint(interrupted) == before                 # no row gone, no summary, no run: the transaction never committed
    report = R.recover(interrupted, tmp_path / "ai")
    assert report["verified"] == [] and report["damaged"] == [] and len(report["orphans"]) == 1
    assert (report["orphans"][0]["rows"], report["orphans"][0]["rows_still_in_database"]) == (24, 24)
    done = R.run_retention(interrupted, table=METRICS, archive_dir=tmp_path / "ai", now_ms=NOW)
    clean = R.run_retention(reference, table=METRICS, archive_dir=tmp_path / "ar", now_ms=NOW)
    # exactly what an uninterrupted run does: the same run, the same archive bytes, the same remaining rows and summaries
    assert (done["status"], done["run_id"], done["archive_sha256"]) == ("COMPLETED", clean["run_id"], clean["archive_sha256"])
    remaining = f"SELECT rowid, * FROM {METRICS} ORDER BY rowid"
    assert _rows(interrupted, remaining) == _rows(reference, remaining) and len(_rows(interrupted, remaining)) == 6
    assert _rows(interrupted, "SELECT run_id, rows_removed, archive_sha256 FROM cati_retention_runs") ==         _rows(reference, "SELECT run_id, rows_removed, archive_sha256 FROM cati_retention_runs")
    assert _rows(interrupted, "SELECT bucket, rows, summary FROM cati_retention_summaries ORDER BY bucket") == \
        _rows(reference, "SELECT bucket, rows, summary FROM cati_retention_summaries ORDER BY bucket")
    assert R.recover(interrupted, tmp_path / "ai") == {"verified": [done["run_id"]], "damaged": [], "orphans": [],
                                                       "partials_removed": []}


def test_a_crash_after_the_commit_leaves_a_recorded_run_and_a_verified_archive(db, tmp_path):
    seed_metrics(db)
    with pytest.raises(Crash):
        R.run_retention(db, table=METRICS, archive_dir=tmp_path / "a", now_ms=NOW, fault=crash_at("after_commit"))
    run = _rows(db, "SELECT run_id, rows_removed, archive_path, archive_sha256 FROM cati_retention_runs")
    assert len(run) == 1 and run[0][1] == 24 and count(db, METRICS) == 6
    assert R.verify_archive(tmp_path / "a" / run[0][2], sha256=run[0][3])["rows"] == 24
    assert R.run_retention(db, table=METRICS, archive_dir=tmp_path / "a", now_ms=NOW)["status"] == "NOOP"
    assert R.recover(db, tmp_path / "a")["verified"] == [run[0][0]]


def test_an_export_that_died_half_written_is_cleared_and_nothing_else_is_deleted(db, tmp_path):
    seed_metrics(db)
    done = R.run_retention(db, table=METRICS, archive_dir=tmp_path / "a", now_ms=NOW)
    folder = tmp_path / "a" / METRICS
    partial = folder / ("ret_" + "0" * 24 + ".jsonl.gz.tmp")
    partial.write_bytes(b"\x1f\x8b half a file")
    bystander, outside = folder / "notes.txt", tmp_path / "a" / "someone-elses.jsonl.gz"
    bystander.write_text("keep me")
    outside.write_text("keep me too")
    report = R.recover(db, tmp_path / "a")
    assert report["partials_removed"] == [f"{METRICS}/{partial.name}"] and not partial.exists()
    assert bystander.exists() and outside.exists() and (tmp_path / "a" / done["archive_path"]).exists()
    assert report["verified"] == [done["run_id"]]
    # a recorded archive that was damaged afterwards is reported, never silently accepted
    (tmp_path / "a" / done["archive_path"]).write_bytes(b"not the archive")
    assert R.recover(db, tmp_path / "a")["damaged"] == [{"run_id": done["run_id"], "table": METRICS,
                                                        "reason": "RETENTION_ARCHIVE_HASH_MISMATCH"}]


def test_nothing_is_removed_when_the_archive_or_the_rows_are_not_what_was_exported(db, tmp_path):
    seed_metrics(db)
    before = count(db, METRICS)

    def damage_the_archive(stage):
        if stage == "after_export":
            path = next((tmp_path / "a" / METRICS).glob("*.jsonl.gz"))
            path.write_bytes(path.read_bytes()[:-20])            # a torn write

    with pytest.raises(R.RetentionConflict, match="ARCHIVE"):
        R.run_retention(db, table=METRICS, archive_dir=tmp_path / "a", now_ms=NOW, fault=damage_the_archive)

    def change_a_row(stage):
        if stage == "after_export":
            with db.connect() as conn:
                conn.execute(f"UPDATE {METRICS} SET duration_ms = 999 WHERE cycle_id='old-0' AND stage='MARKET_STAGE'")

    with pytest.raises(R.RetentionConflict, match="ROWS_CHANGED"):
        R.run_retention(db, table=METRICS, archive_dir=tmp_path / "b", now_ms=NOW, fault=change_a_row)
    assert count(db, METRICS) == before and count(db, "cati_retention_runs") == 0 and count(db, "cati_retention_summaries") == 0
    assert R.run_retention(db, table=METRICS, archive_dir=tmp_path / "b", now_ms=NOW)["rows_removed"] == 24


# ═════════════════════════════════════════════════════════════════ open and unknown execution
def test_nothing_of_an_account_with_open_or_unknown_execution_is_removed(db, tmp_path):
    with db.connect() as conn:
        add_cycle(conn, "done")
        for account in ("settled", "closing", "reserved", "uncertain", "holding", "unread"):
            add_metric(conn, "done", "ACCOUNT", 50, account=account)
        conn.execute("INSERT INTO cati_production_closes (identity, account_id, symbol, status) VALUES ('p|h', 'closing', 'ADAUSDT', 'SUBMITTED')")
        conn.execute("INSERT INTO cati_production_protection_uncertainty (account_id, symbol, reason, first_seen_at, last_seen_at) "
                     "VALUES ('uncertain', 'ADAUSDT', 'PROTECTION_STATE_UNKNOWN', 1, 1)")
        conn.execute("INSERT INTO cati_production_state VALUES ('holding', 'u', ?, ?)",
                     (NOW, json.dumps({"positions": [{"symbol": "ADAUSDT", "positionAmt": "-3"}]})))
        conn.execute("INSERT INTO cati_production_state VALUES ('unread', 'u', ?, ?)", (NOW, json.dumps({"positions": None})))
        conn.execute("INSERT INTO cati_production_state VALUES ('settled', 'u', ?, ?)", (NOW, json.dumps({"positions": []})))
        columns = [r[1] for r in conn.execute("PRAGMA table_info(cati_portfolio_reservations)")]
        required = {r[1]: r[2] for r in conn.execute("PRAGMA table_info(cati_portfolio_reservations)") if r[3] and r[4] is None}
        values = {c: (0 if required[c].upper().startswith(("INT", "REAL")) else "x") for c in required}
        values.update(broker_account_id="reserved", status="RESOLUTION_PENDING")
        assert {"broker_account_id", "status"} <= set(columns)
        conn.execute(f"INSERT INTO cati_portfolio_reservations ({', '.join(values)}) VALUES ({', '.join('?' * len(values))})",
                     tuple(values.values()))
        assert R.accounts_with_open_execution(conn) == frozenset({"closing", "reserved", "uncertain", "holding", "unread"})
    result = R.run_retention(db, table=METRICS, archive_dir=tmp_path / "a", now_ms=NOW)
    assert result["rows_removed"] == 1
    assert sorted(r[0] for r in _rows(db, f"SELECT account_id FROM {METRICS}")) == ["closing", "holding", "reserved", "uncertain", "unread"]
    with db.connect() as conn:                       # the close is confirmed: its account's detail may now age out
        conn.execute("UPDATE cati_production_closes SET status='CLOSED' WHERE account_id='closing'")
    assert R.run_retention(db, table=METRICS, archive_dir=tmp_path / "a", now_ms=NOW)["rows_removed"] == 1
    assert "closing" not in {r[0] for r in _rows(db, f"SELECT account_id FROM {METRICS}")}


def test_an_execution_state_that_cannot_be_read_refuses_the_run(db, tmp_path):
    seed_metrics(db)
    with db.connect() as conn:
        conn.execute("INSERT INTO cati_production_state VALUES ('acct', 'u', ?, 'not json')", (NOW,))
    with pytest.raises(R.RetentionRefused, match="OPEN_EXECUTION_STATE_UNREADABLE"):
        R.run_retention(db, table=METRICS, archive_dir=tmp_path / "a", now_ms=NOW)
    assert count(db, METRICS) == 30


# ══════════════════════════════════════════════════════════════════════════ the equity history
def seed_equity(db):
    with db.connect() as conn:
        n = 0
        for account in ("acct-a", "acct-b"):
            for age in (200, 199.5, 199.2, 120, 119.9, 91, 89, 30, 1):
                for reason in ("HOURLY", "FILL") if age in (199.5, 120) else ("HOURLY",):
                    n += 1
                    conn.execute(f"INSERT INTO {SNAPSHOTS} (snapshot_id, user_id, broker_account_id, bot_instance_id, observed_at, asset, "
                                 "equity, wallet, available, unrealized, source, freshness_ms, reason, dedupe_key) "
                                 "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                                 (f"eq-{n}", "u", account, None, NOW - int(age * DAY) + n, "USDT", 1000.0 + 7 * n, 1000.0, 900.0,
                                  0.0, "ENGINE_CYCLE", 100, reason, f"k-{n}"))


def test_the_equity_history_is_rolled_up_exactly_as_its_recorder_defines_before_it_is_removed(tmp_path):
    controlled, recorder = full_database(sub(tmp_path, "controlled")), full_database(sub(tmp_path, "recorder"))
    for database in (controlled, recorder):
        seed_equity(database)
        with database.connect() as conn:             # a day that already has a daily row: the roll-up must merge into it
            conn.execute("INSERT INTO account_equity_daily VALUES ('acct-a', ?, 'USDT', 900.0, 2000.0, 800.0, 950.0, 3, ?, ?)",
                         (time.strftime("%Y-%m-%d", time.gmtime((NOW - 120 * DAY) / 1000)), NOW - 120 * DAY - 5_000, NOW - 120 * DAY - 1_000))
    result = R.run_retention(controlled, table=SNAPSHOTS, archive_dir=tmp_path / "a", now_ms=NOW)
    pruned = account_recorder.prune(recorder, NOW)
    assert pruned["deleted"] == 16 and pruned["rolled_days"] == count(recorder, "account_equity_daily") == 8
    assert (result["status"], result["rows_removed"]) == ("COMPLETED", 16)
    for table in ("account_equity_daily", SNAPSHOTS):                            # the same daily history, the same rows kept
        assert _rows(controlled, f"SELECT * FROM {table} ORDER BY 1, 2, 3") == _rows(recorder, f"SELECT * FROM {table} ORDER BY 1, 2, 3")
    # and, unlike the recorder's own prune, the removed samples are archived, hashed and summarised
    assert R.verify_archive(tmp_path / "a" / result["archive_path"], sha256=result["archive_sha256"])["rows"] == 16
    by_reason = {}
    for (summary,) in _rows(controlled, "SELECT summary FROM cati_retention_summaries"):
        for reason, n in json.loads(summary)["by"]["reason"].items():
            by_reason[reason] = by_reason.get(reason, 0) + n
    assert by_reason == {"HOURLY": 12, "FILL": 4}
    assert R.run_retention(controlled, table=SNAPSHOTS, archive_dir=tmp_path / "a", now_ms=NOW)["status"] == "NOOP"
    assert _rows(controlled, "SELECT * FROM account_equity_daily ORDER BY 1, 2, 3") == _rows(recorder, "SELECT * FROM account_equity_daily ORDER BY 1, 2, 3")


def test_a_crash_during_the_equity_roll_up_never_leaves_a_half_counted_day(db, tmp_path):
    seed_equity(db)
    before = fingerprint(db)
    with pytest.raises(Crash):
        R.run_retention(db, table=SNAPSHOTS, archive_dir=tmp_path / "a", now_ms=NOW, fault=crash_at("after_delete"))
    assert fingerprint(db) == before and count(db, "account_equity_daily") == 0
    assert R.run_retention(db, table=SNAPSHOTS, archive_dir=tmp_path / "a", now_ms=NOW)["rows_removed"] == 16
    assert sum(r[0] for r in _rows(db, "SELECT samples FROM account_equity_daily")) == 16     # each sample counted once


# ════════════════════════════════════════════════════════════════════ protected evidence survives
class Platform:
    def __init__(self, tmp_path, connected=4):
        self.db = full_database(tmp_path)
        research = _step2.new_register(tmp_path / "register.jsonl")
        self.register_path = tmp_path / "register.jsonl"
        dataset = _step2.SyntheticDataset()
        self.data = FrozenPanelBars(dataset._load("2020-01-01", "2021-06-30"), dataset.metadata)
        self.registry = FR.FamilyRegistry(self.db, research_register=research)
        self.registry.register_all(actor_ref="system:test")
        with self.db.connect() as conn:
            for n in range(connected):
                account = f"acct-{n}"
                conn.execute("INSERT INTO broker_accounts (id, user_id, broker_id, market_type, label, status, environment, "
                             "created_at, updated_at) VALUES (?,?,?,?,?,?,?,?,?)",
                             (account, f"user-{n}", "binance", "futures", account, "connected", "demo", 1, 1))
                conn.execute("INSERT INTO bot_instances (id, user_id, broker_account_id, market_type, strategy_id, "
                             "strategy_version, symbols, timeframes, allocation_type, allocation_value, mode, status, "
                             "created_at, updated_at, risk_level, capital_allocation, capital_allocation_type) "
                             "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                             (f"bot-{account}", f"user-{n}", account, "futures", "cati", "1", "[]", "[]", "risk_based", 0.5,
                              "live", "active", 1, 1, ("conservative", "balanced", "aggressive")[n % 3], 5_000.0 + 500 * n,
                              "fixed_amount"))
        self.accounts = connected + 1
        self.driver = D.PlatformCycleDriver(
            self.db, registry=self.registry, data={"1d": self.data},
            accounts=CombinedAccounts(ModelAccounts(initial_equity=EQUITY, levels=("balanced",), policies=(T.MANDATE,)),
                                      ConnectedAccounts(self.db)),
            account_runner=K.BoundedAccountRunner(db=self.db, leases=K.AccountLeases(self.db), emergency=K.EmergencyControl(self.db)))

    def run(self, days):
        for day in range(START, START + days):
            assert self.driver.run_cycle("DAILY_TREND", DT.day_text(day), now_ms=DT.close_ms(day) + 60_000).status == D.COMPLETED


def sub(tmp_path, name):
    path = tmp_path / name
    path.mkdir()
    return path


def test_a_retention_job_preserves_every_protected_evidence_class(tmp_path):
    """Real activity first -- cycles, immutable decisions, the shadow book, governance records, an emergency,
    leases, equity samples -- then every retention job, with every raw-detail row long expired."""
    p = Platform(tmp_path)
    p.run(8)
    PromotionGovernance(p.db).set_kill_switch(True, reason="drill", actor_ref="admin:7", scope="acct-1")
    control = K.EmergencyControl(p.db)
    control.request(scope=K.ACCOUNT, account_id="acct-2", requested_by="admin:7", reason="incident", request_id="r1")
    control.begin("r1")
    control.finish("r1", outcome=K.COMPLETED)
    seed_equity(p.db)
    register_before = p.register_path.read_bytes()
    guards = triggers(p.db)
    with p.db.connect() as conn:
        classes = {t: R.POLICY[t] for t in R.database_tables(conn)}
    populated = {t for t, (rows, _) in fingerprint(p.db).items() if rows}
    assert {"cati_family_decisions", "cati_shadow_fills", "cati_shadow_days", "cati_family_events", "cati_governance_controls",
            "cati_emergency_events", "cati_platform_cycles", "cati_platform_cycle_accounts", METRICS, SNAPSHOTS} <= populated
    protected_before = fingerprint(p.db, skip=tuple(t for t, c in classes.items() if not R.CLASS_RULES[c].protected))
    far_future = int(time.time() * 1000) + 3_650 * DAY
    results = R.run_all(p.db, archive_dir=tmp_path / "archive", now_ms=far_future)
    assert {r["table"]: r["rows_removed"] for r in results} == {SNAPSHOTS: 22, METRICS: 32}
    # every protected table, row for row -- except the two that record the retention itself, which only grew
    after = fingerprint(p.db, skip=tuple(t for t, c in classes.items() if not R.CLASS_RULES[c].protected))
    changed = {t for t in after if after[t] != protected_before[t]}
    assert changed == {"cati_retention_runs", "cati_retention_summaries", "account_equity_daily"}
    assert after["cati_retention_runs"][0] == 2 and protected_before["cati_retention_runs"][0] == 0
    assert protected_before["account_equity_daily"][0] == 0 and after["account_equity_daily"][0] > 0   # the roll-up, added
    for evidence_class in R.PROTECTED_CLASSES:
        assert any(classes[t] == evidence_class for t in after), evidence_class
    assert triggers(p.db) == guards                                              # no guard dropped, disabled or replaced
    assert p.register_path.read_bytes() == register_before                       # the research register is not in scope
    assert count(p.db, METRICS) == 0 and count(p.db, SNAPSHOTS) == 0
    # the per-cycle summaries, the errors and the decisions that explain those cycles are all still there
    assert count(p.db, "cati_platform_cycles") == 8 and count(p.db, "cati_platform_cycle_accounts") == 8 * p.accounts
    assert count(p.db, "cati_family_decisions") == 8 * p.accounts
    with pytest.raises(sqlite3.DatabaseError, match="append-only"):
        with p.db.connect() as conn:
            conn.execute("DELETE FROM cati_retention_summaries")


def test_freed_pages_are_returned_only_by_an_explicit_maintenance_step(db, tmp_path):
    with db.connect() as conn:
        add_cycle(conn, "big")
        conn.executemany(f"INSERT INTO {METRICS} (cycle_id, stage, account_id, duration_ms, items, recorded_at) VALUES (?,?,?,?,?,?)",
                         [("big", f"S{n}", "x" * 200, 1.0, 0, NOW - 60 * DAY) for n in range(4_000)])
    grown = R.database_size(db)
    assert R.run_retention(db, table=METRICS, archive_dir=tmp_path / "a", now_ms=NOW)["rows_removed"] == 4_000
    freed = R.database_size(db)
    assert freed["bytes"] >= grown["bytes"] and freed["reclaimable_bytes"] > 500_000      # removal alone frees pages, not disk
    with db.connect() as conn:                       # a trading runtime holds this database: maintenance is refused
        conn.execute("INSERT INTO runtime_ownership (lease_name, database_path, runtime_owner_id, pid, hostname, started_at, "
                     "heartbeat_at) VALUES ('trading_scheduler', 'x', 'own', 1, 'h', ?, ?)",
                     (time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime()),) * 2)
    with pytest.raises(R.RetentionRefused, match="RUNTIME_HOLDS_THE_DATABASE"):
        R.reclaim_space(db)
    with db.connect() as conn:
        conn.execute("UPDATE runtime_ownership SET released_at = heartbeat_at")
    returned = R.reclaim_space(db)
    assert returned["bytes_returned"] > 500_000 and R.database_size(db)["reclaimable_bytes"] == 0


# ══════════════════════════════════════════════════════════════════ repeated evaluations (forward-looking)
def evaluation(reason="AWAITING_NATURAL_CATI_DECISION", permission="WAITING_SIGNAL", decision="d1", **over):
    return {"latest_cati_decision": {"decision_id": decision}, "reason": reason, "execution_permission": permission, **over}


def test_by_default_every_evaluation_is_written_as_today(db):
    assert R.EVALUATION_REPEAT_INTERVAL_MS == 0
    account = {"id": "acct", "user_id": "u"}
    from app.trading_intelligence.integration import production_evidence

    with db.connect() as conn:
        for n in range(5):
            assert R.evaluation_is_unchanged_repeat(conn, account_id="acct", result=evaluation(), now_ms=NOW + n) is False
    for n in range(5):
        production_evidence.record(db, account, evaluation(), NOW + 30_000 * n)
    assert count(db, "cati_execution_evaluations") == 5


def test_an_enabled_interval_skips_only_unchanged_idle_repeats_and_keeps_a_heartbeat(db):
    from app.trading_intelligence.integration import production_evidence

    account, interval, written = {"id": "acct", "user_id": "u"}, 15 * 60_000, []

    def cycle(now, result):
        with db.connect() as conn:
            repeat = R.evaluation_is_unchanged_repeat(conn, account_id="acct", result=result, now_ms=now, repeat_interval_ms=interval)
        if not repeat:
            production_evidence.record(db, account, dict(result), now)
            written.append((now - NOW) // 30_000)
        return repeat

    for n in range(2_880):                           # one idle day at one evaluation every 30 seconds
        cycle(NOW + 30_000 * n, evaluation())
    assert len(written) == 96 == count(db, "cati_execution_evaluations")         # one per 15 minutes instead of 2,880
    later = NOW + 30_000 * 2_880
    assert cycle(later, evaluation(reason="USER_DAILY_LOSS_LIMIT_REACHED", permission="BLOCKED_RISK")) is False    # a veto: written
    assert cycle(later + 30_000, evaluation(reason="USER_DAILY_LOSS_LIMIT_REACHED", permission="BLOCKED_RISK")) is True
    assert cycle(later + 60_000, evaluation(decision="d2", reason="USER_DAILY_LOSS_LIMIT_REACHED", permission="BLOCKED_RISK")) is False
    always = [evaluation(stage="EVALUATION_FAILED"), evaluation(stage="DECISION_ACCEPTED"), evaluation(stage="BOUNDARY_RESULT"),
              evaluation(orders_submitted=True), evaluation(trade_plan_id="tp"), evaluation(execution_attempt_id="ea"),
              evaluation(reservation_id="rs"), evaluation(recovery=[{"status": "STILL_UNKNOWN"}]),
              evaluation(execution_history=[{"protection": {"state": "CONFIRMED"}}]), evaluation(execution_portfolio_active=True)]
    for n, result in enumerate(always):              # anything about an order, an error or open execution: always written
        production_evidence.record(db, account, dict(result), later + 90_000 + n)
        with db.connect() as conn:
            assert R.evaluation_is_unchanged_repeat(conn, account_id="acct", result=result, now_ms=later + 90_001 + n,
                                                    repeat_interval_ms=interval) is False, result
    with db.connect() as conn:                       # another account never borrows this account's last evaluation
        assert R.evaluation_is_unchanged_repeat(conn, account_id="other", result=evaluation(), now_ms=later,
                                                repeat_interval_ms=interval) is False


# ═════════════════════════════════════════════════════════════════════════════════ growth measurement
def test_sizes_are_measured_per_table_and_the_method_is_stated(db):
    seed_metrics(db)
    sizes = R.table_sizes(db)
    with db.connect() as conn:
        assert set(sizes) == set(R.database_tables(conn))
    assert sizes[METRICS]["rows"] == 30 and sizes[METRICS]["bytes"] > 0
    assert {s["method"] for s in sizes.values()} in ({"dbstat"}, {"payload_estimate"})
    whole = R.database_size(db)
    assert whole["bytes"] == whole["page_size"] * whole["page_count"] == os.path.getsize(db.path)
    before = R.table_sizes(db, [METRICS])
    with db.connect() as conn:
        add_cycle(conn, "more")
        for n in range(20):
            add_metric(conn, "more", f"S{n}", 1)
    growth = R.growth_between(before, R.table_sizes(db, [METRICS]), cycles=2, accounts=5)
    assert growth[METRICS]["rows_added"] == 20 and growth[METRICS]["rows_per_cycle"] == 10.0
    assert growth[METRICS]["rows_per_account_cycle"] == 2.0 and growth[METRICS]["prunable"] is True
    assert growth[METRICS]["retention_class"] == R.RAW_OPERATIONAL_DETAIL
    # dbstat counts allocated pages: these inserts can fit in existing pages.
    # A payload estimate grows per row; allocated storage only grows on a split.
    if growth[METRICS]["method"] == "dbstat":
        assert growth[METRICS]["bytes_added"] >= 0
    else:
        assert growth[METRICS]["bytes_added"] > 0


def test_database_growth_of_a_synthetic_run(broker, monkeypatch, tmp_path):
    """The measurement behind ``docs/implementation/evidence/step3/database_growth.json``.

    Two synthetic runs, both on temporary databases with no exchange:

    * the PLATFORM shadow cycle: the real cycle driver, registry and shadow book on the synthetic market
      (``_step2.SyntheticDataset``), through the bounded runner with leases, for 21 accounts over 30 daily cycles;
    * the PRODUCTION broker cycle: the real ``production_runtime.sync_account(execute=True)`` against the fake
      broker of the boundary-certification tests, one account with one running bot that is waiting for a
      signal (the commonest state), for 60 cycles.

    Set ``STEP3_GROWTH_OUT=<file>`` to write the evidence file. The assertions below are the facts the document
    relies on; the byte figures are recorded, not asserted.
    """
    # ---- platform shadow cycle
    accounts, cycles = 21, 30
    p = Platform(sub(tmp_path, "platform"), connected=accounts - 1)
    p.run(2)                                             # books opened, registry recorded: not part of the per-cycle rate
    before, file_before = R.table_sizes(p.db), R.database_size(p.db)
    for day in range(START + 2, START + 2 + cycles):
        assert p.driver.run_cycle("DAILY_TREND", DT.day_text(day), now_ms=DT.close_ms(day) + 60_000).status == D.COMPLETED
    platform = R.growth_between(before, R.table_sizes(p.db), cycles=cycles, accounts=accounts)
    file_after = R.database_size(p.db)
    assert platform["cati_family_decisions"]["rows_per_account_cycle"] == 1.0     # one immutable decision per account per cycle
    assert platform["cati_shadow_days"]["rows_per_account_cycle"] == 1.0
    assert platform["cati_platform_cycle_accounts"]["rows_per_account_cycle"] == 1.0
    assert platform["cati_platform_cycles"]["rows_per_cycle"] == 1.0 and platform[METRICS]["rows_per_cycle"] == 4.0
    assert platform.get("cati_account_leases", {"rows_added": 0})["rows_added"] == 0   # state: one row per account
    assert platform.get("cati_shadow_accounts", {"rows_added": 0})["rows_added"] == 0
    assert {t for t, g in platform.items() if g["prunable"]} == {METRICS}

    # ---- production broker cycle (the fixture set-up of test_runtime_cost_budget)
    h, _ = broker
    from app.activation import account_status

    monkeypatch.setattr(runtime, "resolve_broker_auth", lambda *a: SimpleNamespace(
        environment="demo", account_id=h.account["id"], user_id=h.account["user_id"], broker_type="binance",
        base_url=h.client.base_url, credential_version=1, key_fingerprint="fp"))
    monkeypatch.setattr(account_status, "refresh_if_stale", lambda *a, **kw: {"status": "SYNCED"})
    monkeypatch.setattr(runtime, "owner_current", lambda database: True)
    original = production.process_account
    monkeypatch.setattr(production, "process_account", lambda database, account, client, snapshot, **kw: original(
        database, account, client, snapshot, boundary_factory=h.boundary_for, **kw))
    monkeypatch.setattr(production, "eligibility", lambda row, now: "AWAITING_NATURAL_CATI_DECISION")
    runtime._clients.clear()
    runtime.initialize(h.db)
    raw = h.client.account.return_value
    h.client.get_balance.return_value = {"wallet": raw["totalWalletBalance"], "equity": raw["totalMarginBalance"],
                                         "available": raw["availableBalance"]}
    broker_cycles = 60
    for _ in range(2):                                   # warm-up: schema and caches
        runtime.sync_account(h.db, h.account, factory=lambda auth: h.client, execute=True)
    legacy_before, legacy_file_before = R.table_sizes(h.db), R.database_size(h.db)
    for _ in range(broker_cycles):
        out = runtime.sync_account(h.db, h.account, factory=lambda auth: h.client, execute=True)
    assert out["execution"]["evaluation_scope"] == "FULL"
    legacy = R.growth_between(legacy_before, R.table_sizes(h.db), cycles=broker_cycles, accounts=1)
    legacy_file_after = R.database_size(h.db)
    runtime._clients.clear()
    evaluations = legacy["cati_execution_evaluations"]
    assert evaluations["rows_per_account_cycle"] == 1.0                           # one evaluation row per account per cycle
    assert evaluations["retention_class"] == R.EXECUTION_AUDIT and evaluations["prunable"] is False
    assert max(legacy.values(), key=lambda g: g["bytes_added"]) is evaluations    # and it is the largest growth there is
    with h.db.connect() as conn:
        assert "cati_execution_evaluations" in R.guarded_tables(conn)             # append-only: it can only be written less
    assert not [t for t, g in legacy.items() if g["prunable"] and t != SNAPSHOTS]

    per_day = 2_880                                      # broker cycles per account per day at one every 30 seconds
    row_bytes = evaluations["bytes_per_account_cycle"]
    document = {
        "generated_by": "tests/trading_intelligence/test_step3_retention.py::test_database_growth_of_a_synthetic_run",
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "retention_policy_version": R.RETENTION_VERSION,
        "what_this_is": "Measured growth of two synthetic runs on temporary SQLite databases. No exchange, no real account.",
        "byte_method": next(iter(R.table_sizes(p.db, [METRICS]).values()))["method"],
        "byte_method_note": "dbstat: pages of the table and its indexes. payload_estimate: summed payload of the rows, "
                            "without page or index overhead (this SQLite build has no dbstat); the database_file figures "
                            "are exact (page_count x page_size) and include every index.",
        "platform_shadow_cycle": {
            "run": {"accounts": accounts, "daily_cycles": cycles, "family": "DAILY_TREND", "market": "synthetic",
                    "runner": "BoundedAccountRunner(max_concurrency=1) with account leases"},
            "database_file": {"bytes_before": file_before["bytes"], "bytes_after": file_after["bytes"],
                              "bytes_per_cycle": round((file_after["bytes"] - file_before["bytes"]) / cycles, 1),
                              "bytes_per_account_cycle": round((file_after["bytes"] - file_before["bytes"]) / (cycles * accounts), 1)},
            "tables": platform},
        "production_broker_cycle": {
            "run": {"accounts": 1, "broker_cycles": broker_cycles, "state": "one running bot, waiting for a signal, no position",
                    "broker": "the fake Binance client of the boundary-certification tests"},
            "database_file": {"bytes_before": legacy_file_before["bytes"], "bytes_after": legacy_file_after["bytes"],
                              "bytes_per_cycle": round((legacy_file_after["bytes"] - legacy_file_before["bytes"]) / broker_cycles, 1)},
            "tables": legacy},
        "projection_from_the_measured_rate": {
            "note": "Arithmetic on the measured per-cycle rate, NOT a measurement: 2,880 broker cycles per account per day. "
                    "A real account document is larger than the fake broker's, so real rows are larger than these.",
            "cati_execution_evaluations": {
                "rows_per_account_per_day": per_day, "bytes_per_account_per_day": round(row_bytes * per_day),
                "bytes_per_day_at_10_accounts": round(row_bytes * per_day * 10),
                "bytes_per_day_at_1000_accounts": round(row_bytes * per_day * 1_000),
                "rows_per_account_per_day_with_a_15_minute_repeat_interval_when_idle": 96}},
        "prunable_tables": sorted(R.PRUNE_RULES),
        "largest_growth_is_append_only": {"table": "cati_execution_evaluations", "class": R.EXECUTION_AUDIT,
                                          "guarded_by_trigger": True, "pruned_by_retention": False},
    }
    target = os.environ.get("STEP3_GROWTH_OUT")
    if target:
        Path(target).parent.mkdir(parents=True, exist_ok=True)
        Path(target).write_text(json.dumps(document, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print("\nSTEP3 DATABASE GROWTH", json.dumps({"platform": platform, "production": legacy}, indent=1, sort_keys=True)[:4000])
    assert shutil.disk_usage(tmp_path).free > 0
