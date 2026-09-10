"""The historical-contamination classifier: dry-run by default, provenance-only on --apply."""
from __future__ import annotations

import importlib.util
import sqlite3
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[3] / "scripts" / "classify_test_evidence_provenance.py"


@pytest.fixture(scope="module")
def classifier():
    spec = importlib.util.spec_from_file_location("classify_test_evidence_provenance", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def database(tmp_path):
    path = tmp_path / "contaminated.db"
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE bot_instances (id TEXT PRIMARY KEY);
        CREATE TABLE canonical_trade_decisions (
            decision_id TEXT PRIMARY KEY, run_id TEXT, bot_instance_id TEXT,
            decision_timestamp TEXT, confidence REAL, provenance TEXT);
        CREATE TABLE decision_traces (
            trace_id TEXT PRIMARY KEY, run_id TEXT, bot_instance_id TEXT,
            ts TEXT, confidence REAL, provenance TEXT);
        INSERT INTO bot_instances VALUES ('bot_a8117dc719fc');
        """
    )
    rows = [
        ("d1", "run_1", "bot_replay_unmodified", "2026-09-10T17:51:58", 0.1),
        ("d2", "run_1", "bot_replay_lifecycle", "2026-09-10T17:52:47", 0.2),
        ("d3", "run_2", "bot_determinism_a", "2026-09-10T17:55:53", 0.3),
        ("d4", "run_3", "bot_sensitivity", "2026-09-10T18:00:07", 0.4),
        ("d5", "run_live", "bot_a8117dc719fc", "2026-09-10T18:00:05", 0.31),
    ]
    for decision_id, run_id, bot, ts, conf in rows:
        conn.execute("INSERT INTO canonical_trade_decisions VALUES (?,?,?,?,?,NULL)",
                     (decision_id, run_id, bot, ts, conf))
        conn.execute("INSERT INTO decision_traces VALUES (?,?,?,?,?,NULL)",
                     ("t" + decision_id, run_id, bot, ts, conf))
    conn.commit()
    conn.close()
    return path


def _snapshot(path):
    conn = sqlite3.connect(path)
    try:
        return {
            table: conn.execute(f"SELECT * FROM {table} ORDER BY 1").fetchall()
            for table in ("canonical_trade_decisions", "decision_traces")
        }
    finally:
        conn.close()


def test_the_dry_run_reports_and_writes_nothing(classifier, database, capsys):
    before = _snapshot(database)
    assert classifier.main(["--db", str(database), "--json"]) == 0
    assert _snapshot(database) == before
    output = capsys.readouterr().out
    assert "DRY_RUN" in output
    assert "bot_replay_*" in output


def test_the_scan_counts_only_known_test_families(classifier, database):
    conn = sqlite3.connect(database)
    try:
        report = classifier.scan(conn)
    finally:
        conn.close()
    by_key = {(r["table"], r["family"]): r for r in report}
    assert by_key[("canonical_trade_decisions", "bot_replay_*")]["row_count"] == 2
    assert by_key[("canonical_trade_decisions", "bot_determinism_*")]["row_count"] == 1
    assert by_key[("decision_traces", "bot_sensitivity")]["run_ids"] == ["run_3"]
    assert all(r["classification_confidence"].startswith("HIGH") for r in report)
    assert not any("a8117" in r["family"] for r in report)


def test_apply_changes_provenance_only(classifier, database):
    before = _snapshot(database)
    assert classifier.main(["--db", str(database), "--apply"]) == 0
    after = _snapshot(database)

    for table in before:
        for old, new in zip(before[table], after[table]):
            assert old[:-1] == new[:-1], "a value field changed"
            if new[2] == "bot_a8117dc719fc":
                assert new[-1] is None, "the live bot's rows must not be labelled"
            else:
                assert new[-1] == "TEST_FIXTURE"


def test_apply_refuses_without_a_provenance_column(classifier, tmp_path):
    path = tmp_path / "old.db"
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE canonical_trade_decisions (decision_id TEXT, "
                 "bot_instance_id TEXT, decision_timestamp TEXT)")
    conn.execute("INSERT INTO canonical_trade_decisions VALUES ('d', 'bot_sensitivity', 't')")
    conn.commit()
    conn.close()
    with pytest.raises(SystemExit):
        classifier.main(["--db", str(path), "--apply"])
