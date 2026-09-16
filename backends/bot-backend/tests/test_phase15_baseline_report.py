from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

from shared_lib.persistence.db import DB
from shared_lib.persistence.evidence_schema import ensure_evidence_schema

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "scripts"))

from build_master_ensemble_baseline import build, markdown_report


def _db(tmp_path: Path) -> DB:
    db = DB(str(tmp_path / "phase15.db"))
    ensure_evidence_schema(db)
    return db


def _decision(
    conn,
    decision_id: str,
    *,
    reason: str,
    raw: float,
    threshold: float,
    symbol: str = "BTCUSDT",
    regime: str = "WEAK_TREND",
    supporting: list[str] | None = None,
    attempt: str | None = None,
    position: str | None = None,
    evaluated_at: str = "2026-09-15T12:00:00+00:00",
) -> None:
    supporting = supporting or []
    conn.execute(
        """
        INSERT INTO trading_decisions (
            decision_id, bot_instance_id, symbol, timeframe, evaluated_at,
            provenance, active_strategies_json, supporting_strategies_json,
            opposing_strategies_json, buy_score, sell_score,
            consensus_observed, raw_confidence, effective_entry_threshold,
            execution_attempt_id, position_id, final_action, primary_reason,
            complete, finalized_at, regime, stop_price
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            decision_id,
            "bot_phase15",
            symbol,
            "15m",
            evaluated_at,
            "REPLAY",
            json.dumps(["supertrend", "sma_cross"]),
            json.dumps(supporting),
            "[]",
            raw if raw >= 0.5 else 0.0,
            raw if raw < 0.5 else 0.0,
            raw,
            raw,
            threshold,
            attempt,
            position,
            "APPROVED" if reason == "APPROVED_FOR_EXECUTION" else "REJECTED",
            reason,
            1,
            evaluated_at,
            regime,
            95.0,
        ),
    )


def test_phase15_report_contains_required_funnel_dimensions(tmp_path):
    db = _db(tmp_path)
    with db.connect() as conn:
        _decision(conn, "d1", reason="NO_OPPORTUNITY", raw=0.0, threshold=0.7)
        _decision(conn, "d2", reason="REGIME_BLOCKED", raw=0.2, threshold=0.7)
        _decision(
            conn,
            "d3",
            reason="APPROVED_FOR_EXECUTION",
            raw=0.9,
            threshold=0.7,
            supporting=["supertrend"],
            attempt="a1",
            position="p1",
        )

    report = build(db, bot_id="bot_phase15", provenance="REPLAY")

    assert report["reason_counts"]["no_trade"]["NO_OPPORTUNITY"] == 1
    assert report["reason_counts"]["block"]["REGIME_BLOCKED"] == 1
    assert report["reason_counts"]["approved"]["APPROVED_FOR_EXECUTION"] == 1
    assert report["reason_counts"]["attempt"]["APPROVED_FOR_EXECUTION"] == 1
    assert report["reason_counts"]["fill"]["APPROVED_FOR_EXECUTION"] == 1
    assert report["transition_rates"]["evaluation_to_opportunity"] == 2 / 3
    assert report["threshold_gap_buckets"]["above_threshold"] == 1
    assert report["components"]["support_count_distribution"][1] == 1
    assert "symbol" in report["confidence_distributions_by_dimension"]
    assert report["phase15_classification"]["classification"] == "E_INCONCLUSIVE"


def test_phase15_profitability_segments_are_after_costs(tmp_path):
    db = _db(tmp_path)
    with db.connect() as conn:
        _decision(
            conn,
            "d1",
            reason="APPROVED_FOR_EXECUTION",
            raw=0.9,
            threshold=0.7,
            supporting=["supertrend"],
            attempt="a1",
            position="p1",
        )
        conn.execute(
            """
            INSERT INTO positions (
                position_id, bot_instance_id, decision_id, symbol, side,
                provenance, original_qty, remaining_qty, realized_qty,
                entry_price, realized_pnl, fees, status, opened_at, closed_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                "p1",
                "bot_phase15",
                "d1",
                "BTCUSDT",
                "LONG",
                "REPLAY",
                1.0,
                0.0,
                1.0,
                100.0,
                8.0,
                1.5,
                "CLOSED",
                "2026-09-15T12:00:00+00:00",
                "2026-09-15T12:15:00+00:00",
            ),
        )

    report = build(db, bot_id="bot_phase15", provenance="REPLAY")

    profit = report["profitability"]
    assert profit["closed_trades"] == 1
    assert profit["net_pnl"] == 6.5
    assert profit["by_year"]["2026"]["expectancy"] == 6.5
    assert profit["by_component"]["supertrend"]["net_pnl"] == 6.5


def test_markdown_report_preserves_phase16_non_completion_warning(tmp_path):
    db = _db(tmp_path)
    with db.connect() as conn:
        _decision(conn, "d1", reason="NO_OPPORTUNITY", raw=0.0, threshold=0.7)

    text = markdown_report(build(db, bot_id="bot_phase15", provenance="REPLAY"))

    assert "Phase 16 still requires" in text
    assert "Historical Benchmark" in text
    assert "phase15_master_ensemble_baseline_results.json" in text


def test_cli_relative_db_path_resolves_from_repo_root(tmp_path):
    db = _db(tmp_path)
    with db.connect() as conn:
        _decision(conn, "d1", reason="NO_OPPORTUNITY", raw=0.0, threshold=0.7)
    relative = os.path.relpath(db.path, REPO_ROOT)

    env = os.environ.copy()
    env["COSMICFORGE_TEST_MODE"] = "0"
    result = subprocess.run(
        [
            sys.executable,
            str(REPO_ROOT / "scripts" / "build_master_ensemble_baseline.py"),
            "--db",
            relative,
            "--provenance",
            "REPLAY",
            "--json",
        ],
        cwd=str(REPO_ROOT),
        check=True,
        capture_output=True,
        env=env,
        text=True,
    )

    payload = json.loads(result.stdout)
    assert payload["totals"]["real_evaluations"] == 1
