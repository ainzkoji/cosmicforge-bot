from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from scripts.validation.analyze_signal_thresholds import run_audit


def _create_fixture_db(path: Path) -> None:
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE decision_traces (
            trace_id TEXT, symbol TEXT, ts TEXT, signal TEXT, confidence REAL,
            intended_action TEXT, gate_reason TEXT, reason_codes TEXT,
            regime_state TEXT, buy_score REAL, sell_score REAL, threshold REAL,
            adx REAL, atr_pct REAL, ma_slope REAL, compression_ratio REAL,
            breakout_pressure REAL
        );
        CREATE TABLE decision_logs (
            run_id TEXT, strategy_signal_json TEXT, final_action TEXT
        );
        """
    )
    rows = [
        ("t1", "BTCUSDT", "2026-06-14T12:00:00+00:00", "hold", 0.52, "HOLD", "master_ensemble_v2", "master_ensemble_v2", "WEAK_TREND", 0.52, 0.0),
        ("t2", "ETHUSDT", "2026-06-14T14:00:00+00:00", "hold", 0.47, "HOLD", "master_ensemble_v2", "master_ensemble_v2", "WEAK_TREND", 0.0, 0.47),
        ("t3", "BTCUSDT", "2026-06-14T20:00:00+00:00", "hold", 0.65, "HOLD", "SESSION_BLOCKED", "SESSION_BLOCKED", "WEAK_TREND", 0.65, 0.0),
        ("t4", "ETHUSDT", "2026-06-14T15:00:00+00:00", "hold", 0.0, "HOLD", "REGIME_BLOCKED_STRONG_TREND", "REGIME_BLOCKED_STRONG_TREND", "STRONG_TREND", 0.0, 0.0),
        ("t5", "BTCUSDT", "2026-06-14T18:00:00+00:00", "hold", 0.0, "HOLD", "master_ensemble_v2", "master_ensemble_v2", "WEAK_TREND", 0.0, 0.0),
    ]
    for row in rows:
        conn.execute(
            "INSERT INTO decision_traces VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (*row, 0.55, 30.0, 0.5, 0.1, 1.0, 0.0),
        )
        component_meta = {
            "regime": row[8],
            "votes": [
                "supertrend:HOLD(0.00)",
                "trend_pullback:HOLD(0.00)",
            ],
        }
        conn.execute(
            "INSERT INTO decision_logs VALUES (?,?,?)",
            (
                row[0],
                json.dumps(
                    {
                        "signal": row[3],
                        "confidence": row[4],
                        "reason": row[6],
                        "meta": component_meta,
                    }
                ),
                "hold",
            ),
        )
    conn.commit()
    conn.close()


def _run(tmp_path: Path):
    db_path = tmp_path / "audit.db"
    _create_fixture_db(db_path)
    return run_audit(
        symbols=["BTCUSDT", "ETHUSDT"],
        lookback_decisions=500,
        thresholds=[0.55, 0.50, 0.45],
        db_path=db_path,
        output_md=tmp_path / "signal_starvation_audit.md",
        output_json=tmp_path / "signal_starvation_audit.json",
        recommendation_md=tmp_path / "signal_tuning_recommendation.md",
    )


def test_decision_breakdown_and_impact_reports_are_generated(tmp_path):
    payload = _run(tmp_path)

    assert (tmp_path / "signal_starvation_audit.md").exists()
    assert (tmp_path / "signal_starvation_audit.json").exists()
    assert (tmp_path / "signal_tuning_recommendation.md").exists()
    assert payload["decision_summary"]["structured_hold_breakdowns"]
    assert payload["regime_impact"]["strong_trend_block_impact_in_sample"] == 1
    assert "runtime_missing_valid_signals" in payload["session_impact"]


def test_threshold_analysis_reports_additional_possible_signals(tmp_path):
    payload = _run(tmp_path)

    assert payload["threshold_sensitivity"]["0.55"]["total_possible_signals"] == 0
    assert payload["threshold_sensitivity"]["0.50"]["additional_vs_current_floor"] == 1
    assert payload["threshold_sensitivity"]["0.45"]["additional_vs_current_floor"] == 2
    assert payload["threshold_sensitivity"]["0.45"]["would_be_blocked_by_iofs_shadow"] == 0


def test_threshold_analysis_does_not_modify_active_env(tmp_path):
    root = Path(__file__).resolve().parents[1]
    env_path = root / ".env"
    before = env_path.read_bytes()

    payload = _run(tmp_path)

    assert env_path.read_bytes() == before
    assert payload["safety"]["active_env_unchanged"] is True
    assert payload["safety"]["active_env_modified"] is False


def test_recommendation_never_enables_ml_or_live_mode(tmp_path):
    payload = _run(tmp_path)
    recommendation = payload["recommendation"]

    assert recommendation["paper_only"] is True
    assert recommendation["ml_enabled"] is False
    assert recommendation["live_enabled"] is False
    assert payload["safety"]["execution_mode_is_paper"] is True
    assert payload["safety"]["ml_disabled"] is True
