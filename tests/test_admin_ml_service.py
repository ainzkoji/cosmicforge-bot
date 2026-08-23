import sqlite3
import sys
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "backends" / "shared"))
sys.path.insert(0, str(ROOT / "backends" / "user-backend"))

from shared_lib.persistence.db import DB  # noqa: E402
from app.admin_ml.service import MLAdminService, settings as ml_settings  # noqa: E402
from app.api.admin_ml import get_ml_admin_service, router as admin_ml_router  # noqa: E402
from app.core.deps import require_admin  # noqa: E402


def _create_ml_test_db(db_path: Path) -> None:
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(
            """
            CREATE TABLE runs (
                run_id TEXT PRIMARY KEY,
                started_at TEXT,
                stopped_at TEXT,
                mode TEXT,
                interval_seconds INTEGER,
                max_symbols INTEGER,
                config_json TEXT,
                status TEXT
            );

            CREATE TABLE trade_fills (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT,
                cycle_id TEXT,
                trace_id TEXT,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                action TEXT NOT NULL,
                qty REAL NOT NULL,
                price REAL NOT NULL,
                realized_pnl REAL,
                timestamp_utc TEXT NOT NULL,
                account_id TEXT,
                position_id TEXT,
                bot_instance_id TEXT,
                stop_loss_price REAL
            );

            CREATE TABLE decision_traces (
                trace_id TEXT PRIMARY KEY,
                run_id TEXT,
                cycle_id TEXT,
                symbol TEXT,
                timeframe TEXT,
                ts TEXT,
                regime_state TEXT,
                regime_confidence REAL,
                signal TEXT,
                confidence REAL,
                chosen_strategy TEXT,
                adx REAL,
                atr_pct REAL,
                ma_slope REAL,
                compression_ratio REAL,
                breakout_pressure REAL,
                buy_score REAL,
                sell_score REAL,
                threshold REAL,
                active_strategy_count INTEGER,
                htf_opposed INTEGER,
                drawdown_pct REAL,
                portfolio_risk_used REAL,
                open_positions_count INTEGER,
                margin_level REAL,
                last_price REAL,
                mark_price REAL,
                ml_model_version TEXT,
                ml_threshold REAL,
                ml_score REAL,
                ml_action TEXT,
                sl_plan REAL,
                tp_plan REAL,
                position_id TEXT
            );

            CREATE TABLE shadow_trades (
                id TEXT PRIMARY KEY,
                bot_instance_id TEXT NOT NULL,
                trace_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                side TEXT,
                regime TEXT,
                strategy TEXT,
                confidence REAL,
                threshold REAL,
                confidence_gap REAL,
                ml_score REAL,
                ml_action TEXT,
                gate_reason TEXT,
                rejection_stage TEXT NOT NULL,
                rejection_reason TEXT,
                entry_time TEXT NOT NULL,
                entry_price REAL,
                stop_loss REAL,
                take_profit REAL,
                expiry_time TEXT,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE shadow_trade_outcomes (
                id TEXT PRIMARY KEY,
                shadow_trade_id TEXT NOT NULL,
                outcome TEXT NOT NULL,
                exit_time TEXT,
                exit_price REAL,
                pnl_abs REAL,
                pnl_pct REAL,
                pnl_net REAL,
                mfe REAL,
                mae REAL,
                bars_elapsed INTEGER,
                minutes_elapsed REAL,
                evaluation_notes TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE auth_audit_log (
                id TEXT PRIMARY KEY,
                event_type TEXT NOT NULL,
                user_id TEXT,
                email TEXT,
                details TEXT,
                created_at TEXT NOT NULL
            );
            """
        )

        conn.execute(
            """
            INSERT INTO runs (run_id, started_at, mode, interval_seconds, max_symbols, status)
            VALUES ('run-1', '2026-04-22T12:00:00+00:00', 'live', 60, 20, 'running')
            """
        )

        conn.executemany(
            """
            INSERT INTO trade_fills (
                run_id, cycle_id, trace_id, symbol, side, action, qty, price, realized_pnl,
                timestamp_utc, account_id, position_id, bot_instance_id, stop_loss_price
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                ("run-1", "cycle-open-1", "trace-1", "BTCUSDT", "LONG", "OPEN", 1.0, 100.0, None, "2026-04-22T12:01:00+00:00", "default", "pos-1", "bot-1", 95.0),
                ("run-1", "cycle-close-1", None, "BTCUSDT", "LONG", "CLOSE", 1.0, 110.0, 10.0, "2026-04-22T12:10:00+00:00", "default", "pos-1", "bot-1", None),
                ("run-1", "cycle-open-2", "trace-2", "ETHUSDT", "SHORT", "OPEN", 2.0, 50.0, None, "2026-04-22T12:02:00+00:00", "default", "pos-2", "bot-1", 55.0),
                ("run-1", "cycle-close-2", None, "ETHUSDT", "SHORT", "CLOSE", 2.0, 55.0, None, "2026-04-22T12:11:00+00:00", "default", "pos-2", "bot-1", None),
                ("run-1", "cycle-open-3", "trace-3", "XRPUSDT", "LONG", "OPEN", 5.0, 2.0, None, "2026-04-22T12:03:00+00:00", "default", "pos-3", "bot-1", 1.8),
            ],
        )

        conn.executemany(
            """
            INSERT INTO decision_traces (
                trace_id, run_id, cycle_id, symbol, timeframe, ts, regime_state,
                regime_confidence, signal, confidence, chosen_strategy, adx, atr_pct,
                ma_slope, compression_ratio, breakout_pressure, buy_score, sell_score,
                threshold, active_strategy_count, htf_opposed, drawdown_pct,
                portfolio_risk_used, open_positions_count, margin_level, last_price,
                mark_price, ml_model_version, ml_threshold, ml_score, ml_action,
                sl_plan, tp_plan, position_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    "trace-1", "run-1", "cycle-open-1", "BTCUSDT", "15m", "2026-04-22T12:00:59+00:00",
                    "STRONG_TREND", 0.88, "BUY", 0.81, "robust_ensemble", 24.1, 1.5, 0.12,
                    0.8, 0.45, 0.9, 0.1, 0.7, 4, 0, 1.2, 0.35, 1, 3.0, 100.0, 100.1,
                    "entry_quality_v1.1_20260322", 0.3, 0.82, "ALLOW", 95.0, 110.0, "pos-1",
                ),
                (
                    "trace-2", "run-1", "cycle-open-2", "ETHUSDT", "15m", "2026-04-22T12:01:59+00:00",
                    "WEAK_TREND", 0.66, "SELL", 0.58, "robust_ensemble", None, 1.9, -0.08,
                    0.6, 0.31, 0.2, 0.8, 0.55, 4, 1, 1.5, 0.4, 2, 2.4, 50.0, 50.1,
                    "entry_quality_v1.1_20260322", 0.3, 0.18, "BLOCK", 55.0, 45.0, "pos-2",
                ),
                (
                    "trace-3", "run-1", "cycle-open-3", "XRPUSDT", "15m", "2026-04-22T12:02:59+00:00",
                    "RANGE", 0.71, "BUY", 0.62, "robust_ensemble", 18.0, 1.3, 0.05,
                    0.42, 0.27, 0.7, 0.2, 0.5, 3, 0, 1.1, 0.32, 1, 2.0, 2.0, 2.01,
                    "entry_quality_v1.1_20260322", 0.3, 0.24, "SHADOW", 1.8, 2.3, "pos-3",
                ),
            ],
        )

        conn.executemany(
            """
            INSERT INTO shadow_trades (
                id, bot_instance_id, trace_id, symbol, side, regime, strategy, confidence,
                threshold, confidence_gap, ml_score, ml_action, gate_reason, rejection_stage,
                rejection_reason, entry_time, entry_price, stop_loss, take_profit, expiry_time,
                status, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    "shadow-1", "bot-1", "trace-2", "ETHUSDT", "SHORT", "WEAK_TREND", "robust_ensemble",
                    0.58, 0.3, -0.12, 0.18, "BLOCK", "ml_floor", "ML_BLOCKED", "low_score",
                    "2026-04-22T12:01:59+00:00", 50.0, 55.0, 45.0, "2026-04-22T14:01:59+00:00",
                    "EVALUATED", "2026-04-22T12:01:59+00:00", "2026-04-22T14:01:59+00:00",
                ),
                (
                    "shadow-2", "bot-1", "trace-3", "XRPUSDT", "LONG", "RANGE", "robust_ensemble",
                    0.62, 0.3, -0.06, 0.24, "SHADOW", "shadow_only", "THRESHOLD_BLOCKED", "shadow_eval",
                    "2026-04-22T12:02:59+00:00", 2.0, 1.8, 2.3, "2026-04-22T14:02:59+00:00",
                    "EVALUATED", "2026-04-22T12:02:59+00:00", "2026-04-22T14:02:59+00:00",
                ),
            ],
        )

        conn.executemany(
            """
            INSERT INTO shadow_trade_outcomes (
                id, shadow_trade_id, outcome, exit_time, exit_price, pnl_abs, pnl_pct, pnl_net,
                mfe, mae, bars_elapsed, minutes_elapsed, evaluation_notes, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    "shadow-outcome-1", "shadow-1", "SL_HIT", "2026-04-22T13:00:00+00:00", 55.0,
                    -10.0, -20.0, -10.0, 0.5, -1.2, 4, 58.0, "{}", "2026-04-22T13:00:00+00:00", "2026-04-22T13:00:00+00:00",
                ),
                (
                    "shadow-outcome-2", "shadow-2", "TP_HIT", "2026-04-22T13:30:00+00:00", 2.3,
                    0.3, 15.0, 0.3, 0.6, -0.2, 6, 87.0, "{}", "2026-04-22T13:30:00+00:00", "2026-04-22T13:30:00+00:00",
                ),
            ],
        )
        conn.commit()
    finally:
        conn.close()


@pytest.fixture
def ml_service(tmp_path, monkeypatch):
    db_path = tmp_path / "ml_admin.db"
    _create_ml_test_db(db_path)
    artifact_dir = tmp_path / "artifacts"
    dataset_dir = tmp_path / "datasets"
    artifact_dir.mkdir()
    dataset_dir.mkdir()
    (artifact_dir / "entry_quality_v1.0_20260322_meta.json").write_text(
        """
        {
          "training_date": "2026-03-22T20:00:00+00:00",
          "dataset_path": "training_20260322.parquet",
          "row_count": 1000,
          "accepted": true,
          "test_frac": 0.15,
          "val_frac": 0.15,
          "n_folds": 1,
          "isotonic_calibration": true,
          "champion_reason": "older model",
          "walk_forward_metrics": {"auc": {"mean": 0.71}}
        }
        """.strip(),
        encoding="utf-8",
    )
    (artifact_dir / "entry_quality_v1.1_20260322_meta.json").write_text(
        """
        {
          "training_date": "2026-03-22T22:30:59+00:00",
          "dataset_path": "training_20260422.parquet",
          "row_count": 2,
          "accepted": true,
          "test_frac": 0.15,
          "val_frac": 0.15,
          "n_folds": 1,
          "isotonic_calibration": true,
          "champion_reason": "current model",
          "walk_forward_metrics": {"auc": {"mean": 0.93}}
        }
        """.strip(),
        encoding="utf-8",
    )
    (dataset_dir / "training_20260422_meta.json").write_text(
        """
        {
          "dataset_build_timestamp": "2026-04-22T15:29:32+00:00",
          "row_count": 2,
          "usable_row_count": 1,
          "dropped_row_count": 1,
          "drop_reasons": {"missing_critical_features": 1},
          "feature_null_counts": {"adx_normalized": 1},
          "label_null_counts": {"label_realized_pnl": 0},
          "class_balance": {
            "label": {"positive": 1, "negative": 1, "single_class": false},
            "label_win": {"positive": 1, "negative": 1, "single_class": false}
          },
          "date_range": {
            "earliest": "2026-04-22T12:01:00+00:00",
            "latest": "2026-04-22T12:11:00+00:00"
          }
        }
        """.strip(),
        encoding="utf-8",
    )
    (dataset_dir / "training_20260422.parquet").write_text("placeholder", encoding="utf-8")
    monkeypatch.setattr(ml_settings, "ML_ENABLED", True)
    monkeypatch.setattr(ml_settings, "ML_SHADOW_MODE", True)
    monkeypatch.setattr(ml_settings, "ML_MODEL_PATH", "models/entry_quality_v1.1_20260322.pkl")
    monkeypatch.setattr(ml_settings, "ML_ENCODERS_PATH", "models/entry_quality_v1.1_20260322_encoders.pkl")
    monkeypatch.setattr(ml_settings, "ML_METADATA_PATH", "models/entry_quality_v1.1_20260322_metadata.json")
    monkeypatch.setattr(ml_settings, "ML_SCORE_THRESHOLD", 0.3)
    monkeypatch.setattr(ml_settings, "ML_HARD_BLOCK_FLOOR", 0.1)
    return MLAdminService(DB(path=str(db_path)), artifact_dir=artifact_dir, dataset_dir=dataset_dir)


def test_training_gate_uses_completed_linked_trades(ml_service):
    gate = ml_service.get_training_gate()

    assert gate["total_linked_completed_trades"] == 2
    assert gate["wins"] == 1
    assert gate["losses"] == 1
    assert gate["breakeven_trades"] == 0
    assert gate["excluded_open_positions"] == 1
    assert gate["trades_with_full_feature_coverage"] == 1
    assert gate["trades_missing_critical_features"] == 1
    assert gate["current_win_rate"] == 50.0
    assert gate["feature_coverage_pct"] == 50.0
    assert gate["linkage_healthy"] is False
    assert gate["label_distribution_single_class"] is False
    assert gate["training_ready"] is False
    assert gate["status"] == "blocked"


def test_overview_and_routes_return_expected_shapes(ml_service):
    overview = ml_service.get_overview()
    summary = ml_service.get_dashboard_summary()

    assert overview["ml_enabled"] is True
    assert overview["ml_mode"] == "shadow"
    assert overview["current_model_version"] == "entry_quality_v1.1_20260322"
    assert overview["current_threshold"] == 0.3
    assert overview["current_hard_block_floor"] == 0.1
    assert overview["last_bot_restart_time"] == "2026-04-22T12:00:00+00:00"
    assert overview["current_ml_status"] == "not_ready"

    assert summary["ml_mode"] == "shadow"
    assert summary["current_model_version"] == "entry_quality_v1.1_20260322"
    assert summary["total_linked_completed_trades"] == 2

    app = FastAPI()
    app.include_router(admin_ml_router, prefix="/api")
    app.dependency_overrides[require_admin] = lambda: {"id": "admin-1", "email": "admin@example.com"}
    app.dependency_overrides[get_ml_admin_service] = lambda: ml_service
    client = TestClient(app)

    overview_response = client.get("/api/admin/ml/overview")
    gate_response = client.get("/api/admin/ml/training-gate")
    summary_response = client.get("/api/admin/ml/dashboard-summary")

    assert overview_response.status_code == 200
    assert gate_response.status_code == 200
    assert summary_response.status_code == 200

    assert overview_response.json()["ml_mode"] == "shadow"
    assert gate_response.json()["total_linked_completed_trades"] == 2
    assert summary_response.json()["status"] == "blocked"


def test_stage2_monitoring_endpoints_and_summaries(ml_service):
    feature = ml_service.get_feature_completeness(recent_limit=10)
    linkage = ml_service.get_linkage_health()
    activity = ml_service.get_activity(days=30, page=1, page_size=10)
    shadow = ml_service.get_shadow_performance(days=90)
    validation = ml_service.get_validation_history(limit=10)
    dataset_status = ml_service.get_dataset_builder_status()
    dashboard = ml_service.get_dashboard()

    assert feature["recent_window_size"] == 2
    assert feature["partially_missing_feature_count"] >= 1
    assert any(item["feature_name"] == "adx_normalized" for item in feature["features"])

    assert linkage["total_post_fix_fills"] == 5
    assert linkage["fills_with_non_null_position_id"] == 5
    assert linkage["fully_linked_completed_trades"] == 2
    assert linkage["orphan_open_fills"] == 1
    assert linkage["linkage_healthy"] is False

    assert activity["total_ml_scored_entries"] == 3
    assert activity["allow_count"] == 1
    assert activity["shadow_count"] == 1
    assert activity["block_count"] == 1
    assert len(activity["recent_activity_rows"]) == 3

    assert shadow["decision_groups"]["ALLOW"]["wins"] == 1
    assert shadow["decision_groups"]["BLOCK"]["losses"] == 1
    assert shadow["decision_groups"]["SHADOW"]["count"] == 0
    assert shadow["good_allows"] == 1
    assert shadow["good_blocks"] == 1

    assert len(validation["items"]) == 2
    assert validation["items"][0]["model_version"] == "entry_quality_v1.1_20260322"
    assert validation["items"][0]["deployed_mode"] == "shadow"
    assert validation["items"][1]["deployed_mode"] == "rolled_back"

    assert dataset_status["linked_trade_count"] == 2
    assert dataset_status["last_dataset_build_time"] == "2026-04-22T15:29:32+00:00"
    assert dataset_status["rebuild_dataset_allowed"] is True

    assert dashboard["overview"]["ml_mode"] == "shadow"
    assert dashboard["training_gate"]["total_linked_completed_trades"] == 2
    assert dashboard["dataset_builder_status"]["linked_trade_count"] == 2
    assert "alerts" in dashboard
    assert "control_panel" in dashboard
    assert "drift_monitoring" in dashboard


def test_stage2_routes_are_registered(ml_service):
    app = FastAPI()
    app.include_router(admin_ml_router, prefix="/api")
    app.dependency_overrides[require_admin] = lambda: {"id": "admin-1", "email": "admin@example.com"}
    app.dependency_overrides[get_ml_admin_service] = lambda: ml_service
    client = TestClient(app)

    paths = [
        "/api/admin/ml/feature-completeness",
        "/api/admin/ml/linkage-health",
        "/api/admin/ml/activity",
        "/api/admin/ml/shadow-performance",
        "/api/admin/ml/validation-history",
        "/api/admin/ml/dataset-builder-status",
        "/api/admin/ml/alerts",
        "/api/admin/ml/control-panel",
        "/api/admin/ml/drift-monitoring",
        "/api/admin/ml/dashboard",
    ]

    for path in paths:
        response = client.get(path)
        assert response.status_code == 200, path


def test_stage4_control_panel_alerts_drift_and_actions(ml_service):
    control_panel = ml_service.get_control_panel()
    alerts = ml_service.get_alerts()
    drift = ml_service.get_drift_monitoring(days=30)

    assert control_panel["training_allowed_right_now"] is False
    assert any(action["action_key"] == "run_training" and action["allowed"] is False for action in control_panel["actions"])
    assert any(action["action_key"] == "deploy_shadow" and action["supported"] is False for action in control_panel["actions"])
    assert any(item["code"] == "linkage_unhealthy" for item in alerts["items"])
    assert any(item["code"] == "training_blocked" for item in alerts["items"])
    assert drift["live_win_rate"] == 50.0
    assert drift["historical_win_rate"] == 50.0
    assert len(drift["live_score_distribution"]) == 10

    blocked = ml_service.trigger_action(
        action_key="run_training",
        admin={"id": "admin-1", "email": "admin@example.com", "role": "admin"},
        confirmation_phrase="RUN TRAINING",
        note="Should remain blocked",
        scheduler=lambda *_args, **_kwargs: None,
    )
    assert blocked["status"] == "blocked"

    scheduled: list[tuple] = []
    queued = ml_service.trigger_action(
        action_key="rebuild_dataset",
        admin={"id": "admin-1", "email": "admin@example.com", "role": "admin"},
        confirmation_phrase="REBUILD DATASET",
        note="Refresh linked dataset",
        scheduler=lambda fn, *args: scheduled.append((fn, args)),
    )
    assert queued["status"] == "queued"
    assert queued["action_key"] == "rebuild_dataset"
    assert scheduled and scheduled[0][1][0] == queued["id"]


def test_stage4_route_supports_protected_action_contracts(ml_service):
    app = FastAPI()
    app.include_router(admin_ml_router, prefix="/api")
    app.dependency_overrides[require_admin] = lambda: {"id": "admin-1", "email": "admin@example.com", "role": "admin"}
    app.dependency_overrides[get_ml_admin_service] = lambda: ml_service
    client = TestClient(app)

    response = client.post(
        "/api/admin/ml/actions/deploy_shadow",
        json={"confirmation_phrase": "DEPLOY TO SHADOW", "note": "Expect fail-closed stub"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "unsupported"
    assert payload["action_key"] == "deploy_shadow"
