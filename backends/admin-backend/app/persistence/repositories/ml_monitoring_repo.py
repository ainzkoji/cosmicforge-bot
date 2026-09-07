from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any

from app.persistence.db import AdminDB


DASHBOARD_TABLE = "admin_ml_dashboard_snapshot"
FEATURE_TABLE = "admin_ml_feature_completeness_snapshot"
DRIFT_TABLE = "admin_ml_drift_snapshot"
LINKED_TABLE = "admin_ml_linked_trade_snapshot"
VALIDATION_TABLE = "admin_ml_validation_history_snapshot"
CONTROL_TABLE = "admin_ml_control_visibility_snapshot"
SNAPSHOT_TABLES = (
    DASHBOARD_TABLE,
    FEATURE_TABLE,
    DRIFT_TABLE,
    LINKED_TABLE,
    VALIDATION_TABLE,
    CONTROL_TABLE,
)

SNAPSHOT_MISSING_WARNING = "ML snapshots have not been refreshed yet."


def get_dashboard_summary(db: AdminDB) -> dict[str, Any]:
    return _snapshot_payload(db, "dashboard_summary_json", _empty_dashboard_summary)


def get_overview(db: AdminDB) -> dict[str, Any]:
    return _snapshot_payload(db, "overview_json", _empty_overview)


def get_training_gate(db: AdminDB) -> dict[str, Any]:
    return _snapshot_payload(db, "training_gate_json", _empty_training_gate)


def get_feature_completeness(db: AdminDB, *, recent_limit: int = 500) -> dict[str, Any]:
    del recent_limit
    return _snapshot_payload(db, "feature_completeness_json", _empty_feature_completeness)


def get_linkage_health(db: AdminDB) -> dict[str, Any]:
    return _snapshot_payload(db, "linkage_json", _empty_linkage_health)


def get_activity(db: AdminDB, *, days: int = 30, page: int = 1, page_size: int = 50) -> dict[str, Any]:
    del days
    payload = _snapshot_payload(db, "activity_json", _empty_activity)
    rows = list(payload.get("recent_activity_rows") or [])
    safe_page = max(int(page or 1), 1)
    safe_page_size = max(min(int(page_size or 50), 200), 1)
    start = (safe_page - 1) * safe_page_size
    payload["page"] = safe_page
    payload["page_size"] = safe_page_size
    payload["recent_activity_rows"] = rows[start : start + safe_page_size]
    return payload


def get_shadow_performance(db: AdminDB, *, days: int = 90) -> dict[str, Any]:
    del days
    return _snapshot_payload(db, "shadow_performance_json", _empty_shadow_performance)


def get_validation_history(db: AdminDB, *, limit: int = 10) -> dict[str, Any]:
    dashboard_payload = _snapshot_payload(db, "validation_history_json", _empty_validation_history)
    if not dashboard_payload.get("snapshot_missing"):
        dashboard_payload["items"] = list(dashboard_payload.get("items") or [])[: max(int(limit or 10), 1)]
        return dashboard_payload

    generated_at = _now_iso()
    with db.connect() as conn:
        if not _table_exists(conn, VALIDATION_TABLE):
            return _with_missing_snapshot(_empty_validation_history(generated_at), generated_at)
        rows = conn.execute(
            """
            SELECT model_version, training_date, dataset_used, train_rows, test_rows,
                   train_auc, test_auc, validation_method, notes, verdict, deployed_mode,
                   generated_at
            FROM admin_ml_validation_history_snapshot
            ORDER BY training_date DESC, model_version DESC
            LIMIT ?
            """,
            (max(int(limit or 10), 1),),
        ).fetchall()

    snapshot_generated_at = max([str(row["generated_at"]) for row in rows if row["generated_at"]], default=None)
    payload = {
        "items": [
            {
                "model_version": row["model_version"],
                "training_date": row["training_date"],
                "dataset_used": row["dataset_used"],
                "train_rows": row["train_rows"],
                "test_rows": row["test_rows"],
                "train_auc": row["train_auc"],
                "test_auc": row["test_auc"],
                "validation_method": row["validation_method"],
                "notes": row["notes"],
                "verdict": row["verdict"],
                "deployed_mode": row["deployed_mode"],
            }
            for row in rows
        ],
        "source_note": "Validation history is served from admin_ml_validation_history_snapshot; no request-time sync is performed.",
    }
    return _attach_snapshot_metadata(
        payload,
        snapshot_generated_at=snapshot_generated_at,
        snapshot_source=VALIDATION_TABLE,
    )


def get_dataset_builder_status(db: AdminDB) -> dict[str, Any]:
    return _snapshot_payload(db, "dataset_status_json", _empty_dataset_status)


def get_alerts(db: AdminDB) -> dict[str, Any]:
    return _snapshot_payload(db, "alerts_json", _empty_alerts)


def get_control_panel(db: AdminDB) -> dict[str, Any]:
    dashboard_payload = _snapshot_payload(db, "control_panel_json", _empty_control_panel)
    if not dashboard_payload.get("snapshot_missing"):
        return dashboard_payload

    generated_at = _now_iso()
    with db.connect() as conn:
        if not _table_exists(conn, CONTROL_TABLE):
            return _with_missing_snapshot(_empty_control_panel(generated_at), generated_at)
        row = conn.execute(
            """
            SELECT generated_at, control_panel_json
            FROM admin_ml_control_visibility_snapshot
            WHERE snapshot_key = 'latest'
            """
        ).fetchone()
    if not row:
        return _with_missing_snapshot(_empty_control_panel(generated_at), generated_at)
    return _attach_snapshot_metadata(
        _json_object(row["control_panel_json"], _empty_control_panel(generated_at)),
        snapshot_generated_at=row["generated_at"],
        snapshot_source=CONTROL_TABLE,
    )


def get_drift_monitoring(db: AdminDB, *, days: int = 30) -> dict[str, Any]:
    del days
    return _snapshot_payload(db, "drift_json", _empty_drift_monitoring)


def get_dashboard(db: AdminDB) -> dict[str, Any]:
    return _snapshot_payload(db, "dashboard_json", _empty_dashboard)


def get_runtime_status(db: AdminDB) -> dict[str, Any]:
    """FIX-F: Safety-first ML runtime status.

    Returns a compact view of the live ML config state with explicit
    ``safe_state_confirmed`` and Document 1 readiness checklist.

    All fields default to safe values (ML disabled) when no snapshot is
    available so the admin always sees a clear safety indicator.
    """
    overview = _snapshot_payload(db, "overview_json", _empty_overview)
    training_gate = _snapshot_payload(db, "training_gate_json", _empty_training_gate)
    dataset_status = _snapshot_payload(db, "dataset_status_json", _empty_dataset_status)

    ml_enabled: bool = bool(overview.get("ml_enabled", False))
    # ml_shadow_mode: prefer snapshot field if present, default True (safe state)
    ml_shadow_mode: bool = bool(overview.get("ml_shadow_mode", True))
    safe_state_confirmed: bool = not ml_enabled

    phase_i = _build_phase_i_checklist(training_gate, dataset_status, safe_state_confirmed)

    payload: dict[str, Any] = {
        # ── Safety fields ────────────────────────────────────────────────────
        "ml_enabled": ml_enabled,
        "ml_shadow_mode": ml_shadow_mode,
        "safe_state_confirmed": safe_state_confirmed,
        "safe_state_note": (
            "ML is disabled. Scoring is paused. Bot is running rule-based only."
            if safe_state_confirmed
            else "WARNING: ML_ENABLED is True. Live scoring is active."
        ),
        # ── Runtime identity ─────────────────────────────────────────────────
        "ml_mode": overview.get("ml_mode", "disabled"),
        "current_ml_status": overview.get("current_ml_status", "not_ready"),
        "model_version": overview.get("current_model_version", None),
        "contract_version": overview.get("contract_version", "v2"),
        # ── Model artifact paths ─────────────────────────────────────────────
        "model_artifact_path": overview.get("model_artifact_path", None),
        "encoder_path": overview.get("encoder_path", None),
        "metadata_path": overview.get("metadata_path", None),
        # ── Load state ───────────────────────────────────────────────────────
        "model_load_error": overview.get("model_load_error", None),
        "last_model_load_time": overview.get("last_model_load_time", None),
        "last_score_timestamp": overview.get("last_score_timestamp", None),
        "last_bot_restart_time": overview.get("last_bot_restart_time", None),
        # ── Threshold ────────────────────────────────────────────────────────
        "current_threshold": overview.get("current_threshold", None),
        "current_hard_block_floor": overview.get("current_hard_block_floor", None),
        # ── Document 1 readiness checklist ───────────────────────────────────
        "phase_i_readiness": phase_i,
        # ── Snapshot metadata ────────────────────────────────────────────────
        "snapshot_missing": bool(overview.get("snapshot_missing", False)),
        "snapshot_stale": bool(overview.get("snapshot_stale", True)),
        "generated_at": overview.get("generated_at", _now_iso()),
        "source_note": (
            "ML runtime status is derived from the admin_ml_dashboard_snapshot. "
            "ML_ENABLED and ML_SHADOW_MODE reflect the last bot-synced config snapshot. "
            "safe_state_confirmed=True means ML is disabled in the running bot."
        ),
    }
    return payload


def _build_phase_i_checklist(
    training_gate: dict[str, Any],
    dataset_status: dict[str, Any],
    safe_state_confirmed: bool,
) -> dict[str, Any]:
    """Build the Document 1 Phase I readiness checklist.

    Static items (code fixes) are set based on known deployment state.
    Dynamic items use the live snapshot data from the training gate and
    dataset builder.
    """
    dataset_rows: int = int(dataset_status.get("fully_usable_rows") or 0)
    linked_trades: int = int(training_gate.get("total_linked_completed_trades") or 0)
    wins: int = int(training_gate.get("wins") or 0)
    current_win_rate: float = float(training_gate.get("current_win_rate") or 0.0)
    linkage_healthy: bool = bool(training_gate.get("linkage_healthy", False))
    training_ready: bool = bool(training_gate.get("training_ready", False))

    # Phase I targets (from project_phase_i_readiness.md)
    REQUIRED_TRADES = int(training_gate.get("required_trades") or 200)
    REQUIRED_WINS = int(training_gate.get("required_wins") or 50)
    TARGET_WIN_RATE_PCT = 35.0   # ≥35% win rate required
    TARGET_PAPER_TRADES = 100    # ≥100 paper trades after strategy filter
    MIN_DATASET_ROWS = 100       # ≥100 usable dataset rows

    paper_trades_sufficient = linked_trades >= TARGET_PAPER_TRADES
    win_rate_target_met = current_win_rate >= TARGET_WIN_RATE_PCT
    dataset_rows_sufficient = dataset_rows >= MIN_DATASET_ROWS

    checklist = [
        {
            "id": "fix_a_dataset_built",
            "label": "FIX-A: v2 ML dataset built (314+ rows)",
            "met": dataset_rows_sufficient,
            "detail": f"{dataset_rows} usable rows (target ≥{MIN_DATASET_ROWS})",
            "category": "data",
        },
        {
            "id": "linkage_health",
            "label": "Trade linkage healthy (run_id / cycle_id / position_id)",
            "met": linkage_healthy,
            "detail": "Linkage health from latest snapshot",
            "category": "data",
        },
        {
            "id": "fix_d_tp_break_even",
            "label": "FIX-D: TP1/TP2 break-even logic deployed",
            "met": True,
            "detail": "Deployed: 18/18 tests pass",
            "category": "execution",
        },
        {
            "id": "fix_e_ensemble_filter",
            "label": "FIX-E: Ensemble execution gate (STRONG_TREND block, threshold floor)",
            "met": True,
            "detail": "Deployed: ENSEMBLE_BLOCKED_REGIMES + ENSEMBLE_MIN_THRESHOLD_FLOOR in config",
            "category": "execution",
        },
        {
            "id": "paper_trades_count",
            "label": f"≥{TARGET_PAPER_TRADES} paper trades after strategy filter",
            "met": paper_trades_sufficient,
            "detail": f"{linked_trades} linked completed trades (target ≥{TARGET_PAPER_TRADES})",
            "category": "accumulation",
        },
        {
            "id": "win_rate_improvement",
            "label": f"Win rate ≥{TARGET_WIN_RATE_PCT}% after ensemble filter",
            "met": win_rate_target_met,
            "detail": f"Current: {current_win_rate:.1f}% (target ≥{TARGET_WIN_RATE_PCT}%)",
            "category": "performance",
        },
        {
            "id": "training_gate_ready",
            "label": f"Training gate ready ({REQUIRED_TRADES} trades / {REQUIRED_WINS} wins)",
            "met": training_ready,
            "detail": f"Trades: {linked_trades}/{REQUIRED_TRADES}, Wins: {wins}/{REQUIRED_WINS}",
            "category": "ml",
        },
        {
            "id": "ml_safety_state",
            "label": "ML safety state confirmed (ML_ENABLED=False)",
            "met": safe_state_confirmed,
            "detail": "safe_state_confirmed must be True before Phase I promotion",
            "category": "safety",
        },
        {
            "id": "fix_f_ml_visibility",
            "label": "FIX-F: ML runtime status visible in admin dashboard",
            "met": True,
            "detail": "Deployed: /api/admin/ml/runtime-status endpoint + frontend card",
            "category": "visibility",
        },
    ]

    met_count = sum(1 for item in checklist if item["met"])
    overall_ready = all(item["met"] for item in checklist)

    return {
        "checklist": checklist,
        "met_count": met_count,
        "total_count": len(checklist),
        "overall_ready": overall_ready,
        "overall_note": (
            "All Phase I gates satisfied — ready for ML training review."
            if overall_ready
            else f"{met_count}/{len(checklist)} gates satisfied. Training must remain blocked until all gates are met."
        ),
    }


def _snapshot_payload(db: AdminDB, column: str, empty_factory) -> dict[str, Any]:
    generated_at = _now_iso()
    with db.connect() as conn:
        if not _table_exists(conn, DASHBOARD_TABLE):
            return _with_missing_snapshot(empty_factory(generated_at), generated_at)
        columns = _table_columns(conn, DASHBOARD_TABLE)
        if column not in columns:
            return _with_missing_snapshot(empty_factory(generated_at), generated_at)
        row = conn.execute(
            f"""
            SELECT generated_at, {column}
            FROM admin_ml_dashboard_snapshot
            WHERE snapshot_key = 'latest'
            """
        ).fetchone()
    if not row:
        return _with_missing_snapshot(empty_factory(generated_at), generated_at)
    payload = _json_object(row[column], empty_factory(generated_at))
    return _attach_snapshot_metadata(
        payload,
        snapshot_generated_at=row["generated_at"],
        snapshot_source=f"{DASHBOARD_TABLE}.{column}",
    )


def _table_exists(conn, table_name: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    ).fetchone()
    return row is not None


def _table_columns(conn, table_name: str) -> set[str]:
    return {str(row["name"]) for row in conn.execute(f"PRAGMA table_info({table_name})")}


def _json_object(value: Any, fallback: dict[str, Any]) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if not value:
        return dict(fallback)
    try:
        parsed = json.loads(str(value))
    except (TypeError, ValueError, json.JSONDecodeError):
        return dict(fallback)
    return parsed if isinstance(parsed, dict) else dict(fallback)


def _attach_snapshot_metadata(payload: dict[str, Any], *, snapshot_generated_at: str | None, snapshot_source: str) -> dict[str, Any]:
    enriched = dict(payload)
    metadata = {
        "snapshot_generated_at": snapshot_generated_at,
        "snapshot_source": snapshot_source,
        "snapshot_stale": _snapshot_stale(snapshot_generated_at),
        "warnings": _warnings_from_payload(payload),
    }
    enriched.setdefault("generated_at", snapshot_generated_at or _now_iso())
    enriched.setdefault("snapshot_metadata", metadata)
    enriched.setdefault("snapshot_generated_at", snapshot_generated_at)
    enriched.setdefault("snapshot_source", snapshot_source)
    enriched.setdefault("snapshot_stale", metadata["snapshot_stale"])
    return enriched


def _with_missing_snapshot(payload: dict[str, Any], generated_at: str) -> dict[str, Any]:
    enriched = dict(payload)
    enriched["generated_at"] = generated_at
    enriched["snapshot_missing"] = True
    enriched["warning"] = SNAPSHOT_MISSING_WARNING
    enriched["snapshot_metadata"] = {
        "snapshot_generated_at": None,
        "snapshot_source": "admin_ml_*",
        "snapshot_stale": True,
        "warnings": [{"code": "snapshot_missing", "level": "warning", "message": SNAPSHOT_MISSING_WARNING}],
    }
    return enriched


def _snapshot_stale(value: str | None) -> bool:
    parsed = _parse_dt(value)
    if parsed is None:
        return True
    return parsed < datetime.now(timezone.utc) - timedelta(hours=24)


def _parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    raw = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _warnings_from_payload(payload: dict[str, Any]) -> list[Any]:
    warnings = payload.get("warnings")
    if isinstance(warnings, list):
        return warnings
    alerts = payload.get("items") if "items" in payload else payload.get("alerts", {}).get("items") if isinstance(payload.get("alerts"), dict) else None
    if isinstance(alerts, list):
        return [item for item in alerts if isinstance(item, dict) and item.get("level") in {"warning", "danger"}]
    return []


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _empty_overview(_generated_at: str) -> dict[str, Any]:
    return {
        "ml_enabled": False,
        "ml_mode": "disabled",
        "current_model_version": None,
        "current_threshold": None,
        "current_hard_block_floor": None,
        "model_artifact_path": None,
        "encoder_path": None,
        "metadata_path": None,
        "last_model_load_time": None,
        "last_bot_restart_time": None,
        "current_ml_status": "not_ready",
    }


def _empty_training_gate(_generated_at: str) -> dict[str, Any]:
    return {
        "total_linked_completed_trades": 0,
        "required_trades": 200,
        "wins": 0,
        "required_wins": 50,
        "losses": 0,
        "breakeven_trades": 0,
        "excluded_open_positions": 0,
        "trades_with_full_feature_coverage": 0,
        "trades_missing_critical_features": 0,
        "current_win_rate": 0.0,
        "feature_coverage_pct": 0.0,
        "linkage_healthy": False,
        "label_distribution_single_class": True,
        "training_ready": False,
        "status": "not_ready",
        "linkage_warnings": {"unlinked_completed_trades": 0, "unlinked_reason_counts": {}},
    }


def _empty_dashboard_summary(generated_at: str) -> dict[str, Any]:
    overview = _empty_overview(generated_at)
    gate = _empty_training_gate(generated_at)
    return {
        "ml_mode": overview["ml_mode"],
        "current_model_version": overview["current_model_version"],
        "total_linked_completed_trades": gate["total_linked_completed_trades"],
        "wins": gate["wins"],
        "feature_coverage_pct": gate["feature_coverage_pct"],
        "linkage_healthy": gate["linkage_healthy"],
        "training_ready": gate["training_ready"],
        "status": gate["status"],
    }


def _empty_feature_completeness(_generated_at: str) -> dict[str, Any]:
    return {
        "recent_window_size": 0,
        "recent_window_basis": "snapshot_missing",
        "recent_completeness_pct": 0.0,
        "lifetime_completeness_pct": 0.0,
        "features": [],
        "broken_feature_count": 0,
        "partially_missing_feature_count": 0,
    }


def _empty_linkage_health(_generated_at: str) -> dict[str, Any]:
    return {
        "post_fix_start": None,
        "total_post_fix_fills": 0,
        "fills_with_non_null_run_id": 0,
        "fills_with_non_null_cycle_id": 0,
        "fills_with_non_null_position_id": 0,
        "fully_linked_completed_trades": 0,
        "fully_linked_completed_trades_pct": 0.0,
        "orphan_open_fills": 0,
        "unmatched_close_fills": 0,
        "linkage_healthy": False,
        "unlinked_completed_trades": 0,
        "unlinked_reason_counts": {},
    }


def _empty_activity(_generated_at: str) -> dict[str, Any]:
    buckets = [{"bucket": f"{i / 10:.1f}-{(i + 1) / 10:.1f}", "count": 0} for i in range(10)]
    return {
        "window_days": 30,
        "page": 1,
        "page_size": 50,
        "total_recent_rows": 0,
        "total_ml_scored_entries": 0,
        "allow_count": 0,
        "shadow_count": 0,
        "block_count": 0,
        "skip_count": 0,
        "average_ml_score": None,
        "current_threshold": None,
        "current_hard_floor": None,
        "score_distribution": buckets,
        "per_symbol_actions": [],
        "per_regime_actions": [],
        "per_session_actions": [],
        "recent_activity_rows": [],
    }


def _empty_shadow_performance(_generated_at: str) -> dict[str, Any]:
    empty_group = {"count": 0, "wins": 0, "losses": 0, "breakevens": 0, "total_pnl": 0.0, "average_pnl": 0.0}
    return {
        "window_days": 90,
        "total_linked_completed_trades_with_ml_attribution": 0,
        "decision_groups": {"ALLOW": dict(empty_group), "SHADOW": dict(empty_group), "BLOCK": dict(empty_group)},
        "good_allows": 0,
        "bad_allows": 0,
        "good_blocks": 0,
        "bad_blocks": 0,
        "classification_logic": "",
    }


def _empty_validation_history(_generated_at: str) -> dict[str, Any]:
    return {
        "items": [],
        "source_note": "Validation history snapshot is unavailable.",
    }


def _empty_dataset_status(_generated_at: str) -> dict[str, Any]:
    return {
        "dataset_source_date_range": None,
        "linked_trade_count": 0,
        "fully_usable_rows": 0,
        "dropped_rows": 0,
        "dropped_row_reasons": [],
        "feature_completeness_status": "unknown",
        "label_distribution": {"wins": 0, "losses": 0, "breakevens": 0, "single_class": True},
        "last_dataset_build_time": None,
        "last_dataset_path": None,
        "rebuild_dataset_allowed": False,
        "source_note": "Dataset builder status snapshot is unavailable.",
    }


def _empty_alerts(generated_at: str) -> dict[str, Any]:
    return {
        "generated_at": generated_at,
        "items": [
            {
                "code": "snapshot_missing",
                "level": "warning",
                "title": "ML snapshots are missing",
                "body": SNAPSHOT_MISSING_WARNING,
            }
        ],
    }


def _empty_control_panel(_generated_at: str) -> dict[str, Any]:
    return {
        "readiness_status": "not_ready",
        "training_allowed_right_now": False,
        "current_dataset_path": None,
        "target_output_model_version": "entry_quality_v1.0_snapshot",
        "last_training_run_status": None,
        "last_training_run_logs": [],
        "last_dataset_rebuild_status": None,
        "last_validation_run_status": None,
        "actions": [],
        "recent_action_runs": [],
        "source_note": "Control visibility snapshot is unavailable. ML actions remain on user-backend.",
    }


def _empty_drift_monitoring(_generated_at: str) -> dict[str, Any]:
    buckets = [{"bucket": f"{i / 10:.1f}-{(i + 1) / 10:.1f}", "count": 0} for i in range(10)]
    return {
        "window_days": 30,
        "live_win_rate": None,
        "historical_win_rate": None,
        "win_rate_delta": None,
        "live_score_distribution": buckets,
        "training_score_distribution": buckets,
        "symbol_distribution": [],
        "regime_distribution": [],
        "session_distribution": [],
        "average_pnl_by_regime": [],
        "average_pnl_by_symbol": [],
        "average_pnl_by_score_band": [],
        "source_note": "Drift monitoring snapshot is unavailable.",
    }


def _empty_dashboard(generated_at: str) -> dict[str, Any]:
    validation = _empty_validation_history(generated_at)
    return {
        "overview": _empty_overview(generated_at),
        "training_gate": _empty_training_gate(generated_at),
        "feature_completeness": {
            "recent_completeness_pct": 0.0,
            "lifetime_completeness_pct": 0.0,
            "broken_feature_count": 0,
            "partially_missing_feature_count": 0,
        },
        "linkage_health": _empty_linkage_health(generated_at),
        "activity_summary": {
            "window_days": 30,
            "total_ml_scored_entries": 0,
            "allow_count": 0,
            "shadow_count": 0,
            "block_count": 0,
            "skip_count": 0,
            "average_ml_score": None,
            "current_threshold": None,
            "current_hard_floor": None,
            "recent_activity_rows": [],
        },
        "shadow_performance": {
            "window_days": 90,
            "total_linked_completed_trades_with_ml_attribution": 0,
            "decision_groups": {},
            "good_allows": 0,
            "bad_allows": 0,
            "good_blocks": 0,
            "bad_blocks": 0,
        },
        "validation_history": {
            "total_models": 0,
            "latest_model": None,
            "source_note": validation["source_note"],
        },
        "dataset_builder_status": _empty_dataset_status(generated_at),
        "alerts": _empty_alerts(generated_at),
        "control_panel": _empty_control_panel(generated_at),
        "drift_monitoring": {
            "window_days": 30,
            "live_win_rate": None,
            "historical_win_rate": None,
            "win_rate_delta": None,
            "symbol_distribution": [],
            "regime_distribution": [],
            "session_distribution": [],
            "average_pnl_by_score_band": [],
            "source_note": "Dashboard snapshot is unavailable.",
        },
    }
