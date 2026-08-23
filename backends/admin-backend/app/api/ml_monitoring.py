from __future__ import annotations

import logging
import time

from fastapi import APIRouter, Depends, Query

from app.core.deps import require_admin
from app.persistence.db import AdminDB, get_db
from app.persistence.repositories import ml_monitoring_repo


router = APIRouter(prefix="/api/admin/ml", tags=["admin-ml-monitoring"])
logger = logging.getLogger(__name__)


def _log_endpoint(endpoint: str, started_at: float, payload: dict) -> None:
    logger.info(
        "admin_ml_snapshot_endpoint endpoint=%s duration_ms=%s snapshot_missing=%s",
        endpoint,
        round((time.perf_counter() - started_at) * 1000, 2),
        bool(payload.get("snapshot_missing")),
    )


@router.get("/dashboard-summary")
def ml_dashboard_summary(
    _admin: dict = Depends(require_admin),
    db: AdminDB = Depends(get_db),
) -> dict:
    del _admin
    started_at = time.perf_counter()
    payload = ml_monitoring_repo.get_dashboard_summary(db)
    _log_endpoint("GET /api/admin/ml/dashboard-summary", started_at, payload)
    return payload


@router.get("/overview")
def ml_overview(
    _admin: dict = Depends(require_admin),
    db: AdminDB = Depends(get_db),
) -> dict:
    del _admin
    started_at = time.perf_counter()
    payload = ml_monitoring_repo.get_overview(db)
    _log_endpoint("GET /api/admin/ml/overview", started_at, payload)
    return payload


@router.get("/training-gate")
def ml_training_gate(
    _admin: dict = Depends(require_admin),
    db: AdminDB = Depends(get_db),
) -> dict:
    del _admin
    started_at = time.perf_counter()
    payload = ml_monitoring_repo.get_training_gate(db)
    _log_endpoint("GET /api/admin/ml/training-gate", started_at, payload)
    return payload


@router.get("/feature-completeness")
def ml_feature_completeness(
    recent_limit: int = Query(default=500, ge=50, le=5000),
    _admin: dict = Depends(require_admin),
    db: AdminDB = Depends(get_db),
) -> dict:
    del _admin
    started_at = time.perf_counter()
    payload = ml_monitoring_repo.get_feature_completeness(db, recent_limit=recent_limit)
    _log_endpoint("GET /api/admin/ml/feature-completeness", started_at, payload)
    return payload


@router.get("/linkage-health")
def ml_linkage_health(
    _admin: dict = Depends(require_admin),
    db: AdminDB = Depends(get_db),
) -> dict:
    del _admin
    started_at = time.perf_counter()
    payload = ml_monitoring_repo.get_linkage_health(db)
    _log_endpoint("GET /api/admin/ml/linkage-health", started_at, payload)
    return payload


@router.get("/activity")
def ml_activity(
    days: int = Query(default=30, ge=1, le=180),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
    _admin: dict = Depends(require_admin),
    db: AdminDB = Depends(get_db),
) -> dict:
    del _admin
    started_at = time.perf_counter()
    payload = ml_monitoring_repo.get_activity(db, days=days, page=page, page_size=page_size)
    _log_endpoint("GET /api/admin/ml/activity", started_at, payload)
    return payload


@router.get("/shadow-performance")
def ml_shadow_performance(
    days: int = Query(default=90, ge=1, le=365),
    _admin: dict = Depends(require_admin),
    db: AdminDB = Depends(get_db),
) -> dict:
    del _admin
    started_at = time.perf_counter()
    payload = ml_monitoring_repo.get_shadow_performance(db, days=days)
    _log_endpoint("GET /api/admin/ml/shadow-performance", started_at, payload)
    return payload


@router.get("/validation-history")
def ml_validation_history(
    limit: int = Query(default=10, ge=1, le=100),
    _admin: dict = Depends(require_admin),
    db: AdminDB = Depends(get_db),
) -> dict:
    del _admin
    started_at = time.perf_counter()
    payload = ml_monitoring_repo.get_validation_history(db, limit=limit)
    _log_endpoint("GET /api/admin/ml/validation-history", started_at, payload)
    return payload


@router.get("/dataset-builder-status")
def ml_dataset_builder_status(
    _admin: dict = Depends(require_admin),
    db: AdminDB = Depends(get_db),
) -> dict:
    del _admin
    started_at = time.perf_counter()
    payload = ml_monitoring_repo.get_dataset_builder_status(db)
    _log_endpoint("GET /api/admin/ml/dataset-builder-status", started_at, payload)
    return payload


@router.get("/alerts")
def ml_alerts(
    _admin: dict = Depends(require_admin),
    db: AdminDB = Depends(get_db),
) -> dict:
    del _admin
    started_at = time.perf_counter()
    payload = ml_monitoring_repo.get_alerts(db)
    _log_endpoint("GET /api/admin/ml/alerts", started_at, payload)
    return payload


@router.get("/control-panel")
def ml_control_panel(
    _admin: dict = Depends(require_admin),
    db: AdminDB = Depends(get_db),
) -> dict:
    del _admin
    started_at = time.perf_counter()
    payload = ml_monitoring_repo.get_control_panel(db)
    _log_endpoint("GET /api/admin/ml/control-panel", started_at, payload)
    return payload


@router.get("/drift-monitoring")
def ml_drift_monitoring(
    days: int = Query(default=30, ge=7, le=365),
    _admin: dict = Depends(require_admin),
    db: AdminDB = Depends(get_db),
) -> dict:
    del _admin
    started_at = time.perf_counter()
    payload = ml_monitoring_repo.get_drift_monitoring(db, days=days)
    _log_endpoint("GET /api/admin/ml/drift-monitoring", started_at, payload)
    return payload


@router.get("/dashboard")
def ml_dashboard(
    _admin: dict = Depends(require_admin),
    db: AdminDB = Depends(get_db),
) -> dict:
    del _admin
    started_at = time.perf_counter()
    payload = ml_monitoring_repo.get_dashboard(db)
    _log_endpoint("GET /api/admin/ml/dashboard", started_at, payload)
    return payload


@router.get("/runtime-status")
def ml_runtime_status(
    _admin: dict = Depends(require_admin),
    db: AdminDB = Depends(get_db),
) -> dict:
    """FIX-F: Safety-first ML runtime status endpoint.

    Returns the current ML enable/disable state, safe_state_confirmed flag,
    model load error (if any), and the Document 1 Phase I readiness checklist.

    CRITICAL: This endpoint is read-only. It must never set ML_ENABLED=True
    or ML_SHADOW_MODE=False. It only surfaces existing config/snapshot state.
    """
    del _admin
    started_at = time.perf_counter()
    payload = ml_monitoring_repo.get_runtime_status(db)
    _log_endpoint("GET /api/admin/ml/runtime-status", started_at, payload)
    return payload
