from __future__ import annotations

from functools import lru_cache

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query

from app.admin_ml.service import MLAdminService
from app.core.deps import require_admin
from app.schemas.admin_ml import (
    MLActivityResponse,
    MLActionRequest,
    MLActionRun,
    MLAlertsResponse,
    MLControlPanelResponse,
    MLDashboardSummaryResponse,
    MLDashboardResponse,
    MLDatasetBuilderStatusResponse,
    MLDriftMonitoringResponse,
    MLFeatureCompletenessResponse,
    MLLinkageHealthResponse,
    MLOverviewResponse,
    MLShadowPerformanceResponse,
    MLTrainingGateResponse,
    MLValidationHistoryResponse,
)


router = APIRouter(prefix="/admin/ml", tags=["admin-ml"])


@lru_cache(maxsize=1)
def get_ml_admin_service() -> MLAdminService:
    return MLAdminService()


@router.get("/overview", response_model=MLOverviewResponse)
async def get_ml_overview(
    _admin: dict = Depends(require_admin),
    service: MLAdminService = Depends(get_ml_admin_service),
) -> MLOverviewResponse:
    return MLOverviewResponse(**service.get_overview())


@router.get("/training-gate", response_model=MLTrainingGateResponse)
async def get_ml_training_gate(
    _admin: dict = Depends(require_admin),
    service: MLAdminService = Depends(get_ml_admin_service),
) -> MLTrainingGateResponse:
    return MLTrainingGateResponse(**service.get_training_gate())


@router.get("/dashboard-summary", response_model=MLDashboardSummaryResponse)
async def get_ml_dashboard_summary(
    _admin: dict = Depends(require_admin),
    service: MLAdminService = Depends(get_ml_admin_service),
) -> MLDashboardSummaryResponse:
    return MLDashboardSummaryResponse(**service.get_dashboard_summary())


@router.get("/feature-completeness", response_model=MLFeatureCompletenessResponse)
async def get_ml_feature_completeness(
    recent_limit: int = Query(default=500, ge=50, le=5000),
    _admin: dict = Depends(require_admin),
    service: MLAdminService = Depends(get_ml_admin_service),
) -> MLFeatureCompletenessResponse:
    return MLFeatureCompletenessResponse(**service.get_feature_completeness(recent_limit=recent_limit))


@router.get("/linkage-health", response_model=MLLinkageHealthResponse)
async def get_ml_linkage_health(
    _admin: dict = Depends(require_admin),
    service: MLAdminService = Depends(get_ml_admin_service),
) -> MLLinkageHealthResponse:
    return MLLinkageHealthResponse(**service.get_linkage_health())


@router.get("/activity", response_model=MLActivityResponse)
async def get_ml_activity(
    days: int = Query(default=30, ge=1, le=180),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
    _admin: dict = Depends(require_admin),
    service: MLAdminService = Depends(get_ml_admin_service),
) -> MLActivityResponse:
    return MLActivityResponse(**service.get_activity(days=days, page=page, page_size=page_size))


@router.get("/shadow-performance", response_model=MLShadowPerformanceResponse)
async def get_ml_shadow_performance(
    days: int = Query(default=90, ge=1, le=365),
    _admin: dict = Depends(require_admin),
    service: MLAdminService = Depends(get_ml_admin_service),
) -> MLShadowPerformanceResponse:
    return MLShadowPerformanceResponse(**service.get_shadow_performance(days=days))


@router.get("/validation-history", response_model=MLValidationHistoryResponse)
async def get_ml_validation_history(
    limit: int = Query(default=10, ge=1, le=100),
    _admin: dict = Depends(require_admin),
    service: MLAdminService = Depends(get_ml_admin_service),
) -> MLValidationHistoryResponse:
    return MLValidationHistoryResponse(**service.get_validation_history(limit=limit))


@router.get("/dataset-builder-status", response_model=MLDatasetBuilderStatusResponse)
async def get_ml_dataset_builder_status(
    _admin: dict = Depends(require_admin),
    service: MLAdminService = Depends(get_ml_admin_service),
) -> MLDatasetBuilderStatusResponse:
    return MLDatasetBuilderStatusResponse(**service.get_dataset_builder_status())


@router.get("/alerts", response_model=MLAlertsResponse)
async def get_ml_alerts(
    _admin: dict = Depends(require_admin),
    service: MLAdminService = Depends(get_ml_admin_service),
) -> MLAlertsResponse:
    return MLAlertsResponse(**service.get_alerts())


@router.get("/control-panel", response_model=MLControlPanelResponse)
async def get_ml_control_panel(
    _admin: dict = Depends(require_admin),
    service: MLAdminService = Depends(get_ml_admin_service),
) -> MLControlPanelResponse:
    return MLControlPanelResponse(**service.get_control_panel())


@router.get("/drift-monitoring", response_model=MLDriftMonitoringResponse)
async def get_ml_drift_monitoring(
    days: int = Query(default=30, ge=7, le=365),
    _admin: dict = Depends(require_admin),
    service: MLAdminService = Depends(get_ml_admin_service),
) -> MLDriftMonitoringResponse:
    return MLDriftMonitoringResponse(**service.get_drift_monitoring(days=days))


@router.post("/actions/{action_key}", response_model=MLActionRun)
async def trigger_ml_action(
    action_key: str,
    payload: MLActionRequest,
    background_tasks: BackgroundTasks,
    admin: dict = Depends(require_admin),
    service: MLAdminService = Depends(get_ml_admin_service),
) -> MLActionRun:
    try:
        result = service.trigger_action(
            action_key=action_key,
            admin=admin,
            confirmation_phrase=payload.confirmation_phrase,
            note=payload.note,
            scheduler=background_tasks.add_task,
        )
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return MLActionRun(**result)


@router.get("/dashboard", response_model=MLDashboardResponse)
async def get_ml_dashboard(
    _admin: dict = Depends(require_admin),
    service: MLAdminService = Depends(get_ml_admin_service),
) -> MLDashboardResponse:
    return MLDashboardResponse(**service.get_dashboard())
