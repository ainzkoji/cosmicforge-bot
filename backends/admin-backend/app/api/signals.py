from __future__ import annotations

import logging
import time
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query

from app.core.deps import require_admin
from app.persistence.db import AdminDB, get_db
from app.persistence.repositories.signals_repo import (
    InvalidSignalFilter,
    get_signal_scan_run_detail,
    list_signal_candidates,
    list_signal_pair_metrics,
    list_signal_pairs,
    list_signal_scan_runs,
    list_trading_signals,
)


router = APIRouter(prefix="/api/admin/signals", tags=["admin-signals"])
logger = logging.getLogger(__name__)


def _log_timing(endpoint: str, started_at: float, *, row_count: int | None = None) -> None:
    logger.info(
        "admin_signals_endpoint endpoint=%s duration_ms=%s row_count=%s",
        endpoint,
        round((time.perf_counter() - started_at) * 1000, 2),
        row_count,
    )


def _bad_filter(exc: InvalidSignalFilter) -> HTTPException:
    return HTTPException(status_code=400, detail=str(exc))


@router.get("")
def admin_signals(
    asset_class: str | None = Query(default="crypto"),
    status: str | None = Query(default=None),
    symbol: str | None = Query(default=None),
    side: str | None = Query(default=None),
    is_published: bool | int | str | None = Query(default=None),
    dev_mode: bool | int | str | None = Query(default=None),
    limit: int = Query(default=50, ge=1),
    offset: int = Query(default=0, ge=0),
    _admin: dict[str, Any] = Depends(require_admin),
    db: AdminDB = Depends(get_db),
) -> dict[str, Any]:
    del _admin
    started_at = time.perf_counter()
    try:
        payload = list_trading_signals(
            db,
            asset_class=asset_class,
            status=status,
            symbol=symbol,
            side=side,
            is_published=is_published,
            dev_mode=dev_mode,
            limit=limit,
            offset=offset,
        )
    except InvalidSignalFilter as exc:
        raise _bad_filter(exc) from exc
    _log_timing("GET /api/admin/signals", started_at, row_count=len(payload.get("items", [])))
    return payload


@router.get("/candidates")
def admin_signal_candidates(
    asset_class: str | None = Query(default="crypto"),
    status: str | None = Query(default=None),
    symbol: str | None = Query(default=None),
    side: str | None = Query(default=None),
    dev_mode: bool | int | str | None = Query(default=None),
    limit: int = Query(default=50, ge=1),
    offset: int = Query(default=0, ge=0),
    _admin: dict[str, Any] = Depends(require_admin),
    db: AdminDB = Depends(get_db),
) -> dict[str, Any]:
    del _admin
    started_at = time.perf_counter()
    try:
        payload = list_signal_candidates(
            db,
            asset_class=asset_class,
            status=status,
            symbol=symbol,
            side=side,
            dev_mode=dev_mode,
            limit=limit,
            offset=offset,
        )
    except InvalidSignalFilter as exc:
        raise _bad_filter(exc) from exc
    _log_timing("GET /api/admin/signals/candidates", started_at, row_count=len(payload.get("items", [])))
    return payload


@router.get("/pairs")
def admin_signal_pairs(
    exchange: str | None = None,
    asset_class: str | None = None,
    quote_asset: str | None = None,
    contract_type: str | None = None,
    tier: str | None = None,
    enabled: bool | int | str | None = Query(default=None),
    whitelisted: bool | int | str | None = Query(default=None),
    blacklisted: bool | int | str | None = Query(default=None),
    search: str | None = None,
    limit: int = Query(default=100, ge=1),
    offset: int = Query(default=0, ge=0),
    _admin: dict[str, Any] = Depends(require_admin),
    db: AdminDB = Depends(get_db),
) -> dict[str, Any]:
    del _admin
    started_at = time.perf_counter()
    try:
        payload = list_signal_pairs(
            db,
            exchange=exchange,
            asset_class=asset_class,
            quote_asset=quote_asset,
            contract_type=contract_type,
            tier=tier,
            enabled=enabled,
            whitelisted=whitelisted,
            blacklisted=blacklisted,
            search=search,
            limit=limit,
            offset=offset,
        )
    except InvalidSignalFilter as exc:
        raise _bad_filter(exc) from exc
    _log_timing("GET /api/admin/signals/pairs", started_at, row_count=len(payload.get("items", [])))
    return payload


@router.get("/pairs/metrics")
def admin_signal_pair_metrics(
    exchange: str | None = None,
    is_safe: bool | int | str | None = Query(default=None),
    min_volume: float | None = None,
    max_spread: float | None = None,
    symbol: str | None = None,
    search: str | None = None,
    limit: int = Query(default=100, ge=1),
    offset: int = Query(default=0, ge=0),
    _admin: dict[str, Any] = Depends(require_admin),
    db: AdminDB = Depends(get_db),
) -> dict[str, Any]:
    del _admin
    started_at = time.perf_counter()
    try:
        payload = list_signal_pair_metrics(
            db,
            exchange=exchange,
            is_safe=is_safe,
            min_volume=min_volume,
            max_spread=max_spread,
            symbol=symbol,
            search=search,
            limit=limit,
            offset=offset,
        )
    except InvalidSignalFilter as exc:
        raise _bad_filter(exc) from exc
    _log_timing("GET /api/admin/signals/pairs/metrics", started_at, row_count=len(payload.get("items", [])))
    return payload


@router.get("/scan-runs")
def admin_signal_scan_runs(
    scan_type: str | None = None,
    status: str | None = None,
    limit: int = Query(default=50, ge=1),
    offset: int = Query(default=0, ge=0),
    _admin: dict[str, Any] = Depends(require_admin),
    db: AdminDB = Depends(get_db),
) -> dict[str, Any]:
    del _admin
    started_at = time.perf_counter()
    payload = list_signal_scan_runs(db, scan_type=scan_type, status=status, limit=limit, offset=offset)
    _log_timing("GET /api/admin/signals/scan-runs", started_at, row_count=len(payload.get("items", [])))
    return payload


@router.get("/scan-runs/{scan_run_id}")
def admin_signal_scan_run_detail(
    scan_run_id: str,
    _admin: dict[str, Any] = Depends(require_admin),
    db: AdminDB = Depends(get_db),
) -> dict[str, Any]:
    del _admin
    started_at = time.perf_counter()
    payload = get_signal_scan_run_detail(db, scan_run_id)
    _log_timing("GET /api/admin/signals/scan-runs/{scan_run_id}", started_at, row_count=1 if payload else 0)
    if payload is None:
        raise HTTPException(status_code=404, detail="SCAN_RUN_NOT_FOUND")
    return payload
