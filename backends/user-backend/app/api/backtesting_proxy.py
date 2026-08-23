from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from typing import Optional, Dict, Any
import logging
from app.api.auth import get_current_active_user
from app.api.proxy_utils import proxy_request

router = APIRouter()
logger = logging.getLogger(__name__)

@router.post("/")
async def proxy_create_backtest(
    request: Request,
    user: dict = Depends(get_current_active_user)
):
    """Proxy: Create backtest run"""
    try:
        json_body = await request.json()
        return await proxy_request(
            request=request,
            target_path="/api/v1/backtests/",
            method="POST",
            json_body=json_body
        )
    except Exception as e:
        logger.error(f"Failed to proxy create backtest: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/")
async def proxy_list_backtests(
    request: Request,
    status: Optional[str] = Query(None),
    page: int = Query(1),
    size: int = Query(20),
    user: dict = Depends(get_current_active_user)
):
    """Proxy: List backtests"""
    return await proxy_request(
        request=request,
        target_path="/api/v1/backtests/",
        method="GET",
        params={"status": status, "page": page, "size": size}
    )

@router.get("/{run_id}")
async def proxy_get_backtest(
    request: Request,
    run_id: str,
    user: dict = Depends(get_current_active_user)
):
    """Proxy: Get backtest details"""
    return await proxy_request(
        request=request,
        target_path=f"/api/v1/backtests/{run_id}",
        method="GET"
    )

@router.get("/{run_id}/equity")
async def proxy_get_equity(
    request: Request,
    run_id: str,
    user: dict = Depends(get_current_active_user)
):
    """Proxy: Get equity curve"""
    return await proxy_request(
        request=request,
        target_path=f"/api/v1/backtests/{run_id}/equity",
        method="GET"
    )

@router.get("/{run_id}/fills")
async def proxy_get_fills(
    request: Request,
    run_id: str,
    page: int = Query(1),
    size: int = Query(50),
    user: dict = Depends(get_current_active_user)
):
    """Proxy: Get fills"""
    return await proxy_request(
        request=request,
        target_path=f"/api/v1/backtests/{run_id}/fills",
        method="GET",
        params={"page": page, "size": size}
    )

@router.post("/{run_id}/cancel")
async def proxy_cancel_backtest(
    request: Request,
    run_id: str,
    user: dict = Depends(get_current_active_user)
):
    """Proxy: Cancel backtest"""
    return await proxy_request(
        request=request,
        target_path=f"/api/v1/backtests/{run_id}/cancel",
        method="POST"
    )

@router.get("/{run_id}/export")
async def proxy_export_backtest(
    request: Request,
    run_id: str,
    format: str = Query("csv"),
    user: dict = Depends(get_current_active_user)
):
    """Proxy: Export backtest results"""
    return await proxy_request(
        request=request,
        target_path=f"/api/v1/backtests/{run_id}/export",
        method="GET",
        params={"format": format}
    )
