"""
Analytics Proxy API

Proxies analytics requests from frontend to bot-backend service.
"""
from fastapi import APIRouter, Depends, Query, HTTPException
from fastapi.responses import StreamingResponse
from starlette.background import BackgroundTask
from starlette.requests import Request
from typing import Optional
import httpx
import logging

from app.api.auth import get_current_active_user
from app.core.config import settings

router = APIRouter()
logger = logging.getLogger(__name__)

# Bot-backend service URL (internal service-to-service)
BOT_BACKEND_URL = getattr(settings, 'BOT_BACKEND_URL', 'http://127.0.0.1:9000')


@router.get("/overview")
async def get_overview(
    request: Request,
    environment: str = Query("PAPER", description="Environment (LIVE/PAPER)"),
    timeframe: str = Query("ALL", description="Timeframe filter (1M, 3M, YTD, ALL)"),
    user: dict = Depends(get_current_active_user)
):
    """Proxy: Get high-level portfolio overview statistics."""
    try:
        # Forward Authorization header
        auth_header = request.headers.get("authorization")
        headers = {"Authorization": auth_header} if auth_header else {}

        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(
                f"{BOT_BACKEND_URL}/api/v1/analytics/overview",
                params={
                    "environment": environment,
                    "timeframe": timeframe
                },
                headers=headers
            )
            
            if response.status_code != 200:
                raise HTTPException(
                    status_code=response.status_code,
                    detail=response.text
                )
            
            return response.json()
            
    except httpx.RequestError as e:
        logger.error(f"Failed to connect to bot-backend: {e}")
        raise HTTPException(status_code=503, detail=f"Bot service unavailable: {str(e)}")


@router.get("/leaderboard")
async def get_leaderboard(
    request: Request,
    environment: str = Query("PAPER"),
    limit: int = 20,
    user: dict = Depends(get_current_active_user)
):
    """Proxy: Get top performing strategies."""
    try:
        # Forward Authorization header
        auth_header = request.headers.get("authorization")
        headers = {"Authorization": auth_header} if auth_header else {}

        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(
                f"{BOT_BACKEND_URL}/api/v1/analytics/leaderboard",
                params={
                    "environment": environment,
                    "limit": limit
                },
                headers=headers
            )
            
            if response.status_code != 200:
                raise HTTPException(
                    status_code=response.status_code,
                    detail=response.text
                )
            
            return response.json()
            
    except httpx.RequestError as e:
        logger.error(f"Failed to connect to bot-backend: {e}")
        raise HTTPException(status_code=503, detail=f"Bot service unavailable: {str(e)}")


@router.get("/calibration")
async def get_calibration(
    request: Request,
    environment: str = Query("PAPER"),
    strategy: Optional[str] = None,
    user: dict = Depends(get_current_active_user)
):
    """Proxy: Get confidence calibration buckets."""
    try:
        # Forward Authorization header
        auth_header = request.headers.get("authorization")
        headers = {"Authorization": auth_header} if auth_header else {}

        params = {"environment": environment}
        if strategy:
            params["strategy"] = strategy

        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(
                f"{BOT_BACKEND_URL}/api/v1/analytics/calibration",
                params=params,
                headers=headers
            )
            
            if response.status_code != 200:
                raise HTTPException(
                    status_code=response.status_code,
                    detail=response.text
                )
            
            return response.json()
            
            
    except httpx.RequestError as e:
        logger.error(f"Failed to connect to bot-backend: {e}")
        raise HTTPException(status_code=503, detail=f"Bot service unavailable: {str(e)}")


@router.get("/export")
async def export_analytics(
    request: Request,
    timeframe: str = Query("ALL", description="Timeframe filter"),
    format: str = Query("csv", description="Export format"),
    user: dict = Depends(get_current_active_user)
):
    """Proxy: Export analytics data."""
    try:
        # Forward Authorization header
        auth_header = request.headers.get("authorization")
        headers = {"Authorization": auth_header} if auth_header else {}
        
        client = httpx.AsyncClient(timeout=60.0)
        url = f"{BOT_BACKEND_URL}/api/v1/analytics/export"
        
        req = client.build_request(
            "GET", 
            url, 
            params={"timeframe": timeframe, "format": format},
            headers=headers
        )
        
        r = await client.send(req, stream=True)
        
        if r.status_code != 200:
            await r.aclose()
            await client.aclose()
            try:
                error_detail = r.text
            except Exception:
                error_detail = "Export failed upstream"
            raise HTTPException(status_code=r.status_code, detail=error_detail)
            
        proxy_headers = {}
        if r.headers.get("content-disposition"):
            proxy_headers["Content-Disposition"] = r.headers.get("content-disposition")
        if r.headers.get("content-length"):
            proxy_headers["Content-Length"] = r.headers.get("content-length")

        return StreamingResponse(
            r.aiter_bytes(),
            status_code=r.status_code,
            media_type=r.headers.get("content-type"),
            headers=proxy_headers,
            background=BackgroundTask(client.aclose)
        )
            
    except httpx.RequestError as e:
        logger.error(f"Failed to connect to bot-backend: {e}")
        raise HTTPException(status_code=503, detail=f"Bot service unavailable: {str(e)}")


# ========================================
# EQUITY REPORTING ENDPOINTS
# ========================================

@router.get("/equity-curve")
async def proxy_equity_curve(
    request: Request,
    broker_account_id: Optional[str] = None,
    bot_instance_id: Optional[str] = None,
    days: int = 30,
    interval: str = "1h",
    user: dict = Depends(get_current_active_user)
):
    """Proxy equity curve data from bot-backend."""
    try:
        auth_header = request.headers.get("authorization")
        headers = {"Authorization": auth_header} if auth_header else {}
        
        params = {"days": days, "interval": interval}
        if broker_account_id:
            params["broker_account_id"] = broker_account_id
        if bot_instance_id:
            params["bot_instance_id"] = bot_instance_id
        
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(
                f"{BOT_BACKEND_URL}/api/v1/analytics/equity-curve",
                params=params,
                headers=headers
            )
            
            if response.status_code != 200:
                raise HTTPException(
                    status_code=response.status_code,
                    detail=response.text
                )
            
            return response.json()
            
    except httpx.RequestError as e:
        logger.error(f"Failed to connect to bot-backend: {e}")
        raise HTTPException(status_code=503, detail=f"Bot service unavailable: {str(e)}")


@router.get("/equity-latest")
async def proxy_equity_latest(
    request: Request,
    user: dict = Depends(get_current_active_user)
):
    """Proxy latest equity snapshots from bot-backend."""
    try:
        auth_header = request.headers.get("authorization")
        headers = {"Authorization": auth_header} if auth_header else {}
        
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(
                f"{BOT_BACKEND_URL}/api/v1/analytics/equity-latest",
                headers=headers
            )
            
            if response.status_code != 200:
                raise HTTPException(
                    status_code=response.status_code,
                    detail=response.text
                )
            
            return response.json()
            
    except httpx.RequestError as e:
        logger.error(f"Failed to connect to bot-backend: {e}")
        raise HTTPException(status_code=503, detail=f"Bot service unavailable: {str(e)}")


@router.get("/tax-report")
async def get_tax_report(
    request: Request,
    year: int = Query(..., description="Tax year"),
    bot_instance_id: Optional[str] = Query(None),
    broker_account_id: Optional[str] = Query(None),
    format: str = Query("json"),
    user: dict = Depends(get_current_active_user)
):
    """Proxy: Get tax report"""
    try:
        # Forward Authorization header
        auth_header = request.headers.get("authorization")
        headers = {"Authorization": auth_header} if auth_header else {}
        
        params = {"year": year, "format": format}
        if bot_instance_id:
            params["bot_instance_id"] = bot_instance_id
        if broker_account_id:
            params["broker_account_id"] = broker_account_id
            
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(
                f"{BOT_BACKEND_URL}/api/v1/analytics/tax-report",
                params=params,
                headers=headers
            )
            
            if response.status_code != 200:
                raise HTTPException(status_code=response.status_code, detail=response.text)
            
            # If CSV, return proper response
            if format.lower() == "csv":
                 return Response(
                    content=response.content,
                    media_type="text/csv",
                    headers={
                        "Content-Disposition": response.headers.get("content-disposition", f"attachment; filename=tax_report_{year}.csv")
                    }
                )
            
            return response.json()
            
    except httpx.RequestError as e:
        logger.error(f"Failed to connect to bot-backend for tax report: {e}")
        raise HTTPException(status_code=503, detail=f"Bot service unavailable: {str(e)}")
