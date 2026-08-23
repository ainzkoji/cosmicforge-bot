"""
Monitoring Proxy API

Proxies admin monitoring requests from frontend to bot-backend service.
"""
from fastapi import APIRouter, Depends, HTTPException, Request
from typing import Optional
import httpx
import logging

from app.api.auth import get_current_active_user
from app.core.config import settings

router = APIRouter(prefix="/api/v1/monitoring", tags=["monitoring-proxy"])
logger = logging.getLogger(__name__)

# Bot-backend service URL (internal service-to-service)
BOT_BACKEND_URL = getattr(settings, 'BOT_BACKEND_URL', 'http://127.0.0.1:9000')


@router.get("/system-health")
async def get_system_health(
    request: Request,
    user: dict = Depends(get_current_active_user)
):
    """Proxy: Get overall system health status."""
    try:
        auth_header = request.headers.get("authorization")
        headers = {"Authorization": auth_header} if auth_header else {}

        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(
                f"{BOT_BACKEND_URL}/api/v1/monitoring/system-health",
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


@router.get("/system-metrics")
async def get_system_metrics(
    request: Request,
    user: dict = Depends(get_current_active_user)
):
    """Proxy: Get detailed system metrics."""
    try:
        auth_header = request.headers.get("authorization")
        headers = {"Authorization": auth_header} if auth_header else {}

        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(
                f"{BOT_BACKEND_URL}/api/v1/monitoring/system-metrics",
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


@router.get("/bots-overview")
async def get_bots_overview(
    request: Request,
    user: dict = Depends(get_current_active_user)
):
    """Proxy: Get overview of all bots and their activity."""
    try:
        auth_header = request.headers.get("authorization")
        headers = {"Authorization": auth_header} if auth_header else {}

        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(
                f"{BOT_BACKEND_URL}/api/v1/monitoring/bots-overview",
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


@router.get("/activity-events")
async def get_activity_events(
    request: Request,
    limit: int = 100,
    event_type: Optional[str] = None,
    severity: Optional[str] = None,
    user: dict = Depends(get_current_active_user)
):
    """Proxy: Get recent activity events."""
    try:
        auth_header = request.headers.get("authorization")
        headers = {"Authorization": auth_header} if auth_header else {}

        params = {"limit": limit}
        if event_type:
            params["event_type"] = event_type
        if severity:
            params["severity"] = severity

        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(
                f"{BOT_BACKEND_URL}/api/v1/monitoring/activity-events",
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
