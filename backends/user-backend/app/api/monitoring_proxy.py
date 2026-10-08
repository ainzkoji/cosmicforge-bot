"""Monitoring proxy: /api/v1/monitoring/* on the user backend -> the bot backend.

Repaired in Step 1.7: every route forwards to the path the bot backend actually
serves (``/system/health``, ``/system/metrics``, ``/bots/overview``,
``/activity/events``), goes through the shared ``proxy_request`` (one pooled
client, the caller's Authorization header, timeouts, 502 on connection
failure, upstream status and body passed through unchanged), and refuses
non-administrators here, matching the bot backend's ``require_admin`` on
these routes instead of forwarding a request that will be refused.
"""
from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Request

from app.api.auth import get_current_active_user
from app.api.proxy_utils import proxy_request

router = APIRouter(prefix="/api/v1/monitoring", tags=["monitoring-proxy"])

#: user-backend path -> bot-backend path
UPSTREAM = {
    "/system-health": "/api/v1/monitoring/system/health",
    "/system-metrics": "/api/v1/monitoring/system/metrics",
    "/bots-overview": "/api/v1/monitoring/bots/overview",
    "/activity-events": "/api/v1/monitoring/activity/events",
}


def _require_admin_user(user: dict) -> None:
    if str(user.get("role") or "user") != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")


@router.get("/system-health")
async def get_system_health(request: Request, user: dict = Depends(get_current_active_user)):
    """Proxy: overall system health (admin)."""
    _require_admin_user(user)
    return await proxy_request(request, UPSTREAM["/system-health"], method="GET", params={})


@router.get("/system-metrics")
async def get_system_metrics(request: Request, user: dict = Depends(get_current_active_user)):
    """Proxy: detailed system metrics (admin)."""
    _require_admin_user(user)
    return await proxy_request(request, UPSTREAM["/system-metrics"], method="GET", params={})


@router.get("/bots-overview")
async def get_bots_overview(request: Request, user: dict = Depends(get_current_active_user)):
    """Proxy: overview of all bots and their activity (admin)."""
    _require_admin_user(user)
    return await proxy_request(request, UPSTREAM["/bots-overview"], method="GET", params={})


@router.get("/activity-events")
async def get_activity_events(request: Request, limit: int = 100, event_type: Optional[str] = None,
                              severity: Optional[str] = None, user: dict = Depends(get_current_active_user)):
    """Proxy: recent activity events (admin)."""
    _require_admin_user(user)
    params = {"limit": limit}
    if event_type:
        params["event_type"] = event_type
    if severity:
        params["severity"] = severity
    return await proxy_request(request, UPSTREAM["/activity-events"], method="GET", params=params)


__all__ = ["router", "UPSTREAM"]
