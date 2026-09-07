"""
Reports Proxy API

Proxies reporting requests from frontend to bot-backend service.
Forwards all reporting endpoints (P&L, stats, drawdown, tax, benchmarking).
"""
from fastapi import APIRouter, Depends, Query, HTTPException, Response
from typing import Optional
import httpx
import logging

from app.api.auth import get_current_active_user
from app.core.config import settings

router = APIRouter()
logger = logging.getLogger(__name__)

# Bot-backend service URL (internal service-to-service)
BOT_BACKEND_URL = getattr(settings, 'BOT_BACKEND_URL', 'http://127.0.0.1:9000')


# ============================================================================
# P&L ENDPOINTS
# ============================================================================

@router.get("/pnl/realized")
async def proxy_realized_pnl(
    broker_account_id: Optional[str] = Query(None),
    bot_instance_id: Optional[str] = Query(None),
    symbol: Optional[str] = Query(None),
    days: Optional[int] = Query(None),
    user: dict = Depends(get_current_active_user)
):
    """Proxy: Get realized P&L"""
    params = {}
    if broker_account_id:
        params["broker_account_id"] = broker_account_id
    if bot_instance_id:
        params["bot_instance_id"] = bot_instance_id
    if symbol:
        params["symbol"] = symbol
    if days:
        params["days"] = days
    
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(
                f"{BOT_BACKEND_URL}/api/v1/reports/pnl/realized",
                params=params,
                headers={"Authorization": f"Bearer {user.get('access_token', '')}"},
                timeout=30.0
            )
            response.raise_for_status()
            return response.json()
        except httpx.HTTPError as e:
            logger.error(f"Failed to proxy realized P&L: {e}")
            raise HTTPException(status_code=500, detail=str(e))


@router.get("/pnl/unrealized")
async def proxy_unrealized_pnl(
    broker_account_id: Optional[str] = Query(None),
    user: dict = Depends(get_current_active_user)
):
    """Proxy: Get unrealized P&L"""
    params = {}
    if broker_account_id:
        params["broker_account_id"] = broker_account_id
    
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(
                f"{BOT_BACKEND_URL}/api/v1/reports/pnl/unrealized",
                params=params,
                headers={"Authorization": f"Bearer {user.get('access_token', '')}"},
                timeout=30.0
            )
            response.raise_for_status()
            return response.json()
        except httpx.HTTPError as e:
            logger.error(f"Failed to proxy unrealized P&L: {e}")
            raise HTTPException(status_code=500, detail=str(e))


@router.get("/pnl/total")
async def proxy_total_pnl(
    broker_account_id: Optional[str] = Query(None),
    bot_instance_id: Optional[str] = Query(None),
    days: Optional[int] = Query(None),
    user: dict = Depends(get_current_active_user)
):
    """Proxy: Get total P&L"""
    params = {}
    if broker_account_id:
        params["broker_account_id"] = broker_account_id
    if bot_instance_id:
        params["bot_instance_id"] = bot_instance_id
    if days:
        params["days"] = days
    
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(
                f"{BOT_BACKEND_URL}/api/v1/reports/pnl/total",
                params=params,
                headers={"Authorization": f"Bearer {user.get('access_token', '')}"},
                timeout=30.0
            )
            response.raise_for_status()
            return response.json()
        except httpx.HTTPError as e:
            logger.error(f"Failed to proxy total P&L: {e}")
            raise HTTPException(status_code=500, detail=str(e))


@router.get("/pnl/breakdown")
async def proxy_pnl_breakdown(
    group_by: str = Query("broker_account"),
    days: Optional[int] = Query(None),
    user: dict = Depends(get_current_active_user)
):
    """Proxy: Get P&L breakdown"""
    params = {"group_by": group_by}
    if days:
        params["days"] = days
    
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(
                f"{BOT_BACKEND_URL}/api/v1/reports/pnl/breakdown",
                params=params,
                headers={"Authorization": f"Bearer {user.get('access_token', '')}"},
                timeout=30.0
            )
            response.raise_for_status()
            return response.json()
        except httpx.HTTPError as e:
            logger.error(f"Failed to proxy P&L breakdown: {e}")
            raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# TRADE STATISTICS ENDPOINTS
# ============================================================================

@router.get("/stats/summary")
async def proxy_trade_summary(
    broker_account_id: Optional[str] = Query(None),
    bot_instance_id: Optional[str] = Query(None),
    days: Optional[int] = Query(None),
    user: dict = Depends(get_current_active_user)
):
    """Proxy: Get trade summary"""
    params = {}
    if broker_account_id:
        params["broker_account_id"] = broker_account_id
    if bot_instance_id:
        params["bot_instance_id"] = bot_instance_id
    if days:
        params["days"] = days
    
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(
                f"{BOT_BACKEND_URL}/api/v1/reports/stats/summary",
                params=params,
                headers={"Authorization": f"Bearer {user.get('access_token', '')}"},
                timeout=30.0
            )
            response.raise_for_status()
            return response.json()
        except httpx.HTTPError as e:
            logger.error(f"Failed to proxy trade summary: {e}")
            raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# DRAWDOWN ENDPOINTS
# ============================================================================

@router.get("/drawdown/current")
async def proxy_current_drawdown(
    broker_account_id: Optional[str] = Query(None),
    user: dict = Depends(get_current_active_user)
):
    """Proxy: Get current drawdown"""
    params = {}
    if broker_account_id:
        params["broker_account_id"] = broker_account_id
    
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(
                f"{BOT_BACKEND_URL}/api/v1/reports/drawdown/current",
                params=params,
                headers={"Authorization": f"Bearer {user.get('access_token', '')}"},
                timeout=30.0
            )
            response.raise_for_status()
            return response.json()
        except httpx.HTTPError as e:
            logger.error(f"Failed to proxy current drawdown: {e}")
            raise HTTPException(status_code=500, detail=str(e))


@router.get("/drawdown/max")
async def proxy_max_drawdown(
    broker_account_id: Optional[str] = Query(None),
    bot_instance_id: Optional[str] = Query(None),
    days: int = Query(90),
    user: dict = Depends(get_current_active_user)
):
    """Proxy: Get max drawdown"""
    params = {"days": days}
    if broker_account_id:
        params["broker_account_id"] = broker_account_id
    if bot_instance_id:
        params["bot_instance_id"] = bot_instance_id
    
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(
                f"{BOT_BACKEND_URL}/api/v1/reports/drawdown/max",
                params=params,
                headers={"Authorization": f"Bearer {user.get('access_token', '')}"},
                timeout=30.0
            )
            response.raise_for_status()
            return response.json()
        except httpx.HTTPError as e:
            logger.error(f"Failed to proxy max drawdown: {e}")
            raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# TAX REPORT ENDPOINTS
# ============================================================================

@router.get("/tax/report/{tax_year}")
async def proxy_tax_report(
    tax_year: int,
    broker_account_id: Optional[str] = Query(None),
    user: dict = Depends(get_current_active_user)
):
    """Proxy: Get tax report"""
    params = {}
    if broker_account_id:
        params["broker_account_id"] = broker_account_id
    
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(
                f"{BOT_BACKEND_URL}/api/v1/reports/tax/report/{tax_year}",
                params=params,
                headers={"Authorization": f"Bearer {user.get('access_token', '')}"},
                timeout=30.0
            )
            response.raise_for_status()
            return response.json()
        except httpx.HTTPError as e:
            logger.error(f"Failed to proxy tax report: {e}")
            raise HTTPException(status_code=500, detail=str(e))


@router.get("/tax/export/{tax_year}/csv")
async def proxy_tax_export_csv(
    tax_year: int,
    broker_account_id: Optional[str] = Query(None),
    user: dict = Depends(get_current_active_user)
):
    """Proxy: Export tax report as CSV"""
    params = {}
    if broker_account_id:
        params["broker_account_id"] = broker_account_id
    
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(
                f"{BOT_BACKEND_URL}/api/v1/reports/tax/export/{tax_year}/csv",
                params=params,
                headers={"Authorization": f"Bearer {user.get('access_token', '')}"},
                timeout=30.0
            )
            response.raise_for_status()
            
            return Response(
                content=response.content,
                media_type="text/csv",
                headers={
                    "Content-Disposition": f"attachment; filename=tax_report_{tax_year}.csv"
                }
            )
        except httpx.HTTPError as e:
            logger.error(f"Failed to proxy tax CSV export: {e}")
            raise HTTPException(status_code=500, detail=str(e))


@router.get("/tax/export/{tax_year}/json")
async def proxy_tax_export_json(
    tax_year: int,
    broker_account_id: Optional[str] = Query(None),
    user: dict = Depends(get_current_active_user)
):
    """Proxy: Export tax report as JSON"""
    params = {}
    if broker_account_id:
        params["broker_account_id"] = broker_account_id
    
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(
                f"{BOT_BACKEND_URL}/api/v1/reports/tax/export/{tax_year}/json",
                params=params,
                headers={"Authorization": f"Bearer {user.get('access_token', '')}"},
                timeout=30.0
            )
            response.raise_for_status()
            
            return Response(
                content=response.content,
                media_type="application/json",
                headers={
                    "Content-Disposition": f"attachment; filename=tax_report_{tax_year}.json"
                }
            )
        except httpx.HTTPError as e:
            logger.error(f"Failed to proxy tax JSON export: {e}")
            raise HTTPException(status_code=500, detail=str(e))


@router.get("/tax/export/{tax_year}/pdf")
async def proxy_tax_export_pdf(
    tax_year: int,
    broker_account_id: Optional[str] = Query(None),
    user: dict = Depends(get_current_active_user)
):
    """Proxy: Export tax report as PDF"""
    params = {}
    if broker_account_id:
        params["broker_account_id"] = broker_account_id
    
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(
                f"{BOT_BACKEND_URL}/api/v1/reports/tax/export/{tax_year}/pdf",
                params=params,
                headers={"Authorization": f"Bearer {user.get('access_token', '')}"},
                timeout=30.0
            )
            response.raise_for_status()
            
            return Response(
                content=response.content,
                media_type="application/pdf",
                headers={
                    "Content-Disposition": f"attachment; filename=tax_report_{tax_year}.pdf"
                }
            )
        except httpx.HTTPError as e:
            logger.error(f"Failed to proxy tax PDF export: {e}")
            raise HTTPException(status_code=500, detail=str(e))



# ============================================================================
# BENCHMARKING ENDPOINTS
# ============================================================================

@router.get("/benchmark/sharpe-ratio")
async def proxy_sharpe_ratio(
    broker_account_id: Optional[str] = Query(None),
    days: int = Query(30),
    risk_free_rate: float = Query(0.0),
    user: dict = Depends(get_current_active_user)
):
    """Proxy: Get Sharpe ratio"""
    params = {"days": days, "risk_free_rate": risk_free_rate}
    if broker_account_id:
        params["broker_account_id"] = broker_account_id
    
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(
                f"{BOT_BACKEND_URL}/api/v1/reports/benchmark/sharpe-ratio",
                params=params,
                headers={"Authorization": f"Bearer {user.get('access_token', '')}"},
                timeout=30.0
            )
            response.raise_for_status()
            return response.json()
        except httpx.HTTPError as e:
            logger.error(f"Failed to proxy Sharpe ratio: {e}")
            raise HTTPException(status_code=500, detail=str(e))
