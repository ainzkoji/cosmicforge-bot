"""
Reporting API - User-Facing Analytics Endpoints

Enforces ownership via JWT. All user_id is derived from token, never from client.
Implements Phase 8 contracts.
"""
from fastapi import APIRouter, Depends, HTTPException, Query, Response
from typing import Optional, Dict, Any, List
import logging
import httpx
import asyncio

from app.api.auth import get_current_active_user, oauth2_scheme
from app.core.config import settings

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/analytics", tags=["Analytics"])
BOT_BACKEND_URL = getattr(settings, 'BOT_BACKEND_URL', 'http://127.0.0.1:9000')


def _shape_overview_response(raw: dict) -> dict:
    """
    Transform bot-backend analytics/reporting/overview into the stable OverviewStats
    shape the frontend expects. Missing metrics are returned as null with metadata notes
    so the frontend can show N/A rather than a silent 0.
    """
    unavailable: list = []

    pnl_net = raw.get("pnl_net", 0.0) or 0.0
    max_drawdown = raw.get("max_drawdown", 0.0) or 0.0
    sharpe_ratio = raw.get("sharpe_ratio")

    # profit_change_pct needs previous-period snapshot — not yet implemented
    unavailable.append("profit_change_pct")
    # monthly_pnl needs a separate aggregation query not part of the overview call
    unavailable.append("monthly_pnl")
    # Advanced risk metrics not computed by trade_stats_service yet
    unavailable.extend(["volatility_30d", "sortino_ratio", "alpha"])
    # asset_allocation not in overview endpoint
    unavailable.append("asset_allocation")

    return {
        # Primary PnL — frontend reads total_profit; pnl_net preserved for transparency
        "total_profit": pnl_net,
        "pnl_net": pnl_net,
        "pnl_gross": raw.get("pnl_gross", 0.0) or 0.0,
        "fees_total": raw.get("fees_total", 0.0) or 0.0,
        "profit_change_pct": None,  # unavailable — show N/A
        # Trade stats
        "win_rate": raw.get("win_rate", 0.0) or 0.0,
        "profit_factor": raw.get("profit_factor", 0.0) or 0.0,
        "sharpe_ratio": sharpe_ratio,
        "total_trades": raw.get("total_trades", 0) or 0,
        "wins": raw.get("wins", 0) or 0,
        "losses": raw.get("losses", 0) or 0,
        "breakevens": raw.get("breakevens", 0) or 0,
        "expectancy": raw.get("expectancy"),
        # Equity
        "equity_start": raw.get("equity_start"),
        "equity_end": raw.get("equity_end"),
        "unrealized_pnl_current": raw.get("unrealized_pnl_current"),
        # max_drawdown at top level AND nested for backward compatibility
        "max_drawdown": max_drawdown,
        "risk_metrics": {
            "max_drawdown": max_drawdown,
            "volatility_30d": None,   # unavailable — show N/A
            "sortino_ratio": None,    # unavailable — show N/A
            "alpha": None,            # unavailable — show N/A
        },
        # Empty until dedicated endpoints are wired up
        "monthly_pnl": [],
        "asset_allocation": [],
        "equity_curve": [],
        # Metadata for debugging
        "metadata": {
            **raw.get("metadata", {}),
            "unavailable_metrics": unavailable,
        },
    }

async def fetch_with_retries(url: str, params: dict, headers: dict) -> Any:
    """
    Helper to fetch data from bot-backend with retries for transient errors.
    """
    retries = 3
    last_error = None
    
    for attempt in range(retries):
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.get(url, params=params, headers=headers)
                
                if response.status_code != 200:
                    raise HTTPException(status_code=response.status_code, detail=response.text)
                
                return response.json()
        except (httpx.ConnectError, httpx.ReadError, httpx.TimeoutException) as e:
            last_error = e
            if attempt < retries - 1:
                wait_time = 0.5 * (2 ** attempt)
                logger.warning(f"Connection to bot-backend failed (attempt {attempt+1}/{retries}). Retrying in {wait_time}s... request: {url}")
                await asyncio.sleep(wait_time)
            else:
                logger.error(f"Failed to connect to bot-backend after {retries} attempts: {e}")
        except httpx.RequestError as e:
            logger.error(f"Request error connecting to bot-backend: {e}")
            raise HTTPException(status_code=503, detail=f"Bot service unavailable: {str(e)}")
            
    raise HTTPException(status_code=503, detail=f"Bot service unavailable: {str(last_error)}")



@router.get("/overview")
async def get_analytics_overview(
    timeframe: str = Query("ALL"),
    bot_instance_id: Optional[str] = Query(None),
    broker_account_id: Optional[str] = Query(None),
    user: dict = Depends(get_current_active_user),
    token: str = Depends(oauth2_scheme)
):
    """
    Get comprehensive analytics overview with PnL, win rate, profit factor, drawdown, etc.
    """
    try:
        params = {"timeframe": timeframe}
        if bot_instance_id:
            params["bot_instance_id"] = bot_instance_id
        if broker_account_id:
            params["broker_account_id"] = broker_account_id

        logger.info(f"Sending analytics request. User: {user.get('id')}, Token prefix: {token[:10] if token else 'None'}...")
        headers = {"Authorization": f"Bearer {token}"}

        raw = await fetch_with_retries(
            f"{BOT_BACKEND_URL}/api/v1/analytics/reporting/overview",
            params,
            headers
        )
        return _shape_overview_response(raw)
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error in overview proxy: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/trades")
async def get_analytics_trades(
    timeframe: str = Query("ALL"),
    bot_instance_id: Optional[str] = Query(None),
    broker_account_id: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
    symbol: Optional[str] = Query(None),
    action: Optional[str] = Query(None),
    result: Optional[str] = Query(None),
    user: dict = Depends(get_current_active_user),
    token: str = Depends(oauth2_scheme)
):
    """
    Get paginated list of trade executions.
    Supports filters: symbol, action (OPEN|CLOSE), result (winner|loser).
    """
    try:
        params = {
            "timeframe": timeframe,
            "page": page,
            "page_size": page_size
        }
        if bot_instance_id:
            params["bot_instance_id"] = bot_instance_id
        if broker_account_id:
            params["broker_account_id"] = broker_account_id
        if symbol:
            params["symbol"] = symbol
        if action:
            params["action"] = action
        if result:
            params["result"] = result

        headers = {"Authorization": f"Bearer {token}"}

        return await fetch_with_retries(
            f"{BOT_BACKEND_URL}/api/v1/analytics/reporting/trades",
            params,
            headers
        )
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error in trades proxy: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/equity-curve")
async def get_equity_curve(
    timeframe: str = Query("ALL"),
    bot_instance_id: Optional[str] = Query(None),
    broker_account_id: Optional[str] = Query(None),
    user: dict = Depends(get_current_active_user),
    token: str = Depends(oauth2_scheme)
):
    """
    Get equity curve series from equity_snapshots.
    """
    try:
        params = {"timeframe": timeframe}
        if bot_instance_id:
            params["bot_instance_id"] = bot_instance_id
        if broker_account_id:
            params["broker_account_id"] = broker_account_id

        headers = {"Authorization": f"Bearer {token}"}
        
        return await fetch_with_retries(
            f"{BOT_BACKEND_URL}/api/v1/analytics/reporting/equity-curve",
            params,
            headers
        )
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error in equity-curve proxy: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/drawdown")
async def get_drawdown(
    timeframe: str = Query("ALL"),
    bot_instance_id: Optional[str] = Query(None),
    broker_account_id: Optional[str] = Query(None),
    user: dict = Depends(get_current_active_user),
    token: str = Depends(oauth2_scheme)
):
    """
    Get drawdown series and statistics.
    """
    try:
        params = {"timeframe": timeframe}
        if bot_instance_id:
            params["bot_instance_id"] = bot_instance_id
        if broker_account_id:
            params["broker_account_id"] = broker_account_id

        headers = {"Authorization": f"Bearer {token}"}
        
        return await fetch_with_retries(
            f"{BOT_BACKEND_URL}/api/v1/analytics/reporting/drawdown",
            params,
            headers
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error in drawdown proxy: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/benchmarks")
async def get_benchmarks(
    timeframe: str = Query("ALL"),
    bot_instance_id: Optional[str] = Query(None),
    broker_account_id: Optional[str] = Query(None),
    benchmark: str = Query("BTCUSDT"),
    user: dict = Depends(get_current_active_user),
    token: str = Depends(oauth2_scheme)
):
    """
    Get benchmark comparison with bot performance.
    """
    try:
        params = {
            "timeframe": timeframe,
            "benchmark": benchmark
        }
        if bot_instance_id:
            params["bot_instance_id"] = bot_instance_id
        if broker_account_id:
            params["broker_account_id"] = broker_account_id

        headers = {"Authorization": f"Bearer {token}"}
        
        return await fetch_with_retries(
            f"{BOT_BACKEND_URL}/api/v1/analytics/reporting/benchmarks",
            params,
            headers
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error in benchmarks proxy: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/export/pdf")
async def export_analytics_pdf(
    token: str = Depends(oauth2_scheme),
    timeframe: str = Query("ALL"),
    bot_instance_id: Optional[str] = Query(None),
    broker_account_id: Optional[str] = Query(None)
):
    """Export analytics overview as PDF"""
    headers = {"Authorization": f"Bearer {token}"}

    params = {"timeframe": timeframe}
    if bot_instance_id:
        params["bot_instance_id"] = bot_instance_id
    if broker_account_id:
        params["broker_account_id"] = broker_account_id

    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            response = await client.get(
                f"{BOT_BACKEND_URL}/api/v1/analytics/reporting/export/pdf",
                params=params,
                headers=headers
            )
            response.raise_for_status()

            return Response(
                content=response.content,
                media_type="application/pdf",
                headers=response.headers
            )
        except httpx.HTTPError as e:
            logger.error(f"Failed to export analytics PDF: {e}")
            raise HTTPException(status_code=500, detail=f"Export failed: {str(e)}")


@router.get("/positions/history")
async def get_positions_history(
    timeframe: str = Query("ALL"),
    bot_instance_id: Optional[str] = Query(None),
    broker_account_id: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    symbol: Optional[str] = Query(None),
    side: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    result: Optional[str] = Query(None),
    sort_by: Optional[str] = Query("opened_at"),
    sort_order: Optional[str] = Query("DESC"),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    user: dict = Depends(get_current_active_user),
    token: str = Depends(oauth2_scheme)
):
    """
    Proxy: position-level trade history grouped by position_id.
    """
    try:
        params: Dict[str, Any] = {
            "timeframe": timeframe,
            "page": page,
            "page_size": page_size,
            "sort_by": sort_by,
            "sort_order": sort_order,
        }
        if bot_instance_id:
            params["bot_instance_id"] = bot_instance_id
        if broker_account_id:
            params["broker_account_id"] = broker_account_id
        if symbol:
            params["symbol"] = symbol
        if side:
            params["side"] = side
        if status:
            params["status"] = status
        if result:
            params["result"] = result
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date

        headers = {"Authorization": f"Bearer {token}"}

        return await fetch_with_retries(
            f"{BOT_BACKEND_URL}/api/v1/analytics/reporting/positions/history",
            params,
            headers
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error in positions/history proxy: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/positions/history/export")
async def export_positions_history_csv(
    timeframe: str = Query("ALL"),
    bot_instance_id: Optional[str] = Query(None),
    broker_account_id: Optional[str] = Query(None),
    symbol: Optional[str] = Query(None),
    side: Optional[str] = Query(None),
    result: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    user: dict = Depends(get_current_active_user),
    token: str = Depends(oauth2_scheme)
):
    """
    Proxy: export completed position history as CSV. Auth required, user-scoped.
    """
    params: Dict[str, Any] = {"timeframe": timeframe}
    if bot_instance_id:
        params["bot_instance_id"] = bot_instance_id
    if broker_account_id:
        params["broker_account_id"] = broker_account_id
    if symbol:
        params["symbol"] = symbol
    if side:
        params["side"] = side
    if result:
        params["result"] = result
    if start_date:
        params["start_date"] = start_date
    if end_date:
        params["end_date"] = end_date

    headers = {"Authorization": f"Bearer {token}"}

    async with httpx.AsyncClient(timeout=60.0) as client:
        try:
            response = await client.get(
                f"{BOT_BACKEND_URL}/api/v1/analytics/reporting/positions/history/export",
                params=params,
                headers=headers,
            )
            response.raise_for_status()

            return Response(
                content=response.content,
                media_type="text/csv",
                headers={
                    "Content-Disposition": response.headers.get(
                        "Content-Disposition",
                        "attachment; filename=cosmicforge-completed-trades.csv"
                    ),
                    "Content-Type": "text/csv; charset=utf-8",
                }
            )
        except httpx.HTTPStatusError as e:
            raise HTTPException(status_code=e.response.status_code, detail=f"Export failed: {str(e)}")
        except httpx.RequestError as e:
            raise HTTPException(status_code=503, detail=f"Bot service unavailable: {str(e)}")


@router.get("/reconciliation")
async def get_reconciliation(
    days: int = Query(30, ge=1, le=365),
    broker_account_id: Optional[str] = Query(None),
    bot_instance_id: Optional[str] = Query(None),
    user: dict = Depends(get_current_active_user),
    token: str = Depends(oauth2_scheme),
):
    """
    Proxy: Reconcile broker equity movement vs closed-trade PnL derived from trade_fills.

    Read-only; user-scoped via JWT forwarded to bot-backend.
    """
    params: Dict[str, Any] = {"days": days}
    if broker_account_id:
        params["broker_account_id"] = broker_account_id
    if bot_instance_id:
        params["bot_instance_id"] = bot_instance_id

    headers = {"Authorization": f"Bearer {token}"}

    return await fetch_with_retries(
        f"{BOT_BACKEND_URL}/api/v1/analytics/reconciliation",
        params,
        headers,
    )

