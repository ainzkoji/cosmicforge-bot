"""
Reporting API Endpoints

Exposes comprehensive reporting services via REST API:
- P&L reporting (realized, unrealized, breakdowns)
- Trade statistics (win rate, profit factor, performance)
- Drawdown analysis (equity curve, max drawdown)
- Tax reports (execution-based with CSV/JSON export)
- Benchmarking (Sharpe ratio, market comparison)
"""
from fastapi import APIRouter, Depends, HTTPException, Query, Response
from typing import Optional
from datetime import datetime, timezone, timedelta
from app.core.auth import get_current_active_user
from app.analytics.pnl_service import get_pnl_service
from app.analytics.trade_stats_service import get_trade_stats_service
from app.analytics.drawdown_service import get_drawdown_service
from app.analytics.tax_report_service import get_tax_report_service
from app.analytics.benchmark_service import get_benchmark_service
from app.analytics.pdf_report_service import get_pdf_report_service
import logging

logger = logging.getLogger(__name__)

router = APIRouter()


# ============================================================================
# P&L ENDPOINTS
# ============================================================================

@router.get("/pnl/realized")
async def get_realized_pnl(
    broker_account_id: Optional[str] = Query(None),
    bot_instance_id: Optional[str] = Query(None),
    symbol: Optional[str] = Query(None),
    days: Optional[int] = Query(None, ge=1, le=365),
    current_user: dict = Depends(get_current_active_user)
):
    """Get realized P&L from closed trades"""
    user_id = current_user.get("id")
    
    start_date = None
    end_date = None
    if days:
        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=days)
    
    try:
        service = get_pnl_service()
        result = service.get_realized_pnl(
            user_id=user_id,
            broker_account_id=broker_account_id,
            bot_instance_id=bot_instance_id,
            symbol=symbol,
            start_date=start_date,
            end_date=end_date
        )
        return result
    except Exception as e:
        logger.error(f"Failed to get realized P&L: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/pnl/unrealized")
async def get_unrealized_pnl(
    broker_account_id: Optional[str] = Query(None),
    current_user: dict = Depends(get_current_active_user)
):
    """Get current unrealized P&L from open positions"""
    user_id = current_user.get("id")
    
    try:
        service = get_pnl_service()
        result = service.get_unrealized_pnl(
            user_id=user_id,
            broker_account_id=broker_account_id
        )
        return result
    except Exception as e:
        logger.error(f"Failed to get unrealized P&L: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/pnl/total")
async def get_total_pnl(
    broker_account_id: Optional[str] = Query(None),
    bot_instance_id: Optional[str] = Query(None),
    days: Optional[int] = Query(None, ge=1, le=365),
    current_user: dict = Depends(get_current_active_user)
):
    """Get total P&L (realized + unrealized)"""
    user_id = current_user.get("id")
    
    start_date = None
    end_date = None
    if days:
        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=days)
    
    try:
        service = get_pnl_service()
        result = service.get_total_pnl(
            user_id=user_id,
            broker_account_id=broker_account_id,
            bot_instance_id=bot_instance_id,
            start_date=start_date,
            end_date=end_date
        )
        return result
    except Exception as e:
        logger.error(f"Failed to get total P&L: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/pnl/breakdown")
async def get_pnl_breakdown(
    group_by: str = Query("broker_account", pattern="^(broker_account|bot_instance|symbol)$"),
    days: Optional[int] = Query(None, ge=1, le=365),
    current_user: dict = Depends(get_current_active_user)
):
    """Get P&L breakdown by account, bot, or symbol"""
    user_id = current_user.get("id")
    
    start_date = None
    end_date = None
    if days:
        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=days)
    
    try:
        service = get_pnl_service()
        result = service.get_pnl_breakdown(
            user_id=user_id,
            group_by=group_by,
            start_date=start_date,
            end_date=end_date
        )
        return result
    except Exception as e:
        logger.error(f"Failed to get P&L breakdown: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# TRADE STATISTICS ENDPOINTS
# ============================================================================

@router.get("/stats/win-rate")
async def get_win_rate(
    broker_account_id: Optional[str] = Query(None),
    bot_instance_id: Optional[str] = Query(None),
    symbol: Optional[str] = Query(None),
    days: Optional[int] = Query(None, ge=1, le=365),
    current_user: dict = Depends(get_current_active_user)
):
    """Get win rate statistics"""
    user_id = current_user.get("id")
    
    start_date = None
    end_date = None
    if days:
        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=days)
    
    try:
        service = get_trade_stats_service()
        result = service.get_win_rate(
            user_id=user_id,
            broker_account_id=broker_account_id,
            bot_instance_id=bot_instance_id,
            symbol=symbol,
            start_date=start_date,
            end_date=end_date
        )
        return result
    except Exception as e:
        logger.error(f"Failed to get win rate: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/stats/summary")
async def get_trade_summary(
    broker_account_id: Optional[str] = Query(None),
    bot_instance_id: Optional[str] = Query(None),
    days: Optional[int] = Query(None, ge=1, le=365),
    current_user: dict = Depends(get_current_active_user)
):
    """Get comprehensive trade summary with all statistics"""
    user_id = current_user.get("id")
    
    start_date = None
    end_date = None
    if days:
        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=days)
    
    try:
        service = get_trade_stats_service()
        result = service.get_trade_summary(
            user_id=user_id,
            broker_account_id=broker_account_id,
            bot_instance_id=bot_instance_id,
            start_date=start_date,
            end_date=end_date
        )
        return result
    except Exception as e:
        logger.error(f"Failed to get trade summary: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/stats/best-worst")
async def get_best_worst_trades(
    broker_account_id: Optional[str] = Query(None),
    bot_instance_id: Optional[str] = Query(None),
    limit: int = Query(10, ge=1, le=50),
    current_user: dict = Depends(get_current_active_user)
):
    """Get best and worst performing trades"""
    user_id = current_user.get("id")
    
    try:
        service = get_trade_stats_service()
        result = service.get_best_worst_trades(
            user_id=user_id,
            broker_account_id=broker_account_id,
            bot_instance_id=bot_instance_id,
            limit=limit
        )
        return result
    except Exception as e:
        logger.error(f"Failed to get best/worst trades: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/stats/by-symbol")
async def get_symbol_performance(
    broker_account_id: Optional[str] = Query(None),
    bot_instance_id: Optional[str] = Query(None),
    days: Optional[int] = Query(None, ge=1, le=365),
    current_user: dict = Depends(get_current_active_user)
):
    """Get performance breakdown by symbol"""
    user_id = current_user.get("id")
    
    start_date = None
    end_date = None
    if days:
        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=days)
    
    try:
        service = get_trade_stats_service()
        result = service.get_symbol_performance(
            user_id=user_id,
            broker_account_id=broker_account_id,
            bot_instance_id=bot_instance_id,
            start_date=start_date,
            end_date=end_date
        )
        return result
    except Exception as e:
        logger.error(f"Failed to get symbol performance: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/stats/time-series")
async def get_time_series_performance(
    broker_account_id: Optional[str] = Query(None),
    bot_instance_id: Optional[str] = Query(None),
    interval: str = Query("daily", pattern="^(daily|weekly|monthly)$"),
    days: int = Query(30, ge=1, le=365),
    current_user: dict = Depends(get_current_active_user)
):
    """Get cumulative P&L over time"""
    user_id = current_user.get("id")
    
    try:
        service = get_trade_stats_service()
        result = service.get_time_series_performance(
            user_id=user_id,
            broker_account_id=broker_account_id,
            bot_instance_id=bot_instance_id,
            interval=interval,
            days=days
        )
        return result
    except Exception as e:
        logger.error(f"Failed to get time series performance: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# DRAWDOWN ENDPOINTS
# ============================================================================

@router.get("/drawdown/equity-curve")
async def get_equity_curve(
    broker_account_id: Optional[str] = Query(None),
    bot_instance_id: Optional[str] = Query(None),
    days: int = Query(30, ge=1, le=365),
    current_user: dict = Depends(get_current_active_user)
):
    """Get equity curve time series"""
    user_id = current_user.get("id")
    
    try:
        service = get_drawdown_service()
        result = service.get_equity_curve(
            user_id=user_id,
            broker_account_id=broker_account_id,
            bot_instance_id=bot_instance_id,
            days=days
        )
        return result
    except Exception as e:
        logger.error(f"Failed to get equity curve: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/drawdown/max")
async def get_max_drawdown(
    broker_account_id: Optional[str] = Query(None),
    bot_instance_id: Optional[str] = Query(None),
    days: int = Query(90, ge=1, le=365),
    current_user: dict = Depends(get_current_active_user)
):
    """Get maximum drawdown over period"""
    user_id = current_user.get("id")
    
    try:
        service = get_drawdown_service()
        result = service.get_max_drawdown(
            user_id=user_id,
            broker_account_id=broker_account_id,
            bot_instance_id=bot_instance_id,
            days=days
        )
        return result
    except Exception as e:
        logger.error(f"Failed to get max drawdown: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/drawdown/current")
async def get_current_drawdown(
    broker_account_id: Optional[str] = Query(None),
    current_user: dict = Depends(get_current_active_user)
):
    """Get current drawdown from peak"""
    user_id = current_user.get("id")
    
    try:
        service = get_drawdown_service()
        result = service.get_current_drawdown(
            user_id=user_id,
            broker_account_id=broker_account_id
        )
        return result
    except Exception as e:
        logger.error(f"Failed to get current drawdown: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/drawdown/periods")
async def get_drawdown_periods(
    broker_account_id: Optional[str] = Query(None),
    days: int = Query(90, ge=1, le=365),
    current_user: dict = Depends(get_current_active_user)
):
    """Get all drawdown periods with recovery times"""
    user_id = current_user.get("id")
    
    try:
        service = get_drawdown_service()
        result = service.get_drawdown_periods(
            user_id=user_id,
            broker_account_id=broker_account_id,
            days=days
        )
        return result
    except Exception as e:
        logger.error(f"Failed to get drawdown periods: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# TAX REPORT ENDPOINTS
# ============================================================================

@router.get("/tax/report/{tax_year}")
async def get_tax_report(
    tax_year: int,
    broker_account_id: Optional[str] = Query(None),
    current_user: dict = Depends(get_current_active_user)
):
    """Get execution-based tax report for a tax year"""
    user_id = current_user.get("id")
    
    # Validate tax year
    current_year = datetime.now(timezone.utc).year
    if tax_year < 2020 or tax_year > current_year:
        raise HTTPException(
            status_code=400,
            detail=f"Tax year must be between 2020 and {current_year}"
        )
    
    try:
        service = get_tax_report_service()
        result = service.get_execution_based_tax_report(
            user_id=user_id,
            tax_year=tax_year,
            broker_account_id=broker_account_id
        )
        return result
    except Exception as e:
        logger.error(f"Failed to generate tax report: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/tax/export/{tax_year}/csv")
async def export_tax_report_csv(
    tax_year: int,
    broker_account_id: Optional[str] = Query(None),
    current_user: dict = Depends(get_current_active_user)
):
    """Export tax report as CSV"""
    user_id = current_user.get("id")
    
    # Validate tax year
    current_year = datetime.now(timezone.utc).year
    if tax_year < 2020 or tax_year > current_year:
        raise HTTPException(
            status_code=400,
            detail=f"Tax year must be between 2020 and {current_year}"
        )
    
    try:
        service = get_tax_report_service()
        csv_content = service.export_tax_report_csv(
            user_id=user_id,
            tax_year=tax_year,
            broker_account_id=broker_account_id
        )
        
        return Response(
            content=csv_content,
            media_type="text/csv",
            headers={
                "Content-Disposition": f"attachment; filename=tax_report_{tax_year}.csv"
            }
        )
    except Exception as e:
        logger.error(f"Failed to export tax report CSV: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/tax/export/{tax_year}/json")
async def export_tax_report_json(
    tax_year: int,
    broker_account_id: Optional[str] = Query(None),
    current_user: dict = Depends(get_current_active_user)
):
    """Export tax report as JSON"""
    user_id = current_user.get("id")
    
    # Validate tax year
    current_year = datetime.now(timezone.utc).year
    if tax_year < 2020 or tax_year > current_year:
        raise HTTPException(
            status_code=400,
            detail=f"Tax year must be between 2020 and {current_year}"
        )
    
    try:
        service = get_tax_report_service()
        json_content = service.export_tax_report_json(
            user_id=user_id,
            tax_year=tax_year,
            broker_account_id=broker_account_id
        )
        
        return Response(
            content=json_content,
            media_type="application/json",
            headers={
                "Content-Disposition": f"attachment; filename=tax_report_{tax_year}.json"
            }
        )
    except Exception as e:
        logger.error(f"Failed to export tax report JSON: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/tax/export/{tax_year}/pdf")
async def export_tax_report_pdf(
    tax_year: int,
    broker_account_id: Optional[str] = Query(None),
    current_user: dict = Depends(get_current_active_user)
):
    """Export tax report as PDF"""
    user_id = current_user.get("id")
    
    # Validate tax year
    current_year = datetime.now(timezone.utc).year
    if tax_year < 2020 or tax_year > current_year:
        raise HTTPException(
            status_code=400,
            detail=f"Tax year must be between 2020 and {current_year}"
        )
    
    try:
        service = get_pdf_report_service()
        pdf_bytes = service.generate_tax_report_pdf(
            user_id=user_id,
            tax_year=tax_year,
            broker_account_id=broker_account_id
        )
        
        return Response(
            content=pdf_bytes,
            media_type="application/pdf",
            headers={
                "Content-Disposition": f"attachment; filename=tax_report_{tax_year}.pdf"
            }
        )
    except Exception as e:
        logger.error(f"Failed to export tax report PDF: {e}")
        raise HTTPException(status_code=500, detail=str(e))



# ============================================================================
# BENCHMARKING ENDPOINTS
# ============================================================================

@router.get("/benchmark/available")
async def get_available_benchmarks(
    current_user: dict = Depends(get_current_active_user)
):
    """Get list of available benchmark symbols"""
    try:
        service = get_benchmark_service()
        result = service.get_available_benchmarks()
        return {"benchmarks": result}
    except Exception as e:
        logger.error(f"Failed to get available benchmarks: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/benchmark/comparison")
async def get_benchmark_comparison(
    benchmark_symbol: str = Query("BTCUSDT"),
    broker_account_id: Optional[str] = Query(None),
    days: int = Query(30, ge=1, le=365),
    current_user: dict = Depends(get_current_active_user)
):
    """Compare bot performance against benchmark"""
    user_id = current_user.get("id")
    
    try:
        service = get_benchmark_service()
        result = service.get_benchmark_comparison(
            user_id=user_id,
            benchmark_symbol=benchmark_symbol,
            broker_account_id=broker_account_id,
            days=days
        )
        return result
    except Exception as e:
        logger.error(f"Failed to get benchmark comparison: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/benchmark/sharpe-ratio")
async def get_sharpe_ratio(
    broker_account_id: Optional[str] = Query(None),
    days: int = Query(30, ge=1, le=365),
    risk_free_rate: float = Query(0.0, ge=0.0, le=0.1),
    current_user: dict = Depends(get_current_active_user)
):
    """Calculate Sharpe ratio (risk-adjusted return)"""
    user_id = current_user.get("id")
    
    try:
        service = get_benchmark_service()
        result = service.get_sharpe_ratio(
            user_id=user_id,
            broker_account_id=broker_account_id,
            days=days,
            risk_free_rate=risk_free_rate
        )
        return result
    except Exception as e:
        logger.error(f"Failed to calculate Sharpe ratio: {e}")
        raise HTTPException(status_code=500, detail=str(e))
