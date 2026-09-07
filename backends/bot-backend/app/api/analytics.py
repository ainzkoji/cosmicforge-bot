"""
Analytics API - Endpoints for global trading performance.

Provides aggregated stats, strategy leaderboards, and confidence calibration.

TIMEZONE: All timestamps use UTC (datetime.utcnow()). 
          Database stores timestamps in ISO format (YYYY-MM-DDTHH:MM:SS).
          Timeframe filtering uses UTC boundaries.
"""
from fastapi import APIRouter, Depends, Query, HTTPException
from fastapi.responses import StreamingResponse, Response
from typing import List, Optional, Dict, Any
from pydantic import BaseModel
from enum import Enum
import io
import json

from shared_lib.persistence.global_analytics import get_global_analytics
from app.core.auth import get_current_active_user

router = APIRouter(tags=["Analytics"])

# --- Timeframe Enum ---

class Timeframe(str, Enum):
    """Supported timeframe filters for analytics."""
    ONE_MONTH = "1M"
    THREE_MONTHS = "3M"
    YEAR_TO_DATE = "YTD"
    ALL_TIME = "ALL"
    
    @classmethod
    def validate(cls, value: str) -> str:
        """Validate and normalize timeframe string."""
        value = value.upper()
        valid_values = [e.value for e in cls]
        if value not in valid_values:
            raise ValueError(
                f"Invalid timeframe '{value}'. Must be one of: {', '.join(valid_values)}"
            )
        return value

# --- Response Models ---

class MonthlyPnLItem(BaseModel):
    month: str
    value: float

class RiskMetrics(BaseModel):
    max_drawdown: float = 0.0
    volatility: float = 0.0 # Deprecated, use volatility_30d
    volatility_30d: float = 0.0 # New standard
    sortino_ratio: float = 0.0
    alpha: float = 0.0
    sharpe_ratio: float = 0.0 # Moved inside risk metrics

class AssetAllocationItem(BaseModel):
    label: str # Deprecated, use symbol
    symbol: str = "Unknown" # New standard
    value: float = 0.0 # Deprecated, use value_usdt
    value_usdt: float = 0.0 # New standard
    percent: float = 0.0 # New standard
    color: str = "#888888"

class EquityCurvePoint(BaseModel):
    timestamp: str
    equity: float

class OverviewStats(BaseModel):
    total_profit: float
    total_trades: int
    wins: int = 0
    losses: int = 0
    win_rate: float
    profit_factor: float
    sharpe_ratio: float = 0.0
    profit_change_pct: float = 0.0
    monthly_pnl: List[MonthlyPnLItem] = []
    risk_metrics: Optional[RiskMetrics] = None
    asset_allocation: List[AssetAllocationItem] = []
    equity_curve: List[EquityCurvePoint] = []

class StrategyPerfItem(BaseModel):
    strategy: str
    symbol: str
    net_pnl: float
    win_rate: float
    total_trades: int
    profit_factor: float

class CalibrationItem(BaseModel):
    bucket_low: float
    bucket_high: float
    count: int
    win_rate: float
    avg_pnl: float

@router.get("/overview", response_model=OverviewStats)
def get_overview(
    environment: str = Query("PAPER", description="Environment (LIVE/PAPER)"),
    timeframe: str = Query("ALL", description="Timeframe filter: 1M (30 days), 3M (90 days), YTD (year-to-date), ALL (all time)"),
    user: dict = Depends(get_current_active_user)
):
    """
    Get high-level portfolio overview statistics with real-time data.
    
    **Timeframe Filtering**:
    - `1M`: Last 30 days from current UTC time
    - `3M`: Last 90 days from current UTC time  
    - `YTD`: From January 1 of current year (UTC) to now
    - `ALL`: Complete trading history (no date filter)
    
    **Timezone**: All timestamps use UTC. Filtering is based on `exit_time` field in trades table.
    
    **Error Codes**:
    - 422: Invalid timeframe parameter
    - 500: Database or calculation error
    """
    import logging
    logger = logging.getLogger(__name__)
    
    # Validate timeframe
    try:
        timeframe = Timeframe.validate(timeframe)
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))
    
    from shared_lib.persistence.analytics_service import AnalyticsService
    
    user_id = user.get("id")
    
    try:
        analytics = AnalyticsService()
        
        # Get all data with timeframe filtering
        stats = analytics.get_total_stats(user_id, timeframe)
        monthly_data = analytics.get_monthly_pnl(user_id, timeframe, months=12)
        allocation_data = analytics.get_asset_allocation(user_id)
        risk_metrics = analytics.get_risk_metrics(user_id, timeframe, window_days=30)
        equity_curve = analytics.get_equity_curve(user_id, timeframe)
        
        return OverviewStats(
            total_profit=round(stats["total_profit"], 2),
            total_trades=stats["total_trades"],
            wins=stats["wins"],
            losses=stats["losses"],
            win_rate=round(stats["win_rate"], 1),
            profit_factor=round(stats["profit_factor"], 2),
            sharpe_ratio=round(stats["sharpe_ratio"], 2),
            profit_change_pct=round(stats["profit_change_pct"], 1),
            monthly_pnl=[MonthlyPnLItem(**i) for i in monthly_data],
            risk_metrics=RiskMetrics(
                max_drawdown=risk_metrics["max_drawdown"],
                volatility=risk_metrics["volatility_30d"],
                volatility_30d=risk_metrics["volatility_30d"],
                sortino_ratio=risk_metrics["sortino_ratio"],
                alpha=risk_metrics["alpha"],
                sharpe_ratio=risk_metrics["sharpe_ratio"]
            ),
            asset_allocation=[AssetAllocationItem(**i) for i in allocation_data],
            equity_curve=[EquityCurvePoint(**i) for i in equity_curve]
        )
    except Exception as e:
        logger.error(f"Failed to generate analytics overview for user {user_id}, timeframe {timeframe}: {e}", exc_info=True)
        raise HTTPException(
            status_code=500, 
            detail=f"Analytics calculation failed: {str(e)}"
        )


@router.get("/leaderboard", response_model=List[StrategyPerfItem])
def get_max_leaderboard(
    environment: str = Query("PAPER"),
    limit: int = 20,
    user: dict = Depends(get_current_active_user)
):
    """Get top performing strategies."""
    ga = get_global_analytics()
    rows = ga.get_strategy_leaderboard(environment=environment, limit=limit)
    
    # Map row dicts to model
    return [
        StrategyPerfItem(
            strategy=r["strategy"],
            symbol=r["symbol"],
            net_pnl=r["net_pnl"],
            win_rate=r["win_rate"],
            total_trades=r["total_trades"],
            profit_factor=r["profit_factor"]
        )
        for r in rows
    ]

@router.get("/calibration", response_model=List[CalibrationItem])
def get_calibration(
    environment: str = Query("PAPER"),
    strategy: Optional[str] = None,
    user: dict = Depends(get_current_active_user)
):
    """Get confidence calibration buckets."""
    ga = get_global_analytics()
    rows = ga.get_confidence_calibration(
        strategy=strategy,
        environment=environment
    )
    
    return [
        CalibrationItem(
            bucket_low=r["bucket_low"],
            bucket_high=r["bucket_high"],
            count=r["count"],
            win_rate=r["win_rate"],
            avg_pnl=r["avg_pnl"]
        )
        for r in rows
    ]


@router.get("/export")
async def export_analytics(
    timeframe: str = Query("ALL", description="Timeframe filter"),
    format: str = Query("csv", description="Export format: csv, xlsx, pdf"),
    user: dict = Depends(get_current_active_user)
):
    """
    Export analytics data in specified format.
    """
    import pandas as pd
    from shared_lib.persistence.analytics_service import AnalyticsService
    import logging
    
    logger = logging.getLogger(__name__)
    logger.info(f"Export endpoint hit. Timeframe: {timeframe}, Format: {format}, User: {user.get('id')}")
    
    try:
        try:
            timeframe = Timeframe.validate(timeframe)
        except ValueError as e:
            logger.error(f"Timeframe validation failed: {e}")
            raise HTTPException(status_code=422, detail=str(e))
            
        user_id = user.get("id")
        logger.info(f"Initializing AnalyticsService for user {user_id}")
        analytics = AnalyticsService()
        
        # Fetch data
        logger.info("Fetching raw trades...")
        stats = analytics.get_total_stats(user_id, timeframe)
        raw_trades = analytics.get_raw_trades(user_id, timeframe)
        logger.info(f"Fetched {len(raw_trades)} trades")
        
        # Create DataFrame for trades
        df = pd.DataFrame(raw_trades) if raw_trades else pd.DataFrame()
        logger.info(f"DataFrame created. Shape: {df.shape}")
        
        # Prepare filename
        timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
        filename = f"analytics_{timeframe}_{timestamp}"
        
        stream = io.BytesIO()
        media_type = "text/csv"
        
        logger.info(f"Processing format: {format}")
        
        if format.lower() == "csv":
            # safer: generate string -> encode -> write to BytesIO
            csv_str = df.to_csv(index=False)
            stream.write(csv_str.encode('utf-8'))
            media_type = "text/csv"
            filename += ".csv"
            
        elif format.lower() == "xlsx":
            try:
                import openpyxl
            except ImportError:
                logger.error("openpyxl not installed")
                raise HTTPException(status_code=500, detail="openpyxl not installed")
                
            with pd.ExcelWriter(stream, engine='openpyxl') as writer:
                # Summary Sheet
                summary_data = [{"Metric": k, "Value": v} for k, v in stats.items()]
                pd.DataFrame(summary_data).to_excel(writer, sheet_name="Summary", index=False)
                
                # Trades Sheet
                df.to_excel(writer, sheet_name="Trades", index=False)
                
            media_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            filename += ".xlsx"
            
        elif format.lower() == "pdf":
            try:
                from reportlab.lib import colors
                from reportlab.lib.pagesizes import letter, landscape
                from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
                from reportlab.lib.styles import getSampleStyleSheet
            except ImportError:
                logger.error("reportlab not installed")
                raise HTTPException(status_code=500, detail="reportlab not installed")
                
            doc = SimpleDocTemplate(stream, pagesize=landscape(letter))
            elements = []
            styles = getSampleStyleSheet()
            
            # Title
            elements.append(Paragraph(f"Analytics Report ({timeframe})", styles['Title']))
            elements.append(Spacer(1, 12))
            
            # Summary Table
            elements.append(Paragraph("Summary Metrics", styles['Heading2']))
            summary_data = [["Metric", "Value"]]
            for k, v in stats.items():
                summary_data.append([k.replace("_", " ").title(), str(v)])
                
            t = Table(summary_data)
            t.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                ('GRID', (0, 0), (-1, -1), 1, colors.black),
            ]))
            elements.append(t)
            elements.append(Spacer(1, 20))
            
            # Trades Table (First 50 only to keep PDF manageable)
            elements.append(Paragraph("Recent Trades (Top 50)", styles['Heading2']))
            if not df.empty:
                trades_data = [list(df.columns)] + df.head(50).values.tolist()
                # Convert all to strings
                trades_data = [[str(cell) for cell in row] for row in trades_data]
                
                t2 = Table(trades_data, repeatRows=1)
                t2.setStyle(TableStyle([
                    ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
                    ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                    ('FONTSIZE', (0, 0), (-1, -1), 8),
                    ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
                ]))
                elements.append(t2)
            else:
                elements.append(Paragraph("No trades found.", styles['Normal']))
                
            doc.build(elements)
            media_type = "application/pdf"
            filename += ".pdf"
            
        elif format.lower() == "json":
            # Export raw trades as JSON
            stream.write(df.to_json(orient="records", date_format="iso").encode('utf-8'))
            media_type = "application/json"
            filename += ".json"
            
        else:
            raise HTTPException(status_code=422, detail=f"Unsupported format: {format}")
            
        stream.seek(0)
        logger.info(f"Export successful. Filename: {filename}")
        
        headers = {
            "Content-Disposition": f"attachment; filename={filename}",
            "Content-Length": str(stream.getbuffer().nbytes)
        }
        
        return Response(
            content=stream.getvalue(),
            media_type=media_type,
            headers=headers
        )
        
    except Exception as e:
        logger.error(f"CRITICAL ERROR in export_analytics: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Export failed: {str(e)}")


@router.get("/tax-report")
def get_tax_report(
    year: int = Query(..., description="Tax year (e.g. 2023)"),
    bot_instance_id: Optional[str] = Query(None),
    broker_account_id: Optional[str] = Query(None),
    format: str = Query("json", pattern="^(json|csv)$"),
    user: dict = Depends(get_current_active_user)
):
    """
    Get tax report for a specific year.
    Supports execution-based reporting (Phase 1).
    """
    from app.analytics.tax_report_service import get_tax_report_service
    
    service = get_tax_report_service()
    user_id = user.get("id")
    
    try:
        if format == "csv":
            csv_content = service.export_tax_report_csv(
                user_id=user_id,
                tax_year=year,
                broker_account_id=broker_account_id
            )
            return Response(
                content=csv_content,
                media_type="text/csv",
                headers={
                    "Content-Disposition": f"attachment; filename=tax_report_{year}.csv"
                }
            )
        else:
            # JSON
            report = service.get_execution_based_tax_report(
                user_id=user_id,
                tax_year=year,
                broker_account_id=broker_account_id
            )
            return report
            
    except Exception as e:
        import logging
        logger = logging.getLogger(__name__)
        logger.error(f"Failed to generate tax report: {e}")
        raise HTTPException(status_code=500, detail=str(e))
