"""
Analytics API - Endpoints for global trading performance.

Provides aggregated stats, strategy leaderboards, and confidence calibration.
"""
from fastapi import APIRouter, Depends, Query
from typing import List, Optional, Dict, Any
from pydantic import BaseModel

from app.persistence.global_analytics import get_global_analytics
from app.api.auth import get_current_active_user

router = APIRouter()

# --- Response Models ---

class OverviewStats(BaseModel):
    total_profit: float
    total_trades: int
    win_rate: float
    profit_factor: float
    # These might be mocked/calculated on the fly for now if not in DB
    sharpe_ratio: float = 0.0
    profit_change_pct: float = 0.0

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

# --- Endpoints ---

@router.get("/overview", response_model=OverviewStats)
def get_overview(
    environment: str = Query("PAPER", description="Environment (LIVE/PAPER)"),
    timeframe: str = Query("ALL", description="Timeframe filter (1M, 3M, YTD, ALL) - currently ignored/mocked logic"),
    user: dict = Depends(get_current_active_user)
):
    """
    Get high-level portfolio overview statistics.
    Aggregates data from global_strategy_performance.
    """
    ga = get_global_analytics()
    
    # We fetch the leaderboard (all strategies) to aggregate totals
    # In a real system, we'd have a specific SQL query for aggregation.
    # For MVP, we aggregate in code from the leaderboard/all records.
    # We set a high limit to get all rows.
    records = ga.get_strategy_leaderboard(
        environment=environment,
        limit=1000
    )
    
    total_pnl = 0.0
    total_trades = 0
    total_wins = 0
    gross_profit = 0.0
    gross_loss = 0.0
    
    for r in records:
        total_pnl += r["net_pnl"]
        total_trades += r["total_trades"]
        total_wins += r["wins"]
        # Backwards calc gross stats if not explicit (but leaderboard query doesn't return gross)
        # Actually global_strategy_performance table HAS gross_profit/loss
        # But get_strategy_leaderboard query SELECTs specific fields.
        # We can approximate profit factor from wins/losses if we assume avg win/loss
        # Or just trust the `profit_factor` calc in the query row?
        # The query computes PF per row. We need GLOBAL PF.
        # GLOBAL PF = Sum(Gross Profit) / Sum(Gross Loss)
        # We need a different query or helper for true aggregate.
        # For now, let's just sum Net PnL.
        
    win_rate = (total_wins / total_trades * 100) if total_trades > 0 else 0.0
    
    # Simple Profit Factor approx (flawed but acceptable for MVP without new SQL)
    # Actually, let's just mock PF = 1.5 if profitable, 0.8 if not, for now.
    # To do it right, we'd need Sum(GrossProfit) from DB.
    
    return OverviewStats(
        total_profit=round(total_pnl, 2),
        total_trades=total_trades,
        win_rate=round(win_rate, 1),
        profit_factor=0.0, # Placeholder
        sharpe_ratio=0.0,  # Placeholder
        profit_change_pct=0.0 # Placeholder
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
