from fastapi import APIRouter, Depends, Query, HTTPException
from typing import List, Optional
from app.api.auth import get_current_active_user
from app.persistence.db import DB
from app.core.strategy_service import StrategyService
from app.schemas.strategies import StrategyListResponse, Strategy

router = APIRouter()

def get_service():
    return StrategyService(DB())

@router.get("/", response_model=List[Strategy])
def list_marketplace_strategies(
    market_type: Optional[str] = Query(None, description="Filter by market type (crypto, forex)"),
    style: Optional[str] = Query(None, description="Filter by tag (trend, scalping)"),
    risk_style: Optional[str] = Query(None, description="Filter by risk (conservative, aggressive)"),
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0),
    user: dict = Depends(get_current_active_user),
    service: StrategyService = Depends(get_service)
):
    """
    List strategies available in the marketplace (Official + Community).
    excludes private strategies not owned by the user (managed by service layer).
    """
    filters = {}
    if market_type:
        filters["market_type"] = market_type
    if style:
        filters["tag"] = style
    if risk_style:
        filters["risk_style"] = risk_style
        
    strategies = service.list_strategies(
        user_id=user["id"],
        filters=filters,
        limit=limit,
        offset=offset
    )
    return strategies

@router.get("/{strategy_id}", response_model=Strategy)
def get_strategy_details(
    strategy_id: str,
    user: dict = Depends(get_current_active_user),
    service: StrategyService = Depends(get_service)
):
    """
    Get public details for a strategy.
    """
    strategy = service.get_strategy(strategy_id, user_id=user["id"])
    if not strategy:
        raise HTTPException(status_code=404, detail="Strategy not found")
        
    return strategy
