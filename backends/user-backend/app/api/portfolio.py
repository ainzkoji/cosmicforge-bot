
from fastapi import APIRouter, Depends
from app.api.auth import get_current_user_id
from app.core.portfolio_service import get_portfolio_summary

router = APIRouter(tags=["Portfolio"])

@router.get("/summary")
def get_portfolio(user_id: str = Depends(get_current_user_id)):
    """
    Get aggregated portfolio data (Balance, PnL, Positions, History)
    from all connected brokers.
    """
    return get_portfolio_summary(user_id)

from app.core.transaction_service import get_portfolio_transactions

@router.get("/transactions")
def get_transactions(user_id: str = Depends(get_current_user_id)):
    """
    Get recent deposit/withdrawal transactions from all connected brokers.
    """
    return get_portfolio_transactions(user_id, limit=50)
