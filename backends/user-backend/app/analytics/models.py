from pydantic import BaseModel, Field, validator
from typing import Optional, List, Dict, Any, Union
from decimal import Decimal
from datetime import datetime
from enum import Enum

class Currency(str, Enum):
    USDT = "USDT"
    USDC = "USDC"
    USD = "USD"
    EUR = "EUR"
    GBP = "GBP"
    BTC = "BTC"
    ETH = "ETH"
    # Add more as needed

class ReportPeriod(str, Enum):
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    ALL_TIME = "all_time"

class MonetaryValue(BaseModel):
    """
    Represents a monetary value in a specific currency.
    Used for all financial metrics to ensure currency context is never lost.
    """
    amount: Decimal
    currency: str = Field(default="USDT")

    class Config:
        json_encoders = {
            Decimal: lambda v: float(v)
        }

class PnLBreakdownItem(BaseModel):
    label: str
    pnl: MonetaryValue
    roi_percent: Optional[float] = None
    volume: Optional[MonetaryValue] = None
    trades_count: int = 0

class TradeMetric(BaseModel):
    """
    Aggregated trade statistics.
    """
    total_trades: int = 0
    winning_trades: int = 0
    losing_trades: int = 0
    win_rate: float = 0.0
    profit_factor: float = 0.0
    total_pnl: MonetaryValue
    total_volume: Optional[MonetaryValue] = None
    avg_trade_pnl: Optional[MonetaryValue] = None
    largest_win: Optional[MonetaryValue] = None
    largest_loss: Optional[MonetaryValue] = None

class PortfolioSnapshot(BaseModel):
    """
    A unified view of portfolio equity at a point in time.
    """
    timestamp: datetime
    total_equity: MonetaryValue
    unrealized_pnl: MonetaryValue
    wallet_balance: MonetaryValue
    margin_used: Optional[MonetaryValue] = None
    
    # Breakdown by broker/account
    accounts: List[Dict[str, Any]] = []

class AnalyticsContext(BaseModel):
    """
    Context for analytics requests, defining the target reporting currency
    and time range.
    """
    user_id: str
    reporting_currency: str = "USDT"
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
