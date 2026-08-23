from pydantic import BaseModel, Field, validator
from typing import List, Optional, Dict, Any
from datetime import datetime

class BacktestCreate(BaseModel):
    """Initial request to start a backtest"""
    strategy_id: str = Field(..., description="Strategy identifier (e.g., 'MovingAverageCross')")
    name: str = Field(..., description="Human-readable name for this run")
    symbols: List[str] = Field(..., description="List of symbols to trade")
    interval: str = Field("1m", description="Candle timeframe")
    start_date: str = Field(..., description="Start date (ISO 8601)")
    end_date: str = Field(..., description="End date (ISO 8601)")
    initial_capital: float = Field(10000.0, gt=0)
    
    # Optional parameters
    strategy_params: Optional[Dict[str, Any]] = None
    risk_params: Optional[Dict[str, Any]] = None
    
    # Advanced settings (with defaults from config mostly, but overrideable)
    slippage_bps: Optional[float] = 10.0
    fee_bps: Optional[float] = 6.0
    market_type: str = "crypto"
    data_source: str = "binance"
    
    @validator("symbols")
    def validate_symbols(cls, v):
        if not v:
            raise ValueError("At least one symbol required")
        return [s.upper() for s in v]

class BacktestMetrics(BaseModel):
    """Performance metrics for a run"""
    total_trades: int = 0
    win_rate: float = 0.0
    net_pnl: float = 0.0
    gross_pnl: float = 0.0
    total_fees: float = 0.0
    max_drawdown: float = 0.0
    sharpe_ratio: Optional[float] = None
    return_pct: Optional[float] = None

class BacktestRun(BaseModel):
    """Backtest run details"""
    id: str
    user_id: str
    name: str
    strategy_id: str
    status: str
    created_at: str
    completed_at: Optional[str] = None
    
    # Config summary
    symbols: List[str]
    timeframe: str
    start_date: str
    end_date: str
    initial_capital: float
    
    # Result summary
    metrics: BacktestMetrics
    error_message: Optional[str] = None
    progress_pct: float = 0.0

class BacktestListResponse(BaseModel):
    """Paginated list of runs"""
    items: List[BacktestRun]
    total: int
    page: int
    size: int

class EquityPoint(BaseModel):
    """Single point in equity curve"""
    timestamp: str
    equity: float
    balance: float
    drawdown_pct: float
    unrealized_pnl: float

class EquityCurveResponse(BaseModel):
    """Full equity curve series"""
    run_id: str
    datapoints: List[EquityPoint]

class FillItem(BaseModel):
    """Single trade execution"""
    timestamp: str
    symbol: str
    side: str
    price: float
    quantity: float
    fee_usdt: float
    pnl: Optional[float] = None
    
class FillListResponse(BaseModel):
    """Paginated list of fills"""
    items: List[FillItem]
    total: int
    page: int
    size: int
