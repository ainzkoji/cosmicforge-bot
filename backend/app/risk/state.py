from __future__ import annotations
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Optional

@dataclass
class DailyRiskState:
    day: date
    realized_pnl: float = 0.0
    kill: bool = False
    trade_count: int = 0
    
@dataclass
class PeriodSnapshot:
    start_date: date
    start_equity: float
    peak_equity: float
    low_equity: float
    
    @property
    def current_drawdown(self) -> float:
        """Drawdown from peak equity"""
        if self.peak_equity <= 0:
            return 0.0
        # If low_equity is used for max historical DD, that's different.
        # But we mostly care about *current* DD vs peak.
        # This dataclass holds snapshot state. Implementation logic will compare current equity to this peak.
        return 0.0 

@dataclass
class RiskState:
    daily: DailyRiskState
    weekly: Optional[PeriodSnapshot] = None
    monthly: Optional[PeriodSnapshot] = None
    
    # Live metrics (not persisted in risk tables, but passed from runner)
    open_positions: int = 0
    current_equity: float = 0.0
    
    # Strategy Health (Layer E)
    health: Optional['HealthMetrics'] = None


def get_week_start(d: date) -> date:
    """Return Monday of the week for date d"""
    return d - timedelta(days=d.weekday())

def get_month_start(d: date) -> date:
    """Return 1st of the month for date d"""
    return d.replace(day=1)
