from __future__ import annotations
from dataclasses import dataclass
from typing import Optional
from shared_lib.persistence.db import DB

@dataclass
class HealthMetrics:
    win_rate: float = 0.5
    profit_factor: float = 1.0
    avg_r: float = 0.0
    trades: int = 0
    
class StrategyHealthMonitor:
    def __init__(self, db: DB):
        self.db = db

    def get_rolling_health(
        self,
        symbol: str,
        strategy: str = "default",
        limit: int = 20
    ) -> HealthMetrics:
        with self.db.connect() as conn:
            rows = conn.execute(
                """
                SELECT 
                    CASE WHEN realized_pnl > 0 THEN 1 ELSE 0 END as outcome, 
                    realized_pnl as pnl, 
                    r_multiple 
                FROM trade_fills 
                WHERE symbol = ? AND action = 'CLOSE'
                ORDER BY timestamp_utc DESC 
                LIMIT ?
                """,
                (symbol, limit)
            ).fetchall()

        if not rows:
            return HealthMetrics(0.0, 0.0, 0.0, 0)

        wins = 0
        total_profit = 0.0
        total_loss = 0.0
        total_r = 0.0
        count = len(rows)

        for r in rows:
            if r["outcome"] > 0: # 1=win
                wins += 1
            
            pnl = float(r["pnl"] or 0.0)
            if pnl > 0:
                total_profit += pnl
            else:
                total_loss += abs(pnl)
                
            total_r += float(r["r_multiple"] or 0.0)

        win_rate = wins / count
        profit_factor = (total_profit / total_loss) if total_loss > 0 else 999.0 # Infinity cap
        avg_r = total_r / count
        
        return HealthMetrics(win_rate, profit_factor, avg_r, count)
