"""
Confidence Calibration System - Full Spec (C4)

Implements:
- Confidence bucketing from signal outcomes
- Per-bucket win rate, PnL, profit factor calculation
- Calibration gating rules (sample size, win rate, PF)
- Training mode fallback
"""
from __future__ import annotations
from dataclasses import dataclass
from typing import List, Dict, Optional, Tuple
from datetime import datetime

from shared_lib.persistence.db import DB


@dataclass
class ConfidenceBucket:
    """Statistics for a confidence bucket."""
    range_low: float    # e.g., 0.6
    range_high: float   # e.g., 0.7
    bucket_label: str   # e.g., "0.6-0.7"
    
    # Stats
    trade_count: int
    wins: int
    losses: int
    win_rate: float
    
    # PnL
    total_pnl: float
    avg_pnl: float
    
    # R-based metrics
    avg_r: float
    profit_factor: float
    expectancy: float  # avg_win * win_rate - avg_loss * loss_rate


@dataclass
class CalibrationResult:
    """Full calibration report for a strategy/symbol/timeframe."""
    strategy: str
    symbol: str
    timeframe: str
    
    total_trades: int
    overall_win_rate: float
    overall_profit_factor: float
    
    buckets: List[ConfidenceBucket]
    
    # Gating result
    min_tradeable_confidence: Optional[float]  # Lowest bucket that meets criteria
    is_calibrated: bool  # Has enough sample size


@dataclass
class CalibrationThresholds:
    """C4: Thresholds for calibration gating."""
    min_sample_size: int = 30       # Minimum trades per bucket
    min_win_rate: float = 0.65      # For Precision mode
    min_profit_factor: float = 1.2
    
    # Fallback for insufficient data
    fallback_min_confidence: float = 0.80  # Conservative when uncalibrated
    training_size_multiplier: float = 0.25  # Reduce size in training mode


class ConfidenceCalibrator:
    """
    Analyzes signal outcomes to calibrate confidence thresholds.
    
    Uses stored outcomes to determine:
    - Which confidence buckets are profitable
    - Minimum tradeable confidence per strategy/symbol
    """
    
    def __init__(
        self, 
        db: Optional[DB] = None,
        thresholds: Optional[CalibrationThresholds] = None,
    ):
        self.db = db or DB()
        self.thresholds = thresholds or CalibrationThresholds()
    
    def get_bucket_stats(
        self,
        strategy: Optional[str] = None,
        symbol: Optional[str] = None,
        timeframe: Optional[str] = None,
        bucket_count: int = 10,  # 0.0-0.1, 0.1-0.2, ..., 0.9-1.0
    ) -> List[ConfidenceBucket]:
        """
        Query signal_outcomes and bucket by confidence.
        """
        # Build query
        query = """
            SELECT 
                t.confidence,
                CASE WHEN f.realized_pnl > 0 THEN 1 ELSE 0 END as outcome,
                f.realized_pnl as pnl,
                f.r_multiple
            FROM trade_fills f
            JOIN trades t ON f.position_id = t.position_id
            WHERE f.action = 'CLOSE' AND t.confidence IS NOT NULL
        """
        params: List = []
        
        if strategy:
            query += " AND t.strategy = ?"
            params.append(strategy)
        if symbol:
            query += " AND f.symbol = ?"
            params.append(symbol)
        if timeframe:
            query += " AND t.timeframe = ?"
            params.append(timeframe)
        
        with self.db.connect() as conn:
            rows = conn.execute(query, params).fetchall()
        
        # Initialize buckets
        bucket_size = 1.0 / bucket_count
        buckets_data: Dict[int, Dict] = {}
        
        for i in range(bucket_count):
            buckets_data[i] = {
                "trades": 0,
                "wins": 0,
                "losses": 0,
                "pnl_list": [],
                "r_list": [],
            }
        
        # Fill buckets
        for row in rows:
            conf = float(row["confidence"])
            outcome = int(row["outcome"])
            pnl = float(row["pnl"]) if row["pnl"] else 0.0
            r_mult = float(row["r_multiple"]) if row["r_multiple"] else 0.0
            
            bucket_idx = min(bucket_count - 1, max(0, int(conf * bucket_count)))
            
            buckets_data[bucket_idx]["trades"] += 1
            if outcome == 1:
                buckets_data[bucket_idx]["wins"] += 1
            else:
                buckets_data[bucket_idx]["losses"] += 1
            buckets_data[bucket_idx]["pnl_list"].append(pnl)
            buckets_data[bucket_idx]["r_list"].append(r_mult)
        
        # Compute stats per bucket
        result: List[ConfidenceBucket] = []
        
        for i in range(bucket_count):
            data = buckets_data[i]
            range_low = i * bucket_size
            range_high = (i + 1) * bucket_size
            
            trades = data["trades"]
            wins = data["wins"]
            losses = data["losses"]
            pnl_list = data["pnl_list"]
            r_list = data["r_list"]
            
            win_rate = wins / trades if trades > 0 else 0.0
            total_pnl = sum(pnl_list)
            avg_pnl = total_pnl / trades if trades > 0 else 0.0
            avg_r = sum(r_list) / trades if trades > 0 else 0.0
            
            # Profit factor
            gross_profit = sum(p for p in pnl_list if p > 0)
            gross_loss = abs(sum(p for p in pnl_list if p < 0))
            profit_factor = gross_profit / gross_loss if gross_loss > 0 else (10.0 if gross_profit > 0 else 0.0)
            
            # Expectancy
            win_pnl_list = [p for p in pnl_list if p > 0]
            loss_pnl_list = [p for p in pnl_list if p < 0]
            avg_win = sum(win_pnl_list) / len(win_pnl_list) if win_pnl_list else 0
            avg_loss = abs(sum(loss_pnl_list) / len(loss_pnl_list)) if loss_pnl_list else 0
            loss_rate = 1 - win_rate
            expectancy = (avg_win * win_rate) - (avg_loss * loss_rate)
            
            result.append(ConfidenceBucket(
                range_low=range_low,
                range_high=range_high,
                bucket_label=f"{range_low:.1f}-{range_high:.1f}",
                trade_count=trades,
                wins=wins,
                losses=losses,
                win_rate=win_rate,
                total_pnl=total_pnl,
                avg_pnl=avg_pnl,
                avg_r=avg_r,
                profit_factor=profit_factor,
                expectancy=expectancy,
            ))
        
        return result
    
    def get_calibration_report(
        self,
        strategy: str,
        symbol: str,
        timeframe: str,
    ) -> CalibrationResult:
        """
        Generate full calibration report for a strategy/symbol/timeframe combo.
        """
        buckets = self.get_bucket_stats(strategy, symbol, timeframe)
        
        total_trades = sum(b.trade_count for b in buckets)
        total_wins = sum(b.wins for b in buckets)
        total_pnl = sum(b.total_pnl for b in buckets)
        
        overall_win_rate = total_wins / total_trades if total_trades > 0 else 0.0
        
        # Overall profit factor
        gross_profit = sum(b.total_pnl for b in buckets if b.total_pnl > 0)
        gross_loss = abs(float(sum(b.total_pnl for b in buckets if b.total_pnl < 0)))
        overall_pf = gross_profit / gross_loss if gross_loss > 0 else (10.0 if gross_profit > 0 else 0.0)
        
        # Find minimum tradeable confidence (C4 gating)
        min_tradeable = None
        is_calibrated = False
        
        for bucket in buckets:
            if bucket.trade_count >= self.thresholds.min_sample_size:
                is_calibrated = True
                if (bucket.win_rate >= self.thresholds.min_win_rate and
                    bucket.profit_factor >= self.thresholds.min_profit_factor):
                    min_tradeable = bucket.range_low
                    break
        
        return CalibrationResult(
            strategy=strategy,
            symbol=symbol,
            timeframe=timeframe,
            total_trades=total_trades,
            overall_win_rate=overall_win_rate,
            overall_profit_factor=overall_pf,
            buckets=buckets,
            min_tradeable_confidence=min_tradeable,
            is_calibrated=is_calibrated,
        )
    
    def should_allow_trade(
        self,
        strategy: str,
        symbol: str,
        timeframe: str,
        confidence: float,
        is_precision_mode: bool = True,
    ) -> Tuple[bool, str]:
        """
        C4: Calibration gating rule for production.
        
        Returns: (allowed, reason)
        """
        report = self.get_calibration_report(strategy, symbol, timeframe)
        
        # Not calibrated - use fallback
        if not report.is_calibrated:
            if confidence >= self.thresholds.fallback_min_confidence:
                return True, "uncalibrated_fallback_ok"
            return False, "uncalibrated_confidence_too_low"
        
        # Find bucket for this confidence
        bucket_idx = min(9, max(0, int(confidence * 10)))
        bucket = report.buckets[bucket_idx]
        
        # Check sample size
        if bucket.trade_count < self.thresholds.min_sample_size:
            # Fallback for this bucket
            if confidence >= self.thresholds.fallback_min_confidence:
                return True, "bucket_small_sample_fallback_ok"
            return False, "bucket_small_sample"
        
        # Check win rate threshold
        min_wr = self.thresholds.min_win_rate if is_precision_mode else 0.50
        if bucket.win_rate < min_wr:
            return False, f"bucket_win_rate_too_low:{bucket.win_rate:.2f}"
        
        # Check profit factor
        if bucket.profit_factor < self.thresholds.min_profit_factor:
            return False, f"bucket_pf_too_low:{bucket.profit_factor:.2f}"
        
        return True, "calibration_passed"
    
    def get_size_multiplier(
        self,
        strategy: str,
        symbol: str,
        timeframe: str,
        confidence: float,
    ) -> float:
        """
        Get position size multiplier based on calibration.
        
        - Uncalibrated: training_size_multiplier (reduced size)
        - Calibrated with good stats: 1.0
        - Calibrated with edge stats: up to 1.2
        """
        report = self.get_calibration_report(strategy, symbol, timeframe)
        
        if not report.is_calibrated:
            return self.thresholds.training_size_multiplier
        
        bucket_idx = min(9, max(0, int(confidence * 10)))
        bucket = report.buckets[bucket_idx]
        
        if bucket.trade_count < self.thresholds.min_sample_size:
            return self.thresholds.training_size_multiplier
        
        # Scale based on bucket quality
        if bucket.win_rate >= 0.75 and bucket.profit_factor >= 2.0:
            return 1.2
        elif bucket.win_rate >= 0.65:
            return 1.0
        elif bucket.win_rate >= 0.50:
            return 0.75
        else:
            return 0.5
