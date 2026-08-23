"""
Strategy Performance Tracker
============================
Phase 4: Controlled Adaptive Learning (Strategy Weights)

Provides deterministically reconstructed, bounded, and smoothed strategy
weight adjustments based on recent execution outcomes.
Replaces the old in-memory _PerformanceTracker from MasterEnsembleStrategy.
"""
import logging
from collections import deque
from typing import Dict, List, Optional, Tuple

from shared_lib.persistence.db import DB

logger = logging.getLogger(__name__)

class StrategyPerformanceTracker:
    """
    Centralized tracker for strategy performance.
    Computes smoothed weight multipliers based on rolling win rates.
    
    Rules:
    - Min samples required: 10
    - Lookback window: last 20 trades per strategy
    - Reference win rate: 55%
    - Multiplier bounds: [0.70, 1.30]
    - Smoothing (Hysteresis): limits shifts to max 0.05 per trade.
    """
    
    WINDOW = 20
    MIN_SAMPLES = 10
    REF_WIN_RATE = 0.55
    MIN_WEIGHT = 0.70
    MAX_WEIGHT = 1.30
    SMOOTHING_STEP = 0.05

    def __init__(self, db: DB, bot_instance_id: str = "default"):
        self.db = db
        self.bot_instance_id = bot_instance_id
        # Maps strategy_name -> current smoothed weight multiplier
        self._smoothed_weights: Dict[str, float] = {}
        # Stores exact raw multiplier from the DB reconstruction
        self._raw_targets: Dict[str, float] = {}

    def get_weight_adjustments(self, config_id: str) -> Dict[str, float]:
        """
        Reconstructs strategy win rates from durable DB tables,
        applies smoothing, and returns the active weight adjustments.
        """
        outcomes = self._get_outcomes_from_db(config_id)
        
        updates: Dict[str, float] = {}
        for strat, results in outcomes.items():
            if len(results) < self.MIN_SAMPLES:
                target = 1.0
            else:
                win_rate = sum(results) / len(results)
                target = win_rate / self.REF_WIN_RATE
                target = max(self.MIN_WEIGHT, min(self.MAX_WEIGHT, target))
                
            self._raw_targets[strat] = target
            
            # Apply smoothing: move smoothed weight toward target
            current = self._smoothed_weights.get(strat, 1.0)
            if abs(target - current) < 1e-5:
                smoothed = target
            elif target > current:
                smoothed = min(target, current + self.SMOOTHING_STEP)
            else:
                smoothed = max(target, current - self.SMOOTHING_STEP)
                
            self._smoothed_weights[strat] = smoothed
            
            if abs(smoothed - 1.0) > 1e-4:  # Only populate if not exactly 1.0
                updates[strat] = smoothed
                
        return updates

    def _get_outcomes_from_db(self, config_id: str) -> Dict[str, List[bool]]:
        """
        Derives strategy performance by matching CLOSE fills to the strategies
        that were active when the position was opened.
        
        Since decision_logs stores "active_strategies" in its meta payload,
        we match the latest OPEN decision before each CLOSE fill to credit/blame
        those specific strategies.
        """
        outcomes: Dict[str, List[bool]] = {}
        
        try:
            with self.db.connect() as conn:
                # 1. Fetch recent CLOSE fills with PnL — scoped to this bot
                fills = conn.execute(
                    """
                    SELECT symbol, realized_pnl, timestamp_utc
                    FROM trade_fills
                    WHERE action = 'CLOSE'
                      AND (bot_instance_id = ? OR (bot_instance_id IS NULL AND ? = 'default'))
                    ORDER BY timestamp_utc DESC
                    LIMIT 200
                    """,
                    (self.bot_instance_id, self.bot_instance_id),
                ).fetchall()

                # 2. For each fill, find the OPEN decision that precedes it
                # and extract the strategies that voted for it.
                # decision_logs is scoped by config_id which maps to this bot's config.

                if not fills:
                    return outcomes

                decisions = conn.execute(
                    """
                    SELECT symbol, final_action, strategy_signal_json, created_at
                    FROM decision_logs
                    WHERE final_action = 'execute'
                      AND config_id = ?
                    ORDER BY created_at DESC
                    LIMIT 1000
                    """,
                    (config_id,),
                ).fetchall()
                
        except Exception as exc:
            logger.warning(f"[StrategyPerfTracker] DB query failed: {exc}")
            return outcomes
            
        import json
        
        # Group decisions by symbol for fast lookup
        decs_by_sym = {}
        for row in decisions:
            sym = row["symbol"] if isinstance(row, dict) else row[0]
            if sym not in decs_by_sym:
                decs_by_sym[sym] = []
            decs_by_sym[sym].append(row)
            
        # Match fills to decisions
        for fill_row in fills:
            sym = fill_row["symbol"] if isinstance(fill_row, dict) else fill_row[0]
            pnl = fill_row["realized_pnl"] if isinstance(fill_row, dict) else fill_row[1]
            fill_time = fill_row["timestamp_utc"] if isinstance(fill_row, dict) else fill_row[2]
            
            if pnl is None:
                continue
                
            is_win = float(pnl) > 0
            
            # Find the most recent OPEN decision for this symbol before the fill_time
            sym_decs = decs_by_sym.get(sym, [])
            matching_dec = None
            for dec in sym_decs:
                dec_time = dec["created_at"] if isinstance(dec, dict) else dec[3]
                if dec_time <= fill_time:
                    matching_dec = dec
                    break
                    
            if matching_dec:
                try:
                    signal_raw = matching_dec["strategy_signal_json"] if isinstance(matching_dec, dict) else matching_dec[2]
                    signal_data = json.loads(signal_raw) if isinstance(signal_raw, str) else (signal_raw or {})
                    # active_strategies is stored inside the nested "meta" key of strategy_signal_json
                    meta = signal_data.get("meta") or {}
                    active_strats = meta.get("active_strategies", [])
                    
                    # Credit/blame those strategies
                    for strat in active_strats:
                        if strat not in outcomes:
                            outcomes[strat] = []
                        if len(outcomes[strat]) < self.WINDOW:
                            # We append to end, but since we iterate fills DESC, 
                            # we are building the list oldest to newest if we reverse?
                            # Actually, we want the list to just be a bag of recent bools, order doesn't strictly matter for sum/len
                            outcomes[strat].append(is_win)
                except Exception:
                    pass
                    
        return outcomes
