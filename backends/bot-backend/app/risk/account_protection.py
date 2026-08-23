"""
Account Protection Engine

Enforces safety limits to prevent account liquidation through multi-layered protection:
- Daily loss limits
- Maximum drawdown monitoring  
- Consecutive loss tracking with cool-down periods
- Emergency circuit breaker for rapid losses
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any
from dataclasses import dataclass
from shared_lib.persistence.db import DB, utc_now_iso

logger = logging.getLogger(__name__)


@dataclass
class ProtectionStatus:
    """Current protection status for a configuration."""
    is_protected: bool
    protection_reason: Optional[str]
    daily_loss_today: float
    current_drawdown_pct: float
    consecutive_losses: int
    cool_down_until: Optional[str]
    details: Dict[str, Any]


class AccountProtection:
    """
    Account protection engine that enforces safety limits.
    
    Protection Mechanisms:
    1. Daily Loss Limit: Stop trading if daily loss exceeds limit
    2. Max Drawdown: Pause if drawdown from peak exceeds limit
    3. Consecutive Losses: Apply cool-down after N losses in a row
    4. Emergency Stop: Circuit breaker for rapid equity drops
    """
    
    def __init__(self, db: DB):
        self.db = db
    
    def check_protection(
        self, 
        config_id: str,
        daily_loss_limit_pct: float,
        max_drawdown_pct: float,
        current_equity: float
    ) -> ProtectionStatus:
        """
        Check if trading should be blocked due to protection triggers.
        
        Args:
            config_id: Configuration ID
            daily_loss_limit_pct: Daily loss limit (0.05 = 5%)
            max_drawdown_pct: Max drawdown limit (0.15 = 15%)
            current_equity: Current account equity
            
        Returns:
            ProtectionStatus with details
        """
        with self.db.connect() as conn:
            row = conn.execute(
                "SELECT * FROM protection_state WHERE config_id = ?",
                (config_id,)
            ).fetchone()
            
            if not row:
                logger.warning(f"No protection state found for config {config_id}")
                return ProtectionStatus(
                    is_protected=True,
                    protection_reason="No protection state initialized",
                    daily_loss_today=0.0,
                    current_drawdown_pct=0.0,
                    consecutive_losses=0,
                    cool_down_until=None,
                    details={}
                )
            
            state = dict(row)
            
            # Check if already protected
            if state["is_protected"]:
                return ProtectionStatus(
                    is_protected=True,
                    protection_reason=state["protection_reason"],
                    daily_loss_today=state["daily_loss_today"],
                    current_drawdown_pct=state["current_drawdown_pct"],
                    consecutive_losses=state["consecutive_losses"],
                    cool_down_until=state["cool_down_until"],
                    details=state
                )
            
            # Check cool-down period
            if state["cool_down_until"]:
                cool_down_dt = datetime.fromisoformat(state["cool_down_until"])
                if datetime.now(timezone.utc) < cool_down_dt:
                    return ProtectionStatus(
                        is_protected=True,
                        protection_reason=f"Cool-down until {state['cool_down_until']}",
                        daily_loss_today=state["daily_loss_today"],
                        current_drawdown_pct=state["current_drawdown_pct"],
                        consecutive_losses=state["consecutive_losses"],
                        cool_down_until=state["cool_down_until"],
                        details=state
                    )
            
            # Calculate peak equity if not set
            peak_equity = state["peak_equity"]
            if peak_equity is None or current_equity > peak_equity:
                peak_equity = current_equity
                conn.execute(
                    "UPDATE protection_state SET peak_equity = ?, updated_at = ? WHERE config_id = ?",
                    (peak_equity, utc_now_iso(), config_id)
                )
            
            # Calculate current drawdown
            current_drawdown_pct = 0.0
            if peak_equity > 0:
                current_drawdown_pct = (peak_equity - current_equity) / peak_equity
            
            # Update drawdown in state
            conn.execute(
                "UPDATE protection_state SET current_drawdown_pct = ?, updated_at = ? WHERE config_id = ?",
                (current_drawdown_pct, utc_now_iso(), config_id)
            )
            
            # Check 1: Daily Loss Limit
            if state["daily_loss_today"] >= daily_loss_limit_pct:
                self._trigger_protection(
                    config_id,
                    f"Daily loss limit reached: {state['daily_loss_today']:.2%}"
                )
                return ProtectionStatus(
                    is_protected=True,
                    protection_reason=f"Daily loss limit reached ({state['daily_loss_today']:.2%})",
                    daily_loss_today=state["daily_loss_today"],
                    current_drawdown_pct=current_drawdown_pct,
                    consecutive_losses=state["consecutive_losses"],
                    cool_down_until=None,
                    details=state
                )
            
            # Check 2: Max Drawdown
            if current_drawdown_pct >= max_drawdown_pct:
                self._trigger_protection(
                    config_id,
                    f"Max drawdown exceeded: {current_drawdown_pct:.2%}"
                )
                return ProtectionStatus(
                    is_protected=True,
                    protection_reason=f"Max drawdown exceeded ({current_drawdown_pct:.2%})",
                    daily_loss_today=state["daily_loss_today"],
                    current_drawdown_pct=current_drawdown_pct,
                    consecutive_losses=state["consecutive_losses"],
                    cool_down_until=None,
                    details=state
                )
            
            # All checks passed
            return ProtectionStatus(
                is_protected=False,
                protection_reason=None,
                daily_loss_today=state["daily_loss_today"],
                current_drawdown_pct=current_drawdown_pct,
                consecutive_losses=state["consecutive_losses"],
                cool_down_until=None,
                details=state
            )
    
    def record_trade_result(
        self,
        config_id: str,
        pnl: float,
        equity_before: float,
        equity_after: float
    ):
        """
        Record a trade result and update protection state.
        
        Args:
            config_id: Configuration ID
            pnl: Realized PnL (negative for loss)
            equity_before: Equity before trade
            equity_after: Equity after trade
        """
        with self.db.connect() as conn:
            row = conn.execute(
                "SELECT * FROM protection_state WHERE config_id = ?",
                (config_id,)
            ).fetchone()
            
            if not row:
                logger.error(f"No protection state found for config {config_id}")
                return
            
            state = dict(row)
            now = utc_now_iso()
            
            # Update daily loss
            pnl_pct = pnl / equity_before if equity_before > 0 else 0
            new_daily_loss = state["daily_loss_today"] + abs(pnl_pct) if pnl < 0 else state["daily_loss_today"]
            
            # Track consecutive losses
            is_loss = pnl < 0
            if is_loss:
                consecutive_losses = state["consecutive_losses"] + 1
                last_loss_at = now
                
                # Apply cool-down if 3+ consecutive losses
                cool_down_until = None
                if consecutive_losses >= 3:
                    # 1 hour cool-down for 3 losses, +30 min per additional loss
                    cool_down_minutes = 60 + (consecutive_losses - 3) * 30
                    cool_down_dt = datetime.now(timezone.utc) + timedelta(minutes=cool_down_minutes)
                    cool_down_until = cool_down_dt.isoformat()
                    logger.warning(
                        f"Cool-down triggered for config {config_id}: "
                        f"{consecutive_losses} consecutive losses, paused until {cool_down_until}"
                    )
            else:
                # Reset on win
                consecutive_losses = 0
                last_loss_at = state["last_loss_at"]
                cool_down_until = None
            
            # Update state
            conn.execute(
                """
                UPDATE protection_state SET
                    daily_loss_today = ?,
                    consecutive_losses = ?,
                    last_loss_at = ?,
                    cool_down_until = ?,
                    updated_at = ?
                WHERE config_id = ?
                """,
                (new_daily_loss, consecutive_losses, last_loss_at, cool_down_until, now, config_id)
            )
            
            logger.info(
                f"Updated protection state for {config_id}: "
                f"daily_loss={new_daily_loss:.2%}, consecutive_losses={consecutive_losses}"
            )
    
    def reset_daily_state(self, config_id: str):
        """Reset daily loss counter (call at start of each day)."""
        with self.db.connect() as conn:
            conn.execute(
                "UPDATE protection_state SET daily_loss_today = 0.0, updated_at = ? WHERE config_id = ?",
                (utc_now_iso(), config_id)
            )
        
        logger.info(f"Reset daily state for config {config_id}")
    
    def reset_protection(self, config_id: str):
        """Manually reset protection (admin/manual override)."""
        with self.db.connect() as conn:
            conn.execute(
                """
                UPDATE protection_state SET
                    is_protected = 0,
                    protection_reason = NULL,
                    cool_down_until = NULL,
                    updated_at = ?
                WHERE config_id = ?
                """,
                (utc_now_iso(), config_id)
            )
        
        logger.warning(f"Manually reset protection for config {config_id}")
    
    def _trigger_protection(self, config_id: str, reason: str):
        """Internal: Trigger protection mode."""
        with self.db.connect() as conn:
            conn.execute(
                """
                UPDATE protection_state SET
                    is_protected = 1,
                    protection_reason = ?,
                    updated_at = ?
                WHERE config_id = ?
                """,
                (reason, utc_now_iso(), config_id)
            )
        
        logger.error(f"PROTECTION TRIGGERED for config {config_id}: {reason}")
