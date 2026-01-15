from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional, Dict, TYPE_CHECKING

if TYPE_CHECKING:
    from app.risk.state import RiskState
    from app.risk.drawdown import DrawdownMonitor
    from app.risk.circuit import ExchangeCircuitBreaker
    from app.risk.risk_budget import RiskBudgetEngine


@dataclass
class RiskDecision:
    allowed: bool
    reason_code: str  # e.g. "KILL_SWITCH_ACTIVE"
    reason: str       # Human-readable message
    severity: str = "INFO"  # INFO, WARN, HARD_BLOCK
    kill: bool = False
    modifiers: Dict[str, float] = field(default_factory=dict)
    
@dataclass
class GateSettings:
    max_loss_usdt: float = 100.0
    max_trades_daily: int = 20
    max_open_positions: int = 20  # Fallback only - RiskBudgetEngine takes precedence
    daily_soft_loss_usdt: float = 80.0
    max_weekly_drawdown_pct: float = 0.0
    max_monthly_drawdown_pct: float = 0.0
    # Layer E
    min_strategy_win_rate: float = 0.0 # Disabled by default unless configured
    min_strategy_profit_factor: float = 0.0
    # Risk Budget Engine
    use_budget_engine: bool = True  # Enable dynamic budget system

class RiskGate:
    def __init__(
        self,
        settings: GateSettings,
        circuit_breaker: Optional[ExchangeCircuitBreaker] = None,  # Deprecated - use registry
        drawdown_monitor: Optional[DrawdownMonitor] = None,
        budget_engine: Optional[RiskBudgetEngine] = None,
    ):
        self.settings = settings
        self.circuit = circuit_breaker  # Legacy support
        self.drawdown = drawdown_monitor
        self.budget_engine = budget_engine

    def can_open(
        self,
        state: RiskState,
        signal_symbol: str,
        # New params for budget engine
        qty: float = 0.0,
        entry_price: float = 0.0,
        stop_price: Optional[float] = None,
        strategy: Optional[str] = None,
        margin_required: float = 0.0,
        broker_id: Optional[str] = None,  # For registry-based circuit breaker
    ) -> RiskDecision:
        """
        Evaluates risk layers in order.
        """
        # =========================================================
        # Layer A: System Safety (Hard Blocks)
        # =========================================================
        if state.daily.kill:
             return RiskDecision(False, "KILL_SWITCH_ACTIVE", "Daily kill switch is active", "HARD_BLOCK")

        # Check circuit breaker (registry-based if broker_id provided, else legacy)
        if broker_id:
            from app.risk.circuit import get_circuit_registry
            if get_circuit_registry().is_tripped(broker_id):
                return RiskDecision(False, "CIRCUIT_BREAKER_ACTIVE", f"Circuit breaker tripped for {broker_id}", "HARD_BLOCK")
        elif self.circuit and self.circuit.is_tripped():
            return RiskDecision(False, "CIRCUIT_BREAKER_ACTIVE", "Exchange circuit breaker is tripped", "HARD_BLOCK")

        # =========================================================
        # Layer B: Capital Safety (Hard/Soft Blocks)
        # =========================================================
        # Daily Hard Loss (Auto-block even if kill=False if we passed it just now)
        if state.daily.realized_pnl <= -abs(self.settings.max_loss_usdt):
             return RiskDecision(False, "DAILY_MAX_LOSS_REACHED", f"Realized PnL {state.daily.realized_pnl} <= {self.settings.max_loss_usdt}", "HARD_BLOCK")

        # Drawdown Limits
        if self.drawdown:
            if self.drawdown.check_weekly_drawdown(self.settings.max_weekly_drawdown_pct, state.current_equity, state.weekly):
                 return RiskDecision(False, "WEEKLY_DRAWDOWN_LIMIT", "Weekly drawdown limit reached", "HARD_BLOCK")
            
            if self.drawdown.check_monthly_drawdown(self.settings.max_monthly_drawdown_pct, state.current_equity, state.monthly):
                 return RiskDecision(False, "MONTHLY_DRAWDOWN_LIMIT", "Monthly drawdown limit reached", "HARD_BLOCK")

        # =========================================================
        # Layer C: Overtrading Controls
        # =========================================================
        if state.daily.trade_count >= self.settings.max_trades_daily:
            return RiskDecision(False, "DAILY_MAX_TRADES_REACHED", f"Trade count {state.daily.trade_count} >= {self.settings.max_trades_daily}", "WARN")

        # =========================================================
        # Layer D: Exposure Controls (Risk Budget Engine OR fallback)
        # =========================================================
        if self.budget_engine and self.settings.use_budget_engine and qty > 0:
            # Use dynamic Risk Budget Engine
            side = "LONG" if entry_price > 0 else "LONG"  # Determined by signal later
            budget_decision = self.budget_engine.can_add_position(
                symbol=signal_symbol,
                side=side,
                qty=qty,
                entry_price=entry_price,
                stop_price=stop_price,
                strategy=strategy,
                margin_required=margin_required,
            )
            
            if not budget_decision.allowed:
                return RiskDecision(
                    False,
                    budget_decision.reason_code,
                    budget_decision.reason,
                    budget_decision.severity,
                )
        else:
            # Fallback to simple position count
            if state.open_positions >= self.settings.max_open_positions:
                return RiskDecision(False, "MAX_OPEN_POSITIONS_REACHED", f"Open positions {state.open_positions} >= {self.settings.max_open_positions}", "WARN")

        # =========================================================
        # Layer E: Strategy Health (Selectivity)
        # =========================================================
        if state.health:
            # Only enforce if we have sample size (e.g. 5 trades)
            if state.health.trades >= 5:
                if self.settings.min_strategy_win_rate > 0 and state.health.win_rate < self.settings.min_strategy_win_rate:
                    return RiskDecision(
                        False, 
                        "LOW_STRATEGY_WIN_RATE", 
                        f"Win rate {state.health.win_rate:.2f} < {self.settings.min_strategy_win_rate}", 
                        "WARN"
                    )


        # =========================================================
        # Outcome: ALLOW
        # =========================================================
        return RiskDecision(True, "OK", "Risk checks passed", "INFO")

