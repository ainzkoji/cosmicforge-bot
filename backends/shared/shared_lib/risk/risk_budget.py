"""
Risk Budget Engine - Dynamic Position Limits

Replaces fixed max_open_positions with dynamic budgets based on:
- Portfolio risk-to-stop (primary)
- Margin usage (critical for futures)
- Concentration limits (per-symbol, per-strategy, per-side)

This is production-grade risk management.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, TYPE_CHECKING
from enum import Enum

if TYPE_CHECKING:
    from app.exchange.binance.client import BinanceFuturesClient


class RiskProfile(Enum):
    """User risk tolerance profiles."""
    CONSERVATIVE = "conservative"
    BALANCED = "balanced"
    AGGRESSIVE = "aggressive"


@dataclass
class RiskBudgetConfig:
    """
    Configuration for risk budgets.
    All percentages are expressed as decimals (0.05 = 5%).
    """
    # Portfolio Risk Budget
    portfolio_risk_pct: float = 0.05  # 5% of equity at risk max
    
    # Margin Budget
    max_margin_usage_pct: float = 0.50  # 50% max margin usage
    min_margin_level: float = 1.5  # Minimum margin level (exchange-specific)
    
    # Concentration Limits
    max_exposure_per_symbol_pct: float = 0.20  # 20% max per symbol
    max_exposure_per_strategy_pct: float = 0.40  # 40% max per strategy
    max_exposure_per_side_pct: float = 0.70  # 70% max long or short exposure
    
    # Gross/Net Exposure
    max_gross_exposure_mult: float = 3.0  # Max 3x equity in notional
    max_net_exposure_mult: float = 1.5  # Max 1.5x net directional

    # --- ADVANCED OVERRIDES & MULTIPLIERS ---
    
    # Optional fixed USDT caps (if set, uses min(pct_limit, fixed_cap))
    max_portfolio_risk_usdt: Optional[float] = None  # Cap (Ceiling)
    min_portfolio_risk_usdt: Optional[float] = None  # Floor
    
    max_per_trade_risk_usdt: Optional[float] = None  # Cap
    min_per_trade_risk_usdt: Optional[float] = None  # Floor
    
    # Per-trade risk cap (as % of equity)
    per_trade_risk_pct: float = 0.01  # Max 1% risk per single trade
    
    # Margin Caps
    max_margin_used_cap_usdt: Optional[float] = None
    
    # Dynamic Multipliers (tighten budgets in bad conditions)
    drawdown_risk_multiplier: float = 0.7  # x0.7 to budgets if in heavy drawdown
    high_vol_risk_multiplier: float = 0.8  # x0.8 if high vol
    loss_streak_multiplier: float = 0.8    # x0.8 after loss streak
    
    # Thresholds for multipliers
    drawdown_trigger_pct: float = 0.10     # Trigger multiplier at 10% DD
    
    # Dynamic Position Slots (fallback)
    base_slots: int = 5
    slot_equity_unit: float = 1000.0  # +1 slot per $1000 equity
    max_slots: int = 50  # Hard cap
    
    # Drawdown penalty
    drawdown_penalty_per_pct: int = 1  # -1 slot per 1% drawdown

    # --- CORRELATION BUCKETS ---
    # Default: "BTC" -> "MAJOR", "ETH" -> "MAJOR", Others -> "ALT"
    # Limits apply to the aggregate exposure of the bucket
    bucket_mapping: Dict[str, str] = field(default_factory=lambda: {
        "BTCUSDT": "MAJOR",
        "ETHUSDT": "MAJOR",
        "SOLUSDT": "MAJOR",
        "BNBUSDT": "MAJOR",
    })
    
    # Max exposure % per bucket (e.g., 0.60 = 60% of equity allowed in MAJORS)
    # Default fallback for unknown buckets is 0.30 (30%)
    bucket_limits_pct: Dict[str, float] = field(default_factory=lambda: {
        "MAJOR": 0.60,
        "ALT": 0.40,
        "MEME": 0.15, 
    })
    default_bucket_limit_pct: float = 0.30

    def calculate_effective_budget(
        self, 
        equity: float, 
        pct: float, 
        cap: Optional[float], 
        floor: Optional[float], 
        multiplier: float = 1.0
    ) -> float:
        """
        Compute effective budget: clamp(equity * pct * multiplier, floor, cap)
        Precedence: Multiplier applies to pct-based amount, then clamped.
        """
        raw_budget = equity * pct * multiplier
        
        # Apply Floor
        if floor is not None:
            raw_budget = max(raw_budget, floor)
            
        # Apply Cap
        if cap is not None:
            raw_budget = min(raw_budget, cap)
            
        return raw_budget

    @classmethod
    def from_profile(cls, profile: RiskProfile) -> "RiskBudgetConfig":
        """Factory for preset risk profiles."""
        if profile == RiskProfile.CONSERVATIVE:
            return cls(
                portfolio_risk_pct=0.02,
                max_margin_usage_pct=0.35,
                max_exposure_per_symbol_pct=0.15,
                max_exposure_per_side_pct=0.60,
                base_slots=3,
            )
        elif profile == RiskProfile.AGGRESSIVE:
            return cls(
                portfolio_risk_pct=0.10,
                max_margin_usage_pct=0.65,
                max_exposure_per_symbol_pct=0.30,
                max_exposure_per_side_pct=0.80,
                base_slots=8,
            )
        else:  # BALANCED (default)
            return cls()


@dataclass
class PositionRisk:
    """Risk data for a single position."""
    symbol: str
    side: str  # "LONG" or "SHORT"
    qty: float
    entry_price: float
    stop_price: Optional[float]
    notional: float
    strategy: Optional[str] = None
    
    @property
    def risk_usdt(self) -> float:
        """Calculate risk to stop in USDT."""
        if self.stop_price is None or self.stop_price <= 0:
            # No stop = assume 2% risk as fallback
            return self.notional * 0.02
        
        if self.side == "LONG":
            stop_distance = self.entry_price - self.stop_price
        else:
            stop_distance = self.stop_price - self.entry_price
        
        return max(0, self.qty * stop_distance)


@dataclass
class BudgetState:
    """Current state of all risk budgets."""
    # Equity
    equity: float
    
    # Portfolio Risk
    total_risk_usdt: float
    portfolio_risk_budget: float
    portfolio_risk_usage_pct: float
    
    # Margin
    margin_used: float
    margin_available: float
    margin_usage_pct: float
    margin_level: float
    
    # Concentration
    exposure_by_symbol: Dict[str, float] = field(default_factory=dict)
    exposure_by_strategy: Dict[str, float] = field(default_factory=dict)
    exposure_long: float = 0.0
    exposure_short: float = 0.0
    gross_exposure: float = 0.0
    net_exposure: float = 0.0
    
    # Dynamic Slots
    position_count: int = 0
    allowed_slots: int = 0
    
    # Limits for UI
    limits: Dict[str, float] = field(default_factory=dict)


@dataclass
class BudgetDecision:
    """Result of a budget check."""
    allowed: bool
    reason_code: str
    reason: str
    severity: str = "INFO"  # INFO, WARN, HARD_BLOCK
    
    # Details for debugging
    budget_type: str = ""  # PORTFOLIO_RISK, MARGIN, CONCENTRATION, SLOTS
    current_value: float = 0.0
    limit_value: float = 0.0
    remaining: float = 0.0


class RiskBudgetEngine:
    """
    Dynamic risk budget engine.
    
    Replaces fixed max_open_positions with intelligent budgets.
    """
    
    def __init__(self, config: RiskBudgetConfig):
        self.config = config
        self._positions: List[PositionRisk] = []
        self._equity: float = 0.0
        self._margin_used: float = 0.0
        self._margin_available: float = 0.0
        self._current_drawdown_pct: float = 0.0
    
    def update_account_state(
        self,
        equity: float,
        margin_used: float,
        margin_available: float,
        current_drawdown_pct: float = 0.0,
    ):
        """Update account state from exchange."""
        self._equity = equity
        self._margin_used = margin_used
        self._margin_available = margin_available
        self._current_drawdown_pct = current_drawdown_pct
    
    def update_positions(self, positions: List[PositionRisk]):
        """Update current positions."""
        self._positions = positions
    
    def get_budget_state(self) -> BudgetState:
        """Get current state of all budgets."""
        equity = self._equity
        
        # Determine multipliers
        risk_mult = 1.0
        if self._current_drawdown_pct >= self.config.drawdown_trigger_pct:
            risk_mult *= self.config.drawdown_risk_multiplier
        
        # Portfolio risk (Percent + Fixed Cap/Floor + Multiplier)
        total_risk = sum(p.risk_usdt for p in self._positions)
        
        portfolio_budget = self.config.calculate_effective_budget(
            equity, 
            self.config.portfolio_risk_pct, 
            self.config.max_portfolio_risk_usdt, 
            self.config.min_portfolio_risk_usdt,
            risk_mult
        )
        
        # Margin
        margin_usage_pct = self._margin_used / equity if equity > 0 else 0
        margin_level = (equity / self._margin_used) if self._margin_used > 0 else 999
        
        # Margin Budget (effective)
        margin_budget_usdt = equity * self.config.max_margin_usage_pct
        if self.config.max_margin_used_cap_usdt:
            margin_budget_usdt = min(margin_budget_usdt, self.config.max_margin_used_cap_usdt)
        if margin_budget_usdt > 0:
            margin_usage_pct_effective = self._margin_used / margin_budget_usdt # normalized to 1.0 = full budget
        else:
             margin_usage_pct_effective = margin_usage_pct # fallback
        
        # Concentration
        exposure_by_symbol: Dict[str, float] = {}
        exposure_by_strategy: Dict[str, float] = {}
        exposure_by_bucket: Dict[str, float] = {}  # ✅ ADDED
        
        exposure_long = 0.0
        exposure_short = 0.0
        
        for p in self._positions:
            # By symbol
            exposure_by_symbol[p.symbol] = exposure_by_symbol.get(p.symbol, 0) + p.notional
            
            # By strategy
            if p.strategy:
                exposure_by_strategy[p.strategy] = exposure_by_strategy.get(p.strategy, 0) + p.notional
            
            # By Bucket
            bucket = self._get_bucket(p.symbol)
            exposure_by_bucket[bucket] = exposure_by_bucket.get(bucket, 0) + p.notional
            
            # By side
            if p.side == "LONG":
                exposure_long += p.notional
            else:
                exposure_short += p.notional
        
        gross_exposure = exposure_long + exposure_short
        net_exposure = abs(exposure_long - exposure_short)
        
        # Dynamic slots
        allowed_slots = self._compute_allowed_slots()
        
        return BudgetState(
            equity=equity,
            total_risk_usdt=total_risk,
            portfolio_risk_budget=portfolio_budget,
            portfolio_risk_usage_pct=(total_risk / portfolio_budget) if portfolio_budget > 0 else 0,
            margin_used=self._margin_used,
            margin_available=self._margin_available,
            margin_usage_pct=margin_usage_pct,
            margin_level=margin_level,
            exposure_by_symbol=exposure_by_symbol,
            exposure_by_strategy=exposure_by_strategy,
            exposure_long=exposure_long,
            exposure_short=exposure_short,
            gross_exposure=gross_exposure,
            net_exposure=net_exposure,
            position_count=len(self._positions),
            allowed_slots=allowed_slots,
            limits={
                "portfolio_risk_budget": portfolio_budget,
                "max_margin_usage": self.config.max_margin_usage_pct,
                "max_gross_exposure": equity * self.config.max_gross_exposure_mult,
                "max_net_exposure": equity * self.config.max_net_exposure_mult,
                "allowed_slots": allowed_slots,
            }
        )
    
    def _get_bucket(self, symbol: str) -> str:
        """Resolve symbol to correlation bucket."""
        # Exact match
        if symbol in self.config.bucket_mapping:
            return self.config.bucket_mapping[symbol]
        
        # Fallback logic (could contain more complex rules)
        # e.g. "DOGE" -> "MEME" if logic existed
        
        return "ALT"  # Default bucket
    
    def _compute_allowed_slots(self) -> int:
        """Compute dynamic position slot limit."""
        equity = self._equity
        config = self.config
        
        # Base slots
        slots = config.base_slots
        
        # Equity bonus
        if equity > 0 and config.slot_equity_unit > 0:
            slots += int(equity / config.slot_equity_unit)
        
        # Drawdown penalty
        if self._current_drawdown_pct > 0:
            penalty = int(self._current_drawdown_pct * config.drawdown_penalty_per_pct)
            slots -= penalty
        
        # Margin safety bonus/penalty
        margin_usage = self._margin_used / equity if equity > 0 else 0
        if margin_usage < 0.25:
            slots += 2  # Bonus for very safe margin
        elif margin_usage > 0.50:
            slots -= 2  # Penalty for high margin usage
        
        # Clamp to bounds
        slots = max(1, min(slots, config.max_slots))
        
        return slots
    
    def can_add_position(
        self,
        symbol: str,
        side: str,
        qty: float,
        entry_price: float,
        stop_price: Optional[float],
        strategy: Optional[str] = None,
        margin_required: float = 0.0,
    ) -> BudgetDecision:
        """
        Check if a new position is allowed by all budgets.
        
        Returns BudgetDecision with allowed=True only if ALL budgets pass.
        """
        equity = self._equity
        if equity <= 0:
            return BudgetDecision(
                False, "NO_EQUITY", "Account equity is zero or negative",
                "HARD_BLOCK", "EQUITY", 0, 0, 0
            )
        
        notional = qty * entry_price
        
        # Create hypothetical position
        new_pos = PositionRisk(
            symbol=symbol,
            side=side,
            qty=qty,
            entry_price=entry_price,
            stop_price=stop_price,
            notional=notional,
            strategy=strategy,
        )
        
        # =========================================================
        # 1) Portfolio Risk Budget (pct + clamp + multipliers)
        # =========================================================
        current_risk = sum(p.risk_usdt for p in self._positions)
        new_total_risk = current_risk + new_pos.risk_usdt
        
        # Determine multipliers
        risk_mult = 1.0
        if self._current_drawdown_pct >= self.config.drawdown_trigger_pct:
            risk_mult *= self.config.drawdown_risk_multiplier
            
        # Calculate Effective Portfolio Budget
        portfolio_budget = self.config.calculate_effective_budget(
            equity, 
            self.config.portfolio_risk_pct, 
            self.config.max_portfolio_risk_usdt, 
            self.config.min_portfolio_risk_usdt,
            risk_mult
        )
        
        # CHECK 1.1: Total Portfolio Risk
        if new_total_risk > portfolio_budget:
            return BudgetDecision(
                False,
                "PORTFOLIO_RISK_EXCEEDED",
                f"Portfolio risk {new_total_risk:.2f} > budget {portfolio_budget:.2f} (Mult: {risk_mult})",
                "HARD_BLOCK",
                "PORTFOLIO_RISK",
                new_total_risk,
                portfolio_budget,
                portfolio_budget - current_risk,
            )
            
        # CHECK 1.2: Per-Trade Risk
        per_trade_cap = self.config.calculate_effective_budget(
            equity,
            self.config.per_trade_risk_pct,
            self.config.max_per_trade_risk_usdt,
            self.config.min_per_trade_risk_usdt,
            risk_mult
        )
        
        if new_pos.risk_usdt > per_trade_cap:
             return BudgetDecision(
                False,
                "PER_TRADE_RISK_EXCEEDED",
                f"Trade risk {new_pos.risk_usdt:.2f} > limit {per_trade_cap:.2f}",
                "HARD_BLOCK",
                "PER_TRADE_RISK",
                new_pos.risk_usdt,
                per_trade_cap,
                per_trade_cap - new_pos.risk_usdt,
            )
        
        # =========================================================
        # 2) Margin Usage Budget
        # =========================================================
        new_margin_used = self._margin_used + margin_required
        
        # Effective Margin Budget
        margin_budget_usdt = equity * self.config.max_margin_usage_pct
        if self.config.max_margin_used_cap_usdt:
             margin_budget_usdt = min(margin_budget_usdt, self.config.max_margin_used_cap_usdt)
             
        if new_margin_used > margin_budget_usdt:
            return BudgetDecision(
                False,
                "MARGIN_USAGE_EXCEEDED",
                f"Margin used {new_margin_used:.2f} > limit {margin_budget_usdt:.2f}",
                "HARD_BLOCK",
                "MARGIN",
                new_margin_used,
                margin_budget_usdt,
                margin_budget_usdt - self._margin_used,
            )
            
        # =========================================================
        # 3) Margin Level Constraint (Liquidation Safety)
        # =========================================================
        # Estimate new margin level. 
        # ML = Equity / Maintenance Margin Used. 
        # Note: 'margin_used' in this context is mostly 'initial margin' + 'unrealized pnl' roughly.
        # We use a simplified check here: if current ML allows.
        
        # If we have current ML, and adding margin reduces it...
        # Simpler: if new_margin_used > 0, implied_level approx equity / new_margin_used (very rough)
        # A better proxy is preventing margin usage from going too high, which we did above.
        # But if we want to enforce explicit Margin Level:
        
        if new_margin_used > 0:
             implied_level = equity / new_margin_used
             if implied_level < self.config.min_margin_level:
                  return BudgetDecision(
                    False,
                    "MARGIN_LEVEL_CRITICAL",
                    f"Est. Margin Level {implied_level:.2f} < min {self.config.min_margin_level}",
                    "HARD_BLOCK",
                    "MARGIN_LEVEL",
                    implied_level,
                    self.config.min_margin_level,
                    0
                )
        
        # =========================================================
        # 4) Symbol Concentration
        # =========================================================
        current_symbol_exposure = sum(
            p.notional for p in self._positions if p.symbol == symbol
        )
        new_symbol_exposure = current_symbol_exposure + notional
        max_symbol_exposure = equity * self.config.max_exposure_per_symbol_pct
        
        if new_symbol_exposure > max_symbol_exposure:
            return BudgetDecision(
                False,
                "SYMBOL_CONCENTRATION_EXCEEDED",
                f"Symbol {symbol} exposure {new_symbol_exposure:.2f} > limit {max_symbol_exposure:.2f}",
                "WARN",
                "CONCENTRATION",
                new_symbol_exposure,
                max_symbol_exposure,
                max_symbol_exposure - current_symbol_exposure,
            )
            
        # =========================================================
        # 4.1) Correlation Bucket Concentration (New)
        # =========================================================
        bucket = self._get_bucket(symbol)
        
        current_bucket_exposure = sum(
            p.notional for p in self._positions if self._get_bucket(p.symbol) == bucket
        )
        new_bucket_exposure = current_bucket_exposure + notional
        
        limit_pct = self.config.bucket_limits_pct.get(bucket, self.config.default_bucket_limit_pct)
        max_bucket_exposure = equity * limit_pct
        
        if new_bucket_exposure > max_bucket_exposure:
             return BudgetDecision(
                False,
                "BUCKET_EXPOSURE_EXCEEDED",
                f"Bucket '{bucket}' exposure {new_bucket_exposure:.2f} > limit {max_bucket_exposure:.2f} ({limit_pct:.0%})",
                "WARN",
                "CONCENTRATION",
                new_bucket_exposure,
                max_bucket_exposure,
                max_bucket_exposure - current_bucket_exposure,
            )
        
        # =========================================================
        # 4.2) Strategy Concentration
        # =========================================================
        if strategy:
            current_strategy_exposure = sum(
                p.notional for p in self._positions if p.strategy == strategy
            )
            new_strategy_exposure = current_strategy_exposure + notional
            max_strategy_exposure = equity * self.config.max_exposure_per_strategy_pct
            
            if new_strategy_exposure > max_strategy_exposure:
                return BudgetDecision(
                    False,
                    "STRATEGY_CONCENTRATION_EXCEEDED",
                    f"Strategy {strategy} exposure {new_strategy_exposure:.2f} > limit {max_strategy_exposure:.2f}",
                    "WARN",
                    "CONCENTRATION",
                    new_strategy_exposure,
                    max_strategy_exposure,
                    max_strategy_exposure - current_strategy_exposure,
                )
        
        # =========================================================
        # 5) Side Concentration (avoid all-long or all-short)
        # =========================================================
        long_exposure = sum(p.notional for p in self._positions if p.side == "LONG")
        short_exposure = sum(p.notional for p in self._positions if p.side == "SHORT")
        
        if side == "LONG":
            new_long = long_exposure + notional
            new_side_pct = new_long / (new_long + short_exposure) if (new_long + short_exposure) > 0 else 1.0
        else:
            new_short = short_exposure + notional
            new_side_pct = new_short / (long_exposure + new_short) if (long_exposure + new_short) > 0 else 1.0
        
        if new_side_pct > self.config.max_exposure_per_side_pct:
            return BudgetDecision(
                False,
                "SIDE_CONCENTRATION_EXCEEDED",
                f"Side {side} concentration {new_side_pct:.1%} > limit {self.config.max_exposure_per_side_pct:.1%}",
                "WARN",
                "CONCENTRATION",
                new_side_pct,
                self.config.max_exposure_per_side_pct,
                0,
            )
        
        # =========================================================
        # 6) Gross Exposure
        # =========================================================
        current_gross = long_exposure + short_exposure
        new_gross = current_gross + notional
        max_gross = equity * self.config.max_gross_exposure_mult
        
        if new_gross > max_gross:
            return BudgetDecision(
                False,
                "GROSS_EXPOSURE_EXCEEDED",
                f"Gross exposure {new_gross:.2f} > limit {max_gross:.2f}",
                "HARD_BLOCK",
                "EXPOSURE",
                new_gross,
                max_gross,
                max_gross - current_gross,
            )
        
        # =========================================================
        # 7) Dynamic Position Slots (soft cap)
        # =========================================================
        allowed_slots = self._compute_allowed_slots()
        if len(self._positions) >= allowed_slots:
            return BudgetDecision(
                False,
                "DYNAMIC_SLOTS_EXCEEDED",
                f"Position count {len(self._positions)} >= allowed {allowed_slots}",
                "WARN",
                "SLOTS",
                len(self._positions),
                allowed_slots,
                0,
            )
        
        # =========================================================
        # All checks passed
        # =========================================================
        return BudgetDecision(
            True,
            "OK",
            "All risk budgets satisfied",
            "INFO",
            "",
            0,
            0,
            portfolio_budget - new_total_risk,
        )
    
    def get_remaining_capacity(self) -> Dict[str, float]:
        """Get remaining capacity for each budget."""
        equity = self._equity
        current_risk = sum(p.risk_usdt for p in self._positions)
        current_gross = sum(p.notional for p in self._positions)
        
        return {
            "portfolio_risk_remaining": (equity * self.config.portfolio_risk_pct) - current_risk,
            "margin_remaining": (equity * self.config.max_margin_usage_pct) - self._margin_used,
            "gross_exposure_remaining": (equity * self.config.max_gross_exposure_mult) - current_gross,
            "slots_remaining": self._compute_allowed_slots() - len(self._positions),
        }


# Singleton instance
_budget_engine: Optional[RiskBudgetEngine] = None


def get_risk_budget_engine(config: Optional[RiskBudgetConfig] = None) -> RiskBudgetEngine:
    """Get or create the risk budget engine singleton."""
    global _budget_engine
    if _budget_engine is None:
        _budget_engine = RiskBudgetEngine(config or RiskBudgetConfig())
    return _budget_engine
