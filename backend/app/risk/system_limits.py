"""
System Safety Limits - Non-Negotiable Bounds

These limits CANNOT be bypassed by users, even with "aggressive" settings.
They represent the absolute maximum risk the system will allow.
"""
from dataclasses import dataclass
from typing import Dict, Optional
from enum import Enum


class RiskLevel(Enum):
    """User-selectable risk levels."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


class AssetClass(Enum):
    """Asset classification for leverage limits."""
    MAJOR_CRYPTO = "major_crypto"  # BTC, ETH
    ALT_CRYPTO = "alt_crypto"  # Other established coins
    MEME_CRYPTO = "meme_crypto"  # High volatility meme coins
    STABLE_PAIRS = "stable_pairs"  # USDT, USDC pairs


@dataclass
class SystemLimits:
    """
    System-enforced safety limits that cannot be overridden.
    
    These are the ABSOLUTE maximums. User configurations will be clamped to these.
    """
    # Stop-loss requirements (ATR based)
    min_stop_loss_pct: float = 0.005  # 0.5% absolute minimum if ATR undefined
    max_stop_loss_pct: float = 0.15   # 15% absolute maximum
    max_stop_loss_atr_multiplier: float = 3.5  # Cap SL at 3.5x ATR
    min_stop_loss_atr_multiplier: float = 0.6  # Min SL at 0.6x ATR

    # Leverage ceilings per asset class (Crypto Futures)
    max_leverage_major: float = 10.0  # BTC, ETH (User requested 10x)
    max_leverage_alt: float = 7.0     # Large alts (User requested 7x)
    max_leverage_meme: float = 5.0    # Mid/Low liquidity (User requested 5x)
    max_leverage_stable: float = 10.0 # Stable pairs (safe)
    
    # Risk per trade ceiling (absolute max)
    max_risk_per_trade_ceiling: float = 0.015  # 1.5% max risk per trade (User requested 1.5%)
    
    # Exposure ceilings
    max_exposure_per_symbol_pct: float = 0.40  # 40% of allocated capital (User requested 40%)
    max_total_exposure_mult: float = 2.5       # 2.5x allocated capital (User requested 2.5x)
    max_correlation_exposure_pct: float = 0.60 # 60% in correlated bucket (User requested 60%)
    
    # Position limits
    max_open_positions: int = 6  # Max 6 simultaneous positions (User requested 6)
    min_open_positions: int = 0
    
    # Daily / Weekly limits
    max_daily_loss_pct: float = 0.05    # 5% allocated capital hard stop (User requested 5%)
    max_weekly_drawdown_pct: float = 0.12 # 12% allocated capital hard stop
    max_consecutive_losses: int = 6     # Stop after 6 consecutive losses
    
    # Execution safety
    max_slippage_majors: float = 0.0020 # 0.20%
    max_slippage_alts: float = 0.0035   # 0.35%
    
    # Emergency rules (ALWAYS enforced)
    emergency_drawdown_halt_pct: float = 0.25
    emergency_margin_ratio_threshold: float = 1.35 # Minimum free margin buffer >= 35% equity (approx 1.35 ratio depending on logic)
    force_stop_loss_always: bool = True


@dataclass
class UserConfigurableLimits:
    """
    User-configurable settings that will be clamped by SystemLimits.
    
    Users can request these, but they'll be validated and clamped.
    """
    # Risk level (determines several derived limits)
    risk_level: RiskLevel = RiskLevel.MEDIUM
    
    # Capital allocation
    capital_allocation_pct: float = 1.0  # Use 100% of capital
    use_fixed_size: bool = False  # False = percentage-based, True = fixed USDT
    fixed_size_usdt: Optional[float] = None  # If using fixed size
    
    # Daily limits (will be clamped to system max)
    max_daily_loss_pct: float = 0.05  # User wants 5% (system allows up to 10%)
    max_trades_per_day: int = 50  # User wants 50 (system allows up to 200)
    
    # Position limits (will be clamped)
    max_open_positions: int = 5  # User wants 5 (system allows up to 20)
    
    # Leverage (will be clamped per asset class)
    requested_leverage: Dict[str, float] = None  # e.g., {"BTCUSDT": 10, "default": 5}
    
    # Stop-loss preference (will be clamped to min/max)
    default_stop_loss_pct: float = 0.02  # 2% default stop
    
    # Symbol allowlist (user chooses which symbols to trade)
    allowed_symbols: list = None  # e.g., ["BTCUSDT", "ETHUSDT"]
    
    # Trade mode
    paper_mode: bool = True  # Default to paper trading
    
    # Advanced Risk Filters
    min_strategy_confidence: float = 0.5  # Minimum confidence to take a trade
    volatility_filter_enabled: bool = True  # Enable volatility gating/scaling
    strict_circuit_breakers: bool = False  # Enable tighter circuit breakers
    
    def __post_init__(self):
        if self.requested_leverage is None:
            self.requested_leverage = {}
        if self.allowed_symbols is None:
            self.allowed_symbols = []


class ConfigValidator:
    """
    Validates and clamps user configurations to system limits.
    
    "Users can request aggressive settings, but the system clamps them."
    """
    
    def __init__(self, system_limits: SystemLimits = None):
        self.limits = system_limits or SystemLimits()
    
    def validate_and_clamp(
        self,
        user_config: UserConfigurableLimits
    ) -> tuple[UserConfigurableLimits, list[str]]:
        """
        Validate user configuration and clamp to system limits.
        
        Returns:
            (clamped_config, warnings)
            
        Warnings list contains messages about what was clamped.
        """
        warnings = []
        clamped = UserConfigurableLimits(
            risk_level=user_config.risk_level,
            capital_allocation_pct=user_config.capital_allocation_pct,
            use_fixed_size=user_config.use_fixed_size,
            fixed_size_usdt=user_config.fixed_size_usdt,
            paper_mode=user_config.paper_mode,
            allowed_symbols=user_config.allowed_symbols.copy(),
            min_strategy_confidence=user_config.min_strategy_confidence,
            volatility_filter_enabled=user_config.volatility_filter_enabled,
            strict_circuit_breakers=user_config.strict_circuit_breakers
        )
        
        # 1. Clamp daily loss
        if user_config.max_daily_loss_pct > self.limits.max_daily_loss_pct:
            warnings.append(
                f"Daily loss limit clamped from {user_config.max_daily_loss_pct:.1%} "
                f"to system maximum {self.limits.max_daily_loss_pct:.1%}"
            )
            clamped.max_daily_loss_pct = self.limits.max_daily_loss_pct
        else:
            clamped.max_daily_loss_pct = user_config.max_daily_loss_pct
        
        # 2. Clamp trades per day
        if user_config.max_trades_per_day > self.limits.max_trades_per_day:
            warnings.append(
                f"Trades per day clamped from {user_config.max_trades_per_day} "
                f"to system maximum {self.limits.max_trades_per_day}"
            )
            clamped.max_trades_per_day = self.limits.max_trades_per_day
        else:
            clamped.max_trades_per_day = user_config.max_trades_per_day
        
        # 3. Clamp max positions
        if user_config.max_open_positions > self.limits.max_open_positions:
            warnings.append(
                f"Max positions clamped from {user_config.max_open_positions} "
                f"to system maximum {self.limits.max_open_positions}"
            )
            clamped.max_open_positions = self.limits.max_open_positions
        else:
            clamped.max_open_positions = user_config.max_open_positions
        
        # 4. Clamp stop-loss
        if user_config.default_stop_loss_pct < self.limits.min_stop_loss_pct:
            warnings.append(
                f"Stop-loss increased from {user_config.default_stop_loss_pct:.2%} "
                f"to system minimum {self.limits.min_stop_loss_pct:.2%}"
            )
            clamped.default_stop_loss_pct = self.limits.min_stop_loss_pct
        elif user_config.default_stop_loss_pct > self.limits.max_stop_loss_pct:
            warnings.append(
                f"Stop-loss decreased from {user_config.default_stop_loss_pct:.2%} "
                f"to system maximum {self.limits.max_stop_loss_pct:.2%}"
            )
            clamped.default_stop_loss_pct = self.limits.max_stop_loss_pct
        else:
            clamped.default_stop_loss_pct = user_config.default_stop_loss_pct
        
        # 5. Clamp leverage per symbol (requires asset classification)
        clamped.requested_leverage = {}
        for symbol, leverage in user_config.requested_leverage.items():
            asset_class = self._classify_asset(symbol)
            max_leverage = self._get_max_leverage_for_asset(asset_class)
            
            if leverage > max_leverage:
                warnings.append(
                    f"Leverage for {symbol} clamped from {leverage}x to {max_leverage}x "
                    f"(asset class: {asset_class.value})"
                )
                clamped.requested_leverage[symbol] = max_leverage
            else:
                clamped.requested_leverage[symbol] = leverage
        
        # 6. Get risk per trade limit based on risk level
        # This logic is used for sizing, now clamped by global ceiling
        # Note: We return ceiling here, but the specific preset values (low=0.35%, etc) 
        # are handled in UserStrategyConfigService default presets.
        # If user provides custom overrides, they will be clamped to this ceiling.
        return self.limits.max_risk_per_trade_ceiling

    def get_risk_per_trade_limit(self, risk_level: RiskLevel) -> float:
        """Get the max risk per trade (global ceiling). Specific risk levels are handled in presets."""
        return self.limits.max_risk_per_trade_ceiling


# =============================================================================
# USAGE EXAMPLE
# =============================================================================

if __name__ == "__main__":
    # User wants aggressive settings
    user_config = UserConfigurableLimits(
        risk_level=RiskLevel.HIGH,
        max_daily_loss_pct=0.20,  # Wants 20% (system max is 10%)
        max_trades_per_day=500,  # Wants 500 (system max is 200)
        max_open_positions=30,  # Wants 30 (system max is 20)
        default_stop_loss_pct=0.001,  # Wants 0.1% stop (system min is 0.5%)
        requested_leverage={
            "BTCUSDT": 50,  # Wants 50x (system max is 20x for BTC)
            "SHIBUSDT": 20,  # Wants 20x (system max is 5x for meme)
        },
        allowed_symbols=["BTCUSDT", "ETHUSDT", "SHIBUSDT"],
        paper_mode=False
    )
    
    # Validate and clamp
    validator = ConfigValidator()
    clamped_config, warnings = validator.validate_and_clamp(user_config)
    
    print("User requested aggressive settings:")
    print(f"  Daily loss: 20%")
    print(f"  Trades/day: 500")
    print(f"  Max positions: 30")
    print(f"  Stop-loss: 0.1%")
    print(f"  BTC leverage: 50x")
    print(f"  SHIB leverage: 20x")
    print()
    print("System clamped to safe limits:")
    print(f"  Daily loss: {clamped_config.max_daily_loss_pct:.1%}")
    print(f"  Trades/day: {clamped_config.max_trades_per_day}")
    print(f"  Max positions: {clamped_config.max_open_positions}")
    print(f"  Stop-loss: {clamped_config.default_stop_loss_pct:.2%}")
    print(f"  BTC leverage: {clamped_config.requested_leverage['BTCUSDT']}x")
    print(f"  SHIB leverage: {clamped_config.requested_leverage['SHIBUSDT']}x")
    print()
    print("Warnings:")
    for warning in warnings:
        print(f"  ⚠️  {warning}")
