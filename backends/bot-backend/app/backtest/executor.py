"""
Backtest Executor - Simulated order fills for backtesting

FILL PRICE MODEL:
- Execute at NEXT candle open (more realistic than same-candle)
- Fallback to current close if next candle unavailable
- Apply slippage based on direction (BUY: +slippage, SELL: -slippage)

COMPATIBILITY:
- Returns ExecResult with same interface as BinanceExecutor
- Records to backtest_fills table (NOT trade_fills)
- Forex-ready with config hooks for precision/constraints
"""
import logging
import json
from typing import Optional, Dict, Any, List
from dataclasses import dataclass
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


def utc_now_iso() -> str:
    """Return current UTC time as ISO string"""
    return datetime.now(timezone.utc).isoformat()


@dataclass
class ExecResult:
    """
    Execution result matching BinanceExecutor interface.
    
    Used by TradingOrchestrator and PaperRunner.
    """
    action: str                          # "ORDER_PLACED", "CLOSED_POSITION", etc.
    details: dict                        # Order details + metadata
    order_id: Optional[str] = None       # Simulated order ID
    error: Optional[str] = None          # Error message if failed
    
    @property
    def success(self) -> bool:
        """True if action succeeded"""
        return self.action in {
            "ORDER_PLACED",
            "CLOSED_POSITION",
            "CLOSED_LONG",
            "CLOSED_SHORT",
        }
    
    @property
    def avg_price(self) -> Optional[float]:
        """Extract average execution price"""
        # Try "order" (entry)
        if "order" in self.details:
            try:
                return float(self.details["order"].get("avgPrice", 0.0))
            except (ValueError, TypeError):
                pass
        
        # Try "close_order" (exit)
        if "close_order" in self.details:
            try:
                return float(self.details["close_order"].get("avgPrice", 0.0))
            except (ValueError, TypeError):
                pass
        
        # Try direct avg_price (backtest)
        if "avg_price" in self.details:
            try:
                return float(self.details["avg_price"])
            except (ValueError, TypeError):
                pass
        
        return None


class BacktestConstraints:
    """
    Placeholder for broker-specific constraints (forex-ready).
    
    Future: Support different precision rules for forex vs crypto.
    """
    
    def __init__(
        self,
        price_precision: int = 2,
        quantity_precision: int = 6,
        min_quantity: float = 0.001,
        min_notional: float = 5.0
    ):
        self.price_precision = price_precision
        self.quantity_precision = quantity_precision
        self.min_quantity = min_quantity
        self.min_notional = min_notional
    
    def round_price(self, price: float) -> float:
        """Round price to broker precision"""
        return round(price, self.price_precision)
    
    def round_quantity(self, quantity: float) -> float:
        """Round quantity to broker precision"""
        return round(quantity, self.quantity_precision)
    
    def validate_order(self, quantity: float, price: float) -> tuple[bool, Optional[str]]:
        """
        Validate order against constraints.
        
        Returns:
            (is_valid, error_message)
        """
        if quantity < self.min_quantity:
            return False, f"Quantity {quantity} below minimum {self.min_quantity}"
        
        notional = quantity * price
        if notional < self.min_notional:
            return False, f"Notional {notional} below minimum {self.min_notional}"
        
        return True, None


class BacktestExecutor:
    """
    Simulated executor for backtesting.
    
    Matches BinanceExecutor interface for compatibility with TradingOrchestrator.
    
    Key differences from live executor:
    - Fills at NEXT candle open (more realistic)
    - No real API calls
    - Records to backtest_fills table
    - Configurable slippage and fees
    """
    
    def __init__(
        self,
        run_id: str,
        slippage_bps: float = 10.0,      # 0.1% slippage
        fee_bps: float = 6.0,             # 0.06% maker/taker fee
        constraints: Optional[BacktestConstraints] = None,
        db = None
    ):
        """
        Args:
            run_id: Backtest run ID (for recording fills)
            slippage_bps: Slippage in basis points (10 = 0.1%)
            fee_bps: Trading fee in basis points (6 = 0.06%)
            constraints: Broker constraints (forex-ready)
            db: Database connection for recording fills
        """
        self.run_id = run_id
        self.slippage_bps = slippage_bps
        self.fee_bps = fee_bps
        self.constraints = constraints or BacktestConstraints()
        
        # DB connection
        self.db = db
        if not self.db:
            from shared_lib.persistence.db import DB
            self.db = DB()
        
        # Track fills for audit
        self.fills = []
        self.fill_counter = 0
    
    def execute_signal(
        self,
        symbol: str,
        signal: str,
        usdt: float,
        current_open_count: int = 0,
        current_equity: float = 0.0,
        current_candle: Optional[List] = None,
        next_candle: Optional[List] = None,
        strategy_name: Optional[str] = None,
        confidence: Optional[float] = None,
        leverage_mult: float = 1.0,
    ) -> ExecResult:
        """
        Simulate order execution (matches BinanceExecutor.execute_signal interface).
        
        Args:
            symbol: Trading pair (e.g. 'BTCUSDT')
            signal: 'BUY', 'SELL', 'CLOSE'
            usdt: Notional amount in USDT (actually margin)
            current_open_count: Number of open positions
            current_equity: Current account equity
            current_candle: Current candle list [open_time, open, high, low, close, ...]
            next_candle: Next candle list (for realistic fill pricing)
            strategy_name: Optional strategy name for recording
            confidence: Optional signal confidence
            leverage_mult: Dynamic risk multiplier applied to configured leverage
        
        Returns:
            ExecResult with same interface as BinanceExecutor
        """
        if not current_candle:
            return ExecResult(
                action="ERROR",
                details={},
                error="BacktestExecutor requires current_candle parameter"
            )
        
        # Determine fill price (NEXT candle open, or current close as fallback)
        try:
            if next_candle:
                # Use next candle open (more realistic - can't execute on same candle you see)
                base_price = float(next_candle[1])  # Open price
                fill_source = "next_open"
            else:
                # Fallback: current candle close (end of backtest data)
                base_price = float(current_candle[4])  # Close price
                fill_source = "current_close"
            
            if base_price <= 0:
                return ExecResult(
                    action="ERROR",
                    details={},
                    error=f"Invalid price: {base_price}"
                )
        
        except (ValueError, TypeError, IndexError) as e:
            return ExecResult(
                action="ERROR",
                details={},
                error=f"Failed to parse candle data: {e}"
            )
        
        # Apply slippage
        if signal == "BUY":
            # Buy slippage: price goes UP
            fill_price = base_price * (1 + self.slippage_bps / 10000.0)
        elif signal in ("SELL", "CLOSE"):
            # Sell slippage: price goes DOWN
            fill_price = base_price * (1 - self.slippage_bps / 10000.0)
        else:
            return ExecResult(
                action="ERROR",
                details={},
                error=f"Unknown signal: {signal}"
            )
        
        # Round to broker precision
        fill_price = self.constraints.round_price(fill_price)
        
        # Determine target notional
        try:
            from app.core.config import settings
            from app.symbols.leverage import leverage_for, parse_leverage_map
            lev_map = parse_leverage_map(settings.SYMBOL_LEVERAGE_MAP)
            base_lev = leverage_for(symbol, lev_map, getattr(settings, "DEFAULT_LEVERAGE", 1), getattr(settings, "MIN_LEVERAGE", 1))
        except BaseException:
            base_lev = 1
            
        effective_lev = max(1, int(float(base_lev) * leverage_mult))
        target_notional = usdt * effective_lev
        
        # Calculate quantity
        quantity = target_notional / fill_price
        quantity = self.constraints.round_quantity(quantity)
        
        # Validate order
        is_valid, error_msg = self.constraints.validate_order(quantity, fill_price)
        if not is_valid:
            return ExecResult(
                action="ERROR",
                details={},
                error=error_msg
            )
        
        # Calculate fees
        notional = quantity * fill_price
        fee_usdt = notional * (self.fee_bps / 10000.0)
        
        # Calculate effective quantity (after fees)
        effective_notional = target_notional - fee_usdt
        effective_qty = effective_notional / fill_price
        effective_qty = self.constraints.round_quantity(effective_qty)
        
        # Generate simulated order ID
        self.fill_counter += 1
        order_id = f"backtest_{self.run_id}_{self.fill_counter}"
        
        # Record fill to database
        candle_timestamp = self._get_timestamp_from_candle(current_candle)
        self._record_fill(
            symbol=symbol,
            side=signal,
            quantity=effective_qty,
            entry_price=base_price,
            fill_price=fill_price,
            fee_usdt=fee_usdt,
            timestamp_utc=candle_timestamp,
            strategy=strategy_name,
            confidence=confidence,
            position_state="OPEN" if signal in ("BUY", "SELL") else "CLOSE"
        )
        
        # Create result matching BinanceExecutor format
        action = "ORDER_PLACED" if signal in ("BUY", "SELL") else "CLOSED_POSITION"
        
        details = {
            "order": {
                "orderId": order_id,
                "symbol": symbol,
                "side": signal,
                "avgPrice": fill_price,
                "executedQty": effective_qty,
                "status": "FILLED"
            },
            "avg_price": fill_price,
            "filled_qty": effective_qty,
            "fee_usdt": fee_usdt,
            "slippage_bps": self.slippage_bps,
            "fill_source": fill_source,
            "base_price": base_price,
            "candle_timestamp": candle_timestamp,
            "notional_usdt": notional
        }
        
        logger.debug(
            f"Simulated {signal} {symbol}: "
            f"qty={effective_qty:.6f} @ {fill_price:.2f} "
            f"(base={base_price:.2f}, source={fill_source}, fee={fee_usdt:.2f})"
        )
        
        return ExecResult(
            action=action,
            details=details,
            order_id=order_id,
            error=None
        )
    
    def _get_timestamp_from_candle(self, candle: List) -> str:
        """Extract ISO timestamp from candle"""
        try:
            open_time_ms = int(candle[0])
            dt = datetime.fromtimestamp(open_time_ms / 1000, tz=timezone.utc)
            return dt.isoformat()
        except (ValueError, IndexError):
            return utc_now_iso()
    
    def _record_fill(
        self,
        symbol: str,
        side: str,
        quantity: float,
        entry_price: float,
        fill_price: float,
        fee_usdt: float,
        timestamp_utc: str,
        strategy: Optional[str] = None,
        confidence: Optional[float] = None,
        position_state: str = "OPEN",
        pnl: float = 0.0
    ):
        """Record fill to backtest_fills table"""
        try:
            slippage_usdt = abs(fill_price - entry_price) * quantity
            
            metadata = {
                "run_id": self.run_id,
                "slippage_bps": self.slippage_bps,
                "fee_bps": self.fee_bps
            }
            
            with self.db.connect() as conn:
                conn.execute(
                    """
                    INSERT INTO backtest_fills (
                        run_id, timestamp_utc, symbol, side,
                        quantity, entry_price, fill_price,
                        fee_usdt, slippage_usdt, pnl,
                        strategy, confidence, position_state,
                        metadata_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        self.run_id, timestamp_utc, symbol, side,
                        quantity, entry_price, fill_price,
                        fee_usdt, slippage_usdt, pnl,
                        strategy, confidence, position_state,
                        json.dumps(metadata)
                    )
                )
            
            # Also store in memory for audit
            self.fills.append({
                "symbol": symbol,
                "side": side,
                "quantity": quantity,
                "fill_price": fill_price,
                "fee_usdt": fee_usdt,
                "timestamp_utc": timestamp_utc
            })
            
        except Exception as e:
            logger.warning(f"Failed to record fill to database: {e}")
    
    def get_fills(self) -> list:
        """Return all recorded fills for audit"""
        return self.fills
    
    def reset(self):
        """Reset fill history (useful between backtest runs)"""
        self.fills = []
        self.fill_counter = 0


def calculate_pnl(
    side: str,
    entry_price: float,
    exit_price: float,
    quantity: float,
    fee_bps: float = 6.0
) -> Dict[str, float]:
    """
    Calculate PnL for a closed position.
    
    Args:
        side: 'LONG' or 'SHORT'
        entry_price: Entry price
        exit_price: Exit price
        quantity: Position size
        fee_bps: Trading fee in basis points
    
    Returns:
        {
            'gross_pnl': PnL before fees,
            'fees': Total fees (entry + exit),
            'net_pnl': PnL after fees,
            'return_pct': Return percentage
        }
    """
    # Calculate gross P&L
    if side == "LONG":
        gross_pnl = (exit_price - entry_price) * quantity
    elif side == "SHORT":
        gross_pnl = (entry_price - exit_price) * quantity
    else:
        raise ValueError(f"Unknown side: {side}")
    
    # Calculate fees
    fee_multiplier = fee_bps / 10000.0
    entry_notional = entry_price * quantity
    exit_notional = exit_price * quantity
    total_fees = (entry_notional + exit_notional) * fee_multiplier
    
    # Net PnL
    net_pnl = gross_pnl - total_fees
    
    # Return percentage (based on entry notional)
    return_pct = (net_pnl / entry_notional) * 100 if entry_notional > 0 else 0
    
    return {
        'gross_pnl': gross_pnl,
        'fees': total_fees,
        'net_pnl': net_pnl,
        'return_pct': return_pct
    }


def calculate_unrealized_pnl(
    side: str,
    entry_price: float,
    current_price: float,
    quantity: float
) -> float:
    """
    Calculate unrealized PnL for an open position.
    
    Args:
        side: 'LONG' or 'SHORT'
        entry_price: Entry price
        current_price: Current market price
        quantity: Position size
    
    Returns:
        Unrealized PnL (not including fees yet)
    """
    if side == "LONG":
        return (current_price - entry_price) * quantity
    elif side == "SHORT":
        return (entry_price - current_price) * quantity
    else:
        return 0.0
