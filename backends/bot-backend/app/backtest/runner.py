"""
Backtest Runner - Time-driven backtesting engine

DESIGN:
- Mirrors PaperRunner.run_cycle / step_symbol structure
- Drives time forward using historical candles from HistoricalDataProvider
- Reuses TradingOrchestrator, SafetyEngine, strategy loader
- Maintains in-memory SymbolState (same as live trading)
- Records equity curve and updates progress

EXECUTION MODEL:
For each candle timestamp t from start → end:
    for each symbol:
        - Get historical context window
        - Call orchestrator.process_trading_opportunity()
        - Execute fills via BacktestExecutor
        - Update state and equity accounting
        - Record equity curve snapshot
        - Update progress
"""
import logging
import json
import time
import uuid
from typing import Dict, Any, List, Optional, Callable
from datetime import datetime, timezone, date
from dataclasses import dataclass

from app.backtest.data_provider import HistoricalDataProvider
from app.backtest.executor import BacktestExecutor, calculate_unrealized_pnl
from app.runner.models import SymbolState
from app.strategy.loader import build_strategy
from app.core.trading_orchestrator import TradingOrchestrator
from app.risk.system_limits import UserConfigurableLimits, RiskLevel
from shared_lib.persistence.db import DB
from shared_lib.persistence.audit import Audit

logger = logging.getLogger(__name__)


def utc_now_iso() -> str:
    """Return current UTC time as ISO string"""
    return datetime.now(timezone.utc).isoformat()


@dataclass
class BacktestConfig:
    """Configuration for a backtest run"""
    run_id: str
    user_id: str
    name: str
    
    # Strategy
    strategy_id: str
    strategy_params: Optional[Dict] = None
    
    # Symbols and timeframe
    symbols: List[str] = None
    interval: str = "1m"
    
    # Date range
    start_date: str = None  # ISO format or YYYY-MM-DD
    end_date: str = None
    
    # Capital and risk
    initial_capital: float = 10000.0
    slippage_bps: float = 10.0
    fee_bps: float = 6.0
    
    # Risk limits (mirrors live trading)
    max_daily_loss_pct: float = 0.05
    max_trades_daily: int = 20
    max_open_positions: int = 3
    stop_loss_pct: float = 0.02
    max_leverage: int = 1
    
    # Data source
    data_source: str = "binance"
    market_type: str = "crypto"
    
    # Trading config
    trade_usdt_per_order: float = 1000.0
    risk_level: str = "medium"
    
    def __post_init__(self):
        if not self.symbols:
            self.symbols = ["BTCUSDT"]


class BacktestRunner:
    """
    Time-driven backtest runner mirroring PaperRunner structure.
    
    Key design:
    - Iterates through historical candles (time-driven)
    - Reuses TradingOrchestrator for decision making
    - Maintains SymbolState dict (same as live)
    - Uses BacktestExecutor for simulated fills
    - Records equity curve and metrics
    """
    
    def __init__(
        self,
        config: BacktestConfig,
        db: Optional[DB] = None,
        cancellation_check: Optional[Callable[[], bool]] = None
    ):
        """
        Args:
            config: Backtest configuration
            db: Database connection (creates new if not provided)
            cancellation_check: Optional callback returning True if run should be cancelled
        """
        self.config = config
        self.cancellation_check = cancellation_check
        
        # Database and audit
        self.db = db or DB()
        self.audit = Audit(self.db)
        
        # Initialize components
        self._init_data_provider()
        self._init_executor()
        self._init_strategy()
        self._init_orchestrator()
        
        # State management (mirrors PaperRunner)
        self.state: Dict[str, SymbolState] = {
            symbol: SymbolState() for symbol in config.symbols
        }
        
        # Accounting
        self.cash_balance = config.initial_capital
        self.equity = config.initial_capital
        self.peak_equity = config.initial_capital
        self.max_drawdown_pct = 0.0
        
        # Metrics
        self.total_trades = 0
        self.winning_trades = 0
        self.losing_trades = 0
        self.total_fees = 0.0
        self.gross_pnl = 0.0
        self.net_pnl = 0.0
        
        # Equity curve history
        self.equity_curve: List[Dict] = []
        
        # Progress tracking
        self.current_candle_idx = 0
        self.total_candles = 0
        
        # Cycle tracking
        self.cycle_id = None
        
    def _init_data_provider(self):
        """Initialize historical data provider"""
        self.data_provider = HistoricalDataProvider(
            data_source=self.config.data_source,
            market_type=self.config.market_type,
            use_db_cache=True,
            use_memory_cache=True,
            db=self.db
        )
    
    def _init_executor(self):
        """Initialize backtest executor"""
        from app.backtest.executor import BacktestConstraints
        
        self.executor = BacktestExecutor(
            run_id=self.config.run_id,
            slippage_bps=self.config.slippage_bps,
            fee_bps=self.config.fee_bps,
            constraints=BacktestConstraints(),
            db=self.db
        )
    
    def _init_strategy(self):
        """Initialize trading strategy"""
        self.strategy = build_strategy(
            name=self.config.strategy_id,
            client=None,  # No client needed for backtest
            interval=self.config.interval,
            params_json=json.dumps(self.config.strategy_params) if self.config.strategy_params else None
        )
    
    def _init_orchestrator(self):
        """Initialize trading orchestrator (reused from live trading)"""
        try:
            risk_level = RiskLevel(self.config.risk_level)
        except ValueError:
            risk_level = RiskLevel.MEDIUM
        
        user_config = UserConfigurableLimits(
            risk_level=risk_level,
            max_daily_loss_pct=self.config.max_daily_loss_pct,
            max_trades_per_day=self.config.max_trades_daily,
            max_open_positions=self.config.max_open_positions,
            default_stop_loss_pct=self.config.stop_loss_pct,
            requested_leverage={s: self.config.max_leverage for s in self.config.symbols},
            allowed_symbols=self.config.symbols,
            paper_mode=True,  # Backtest is similar to paper
            min_strategy_confidence=0.5,
            strict_circuit_breakers=False
        )
        
        self.orchestrator = TradingOrchestrator(
            config_id=self.config.run_id,
            user_config=user_config,
            strategy_id=self.config.strategy_id,
            broker_id="backtest",
            strategy_instance=self.strategy
        )
    
    def run(self) -> Dict[str, Any]:
        """
        Execute the full backtest.
        
        Returns:
            Summary dict with metrics and results
        """
        logger.info(f"Starting backtest {self.config.run_id}: {self.config.name}")
        logger.info(f"Symbols: {self.config.symbols}, Interval: {self.config.interval}")
        logger.info(f"Period: {self.config.start_date} to {self.config.end_date}")
        logger.info(f"Initial Capital: ${self.config.initial_capital:,.2f}")
        
        # Parse dates to timestamps
        start_ms, end_ms = self._parse_date_range()
        
        # Update job status to running
        self._update_job_status("running", progress_pct=0.0)
        
        # Count total candles for progress tracking
        self.total_candles = self.data_provider.get_total_candles(
            self.config.symbols[0],  # Use first symbol for count
            self.config.interval,
            start_ms,
            end_ms
        )
        logger.info(f"Total candles to process: {self.total_candles:,}")
        
        # Main backtest loop
        try:
            self._run_backtest_loop(start_ms, end_ms)
        except Exception as e:
            logger.error(f"Backtest failed: {e}", exc_info=True)
            self._update_job_status("failed", error=str(e))
            self._finalize_backtest_run(status="error", error_message=str(e))
            raise
        
        # Finalize results
        results = self._finalize_backtest()
        
        logger.info(f"Backtest complete: {self.total_trades} trades, "
                   f"Net PnL: ${self.net_pnl:,.2f}, "
                   f"Max DD: {self.max_drawdown_pct:.2f}%")
        
        return results
    
    def _run_backtest_loop(self, start_ms: int, end_ms: int):
        """Main backtest loop - iterate through time"""
        # Get all candles for primary symbol (for iteration)
        primary_symbol = self.config.symbols[0]
        
        # Preload data for performance (optional but recommended)
        for symbol in self.config.symbols:
            logger.info(f"Preloading data for {symbol}...")
            self.data_provider.preload_data(symbol, self.config.interval, start_ms, end_ms)
        
        # Iterate through each candle timestamp
        candle_iterator = self.data_provider.iter_candles(
            primary_symbol,
            self.config.interval,
            start_ms,
            end_ms
        )
        
        all_candles = list(candle_iterator)  # Convert to list for next-candle access
        
        for idx, current_candle in enumerate(all_candles):
            self.current_candle_idx = idx
            current_timestamp = int(current_candle[0])
            
            # Get next candle for realistic fill execution
            next_candle = all_candles[idx + 1] if idx + 1 < len(all_candles) else None
            
            # Process each symbol at this timestamp
            for symbol in self.config.symbols:
                self._step_symbol(
                    symbol=symbol,
                    current_timestamp=current_timestamp,
                    current_candle=current_candle,
                    next_candle=next_candle
                )
            
            # Record equity snapshot (every N candles to avoid bloat)
            if idx % self._get_equity_snapshot_frequency() == 0:
                self._record_equity_snapshot(current_timestamp)
            
            # Update progress and check cancellation
            if idx % 100 == 0:  # Update every 100 candles
                # Check for cancellation
                if self.cancellation_check and self.cancellation_check():
                     logger.info("Backtest cancelled by user request.")
                     raise RuntimeError("Backtest cancelled")

                progress_pct = (idx / len(all_candles)) * 100
                self._update_job_progress(idx, len(all_candles), progress_pct)
                
                if idx % 1000 == 0:
                    logger.info(f"Progress: {progress_pct:.1f}% ({idx}/{len(all_candles)} candles)")
        
        # Final equity snapshot
        if all_candles:
            final_timestamp = int(all_candles[-1][0])
            self._record_equity_snapshot(final_timestamp)
    
    def _step_symbol(
        self,
        symbol: str,
        current_timestamp: int,
        current_candle: List,
        next_candle: Optional[List]
    ):
        """
        Process one symbol at one point in time (mirrors PaperRunner.step_symbol).
        
        Args:
            symbol: Trading pair
            current_timestamp: Unix timestamp ms of current candle
            current_candle: Current candle data
            next_candle: Next candle for fill execution
        """
        st = self.state[symbol]
        
        # 1. Get historical klines for strategy
        klines = self.data_provider.get_klines_window(
            symbol=symbol,
            interval=self.config.interval,
            end_open_time_ms=current_timestamp,
            lookback=100  # Standard lookback for indicators
        )
        
        if not klines or len(klines) < 2:
            return  # Not enough data
        
        # 2. Get current price from candle
        current_price = float(current_candle[4])  # Close price
        
        # 3. Check for exits (if position open)
        if st.position in ("LONG", "SHORT"):
            # Simple exit check (can enhance with orchestrator exit logic)
            should_exit = self._check_exit_conditions(st, current_price)
            
            if should_exit:
                self._execute_close(symbol, st, current_candle, next_candle, current_price)
                return
        
        # 4. Calculate account metrics for orchestrator
        equity = self._calculate_current_equity(current_price)
        open_positions = sum(1 for s in self.state.values() if s.position in ("LONG", "SHORT"))
        total_exposure = sum(
            (s.entry_price or 0.0) * (s.entry_qty or 0.0)
            for s in self.state.values()
            if s.position in ("LONG", "SHORT")
        )
        
        # 5. Call orchestrator (reused from live trading)
        try:
            orch_result = self.orchestrator.process_trading_opportunity(
                symbol=symbol,
                klines=klines,
                current_price=current_price,
                current_equity=equity,
                margin_used=total_exposure,
                margin_available=self.cash_balance,
                open_positions=open_positions,
                total_exposure=total_exposure,
                client=None,  # No client in backtest
                run_id=self.config.run_id
            )
        except Exception as e:
            logger.warning(f"Orchestrator error for {symbol}: {e}")
            return
        
        decision = orch_result.get("decision")
        
        # Update last signal
        strategy_output = orch_result.get("details", {}).get("strategy_output", {})
        st.last_signal = strategy_output.get("signal", "HOLD")
        
        # 6. Execute trade if decision is "execute"
        if decision == "execute" and st.position == "NONE":
            trade_params = orch_result.get("trade_params", {})
            
            if trade_params:
                self._execute_entry(
                    symbol,
                    st,
                    trade_params,
                    current_candle,
                    next_candle,
                    current_price,
                    open_positions,
                    equity
                )
    
    def _execute_entry(
        self,
        symbol: str,
        st: SymbolState,
        trade_params: Dict,
        current_candle: List,
        next_candle: Optional[List],
        current_price: float,
        open_positions: int,
        equity: float
    ):
        """Execute entry trade via BacktestExecutor"""
        side = trade_params.get("side")  # "BUY" or "SELL"
        quantity = trade_params.get("quantity", 0)
        entry_price = trade_params.get("entry_price", current_price)
        
        # Calculate trade USDT
        trade_usdt = quantity * entry_price
        
        # Execute via BacktestExecutor
        result = self.executor.execute_signal(
            symbol=symbol,
            signal=side,
            usdt=trade_usdt,
            current_open_count=open_positions,
            current_equity=equity,
            current_candle=current_candle,
            next_candle=next_candle,
            strategy_name=self.config.strategy_id,
            confidence=trade_params.get("confidence", 0.5)
        )
        
        if result.success:
            # Update state
            st.position = "LONG" if side == "BUY" else "SHORT"
            st.entry_price = result.avg_price
            st.entry_qty = result.details.get("filled_qty", quantity)
            st.last_action = side
            st.last_trade_ms = int(current_candle[0])
            
            # Update accounting
            fee = result.details.get("fee_usdt", 0.0)
            self.cash_balance -= trade_usdt
            self.cash_balance -= fee
            self.total_fees += fee
            self.total_trades += 1
            
            # Feedback to orchestrator
            self.orchestrator.record_trade_execution(
                symbol=symbol,
                success=True,
                expected_price=entry_price,
                executed_price=result.avg_price,
                error_message=None
            )
            
            logger.debug(
                f"✅ ENTRY: {side} {symbol} @ {result.avg_price:.2f} "
                f"(qty={st.entry_qty:.6f}, fee=${fee:.2f})"
            )
    
    def _execute_close(
        self,
        symbol: str,
        st: SymbolState,
        current_candle: List,
        next_candle: Optional[List],
        current_price: float
    ):
        """Execute position close"""
        if not st.entry_price or not st.entry_qty:
            return
        
        # Calculate exit USDT
        exit_usdt = st.entry_qty * current_price
        
        # Execute via BacktestExecutor
        result = self.executor.execute_signal(
            symbol=symbol,
            signal="CLOSE",
            usdt=exit_usdt,
            current_candle=current_candle,
            next_candle=next_candle,
            strategy_name=self.config.strategy_id
        )
        
        if result.success:
            # Calculate PnL
            from app.backtest.executor import calculate_pnl
            
            pnl_data = calculate_pnl(
                side=st.position,
                entry_price=st.entry_price,
                exit_price=result.avg_price,
                quantity=st.entry_qty,
                fee_bps=self.config.fee_bps
            )
            
            # Update accounting
            self.cash_balance += exit_usdt
            self.cash_balance -= pnl_data['fees']
            self.total_fees += pnl_data['fees']
            self.gross_pnl += pnl_data['gross_pnl']
            self.net_pnl += pnl_data['net_pnl']
            
            # Update trade stats
            self.total_trades += 1
            if pnl_data['net_pnl'] > 0:
                self.winning_trades += 1
            else:
                self.losing_trades += 1
            
            # Reset state
            st.position = "NONE"
            st.entry_price = None
            st.entry_qty = 0.0
            st.last_action = "CLOSE"
            st.last_trade_ms = int(current_candle[0])
            
            logger.debug(
                f"✅ CLOSE: {symbol} @ {result.avg_price:.2f} "
                f"(PnL=${pnl_data['net_pnl']:.2f}, fees=${pnl_data['fees']:.2f})"
            )
    
    def _check_exit_conditions(self, st: SymbolState, current_price: float) -> bool:
        """Check if position should be exited (simple stop-loss check)"""
        if not st.entry_price:
            return False
        
        # Simple stop-loss check
        if st.position == "LONG":
            loss_pct = (st.entry_price - current_price) / st.entry_price
            if loss_pct >= self.config.stop_loss_pct:
                return True
        elif st.position == "SHORT":
            loss_pct = (current_price - st.entry_price) / st.entry_price
            if loss_pct >= self.config.stop_loss_pct:
                return True
        
        return False
    
    def _calculate_current_equity(self, current_price: float) -> float:
        """Calculate current equity (cash + unrealized PnL)"""
        unrealized_pnl = 0.0
        
        for symbol, st in self.state.items():
            if st.position in ("LONG", "SHORT") and st.entry_price and st.entry_qty:
                upnl = calculate_unrealized_pnl(
                    side=st.position,
                    entry_price=st.entry_price,
                    current_price=current_price,
                    quantity=st.entry_qty
                )
                unrealized_pnl += upnl
        
        self.equity = self.cash_balance + unrealized_pnl
        
        # Update peak and drawdown
        if self.equity > self.peak_equity:
            self.peak_equity = self.equity
        
        if self.peak_equity > 0:
            drawdown_pct = ((self.peak_equity - self.equity) / self.peak_equity) * 100
            if drawdown_pct > self.max_drawdown_pct:
                self.max_drawdown_pct = drawdown_pct
        
        return self.equity
    
    def _record_equity_snapshot(self, timestamp_ms: int):
        """Record equity curve snapshot to database"""
        try:
            dt = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
            timestamp_iso = dt.isoformat()
            
            unrealized_pnl = self.equity - self.cash_balance
            drawdown_usdt = self.peak_equity - self.equity
            
            with self.db.connect() as conn:
                conn.execute(
                    """
                    INSERT INTO backtest_equity_curve (
                        run_id, timestamp_utc, balance, equity,
                        unrealized_pnl, realized_pnl, drawdown_pct,
                        drawdown_usdt, peak_equity, quote_currency
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        self.config.run_id, timestamp_iso, self.cash_balance, self.equity,
                        unrealized_pnl, self.net_pnl, self.max_drawdown_pct,
                        drawdown_usdt, self.peak_equity, "USDT"
                    )
                )
            
            # Also store in memory for final results
            self.equity_curve.append({
                "timestamp": timestamp_iso,
                "balance": self.cash_balance,
                "equity": self.equity,
                "unrealized_pnl": unrealized_pnl
            })
            
        except Exception as e:
            logger.warning(f"Failed to record equity snapshot: {e}")
    
    def _get_equity_snapshot_frequency(self) -> int:
        """Determine how often to record equity snapshots"""
        # Record every candle for 1m, every 5 for 5m, etc.
        interval_map = {
            "1m": 1,
            "5m": 1,
            "15m": 1,
            "1h": 1,
            "4h": 1,
            "1d": 1
        }
        return interval_map.get(self.config.interval, 1)
    
    def _update_job_status(self, status: str, progress_pct: float = None, error: str = None):
        """Update backtest_jobs table with current status"""
        try:
            with self.db.connect() as conn:
                updates = {"status": status, "updated_at": utc_now_iso()}
                
                if progress_pct is not None:
                    updates["progress_pct"] = progress_pct
                
                if error:
                    updates["last_error"] = error
                
                set_clause = ", ".join(f"{k} = ?" for k in updates.keys())
                values = list(updates.values()) + [self.config.run_id]
                
                conn.execute(
                    f"UPDATE backtest_jobs SET {set_clause} WHERE run_id = ?",
                    values
                )
        except Exception as e:
            logger.warning(f"Failed to update job status: {e}")
    
    def _update_job_progress(self, current: int, total: int, progress_pct: float):
        """Update job progress tracking"""
        try:
            with self.db.connect() as conn:
                conn.execute(
                    """
                    UPDATE backtest_jobs
                    SET current_candle = ?,
                        total_candles = ?,
                        progress_pct = ?,
                        updated_at = ?
                    WHERE run_id = ?
                    """,
                    (current, total, progress_pct, utc_now_iso(), self.config.run_id)
                )
        except Exception as e:
            logger.warning(f"Failed to update progress: {e}")
    
    def _finalize_backtest(self) -> Dict[str, Any]:
        """Finalize backtest and calculate final metrics"""
        # Calculate win rate
        win_rate = (self.winning_trades / self.total_trades * 100) if self.total_trades > 0 else 0.0
        
        # Calculate return
        total_return = ((self.equity - self.config.initial_capital) / self.config.initial_capital) * 100
        
        # Update backtest_runs table
        self._finalize_backtest_run(
            status="completed",
            total_trades=self.total_trades,
            win_rate=win_rate,
            net_pnl=self.net_pnl,
            gross_pnl=self.gross_pnl,
            total_fees=self.total_fees,
            max_drawdown=self.max_drawdown_pct
        )
        
        # Update job status
        self._update_job_status("completed", progress_pct=100.0)
        
        return {
            "status": "completed",
            "run_id": self.config.run_id,
            "initial_capital": self.config.initial_capital,
            "final_equity": self.equity,
            "total_return_pct": total_return,
            "total_trades": self.total_trades,
            "winning_trades": self.winning_trades,
            "losing_trades": self.losing_trades,
            "win_rate": win_rate,
            "gross_pnl": self.gross_pnl,
            "net_pnl": self.net_pnl,
            "total_fees": self.total_fees,
            "max_drawdown_pct": self.max_drawdown_pct,
            "equity_curve_points": len(self.equity_curve)
        }
    
    def _finalize_backtest_run(
        self,
        status: str,
        total_trades: int = 0,
        win_rate: float = 0.0,
        net_pnl: float = 0.0,
        gross_pnl: float = 0.0,
        total_fees: float = 0.0,
        max_drawdown: float = 0.0,
        error_message: str = None
    ):
        """Update backtest_runs table with final results"""
        try:
            with self.db.connect() as conn:
                conn.execute(
                    """
                    UPDATE backtest_runs
                    SET status = ?,
                        total_trades = ?,
                        win_rate = ?,
                        net_pnl = ?,
                        gross_pnl = ?,
                        total_fees = ?,
                        max_drawdown = ?,
                        progress_pct = 100.0,
                        completed_at = ?,
                        updated_at = ?,
                        error_message = ?
                    WHERE id = ?
                    """,
                    (
                        status, total_trades, win_rate, net_pnl, gross_pnl,
                        total_fees, max_drawdown, utc_now_iso(), utc_now_iso(),
                        error_message, self.config.run_id
                    )
                )
        except Exception as e:
            logger.error(f"Failed to finalize backtest run: {e}")
    
    def _parse_date_range(self) -> tuple[int, int]:
        """Parse start_date and end_date to Unix timestamps (ms)"""
        # Handle ISO format or YYYY-MM-DD
        start_str = self.config.start_date
        end_str = self.config.end_date
        
        if 'T' not in start_str:
            start_str += 'T00:00:00Z'
        if 'T' not in end_str:
            end_str += 'T23:59:59Z'
        
        start_dt = datetime.fromisoformat(start_str.replace('Z', '+00:00'))
        end_dt = datetime.fromisoformat(end_str.replace('Z', '+00:00'))
        
        start_ms = int(start_dt.timestamp() * 1000)
        end_ms = int(end_dt.timestamp() * 1000)
        
        return start_ms, end_ms
