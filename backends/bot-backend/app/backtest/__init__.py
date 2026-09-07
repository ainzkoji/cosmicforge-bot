"""
Backtesting Engine - Reuses live trading pipeline for historical simulation

Components:
- data_provider.py: Historical candle fetching and caching
- executor.py: Simulated order fills
- runner.py: Main backtest execution engine
- worker.py: Background job processor
"""
from app.backtest.data_provider import HistoricalDataProvider, prefetch_historical_data
from app.backtest.executor import BacktestExecutor, calculate_pnl, calculate_unrealized_pnl
from app.backtest.runner import BacktestRunner, BacktestConfig
from app.backtest.worker import BacktestWorker

__all__ = [
    'HistoricalDataProvider',
    'prefetch_historical_data',
    'BacktestExecutor',
    'calculate_pnl',
    'calculate_unrealized_pnl',
    'BacktestRunner',
    'BacktestConfig',
    'BacktestWorker',
]
