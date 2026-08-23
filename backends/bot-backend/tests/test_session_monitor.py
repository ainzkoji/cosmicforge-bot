# tests/test_session_monitor.py
"""
Tests for SessionMonitor (Daily Profit Close).
"""
import pytest
import pytest
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch
try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo

from app.runner.session_monitor import SessionMonitor
from app.core.config import settings


@pytest.fixture
def monitor():
    executor = MagicMock()
    audit = MagicMock()
    return SessionMonitor(executor, audit)


@pytest.fixture
def mock_settings():
    """Context manager to temporarily override settings."""
    original_enabled = settings.DAILY_CLOSE_ENABLED
    original_window_start = settings.DAILY_CLOSE_WINDOW_START
    original_window_end = settings.DAILY_CLOSE_WINDOW_END
    original_min_usdt = settings.DAILY_CLOSE_MIN_PROFIT_USDT
    original_min_pct = settings.DAILY_CLOSE_MIN_PROFIT_PCT
    original_tz = settings.DAILY_CLOSE_TIMEZONE
    
    settings.DAILY_CLOSE_ENABLED = True
    # Utilize UTC which is always available via datetime.timezone.utc if ZoneInfo fails
    settings.DAILY_CLOSE_TIMEZONE = "UTC"
    
    yield
    
    settings.DAILY_CLOSE_ENABLED = original_enabled
    settings.DAILY_CLOSE_WINDOW_START = original_window_start
    settings.DAILY_CLOSE_WINDOW_END = original_window_end
    settings.DAILY_CLOSE_MIN_PROFIT_USDT = original_min_usdt
    settings.DAILY_CLOSE_MIN_PROFIT_PCT = original_min_pct
    settings.DAILY_CLOSE_TIMEZONE = original_tz


def test_time_window_logic(monitor, mock_settings):
    """Test standard daytime window (e.g. 14:00 to 18:00)."""
    settings.DAILY_CLOSE_WINDOW_START = "14:00"
    settings.DAILY_CLOSE_WINDOW_END = "18:00"
    
    # 1. Before window (13:59) -> Should be False
    with patch("app.runner.session_monitor.datetime") as mock_dt:
        mock_dt.now.return_value = datetime(2023, 1, 1, 13, 59, tzinfo=timezone.utc)
        
        # We wrap _scan_and_close to detect if it was called
        monitor._scan_and_close = MagicMock()
        
        monitor.check_daily_close({})
        monitor._scan_and_close.assert_not_called()
        
        # Reset throttling
        monitor.last_close_check_min = -1

        # 2. Inside window (14:00) -> Should be True
        mock_dt.now.return_value = datetime(2023, 1, 1, 14, 00, tzinfo=timezone.utc)
        monitor.check_daily_close({})
        monitor._scan_and_close.assert_called_once()


def test_overnight_window_logic(monitor, mock_settings):
    """Test overnight window (e.g. 23:00 to 01:00)."""
    settings.DAILY_CLOSE_WINDOW_START = "23:00"
    settings.DAILY_CLOSE_WINDOW_END = "01:00"
    
    with patch("app.runner.session_monitor.datetime") as mock_dt:
        monitor._scan_and_close = MagicMock()
        
        # 1. Late night (23:30) -> Inside
        monitor.last_close_check_min = -1
        mock_dt.now.return_value = datetime(2023, 1, 1, 23, 30, tzinfo=timezone.utc)
        monitor.check_daily_close({})
        monitor._scan_and_close.assert_called()
        
        # 2. Early morning (00:30) -> Inside
        monitor._scan_and_close.reset_mock()
        monitor.last_close_check_min = -1
        mock_dt.now.return_value = datetime(2023, 1, 2, 0, 30, tzinfo=timezone.utc)
        monitor.check_daily_close({})
        monitor._scan_and_close.assert_called()
        
        # 3. After window (01:01) -> Outside
        monitor._scan_and_close.reset_mock()
        monitor.last_close_check_min = -1
        mock_dt.now.return_value = datetime(2023, 1, 2, 1, 1, tzinfo=timezone.utc)
        monitor.check_daily_close({})
        monitor._scan_and_close.assert_not_called()


def test_profit_threshold_trigger(monitor, mock_settings):
    """Test that positions are only closed if profitable."""
    # Setup state
    state = {} # We iterate items()
    state_mock = MagicMock()
    state_mock.position = "LONG"
    state_mock.entry_price = 1000.0
    state["BTCUSDT"] = state_mock
    
    settings.DAILY_CLOSE_MIN_PROFIT_USDT = 10.0
    settings.DAILY_CLOSE_MIN_PROFIT_PCT = 0.0 # Ignore %
    
    # Mock executor.client.get_position logic
    monitor.executor.client.get_position = MagicMock()
    
    # Scenario 1: Not profitable enough ($5 profit < $10)
    monitor.executor.client.get_position.return_value = {
        "positionAmt": "1.0",
        "unRealizedProfit": "5.0",
        "initialMargin": "100.0"
    }
    
    with patch("app.runner.session_monitor.datetime") as mock_dt:
        mock_dt.now.return_value = datetime(2023, 1, 1, 12, 0, tzinfo=timezone.utc)
        # Force window logic to pass by calling internal method directly or strictly controlling time
        # Let's call _scan_and_close directly to test logic
        monitor._scan_and_close(state, mock_dt.now.return_value)
        
        monitor.executor.cancel_open_orders.assert_not_called()
        
    # Scenario 2: Profitable ($15 > $10)
    monitor.executor.client.get_position.return_value = {
        "positionAmt": "1.0",
        "unRealizedProfit": "15.0",
        "initialMargin": "100.0"
    }
    
    with patch("app.runner.session_monitor.datetime") as mock_dt:
        mock_dt.now.return_value = datetime(2023, 1, 1, 12, 0, tzinfo=timezone.utc)
        monitor._scan_and_close(state, mock_dt.now.return_value)
        
        # Should trigger close
        monitor.executor.cancel_open_orders.assert_called_with("BTCUSDT")
        monitor.executor.client.order_market.assert_called_with(
            symbol="BTCUSDT", side="SELL", quantity=1.0, reduce_only=True
        )

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
