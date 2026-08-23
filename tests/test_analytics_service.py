import pytest
from unittest.mock import MagicMock, patch
from shared_lib.persistence.analytics_service import AnalyticsService

class TestAnalyticsService:
    @pytest.fixture
    def mock_db(self):
        db = MagicMock()
        conn = MagicMock()
        db.connect.return_value.__enter__.return_value = conn
        return db

    def test_get_equity_curve_fallback_no_crash(self, mock_db):
        """
        Verify get_equity_curve does not crash when using fallback (no snapshots).
        Regression test for UnboundLocalError: cumulative_pnl
        """
        service = AnalyticsService(db=mock_db)
        conn = mock_db.connect.return_value.__enter__.return_value
        
        # 1. Snapshots query -> returns empty
        # 2. Trades query -> returns mock trades
        
        # We need to handle sequential execute calls.
        # Call 1: _ensure_tables (ignore)
        # Call 2: snapshots fetch
        # Call 3: trades fetch (if fallback)
        
        # Easier way: simulate execute returns.
        
        cursor_snapshots = MagicMock()
        cursor_snapshots.fetchall.return_value = [] # Empty snapshots
        
        cursor_trades = MagicMock()
        # Return 2 trades
        cursor_trades.fetchall.return_value = [
            {"exit_time": "2023-01-01", "realized_pnl": 100.0},
            {"exit_time": "2023-01-02", "realized_pnl": -50.0}
        ]
        
        # Side effect for conn.execute
        def execute_side_effect(query, params=()):
            if "equity_snapshots" in str(query):
                return cursor_snapshots
            if "trades" in str(query):
                return cursor_trades
            return MagicMock()
            
        conn.execute.side_effect = execute_side_effect
        
        # ACT
        curve = service.get_equity_curve("user1", timeframe="ALL")
        
        # ASSERT
        assert len(curve) == 2
        assert curve[0]["equity"] == 100.0
        assert curve[1]["equity"] == 50.0 # 100 - 50
        assert curve[0]["timestamp"] == "2023-01-01"
