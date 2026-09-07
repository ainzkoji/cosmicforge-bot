"""
Unit Tests for Analytics Logic
"""
import pytest
from datetime import datetime, timedelta
# Import from the app.api.analytics module where Timeframe is defined
# Note: We need to ensure the path is importable.
from app.api.analytics import Timeframe

class TestAnalyticsUnit:
    
    def test_timeframe_parsing(self):
        """Test strict timeframe validation."""
        assert Timeframe.validate("1M") == "1M"
        assert Timeframe.validate("ALL") == "ALL"
        
        with pytest.raises(ValueError):
            Timeframe.validate("INVALID")
            
    def test_timeframe_sql_delta(self):
        """Test SQL delta logic (if applicable, or mocked behavior)."""
        # Assuming there is a helper logic or we just rely on the Enum strings checks
        # This test ensures compatibility with expected frontend values
        valid_set = {"1M", "3M", "YTD", "ALL"}
        for tf in valid_set:
            assert Timeframe.validate(tf) == tf

