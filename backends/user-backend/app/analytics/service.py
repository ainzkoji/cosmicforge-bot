from typing import Optional
from datetime import datetime, timedelta
from decimal import Decimal

from app.analytics.models import AnalyticsContext, TradeMetric, MonetaryValue
from app.analytics.fx import fx_service
# Integration point for actual data fetching in Phase 3
# from app.analytics.queries import AnalyticsQueries 

class AnalyticsService:
    """
    Coordinator service for Analytics.
    Fetches data, applies FX conversion, and aggregates metrics.
    """
    
    def __init__(self):
        self.fx = fx_service
        
    async def get_user_performance_summary(
        self, 
        context: AnalyticsContext
    ) -> TradeMetric:
        """
        Example method to demonstrate architecture.
        In a real scenario, this would:
        1. Fetch raw trade data from DB (via Repositories or Queries)
        2. Convert all PnL values to context.reporting_currency
        3. Aggregate
        """
        # Placeholder logic
        # Ideally, we'd query the DB here. 
        # For now, returning an empty metric structure to satisfy type checking
        # and demonstrate the model usage.
        
        return TradeMetric(
            total_trades=0,
            winning_trades=0,
            losing_trades=0,
            win_rate=0.0,
            profit_factor=0.0,
            total_pnl=MonetaryValue(amount=Decimal("0.0"), currency=context.reporting_currency)
        )

    async def convert_monetary_value(self, value: MonetaryValue, target_currency: str) -> MonetaryValue:
        """
        Exposes FX normalization to other parts of the app.
        """
        return self.fx.normalize_to_reporting_currency(value, target_currency)

analytics_service = AnalyticsService()
