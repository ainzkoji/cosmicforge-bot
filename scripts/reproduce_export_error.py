import sys
import os
import asyncio
from fastapi import HTTPException
import traceback

# Add paths for imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../backends/bot-backend")))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../backends/shared")))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../backends")))

try:
    from app.api.analytics import export_analytics
    from shared_lib.persistence.analytics_service import AnalyticsService
except ImportError as e:
    print(f"Import Error: {e}")
    sys.exit(1)

# Mock user
mock_user = {"id": "test_user"}

async def run_test():
    print("Testing export_analytics(format='csv')...")
    try:
        response = await export_analytics(timeframe="YTD", format="csv", user=mock_user)
        print("Success!")
    except Exception as e:
        print(f"Caught Exception: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(run_test())
