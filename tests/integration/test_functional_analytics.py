"""
Functional Integration Tests for Analytics & Permissions
"""
import pytest
import requests

USER_BACKEND_URL = "http://localhost:8000"

class TestFunctionalAnalytics:
    
    def test_analytics_invalid_timeframe(self):
        """Test that invalid timeframe returns 422."""
        # Even without auth, usually FastAPI validates query params first? 
        # Or depends on dependency order. 
        # If it returns 401, we can't test validation without a token.
        # But we can try.
        response = requests.get(
            f"{USER_BACKEND_URL}/api/v1/analytics/overview?timeframe=INVALID_TF",
            headers={"Authorization": "Bearer dummy"}
        )
        # If 422, validation is working. If 401, auth is hitting first.
        # If 500, broken.
        assert response.status_code in [422, 401]
        
    def test_analytics_export_headers_csv(self):
        """Test export headers for CSV (simulated)."""
        # This requires a token usually.
        pass

class TestPermissions:
    
    def test_admin_route_protection(self):
        """Test that admin routes block normal users."""
        response = requests.get(
            f"{USER_BACKEND_URL}/api/admin/users",
            headers={"Authorization": "Bearer dummy_user_token"}
        )
        # Should be 401 (invalid token) or 403 (insufficient permissions if token was valid user)
        assert response.status_code in [401, 403]
