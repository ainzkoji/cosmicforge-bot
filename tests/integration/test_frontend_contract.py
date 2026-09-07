"""
Contract Tests - Frontend API Expectations

Tests that verify all routes expected by the frontend actually exist.
This prevents breaking changes where backend routes are renamed/removed
without updating the frontend.

Based on ROUTE_INVENTORY.md and frontend API client code.
"""

import pytest
import requests
from typing import List, Dict

USER_BACKEND_URL = "http://localhost:8000"
BOT_BACKEND_URL = "http://localhost:9000"
TEST_TIMEOUT = 5


# Frontend expected endpoints (from user-frontend/src/api/client.ts)
FRONTEND_EXPECTED_ROUTES = [
    # Authentication
    {"method": "POST", "path": "/auth/register", "description": "User registration"},
    {"method": "POST", "path": "/auth/login", "description": "User login"},
    {"method": "POST", "path": "/auth/refresh", "description": "Refresh token"},
    {"method": "GET", "path": "/auth/me", "description": "Get current user"},
    
    # Public routes (no auth required)
    {"method": "GET", "path": "/public/home", "description": "Home page content"},
    {"method": "GET", "path": "/public/features", "description": "Features list"},
    {"method": "GET", "path": "/public/pricing", "description": "Pricing plans"},
    
    # KYC
    {"method": "POST", "path": "/kyc/cases", "description": "Create KYC case"},
    {"method": "GET", "path": "/kyc/cases/{case_id}", "description": "Get KYC status"},
    
    # Brokers
    {"method": "GET", "path": "/api/brokers/catalog", "description": "List brokers"},
    {"method": "POST", "path": "/api/brokers/connections", "description": "Connect broker"},
    {"method": "GET", "path": "/api/brokers/connections", "description": "List connections"},
    
    # Billing
    {"method": "GET", "path": "/api/billing/plans", "description": "List subscription plans"},
    {"method": "POST", "path": "/api/billing/subscriptions", "description": "Create subscription"},
    
    # Onboarding
    {"method": "GET", "path": "/api/onboarding/state", "description": "Onboarding state"},
    {"method": "POST", "path": "/api/onboarding/step", "description": "Save step progress"},
    {"method": "POST", "path": "/api/onboarding/complete", "description": "Complete onboarding"},
    
    # Bot Instances (proxied to bot-backend)
    {"method": "GET", "path": "/api/v1/bot-instances", "description": "List bot instances"},
    {"method": "POST", "path": "/api/v1/bot-instances/apply-official-strategy", "description": "Deploy strategy"},
    {"method": "GET", "path": "/api/v1/bot-instances/{instance_id}", "description": "Get bot instance"},
    {"method": "PUT", "path": "/api/v1/bot-instances/{instance_id}", "description": "Update bot instance"},
    {"method": "POST", "path": "/api/v1/bot-instances/{instance_id}/start", "description": "Start bot"},
    {"method": "POST", "path": "/api/v1/bot-instances/{instance_id}/pause", "description": "Pause bot"},
    {"method": "POST", "path": "/api/v1/bot-instances/{instance_id}/stop", "description": "Stop bot"},
    {"method": "DELETE", "path": "/api/v1/bot-instances/{instance_id}", "description": "Delete bot"},
    
    # Strategy Marketplace (proxied to bot-backend)
    {"method": "GET", "path": "/api/v1/strategies/marketplace", "description": "List strategies"},
    {"method": "GET", "path": "/api/v1/strategies/marketplace/{strategy_id}", "description": "Get strategy details"},
    
    # Analytics (proxied to bot-backend)
    {"method": "GET", "path": "/api/v1/analytics/overview", "description": "Analytics overview"},
    {"method": "GET", "path": "/api/v1/analytics/leaderboard", "description": "Strategy leaderboard"},
    {"method": "GET", "path": "/api/v1/analytics/calibration", "description": "Confidence calibration"},
    
    # Strategy Configs (proxied to bot-backend)
    {"method": "GET", "path": "/api/v1/strategy-configs", "description": "List strategy configs"},
    {"method": "POST", "path": "/api/v1/strategy-configs", "description": "Create config"},
    {"method": "GET", "path": "/api/v1/strategy-configs/{config_id}", "description": "Get config"},
    {"method": "PUT", "path": "/api/v1/strategy-configs/{config_id}", "description": "Update config"},
    {"method": "POST", "path": "/api/v1/strategy-configs/{config_id}/activate", "description": "Activate config"},
    
    # Risk Profiles (proxied to bot-backend)
    {"method": "GET", "path": "/api/v1/risk-profiles/templates", "description": "Risk profile templates"},
    {"method": "POST", "path": "/api/v1/risk-profiles/calculate", "description": "Calculate position size"},
    {"method": "POST", "path": "/api/v1/risk-profiles/validate", "description": "Validate risk params"},
    
    # Portfolio
    {"method": "GET", "path": "/api/portfolio/summary", "description": "Portfolio summary"},
    {"method": "GET", "path": "/api/portfolio/transactions", "description": "Transaction history"},
    
    # Admin routes
    {"method": "GET", "path": "/api/admin/dashboard/stats", "description": "Admin dashboard stats"},
    {"method": "GET", "path": "/api/admin/users", "description": "List all users"},
    {"method": "GET", "path": "/api/admin/revenue/overview", "description": "Revenue analytics"},
    {"method": "GET", "path": "/api/admin/audit-logs", "description": "Audit logs"},
]


class TestFrontendContract:
    """Verify all frontend expected endpoints exist."""
    
    @pytest.mark.parametrize("route", FRONTEND_EXPECTED_ROUTES, ids=lambda r: f"{r['method']} {r['path']}")
    def test_frontend_expected_route_exists(self, route: Dict[str, str]):
        """
        Test that each route expected by frontend exists.
        
        We don't test functionality, just that the route is registered.
        A 404 means the route doesn't exist (contract broken).
        Other status codes (401, 422, 400, etc.) mean the route exists but needs auth/validation.
        """
        method = route["method"]
        path = route["path"]
        description = route["description"]
        
        # Replace path parameters with dummy values
        test_path = path.replace("{case_id}", "test-id")
        test_path = test_path.replace("{instance_id}", "test-id")
        test_path = test_path.replace("{strategy_id}", "test-id")
        test_path = test_path.replace("{config_id}", "test-id")
        test_path = test_path.replace("{id}", "test-id")
        
        url = f"{USER_BACKEND_URL}{test_path}"
        
        try:
            if method == "GET":
                response = requests.get(url, timeout=TEST_TIMEOUT)
            elif method == "POST":
                response = requests.post(url, json={}, timeout=TEST_TIMEOUT)
            elif method == "PUT":
                response = requests.put(url, json={}, timeout=TEST_TIMEOUT)
            elif method == "DELETE":
                response = requests.delete(url, timeout=TEST_TIMEOUT)
            else:
                pytest.fail(f"Unknown HTTP method: {method}")
            
            # 404 means route doesn't exist - CONTRACT BROKEN!
            assert response.status_code != 404, \
                f"Route not found: {method} {path} ({description}). " \
                f"Frontend expects this route but backend doesn't provide it!"
            
            # Any other status is fine - route exists
            # 401 = auth required
            # 422 = validation error
            # 400 = bad request
            # 503 = service unavailable (bot-backend down for proxied routes)
            assert response.status_code in [200, 201, 400, 401, 403, 422, 503], \
                f"Unexpected status for {method} {path}: {response.status_code}"
        
        except requests.exceptions.ConnectionError:
            pytest.fail(f"Cannot connect to user-backend at {USER_BACKEND_URL}")


class TestNoUnexpectedRoutes:
    """Test that we haven't left old deprecated routes active."""
    
    def test_old_analytics_route_not_active(self):
        """Old /api/analytics should return 404."""
        response = requests.get(f"{USER_BACKEND_URL}/api/analytics/overview", timeout=TEST_TIMEOUT)
        assert response.status_code == 404, \
            "Old /api/analytics route still active! Should be /api/v1/analytics"
    
    def test_old_admin_monitoring_route_not_active(self):
        """Old /admin/monitoring should return 404."""
        response = requests.get(f"{USER_BACKEND_URL}/admin/monitoring/system-health", timeout=TEST_TIMEOUT)
        assert response.status_code == 404, \
            "Old /admin/monitoring route still active! Should be /api/v1/monitoring"


class TestHealthEndpoints:
    """Ensure basic health endpoints work."""
    
    def test_user_backend_health(self):
        """User-backend health endpoint should return 200."""
        response = requests.get(f"{USER_BACKEND_URL}/health", timeout=TEST_TIMEOUT)
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
    
    def test_user_backend_root(self):
        """User-backend root should return service info."""
        response = requests.get(f"{USER_BACKEND_URL}/", timeout=TEST_TIMEOUT)
        assert response.status_code == 200
        data = response.json()
        assert "service" in data


if __name__ == "__main__":
    # Run tests
    pytest.main([__file__, "-v", "--tb=short"])
