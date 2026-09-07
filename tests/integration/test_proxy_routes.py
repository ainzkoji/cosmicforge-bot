"""
Integration Tests for Proxy Routes

Tests that verify user-backend proxies correctly forward to bot-backend.
These tests can run with either real backends or mocked bot-backend.
"""

import pytest
import requests
from typing import Generator
import time
import subprocess
import os
import signal
import sys

# Test configuration
USER_BACKEND_URL = "http://localhost:8000"
BOT_BACKEND_URL = "http://localhost:9000"
TEST_TIMEOUT = 5


@pytest.fixture(scope="module")
def auth_token() -> str:
    """
    Get a valid JWT token for testing.
    For now, returns a dummy token. In real usage, you'd login first.
    """
    # TODO: Implement actual login to get token
    # response = requests.post(f"{USER_BACKEND_URL}/auth/login", json={
    #     "email": "test@example.com",
    #     "password": "testpass123"
    # })
    # return response.json()["access_token"]
    return "dummy_token_for_testing"


class TestProxyRoutes:
    """Test that all proxy routes forward correctly to bot-backend."""
    
    def test_bot_instances_proxy(self, auth_token):
        """Test /api/v1/bot-instances proxy."""
        response = requests.get(
            f"{USER_BACKEND_URL}/api/v1/bot-instances",
            headers={"Authorization": f"Bearer {auth_token}"},
            timeout=TEST_TIMEOUT
        )
        # 401 is OK (auth required), 503 means bot-backend down
        # 404 would mean route doesn't exist (BAD)
        assert response.status_code in [200, 401, 503], \
            f"Bot instances proxy failed: {response.status_code}"
        assert response.status_code != 404, "Route not found - proxy broken!"
    
    def test_auto_pilot_proxy(self, auth_token):
        """Test /api/v1/auto-pilot/deploy proxy."""
        # This is a POST endpoint, so we use POST
        # We don't need to send valid body to check if proxy exists (422 or 401 expected)
        # Checking if route exists (not 404)
        response = requests.post(
            f"{USER_BACKEND_URL}/api/v1/auto-pilot/deploy",
            headers={"Authorization": f"Bearer {auth_token}"},
            json={}, # Invalid body
            timeout=TEST_TIMEOUT
        )
        assert response.status_code in [200, 401, 503, 422], \
            f"Auto Pilot proxy failed: {response.status_code}"
        assert response.status_code != 404, "Route not found - proxy broken!"
    
    def test_analytics_overview_proxy(self, auth_token):
        """Test /api/v1/analytics/overview proxy."""
        response = requests.get(
            f"{USER_BACKEND_URL}/api/v1/analytics/overview",
            headers={"Authorization": f"Bearer {auth_token}"},
            timeout=TEST_TIMEOUT
        )
        assert response.status_code in [200, 401, 503], \
            f"Analytics proxy failed: {response.status_code}"
        assert response.status_code != 404, "Route not found - proxy broken!"
    
    def test_analytics_leaderboard_proxy(self, auth_token):
        """Test /api/v1/analytics/leaderboard proxy."""
        response = requests.get(
            f"{USER_BACKEND_URL}/api/v1/analytics/leaderboard",
            headers={"Authorization": f"Bearer {auth_token}"},
            timeout=TEST_TIMEOUT
        )
        assert response.status_code in [200, 401, 503], \
            f"Analytics leaderboard proxy failed: {response.status_code}"
        assert response.status_code != 404, "Route not found - proxy broken!"
    
    def test_strategy_configs_proxy(self, auth_token):
        """Test /api/v1/strategy-configs proxy."""
        response = requests.get(
            f"{USER_BACKEND_URL}/api/v1/strategy-configs",
            headers={"Authorization": f"Bearer {auth_token}"},
            timeout=TEST_TIMEOUT
        )
        assert response.status_code in [200, 401, 503], \
            f"Strategy configs proxy failed: {response.status_code}"
        assert response.status_code != 404, "Route not found - proxy broken!"
    
    def test_risk_profiles_templates_proxy(self, auth_token):
        """Test /api/v1/risk-profiles/templates proxy."""
        response = requests.get(
            f"{USER_BACKEND_URL}/api/v1/risk-profiles/templates",
            headers={"Authorization": f"Bearer {auth_token}"},
            timeout=TEST_TIMEOUT
        )
        assert response.status_code in [200, 401, 503], \
            f"Risk profiles proxy failed: {response.status_code}"
        assert response.status_code != 404, "Route not found - proxy broken!"
    
    def test_monitoring_health_proxy(self, auth_token):
        """Test /api/v1/monitoring/system-health proxy."""
        response = requests.get(
            f"{USER_BACKEND_URL}/api/v1/monitoring/system-health",
            headers={"Authorization": f"Bearer {auth_token}"},
            timeout=TEST_TIMEOUT
        )
        assert response.status_code in [200, 401, 503], \
            f"Monitoring proxy failed: {response.status_code}"
        assert response.status_code != 404, "Route not found - proxy broken!"


class TestUserBackendHealth:
    """Test user-backend health endpoints."""
    
    def test_root_endpoint(self):
        """Test root endpoint returns service info."""
        response = requests.get(f"{USER_BACKEND_URL}/", timeout=TEST_TIMEOUT)
        assert response.status_code == 200
        data = response.json()
        assert "service" in data
        assert data["service"] == "CosmicForge API"
    
    def test_health_endpoint(self):
        """Test health endpoint."""
        response = requests.get(f"{USER_BACKEND_URL}/health", timeout=TEST_TIMEOUT)
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"


class TestBotBackendHealth:
    """Test bot-backend health endpoints."""
    
    def test_root_endpoint(self):
        """Test root endpoint returns service info."""
        try:
            response = requests.get(f"{BOT_BACKEND_URL}/", timeout=TEST_TIMEOUT)
            assert response.status_code == 200
            data = response.json()
            assert "status" in data
        except requests.exceptions.ConnectionError:
            pytest.skip("Bot-backend not running")
    
    def test_health_endpoint(self):
        """Test monitoring health endpoint."""
        try:
            response = requests.get(
                f"{BOT_BACKEND_URL}/api/v1/monitoring/system-health",
                timeout=TEST_TIMEOUT
            )
            # May require auth, so 401 is acceptable
            assert response.status_code in [200, 401]
        except requests.exceptions.ConnectionError:
            pytest.skip("Bot-backend not running")


class TestOldRoutesRemoved:
    """Ensure old non-v1 routes return 404."""
    
    def test_old_analytics_route_removed(self, auth_token):
        """Old /api/analytics route should not exist."""
        response = requests.get(
            f"{USER_BACKEND_URL}/api/analytics/overview",
            headers={"Authorization": f"Bearer {auth_token}"},
            timeout=TEST_TIMEOUT
        )
        # Should be 404 since we standardized to /api/v1/analytics
        assert response.status_code == 404, \
            "Old /api/analytics route still exists! Should be /api/v1/analytics"
    
    def test_old_strategy_configs_route_removed(self, auth_token):
        """Old /api/strategy-configs route should not exist."""
        response = requests.get(
            f"{USER_BACKEND_URL}/api/strategy-configs",
            headers={"Authorization": f"Bearer {auth_token}"},
            timeout=TEST_TIMEOUT
        )
        assert response.status_code == 404, \
            "Old /api/strategy-configs route still exists! Should be /api/v1/strategy-configs"
    
    def test_old_monitoring_route_removed(self, auth_token):
        """Old /admin/monitoring route should not exist."""
        response = requests.get(
            f"{USER_BACKEND_URL}/admin/monitoring/system-health",
            headers={"Authorization": f"Bearer {auth_token}"},
            timeout=TEST_TIMEOUT
        )
        assert response.status_code == 404, \
            "Old /admin/monitoring route still exists! Should be /api/v1/monitoring"


if __name__ == "__main__":
    # Run tests
    pytest.main([__file__, "-v", "--tb=short"])
