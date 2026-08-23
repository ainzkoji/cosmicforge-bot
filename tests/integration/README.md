# Integration Tests - README

## Overview

This directory contains integration and contract tests for the CosmicForge bot platform.

## Test Types

### 1. Proxy Route Tests (`test_proxy_routes.py`)

Tests that verify user-backend correctly proxies requests to bot-backend:
- Bot instances proxy
- Strategy marketplace proxy
- Analytics proxy
- Strategy configs proxy
- Risk profiles proxy
- Monitoring proxy

Also verifies old non-v1 routes are properly removed.

### 2. Frontend Contract Tests (`test_frontend_contract.py`)

Tests that verify all routes expected by the frontend actually exist:
- Validates 40+ endpoints the frontend depends on
- Prevents breaking changes
- Uses parametrized tests for easy failure identification

## Running Tests

### Prerequisites

1. **Install test dependencies**:
   ```bash
   cd tests
   pip install -r requirements.txt
   ```

2. **Start both backends**:
   
   Terminal 1 (User-Backend):
   ```bash
   cd backends/user-backend
   python -m uvicorn app.main:app --reload --reload-dir app --port 8000
   ```
   
   Terminal 2 (Bot-Backend):
   ```bash
   cd backends/bot-backend
   python -m uvicorn app.main:app --reload --reload-dir app --port 9000
   ```

### Run All Tests

```bash
# From project root
pytest tests/integration/ -v
```

### Run Specific Test Files

```bash
# Proxy tests only
pytest tests/integration/test_proxy_routes.py -v

# Contract tests only
pytest tests/integration/test_frontend_contract.py -v
```

### Run Specific Test Classes

```bash
# Test proxy routes only
pytest tests/integration/test_proxy_routes.py::TestProxyRoutes -v

# Test frontend contract only
pytest tests/integration/test_frontend_contract.py::TestFrontendContract -v
```

### Run with Coverage

```bash
pytest tests/integration/ --cov=backends --cov-report=html
```

## CI/CD Integration

### GitHub Actions

Add to `.github/workflows/test.yml`:

```yaml
name: Integration Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    
    steps:
    - uses: actions/checkout@v3
    
    - name: Set up Python
      uses: actions/setup-python@v4
      with:
        python-version: '3.11'
    
    - name: Install dependencies
      run: |
        pip install -r backends/user-backend/requirements.txt
        pip install -r backends/bot-backend/requirements.txt
        pip install -r tests/requirements.txt
    
    - name: Start user-backend
      run: |
        cd backends/user-backend
        python -m uvicorn app.main:app --port 8000 &
        sleep 5
    
    - name: Start bot-backend
      run: |
        cd backends/bot-backend
        python -m uvicorn app.main:app --port 9000 &
        sleep 5
    
    - name: Run integration tests
      run: pytest tests/integration/ -v
```

### Local CI Command

```bash
# Run the same steps as CI locally
./run_tests.sh
```

## Test Results

Tests will report:
- ✅ **PASS**: Route exists and is accessible
- ❌ **FAIL**: Route returns 404 (doesn't exist - contract broken!)
- ⚠️ **SKIP**: Service not running (bot-backend)

### Expected Results

With both backends running:
- All proxy tests should PASS (may return 401/503, but not 404)
- All contract tests should PASS (may return 401/422, but not 404)
- Old route tests should PASS (should return 404)
- Health tests should PASS (should return 200)

## Troubleshooting

### Connection Errors

```
requests.exceptions.ConnectionError: Cannot connect to http://localhost:8000
```

**Solution**: Start user-backend on port 8000

### 503 Service Unavailable

```
AssertionError: Bot instances proxy failed: 503
```

**Solution**: Start bot-backend on port 9000, or fix BOT_BACKEND_URL in user-backend config

### 404 Not Found on Expected Route

```
AssertionError: Route not found: GET /api/v1/bot-instances (List bot instances)
```

**Solution**: This is a REAL failure - the route is missing! Check:
1. Router is registered in main.py
2. Prefix is correct
3. Route path matches frontend expectation

## Adding New Tests

### Add New Proxy Route Test

```python
def test_new_feature_proxy(self, auth_token):
    """Test /api/v1/new-feature proxy."""
    response = requests.get(
        f"{USER_BACKEND_URL}/api/v1/new-feature",
        headers={"Authorization": f"Bearer {auth_token}"},
        timeout=TEST_TIMEOUT
    )
    assert response.status_code in [200, 401, 503]
    assert response.status_code != 404
```

### Add New Contract Test

Add to `FRONTEND_EXPECTED_ROUTES` list:

```python
{"method": "GET", "path": "/api/v1/new-feature", "description": "New feature endpoint"},
```

The parametrized test will automatically pick it up!

## Test Maintenance

When making backend changes:

1. **Adding new route**: Add to contract test list
2. **Removing route**: Remove from contract test, add to "removed routes" test
3. **Changing route path**: Update contract test, verify frontend updated
4. **Adding proxy**: Add proxy test, add to contract test

---

**Last Updated**: 2026-01-20
