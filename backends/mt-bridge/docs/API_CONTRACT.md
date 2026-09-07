# MT Bridge REST API Contract - v1

## Overview

The MT Bridge REST API provides a unified interface for trading MT4/MT5 accounts via HTTP. All communication uses JSON.

## Base URL

```
https://<your-vps-ip>:8443/v1
```

## Authentication

All endpoints require Bearer token authentication.

**Header**:
```
Authorization: Bearer <your-api-token>
```

**Generate Token**:
```bash
python scripts/generate_token.py
```

## Error Responses

All errors return HTTP status codes with JSON body:

```json
{
  "detail": "Error message description"
}
```

**Status Codes**:
- `200` - Success
- `400` - Bad request (invalid parameters)
- `401` - Unauthorized (invalid token)
- `404` - Not found
- `500` - Internal server error
- `504` - Gateway timeout (EA not responding)

## Endpoints

### GET /v1/health

Health check - returns platform and account info.

**Request**:
```http
GET /v1/health HTTP/1.1
Authorization: Bearer <token>
```

**Response** (200 OK):
```json
{
  "status": "ok",
  "platform": "mt5",
  "account": 12345678,
  "server": "Demo-Server",
  "time": "2026-02-08 18:00:00",
  "connected": true
}
```

---

### GET /v1/instruments

Get available trading instruments.

**Request**:
```http
GET /v1/instruments HTTP/1.1
Authorization: Bearer <token>
```

**Response** (200 OK):
```json
{
  "symbols": [
    {
      "symbol": "EURUSD",
      "description": "Euro vs US Dollar",
      "digits": 5,
      "contract_size": 100000.0,
      "min_lot": 0.01,
      "max_lot": 100.0,
      "lot_step": 0.01,
      "tick_size": 0.00001,
      "tick_value": 1.0
    }
  ]
}
```

---

### POST /v1/prices

Get current bid/ask prices for symbols.

**Request**:
```http
POST /v1/prices HTTP/1.1
Authorization: Bearer <token>
Content-Type: application/json

{
  "symbols": ["EURUSD", "GBPUSD"]
}
```

**Response** (200 OK):
```json
{
  "prices": [
    {
      "symbol": "EURUSD",
      "bid": 1.08450,
      "ask": 1.08452,
      "time": "2026-02-08 18:00:00"
    },
    {
      "symbol": "GBPUSD",
      "bid": 1.26320,
      "ask": 1.26324,
      "time": "2026-02-08 18:00:00"
    }
  ]
}
```

---

### POST /v1/klines

Get historical candles.

**Request**:
```http
POST /v1/klines HTTP/1.1
Authorization: Bearer <token>
Content-Type: application/json

{
  "symbol": "EURUSD",
  "interval": "H1",
  "limit": 100
}
```

**Intervals**: `M1`, `M5`, `M15`, `M30`, `H1`, `H4`, `D1`, `W1`, `MN1`

**Response** (200 OK):
```json
{
  "klines": [
    {
      "time": "2026-02-08 17:00:00",
      "open": 1.08420,
      "high": 1.08480,
      "low": 1.08410,
      "close": 1.08450,
      "volume": 1234
    }
  ]
}
```

---

### POST /v1/order

Place a new order.

**Request**:
```http
POST /v1/order HTTP/1.1
Authorization: Bearer <token>
Content-Type: application/json

{
  "symbol": "EURUSD",
  "side": "buy",
  "type": "market",
  "quantity": 0.1,
  "price": null,
  "sl": 1.08000,
  "tp": 1.09000,
  "client_order_id": "bot-order-123"
}
```

**Parameters**:
- `symbol` (string, required): Trading symbol
- `side` (string, required): `"buy"` or `"sell"`
- `type` (string, required): `"market"`, `"limit"`, or `"stop"`
- `quantity` (float, required): Order quantity in LOTS
- `price` (float, optional): Limit/stop price (ignored for market orders)
- `sl` (float, optional): Stop loss price
- `tp` (float, optional): Take profit price
- `client_order_id` (string, optional): Custom order identifier

**Response** (200 OK):
```json
{
  "order_id": "123456789",
  "ticket": 123456789,
  "price": 1.08452,
  "volume": 0.1,
  "status": "filled"
}
```

---

### POST /v1/order/cancel

Cancel a pending order.

**Request**:
```http
POST /v1/order/cancel HTTP/1.1
Authorization: Bearer <token>
Content-Type: application/json

{
  "order_id": "123456789"
}
```

**Response** (200 OK):
```json
{
  "success": true,
  "order_id": "123456789"
}
```

---

### GET /v1/order/{order_id}

Get order status.

**Request**:
```http
GET /v1/order/123456789 HTTP/1.1
Authorization: Bearer <token>
```

**Response** (200 OK - Filled):
```json
{
  "order_id": "123456789",
  "status": "filled",
  "symbol": "EURUSD",
  "side": "buy",
  "quantity": 0.1,
  "price": 1.08452
}
```

**Response** (200 OK - Pending):
```json
{
  "order_id": "123456789",
  "status": "pending",
  "symbol": "EURUSD",
  "quantity": 0.1
}
```

---

### GET /v1/positions

Get all open positions.

**Request**:
```http
GET /v1/positions HTTP/1.1
Authorization: Bearer <token>
```

**Response** (200 OK):
```json
{
  "positions": [
    {
      "ticket": 123456789,
      "symbol": "EURUSD",
      "side": "buy",
      "quantity": 0.1,
      "price_open": 1.08450,
      "price_current": 1.08520,
      "profit": 7.0,
      "sl": 1.08000,
      "tp": 1.09000
    }
  ]
}
```

---

### GET /v1/balance

Get account balance and equity.

**Request**:
```http
GET /v1/balance HTTP/1.1
Authorization: Bearer <token>
```

**Response** (200 OK):
```json
{
  "balance": 10000.0,
  "equity": 10007.0,
  "margin": 108.45,
  "free_margin": 9898.55,
  "margin_level": 9127.5,
  "currency": "USD"
}
```

---

## Usage Examples

### Python

```python
import requests

BASE_URL = "https://your-vps:8443/v1"
TOKEN = "your-api-token"

headers = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/json"
}

# Health check
response = requests.get(f"{BASE_URL}/health", headers=headers)
print(response.json())

# Place market order
order = {
    "symbol": "EURUSD",
    "side": "buy",
    "type": "market",
    "quantity": 0.1,
    "sl": 1.08000,
    "tp": 1.09000
}
response = requests.post(f"{BASE_URL}/order", json=order, headers=headers)
print(response.json())
```

### cURL

```bash
# Health check
curl -X GET "https://your-vps:8443/v1/health" \
  -H "Authorization: Bearer your-token"

# Get balance
curl -X GET "https://your-vps:8443/v1/balance" \
  -H "Authorization: Bearer your-token"

# Place order
curl -X POST "https://your-vps:8443/v1/order" \
  -H "Authorization: Bearer your-token" \
  -H "Content-Type: application/json" \
  -d '{
    "symbol": "EURUSD",
    "side": "buy",
    "type": "market",
    "quantity": 0.1
  }'
```

## Rate Limits

No rate limits currently enforced. However, the EA processes requests sequentially, so high-frequency requests may timeout.

**Recommendations**:
- Max 10 requests/second
- Use websockets for real-time data (future enhancement)

## Timeouts

- **Request Timeout**: 5 seconds (configurable in `config.json`)
- **ZMQ Timeout**: 5 seconds

If EA doesn't respond within timeout, HTTP 504 is returned.

## Version History

### v1.0.0 (2026-02-08)
- Initial release
- Basic trading operations (market, limit, stop orders)
- Position and balance queries
- MT4 and MT5 support
