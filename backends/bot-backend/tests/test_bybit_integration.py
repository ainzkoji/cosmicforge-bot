
import pytest
from unittest.mock import MagicMock, patch
import json
import time
from app.exchange.bybit.client import BybitClient
from app.exchange.bybit.signing import sign_v5
from app.execution.executor import BinanceExecutor
from app.execution.executor import ExecResult
from shared_lib.persistence.db import DB

class TestBybitIntegration:
    
    def test_bybit_signing_deterministic(self):
        """
        Verify V5 signing matches known vector or deterministic behavior.
        """
        api_key = "test_key"
        api_secret = "test_secret"
        timestamp = "1658385588000"
        recv_window = "5000"
        payload_str = '{"category":"linear","symbol":"BTCUSDT"}'
        
        # Manually compute expected signature using HMAC SHA256
        import hmac
        import hashlib
        
        # param_str = timestamp + key + recv_window + payload
        param_str = timestamp + api_key + recv_window + payload_str
        expected_sig = hmac.new(
            api_secret.encode("utf-8"), 
            param_str.encode("utf-8"), 
            hashlib.sha256
        ).hexdigest()
        
        sig = sign_v5(api_secret, payload_str, timestamp, api_key, 5000)
        assert sig == expected_sig

    @patch("app.exchange.bybit.client.requests.get")
    def test_exchange_info_normalization(self, mock_get):
        """
        Verify that Bybit instruments are converted to Binance-like 'symbols' list with filters.
        """
        client = BybitClient("k", "s")
        
        # Mock Bybit Response
        mock_response = {
            "retCode": 0,
            "result": {
                "list": [
                    {
                        "symbol": "BTCUSDT",
                        "baseCoin": "BTC",
                        "quoteCoin": "USDT",
                        "priceFilter": {"tickSize": "0.10"},
                        "lotSizeFilter": {"qtyStep": "0.001", "minOrderQty": "0.001", "maxOrderQty": "100.0"}
                    }
                ]
            }
        }
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = mock_response
        
        info = client.exchange_info_cached()
        
        assert "symbols" in info
        assert len(info["symbols"]) == 1
        sym = info["symbols"][0]
        assert sym["symbol"] == "BTCUSDT"
        assert sym["status"] == "TRADING"
        
        # Check filters
        filters = sym["filters"]
        price_filter = next(f for f in filters if f["filterType"] == "PRICE_FILTER")
        assert price_filter["tickSize"] == "0.10"
        
        lot_filter = next(f for f in filters if f["filterType"] == "LOT_SIZE")
        assert lot_filter["stepSize"] == "0.001"

    @patch("app.exchange.bybit.client.requests.post")
    def test_place_market_order_normalization(self, mock_post):
        """
        Test that Bybit market order returns normalized keys (orderId, avgPrice=0.0)
        """
        client = BybitClient("k", "s")
        
        mock_response = {
            "retCode": 0,
            "result": {
                "orderId": "12345678",
                "orderLinkId": "stub-link"
            }
        }
        mock_post.return_value.status_code = 200
        mock_post.return_value.json.return_value = mock_response
        
        res = client.place_market_order("BTCUSDT", "BUY", 0.5)
        
        # Check Normalized Keys
        assert res["orderId"] == "12345678"
        assert res["avgPrice"] == 0.0
        assert res["status"] == "NEW"
        assert res["symbol"] == "BTCUSDT"
        assert res["executedQty"] == "0.0"

    def test_executor_normalization_integration(self):
        """
        Test that Executor produces details['normalized'] when using a BybitClient.
        """
        # Mock Client
        mock_client = MagicMock()
        # Mock Class Name to "BybitClient"
        mock_client.__class__.__name__ = "BybitClient"
        
        # Setup mocks for executor flow
        mock_client.get_position_amt.return_value = 0.0
        mock_client.last_price.return_value = 50000.0
        mock_client.get_prices = MagicMock(return_value={"BTCUSDT": 50000.0})
        mock_client.get_klines.return_value = []
        mock_client.account = MagicMock(return_value={"availableBalance": "10000.0"})
        
        # Exchange Info Mock
        mock_client.exchange_info_cached.return_value = {
            "symbols": [{
                 "symbol": "BTCUSDT",
                 "filters": [
                     {"filterType": "PRICE_FILTER", "tickSize": "0.1"},
                     {"filterType": "LOT_SIZE", "stepSize": "0.001", "minQty": "0.001"},
                     {"filterType": "MIN_NOTIONAL", "notional": "5.0"}
                 ]
            }]
        }
        
        # Order Response Mock: executor expects a UnifiedOrder object with .avg_fill_price and .model_dump()
        from app.models.unified_trading import UnifiedOrder, OrderStatus, Side as TradeSide
        import time
        mock_order = UnifiedOrder(
            client_order_id="bybit-local-123",
            broker_order_id="bybit-123",
            symbol="BTCUSDT",
            side=TradeSide.BUY,
            type="MARKET",
            qty_ordered="0.002",
            qty_filled="0.002",
            avg_fill_price="50000.0",
            status=OrderStatus.FILLED,
            timestamp=int(time.time() * 1000)
        )
        mock_client.place_order = MagicMock(return_value=mock_order)
        
        # Protection Mocks
        from app.models.unified_trading import ProtectionResult
        mock_client.place_protection = MagicMock(return_value=ProtectionResult(status="ok"))
        mock_client.place_stop_market = MagicMock(return_value={"orderId": "sl-1"})
        mock_client.place_take_profit_market = MagicMock(return_value={"orderId": "tp-1"})
        
        # Account Balance Mock
        mock_client.get_account_snapshot = MagicMock(return_value={
            "available_balance": 10000.0,
            "equity": 10000.0,
            "margin_used": 0.0
        })
        
        # Initialize Executor
        executor = BinanceExecutor(client=mock_client)
        # Bypass instrument spec lookup - provide direct qty
        executor._size_qty = MagicMock(return_value=(0.002, {}))
        
        # Execute Signal
        # Using a budget of 100 USDT
        res = executor.execute_signal("BTCUSDT", "BUY", 100.0)
        
        assert res.success
        assert res.status == "ORDER_PLACED"
        assert "normalized" in res.details
        
        norm = res.details["normalized"]
        assert "order_id" in norm or "broker_order_id" in norm  # key may vary
        assert norm.get("side") == "BUY" or norm.get("side") == "buy"

