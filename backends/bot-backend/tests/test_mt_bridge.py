"""
Tests for MetaTrader Bridge integration.
"""
import pytest
from unittest.mock import Mock, patch, MagicMock
from decimal import Decimal

from app.exchange.mt_bridge.client import MTBridgeClient
from app.exchange.mt_bridge.errors import MTBridgeError
from app.exchange.mt_bridge.adapter import MetaTraderBridgeAdapter
from app.models.unified_trading import (
    OrderRequest,
    Side,
    OrderType,
    OrderStatus
)


class TestMTBridgeClient:
    """Test suite for MTBridgeClient"""
    
    def test_health_endpoint(self):
        """Test health endpoint call"""
        client = MTBridgeClient(
            base_url="https://test.example.com",
            api_token="test_token",
            timeout=10
        )
        
        # Mock the session.request method
        with patch.object(client._session, 'request') as mock_request:
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.json.return_value = {
                "ok": True,
                "platform": "mt5",
                "account": {"login": "12345", "server": "Demo", "currency": "USD"},
                "time": "2026-02-08T17:00:00Z"
            }
            mock_request.return_value = mock_response
            
            health = client.get_health()
            
            assert health["ok"] is True
            assert health["platform"] == "mt5"
            assert "account" in health
    
    def test_get_instruments(self):
        """Test instruments endpoint"""
        client = MTBridgeClient("https://test.example.com", "test_token")
        
        with patch.object(client._session, 'request') as mock_request:
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.json.return_value = {
                "instruments": [
                    {
                        "symbol": "EURUSD",
                        "base": "EUR",
                        "quote": "USD",
                        "digits": 5,
                        "tick_size": 0.00001,
                        "contract_size": 100000,
                        "min_lot": 0.01,
                        "lot_step": 0.01
                    }
                ]
            }
            mock_request.return_value = mock_response
            
            instruments = client.get_instruments()
            
            assert len(instruments) == 1
            assert instruments[0]["symbol"] == "EURUSD"
    
    def test_error_handling(self):
        """Test error handling for non-200 responses"""
        client = MTBridgeClient("https://test.example.com", "test_token")
        
        with patch.object(client._session, 'request') as mock_request:
            mock_response = Mock()
            mock_response.status_code = 401
            mock_response.json.return_value = {
                "ok": False,
                "error_code": "UNAUTHORIZED",
                "error": "Invalid token"
            }
            mock_request.return_value = mock_response
            
            with pytest.raises(MTBridgeError) as exc_info:
                client.get_health()
            
            assert "Invalid token" in str(exc_info.value)


class TestMTAdapter:
    """Test suite for MetaTraderBridgeAdapter"""
    
    def test_adapter_capabilities(self):
        """Test that capabilities are correctly set"""
        mock_client = Mock(spec=MTBridgeClient)
        adapter = MetaTraderBridgeAdapter(client=mock_client, platform="mt5")
        
        caps = adapter.capabilities
        
        assert caps.position_mode.value == "ticket"
        assert caps.supports_hedging is True
        assert caps.supports_attached_sl_tp is True
    
    def test_list_instruments(self):
        """Test instrument listing through adapter"""
        mock_client = Mock(spec=MTBridgeClient)
        mock_client.get_instruments.return_value = [
            {
                "symbol": "EURUSD",
                "base": "EUR",
                "quote": "USD",
                "digits": 5,
                "tick_size": 0.00001,
                "contract_size": 100000,
                "min_lot": 0.01,
                "lot_step": 0.01
            }
        ]
        
        adapter = MetaTraderBridgeAdapter(client=mock_client, platform="mt5")
        specs = adapter.list_instruments()
        
        assert len(specs) == 1
        assert specs[0].symbol_canonical == "EURUSD"
        assert specs[0].contract_size == Decimal("100000")
    
    def test_place_order(self):
        """Test order placement"""
        mock_client = Mock(spec=MTBridgeClient)
        mock_client.place_order.return_value = {
            "order_id": "12345",
            "status": "filled",
            "filled_qty": 0.1,
            "avg_price": 1.08500
        }
        
        adapter = MetaTraderBridgeAdapter(client=mock_client, platform="mt5")
        
        req = OrderRequest(
            symbol="EURUSD",
            side=Side.BUY,
            type=OrderType.MARKET,
            qty=Decimal("0.1")
        )
        
        order = adapter.place_order(req)
        
        assert order.broker_order_id == "12345"
        assert order.status == OrderStatus.FILLED
        assert mock_client.place_order.called
    
    def test_get_positions(self):
        """Test position fetching"""
        mock_client = Mock(spec=MTBridgeClient)
        mock_client.get_positions.return_value = [
            {
                "symbol": "EURUSD",
                "ticket": "987654",
                "side": "buy",
                "lots": 0.1,
                "open_price": 1.08500,
                "sl": 1.08000,
                "tp": 1.09000,
                "profit": 50.0,
                "open_time": "2026-02-08T16:00:00Z"
            }
        ]
        
        adapter = MetaTraderBridgeAdapter(client=mock_client, platform="mt5")
        positions = adapter.get_positions()
        
        assert len(positions) == 1
        assert positions[0].symbol == "EURUSD"
        assert positions[0].position_id == "987654"
        assert positions[0].side == Side.BUY
    
    def test_get_balance(self):
        """Test balance fetching"""
        mock_client = Mock(spec=MTBridgeClient)
        mock_client.get_balance.return_value = {
            "balance": 10000.0,
            "equity": 10050.0,
            "margin": 100.0,
            "free_margin": 9950.0,
            "currency": "USD"
        }
        
        adapter = MetaTraderBridgeAdapter(client=mock_client, platform="mt5")
        balance = adapter.get_balance()
        
        assert balance["wallet"] == Decimal("10000.0")
        assert balance["equity"] == Decimal("10050.0")
        assert balance["available"] == Decimal("9950.0")
