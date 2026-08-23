from unittest.mock import Mock, patch
from app.runner.bot_context import BotRunContext
from app.exchange.factory import build_exchange_client
from app.exchange.mt_bridge.adapter import MetaTraderBridgeAdapter
from app.exchange.mt_bridge.client import MTBridgeClient

def test_mt4_factory_creation():
    """Test that factory correctly builds MT4 adapter from context"""
    context = BotRunContext(
        bot_instance_id="bot_123",
        user_id="user_456",
        broker_account_id="brk_mt4",
        strategy_id="strat_789",
        symbols=["EURUSD"],      # Changed from symbol to symbols
        # interval="1h",         # Removed to avoid TypeError
        broker_type="mt4",
        broker_base_url="https://mt4-bridge.com", # Bridge URL
        broker_api_key="secret_token",           # Token
        execution_mode="live"
    )
    
    # We mock MTBridgeClient to avoid network calls and verify init args
    with patch("app.exchange.factory.MTBridgeClient") as MockClient:
        # Mock the client instance created
        MockClient.return_value = Mock(spec=MTBridgeClient) 
        
        client = build_exchange_client(context)
        
        # Should return a MetaTraderBridgeAdapter wrapping the client
        assert isinstance(client, MetaTraderBridgeAdapter)
        assert client._platform == "mt4"
        
        # Verify bridge client was initialized with correct args
        MockClient.assert_called_once_with(
            base_url="https://mt4-bridge.com",
            api_token="secret_token",
            timeout=10,
            verify_ssl=True
        )

def test_mt5_factory_creation():
    """Test that factory correctly builds MT5 adapter from context"""
    context = BotRunContext(
        bot_instance_id="bot_123",
        user_id="user_456",
        broker_account_id="brk_mt5",
        strategy_id="strat_789",
        symbols=["EURUSD"],      # Changed from symbol to symbols
        # interval="1h",         # Removed to avoid TypeError
        broker_type="mt5",
        broker_base_url="https://mt5-bridge.com:8443",
        broker_api_key="token_123",
        execution_mode="live"
    )
    
    with patch("app.exchange.factory.MTBridgeClient") as MockClient:
        MockClient.return_value = Mock(spec=MTBridgeClient)
        
        client = build_exchange_client(context)
        
        assert isinstance(client, MetaTraderBridgeAdapter)
        assert client._platform == "mt5"
        
        MockClient.assert_called_once_with(
            base_url="https://mt5-bridge.com:8443",
            api_token="token_123",
            timeout=10,
            verify_ssl=True
        )
