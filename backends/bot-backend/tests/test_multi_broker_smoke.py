import pytest
from unittest.mock import MagicMock
from app.exchange.factory import build_exchange_client
from app.runner.bot_context import BotRunContext
from app.exchange.bingx.client import BingXClient
from app.exchange.binance.client import BinanceFuturesClient

def test_factory_builds_bingx_client():
    """
    Test that factory builds BingXClient when broker_type is bingx.
    """
    context = BotRunContext(
        bot_instance_id="test_bingx",
        user_id="user1",
        broker_account_id="brk_1",
        strategy_id="strat1",
        symbols=["BTC-USDT"],
        interval="15m",
        
        # Broker Config
        broker_type="bingx",
        broker_api_key="key",
        broker_api_secret="secret",
        execution_mode="paper"
    )
    
    client = build_exchange_client(context)
    
    assert isinstance(client, BingXClient)
    assert client.api_key == "key"
    assert client.base_url == "https://open-api.bingx.com" 

def test_factory_builds_binance_client():
    """
    Test that factory builds BinanceFuturesClient when broker_type is binance.
    """
    context = BotRunContext(
        bot_instance_id="test_binance",
        user_id="user1",
        broker_account_id="brk_2",
        strategy_id="strat1",
        symbols=["BTC-USDT"],
        interval="15m",
        
        # Broker Config
        broker_type="binance",
        broker_api_key="key",
        broker_api_secret="secret",
        execution_mode="paper"
    )
    
    client = build_exchange_client(context)
    
    assert isinstance(client, BinanceFuturesClient)

def test_multi_broker_simulation():
    """
    Simulate a run cycle with different broker clients.
    """
    # BingX Context
    ctx_bingx = BotRunContext(
        bot_instance_id="b1", user_id="u1", broker_account_id="ba1", strategy_id="s1", 
        symbols=["BTC-USDT"], interval="15m",
        broker_type="bingx", broker_api_key="k1", broker_api_secret="s1", execution_mode="paper"
    )
    
    # Binance Context
    ctx_binance = BotRunContext(
        bot_instance_id="b2", user_id="u1", broker_account_id="ba2", strategy_id="s1", 
        symbols=["ETH-USDT"], interval="15m",
        broker_type="binance", broker_api_key="k2", broker_api_secret="s2", execution_mode="live"
    )
    
    client_bingx = build_exchange_client(ctx_bingx)
    client_binance = build_exchange_client(ctx_binance)
    
    assert isinstance(client_bingx, BingXClient)
    assert isinstance(client_binance, BinanceFuturesClient)
    
    # Verify their implementations differ but conform
    assert hasattr(client_bingx, "place_stop_market")
    assert hasattr(client_binance, "place_stop_market")
