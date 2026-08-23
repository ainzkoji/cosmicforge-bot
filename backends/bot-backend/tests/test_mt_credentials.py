import unittest
import sys
import os

sys.path.append(os.getcwd())

from app.runner.bot_context import BotRunContext
from dataclasses import dataclass

@dataclass
class MockBotInstance:
    id: str = "bot-123"
    user_id: str = "user-1"
    broker_account_id: str = "broker-abc"
    symbols: list = None
    strategy_id: str = "sma_cross"
    mode: str = "paper"
    risk_level: str = "medium"
    capital_allocation: float = 10000.0
    timeframes: list = None
    allocation_type: str = "fixed_usdt"
    allocation_value: float = 10.0

    def __post_init__(self):
        if self.symbols is None:
            self.symbols = ["EURUSD"]
        if self.timeframes is None:
            self.timeframes = ["1h"]

class TestMTCredentials(unittest.TestCase):
    def test_mt_credentials_mapping(self):
        """Test mapping of bridge_url/token to base_url/api_key"""
        instance = MockBotInstance()
        creds = {
            "bridge_url": "https://bridge.example.com",
            "bridge_token": "secret-token",
            "broker_type": "mt5",
            # No base_url or api_key
        }
        
        context = BotRunContext.from_bot_instance(instance, creds)
        
        self.assertEqual(context.broker_base_url, "https://bridge.example.com")
        self.assertEqual(context.broker_api_key, "secret-token")
        self.assertEqual(context.broker_type, "mt5")
        self.assertEqual(context.broker_tls_mode, "strict") # Default

    def test_tls_mode_insecure(self):
        """Test tls_mode=insecure is respected"""
        instance = MockBotInstance()
        creds = {
            "bridge_url": "https://bridge.example.com",
            "bridge_token": "secret-token",
            "broker_type": "mt5",
            "tls_mode": "insecure"
        }
        
        context = BotRunContext.from_bot_instance(instance, creds)
        
        self.assertEqual(context.broker_tls_mode, "insecure")

    def test_tls_mode_strict_fallback(self):
        """Test tls_mode falls back to strict for invalid values"""
        instance = MockBotInstance()
        creds = {
            "bridge_url": "https://bridge.example.com",
            "bridge_token": "secret-token",
            "broker_type": "mt5",
            "tls_mode": "loose" # Invalid value
        }
        
        context = BotRunContext.from_bot_instance(instance, creds)
        
        self.assertEqual(context.broker_tls_mode, "strict")

    def test_standard_creds_precedence(self):
        """Test standard fields take precedence if present (though unlikely for MT)"""
        instance = MockBotInstance()
        creds = {
            "base_url": "https://standard.example.com",
            "api_key": "standard-key",
            "bridge_url": "https://bridge.example.com",
            "bridge_token": "secret-token",
            "broker_type": "mt5"
        }
        
        context = BotRunContext.from_bot_instance(instance, creds)
        
        self.assertEqual(context.broker_base_url, "https://standard.example.com")
        self.assertEqual(context.broker_api_key, "standard-key")

if __name__ == '__main__':
    unittest.main()
