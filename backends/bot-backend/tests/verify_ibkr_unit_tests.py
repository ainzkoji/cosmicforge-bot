import asyncio
import unittest
from unittest.mock import MagicMock, patch
from decimal import Decimal
from app.exchange.ibkr.adapter import IBKRAdapter
from app.models.unified_trading import OrderRequest, OrderType, Side, InstrumentSpec

class TestIBKRAcceptance(unittest.TestCase):
    
    def setUp(self):
        # Mock dependencies
        self.mock_client_patcher = patch('app.exchange.ibkr.adapter.IBKRClient')
        self.mock_session_patcher = patch('app.exchange.ibkr.adapter.IBKRSessionManager')
        self.mock_provider_patcher = patch('app.exchange.ibkr.adapter.IBKRInstrumentProvider')
        
        self.MockClient = self.mock_client_patcher.start()
        self.MockSession = self.mock_session_patcher.start()
        self.MockProvider = self.mock_provider_patcher.start()
        
        # Setup mocks
        self.adapter = IBKRAdapter(account_id="DU123456")
        
        # Mock Instrument Provider
        self.adapter.instruments.get_forex_instruments.return_value = [
            InstrumentSpec(
                symbol_canonical="EUR_USD",
                symbol_exchange="123456", # conid
                base_currency="EUR",
                quote_currency="USD",
                min_qty=1000,
                price_precision=5,
                qty_precision=0
            )
        ]

    def tearDown(self):
        self.mock_client_patcher.stop()
        self.mock_session_patcher.stop()
        self.mock_provider_patcher.stop()

    def test_factory_returns_adapter(self):
        """E1: Factory returns IBKR adapter"""
        self.assertIsInstance(self.adapter, IBKRAdapter)
        self.assertEqual(self.adapter._account_id, "DU123456")

    def test_adapter_mapping(self):
        """E1: adapter mapping test: EUR_USD contract maps to InstrumentSpec"""
        spec = self.adapter._resolve_instrument("EUR_USD")
        self.assertEqual(spec.symbol_exchange, "123456")
        self.assertEqual(spec.base_currency, "EUR")

    def test_place_order_produces_unified_order(self):
        """E1: place_order produces UnifiedOrder"""
        # Mock client.place_order return
        mock_unified_order = MagicMock()
        mock_unified_order.id = "order_123"
        self.adapter._client = self.MockClient.return_value
        self.adapter._client.place_order.return_value = mock_unified_order
        
        req = OrderRequest(
            symbol="EUR_USD",
            side=Side.BUY,
            type=OrderType.MARKET,
            qty=Decimal("10000"),
            time_in_force="GTC"
        )
        
        order = self.adapter.place_order(req)
        
        # Verify payload sent to client
        self.adapter._client.place_order.assert_called_once()
        call_args = self.adapter._client.place_order.call_args
        payload = call_args[0][1] # second arg is payload
        
        self.assertEqual(payload["conid"], 123456)
        self.assertEqual(payload["secType"], "CASH")
        self.assertEqual(payload["orderType"], "MKT")
        self.assertEqual(payload["side"], "BUY")
        self.assertEqual(payload["quantity"], 10000.0)
        
        self.assertEqual(order.id, "order_123")

    def test_connects_and_returns_summary(self):
        """E1 Integration-ish: connects and returns account summary"""
        # Mock get_account_summary return
        expected_summary = {
            "wallet": Decimal("100000.00"),
            "equity": Decimal("100000.00"),
            "available": Decimal("90000.00")
        }
        self.adapter._client = self.MockClient.return_value
        self.adapter._client.get_account_summary.return_value = expected_summary
        
        summary = self.adapter.get_balance()
        
        self.assertEqual(summary["wallet"], Decimal("100000.00"))
        self.adapter._client.get_account_summary.assert_called_with("DU123456")

if __name__ == '__main__':
    unittest.main()
