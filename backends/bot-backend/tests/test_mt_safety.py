import unittest
from unittest.mock import MagicMock, patch
import os
import sys
from decimal import Decimal

# Add bot-backend to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

try:
    from app.exchange.mt_bridge.adapter import MetaTraderBridgeAdapter
    from app.exchange.mt_bridge.client import MTBridgeClient
    from app.models.unified_trading import OrderRequest, OrderType, Side, OrderStatus
except ImportError as e:
    print(f"Import failed: {e}")
    print(f"Sys path: {sys.path}")
    sys.exit(1)

class TestMTSafety(unittest.TestCase):
    
    @patch("app.exchange.mt_bridge.client.MTBridgeClient")
    def test_qty_guardrails(self, MockMTClient):
        # Setup mock client
        client_mock = MockMTClient.return_value
        
        # Create adapter
        adapter = MetaTraderBridgeAdapter(client_mock, "mt4")
        
        # Verify initial instrument cache state
        self.assertEqual(len(adapter._instruments_cache), 0)
        
        # Mock instruments
        client_mock.get_instruments.return_value = [
            {"symbol": "EURUSD", "min_lot": 0.1, "max_lot": 5.0, "lot_step": 0.1},
            {"symbol": "GBPUSD", "min_lot": 0.0, "max_lot": 0.0, "lot_step": 0.0} # No limits
        ]

        # ---------------------------------------------------------
        # Case 1: Zero quantity (Global sanity)
        # ---------------------------------------------------------
        req = OrderRequest(
            symbol="EURUSD",
            side=Side.BUY,
            type=OrderType.MARKET,
            qty=Decimal("0")
        )
        order = adapter.place_order(req)
        print(f"Case 1 (Zero): Status={order.status}, Error={order.error_message}")
        self.assertEqual(order.status, OrderStatus.REJECTED)
        self.assertIn("positive", order.error_message.lower())

        # ---------------------------------------------------------
        # Case 2: Max lots limit (default 10)
        # ---------------------------------------------------------
        req.qty = Decimal("11.0")
        order = adapter.place_order(req)
        print(f"Case 2 (Max 10): Status={order.status}, Error={order.error_message}")
        self.assertEqual(order.status, OrderStatus.REJECTED)
        self.assertIn("exceeds safety limit of 10.0", order.error_message)

        # ---------------------------------------------------------
        # Case 3: Valid quantity (0.2 lots)
        # ---------------------------------------------------------
        req.qty = Decimal("0.2")
        # Mock success response from bridge
        client_mock.place_order.return_value = {"id": "12345", "price": 1.1234}
        
        order = adapter.place_order(req)
        print(f"Case 3 (Valid): Status={order.status}")
        self.assertNotEqual(order.status, OrderStatus.REJECTED)
        
        # Verify instrument cache was populated
        self.assertGreater(len(adapter._instruments_cache), 0)
        
        # ---------------------------------------------------------
        # Case 4: Below min_lot (0.05 < 0.1)
        # ---------------------------------------------------------
        req.qty = Decimal("0.05")
        order = adapter.place_order(req)
        print(f"Case 4 (Below min): Status={order.status}, Error={order.error_message}")
        self.assertEqual(order.status, OrderStatus.REJECTED)
        self.assertIn("below min_lot", order.error_message)

        # ---------------------------------------------------------
        # Case 5: Above max_lot (6.0 > 5.0)
        # ---------------------------------------------------------
        req.symbol = "EURUSD"
        req.qty = Decimal("6.0")
        order = adapter.place_order(req)
        print(f"Case 5 (Above max): Status={order.status}, Error={order.error_message}")
        self.assertEqual(order.status, OrderStatus.REJECTED)
        self.assertIn("exceeds instrument max_lot", order.error_message)

    def test_tls_defaults(self):
        # Verify Client default
        client = MTBridgeClient("http://base", "token")
        self.assertTrue(client.verify_ssl, "MTBridgeClient should default to verify_ssl=True")
        
        # Verify Explicit False
        client_insecure = MTBridgeClient("http://base", "token", verify_ssl=False)
        self.assertFalse(client_insecure.verify_ssl)

if __name__ == "__main__":
    unittest.main()
