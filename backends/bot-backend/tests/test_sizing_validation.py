
import unittest
from decimal import Decimal
from app.symbols.sizing import validate_and_adjust_order, SizeResult
from app.models.unified_trading import SymbolFilters

class TestSizingValidation(unittest.TestCase):
    def setUp(self):
        self.filters = SymbolFilters(
            symbol="BTC-USDT",
            step_size=Decimal("0.001"),
            min_qty=Decimal("0.001"),
            min_notional=Decimal("5.0"),
            tick_size=Decimal("0.1"),
            max_qty=Decimal("1000.0")
        )

    def test_valid_order(self):
        # Quantity 0.1 at price 100.0 = $10.0 (Above min $5.0)
        res = validate_and_adjust_order(
            "BTC-USDT", 0.1, 100.0, self.filters, 1000.0, 1.0
        )
        self.assertEqual(res.qty, 0.1)
        self.assertEqual(res.reason, "ok")

    def test_rounding(self):
        # Quantity 0.1005 -> Should round down to 0.100
        res = validate_and_adjust_order(
            "BTC-USDT", 0.1005, 100.0, self.filters, 1000.0, 1.0
        )
        self.assertEqual(res.qty, 0.100)

    def test_min_notional_bump(self):
        # Quantity 0.01 at price 100.0 = $1.0 (Below min $5.0)
        # Min qty for $5 is 0.05
        # Budget $100 allows bump
        res = validate_and_adjust_order(
            "BTC-USDT", 0.01, 100.0, self.filters, 100.0, 1.0
        )
        self.assertEqual(res.qty, 0.05)
        self.assertIn("qty_bumped_min_notional", res.reason)

    def test_min_notional_block_budget(self):
        # Quantity 0.01 at price 100.0 = $1.0 (Below min $5.0)
        # Budget $4.0 (Insufficient for bump to $5.0)
        res = validate_and_adjust_order(
            "BTC-USDT", 0.01, 100.0, self.filters, 4.0, 1.0
        )
        self.assertEqual(res.qty, 0.0)
        self.assertIn("below_min_notional", res.reason)

    def test_zero_filters(self):
        # No filters passed
        res = validate_and_adjust_order(
            "BTC-USDT", 0.12345, 100.0, None, 1000.0, 1.0
        )
        # Should default to no rounding/checking? Or basic rounding?
        # current impl defaults to 6 decimals, step 0
        self.assertGreater(res.qty, 0.0)
        self.assertEqual(res.qty, 0.12345)

    def test_contract_size_scaling(self):
         # Forex: contract_size=100000. Price=1.1. Lot=0.01 => Notional = 0.01*100000*1.1 = 1100
         fx_filters = SymbolFilters(
            symbol="EURUSD",
            step_size=Decimal("0.01"),
            min_qty=Decimal("0.01"),
            min_notional=Decimal("0"), # usually 0 for MT
            contract_size=Decimal("100000")
         )
         
         # Request 0.01 lots
         res = validate_and_adjust_order(
            "EURUSD", 0.01, 1.1, fx_filters, 2000.0, 100.0
         )
         self.assertEqual(res.qty, 0.01)
         self.assertEqual(res.notional, 1100.0)

if __name__ == '__main__':
    unittest.main()
