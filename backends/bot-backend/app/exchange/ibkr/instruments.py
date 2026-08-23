from decimal import Decimal
from typing import Dict, List, Optional
from app.models.unified_trading import InstrumentSpec, AssetClass

class IBKRInstrumentProvider:
    """
    Manages instrument definitions for IBKR.
    Maps between IBKR 'conid' (Contract ID) and system symbols.
    """
    
    # Static map for common Forex pairs to start with
    # In a real scenario, this would be populated via search or config
    KNOWN_FOREX = {
        "EURUSD": {
            "conid": "12087792", # Example conid for EUR.USD
            "symbol": "EUR.USD",
            "base": "EUR",
            "quote": "USD"
        },
        "GBPUSD": {
            "conid": "12087797",
            "symbol": "GBP.USD",
            "base": "GBP",
            "quote": "USD"
        },
        "USDJPY": {
            "conid": "12087799",
            "symbol": "USD.JPY",
            "base": "USD",
            "quote": "JPY"
        }
    }
    
    def get_forex_instruments(self) -> List[InstrumentSpec]:
        specs = []
        for canonical, info in self.KNOWN_FOREX.items():
            spec = InstrumentSpec(
                symbol_canonical=canonical,
                symbol_exchange=info["conid"], # We use conid as the primary exchange ID for ordering
                asset_class=AssetClass.FOREX_SPOT, # or CFD depending on account
                base_currency=info["base"],
                quote_currency=info["quote"],
                margin_currency=info["quote"], # Assuming generic
                settlement_currency=info["quote"],
                
                contract_size=Decimal("1"), # IBKR Forex is typically cash, so 1 unit = 1 currency unit (e.g. 20000 EUR) OR it's strict lots? 
                                            # Update: IBKR Pro is often exact units (25000). 
                                            # If it were CFD it might be different. We assume Cash logic for now: 1 unit = 1 base currency.
                tick_size=Decimal("0.00005"), # Half-pip standard or smaller
                step_size=Decimal("1"), # Can trade 1 unit (approx)
                min_qty=Decimal("20000"), # Typical min for IBKR Pro Forex is roughly 20-25k USD equivalent for commissions to make sense, but tech min is lower.
                
                price_precision=5,
                qty_precision=0,
                
                max_leverage=Decimal("20") # Account dependent, but placeholder
            )
            specs.append(spec)
        return specs

    def get_by_conid(self, conid: str) -> Optional[InstrumentSpec]:
        # Reverse lookup for internal use if needed
        for spec in self.get_forex_instruments():
            if spec.symbol_exchange == conid:
                return spec
        return None
