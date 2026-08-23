"""
IBKR Contract definitions and instrument discovery.

Maps canonical symbols (EUR_USD) to IB Forex contracts.
Returns InstrumentSpec with proper metadata for the executor.
"""

import logging
from typing import Dict, List, Optional
from decimal import Decimal
from ib_insync import Forex

from app.models.unified_trading import InstrumentSpec
from .errors import IBKRContractError

logger = logging.getLogger(__name__)


# Forex pair definitions
# contract_size: 100,000 = 1 standard lot
# tick_size: minimum price increment
FOREX_PAIRS = {
    "EUR_USD": {
        "base": "EUR",
        "quote": "USD",
        "contract_size": 100000,
        "tick_size": 0.00001,
        "description": "Euro vs US Dollar"
    },
    "GBP_USD": {
        "base": "GBP",
        "quote": "USD",
        "contract_size": 100000,
        "tick_size": 0.00001,
        "description": "British Pound vs US Dollar"
    },
    "USD_JPY": {
        "base": "USD",
        "quote": "JPY",
        "contract_size": 100000,
        "tick_size": 0.001,
        "description": "US Dollar vs Japanese Yen"
    },
    "AUD_USD": {
        "base": "AUD",
        "quote": "USD",
        "contract_size": 100000,
        "tick_size": 0.00001,
        "description": "Australian Dollar vs US Dollar"
    },
    "USD_CAD": {
        "base": "USD",
        "quote": "CAD",
        "contract_size": 100000,
        "tick_size": 0.00001,
        "description": "US Dollar vs Canadian Dollar"
    },
    "USD_CHF": {
        "base": "USD",
        "quote": "CHF",
        "contract_size": 100000,
        "tick_size": 0.00001,
        "description": "US Dollar vs Swiss Franc"
    },
    "NZD_USD": {
        "base": "NZD",
        "quote": "USD",
        "contract_size": 100000,
        "tick_size": 0.00001,
        "description": "New Zealand Dollar vs US Dollar"
    },
    "EUR_GBP": {
        "base": "EUR",
        "quote": "GBP",
        "contract_size": 100000,
        "tick_size": 0.00001,
        "description": "Euro vs British Pound"
    },
    "EUR_JPY": {
        "base": "EUR",
        "quote": "JPY",
        "contract_size": 100000,
        "tick_size": 0.001,
        "description": "Euro vs Japanese Yen"
    },
    "GBP_JPY": {
        "base": "GBP",
        "quote": "JPY",
        "contract_size": 100000,
        "tick_size": 0.001,
        "description": "British Pound vs Japanese Yen"
    },
}


class IBKRContractProvider:
    """
    Provides IB contracts and instrument specifications.
    
    Responsibilities:
    - Map canonical symbols to IB Forex contracts
    - Qualify contracts to get conId
    - Return InstrumentSpec for executor
    - Cache contracts to avoid repeated API calls
    """
    
    def __init__(self, client):
        """
        Initialize contract provider.
        
        Args:
            client: IBKRTwsClient instance
        """
        self.client = client
        self._contract_cache: Dict[str, Forex] = {}
        self._spec_cache: Dict[str, InstrumentSpec] = {}
    
    def get_forex_contract(self, symbol_canonical: str) -> Forex:
        """
        Get IB Forex contract for canonical symbol.
        
        Args:
            symbol_canonical: Canonical symbol (e.g., "EUR_USD")
            
        Returns:
            ib_insync Forex contract
            
        Raises:
            IBKRContractError: If symbol not supported
        """
        if symbol_canonical in self._contract_cache:
            return self._contract_cache[symbol_canonical]
        
        if symbol_canonical not in FOREX_PAIRS:
            raise IBKRContractError(
                f"Forex pair '{symbol_canonical}' not supported. "
                f"Available pairs: {list(FOREX_PAIRS.keys())}"
            )
        
        pair_info = FOREX_PAIRS[symbol_canonical]
        contract = Forex(pair_info["base"], pair_info["quote"])
        
        self._contract_cache[symbol_canonical] = contract
        return contract
    
    async def get_instrument_spec(self, symbol_canonical: str) -> InstrumentSpec:
        """
        Get InstrumentSpec for a symbol.
        
        This is what the executor uses to understand the instrument.
        
        Args:
            symbol_canonical: Canonical symbol (e.g., "EUR_USD")
            
        Returns:
            InstrumentSpec with all necessary metadata
            
        Raises:
            IBKRContractError: If symbol not supported or contract qualification fails
        """
        if symbol_canonical in self._spec_cache:
            return self._spec_cache[symbol_canonical]
        
        if symbol_canonical not in FOREX_PAIRS:
            raise IBKRContractError(f"Unknown Forex pair: {symbol_canonical}")
        
        pair_info = FOREX_PAIRS[symbol_canonical]
        contract = self.get_forex_contract(symbol_canonical)
        
        # Qualify contract to get conId from TWS
        try:
            qualified = await self.client.ib.qualifyContractsAsync(contract)
            if qualified:
                contract = qualified[0]
        except Exception as e:
            logger.warning(f"Failed to qualify contract for {symbol_canonical}: {e}")
            # Continue with unqualified contract
        
        # Create InstrumentSpec
        # CRITICAL: contract_size = 100,000 (1 standard lot)
        # Our internal qty unit is LOTS
        # So qty=1.0 in our system = 1 lot = 100,000 base currency units
        spec = InstrumentSpec(
            symbol_canonical=symbol_canonical,
            symbol_exchange=str(contract.conId) if contract.conId else symbol_canonical,
            asset_class="FOREX_SPOT",
            base_asset=pair_info["base"],
            quote_asset=pair_info["quote"],
            contract_size=Decimal(str(pair_info["contract_size"])),
            tick_size=Decimal(str(pair_info["tick_size"])),
            step_size=Decimal("0.01"),  # Min 0.01 lots (micro lot)
            min_qty=Decimal("0.01"),     # Min order: 0.01 lots = 1,000 base units
            max_qty=Decimal("100.0"),    # Max order: 100 lots (conservative default)
            trading_enabled=True
        )
        
        self._spec_cache[symbol_canonical] = spec
        logger.debug(f"Created InstrumentSpec for {symbol_canonical}: conId={contract.conId}")
        
        return spec
    
    async def list_instruments(self) -> List[InstrumentSpec]:
        """
        List all available Forex instruments.
        
        Returns:
            List of InstrumentSpec for all supported pairs
        """
        specs = []
        for symbol in FOREX_PAIRS.keys():
            try:
                spec = await self.get_instrument_spec(symbol)
                specs.append(spec)
            except Exception as e:
                logger.error(f"Failed to get spec for {symbol}: {e}")
        
        logger.info(f"Listed {len(specs)} Forex instruments")
        return specs
    
    def get_supported_pairs(self) -> List[str]:
        """Get list of all supported Forex pairs."""
        return list(FOREX_PAIRS.keys())
