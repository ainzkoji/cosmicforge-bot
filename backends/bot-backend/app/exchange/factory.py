"""
Exchange client factory with runtime credential support for multiple brokers.
Supports both legacy and generic ExchangeClient interfaces.

PREFERRED path for new code:
    from app.exchange.factory import build_exchange_client_from_auth
    auth = resolve_broker_auth(account_id, user_id, db)
    client = build_exchange_client_from_auth(auth)

This guarantees the correct base_url from broker_accounts.environment
(the authoritative source) instead of the credential blob's environment field.

The legacy build_exchange_client(context) path is retained for gradual migration
but will be removed once all callers have been updated.
"""
from __future__ import annotations
from typing import Any

from app.runner.bot_context import BotRunContext
from app.exchange.binance.client import BinanceFuturesClient
from app.exchange.bybit.client import BybitClient
from app.exchange.bingx.client import BingXClient
from app.exchange.oanda.client import OandaClient
from app.exchange.binance.adapter import BinanceAdapter
from app.exchange.oanda.adapter import OandaAdapter
from app.exchange.mt_bridge.client import MTBridgeClient
from app.exchange.mt_bridge.adapter import MetaTraderBridgeAdapter


def build_exchange_client_from_auth(auth: "BrokerAuth") -> Any:  # type: ignore[name-defined]
    """
    Build an exchange client from a resolved BrokerAuth.

    This is the CORRECT factory method. It uses auth.base_url which was
    resolved by the canonical resolver from broker_accounts.environment —
    never from the credential blob.

    No environment inference, no hardcoded URLs, no credential dicts.
    """
    from shared_lib.broker.client_factory import build_client_from_auth
    return build_client_from_auth(auth)


def build_exchange_client(context: BotRunContext) -> Any:
    """
    Factory to build the correct exchange client based on context.
    
    Supports runtime credential passing (no .env dependency).
    Credentials are passed through BotRunContext from user-backend.
    
    Returns: Broker-specific client (legacy)
    """
    broker_type = (context.broker_type or "binance").lower()
    
    # 1. Binance
    if broker_type == "binance":
        # URL priority: explicit override > credential environment > execution mode
        if context.broker_base_url:
            base_url = context.broker_base_url
        elif context.broker_environment == "demo":
            # User connected a Binance demo/paper account via the frontend
            base_url = "https://demo-fapi.binance.com"
        elif context.execution_mode == "paper":
            # Bot set to paper mode but no demo credential — use testnet
            base_url = "https://testnet.binancefuture.com"
        else:
            # Live mainnet
            base_url = "https://fapi.binance.com"
            
        return BinanceFuturesClient(
            api_key=context.broker_api_key,
            api_secret=context.broker_api_secret,
            base_url=base_url
        )
        
    # 2. Bybit
    elif broker_type == "bybit":
        testnet = context.execution_mode == "paper"
        
        return BybitClient(
            api_key=context.broker_api_key,
            api_secret=context.broker_api_secret,
            testnet=testnet,
            base_url=context.broker_base_url 
        )

    # 3. BingX
    elif broker_type == "bingx":
        testnet = context.execution_mode == "paper"
        return BingXClient(
            api_key=context.broker_api_key,
            api_secret=context.broker_api_secret,
            testnet=testnet,
            base_url=context.broker_base_url
        )
    
    # 4. OANDA (NEW)
    elif broker_type == "oanda":
        # OANDA uses different credential structure:
        # - api_token: Personal access token (instead of api_key/secret)
        # - account_id: OANDA account ID
        # - environment: "practice" or "live" (mapped from execution_mode)
        
        # Extract OANDA-specific credentials
        # Assume broker_api_key contains the token, broker_base_url contains account_id
        api_token = context.broker_api_key or ""
        account_id = context.broker_base_url or ""  # Reuse base_url field for account_id
        
        # Map execution mode to OANDA practice flag
        practice = (context.execution_mode in ("paper", "demo", "testnet"))
        
        return OandaClient(
            api_token=api_token,
            account_id=account_id,
            practice=practice,
            timeout=30
        )
    
    # 5. IBKR (Interactive Brokers) - TWS/Gateway Integration
    elif broker_type == "ibkr":
        # IBKR uses TWS API (TCP socket connection via ib_insync)
        # Credentials structure from context:
        # - broker_api_key: account_id (e.g., "DU123456")
        # - broker_base_url: TWS host:port (e.g., "127.0.0.1:7497")
        # - broker_api_secret: client_id (optional, defaults to 1)
        
        from app.exchange.ibkr_tws.adapter import IBKRTwsAdapter
        
        # Parse host:port from broker_base_url
        base_url = context.broker_base_url or "127.0.0.1:7497"
        if ":" in base_url:
            host, port_str = base_url.split(":")
            port = int(port_str)
        else:
            host = base_url
            port = 7497 if context.execution_mode == "paper" else 7496
        
        # Get account ID and client ID
        account_id = context.broker_api_key or None
        client_id = int(context.broker_api_secret) if context.broker_api_secret else 1
        
        # Create TWS adapter
        adapter = IBKRTwsAdapter(
            host=host,
            port=port,
            client_id=client_id,
            account_id=account_id,
            readonly=False
        )
        
        # Note: Connection happens lazily on first use
        # Or you can explicitly connect here:
        # asyncio.run(adapter.connect())
        
        return adapter
    
    # 6. MT4/MT5 Bridge Integration
    elif broker_type in ("mt4", "mt5"):
        # MT uses bridge URL and API token
        # - broker_base_url: Bridge URL (e.g., "https://vps.example.com:8443")
        # - broker_api_key: API token for bridge authentication
        # - broker_api_secret: unused (could be verify_ssl flag in future)
        
        bridge_url = context.broker_base_url
        api_token = context.broker_api_key
        
        if not bridge_url or not api_token:
            raise ValueError(f"MT bridge requires broker_base_url (bridge URL) and broker_api_key (API token)")
        
        # Create bridge client
        bridge_client = MTBridgeClient(
            base_url=bridge_url,
            api_token=api_token,
            timeout=10,
            verify_ssl=(getattr(context, "broker_tls_mode", "strict") != "insecure")
        )
        
        # Wrap in adapter
        return MetaTraderBridgeAdapter(client=bridge_client, platform=broker_type)
        
    else:
        raise ValueError(f"Unsupported broker type: {broker_type}")


def build_generic_exchange_client(context: BotRunContext) -> "ExchangeClient":
    """
    Builds a broker-agnostic ExchangeClient (Adapter pattern).
    
    This is the PREFERRED method for new code as it provides a unified interface.
    Wraps the specific client in an appropriate adapter.
    
    Returns: ExchangeClient protocol implementation
    """
    # 1. Build broker-specific client
    raw = build_exchange_client(context)
    
    # 2. Wrap in adapter
    from app.exchange.interface import ExchangeClient
    
    if isinstance(raw, BinanceFuturesClient):
        from app.exchange.binance.adapter import BinanceAdapter
        return BinanceAdapter(raw)
    
    elif isinstance(raw, OandaClient):
        from app.exchange.oanda.adapter import OandaAdapter
        return OandaAdapter(raw)
    
    # IBKR TWS returns adapter directly
    elif hasattr(raw, '__class__') and raw.__class__.__name__ == 'IBKRTwsAdapter':
        # Already an adapter, return as-is
        return raw
    
    # MT4/MT5 returns adapter directly
    elif isinstance(raw, MetaTraderBridgeAdapter):
        return raw
        
    # TODO: Implement adapters for Bybit/BingX when ready
    
    raise NotImplementedError(f"Adapter not yet implemented for {type(raw).__name__}")
