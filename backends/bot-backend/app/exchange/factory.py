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
from typing import TYPE_CHECKING, Any

from app.runner.bot_context import BotRunContext
from app.exchange.binance.client import BinanceFuturesClient
from app.exchange.bybit.client import BybitClient
from app.exchange.bingx.client import BingXClient
from app.exchange.oanda.client import OandaClient
from app.exchange.binance.adapter import BinanceAdapter
from app.exchange.oanda.adapter import OandaAdapter
from app.exchange.mt_bridge.client import MTBridgeClient, harden_bridge_client
from app.exchange.mt_bridge.adapter import MetaTraderBridgeAdapter

if TYPE_CHECKING:  # annotation-only names; importing them at runtime would be circular
    from shared_lib.broker.resolver import BrokerAuth
    from app.exchange.interface import ExchangeClient


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


def _guard_stored_bridge_url(bridge_url: Any, broker_type: str) -> Any:
    """Validate a stored MT bridge URL against the outbound (SSRF) policy.

    Production: the URL must pass the same policy as a caller-supplied one --
    https, no query / fragment, no credentials, public addresses only (the
    BROKER_GATEWAY_ALLOWED_HOSTS allow-list is for admins' test connections,
    not for stored user accounts). A refusal is a configuration error and is
    raised as a ``ValueError`` naming the reason, never the URL. Returns a
    callable that re-validates the destination; the client runs it before
    every request.

    Outside production the guard is relaxed: nothing is resolved or refused
    and ``None`` is returned, so local bridges and tests keep working.

    Note: ``build_exchange_client`` currently refuses to run in production at
    all (PRODUCTION_REQUIRES_RESOLVED_BROKER_AUTH_FACTORY), so today this is
    defence in depth for the day that restriction is lifted.
    """
    from app.core.config import settings
    from shared_lib.core.security.url_guard import (
        OutboundPolicy,
        UnsafeDestinationError,
        revalidate_destination,
        validate_gateway_url,
    )

    policy = OutboundPolicy.from_settings(settings).public_only()
    if not policy.production:
        return None
    try:
        destination = validate_gateway_url(bridge_url, policy=policy)
    except UnsafeDestinationError as exc:
        raise ValueError(
            f"MT_BRIDGE_URL_NOT_ALLOWED: the stored {str(broker_type).upper()} bridge URL is refused by the "
            f"outbound policy ({exc.reason}). Fix the broker account's bridge URL (https, public address, "
            "no query string or fragment)."
        ) from None
    return lambda: revalidate_destination(destination, policy=policy)


def build_exchange_client(context: BotRunContext) -> Any:
    """
    Factory to build the correct exchange client based on context.
    
    Supports runtime credential passing (no .env dependency).
    Credentials are passed through BotRunContext from user-backend.
    
    Returns: Broker-specific client (legacy)
    """
    from shared_lib.core.production import production_enabled, require_execution_account
    if production_enabled():
        require_execution_account(context.broker_environment, context.broker_type, context.broker_base_url)
        raise ValueError("PRODUCTION_REQUIRES_RESOLVED_BROKER_AUTH_FACTORY")
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
        
        # The bridge URL is STORED, but a user supplied it: apply the same
        # outbound policy as the test-connection route before the engine
        # sends the bearer token (and orders) there. Relaxed outside
        # production (returns None).
        destination_guard = _guard_stored_bridge_url(bridge_url, broker_type)

        # Create bridge client
        bridge_client = MTBridgeClient(
            base_url=bridge_url,
            api_token=api_token,
            timeout=10,
            verify_ssl=(getattr(context, "broker_tls_mode", "strict") != "insecure")
        )
        # Never follow a redirect off the validated host; in production also
        # re-validate the destination before every request (DNS rebinding).
        harden_bridge_client(bridge_client, destination_guard)

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
