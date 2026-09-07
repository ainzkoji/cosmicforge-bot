"""
Exchange client factory — the ONLY place where exchange clients are constructed.

Rules:
  1. All code that needs an exchange client must call build_client_from_auth().
  2. No other module may construct BinanceFuturesClient / BybitClient / etc. directly
     from raw credentials or environment strings.
  3. The BrokerAuth passed here must come from resolve_broker_auth() — never from
     a raw dict or an ad-hoc credential fetch.

Supported brokers:  binance, bybit, bingx
Extending:          add a branch in _build_client() and update __all__.
"""
from __future__ import annotations

import logging
from typing import Any

from shared_lib.broker.errors import BrokerResolverError
from shared_lib.broker.resolver import BrokerAuth

logger = logging.getLogger(__name__)


def build_client_from_auth(auth: BrokerAuth) -> Any:
    """
    Build and return an exchange client using only the resolved BrokerAuth.

    The returned client is already configured with the correct base_url,
    api_key, and api_secret derived from the canonical resolver — no
    environment inference, no hardcoded URLs, no raw credential dicts.

    Args:
        auth: Fully resolved BrokerAuth from resolve_broker_auth().

    Returns:
        Exchange client appropriate for auth.broker_type.

    Raises:
        BrokerResolverError(REASON_AUTH_FAILED) if the broker type has no
        registered client builder.
    """
    broker = auth.broker_type.lower()
    logger.debug(
        "build_client_from_auth broker=%s account=%s env=%s fingerprint=%s",
        broker, auth.account_id, auth.environment.value, auth.key_fingerprint,
    )

    try:
        return _build_client(broker, auth)
    except BrokerResolverError:
        raise
    except Exception as exc:
        raise BrokerResolverError(
            BrokerResolverError.REASON_AUTH_FAILED,
            f"Failed to instantiate exchange client for broker={broker!r} "
            f"account={auth.account_id!r}: {exc}",
            account_id=auth.account_id,
        ) from exc


def _build_client(broker: str, auth: BrokerAuth) -> Any:
    if broker == "binance":
        return _build_binance(auth)
    if broker == "bybit":
        return _build_bybit(auth)
    if broker == "bingx":
        return _build_bingx(auth)

    raise BrokerResolverError(
        BrokerResolverError.REASON_AUTH_FAILED,
        f"No client builder registered for broker_type={broker!r}. "
        f"Supported: binance, bybit, bingx. "
        f"To add a new broker, register a builder in "
        f"shared_lib.broker.client_factory._build_client().",
        account_id=auth.account_id,
    )


def _build_binance(auth: BrokerAuth) -> Any:
    # Import lazily — avoids circular imports and keeps shared_lib broker
    # module free of exchange-specific top-level dependencies.
    try:
        from app.exchange.binance.client import BinanceFuturesClient  # type: ignore[import]
    except ImportError:
        # Fallback for bot-backend path variations
        from app.exchange.binance_client import BinanceFuturesClient  # type: ignore[import]

    return BinanceFuturesClient(
        api_key=auth.api_key,
        api_secret=auth.api_secret,
        base_url=auth.base_url,
    )


def _build_bybit(auth: BrokerAuth) -> Any:
    try:
        from app.exchange.bybit_client import BybitClient  # type: ignore[import]
    except ImportError:
        from app.exchange.bybit.client import BybitClient  # type: ignore[import]

    return BybitClient(
        api_key=auth.api_key,
        api_secret=auth.api_secret,
        base_url=auth.base_url,
    )


def _build_bingx(auth: BrokerAuth) -> Any:
    try:
        from app.exchange.bingx_client import BingXClient  # type: ignore[import]
    except ImportError:
        raise BrokerResolverError(
            BrokerResolverError.REASON_AUTH_FAILED,
            "BingX client module not found. "
            "Ensure app.exchange.bingx_client is present.",
        )

    return BingXClient(
        api_key=auth.api_key,
        api_secret=auth.api_secret,
        base_url=auth.base_url,
    )
