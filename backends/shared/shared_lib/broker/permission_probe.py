"""Ask the broker what an API key may do, and normalise the answer.

Uses the EXISTING exchange clients (built by the canonical factory, pointed at
the broker's wallet/asset host where that differs from the trading host) --
no second HTTP stack per broker. A broker/environment without a verified
permission endpoint yields UNVERIFIED evidence (``inspected=False``), never a
guessed "all good".
"""
from __future__ import annotations

import logging
from dataclasses import replace
from typing import Any, Callable, Optional

from shared_lib.broker.environment import resolve_wallet_base_url
from shared_lib.broker.permissions import (
    PermissionEvidence,
    normalize_binance_api_restrictions,
    normalize_bybit_query_api,
    unverified,
)
from shared_lib.broker.resolver import BrokerAuth
from shared_lib.core.security.redaction import redact_exception

logger = logging.getLogger(__name__)


def _default_builder(auth: BrokerAuth) -> Any:
    from shared_lib.broker.client_factory import build_client_from_auth

    return build_client_from_auth(auth)


def probe_permissions(auth: BrokerAuth, *, build: Callable[[BrokerAuth], Any] = _default_builder,
                      trading_client: Optional[Any] = None) -> PermissionEvidence:
    broker = auth.broker_type.lower()
    try:
        if broker == "binance":
            wallet = resolve_wallet_base_url("binance", auth.environment)
            if not wallet:
                return unverified("binance", "binance:no-wallet-api-for-environment",
                                  note=f"no verified SAPI host for {auth.environment.value}")
            client = build(replace(auth, base_url=wallet))
            return normalize_binance_api_restrictions(client._signed_get("/sapi/v1/account/apiRestrictions", {}))
        if broker == "bybit":
            client = trading_client or build(auth)
            data = client._request_v5("GET", "/v5/user/query-api")
            if data.get("retCode") != 0:
                return unverified("bybit", "bybit:/v5/user/query-api", note=f"retCode={data.get('retCode')}")
            return normalize_bybit_query_api(data.get("result") or {})
        if broker == "bingx":
            # No verified BingX API-key permission endpoint: evidence stays
            # UNVERIFIED (money-moving features stay off for this key).
            return unverified("bingx", "bingx:not-inspectable", note="BingX key permissions are not API-inspectable here")
    except Exception as exc:  # the probe failing is evidence of nothing
        logger.warning("permission_probe_failed broker=%s account=%s error=%s", broker, auth.account_id,
                       redact_exception(exc))
        return unverified(broker, f"{broker}:probe-error", note=redact_exception(exc)[:200])
    return unverified(broker, f"{broker}:unsupported")


__all__ = ["probe_permissions"]
