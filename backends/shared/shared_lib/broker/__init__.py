"""
shared_lib.broker
=================
Canonical broker authentication module.

All code that needs to connect to a broker exchange must go through this module.
Nothing outside this module should read broker_credentials directly or construct
exchange base URLs independently.

Public API:
    resolve_broker_auth(account_id, user_id, db) -> BrokerAuth
    build_client_from_auth(auth) -> exchange client
    get_broker_health_cache() -> BrokerHealthCache
    BrokerResolverError  (with .reason_code)
    BrokerEnvironment    (enum: LIVE | DEMO)
"""

from shared_lib.broker.errors import BrokerResolverError
from shared_lib.broker.environment import BrokerEnvironment, normalize_environment, resolve_base_url
from shared_lib.broker.resolver import BrokerAuth, resolve_broker_auth
from shared_lib.broker.health_cache import BrokerHealthCache, BrokerHealthEntry, get_broker_health_cache
from shared_lib.broker.client_factory import build_client_from_auth

__all__ = [
    "BrokerResolverError",
    "BrokerEnvironment",
    "normalize_environment",
    "resolve_base_url",
    "BrokerAuth",
    "resolve_broker_auth",
    "BrokerHealthCache",
    "BrokerHealthEntry",
    "get_broker_health_cache",
    "build_client_from_auth",
]
