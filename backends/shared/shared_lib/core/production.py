"""One production profile; order permission is separate from market/runtime mode.

Historical adapters and isolated tests retain their original provenance. They are
never selected by the production runtime or substituted for a LIVE account.
"""
from __future__ import annotations

from typing import Literal

from pydantic import model_validator
from pydantic_settings import BaseSettings

FROZEN_STRATEGY = "RESIDUAL_MOMENTUM_PORTFOLIO_TOP1"
ACTIVE_FLAGS = (
    "BROKER_DISCOVERY_ENABLED", "BALANCE_SYNC_ENABLED", "POSITION_SYNC_ENABLED",
    "ORDER_SYNC_ENABLED", "RECONCILIATION_ENABLED", "RISK_ENGINE_ENABLED",
    "FX_PIPELINE_ENABLED", "FORWARD_OBSERVATION_ENABLED",
)


class ProductionSettings(BaseSettings):
    APP_ENV: Literal["PRODUCTION", "TEST", "DEVELOPMENT"] = "PRODUCTION"
    RUNTIME_ENV: str = "PRODUCTION"
    CATI_MODE: str = "LIVE"
    CATI_SOLE_ENGINE: bool = True
    TRADING_ENABLED: bool = True
    MARKET_DATA_MODE: str = "LIVE"
    BROKER_ENVIRONMENT: str = "ACCOUNT_SCOPED"
    BROKER_DISCOVERY_ENABLED: bool = True
    BALANCE_SYNC_ENABLED: bool = True
    POSITION_SYNC_ENABLED: bool = True
    ORDER_SYNC_ENABLED: bool = True
    RECONCILIATION_ENABLED: bool = True
    RISK_ENGINE_ENABLED: bool = True
    FX_PIPELINE_ENABLED: bool = True
    FORWARD_OBSERVATION_ENABLED: bool = True
    API_ENV: str = "PRODUCTION"
    UI_ENV: str = "PRODUCTION"
    DATABASE_ROLE: str = "production"
    ENVIRONMENT_NAME: str = "production"
    LEGACY_V2_ENABLED: bool = False
    FALLBACK_ENGINE_ENABLED: bool = False
    LIVE_ORDER_SUBMISSION_ENABLED: bool = False
    DEMO_ORDER_SUBMISSION_ENABLED: bool = False

    @model_validator(mode="after")
    def validate_production_profile(self):
        if self.APP_ENV != "PRODUCTION":
            return self
        expected = {"RUNTIME_ENV": "PRODUCTION", "CATI_MODE": "LIVE",
                    "MARKET_DATA_MODE": "LIVE", "BROKER_ENVIRONMENT": "ACCOUNT_SCOPED",
                    "API_ENV": "PRODUCTION", "UI_ENV": "PRODUCTION",
                    "DATABASE_ROLE": "production", "ENVIRONMENT_NAME": "production",
                    "EXECUTION_MODE": "live", "STRATEGY_NAME": FROZEN_STRATEGY,
                    "BINANCE_ENV": "mainnet", "BYBIT_ENV": "mainnet", "BINGX_ENV": "mainnet",
                    "BINANCE_FAPI_BASE_URL": "https://fapi.binance.com",
                    "BYBIT_BASE_URL": "https://api.bybit.com",
                    "BINGX_BASE_URL": "https://open-api.bingx.com"}
        errors = [f"{key} must be {value}" for key, value in expected.items()
                  if hasattr(self, key) and str(getattr(self, key)).casefold() != value.casefold()]
        for key in (*ACTIVE_FLAGS, "CATI_SOLE_ENGINE", "TRADING_ENABLED"):
            if not getattr(self, key):
                errors.append(f"{key} must be true")
        for key in ("LEGACY_V2_ENABLED", "FALLBACK_ENGINE_ENABLED", "PAPER_TRADING_MODE",
                    "ML_ENABLED", "TRADINGVIEW_EXTERNAL_SIGNALS_ENABLED",
                    "TRADINGVIEW_ALLOW_PAPER_LIVE_MODE", "TRADINGVIEW_TESTNET_ONLY",
                    "EVENT_FILTER_STALE_FAILSAFE_TESTNET_WARN_ONLY"):
            if getattr(self, key, False):
                errors.append(f"{key} must be false in production")
        if getattr(self, "ADAPTIVE_DAILY_RISK_ENABLED", True) is not True:
            errors.append("ADAPTIVE_DAILY_RISK_ENABLED must remain true")
        cap = getattr(self, "ADAPTIVE_DAILY_RISK_MAX_DAILY_LOSS_PCT", .025)
        if not 0 < cap <= .025:
            errors.append("daily hard loss cap cannot exceed 2.5%")
        if errors:
            raise ValueError("PRODUCTION_PROFILE_CONFLICT: " + "; ".join(errors))
        return self

    @property
    def production(self) -> bool:
        return self.APP_ENV == "PRODUCTION"

    def configuration_matrix(self) -> dict:
        return {"APP/RUNTIME": self.RUNTIME_ENV, "CATI_MODE": self.CATI_MODE,
                "CATI": "SOLE ENGINE" if self.CATI_SOLE_ENGINE else "MULTIPLE",
                "TRADING": "ACTIVE" if self.TRADING_ENABLED else "DISABLED",
                "MARKET DATA": self.MARKET_DATA_MODE, "BROKER_EXECUTION_SCOPE": "ACCOUNT_SCOPED",
                **{k.removesuffix("_ENABLED").replace("_", " "): "ACTIVE" if getattr(self, k) else "DISABLED"
                   for k in ACTIVE_FLAGS}, "API/UI": f"{self.API_ENV}/{self.UI_ENV}",
                "DATABASE ROLE": self.DATABASE_ROLE.upper(),
                "LEGACY V2/FALLBACK": "DISABLED" if not self.LEGACY_V2_ENABLED and not self.FALLBACK_ENGINE_ENABLED else "ENABLED",
                "LIVE_ORDER_SUBMISSION_ENABLED": self.LIVE_ORDER_SUBMISSION_ENABLED,
                "DEMO_ORDER_SUBMISSION_ENABLED": self.DEMO_ORDER_SUBMISSION_ENABLED,
                "STRATEGY": FROZEN_STRATEGY, "DAILY_HARD_LOSS_CAP": "2.5% maximum"}


def production_enabled() -> bool:
    # Use the backend's loaded settings, not a mutable .env reread. This keeps
    # an already-running process honest when configuration is staged on disk.
    from app.core.config import settings
    return bool(getattr(settings, "production", False))


class LiveOrderSubmissionDisabled(PermissionError):
    pass


class DemoOrderSubmissionDisabled(PermissionError):
    pass


def order_submission_gate(environment):
    from app.core.config import settings
    from shared_lib.broker.environment import BrokerEnvironment, normalize_environment
    env = normalize_environment(environment)
    name = "DEMO_ORDER_SUBMISSION_ENABLED" if env == BrokerEnvironment.DEMO else "LIVE_ORDER_SUBMISSION_ENABLED"
    return {"name": name, "enabled": bool(getattr(settings, name, False)),
            "reason": name.replace("_ENABLED", "_DISABLED"), "environment": env.value.upper()}


def require_execution_account(environment, broker: str = "", base_url: str = "") -> None:
    from shared_lib.broker.environment import normalize_environment, resolve_base_url
    env = normalize_environment(environment)
    if broker and base_url:
        if base_url.rstrip("/") != resolve_base_url(broker.lower(), env).rstrip("/"):
            raise ValueError("BROKER_ENVIRONMENT_MISMATCH")


def endpoint_environment(broker: str, base_url: str):
    from shared_lib.broker.environment import BrokerEnvironment, resolve_base_url
    for env in BrokerEnvironment:
        if base_url.rstrip("/") == resolve_base_url(broker.lower(), env).rstrip("/"):
            return env
    raise ValueError("BROKER_ENVIRONMENT_MISMATCH")


def require_broker_mutation_permission(method: str, path: str = "", *, environment=None,
                                       broker: str = "", base_url: str = "", client=None, payload=None) -> None:
    if method.upper() in {"GET", "HEAD", "OPTIONS"}:
        return
    from app.core.config import settings
    if settings.production:
        # Unbound legacy transports retain the conservative real-money gate.
        env = environment or (endpoint_environment(broker, base_url) if broker and base_url else "LIVE")
        require_execution_account(env, broker, base_url)
        gate = order_submission_gate(env)
        if not gate["enabled"]:
            error = DemoOrderSubmissionDisabled if gate["environment"] == "DEMO" else LiveOrderSubmissionDisabled
            raise error(gate["reason"])
        entry_paths = {"binance": "/fapi/v1/order", "bybit": "/v5/order/create",
                       "bingx": "/openApi/swap/v2/trade/order"}
        if method.upper() == "POST" and path == entry_paths.get(broker):
            params = payload or {}
            closing = any(str(params.get(key, "")).lower() == "true" for key in ("reduceOnly", "closePosition"))
            if not closing:
                try:
                    from app.trading_intelligence.execution.entry_permit import transport_entry_permitted
                    permitted = transport_entry_permitted(client, params.get("symbol"), params.get("side"))
                except ImportError:
                    permitted = False
                if not permitted and gate["environment"] == "DEMO" and broker == "binance":
                    from app.execution.demo_transport_smoke import permitted as smoke_permitted
                    permitted = smoke_permitted(client, params)
                if not permitted:
                    raise ValueError("CATI_ENTRY_AUTHORITY_REQUIRED")


def require_live_account(environment: str, broker: str = "", base_url: str = "") -> None:
    if not production_enabled():
        return
    from shared_lib.broker.environment import BrokerEnvironment, normalize_environment, resolve_base_url
    if normalize_environment(environment) != BrokerEnvironment.LIVE:
        raise ValueError("PRODUCTION_REQUIRES_LIVE_BROKER_ACCOUNT")
    if broker and base_url and broker in {"binance", "bybit", "bingx", "oanda"}:
        if base_url.rstrip("/") != resolve_base_url(broker, BrokerEnvironment.LIVE).rstrip("/"):
            raise ValueError("PRODUCTION_REQUIRES_CANONICAL_LIVE_ENDPOINT")


def require_production_endpoint(broker: str, base_url: str) -> None:
    if production_enabled():
        endpoint_environment(broker, base_url)
