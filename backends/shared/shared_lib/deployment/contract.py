"""The customer deployment contract (Step 1.2), shared by the user-backend proxy
and the bot-backend handlers so both validate the same versioned schema.

    POST /api/v1/auto-pilot/preview
    POST /api/v1/auto-pilot/deploy

Monetary values travel as Decimal-compatible strings. Nothing the client sends
about balances, broker environment, entitlements, margin or exchange metadata
is trusted: the request names a broker account, a budget, a risk level, optional
advanced settings, the acknowledgement flag and an idempotency key -- the
server derives everything else from its own records.
"""
from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator

DEPLOYMENT_SCHEMA_VERSION = "2026-10-08.v1"
#: Identity of the risk acknowledgement text the customer accepts at deployment.
CONSENT_VERSION = "deploy-risk-ack-2026-10-08.v1"
CONSENT_TEXT = ("I understand that automated trading can lose money, that the estimated stop-loss risk is an "
                "estimate and not a guarantee, and that actual losses can exceed it because of slippage, outages "
                "or price gaps. I accept these risks for this deployment.")

# ── blocker codes (the master plan's codes are stable; others are additions) ──
BUDGET_TOO_SMALL_FOR_LEVEL = "BUDGET_TOO_SMALL_FOR_LEVEL"
BUDGET_EXCEEDS_BALANCE = "BUDGET_EXCEEDS_BALANCE"
ACCOUNT_ALREADY_HAS_BOT = "ACCOUNT_ALREADY_HAS_BOT"
ACCOUNT_NOT_CONNECTED = "ACCOUNT_NOT_CONNECTED"
RISK_NOT_ACKNOWLEDGED = "RISK_NOT_ACKNOWLEDGED"
LIVE_NOT_AVAILABLE = "LIVE_NOT_AVAILABLE"
RISK_SIZE_BELOW_EXCHANGE_MINIMUM = "RISK_SIZE_BELOW_EXCHANGE_MINIMUM"
# additions, each a genuinely distinct requirement
BROKER_NOT_SUPPORTED = "BROKER_NOT_SUPPORTED"
ACCOUNT_BALANCE_UNAVAILABLE = "ACCOUNT_BALANCE_UNAVAILABLE"
BROKER_CAPABILITY_INCOMPLETE = "BROKER_CAPABILITY_INCOMPLETE"
REQUEST_ID_REUSED = "REQUEST_ID_REUSED"
ACCOUNT_STATE_CHANGED = "ACCOUNT_STATE_CHANGED"
INVALID_ADVANCED_SETTINGS = "INVALID_ADVANCED_SETTINGS"

BLOCKER_MESSAGES: Dict[str, Dict[str, str]] = {
    BUDGET_TOO_SMALL_FOR_LEVEL: {
        "message": "The budget is too small for this risk level: a correctly sized position would fall below the exchange minimum order size.",
        "action": "Increase the budget to at least the minimum shown, or choose a higher risk level."},
    BUDGET_EXCEEDS_BALANCE: {
        "message": "The budget is larger than the balance of the connected account.",
        "action": "Lower the budget to the available account balance or fund the account."},
    ACCOUNT_ALREADY_HAS_BOT: {
        "message": "This exchange account already has a running or paused bot.",
        "action": "Stop the existing bot before deploying another one on the same account."},
    ACCOUNT_NOT_CONNECTED: {
        "message": "The selected exchange account is not connected to your profile.",
        "action": "Connect the exchange account and complete its validation first."},
    RISK_NOT_ACKNOWLEDGED: {
        "message": "The trading risks have not been acknowledged for this deployment.",
        "action": "Read the risk statement and tick the acknowledgement before deploying."},
    LIVE_NOT_AVAILABLE: {
        "message": "Live trading is not available yet: only demo exchange accounts can be deployed.",
        "action": "Connect a Binance demo account to deploy a bot."},
    RISK_SIZE_BELOW_EXCHANGE_MINIMUM: {
        "message": "At this budget and risk level the engine would have to place orders below the exchange minimum; orders are never inflated to meet it.",
        "action": "Increase the budget or choose a higher risk level."},
    BROKER_NOT_SUPPORTED: {
        "message": "This exchange is not supported for automated execution yet.",
        "action": "Use a Binance demo account."},
    ACCOUNT_BALANCE_UNAVAILABLE: {
        "message": "The account balance could not be read from the exchange right now.",
        "action": "Retry in a moment; if it persists, re-validate the exchange connection."},
    BROKER_CAPABILITY_INCOMPLETE: {
        "message": "The exchange API key lacks a permission the engine needs (futures trading, no withdrawals).",
        "action": "Re-connect the account with an API key that has futures trading enabled and withdrawals disabled."},
    REQUEST_ID_REUSED: {
        "message": "This request identity was already used for a different deployment request.",
        "action": "Start the deployment again from the preview."},
    ACCOUNT_STATE_CHANGED: {
        "message": "The account changed between the preview and the deployment.",
        "action": "Review the new preview and deploy again."},
    INVALID_ADVANCED_SETTINGS: {
        "message": "An advanced setting is outside the permitted range.",
        "action": "Correct the highlighted advanced setting."},
}


def _decimal_text(value: Any, name: str, *, allow_none: bool = False) -> Optional[str]:
    if value is None or value == "":
        if allow_none:
            return None
        raise ValueError(f"{name} is required")
    if isinstance(value, float):
        value = repr(value)
    try:
        parsed = Decimal(str(value).strip())
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"{name} must be a decimal number") from exc
    if not parsed.is_finite():
        raise ValueError(f"{name} must be finite")
    return str(parsed)


class BudgetSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")
    type: Literal["fixed_amount", "percent_balance"]
    value: str = Field(description="Decimal string; USDT for fixed_amount, percent for percent_balance")

    @field_validator("value", mode="before")
    @classmethod
    def _value(cls, v):
        text = _decimal_text(v, "budget.value")
        if Decimal(text) <= 0:
            raise ValueError("budget.value must be positive")
        if Decimal(text) > Decimal("1000000000000"):
            raise ValueError("budget.value is beyond any supported budget")
        return text

    def validated(self) -> "BudgetSpec":
        if self.type == "percent_balance" and Decimal(self.value) > 100:
            raise ValueError("a percentage budget cannot exceed 100")
        return self


class AdvancedSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")
    max_position_usdt: Optional[str] = Field(default=None, description="Decimal string; maximum notional of one position")
    daily_loss_limit_pct: Optional[str] = Field(default=None, description="Decimal string in PERCENT (2 = 2 %)")
    symbols: List[str] = Field(default_factory=list, max_length=50)

    @field_validator("max_position_usdt", mode="before")
    @classmethod
    def _max_position(cls, v):
        text = _decimal_text(v, "advanced.max_position_usdt", allow_none=True)
        if text is not None and Decimal(text) <= 0:
            raise ValueError("advanced.max_position_usdt must be positive")
        return text

    @field_validator("daily_loss_limit_pct", mode="before")
    @classmethod
    def _daily_loss(cls, v):
        text = _decimal_text(v, "advanced.daily_loss_limit_pct", allow_none=True)
        if text is not None and not (Decimal("0.1") <= Decimal(text) <= Decimal("25")):
            raise ValueError("advanced.daily_loss_limit_pct must be between 0.1 and 25 percent")
        return text

    @field_validator("symbols")
    @classmethod
    def _symbols(cls, v):
        out = []
        for s in v or []:
            s = str(s).strip().upper()
            if not s or not s.replace("/", "").replace(":", "").isalnum() or len(s) > 24:
                raise ValueError(f"invalid symbol {s!r}")
            if s not in out:
                out.append(s)
        return out


class DeploymentRequest(BaseModel):
    """The body of both ``/preview`` and ``/deploy``."""
    model_config = ConfigDict(extra="forbid")
    schema_version: str = Field(default=DEPLOYMENT_SCHEMA_VERSION)
    broker_account_id: str = Field(min_length=1, max_length=64)
    budget: BudgetSpec
    risk_level: Literal["conservative", "balanced", "aggressive"]
    advanced: AdvancedSpec = Field(default_factory=AdvancedSpec)
    risk_acknowledged: bool = False
    request_id: str = Field(min_length=8, max_length=128, description="Idempotency key chosen by the client")

    @field_validator("schema_version")
    @classmethod
    def _version(cls, v):
        if v != DEPLOYMENT_SCHEMA_VERSION:
            raise ValueError(f"unsupported deployment schema version {v!r}; expected {DEPLOYMENT_SCHEMA_VERSION}")
        return v

    @field_validator("request_id")
    @classmethod
    def _request_id(cls, v):
        if not all(ch.isalnum() or ch in "-_.:" for ch in v):
            raise ValueError("request_id may contain letters, digits, '-', '_', '.' and ':' only")
        return v

    def fingerprint(self) -> str:
        """The request without its acknowledgement flag: what an idempotent replay must repeat."""
        import hashlib
        import json
        data = self.model_dump(mode="json")
        data.pop("risk_acknowledged", None)
        return hashlib.sha256(json.dumps(data, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def is_new_contract(body: Any) -> bool:
    """Whether a raw JSON body is the Step 1 contract (vs the legacy auto-pilot deploy body)."""
    return isinstance(body, dict) and "broker_account_id" in body and "budget" in body and "risk_level" in body \
        and "broker_account_ids" not in body


def blocker(code: str, **detail: Any) -> Dict[str, Any]:
    text = BLOCKER_MESSAGES.get(code, {"message": code, "action": ""})
    return {"code": code, "message": text["message"], "action": text["action"], **detail}


__all__ = ["DEPLOYMENT_SCHEMA_VERSION", "CONSENT_VERSION", "CONSENT_TEXT", "BLOCKER_MESSAGES", "BudgetSpec",
           "AdvancedSpec", "DeploymentRequest", "is_new_contract", "blocker",
           "BUDGET_TOO_SMALL_FOR_LEVEL", "BUDGET_EXCEEDS_BALANCE", "ACCOUNT_ALREADY_HAS_BOT", "ACCOUNT_NOT_CONNECTED",
           "RISK_NOT_ACKNOWLEDGED", "LIVE_NOT_AVAILABLE", "RISK_SIZE_BELOW_EXCHANGE_MINIMUM", "BROKER_NOT_SUPPORTED",
           "ACCOUNT_BALANCE_UNAVAILABLE", "BROKER_CAPABILITY_INCOMPLETE", "REQUEST_ID_REUSED", "ACCOUNT_STATE_CHANGED",
           "INVALID_ADVANCED_SETTINGS"]
