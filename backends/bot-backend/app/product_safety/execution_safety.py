"""Execution safety decided from the CONNECTED BROKER ACCOUNT, not from the bot's mode.

``execution_mode = live`` means "execute through the connected broker account".
It does not, by itself, mean "real money". The account connection owns that
fact: ``bot -> broker_account_id -> broker_accounts.environment``, resolved by
``shared_lib.broker.resolver`` and carried into the runner as
``BotRunContext.broker_environment``. Nothing here reads a global
``BINANCE_ENV`` or any bot-level demo/mainnet switch, because there is none.

Two gates used to apply to every broker execution regardless of that fact:

* **KYC** -- the canonical requirement (``start_live_trading``) is defined by
  the product as "Required for live trading with real funds";
* **real-capital readiness** -- ``assert_user_capital_activation_allowed``,
  the controlled-beta gate that prevents premature *user-capital* deployment.

Both are real-capital protections. They now apply exactly where real capital
is at stake and are unchanged there:

    connected account LIVE              -> KYC evaluated, readiness approval
                                           mandatory, both fail closed
    connected account DEMO/testnet/...  -> KYC NOT_REQUIRED,
                                           readiness NOT_REQUIRED_FOR_DEMO_EXECUTION
    environment missing or unrecognised -> treated as LIVE (fail closed)

Every downstream protection -- entry quality, risk, the capital ledger,
execution feasibility, entry protection, precision, broker protection orders --
is untouched and applies to demo execution exactly as before.
"""
from __future__ import annotations

import logging
from dataclasses import asdict, dataclass
from typing import Any, Callable

logger = logging.getLogger(__name__)

#: Gate names, as they appear in evidence.
GATE_KYC = "KYC"
GATE_READINESS = "REAL_CAPITAL_READINESS"


class ReadinessGateState:
    APPROVED_FOR_CONTROLLED_BETA = "APPROVED_FOR_CONTROLLED_BETA"
    NOT_REQUIRED_FOR_DEMO_EXECUTION = "NOT_REQUIRED_FOR_DEMO_EXECUTION"
    NOT_MET = "NOT_MET"
    UNAVAILABLE = "UNAVAILABLE"


@dataclass(frozen=True)
class ConnectedAccountCapital:
    """What the connected account's environment says about capital at risk."""

    raw_environment: str | None
    environment: str  # "live" | "demo" | "unknown"
    real_capital: bool
    reason: str


def classify_connected_account(broker_environment: Any) -> ConnectedAccountCapital:
    """Classify the connected account. Unknown is real capital: fail closed."""
    from shared_lib.broker.environment import BrokerEnvironment, normalize_environment

    raw = None if broker_environment is None else str(getattr(broker_environment, "value", broker_environment))
    if not raw:
        return ConnectedAccountCapital(
            None, "unknown", True,
            "no connected-account environment was resolved; treated as real capital",
        )
    try:
        env = normalize_environment(raw)
    except ValueError:
        return ConnectedAccountCapital(
            raw, "unknown", True,
            f"connected-account environment {raw!r} is not recognised; treated as real capital",
        )
    if env == BrokerEnvironment.LIVE:
        return ConnectedAccountCapital(raw, env.value, True, "connected account is a live (real-funds) account")
    return ConnectedAccountCapital(
        raw, env.value, False,
        f"connected account environment {raw!r} is a demo/test environment; no real funds",
    )


@dataclass(frozen=True)
class GateDecision:
    """One execution-safety verdict, with the account context it was made in."""

    gate: str
    state: str
    allowed: bool
    reason: str
    account_environment: str
    real_capital: bool

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def evaluate_execution_kyc(
    *,
    user_id: str | None,
    broker_environment: Any,
    kyc_evaluator: Callable[[str, str], Any] | None = None,
) -> GateDecision:
    """The KYC verdict for broker execution through the connected account."""
    from shared_lib.core.policy.kyc_policy import KYCAction, KYCGateStatus, evaluate_kyc_gate

    account = classify_connected_account(broker_environment)
    if not account.real_capital:
        return GateDecision(
            GATE_KYC, KYCGateStatus.NOT_REQUIRED.value, True,
            f"{account.reason}; the canonical KYC requirement "
            f"({KYCAction.START_LIVE_TRADING.value}) covers live trading with real funds, "
            f"and no demo-account KYC requirement is configured",
            account.environment, False,
        )
    if not user_id:
        return GateDecision(
            GATE_KYC, KYCGateStatus.UNAVAILABLE.value, False,
            "no user identity for the connected account; KYC cannot be evaluated",
            account.environment, True,
        )
    try:
        result = (kyc_evaluator or evaluate_kyc_gate)(user_id, KYCAction.START_LIVE_TRADING.value)
    except Exception as exc:
        return GateDecision(
            GATE_KYC, KYCGateStatus.UNAVAILABLE.value, False,
            f"KYC evaluation failed: {type(exc).__name__}: {exc}",
            account.environment, True,
        )
    return GateDecision(
        GATE_KYC, str(getattr(result.status, "value", result.status)), bool(result.allowed),
        str(result.reason), account.environment, True,
    )


def evaluate_execution_readiness(
    *,
    db: Any,
    bot_instance_id: str | None,
    broker_environment: Any,
    asserter: Callable[..., None] | None = None,
) -> GateDecision:
    """The real-capital readiness verdict for broker execution through the connected account."""
    from app.product_safety.readiness_gate import (
        UserCapitalReadinessError,
        assert_user_capital_activation_allowed,
    )

    account = classify_connected_account(broker_environment)
    if not account.real_capital:
        return GateDecision(
            GATE_READINESS, ReadinessGateState.NOT_REQUIRED_FOR_DEMO_EXECUTION, True,
            f"{account.reason}; the user-capital readiness gate protects real-capital "
            f"deployment and does not apply",
            account.environment, False,
        )
    if db is None or not bot_instance_id:
        return GateDecision(
            GATE_READINESS, ReadinessGateState.UNAVAILABLE, False,
            "no database or bot identity; real-capital readiness cannot be evaluated",
            account.environment, True,
        )
    try:
        (asserter or assert_user_capital_activation_allowed)(db=db, bot_instance_id=bot_instance_id)
    except UserCapitalReadinessError as exc:
        payload = getattr(exc, "payload", {}) or {}
        return GateDecision(
            GATE_READINESS, ReadinessGateState.NOT_MET, False,
            f"{payload.get('reason', 'USER_CAPITAL_READINESS_NOT_MET')}: "
            f"missing={payload.get('missing_requirements', [])}",
            account.environment, True,
        )
    except Exception as exc:
        return GateDecision(
            GATE_READINESS, ReadinessGateState.UNAVAILABLE, False,
            f"readiness evaluation failed: {type(exc).__name__}: {exc}",
            account.environment, True,
        )
    return GateDecision(
        GATE_READINESS, ReadinessGateState.APPROVED_FOR_CONTROLLED_BETA, True,
        "real-capital readiness approved for this bot and policy",
        account.environment, True,
    )


__all__ = [
    "ConnectedAccountCapital",
    "GATE_KYC",
    "GATE_READINESS",
    "GateDecision",
    "ReadinessGateState",
    "classify_connected_account",
    "evaluate_execution_kyc",
    "evaluate_execution_readiness",
]
