"""Broker-INTERNAL transfer entities (Phase 2G).

A transfer moves an asset between two wallets of ONE connected broker
account. It is never a withdrawal, never a transfer to another user or
sub-account, and never cross-broker: there is no destination field that
could name anything outside the account.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Any, Dict, Mapping, Optional


class TransferStatus(str, Enum):
    REQUESTED = "REQUESTED"
    VALIDATING = "VALIDATING"
    BLOCKED = "BLOCKED"
    SUBMITTING = "SUBMITTING"
    SUBMITTED = "SUBMITTED"
    CONFIRMATION_PENDING = "CONFIRMATION_PENDING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    UNKNOWN = "UNKNOWN"
    RECONCILIATION_REQUIRED = "RECONCILIATION_REQUIRED"


TERMINAL = frozenset({TransferStatus.BLOCKED, TransferStatus.COMPLETED, TransferStatus.FAILED})
#: in flight at the broker (or possibly): the account's other transfers wait
IN_FLIGHT = frozenset({TransferStatus.SUBMITTING, TransferStatus.SUBMITTED, TransferStatus.CONFIRMATION_PENDING,
                       TransferStatus.UNKNOWN, TransferStatus.RECONCILIATION_REQUIRED})
RECONCILABLE = frozenset({TransferStatus.SUBMITTED, TransferStatus.CONFIRMATION_PENDING, TransferStatus.UNKNOWN,
                          TransferStatus.RECONCILIATION_REQUIRED})

ALLOWED_TRANSITIONS: Mapping[TransferStatus, frozenset] = {
    TransferStatus.REQUESTED: frozenset({TransferStatus.VALIDATING}),
    TransferStatus.VALIDATING: frozenset({TransferStatus.BLOCKED, TransferStatus.SUBMITTING}),
    TransferStatus.SUBMITTING: frozenset({TransferStatus.SUBMITTED, TransferStatus.CONFIRMATION_PENDING,
                                          TransferStatus.COMPLETED, TransferStatus.FAILED, TransferStatus.UNKNOWN}),
    TransferStatus.SUBMITTED: frozenset({TransferStatus.CONFIRMATION_PENDING, TransferStatus.COMPLETED,
                                         TransferStatus.FAILED, TransferStatus.RECONCILIATION_REQUIRED}),
    TransferStatus.CONFIRMATION_PENDING: frozenset({TransferStatus.COMPLETED, TransferStatus.FAILED,
                                                    TransferStatus.RECONCILIATION_REQUIRED}),
    TransferStatus.UNKNOWN: frozenset({TransferStatus.COMPLETED, TransferStatus.FAILED,
                                       TransferStatus.CONFIRMATION_PENDING, TransferStatus.RECONCILIATION_REQUIRED}),
    TransferStatus.RECONCILIATION_REQUIRED: frozenset({TransferStatus.COMPLETED, TransferStatus.FAILED,
                                                       TransferStatus.CONFIRMATION_PENDING}),
    TransferStatus.BLOCKED: frozenset(),
    TransferStatus.COMPLETED: frozenset(),
    TransferStatus.FAILED: frozenset(),
}


class TransferOrigin(str, Enum):
    MANUAL = "MANUAL"
    AUTOMATED = "AUTOMATED_INTERNAL_REALLOCATION"
    CAPITAL_PLANNER = "CAPITAL_PLANNER"


class BlockReason(str, Enum):
    ACCOUNT_NOT_OWNED = "ACCOUNT_NOT_OWNED"
    CREDENTIAL_NOT_ACTIVE = "CREDENTIAL_NOT_ACTIVE"
    INTERNAL_TRANSFER_UNSUPPORTED = "INTERNAL_TRANSFER_UNSUPPORTED"
    VENUE_API_UNAVAILABLE = "VENUE_API_UNAVAILABLE"
    PERMISSION_EVIDENCE_REQUIRED = "PERMISSION_EVIDENCE_REQUIRED"
    WITHDRAW_PERMISSION_PRESENT = "WITHDRAW_PERMISSION_PRESENT"
    WITHDRAW_PERMISSION_UNVERIFIED = "WITHDRAW_PERMISSION_UNVERIFIED"
    INTERNAL_TRANSFER_PERMISSION_MISSING = "INTERNAL_TRANSFER_PERMISSION_MISSING"
    UNKNOWN_SOURCE_WALLET = "UNKNOWN_SOURCE_WALLET"
    UNKNOWN_DESTINATION_WALLET = "UNKNOWN_DESTINATION_WALLET"
    SAME_WALLET = "SAME_WALLET"
    ROUTE_UNSUPPORTED = "ROUTE_UNSUPPORTED"
    SHARED_COLLATERAL_NO_TRANSFER_NEEDED = "SHARED_COLLATERAL_NO_TRANSFER_NEEDED"
    ASSET_NOT_ALLOWED = "ASSET_NOT_ALLOWED"
    WALLET_NOT_ALLOWED = "WALLET_NOT_ALLOWED"
    INVALID_AMOUNT = "INVALID_AMOUNT"
    MAX_TRANSFER_AMOUNT_EXCEEDED = "MAX_TRANSFER_AMOUNT_EXCEEDED"
    DAILY_TRANSFER_LIMIT_EXCEEDED = "DAILY_TRANSFER_LIMIT_EXCEEDED"
    SOURCE_BALANCE_UNAVAILABLE = "SOURCE_BALANCE_UNAVAILABLE"
    INSUFFICIENT_TRANSFERABLE_BALANCE = "INSUFFICIENT_TRANSFERABLE_BALANCE"
    MIN_FUNDING_BALANCE = "MIN_FUNDING_BALANCE"
    MIN_DERIVATIVES_RESERVE = "MIN_DERIVATIVES_RESERVE"
    MIN_FREE_MARGIN = "MIN_FREE_MARGIN"
    PENDING_RISK_RESERVATIONS = "PENDING_RISK_RESERVATIONS"
    BOT_ALLOCATION_CONSTRAINT = "BOT_ALLOCATION_CONSTRAINT"
    TRANSFER_IN_FLIGHT = "TRANSFER_IN_FLIGHT"
    ACCOUNT_MODE_UNKNOWN = "ACCOUNT_MODE_UNKNOWN"
    AUTOMATION_NOT_AUTHORIZED = "AUTOMATION_NOT_AUTHORIZED"


class IdempotencyConflict(ValueError):
    """The idempotency key was already used for a DIFFERENT transfer."""


@dataclass(frozen=True)
class TransferIntent:
    user_id: str
    broker_account_id: str
    asset: str
    amount: Decimal
    source_wallet: str
    destination_wallet: str
    idempotency_key: str
    origin: TransferOrigin = TransferOrigin.MANUAL
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def fingerprint(self) -> Dict[str, str]:
        return {"asset": self.asset.upper(), "amount": format(self.amount.normalize(), "f"),
                "source_wallet": self.source_wallet.upper(), "destination_wallet": self.destination_wallet.upper()}


@dataclass(frozen=True)
class SubmitOutcome:
    """What the broker said to a submission. ``status`` is COMPLETED, FAILED,
    CONFIRMATION_PENDING or SUBMITTED (accepted, final state not reported)."""
    status: TransferStatus
    broker_transfer_id: Optional[str]
    raw_status: Optional[str] = None
    detail: str = ""


@dataclass(frozen=True)
class LookupOutcome:
    """Broker-authoritative state of one transfer. ``found=False`` means the
    broker's history does not (yet) show it -- NOT that it failed."""
    found: bool
    status: Optional[TransferStatus] = None
    broker_transfer_id: Optional[str] = None
    raw_status: Optional[str] = None


@dataclass(frozen=True)
class HistoryRow:
    broker_transfer_id: str
    asset: str
    amount: Decimal
    source_native: str
    destination_native: str
    status: TransferStatus
    timestamp_ms: int
    raw: Mapping[str, Any] = field(default_factory=dict)


__all__ = ["ALLOWED_TRANSITIONS", "BlockReason", "HistoryRow", "IN_FLIGHT", "IdempotencyConflict", "LookupOutcome",
           "RECONCILABLE", "SubmitOutcome", "TERMINAL", "TransferIntent", "TransferOrigin", "TransferStatus"]
