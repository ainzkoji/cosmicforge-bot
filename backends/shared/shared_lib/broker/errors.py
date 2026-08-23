"""
Typed broker resolver errors with explicit reason codes.

All broker resolution failures raise BrokerResolverError.
Callers must check .reason_code to distinguish auth failures from
programming errors, so that PolicyEngine and logging can surface
actionable context instead of generic "zero equity".
"""
from __future__ import annotations
from typing import Optional


class BrokerResolverError(Exception):
    """
    Raised by resolve_broker_auth() on any resolution failure.

    Attributes:
        reason_code: machine-readable failure category (use the constants below)
        account_id:  broker_account_id being resolved (if known)
        message:     human-readable description
    """

    # ── Reason codes ────────────────────────────────────────────────────────
    REASON_NOT_FOUND         = "broker_not_found"
    REASON_AUTH_FAILED       = "broker_auth_failed"
    REASON_ENV_MISMATCH      = "broker_env_mismatch"
    REASON_INVALID           = "broker_invalid"
    REASON_PENDING           = "broker_validation_pending"
    REASON_SUPERSEDED        = "broker_superseded"
    REASON_NO_CREDENTIALS    = "broker_no_credentials"
    REASON_ACCESS_DENIED     = "broker_access_denied"
    REASON_DECRYPT_FAILED    = "broker_decrypt_failed"
    REASON_REVOKED           = "broker_revoked"

    def __init__(
        self,
        reason_code: str,
        message: str,
        account_id: Optional[str] = None,
    ) -> None:
        super().__init__(message)
        self.reason_code = reason_code
        self.account_id = account_id

    def __repr__(self) -> str:
        return (
            f"BrokerResolverError(reason={self.reason_code!r}, "
            f"account_id={self.account_id!r}, message={str(self)!r})"
        )
