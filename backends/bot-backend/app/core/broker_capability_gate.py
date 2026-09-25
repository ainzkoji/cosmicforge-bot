"""Bot-start gate: a bot may only run on a broker whose adapter can execute.

Checked at bot creation, at explicit start, and on every runtime cycle, so a
bot on a broker without a complete execution adapter is refused up front with
``BROKER_EXECUTION_CAPABILITY_INCOMPLETE`` instead of failing at its first
trade (the Bybit/BingX ``place_order`` AttributeError the multi-asset audit
found).

The gate also enforces ownership: the broker account must exist and belong
to the requesting user.
"""
from __future__ import annotations

import json
import logging
from typing import Any, Mapping, Optional

from shared_lib.broker.capabilities import ExecutionReadiness, execution_readiness

logger = logging.getLogger(__name__)

REASON_ACCOUNT_NOT_FOUND = "BROKER_ACCOUNT_NOT_FOUND"
REASON_ACCOUNT_NOT_OWNED = "BROKER_ACCOUNT_ACCESS_DENIED"


class BrokerCapabilityGateError(ValueError):
    def __init__(self, reason_code: str, message: str, details: Optional[Mapping[str, Any]] = None):
        self.reason_code = reason_code
        self.details = dict(details or {})
        super().__init__(json.dumps({"reason_code": reason_code, "message": message, **self.details}))


def load_permission_evidence(conn: Any, account_id: str) -> Optional[dict]:
    """Permission evidence for the account's active credential version, or None."""
    try:
        row = conn.execute(
            """
            SELECT c.permissions_json FROM broker_credentials_v2 c
            JOIN broker_accounts a ON a.id = c.account_id
            WHERE c.account_id = ? AND c.version = a.active_credential_version
            """,
            (account_id,),
        ).fetchone()
    except Exception:
        return None
    if not row or not row[0]:
        return None
    try:
        data = json.loads(row[0])
    except (TypeError, ValueError):
        return None
    perms = data.get("permissions") if isinstance(data, dict) else None
    return perms if isinstance(perms, dict) else None


def readiness_for_account(db: Any, *, user_id: str, broker_account_id: str) -> ExecutionReadiness:
    with db.connect() as conn:
        row = conn.execute(
            "SELECT id, user_id, broker_id, environment FROM broker_accounts WHERE id = ?",
            (broker_account_id,),
        ).fetchone()
        if row is None:
            raise BrokerCapabilityGateError(REASON_ACCOUNT_NOT_FOUND, f"broker account {broker_account_id!r} not found")
        if str(row["user_id"]) != str(user_id):
            logger.warning("broker_capability_gate access_denied account=%s requested_by=%s", broker_account_id, user_id)
            raise BrokerCapabilityGateError(REASON_ACCOUNT_NOT_OWNED, "broker account does not belong to this user")
        perms = load_permission_evidence(conn, broker_account_id)
    return execution_readiness(row["broker_id"], row["environment"] or "live", permissions=perms)


def assert_broker_execution_capability(db: Any, *, user_id: str, broker_account_id: str) -> ExecutionReadiness:
    readiness = readiness_for_account(db, user_id=user_id, broker_account_id=broker_account_id)
    if not readiness.permitted:
        raise BrokerCapabilityGateError(
            readiness.reason_code or "BROKER_EXECUTION_CAPABILITY_INCOMPLETE",
            readiness.detail or "broker cannot execute",
            {"missing": list(readiness.missing)},
        )
    return readiness


__all__ = [
    "BrokerCapabilityGateError",
    "REASON_ACCOUNT_NOT_FOUND",
    "REASON_ACCOUNT_NOT_OWNED",
    "assert_broker_execution_capability",
    "load_permission_evidence",
    "readiness_for_account",
]
