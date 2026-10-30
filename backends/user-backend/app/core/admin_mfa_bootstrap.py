"""One-time admin MFA enrolment bootstrap codes (Section H 8.4, finding F9).

Without them, the first enrolment is trust-on-first-use: whoever holds an
un-enrolled admin's password when ``ADMIN_MFA_REQUIRED`` is on could register
their own authenticator. While MFA is required, every enrolment (first, or
again after the break-glass reset) therefore also needs a bootstrap code that
an operator with shell access issues out of band::

    python scripts/security_maintenance.py issue-admin-mfa-bootstrap admin@example.com [--minutes 30]

A code is bound to one admin, single use, short lived (default 30 minutes, at
most 24 hours), stored only as a salted SHA-256 hash, and issuing a new one
revokes the admin's earlier unused codes. Issue, use and every failed attempt
are written to ``auth_audit_log``; failed attempts also count against the
admin MFA rate limit of the enrolment endpoint (5 per 15 minutes).

This module provides the mechanism and the audit rows only. Who may issue a
code, and how an operator verifies the admin's identity before doing so, is an
operating decision outside the code.
"""
from __future__ import annotations

import secrets
import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple

from app.core.security import hash_recovery_code, verify_recovery_code
from shared_lib.persistence.admin_mfa_schema import ensure_admin_mfa_schema

BOOTSTRAP_DEFAULT_MINUTES = 30
BOOTSTRAP_MAX_MINUTES = 24 * 60
_ALPHABET = "abcdefghjkmnpqrstuvwxyz23456789"  # no 0/o, 1/l/i

EVENT_ISSUED = "admin_mfa_bootstrap_issued"
EVENT_USED = "admin_mfa_bootstrap_used"
EVENT_FAILED = "admin_mfa_bootstrap_failed"
EVENT_REVOKED = "admin_mfa_bootstrap_revoked"


def _now() -> datetime:
    return datetime.now(timezone.utc)


def generate_bootstrap_code() -> str:
    """``xxxxx-xxxxx-xxxxx`` (~74 bits)."""
    raw = "".join(secrets.choice(_ALPHABET) for _ in range(15))
    return f"{raw[:5]}-{raw[5:10]}-{raw[10:]}"


def _audit(conn, event_type: str, admin, details: Optional[dict] = None, ip: Optional[str] = None) -> None:
    from app.api.auth import audit_event  # late import: app.api.auth imports app.core modules

    audit_event(conn, event_type, user_id=str(admin["id"]), email=admin["email"], ip=ip,
                details={"actor": "admin", **(details or {})})


def revoke_bootstrap_codes(conn, admin_id: str) -> int:
    """Revoke every unused code of the admin. Returns how many."""
    ensure_admin_mfa_schema(conn)
    result = conn.execute(
        "UPDATE admin_mfa_bootstrap_codes SET revoked_at = ? "
        "WHERE admin_id = ? AND used_at IS NULL AND revoked_at IS NULL",
        (_now().isoformat(), admin_id),
    )
    return result.rowcount or 0


def issue_bootstrap_code(conn, admin, *, minutes: int = BOOTSTRAP_DEFAULT_MINUTES,
                         issued_by: str = "operator-cli") -> Tuple[str, str]:
    """Issue a code for ``admin`` (a row with id and email). Returns
    ``(code, expires_at)``; the code exists only in this return value."""
    minutes = int(minutes)
    if not 1 <= minutes <= BOOTSTRAP_MAX_MINUTES:
        raise ValueError(f"minutes must be between 1 and {BOOTSTRAP_MAX_MINUTES}")
    ensure_admin_mfa_schema(conn)
    revoked = revoke_bootstrap_codes(conn, admin["id"])
    code = generate_bootstrap_code()
    now = _now()
    expires_at = (now + timedelta(minutes=minutes)).isoformat()
    conn.execute(
        "INSERT INTO admin_mfa_bootstrap_codes (id, admin_id, code_hash, issued_by, created_at, expires_at) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (str(uuid.uuid4()), admin["id"], hash_recovery_code(code), issued_by, now.isoformat(), expires_at),
    )
    _audit(conn, EVENT_ISSUED, admin, {"actor": issued_by, "expires_at": expires_at,
                                       "replaced_unused_codes": revoked})
    return code, expires_at


def matching_bootstrap_code(conn, admin_id: str, code: Optional[str]) -> Optional[str]:
    """Id of the admin's valid (unused, unrevoked, unexpired) code that matches, else None."""
    if not (code or "").strip():
        return None
    rows = conn.execute(
        "SELECT id, code_hash, expires_at FROM admin_mfa_bootstrap_codes "
        "WHERE admin_id = ? AND used_at IS NULL AND revoked_at IS NULL",
        (admin_id,),
    ).fetchall()
    now = _now()
    match = None
    for row in rows:  # no early exit: every candidate is compared
        try:
            expired = datetime.fromisoformat(row["expires_at"]) <= now
        except (TypeError, ValueError):
            expired = True
        if verify_recovery_code(code, row["code_hash"]) and not expired:
            match = row["id"]
    return match


def consume_bootstrap_code(conn, code_id: str) -> bool:
    """Spend a code exactly once (guarded UPDATE)."""
    spent = conn.execute(
        "UPDATE admin_mfa_bootstrap_codes SET used_at = ? WHERE id = ? AND used_at IS NULL AND revoked_at IS NULL",
        (_now().isoformat(), code_id),
    )
    return spent.rowcount == 1


def record_failure(conn, admin, reason: str, ip: Optional[str] = None) -> None:
    _audit(conn, EVENT_FAILED, admin, {"reason": reason}, ip=ip)


def record_use(conn, admin, ip: Optional[str] = None) -> None:
    _audit(conn, EVENT_USED, admin, ip=ip)


__all__ = [
    "BOOTSTRAP_DEFAULT_MINUTES", "BOOTSTRAP_MAX_MINUTES", "EVENT_FAILED", "EVENT_ISSUED", "EVENT_REVOKED",
    "EVENT_USED", "consume_bootstrap_code", "generate_bootstrap_code", "issue_bootstrap_code",
    "matching_bootstrap_code", "record_failure", "record_use", "revoke_bootstrap_codes",
]
