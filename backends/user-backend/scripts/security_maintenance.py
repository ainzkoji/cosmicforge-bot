"""
Section H 8.4 security maintenance (run from backends/user-backend).

    python scripts/security_maintenance.py reencrypt-totp
        Rewrite every legacy TOTP secret (users.totp_secret, admins.mfa_secret)
        with the HKDF-derived credential key (format "enc:v2:"). Idempotent.
        Prints counts only, never a secret.

    python scripts/security_maintenance.py reset-admin-mfa admin@example.com
        Break-glass for an admin who lost both the authenticator and every
        recovery code: clears the admin's MFA, deletes the recovery codes,
        revokes every admin session and writes an auth_audit_log row
        ("admin_mfa_reset_by_operator"). With ADMIN_MFA_REQUIRED the admin must
        enrol again at the next sign-in. Requires shell access to the server.
        Also ends every access token of the admin (token_version) and voids any
        unused enrolment bootstrap code. ``--issue-bootstrap`` issues a fresh one.

    python scripts/security_maintenance.py issue-admin-mfa-bootstrap admin@example.com [--minutes 30]
        F9: one-time, admin-bound code (default 30 min, max 24 h) that an
        enrolment needs while ADMIN_MFA_REQUIRED is on. Printed once for
        out-of-band delivery, stored hashed; earlier unused codes are revoked;
        issue / use / failures are audited (admin_mfa_bootstrap_*). Who may
        issue it and how the admin's identity is verified first is an operating
        decision, not something this script decides.

The database is the one the backend itself uses (DATABASE_URL / .env).
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

_HERE = Path(__file__).resolve().parent
_USER_BACKEND = _HERE.parent
_SHARED = _USER_BACKEND.parent / "shared"
for _p in (str(_USER_BACKEND), str(_SHARED)):
    if _p not in sys.path:
        sys.path.insert(0, _p)


def _load_env() -> None:
    try:
        from dotenv import load_dotenv
    except ImportError:
        return
    env_file = _USER_BACKEND / ".env"
    if env_file.exists():
        load_dotenv(dotenv_path=env_file, override=False)


def reencrypt_totp(conn) -> dict:
    from app.core.security import reencrypt_stored_totp_secrets
    from shared_lib.persistence.admin_mfa_schema import ensure_admin_mfa_schema

    ensure_admin_mfa_schema(conn)
    return reencrypt_stored_totp_secrets(conn)


def _find_admin(conn, email: str):
    return conn.execute("SELECT id, email FROM admins WHERE lower(email) = ?", (email.lower(),)).fetchone()


def reset_admin_mfa(conn, email: str) -> bool:
    from app.api.auth import audit_event
    from app.core.admin_mfa_bootstrap import revoke_bootstrap_codes
    from shared_lib.core.security.token_state import bump_admin_token_version
    from shared_lib.persistence.admin_mfa_schema import ensure_admin_mfa_schema

    ensure_admin_mfa_schema(conn)
    admin = _find_admin(conn, email)
    if not admin:
        return False
    now = datetime.now(timezone.utc).isoformat()
    conn.execute("UPDATE admins SET mfa_enabled = 0, mfa_secret = NULL, mfa_last_counter = NULL, "
                 "mfa_enrolled_at = NULL WHERE id = ?", (admin["id"],))
    conn.execute("DELETE FROM admin_mfa_recovery_codes WHERE admin_id = ?", (admin["id"],))
    conn.execute("UPDATE admin_sessions SET revoked_at = ? WHERE admin_id = ? AND revoked_at IS NULL",
                 (now, admin["id"]))
    # F1: every access token already issued ends too. F9: a bootstrap code
    # issued before the reset is void; re-enrolment needs a fresh one.
    bump_admin_token_version(conn, admin["id"])
    revoked_codes = revoke_bootstrap_codes(conn, admin["id"])
    audit_event(conn, "admin_mfa_reset_by_operator", user_id=admin["id"], email=admin["email"],
                details={"actor": "operator-cli", "bootstrap_codes_revoked": revoked_codes})
    return True


def issue_admin_mfa_bootstrap(conn, email: str, minutes: int):
    """``(code, expires_at)`` for the admin, or None when there is no such admin."""
    from app.core.admin_mfa_bootstrap import issue_bootstrap_code
    from shared_lib.persistence.admin_mfa_schema import ensure_admin_mfa_schema

    ensure_admin_mfa_schema(conn)
    admin = _find_admin(conn, email)
    if not admin:
        return None
    return issue_bootstrap_code(conn, admin, minutes=minutes, issued_by="operator-cli")


def _print_bootstrap(email: str, issued) -> None:
    code, expires_at = issued
    # The code is printed exactly once, here, for out-of-band delivery to the
    # admin. It is stored only as a salted hash.
    print(f"[OK] One-time MFA enrolment bootstrap code for {email} (valid until {expires_at}):")
    print(code)


# Same values as app.core.admin_mfa_bootstrap (not imported here: the app's
# settings must only load after _load_env()). issue_bootstrap_code re-checks.
BOOTSTRAP_DEFAULT_MINUTES = 30
BOOTSTRAP_MAX_MINUTES = 24 * 60


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Section H 8.4 security maintenance")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("reencrypt-totp", help="re-encrypt legacy TOTP secrets with the HKDF key")
    reset = sub.add_parser("reset-admin-mfa", help="break-glass: clear one admin's MFA")
    reset.add_argument("email")
    reset.add_argument("--issue-bootstrap", action="store_true",
                       help="also issue a fresh enrolment bootstrap code for the re-enrolment")
    reset.add_argument("--minutes", type=int, default=BOOTSTRAP_DEFAULT_MINUTES)
    issue = sub.add_parser("issue-admin-mfa-bootstrap",
                           help="issue a one-time MFA enrolment bootstrap code (deliver it out of band)")
    issue.add_argument("email")
    issue.add_argument("--minutes", type=int, default=BOOTSTRAP_DEFAULT_MINUTES,
                       help=f"validity, 1..{BOOTSTRAP_MAX_MINUTES} (default {BOOTSTRAP_DEFAULT_MINUTES})")
    args = parser.parse_args(argv)
    if not 1 <= getattr(args, "minutes", 1) <= BOOTSTRAP_MAX_MINUTES:
        print(f"[ERROR] --minutes must be between 1 and {BOOTSTRAP_MAX_MINUTES}.")
        return 2

    _load_env()
    from shared_lib.persistence.db import DB

    with DB().connect() as conn:
        if args.command == "reencrypt-totp":
            print(f"[OK] {reencrypt_totp(conn)}")
            return 0
        email = args.email.strip()
        if args.command == "issue-admin-mfa-bootstrap":
            issued = issue_admin_mfa_bootstrap(conn, email, args.minutes)
            if issued:
                _print_bootstrap(email, issued)
                return 0
        elif reset_admin_mfa(conn, email):
            print(f"[OK] MFA cleared and sessions revoked for {email}")
            if args.issue_bootstrap:
                _print_bootstrap(email, issue_admin_mfa_bootstrap(conn, email, args.minutes))
            return 0
    print("[ERROR] No admin with that email.")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
