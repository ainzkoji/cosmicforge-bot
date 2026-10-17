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


def reset_admin_mfa(conn, email: str) -> bool:
    from app.api.auth import audit_event
    from shared_lib.persistence.admin_mfa_schema import ensure_admin_mfa_schema

    ensure_admin_mfa_schema(conn)
    admin = conn.execute("SELECT id, email FROM admins WHERE lower(email) = ?", (email.lower(),)).fetchone()
    if not admin:
        return False
    now = datetime.now(timezone.utc).isoformat()
    conn.execute("UPDATE admins SET mfa_enabled = 0, mfa_secret = NULL, mfa_last_counter = NULL, "
                 "mfa_enrolled_at = NULL WHERE id = ?", (admin["id"],))
    conn.execute("DELETE FROM admin_mfa_recovery_codes WHERE admin_id = ?", (admin["id"],))
    conn.execute("UPDATE admin_sessions SET revoked_at = ? WHERE admin_id = ? AND revoked_at IS NULL",
                 (now, admin["id"]))
    audit_event(conn, "admin_mfa_reset_by_operator", user_id=admin["id"], email=admin["email"],
                details={"actor": "operator-cli"})
    return True


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Section H 8.4 security maintenance")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("reencrypt-totp", help="re-encrypt legacy TOTP secrets with the HKDF key")
    reset = sub.add_parser("reset-admin-mfa", help="break-glass: clear one admin's MFA")
    reset.add_argument("email")
    args = parser.parse_args(argv)

    _load_env()
    from shared_lib.persistence.db import DB

    with DB().connect() as conn:
        if args.command == "reencrypt-totp":
            print(f"[OK] {reencrypt_totp(conn)}")
            return 0
        if reset_admin_mfa(conn, args.email.strip()):
            print(f"[OK] MFA cleared and sessions revoked for {args.email.strip()}")
            return 0
    print("[ERROR] No admin with that email.")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
