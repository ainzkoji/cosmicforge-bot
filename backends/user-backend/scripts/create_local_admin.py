"""
create_local_admin.py — Dev-only admin credential management script.

Creates a new admin record in the `admins` table, or resets the password for
an existing one.  Uses the same passlib bcrypt/argon2 hashing as the live
auth system so credentials are 100% compatible with the login endpoint.

USAGE
-----
  # Create a new admin (fails if email already exists without --reset-password)
  python scripts/create_local_admin.py \\
      --email admin@example.com \\
      --password "YourPassword123!" \\
      --name "Local Dev Admin"

  # Reset password for existing admin (explicit opt-in required)
  python scripts/create_local_admin.py \\
      --email admin@example.com \\
      --password "NewPassword123!" \\
      --reset-password

  # Check status of an existing admin (no writes)
  python scripts/create_local_admin.py --email admin@example.com --check

SAFETY
------
  - LOCAL / DEV ONLY.  Do not run against a production database.
  - Requires explicit --reset-password to overwrite an existing password.
  - Never prints the password hash or any token.
  - Does not start automatically; must be invoked manually.
  - Does not weaken JWT validation or auth middleware.
"""
from __future__ import annotations

import argparse
import os
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# Path bootstrap — allow running from the user-backend root or repo root
# ---------------------------------------------------------------------------
_HERE = Path(__file__).resolve().parent
_BACKENDS = _HERE.parent.parent          # backends/
_SHARED = _BACKENDS / "shared"
_USER_BACKEND = _HERE.parent             # backends/user-backend/

for _p in (str(_USER_BACKEND), str(_BACKENDS), str(_SHARED)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

# Load .env so DATABASE_URL is available to the DB class
try:
    from dotenv import load_dotenv as _load_dotenv
    _env_file = _USER_BACKEND / ".env"
    if not _env_file.exists():
        # Fallback: try bot-backend .env (same DATABASE_URL)
        _env_file = _BACKENDS / "bot-backend" / ".env"
    _load_dotenv(dotenv_path=_env_file, override=False)
except ImportError:
    pass  # python-dotenv not installed; rely on os.environ


def _get_db():
    """Return a DB instance using the same path resolution as the live backend."""
    try:
        from shared_lib.persistence.db import DB
        return DB()
    except ImportError as exc:
        print(f"[ERROR] Cannot import shared_lib.persistence.db: {exc}")
        print("        Run this script from the user-backend directory or ensure")
        print("        shared_lib is on PYTHONPATH.")
        sys.exit(1)


def _get_pwd_context():
    """Return the same passlib CryptContext used by app/core/security.py."""
    try:
        from passlib.context import CryptContext
        return CryptContext(schemes=["bcrypt", "argon2"], deprecated="auto")
    except ImportError as exc:
        print(f"[ERROR] passlib not installed: {exc}")
        print("        pip install passlib[bcrypt,argon2]")
        sys.exit(1)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# Core operations (all writes go through these helpers)
# ---------------------------------------------------------------------------

def check_admin(conn, email: str) -> dict | None:
    row = conn.execute(
        "SELECT id, email, role, is_active, is_superuser, created_at, last_login_at "
        "FROM admins WHERE email = ?",
        (email,),
    ).fetchone()
    return dict(row) if row else None


def create_admin(conn, email: str, hashed_password: str, name: str) -> str:
    admin_id = str(uuid.uuid4())
    now = _utc_now()
    conn.execute(
        """
        INSERT INTO admins
            (id, email, hashed_password, full_name, role,
             is_active, is_superuser, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (admin_id, email, hashed_password, name, "admin", 1, 1, now, now),
    )
    return admin_id


def update_password(conn, email: str, hashed_password: str) -> None:
    now = _utc_now()
    conn.execute(
        "UPDATE admins SET hashed_password = ?, updated_at = ? WHERE email = ?",
        (hashed_password, now, email),
    )


def ensure_active(conn, email: str) -> None:
    now = _utc_now()
    conn.execute(
        "UPDATE admins SET is_active = 1, updated_at = ? WHERE email = ?",
        (now, email),
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Dev-only: create or reset a local admin in the admins table.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--email", required=True, help="Admin email address")
    parser.add_argument("--password", default=None, help="Plain-text password (required unless --check)")
    parser.add_argument("--name", default="Local Dev Admin", help="Display name (used only on create)")
    parser.add_argument(
        "--reset-password",
        action="store_true",
        help="Allow overwriting the password for an existing admin (explicit opt-in)",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Print admin status without making any changes",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    # -- Validate inputs early --
    if not args.check and not args.password:
        print("[ERROR] --password is required unless using --check")
        sys.exit(1)

    if args.password and len(args.password) < 8:
        print("[ERROR] Password must be at least 8 characters")
        sys.exit(1)

    db = _get_db()
    pwd_context = _get_pwd_context() if not args.check else None

    with db.connect() as conn:

        # ── Check-only mode ──────────────────────────────────────────────────
        if args.check:
            existing = check_admin(conn, args.email)
            if not existing:
                print(f"[INFO] No admin found for email: {args.email}")
            else:
                print(f"[OK]  Admin found:")
                print(f"      id          : {existing['id']}")
                print(f"      email       : {existing['email']}")
                print(f"      role        : {existing['role']}")
                print(f"      is_active   : {existing['is_active']}")
                print(f"      is_superuser: {existing['is_superuser']}")
                print(f"      created_at  : {existing['created_at']}")
                print(f"      last_login  : {existing['last_login_at'] or 'never'}")
            return

        # ── Hash the password (never stored or printed after this point) ─────
        hashed = pwd_context.hash(args.password)

        existing = check_admin(conn, args.email)

        if not existing:
            # ── CREATE new admin ─────────────────────────────────────────────
            admin_id = create_admin(conn, args.email, hashed, args.name)
            print(f"[OK]  Created admin:")
            print(f"      email : {args.email}")
            print(f"      name  : {args.name}")
            print(f"      id    : {admin_id}")
            print(f"      role  : admin  |  is_active: 1  |  is_superuser: 1")

        elif args.reset_password:
            # ── RESET password for existing admin ────────────────────────────
            update_password(conn, args.email, hashed)
            ensure_active(conn, args.email)
            print(f"[OK]  Password updated for: {args.email}")
            print(f"      Admin is_active set to 1 (no-op if already active)")

        else:
            # ── Admin exists but --reset-password not given ──────────────────
            print(f"[INFO] Admin already exists for: {args.email}")
            print(f"       To reset the password, re-run with --reset-password")
            print(f"       To check status, re-run with --check")
            sys.exit(0)

    print()
    print("[DONE] You can now log in at http://localhost:4173/login")
    print(f"       Email    : {args.email}")
    print(f"       Login URL: http://localhost:4173/login")


if __name__ == "__main__":
    main()
