"""
Create an admin account (admins table), or reset an existing admin's password.

USAGE (run from backends/user-backend)
    python scripts/create_admin.py admin@your-domain.example
    python scripts/create_admin.py admin@your-domain.example --name "Ops Admin"
    python scripts/create_admin.py admin@your-domain.example --reset-password

The password is NEVER taken from the command line (it would end up in shell
history and the process list) and is never printed or logged. It is read from:
  1. the ADMIN_PASSWORD environment variable, if set (for automation), or
  2. an interactive hidden prompt (asked twice).

It must be at least 12 characters and at most 72 bytes (the bcrypt limit).
The database is the one the backend itself uses (DATABASE_URL / .env).
"""
from __future__ import annotations

import argparse
import getpass
import os
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

MIN_PASSWORD_LENGTH = 12
MAX_PASSWORD_BYTES = 72
PASSWORD_ENV = "ADMIN_PASSWORD"

_HERE = Path(__file__).resolve().parent
_USER_BACKEND = _HERE.parent
_SHARED = _USER_BACKEND.parent / "shared"

for _p in (str(_USER_BACKEND), str(_SHARED)):
    if _p not in sys.path:
        sys.path.insert(0, _p)


def password_problem(password: str) -> str | None:
    """Why the password is not acceptable, or None when it is."""
    if len(password) < MIN_PASSWORD_LENGTH:
        return f"Password must be at least {MIN_PASSWORD_LENGTH} characters."
    if len(password.encode("utf-8")) > MAX_PASSWORD_BYTES:
        return f"Password must be at most {MAX_PASSWORD_BYTES} bytes."
    return None


def read_password() -> str:
    """Password from ADMIN_PASSWORD or a hidden prompt. Exits on an invalid one."""
    from_env = os.environ.get(PASSWORD_ENV)
    if from_env:
        password = from_env
    else:
        if not sys.stdin.isatty():
            sys.exit(f"[ERROR] No terminal for a password prompt. Set {PASSWORD_ENV} instead.")
        password = getpass.getpass("Admin password (hidden): ")
        if password != getpass.getpass("Repeat password: "):
            sys.exit("[ERROR] Passwords do not match.")
    problem = password_problem(password)
    if problem:
        sys.exit(f"[ERROR] {problem}")
    return password


def _load_env() -> None:
    try:
        from dotenv import load_dotenv
    except ImportError:
        return
    env_file = _USER_BACKEND / ".env"
    if env_file.exists():
        load_dotenv(dotenv_path=env_file, override=False)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def upsert_admin(conn, email: str, hashed_password: str, name: str, reset_password: bool) -> str:
    """Create the admin, or (only with reset_password) replace its password.

    Returns "created", "password_reset" or "exists".
    """
    existing = conn.execute("SELECT id FROM admins WHERE email = ?", (email,)).fetchone()
    now = _now()
    if not existing:
        conn.execute(
            """
            INSERT INTO admins
                (id, email, hashed_password, full_name, role,
                 is_active, is_superuser, created_at, updated_at)
            VALUES (?, ?, ?, ?, 'admin', 1, 1, ?, ?)
            """,
            (str(uuid.uuid4()), email, hashed_password, name, now, now),
        )
        return "created"
    if not reset_password:
        return "exists"
    conn.execute(
        "UPDATE admins SET hashed_password = ?, is_active = 1, updated_at = ? WHERE email = ?",
        (hashed_password, now, email),
    )
    # A new password ends every session opened with the old one.
    conn.execute(
        "UPDATE admin_sessions SET revoked_at = ? WHERE admin_id = ? AND revoked_at IS NULL",
        (now, existing["id"]),
    )
    return "password_reset"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Create an admin account or reset an admin password. "
                    f"The password comes from {PASSWORD_ENV} or a hidden prompt, never from an argument.")
    parser.add_argument("email", help="Admin email address")
    parser.add_argument("--name", default="Admin", help="Display name (used only when creating)")
    parser.add_argument("--reset-password", action="store_true",
                        help="Allow replacing the password of an admin that already exists")
    args = parser.parse_args(argv)

    email = args.email.strip().lower()
    if "@" not in email or email.startswith("@") or email.endswith("@"):
        print("[ERROR] That does not look like an email address.")
        return 2

    password = read_password()

    _load_env()
    from app.core.security import get_password_hash
    from shared_lib.persistence.db import DB

    hashed = get_password_hash(password)
    del password

    db = DB()
    with db.connect() as conn:
        outcome = upsert_admin(conn, email, hashed, args.name, args.reset_password)

    if outcome == "created":
        print(f"[OK] Admin created: {email}")
    elif outcome == "password_reset":
        print(f"[OK] Password replaced and existing sessions revoked for: {email}")
    else:
        print(f"[INFO] Admin already exists: {email}. Nothing changed "
              "(re-run with --reset-password to replace the password).")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
