#!/usr/bin/env python3
"""Grant the admin role (admin_roles table) to an existing user.

USAGE (run from backends/user-backend)
    python scripts/grant_admin.py user@your-domain.example

The email address is a required argument: nothing about who becomes an admin
is stored in this file. The user must already exist. No password is involved
(to create an admin-portal login use scripts/create_admin.py).
"""
from __future__ import annotations

import argparse
import sys
import uuid
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


def grant_admin(conn, email: str, now: str) -> str:
    """Returns "not_found", "already_admin" or "granted"."""
    user = conn.execute("SELECT id FROM users WHERE lower(email) = ?", (email,)).fetchone()
    if not user:
        return "not_found"
    user_id = user["id"]
    existing = conn.execute(
        "SELECT id FROM admin_roles WHERE user_id = ? AND revoked_at IS NULL", (user_id,)
    ).fetchone()
    if existing:
        return "already_admin"
    conn.execute(
        "INSERT INTO admin_roles (id, user_id, role, granted_by, granted_at) VALUES (?, ?, 'admin', 'system', ?)",
        (str(uuid.uuid4()), user_id, now),
    )
    return "granted"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Grant the admin role to an existing user.")
    parser.add_argument("email", help="Email address of the user to make an admin")
    args = parser.parse_args(argv)

    email = args.email.strip().lower()
    if "@" not in email or email.startswith("@") or email.endswith("@"):
        print("[ERROR] That does not look like an email address.")
        return 2

    _load_env()
    from shared_lib.persistence.db import DB, utc_now_iso

    db = DB()
    with db.connect() as conn:
        outcome = grant_admin(conn, email, utc_now_iso())
        admin_count = conn.execute(
            "SELECT COUNT(*) AS cnt FROM admin_roles WHERE revoked_at IS NULL"
        ).fetchone()["cnt"]

    if outcome == "not_found":
        print(f"[ERROR] No user with email {email}. The user must register first.")
        return 1
    if outcome == "already_admin":
        print(f"[INFO] {email} already has the admin role. Nothing changed.")
    else:
        print(f"[OK] Admin role granted to {email}")
    print(f"Total active admin roles: {admin_count}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
