import os
import uuid
import logging
from datetime import datetime, timezone
from shared_lib.persistence.db import DB
from app.core.security import get_password_hash

logger = logging.getLogger(__name__)

def bootstrap_admin():
    """
    Safe bootstrap mechanism for the first admin.
    Checks if the admins table is empty, and if so, seeds it using env vars.
    """
    admin_email = os.environ.get("SUPERADMIN_EMAIL")
    admin_pass = os.environ.get("SUPERADMIN_PASS")

    if not admin_email or not admin_pass:
        logger.info("[BOOTSTRAP] SUPERADMIN_EMAIL or SUPERADMIN_PASS not set. Skipping admin bootstrap.")
        return

    db = DB()
    try:
        with db.connect() as conn:
            # Check if any admin exists
            count = conn.execute("SELECT COUNT(*) FROM admins").fetchone()[0]
            if count > 0:
                logger.info("[BOOTSTRAP] Admins table is not empty. Skipping admin bootstrap.")
                return

            # Check if this specific email already exists (just in case)
            existing = conn.execute("SELECT id FROM admins WHERE email = ?", (admin_email,)).fetchone()
            if existing:
                logger.info(f"[BOOTSTRAP] Admin {admin_email} already exists. Skipping.")
                return

            # Insert default admin
            admin_id = str(uuid.uuid4())
            hashed_pw = get_password_hash(admin_pass)
            now = datetime.now(timezone.utc).isoformat()

            conn.execute(
                """
                INSERT INTO admins (
                    id, email, hashed_password, full_name, role, 
                    is_active, is_superuser, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    admin_id,
                    admin_email,
                    hashed_pw,
                    "System Admin",
                    "admin",
                    1,
                    1,
                    now,
                    now
                )
            )
            logger.info(f"[BOOTSTRAP] Successfully created bootstrap admin account: {admin_email}")

    except Exception as e:
        logger.error(f"[BOOTSTRAP] Error during admin bootstrap: {e}")
