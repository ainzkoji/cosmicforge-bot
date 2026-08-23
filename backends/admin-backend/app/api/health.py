from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter, Depends

from app.core.auth import sanitize_admin_identity
from app.core.config import settings
from app.core.deps import require_admin
from app.persistence.db import AdminDB, get_db


router = APIRouter()


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@router.get("/health")
def health(db: AdminDB = Depends(get_db)) -> dict:
    database_reachable = False
    try:
        with db.connect() as conn:
            conn.execute("SELECT 1").fetchone()
        database_reachable = True
    except Exception:
        database_reachable = False

    return {
        "status": "ok" if database_reachable else "degraded",
        "service": "admin-backend",
        "timestamp": _utc_now_iso(),
        "database_reachable": database_reachable,
        "user_backend_url": settings.USER_BACKEND_URL,
        "bot_backend_url": settings.BOT_BACKEND_URL,
    }


@router.get("/health/admin-auth-check")
def admin_auth_check(current_admin: dict = Depends(require_admin)) -> dict:
    return {
        "status": "ok",
        "service": "admin-backend",
        "admin": sanitize_admin_identity(current_admin),
    }
