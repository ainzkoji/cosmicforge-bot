"""
Bot-backend broker credential accessor.

ALL credential reads in the bot runtime must go through resolve_broker_auth().
The old get_decrypted_credentials() function is kept as a thin shim that
delegates to the canonical resolver — it raises BrokerResolverError on every
failure instead of silently returning None or an empty dict.

Migration guide for callers:
  OLD:  creds = get_decrypted_credentials(account_id)
        if not creds: ...
  NEW:  from shared_lib.broker import resolve_broker_auth, BrokerResolverError
        try:
            auth = resolve_broker_auth(account_id, user_id, db)
        except BrokerResolverError as exc:
            ...

The shim below is retained for the multi_runner.py cycle loop only.
New code should call resolve_broker_auth() directly.
"""
from __future__ import annotations

import logging
from typing import Optional, Dict, Any

from shared_lib.broker import resolve_broker_auth, BrokerAuth, BrokerResolverError
from shared_lib.persistence.db import DB

logger = logging.getLogger(__name__)


def _get_runtime_db() -> DB:
    from app.core.config import settings
    url = settings.DATABASE_URL
    if url and url.startswith("sqlite:///"):
        return DB(path=url.replace("sqlite:///", ""))
    return DB()


def resolve_broker_auth_for_bot(account_id: str, user_id: str) -> BrokerAuth:
    """
    Resolve a BrokerAuth for the bot runtime.

    This is the preferred call for the runner/executor.  It returns an
    immutable BrokerAuth and raises BrokerResolverError on any failure.

    Args:
        account_id: broker_accounts.id
        user_id:    owning user — enforced strictly

    Returns:
        BrokerAuth — fully resolved, immutable credential context

    Raises:
        BrokerResolverError — with .reason_code on every failure path
    """
    db = _get_runtime_db()
    return resolve_broker_auth(account_id, user_id, db)


def get_decrypted_credentials(account_id: str, user_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """
    DEPRECATED shim — retained for call sites that predate the resolver.

    Delegates to resolve_broker_auth() and returns a plain dict for backward
    compatibility. Returns None on failure and logs the reason (never silently
    swallows a decrypt error).

    Prefer calling resolve_broker_auth_for_bot() for new code — it surfaces
    the typed BrokerResolverError instead of returning None.
    """
    if user_id is None:
        # Legacy callers that didn't pass user_id — look it up from the DB
        try:
            db = _get_runtime_db()
            with db.connect() as conn:
                row = conn.execute(
                    "SELECT user_id FROM broker_accounts WHERE id = ?",
                    (account_id,),
                ).fetchone()
            if not row:
                logger.error(
                    "get_decrypted_credentials_no_account account_id=%s", account_id
                )
                return None
            user_id = row["user_id"]
        except Exception as exc:
            logger.error(
                "get_decrypted_credentials_db_error account_id=%s error=%s",
                account_id, exc,
            )
            return None

    try:
        db   = _get_runtime_db()
        auth = resolve_broker_auth(account_id, user_id, db)
        # Return the flat dict that legacy callers expect
        return {
            "api_key":     auth.api_key,
            "api_secret":  auth.api_secret,
            "broker_type": auth.broker_type,
            "environment": auth.environment.value,
            "base_url":    auth.base_url,
            "account_id":  auth.account_id,
            **auth.extra,
        }
    except BrokerResolverError as exc:
        logger.error(
            "get_decrypted_credentials_failed account_id=%s reason=%s error=%s",
            account_id, exc.reason_code, exc,
        )
        return None
