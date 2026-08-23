"""
Canonical broker authentication resolver.

This is the ONLY module allowed to read broker_credentials from the database
and construct a BrokerAuth object. All other code — bot runtime, portfolio
service, equity snapshot, broker page — must call resolve_broker_auth() and
use the returned BrokerAuth.

Design guarantees:
  - Strict user ownership: account_id must belong to user_id or REASON_ACCESS_DENIED
  - Explicit status lifecycle: any non-connectable status raises a typed error
  - Single environment source of truth: broker_accounts.environment wins;
    blob environment must match or an error is raised (not silently ignored)
  - Active credential version: reads active_credential_version from broker_accounts;
    falls back to highest-version active credential for legacy rows
  - Decrypt failure raises REASON_DECRYPT_FAILED (never silently returns empty creds)
  - Never returns None — always raises BrokerResolverError on failure
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

from shared_lib.broker.errors import BrokerResolverError
from shared_lib.broker.environment import (
    BrokerEnvironment,
    normalize_environment,
    resolve_base_url,
)
from shared_lib.core.security.broker_security import decrypt_credentials
from shared_lib.persistence.db import DB

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class BrokerAuth:
    """
    Fully resolved, validated broker authentication context.
    Immutable — safe to pass across threads and cache.
    """
    account_id:          str
    user_id:             str
    broker_type:         str
    environment:         BrokerEnvironment
    base_url:            str
    api_key:             str
    api_secret:          str
    credential_version:  int
    key_fingerprint:     str   # last 4 chars of api_key — safe for logs
    # Extra fields for brokers that need more (e.g. OANDA account_id, MT bridge URL)
    extra: Dict[str, Any] = field(default_factory=dict, compare=False, hash=False)

    def safe_log_dict(self) -> dict:
        """Return a dict safe to include in structured logs (no secrets)."""
        return {
            "account_id":         self.account_id,
            "user_id":            self.user_id,
            "broker_type":        self.broker_type,
            "environment":        self.environment.value,
            "base_url":           self.base_url,
            "key_fingerprint":    self.key_fingerprint,
            "credential_version": self.credential_version,
        }


# ── Non-connectable status codes ─────────────────────────────────────────────
_NON_CONNECTABLE_STATUSES = {
    "draft":       BrokerResolverError.REASON_PENDING,
    "pending":     BrokerResolverError.REASON_PENDING,
    "validating":  BrokerResolverError.REASON_PENDING,
    "invalid":     BrokerResolverError.REASON_INVALID,
    "superseded":  BrokerResolverError.REASON_SUPERSEDED,
    "revoked":     BrokerResolverError.REASON_REVOKED,
    "deleted":     BrokerResolverError.REASON_REVOKED,
    "error":       BrokerResolverError.REASON_INVALID,
}

_CONNECTABLE_STATUSES = {"connected", "active"}


def resolve_broker_auth(
    account_id: str,
    user_id: str,
    db: DB,
) -> BrokerAuth:
    """
    Resolve active, validated broker credentials for the given account.

    Args:
        account_id: broker_accounts.id
        user_id:    owning user — enforced strictly
        db:         DB instance pointing at the runtime database

    Returns:
        BrokerAuth — fully resolved, immutable credential context

    Raises:
        BrokerResolverError — with explicit .reason_code on every failure path
    """
    with db.connect() as conn:
        # ── 1. Load account row ───────────────────────────────────────────────
        # Use only columns guaranteed to exist on all DB schema versions.
        # active_credential_version is added by migration section 34; if missing
        # (pre-migration DB), it won't be in the SELECT and account.get() returns None.
        _acct_cols = {r["name"] for r in conn.execute("PRAGMA table_info(broker_accounts)").fetchall()}
        _select_cols = ["id", "user_id", "broker_id", "environment", "status", "last_error_message"]
        if "active_credential_version" in _acct_cols:
            _select_cols.insert(5, "active_credential_version")

        account_row = conn.execute(
            f"SELECT {', '.join(_select_cols)} FROM broker_accounts WHERE id = ?",
            (account_id,),
        ).fetchone()

        if account_row is None:
            raise BrokerResolverError(
                BrokerResolverError.REASON_NOT_FOUND,
                f"Broker account {account_id!r} not found",
                account_id=account_id,
            )

        account = dict(account_row)

        # ── 2. Strict ownership check ────────────────────────────────────────
        if account["user_id"] != user_id:
            # Log at WARN — could be a programming error or a security probe
            logger.warning(
                "broker_access_denied account_id=%s requested_by=%s owner=%s",
                account_id, user_id, account["user_id"],
            )
            raise BrokerResolverError(
                BrokerResolverError.REASON_ACCESS_DENIED,
                f"Broker account {account_id!r} does not belong to user {user_id!r}",
                account_id=account_id,
            )

        # ── 3. Status lifecycle check ────────────────────────────────────────
        status = (account["status"] or "").lower()
        if status not in _CONNECTABLE_STATUSES:
            reason = _NON_CONNECTABLE_STATUSES.get(status, BrokerResolverError.REASON_INVALID)
            detail = account.get("last_error_message") or status
            raise BrokerResolverError(
                reason,
                f"Broker account {account_id!r} cannot be used for trading: "
                f"status={status!r} ({detail})",
                account_id=account_id,
            )

        # ── 4. Resolve canonical environment from account row ────────────────
        raw_env = account.get("environment") or ""
        try:
            environment = normalize_environment(raw_env)
        except ValueError:
            raise BrokerResolverError(
                BrokerResolverError.REASON_ENV_MISMATCH,
                f"Broker account {account_id!r} has unrecognized environment "
                f"value {raw_env!r} — mark account invalid and reconnect",
                account_id=account_id,
            )

        # ── 5. Select active credential version ──────────────────────────────
        # Primary source: broker_credentials_v2 (versioned, supports rotation).
        # Fallback: legacy broker_credentials if v2 has no row yet (supports
        #           databases that have not been migrated yet).
        active_version = account.get("active_credential_version")

        # Note: SELECT does NOT include `id` (autoincrement PK) — the column may be
        # absent on databases where broker_credentials_v2 was created by an older
        # migration that omitted it.  The `id` value is never used by the resolver.
        try:
            if active_version is not None:
                cred_row = conn.execute(
                    """
                    SELECT account_id, version, status, encrypted_blob,
                           key_fingerprint, last_validated_at, validation_error
                    FROM broker_credentials_v2
                    WHERE account_id = ? AND version = ?
                    """,
                    (account_id, active_version),
                ).fetchone()
            else:
                # No version pointer — pick highest active version from v2
                cred_row = conn.execute(
                    """
                    SELECT account_id, version, status, encrypted_blob,
                           key_fingerprint, last_validated_at, validation_error
                    FROM broker_credentials_v2
                    WHERE account_id = ?
                      AND (status = 'active' OR status IS NULL)
                    ORDER BY version DESC, rowid DESC
                    LIMIT 1
                    """,
                    (account_id,),
                ).fetchone()
        except Exception as _v2_err:
            # broker_credentials_v2 table may not exist at all or have schema issues.
            # Fall through to the legacy table fallback below.
            logger.warning(
                "broker_resolver_v2_unavailable account_id=%s error=%s — "
                "falling back to legacy broker_credentials",
                account_id, _v2_err,
            )
            cred_row = None

        # Legacy fallback: broker_credentials_v2 has no row or table doesn't exist.
        # This handles databases where migrations have not been run yet.
        if cred_row is None:
            try:
                legacy_row = conn.execute(
                    """
                    SELECT account_id, encrypted_blob, key_metadata, updated_at
                    FROM broker_credentials
                    WHERE account_id = ?
                    """,
                    (account_id,),
                ).fetchone()
            except Exception:
                legacy_row = None

            if legacy_row:
                # Synthesize a v2-compatible row dict so the rest of the function
                # can proceed without branching on legacy vs v2.
                cred_row = {
                    "account_id":       legacy_row["account_id"],
                    "version":          1,
                    "status":           "active",
                    "encrypted_blob":   legacy_row["encrypted_blob"],
                    "key_fingerprint":  None,
                    "last_validated_at": None,
                    "validation_error": None,
                }
                logger.warning(
                    "broker_resolver_legacy_fallback account_id=%s "
                    "using legacy broker_credentials table — run migrations to upgrade.",
                    account_id,
                )

        if cred_row is None:
            raise BrokerResolverError(
                BrokerResolverError.REASON_NO_CREDENTIALS,
                f"No active credentials found for broker account {account_id!r}",
                account_id=account_id,
            )

        cred = dict(cred_row)

        cred_status = (cred.get("status") or "active").lower()
        if cred_status not in ("active", "connected"):
            reason = _NON_CONNECTABLE_STATUSES.get(
                cred_status, BrokerResolverError.REASON_AUTH_FAILED
            )
            raise BrokerResolverError(
                reason,
                f"Credential version {cred.get('version')} for account "
                f"{account_id!r} has status={cred_status!r}",
                account_id=account_id,
            )

        # ── 6. Decrypt ───────────────────────────────────────────────────────
        decrypted = decrypt_credentials(cred["encrypted_blob"])
        if not decrypted:
            raise BrokerResolverError(
                BrokerResolverError.REASON_DECRYPT_FAILED,
                f"Failed to decrypt credentials for broker account {account_id!r}. "
                f"Key mismatch or data corruption — credential version "
                f"{cred.get('version')} may have been written by a different "
                f"key derivation. Re-submit credentials to resolve.",
                account_id=account_id,
            )

        api_key    = decrypted.get("api_key")    or decrypted.get("key")    or ""
        api_secret = decrypted.get("api_secret") or decrypted.get("secret") or ""

        if not api_key or not api_secret:
            raise BrokerResolverError(
                BrokerResolverError.REASON_NO_CREDENTIALS,
                f"Decrypted credentials for {account_id!r} are missing "
                f"api_key or api_secret — re-submit credentials",
                account_id=account_id,
            )

        # ── 7. Environment cross-check against blob ──────────────────────────
        # The account row is authoritative.  If the blob also contains an
        # environment field and it disagrees, fail loudly — this is exactly
        # the class of bug that caused this incident.
        blob_env_raw = decrypted.get("environment")
        if blob_env_raw:
            try:
                blob_env = normalize_environment(blob_env_raw)
                if blob_env != environment:
                    raise BrokerResolverError(
                        BrokerResolverError.REASON_ENV_MISMATCH,
                        (
                            f"Environment mismatch on broker account {account_id!r}: "
                            f"broker_accounts.environment={account['environment']!r} "
                            f"normalizes to {environment.value!r}, but the credential "
                            f"blob contains environment={blob_env_raw!r} which normalizes "
                            f"to {blob_env.value!r}. "
                            f"Reconnect the broker to resolve this inconsistency."
                        ),
                        account_id=account_id,
                    )
            except ValueError:
                # Blob has an unrecognized env string — account row wins, log warning
                logger.warning(
                    "broker_blob_env_unrecognized account_id=%s blob_env=%r "
                    "using account_row_env=%s",
                    account_id, blob_env_raw, environment.value,
                )

        # ── 8. Resolve base URL ───────────────────────────────────────────────
        broker_type = account["broker_id"].lower()

        # Explicit base_url in blob overrides table (supports MT bridge, IBKR, custom)
        explicit_base_url = decrypted.get("base_url")
        if explicit_base_url:
            base_url = explicit_base_url
        else:
            try:
                base_url = resolve_base_url(broker_type, environment)
            except ValueError as exc:
                raise BrokerResolverError(
                    BrokerResolverError.REASON_ENV_MISMATCH,
                    str(exc),
                    account_id=account_id,
                ) from exc

        # ── 9. Build fingerprint ──────────────────────────────────────────────
        fingerprint = (
            cred.get("key_fingerprint")
            or (f"...{api_key[-4:]}" if len(api_key) >= 4 else "****")
        )

        # Extra broker-specific fields (OANDA account_id, MT bridge token, etc.)
        _skip = {"api_key", "api_secret", "key", "secret", "environment", "base_url"}
        extra = {k: v for k, v in decrypted.items() if k not in _skip}

        return BrokerAuth(
            account_id=account_id,
            user_id=user_id,
            broker_type=broker_type,
            environment=environment,
            base_url=base_url,
            api_key=api_key,
            api_secret=api_secret,
            credential_version=cred.get("version") or 1,
            key_fingerprint=fingerprint,
            extra=extra,
        )
