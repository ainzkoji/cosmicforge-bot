"""When identity verification is required, and whether it permits an action.

* :func:`verification_required` decides per (country, action) from the
  existing ``kyc_requirements_config`` table (the same policy
  ``shared_lib.core.policy.kyc_policy`` reads). There is no blanket
  requirement: an action that is not configured, or configured with
  ``requires_kyc = 0``, or whose ``country_exceptions`` list the user's country,
  does not require verification.
* :func:`identity_permits` is the account-restriction decision for one user
  and one action (e.g. ``start_live_trading``). It is exposed for the
  live-trading path; the existing trading gates are not rewired here.

Failure is closed: an unreadable KYC source, a malformed validity setting or
an unconfigured provider refuse an action that requires verification.
"""
from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterator, List, Optional

from shared_lib.identity.providers import NOT_CONFIGURED_CODE, IdentityProvider, get_identity_provider
from shared_lib.identity.state import VerificationState, derive_state, effective_expiry, validity_days

S = VerificationState


@contextmanager
def _connection(db) -> Iterator[Any]:
    if db is None:
        from shared_lib.persistence.db import DB
        db = DB()
    if hasattr(db, "connect"):
        with db.connect() as conn:
            yield conn
    else:
        yield db


def _fetch_dict(conn, sql: str, params: tuple = ()) -> Optional[Dict[str, Any]]:
    cur = conn.execute(sql, params)
    row = cur.fetchone()
    if row is None:
        return None
    return {d[0]: row[i] for i, d in enumerate(cur.description)}


@dataclass(frozen=True)
class Requirement:
    action: str
    country: Optional[str]
    required: bool
    reason: str


@dataclass(frozen=True)
class IdentityDecision:
    permitted: bool
    code: str
    action: str
    state: Optional[str]
    required: bool
    reason: str
    expires_at: Optional[str] = None
    provider: Optional[str] = None

    def as_dict(self) -> Dict[str, Any]:
        return asdict(self)


def _exempt(country: Optional[str], exceptions: Any) -> bool:
    if not country or not exceptions:
        return False
    try:
        values = json.loads(exceptions) if isinstance(exceptions, str) else list(exceptions)
    except (TypeError, ValueError):
        return False  # unreadable exception list exempts nobody
    return str(country).upper() in {str(v).upper() for v in values}


def verification_required(db, country: Optional[str], action: str) -> Requirement:
    """Whether ``action`` requires a verified identity for a user in ``country``."""
    with _connection(db) as conn:
        config = _fetch_dict(conn, "SELECT * FROM kyc_requirements_config WHERE action_name = ?", (str(action),))
    if not config:
        return Requirement(action, country, False, "action has no verification requirement")
    if not int(config.get("requires_kyc") or 0):
        return Requirement(action, country, False, "verification is optional for this action")
    if _exempt(country, config.get("country_exceptions")):
        return Requirement(action, country, False, f"country {country} is exempt for this action")
    return Requirement(action, country, True, "verification is required for this action")


def user_country(conn, user_id: str) -> Optional[str]:
    """Country of residence from the KYC profile, else the one given at sign-up."""
    profile = _fetch_dict(conn, "SELECT country_of_residence FROM kyc_profiles WHERE user_id = ?", (str(user_id),))
    if profile and profile.get("country_of_residence"):
        return str(profile["country_of_residence"]).upper()
    user = _fetch_dict(conn, "SELECT country FROM users WHERE id = ?", (str(user_id),))
    return str(user["country"]).upper() if user and user.get("country") else None


def _required_for_any(conn, country: Optional[str]) -> bool:
    rows = conn.execute("SELECT action_name FROM kyc_requirements_config WHERE requires_kyc = 1").fetchall()
    return any(verification_required(conn, country, row[0]).required for row in rows)


def identity_state(db, user_id: str, *, now: Optional[datetime] = None,
                   days: Optional[int] = None) -> Dict[str, Any]:
    """``{"state", "kyc_status", "expires_at", "country", "required_for_any_action"}``."""
    now = now or datetime.now(timezone.utc)
    if days is None:
        days = validity_days()
    with _connection(db) as conn:
        case = _fetch_dict(conn, "SELECT * FROM kyc_cases WHERE user_id = ?", (str(user_id),))
        country = user_country(conn, user_id)
        required_any = _required_for_any(conn, country)
    expiry = effective_expiry(case, days)
    return {
        "state": derive_state(case, required=required_any, now=now, days=days),
        "kyc_status": (case or {}).get("status"),
        "expires_at": expiry.isoformat() if expiry else None,
        "country": country,
        "required_for_any_action": required_any,
    }


def identity_permits(user_id: str, action: str, *, db=None, now: Optional[datetime] = None,
                     provider: Optional[IdentityProvider] = None) -> IdentityDecision:
    """May ``user_id`` perform ``action`` as far as identity verification goes?

    Permitted when the action does not require verification for the user's
    country, or when the user's verification is VERIFIED (and not expired).
    """
    now = now or datetime.now(timezone.utc)
    provider = provider or get_identity_provider()
    try:
        days = validity_days()
        with _connection(db) as conn:
            if not conn.execute("SELECT 1 FROM users WHERE id = ?", (str(user_id),)).fetchone():
                return IdentityDecision(False, "UNKNOWN_USER", action, None, True, "unknown user",
                                        provider=provider.name)
            country = user_country(conn, user_id)
            requirement = verification_required(conn, country, action)
            info = identity_state(conn, user_id, now=now, days=days)
    except (sqlite3.Error, ValueError) as exc:
        return IdentityDecision(False, "IDENTITY_UNAVAILABLE", action, None, True,
                                f"identity verification state could not be determined: {exc}",
                                provider=provider.name)

    state = info["state"]
    if not requirement.required:
        return IdentityDecision(True, "NOT_REQUIRED", action, state.value, False, requirement.reason,
                                info["expires_at"], provider.name)
    if state == S.VERIFIED:
        return IdentityDecision(True, "VERIFIED", action, state.value, True, "identity verified",
                                info["expires_at"], provider.name)
    if not provider.is_configured():
        return IdentityDecision(False, NOT_CONFIGURED_CODE, action, state.value, True,
                                "identity verification is required but no identity provider is configured",
                                info["expires_at"], provider.name)
    reasons = {
        S.VERIFICATION_REQUIRED: "identity verification is required for this action",
        S.REGISTERED: "identity verification is required for this action",
        S.PENDING: "identity verification is awaiting review",
        S.MANUAL_REVIEW: "identity verification is under manual review",
        S.REJECTED: "identity verification was rejected",
        S.REVERIFICATION_REQUIRED: "identity verification has expired; verify again",
        S.EXPIRED: "identity verification has expired; verify again",
    }
    code = S.VERIFICATION_REQUIRED.value if state == S.REGISTERED else (
        S.REVERIFICATION_REQUIRED.value if state == S.EXPIRED else state.value)
    return IdentityDecision(False, code, action, state.value, True, reasons.get(state, "identity not verified"),
                            info["expires_at"], provider.name)


def restrictions(user_id: str, *, db=None, now: Optional[datetime] = None,
                 provider: Optional[IdentityProvider] = None) -> List[Dict[str, Any]]:
    """Every configured action and whether identity permits it (for status pages)."""
    with _connection(db) as conn:
        actions = [row[0] for row in conn.execute("SELECT action_name FROM kyc_requirements_config ORDER BY action_name")]
    return [identity_permits(user_id, a, db=db, now=now, provider=provider).as_dict() for a in actions]


__all__ = ["Requirement", "IdentityDecision", "verification_required", "user_country", "identity_state",
           "identity_permits", "restrictions"]
