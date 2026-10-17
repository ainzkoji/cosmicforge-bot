"""Identity provider adapters.

The platform talks to identity verification through :class:`IdentityProvider`.
Two adapters exist:

* :class:`ManualReviewProvider` -- the current flow: the user uploads
  personal information, an identity document and a selfie through the
  user-backend ``/kyc`` API, and an administrator reviews the case.
* :class:`NotConfiguredProvider` -- fails closed: every operation raises
  ``IdentityProviderNotConfigured`` (code ``IDENTITY_PROVIDER_NOT_CONFIGURED``)
  and ``identity_permits`` refuses actions that require verification.

``IDENTITY_PROVIDER`` selects the adapter: unset / ``manual`` -> manual
review (today's behaviour); ``none`` / ``disabled`` -> not configured; any
other name -> not configured (no adapter exists for it, so it fails closed
instead of silently falling back to something else).
"""
from __future__ import annotations

import os
from datetime import datetime
from typing import Any, Callable, Dict, Mapping, Optional, Protocol, runtime_checkable

from shared_lib.identity.state import VerificationState

PROVIDER_ENV = "IDENTITY_PROVIDER"
NOT_CONFIGURED_CODE = "IDENTITY_PROVIDER_NOT_CONFIGURED"


class IdentityProviderNotConfigured(RuntimeError):
    code = NOT_CONFIGURED_CODE

    def __init__(self, message: str = "identity verification is not configured"):
        super().__init__(message)


@runtime_checkable
class IdentityProvider(Protocol):
    name: str

    def is_configured(self) -> bool:
        """Whether a user can complete verification through this provider now."""

    def begin_verification(self, user_id: str) -> Dict[str, Any]:
        """Start (or resume) verification; returns how the user proceeds."""

    def verification_state(self, db: Any, user_id: str, *, now: Optional[datetime] = None) -> VerificationState:
        """The user's current verification state as this provider sees it."""


class ManualReviewProvider:
    """Self-service evidence upload + manual administrator review (the existing KYC flow)."""

    name = "manual_review"

    def __init__(self, config_check: Optional[Callable[[], None]] = None):
        # ``config_check`` raises when the KYC storage / encryption secrets are
        # missing (the user-backend passes its own check); None = assume ready.
        self._config_check = config_check

    def is_configured(self) -> bool:
        if self._config_check is None:
            return True
        try:
            self._config_check()
        except Exception:
            return False
        return True

    def begin_verification(self, user_id: str) -> Dict[str, Any]:
        if not self.is_configured():
            raise IdentityProviderNotConfigured("manual review is not configured (KYC storage/encryption secrets)")
        return {"provider": self.name, "flow": "upload_then_manual_review", "start": "/kyc/start",
                "steps": ["personal_info", "id_document", "face_verification", "submit"]}

    def verification_state(self, db: Any, user_id: str, *, now: Optional[datetime] = None) -> VerificationState:
        from shared_lib.identity.policy import identity_state
        return identity_state(db, user_id, now=now)["state"]


class NotConfiguredProvider:
    """No identity provider: fail closed."""

    name = "not_configured"

    def __init__(self, reason: str = "no identity provider is configured"):
        self.reason = reason

    def is_configured(self) -> bool:
        return False

    def begin_verification(self, user_id: str) -> Dict[str, Any]:
        raise IdentityProviderNotConfigured(self.reason)

    def verification_state(self, db: Any, user_id: str, *, now: Optional[datetime] = None) -> VerificationState:
        raise IdentityProviderNotConfigured(self.reason)


def get_identity_provider(env: Optional[Mapping[str, str]] = None,
                          config_check: Optional[Callable[[], None]] = None) -> IdentityProvider:
    name = str((env if env is not None else os.environ).get(PROVIDER_ENV, "") or "").strip().lower()
    if name in ("", "manual", "manual_review"):
        return ManualReviewProvider(config_check)
    if name in ("none", "disabled", "off"):
        return NotConfiguredProvider(f"{PROVIDER_ENV}={name}: identity verification is disabled")
    return NotConfiguredProvider(f"{PROVIDER_ENV}={name}: no adapter exists for this provider")


__all__ = ["IdentityProvider", "ManualReviewProvider", "NotConfiguredProvider", "IdentityProviderNotConfigured",
           "NOT_CONFIGURED_CODE", "PROVIDER_ENV", "get_identity_provider"]
