"""Identity verification (Section H Step 8.3), built on the existing KYC cases.

* :mod:`shared_lib.identity.state`     -- verification states, expiry, authorized transitions
* :mod:`shared_lib.identity.policy`    -- per (country, action) requirement; ``identity_permits``
* :mod:`shared_lib.identity.providers` -- ``IdentityProvider`` protocol, manual review, not-configured

Verification is never required across the board: only the actions listed in
``kyc_requirements_config`` with ``requires_kyc = 1`` (e.g. live trading)
require it.
"""
from shared_lib.identity.policy import identity_permits, verification_required  # noqa: F401
from shared_lib.identity.state import VerificationState  # noqa: F401
