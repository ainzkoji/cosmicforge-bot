"""Section H Step 8.3: identity verification state model on the existing KYC cases.

Covers the state mapping, every authorized transition, refusal of the rest,
expiry -> REVERIFICATION_REQUIRED, per (country, action) policy (no blanket
requirement), ``identity_permits`` outcomes, the provider adapters, and that
KYC audit / log output never contains document contents or personal data.
"""
from __future__ import annotations

import base64
import json
import logging
import sys
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "backends" / "shared"))
sys.path.insert(0, str(ROOT / "backends" / "user-backend"))

from shared_lib.core.policy.kyc_policy import KYCAction, evaluate_kyc_gate  # noqa: E402
from shared_lib.identity import providers, state as idstate  # noqa: E402
from shared_lib.identity.policy import identity_permits, identity_state, restrictions, verification_required  # noqa: E402
from shared_lib.identity.state import ALLOWED_TRANSITIONS, TransitionError, VerificationState as S  # noqa: E402
from shared_lib.persistence.db import DB  # noqa: E402
from shared_lib.persistence.migrations import migrate  # noqa: E402

# The KYC API harness (fixture + flow helpers) of the existing hardening suite.
from tests.test_kyc_hardening import (  # noqa: E402,F401
    JPG, PERSONAL_INFO, PNG, USER_A, _complete_and_submit, env,
)

NOW = datetime(2026, 10, 17, 12, 0, tzinfo=timezone.utc)
LIVE = KYCAction.START_LIVE_TRADING.value
MANUAL = providers.ManualReviewProvider()


@pytest.fixture
def db(tmp_path, monkeypatch):
    monkeypatch.delenv(idstate.VALIDITY_ENV, raising=False)
    monkeypatch.delenv(providers.PROVIDER_ENV, raising=False)
    path = tmp_path / "identity.db"
    migrate(str(path))
    database = DB(path=str(path))
    with database.connect() as conn:
        for uid, country in (("u1", "NG"), ("u2", "GB")):
            conn.execute("INSERT INTO users (id, email, hashed_password, status, country, created_at, updated_at) "
                         "VALUES (?, ?, 'x', 'active', ?, ?, ?)", (uid, f"{uid}@example.com", country,
                                                                    NOW.isoformat(), NOW.isoformat()))
    return database


def _case(db, uid="u1", status="in_progress", *, evidence=True, approved_at=None, expires_at=None):
    case_id = f"kyc_{uuid.uuid4().hex[:8]}"
    now = NOW.isoformat()
    with db.connect() as conn:
        conn.execute("INSERT INTO kyc_cases (id, user_id, status, approved_at, expires_at, created_at, updated_at) "
                     "VALUES (?, ?, ?, ?, ?, ?, ?)", (case_id, uid, status, approved_at, expires_at, now, now))
        if evidence:
            conn.execute("INSERT INTO kyc_profiles (id, user_id, kyc_case_id, country_of_residence, created_at, "
                         "updated_at) VALUES (?, ?, ?, 'NG', ?, ?)", (f"p_{case_id}", uid, case_id, now, now))
            conn.execute("INSERT INTO kyc_documents (id, user_id, kyc_case_id, doc_type, front_file_ref, status, "
                         "created_at, updated_at) VALUES (?, ?, ?, 'passport', 'ref/front', 'pending_review', ?, ?)",
                         (f"d_{case_id}", uid, case_id, now, now))
            conn.execute("INSERT INTO kyc_selfie_checks (id, user_id, kyc_case_id, status, selfie_file_ref, "
                         "created_at, updated_at) VALUES (?, ?, ?, 'pending_review', 'ref/selfie', ?, ?)",
                         (f"s_{case_id}", uid, case_id, now, now))
    return case_id


def _status(db, uid="u1"):
    with db.connect() as conn:
        return conn.execute("SELECT status FROM kyc_cases WHERE user_id = ?", (uid,)).fetchone()[0]


def _no_problems(conn, case):
    return []


# --------------------------------------------------------------------------- state mapping

@pytest.mark.parametrize("status,required,expected", [
    (None, False, S.REGISTERED),
    (None, True, S.VERIFICATION_REQUIRED),
    ("not_started", True, S.VERIFICATION_REQUIRED),
    ("in_progress", False, S.REGISTERED),
    ("in_progress", True, S.VERIFICATION_REQUIRED),
    ("needs_resubmission", False, S.VERIFICATION_REQUIRED),
    ("submitted", True, S.PENDING),
    ("under_review", True, S.MANUAL_REVIEW),
    ("approved", True, S.VERIFIED),
    ("rejected", True, S.REJECTED),
    ("expired", True, S.REVERIFICATION_REQUIRED),
    ("expired", False, S.EXPIRED),
    ("something_new", True, S.VERIFICATION_REQUIRED),
])
def test_kyc_status_maps_to_verification_state(status, required, expected):
    case = None if status is None else {"status": status, "approved_at": NOW.isoformat()}
    assert idstate.derive_state(case, required=required, now=NOW) == expected


def test_no_expiry_unless_a_validity_period_is_configured(monkeypatch):
    old = {"status": "approved", "approved_at": (NOW - timedelta(days=5000)).isoformat()}
    monkeypatch.delenv(idstate.VALIDITY_ENV, raising=False)
    assert idstate.validity_days() is None
    assert idstate.derive_state(old, required=True, now=NOW, days=idstate.validity_days()) == S.VERIFIED

    monkeypatch.setenv(idstate.VALIDITY_ENV, "365")
    days = idstate.validity_days()
    assert days == 365
    assert idstate.derive_state(old, required=True, now=NOW, days=days) == S.REVERIFICATION_REQUIRED
    assert idstate.derive_state(old, required=False, now=NOW, days=days) == S.EXPIRED
    recent = {"status": "approved", "approved_at": (NOW - timedelta(days=10)).isoformat()}
    assert idstate.derive_state(recent, required=True, now=NOW, days=days) == S.VERIFIED
    # An explicit expires_at wins over the period.
    explicit = {**recent, "expires_at": (NOW - timedelta(seconds=1)).isoformat()}
    assert idstate.derive_state(explicit, required=True, now=NOW, days=days) == S.REVERIFICATION_REQUIRED

    for bad in ("abc", "0", "-3"):
        monkeypatch.setenv(idstate.VALIDITY_ENV, bad)
        with pytest.raises(ValueError):
            idstate.validity_days()


# --------------------------------------------------------------------------- transitions

def _setup_for(db, source: S):
    status = {S.VERIFICATION_REQUIRED: "in_progress", S.PENDING: "submitted", S.MANUAL_REVIEW: "under_review",
              S.VERIFIED: "approved", S.REJECTED: "rejected", S.REVERIFICATION_REQUIRED: "expired"}[source]
    approved = (NOW - timedelta(days=400)).isoformat() if source == S.VERIFIED else None
    _case(db, status=status, approved_at=approved)


@pytest.mark.parametrize("pair,actor", [(pair, actor) for pair, actors in ALLOWED_TRANSITIONS.items()
                                         for actor in sorted(actors)])
def test_every_authorized_transition_is_applied_and_audited(db, pair, actor):
    source, target = pair
    _setup_for(db, source)
    with db.connect() as conn:
        result = idstate.apply_transition(conn, "u1", target, actor_type=actor, actor_id=f"{actor}-1",
                                          reason="reason given", reason_codes=["TEST_CODE"],
                                          evidence_check=_no_problems, now=NOW, days=365)
    assert (result["from"], result["to"]) == (source.value, target.value)
    with db.connect() as conn:
        case = dict(conn.execute("SELECT * FROM kyc_cases WHERE user_id = 'u1'").fetchone())
        audit = [dict(r) for r in conn.execute(
            "SELECT * FROM kyc_audit_log WHERE event_type = 'identity_state_transition'").fetchall()]
    later = NOW + timedelta(seconds=1)
    assert idstate.canonical(idstate.derive_state(case, required=True, now=later, days=365)) == target
    assert len(audit) == 1 and audit[0]["actor_type"] == actor
    data = json.loads(audit[0]["event_data"])
    assert (data["from"], data["to"], data["reason_codes"]) == (source.value, target.value, ["TEST_CODE"])
    assert "reason given" not in audit[0]["event_data"]  # free text is not copied into the audit log
    if target == S.VERIFIED:
        assert case["expires_at"] == (NOW + timedelta(days=365)).isoformat()


ALL_STATES = [S.VERIFICATION_REQUIRED, S.PENDING, S.MANUAL_REVIEW, S.VERIFIED, S.REJECTED, S.REVERIFICATION_REQUIRED]


@pytest.mark.parametrize("source,target,actor", [
    (s, t, a) for s in ALL_STATES for t in ALL_STATES + [S.REGISTERED, S.EXPIRED] for a in ("user", "reviewer", "system")
    if a not in ALLOWED_TRANSITIONS.get((s, idstate.canonical(t)), frozenset()) and s != idstate.canonical(t)
])
def test_unauthorized_transitions_are_refused(db, source, target, actor):
    _setup_for(db, source)
    before = _status(db)
    with pytest.raises(TransitionError) as err:
        with db.connect() as conn:
            # days=0: no expiry, so the VERIFIED fixture really is VERIFIED here.
            idstate.apply_transition(conn, "u1", target, actor_type=actor, reason="r",
                                     evidence_check=_no_problems, now=NOW, days=0)
    assert err.value.code == "UNAUTHORIZED_TRANSITION"
    assert _status(db) == before


def test_users_cannot_verify_themselves_and_approval_needs_evidence(db):
    _case(db, status="submitted", evidence=False)
    with db.connect() as conn:
        for actor in ("user", "system"):
            with pytest.raises(TransitionError) as err:
                idstate.apply_transition(conn, "u1", S.VERIFIED, actor_type=actor, evidence_check=_no_problems)
            assert err.value.code == "UNAUTHORIZED_TRANSITION"
        with pytest.raises(TransitionError) as err:
            idstate.apply_transition(conn, "u1", S.VERIFIED, actor_type="reviewer")
        assert err.value.code == "EVIDENCE_CHECK_REQUIRED"
        with pytest.raises(TransitionError) as err:
            idstate.apply_transition(conn, "u1", S.VERIFIED, actor_type="reviewer",
                                     evidence_check=idstate.minimum_evidence_problems)
        assert err.value.code == "EVIDENCE_INCOMPLETE"
        with pytest.raises(TransitionError) as err:
            idstate.apply_transition(conn, "u1", S.REJECTED, actor_type="reviewer", reason="  ")
        assert err.value.code == "REASON_REQUIRED"
        with pytest.raises(TransitionError) as err:
            idstate.apply_transition(conn, "u1", S.VERIFIED, actor_type="impostor", evidence_check=_no_problems)
        assert err.value.code == "UNKNOWN_ACTOR"
    assert _status(db) == "submitted"


def test_submission_requires_evidence_records(db):
    _case(db, status="in_progress", evidence=False)
    with pytest.raises(TransitionError) as err:
        with db.connect() as conn:
            idstate.apply_transition(conn, "u1", S.PENDING, actor_type="user", now=NOW)
    assert err.value.code == "EVIDENCE_INCOMPLETE"
    with pytest.raises(TransitionError) as err:
        with db.connect() as conn:
            idstate.apply_transition(conn, "u2", S.PENDING, actor_type="user", now=NOW)
    assert err.value.code == "NO_CASE"


def test_system_expiry_only_after_the_validity_period(db, monkeypatch):
    _case(db, status="approved", approved_at=(NOW - timedelta(days=30)).isoformat())
    with pytest.raises(TransitionError) as err:
        with db.connect() as conn:
            idstate.apply_transition(conn, "u1", S.REVERIFICATION_REQUIRED, actor_type="system", now=NOW, days=365)
    assert err.value.code == "NOT_EXPIRED"
    with pytest.raises(TransitionError) as err:  # no validity configured: never expires on its own
        with db.connect() as conn:
            idstate.apply_transition(conn, "u1", S.REVERIFICATION_REQUIRED, actor_type="system", now=NOW, days=0)
    assert err.value.code == "NOT_EXPIRED"


def test_expiry_sweep_moves_verified_to_reverification_required(db, monkeypatch, tmp_path):
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{(tmp_path / 'identity.db').as_posix()}")
    _case(db, "u1", status="approved", approved_at=(NOW - timedelta(days=400)).isoformat())
    _case(db, "u2", status="approved", approved_at=(NOW - timedelta(days=10)).isoformat())
    assert evaluate_kyc_gate("u1", LIVE).allowed is True  # the existing gate does not read expiry...

    assert idstate.expire_due_verifications(db, now=NOW, days=365) == ["u1"]
    assert idstate.expire_due_verifications(db, now=NOW, days=365) == []  # idempotent
    assert (_status(db, "u1"), _status(db, "u2")) == ("expired", "approved")
    assert identity_state(db, "u1", now=NOW, days=365)["state"] == S.REVERIFICATION_REQUIRED
    assert evaluate_kyc_gate("u1", LIVE).allowed is False  # ...but refuses once the expiry is persisted
    assert evaluate_kyc_gate("u2", LIVE).allowed is True

    # The user starts over: back to VERIFICATION_REQUIRED, then PENDING.
    with db.connect() as conn:
        idstate.apply_transition(conn, "u1", S.VERIFICATION_REQUIRED, actor_type="user", now=NOW, days=365)
        idstate.apply_transition(conn, "u1", S.PENDING, actor_type="user", now=NOW, days=365)
    assert _status(db, "u1") == "submitted"


# --------------------------------------------------------------------------- policy

def test_requirement_is_per_action_and_country_not_blanket(db):
    assert verification_required(db, "NG", LIVE).required is True
    assert verification_required(db, "NG", KYCAction.DEVELOPER_API_ACCESS.value).required is False
    assert verification_required(db, "NG", "view_dashboard").required is False  # not configured
    assert verification_required(db, "NG", "connect_demo_broker").required is False
    with db.connect() as conn:
        conn.execute("UPDATE kyc_requirements_config SET country_exceptions = '[\"GB\"]' WHERE action_name = ?",
                     (LIVE,))
    assert verification_required(db, "GB", LIVE).required is False
    assert verification_required(db, "gb", LIVE).required is False
    assert verification_required(db, "NG", LIVE).required is True
    with db.connect() as conn:
        conn.execute("UPDATE kyc_requirements_config SET country_exceptions = 'not json' WHERE action_name = ?",
                     (LIVE,))
    assert verification_required(db, "GB", LIVE).required is True  # unreadable list exempts nobody


def test_identity_permits_outcomes(db, monkeypatch):
    # Not required -> permitted without any verification.
    d = identity_permits("u1", KYCAction.DEVELOPER_API_ACCESS.value, db=db, now=NOW, provider=MANUAL)
    assert (d.permitted, d.code, d.state) == (True, "NOT_REQUIRED", "VERIFICATION_REQUIRED")
    # Required and not verified.
    d = identity_permits("u1", LIVE, db=db, now=NOW, provider=MANUAL)
    assert (d.permitted, d.code) == (False, "VERIFICATION_REQUIRED")
    case_id = _case(db, status="submitted")
    assert identity_permits("u1", LIVE, db=db, now=NOW, provider=MANUAL).code == "PENDING"
    with db.connect() as conn:
        conn.execute("UPDATE kyc_cases SET status = 'under_review' WHERE id = ?", (case_id,))
    assert identity_permits("u1", LIVE, db=db, now=NOW, provider=MANUAL).code == "MANUAL_REVIEW"
    with db.connect() as conn:
        conn.execute("UPDATE kyc_cases SET status = 'rejected' WHERE id = ?", (case_id,))
    assert identity_permits("u1", LIVE, db=db, now=NOW, provider=MANUAL).code == "REJECTED"
    with db.connect() as conn:
        conn.execute("UPDATE kyc_cases SET status = 'approved', approved_at = ? WHERE id = ?",
                     ((NOW - timedelta(days=400)).isoformat(), case_id))
    d = identity_permits("u1", LIVE, db=db, now=NOW, provider=MANUAL)
    assert (d.permitted, d.code) == (True, "VERIFIED")
    # Expired by policy.
    monkeypatch.setenv(idstate.VALIDITY_ENV, "365")
    d = identity_permits("u1", LIVE, db=db, now=NOW, provider=MANUAL)
    assert (d.permitted, d.code, d.state) == (False, "REVERIFICATION_REQUIRED", "REVERIFICATION_REQUIRED")
    # A malformed validity setting fails closed.
    monkeypatch.setenv(idstate.VALIDITY_ENV, "forever")
    assert identity_permits("u1", LIVE, db=db, now=NOW, provider=MANUAL).code == "IDENTITY_UNAVAILABLE"
    monkeypatch.delenv(idstate.VALIDITY_ENV)
    # Unknown user.
    assert identity_permits("ghost", LIVE, db=db, now=NOW, provider=MANUAL).code == "UNKNOWN_USER"
    # Status-page view of every configured action.
    rows = {r["action"]: r for r in restrictions("u1", db=db, now=NOW, provider=MANUAL)}
    assert rows[LIVE]["permitted"] is True and rows[KYCAction.DEVELOPER_API_ACCESS.value]["code"] == "NOT_REQUIRED"


def test_unreadable_kyc_source_refuses(tmp_path):
    broken = DB(path=str(tmp_path / "empty.db"))  # base tables only: no KYC schema
    with broken.connect() as conn:
        conn.execute("INSERT INTO users (id, email, hashed_password, created_at, updated_at) "
                     "VALUES ('u1', 'u1@example.com', 'x', ?, ?)", (NOW.isoformat(), NOW.isoformat()))
    d = identity_permits("u1", LIVE, db=broken, now=NOW, provider=MANUAL)
    assert (d.permitted, d.code) == (False, "IDENTITY_UNAVAILABLE")


# --------------------------------------------------------------------------- providers

def test_not_configured_provider_fails_closed(db):
    nc = providers.get_identity_provider({"IDENTITY_PROVIDER": "none"})
    assert isinstance(nc, providers.NotConfiguredProvider) and nc.is_configured() is False
    with pytest.raises(providers.IdentityProviderNotConfigured) as err:
        nc.begin_verification("u1")
    assert err.value.code == "IDENTITY_PROVIDER_NOT_CONFIGURED"
    with pytest.raises(providers.IdentityProviderNotConfigured):
        nc.verification_state(db, "u1")
    d = identity_permits("u1", LIVE, db=db, now=NOW, provider=nc)
    assert (d.permitted, d.code) == (False, "IDENTITY_PROVIDER_NOT_CONFIGURED")
    # Not-required actions are unaffected; a verified user keeps their verification.
    assert identity_permits("u1", KYCAction.DEVELOPER_API_ACCESS.value, db=db, now=NOW, provider=nc).permitted
    _case(db, status="approved", approved_at=NOW.isoformat())
    assert identity_permits("u1", LIVE, db=db, now=NOW, provider=nc).permitted is True
    # An unknown provider name never falls back to something else.
    assert isinstance(providers.get_identity_provider({"IDENTITY_PROVIDER": "acme-kyc"}),
                      providers.NotConfiguredProvider)


def test_manual_review_provider(db):
    manual = providers.get_identity_provider({})
    assert isinstance(manual, providers.ManualReviewProvider) and isinstance(manual, providers.IdentityProvider)
    assert manual.begin_verification("u1")["start"] == "/kyc/start"

    def missing_secrets():
        raise RuntimeError("KYC_ENCRYPTION_KEY missing")

    unconfigured = providers.ManualReviewProvider(config_check=missing_secrets)
    assert unconfigured.is_configured() is False
    with pytest.raises(providers.IdentityProviderNotConfigured):
        unconfigured.begin_verification("u1")
    assert identity_permits("u1", LIVE, db=db, now=NOW, provider=unconfigured).code == \
        "IDENTITY_PROVIDER_NOT_CONFIGURED"


# --------------------------------------------------------------------------- no document contents in logs

def test_kyc_audit_and_logs_never_contain_document_contents(env, caplog):
    caplog.set_level(logging.DEBUG)
    flow = _complete_and_submit(env.client)
    response = env.client.post(f"/api/admin/compliance/kyc/{flow.case_id}/approve")
    assert response.status_code == 200, response.text
    with env.db.connect() as conn:
        idstate.apply_transition(conn, USER_A, S.REVERIFICATION_REQUIRED, actor_type="reviewer",
                                 actor_id="admin-test", reason="document changed", days=0)
        audit_rows = [dict(r) for r in conn.execute("SELECT * FROM kyc_audit_log").fetchall()]
        stored_profile = dict(conn.execute("SELECT * FROM kyc_profiles").fetchone())
    assert len(audit_rows) >= 8

    secrets = [
        PERSONAL_INFO["full_legal_name"], PERSONAL_INFO["date_of_birth"], PERSONAL_INFO["address_line1"],
        PERSONAL_INFO["address_postal_code"], PERSONAL_INFO["address_city"],
        base64.b64encode(PNG).decode(), base64.b64encode(JPG).decode(),
        PNG.decode("latin-1"), JPG.decode("latin-1"),
    ]
    audit_text = json.dumps(audit_rows, default=str)
    log_text = "\n".join(record.getMessage() for record in caplog.records)
    for secret in secrets:
        assert secret not in audit_text, secret
        assert secret not in log_text, secret
    # Personal data at rest is encrypted, not plain text.
    assert PERSONAL_INFO["full_legal_name"] not in json.dumps(stored_profile)
