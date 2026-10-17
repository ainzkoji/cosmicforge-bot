"""Section H Step 8.2: versioned legal documents, binding consent, withdrawal,
counsel-review recording, country eligibility, and registration capture.

No document here is real legal text: test bodies are placeholders, and the
only seeded document is the clearly labelled DRAFT risk disclosure.
"""
from __future__ import annotations

import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from passlib.context import CryptContext

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "backends" / "shared"))
sys.path.insert(0, str(ROOT / "backends" / "user-backend"))

from app.api import admin_legal, auth, legal as legal_api  # noqa: E402
from app.api.auth import get_current_active_user  # noqa: E402
from app.core import security  # noqa: E402
from app.core.deps import require_admin  # noqa: E402
from shared_lib.legal import documents as legal  # noqa: E402
from shared_lib.legal.seed import DRAFT_NOTICE  # noqa: E402
from shared_lib.persistence.db import DB  # noqa: E402
from shared_lib.persistence.migrations import migrate  # noqa: E402

T0 = datetime(2026, 10, 1, 12, 0, tzinfo=timezone.utc)
ADMIN = {"id": "admin-legal-1", "email": "ops@example.test"}
PLACEHOLDER = "Test placeholder text for an automated test. Not a legal document. Version {n}."


def _db(tmp_path: Path) -> DB:
    path = tmp_path / "legal.db"
    migrate(str(path))
    db = DB(path=str(path))
    with db.connect() as conn:
        for uid in ("user-1", "user-2"):
            conn.execute("INSERT INTO users (id, email, hashed_password, status, created_at, updated_at) "
                         "VALUES (?, ?, 'x', 'active', ?, ?)", (uid, f"{uid}@example.test", T0.isoformat(),
                                                                 T0.isoformat()))
    return db


def _publish(db, category="TERMS_OF_SERVICE", n=1, *, material=True, at=None, locale="en"):
    return legal.publish(db, category=category, title=f"Test {category} v{n}", body=PLACEHOLDER.format(n=n),
                         locale=locale, material_change=material, published_by="admin-legal-1",
                         now=at or (T0 + timedelta(minutes=n)))


def _item(doc):
    return {"doc_id": doc["doc_id"], "content_sha256": doc["content_sha256"]}


# --------------------------------------------------------------------------- seed + drafts

def test_only_a_labelled_draft_risk_disclosure_is_seeded(tmp_path):
    db = _db(tmp_path)
    docs = legal.list_versions(db)
    assert [(d["category"], d["version"], d["review_status"]) for d in docs] == [
        ("TRADING_RISK_DISCLOSURE", 1, "DRAFT_PENDING_COUNSEL")]
    seeded = docs[0]
    assert "Draft" in seeded["title"] and seeded["body"].startswith(DRAFT_NOTICE)
    assert seeded["reviewer_name"] is None
    assert legal.public_view(seeded)["is_draft"] is True
    # No terms or privacy text was invented.
    assert legal.current_document(db, "TERMS_OF_SERVICE") is None
    # Running the migration again adds nothing.
    migrate(str(tmp_path / "legal.db"))
    assert len(legal.list_versions(db)) == 1


def test_publish_is_always_a_draft_and_versions_supersede(tmp_path):
    db = _db(tmp_path)
    v1 = _publish(db, n=1)
    v2 = _publish(db, n=2)
    assert (v1["version"], v2["version"]) == (1, 2)
    assert v1["review_status"] == v2["review_status"] == "DRAFT_PENDING_COUNSEL"
    assert v2["supersedes_doc_id"] == v1["doc_id"]
    assert legal.current_document(db, "TERMS_OF_SERVICE")["doc_id"] == v2["doc_id"]
    assert v1["content_sha256"] == legal.content_sha256(v1["title"], v1["body"])
    with pytest.raises(legal.LegalError) as err:
        legal.publish(db, category="TERMS_OF_SERVICE", title=v2["title"], body=v2["body"])
    assert err.value.code == "DUPLICATE_CONTENT"
    with pytest.raises(legal.LegalError) as err:
        legal.publish(db, category="NOT_A_CATEGORY", title="x", body="y" * 30)
    assert err.value.code == "UNKNOWN_CATEGORY"


def test_published_content_is_immutable_and_undeletable(tmp_path):
    db = _db(tmp_path)
    doc = _publish(db)
    for sql in ("UPDATE legal_documents SET body = 'changed' WHERE doc_id = ?",
                "UPDATE legal_documents SET content_sha256 = 'x' WHERE doc_id = ?",
                "UPDATE legal_documents SET material_change = 0 WHERE doc_id = ?",
                "DELETE FROM legal_documents WHERE doc_id = ?"):
        with pytest.raises(sqlite3.DatabaseError):
            with db.connect() as conn:
                conn.execute(sql, (doc["doc_id"],))
    assert legal.get_document(db, doc["doc_id"])["body"] == doc["body"]


def test_counsel_approval_needs_a_recorded_reviewer_and_is_final(tmp_path):
    db = _db(tmp_path)
    doc = _publish(db)
    # The database refuses an approval without reviewer name and reference.
    with pytest.raises(sqlite3.IntegrityError):
        with db.connect() as conn:
            conn.execute("UPDATE legal_documents SET review_status = 'COUNSEL_APPROVED' WHERE doc_id = ?",
                         (doc["doc_id"],))
    with pytest.raises(legal.LegalError) as err:
        legal.record_counsel_review(db, doc["doc_id"], reviewer_name="", reviewer_reference="REF-1",
                                    recorded_by="admin")
    assert err.value.code == "REVIEWER_REQUIRED"
    approved = legal.record_counsel_review(db, doc["doc_id"], reviewer_name="Test Reviewer",
                                           reviewer_reference="TEST-REF-001", recorded_by="admin-legal-1")
    assert approved["review_status"] == "COUNSEL_APPROVED" and approved["reviewer_reference"] == "TEST-REF-001"
    assert legal.public_view(approved)["is_draft"] is False
    assert "reviewer_name" not in legal.public_view(approved)
    with pytest.raises(legal.LegalError) as err:
        legal.record_counsel_review(db, doc["doc_id"], reviewer_name="X Y", reviewer_reference="R2",
                                    recorded_by="admin")
    assert err.value.code == "ALREADY_APPROVED"
    with pytest.raises(sqlite3.DatabaseError):
        with db.connect() as conn:
            conn.execute("UPDATE legal_documents SET review_status = 'DRAFT_PENDING_COUNSEL' WHERE doc_id = ?",
                         (doc["doc_id"],))


# --------------------------------------------------------------------------- acceptance

def test_acceptance_binds_the_exact_content_hash(tmp_path):
    db = _db(tmp_path)
    doc = _publish(db)
    with pytest.raises(legal.LegalError) as err:
        legal.accept(db, "user-1", [{"doc_id": doc["doc_id"], "content_sha256": "0" * 64}])
    assert err.value.code == "CONTENT_HASH_MISMATCH"
    with pytest.raises(legal.LegalError) as err:
        legal.accept(db, "user-1", [{"doc_id": "legal_missing", "content_sha256": "0" * 64}])
    assert err.value.code == "UNKNOWN_DOCUMENT"

    rows = legal.accept(db, "user-1", [_item(doc)], ip="203.0.113.9", user_agent="pytest-agent", now=T0)
    assert rows[0]["content_sha256"] == doc["content_sha256"]
    assert rows[0]["review_status_at_acceptance"] == "DRAFT_PENDING_COUNSEL"
    # Evidence is stored as keyed hashes only.
    assert rows[0]["ip_hash"] and "203.0.113.9" not in rows[0]["ip_hash"]
    assert rows[0]["user_agent_hash"] != "pytest-agent"
    # Accepting again is a no-op.
    again = legal.accept(db, "user-1", [_item(doc)])
    assert again[0]["acceptance_id"] == rows[0]["acceptance_id"]
    assert len(legal.user_acceptances(db, "user-1")) == 1
    assert legal.has_current_acceptance(db, "user-1", "TERMS_OF_SERVICE") is True
    assert legal.has_current_acceptance(db, "user-2", "TERMS_OF_SERVICE") is False
    # Acceptance evidence is immutable.
    with pytest.raises(sqlite3.DatabaseError):
        with db.connect() as conn:
            conn.execute("UPDATE legal_acceptances SET content_sha256 = 'x'")
    with pytest.raises(sqlite3.DatabaseError):
        with db.connect() as conn:
            conn.execute("DELETE FROM legal_acceptances")


def test_superseded_version_cannot_be_accepted_and_all_or_nothing(tmp_path):
    db = _db(tmp_path)
    v1 = _publish(db, n=1)
    v2 = _publish(db, n=2)
    privacy = _publish(db, "PRIVACY_POLICY", n=1)
    with pytest.raises(legal.LegalError) as err:
        legal.accept(db, "user-1", [_item(privacy), _item(v1)])
    assert err.value.code == "SUPERSEDED" and err.value.details["current_doc_id"] == v2["doc_id"]
    assert legal.user_acceptances(db, "user-1") == []  # nothing partially recorded


def test_material_amendment_requires_reconsent_but_minor_one_does_not(tmp_path):
    db = _db(tmp_path)
    v1 = _publish(db, n=1)
    legal.accept(db, "user-1", [_item(v1)])
    _publish(db, n=2, material=False)  # e.g. typo fix
    assert legal.acceptance_status(db, "user-1", "TERMS_OF_SERVICE")["state"] == "CURRENT"
    assert legal.has_current_acceptance(db, "user-1", "TERMS_OF_SERVICE") is True

    v3 = _publish(db, n=3, material=True)
    status = legal.required_reconsent(db, "user-1")
    terms = [p for p in status["pending"] if p["document"]["category"] == "TERMS_OF_SERVICE"]
    assert terms and terms[0]["reason"] == "MATERIAL_CHANGE" and terms[0]["accepted_version"] == 1
    assert terms[0]["document"]["doc_id"] == v3["doc_id"]
    assert legal.has_current_acceptance(db, "user-1", "TERMS_OF_SERVICE") is False
    # The seeded draft risk disclosure was never accepted; unpublished ones are reported, not required.
    risk = [p for p in status["pending"] if p["document"]["category"] == "TRADING_RISK_DISCLOSURE"]
    assert risk and risk[0]["reason"] == "NOT_ACCEPTED"
    assert "PRIVACY_POLICY" in status["unpublished"] and status["reconsent_required"] is True

    legal.accept(db, "user-1", [_item(v3)])
    assert legal.has_current_acceptance(db, "user-1", "TERMS_OF_SERVICE") is True


def test_material_change_in_another_locale_is_detected(tmp_path):
    db = _db(tmp_path)
    en = _publish(db, n=1, locale="en", at=T0)
    legal.accept(db, "user-1", [_item(en)])
    fr = _publish(db, n=2, locale="fr", at=T0 + timedelta(days=1))
    assert legal.acceptance_status(db, "user-1", "TERMS_OF_SERVICE", "fr")["state"] == "MATERIAL_CHANGE"
    assert legal.current_document(db, "TERMS_OF_SERVICE", "de")["doc_id"] == en["doc_id"]  # falls back to en
    assert fr["locale"] == "fr"


def test_withdrawing_automated_execution_consent_turns_the_gate_off(tmp_path):
    db = _db(tmp_path)
    consent = _publish(db, "AUTOMATED_EXECUTION_CONSENT")
    legal.accept(db, "user-1", [_item(consent)])
    assert legal.has_current_acceptance(db, "user-1", "AUTOMATED_EXECUTION_CONSENT") is True

    result = legal.withdraw(db, "user-1", "AUTOMATED_EXECUTION_CONSENT", reason="stop trading for me")
    assert result["withdrawn"] == 1 and result["has_current_acceptance"] is False
    assert [e["code"] for e in result["effects"]] == ["AUTOMATED_EXECUTION_NOT_PERMITTED"]
    assert legal.has_current_acceptance(db, "user-1", "AUTOMATED_EXECUTION_CONSENT") is False
    assert legal.acceptance_status(db, "user-1", "AUTOMATED_EXECUTION_CONSENT")["state"] == "WITHDRAWN"
    # Withdrawn categories are not re-prompted unless a purpose requires them.
    assert all(p["document"]["category"] != "AUTOMATED_EXECUTION_CONSENT"
               for p in legal.required_reconsent(db, "user-1")["pending"])
    assert any(p["document"]["category"] == "AUTOMATED_EXECUTION_CONSENT" for p in
               legal.required_reconsent(db, "user-1", purposes=["automated_execution"])["pending"])
    with pytest.raises(legal.LegalError) as err:
        legal.withdraw(db, "user-1", "AUTOMATED_EXECUTION_CONSENT")
    assert err.value.code == "NOT_ACCEPTED"
    # The withdrawal cannot be undone by editing the row.
    with pytest.raises(sqlite3.DatabaseError):
        with db.connect() as conn:
            conn.execute("UPDATE legal_acceptances SET withdrawn_at = NULL")
    # Consent can be given again (new acceptance row).
    legal.accept(db, "user-1", [_item(consent)])
    assert legal.has_current_acceptance(db, "user-1", "AUTOMATED_EXECUTION_CONSENT") is True


def test_audit_log_records_every_change_and_is_append_only(tmp_path):
    db = _db(tmp_path)
    doc = _publish(db)
    legal.accept(db, "user-1", [_item(doc)])
    legal.withdraw(db, "user-1", "TERMS_OF_SERVICE")
    legal.record_counsel_review(db, doc["doc_id"], reviewer_name="Test Reviewer", reviewer_reference="T-1",
                                recorded_by="admin")
    legal.record_eligibility(db, "GB", "RESTRICTED", determination_ref="TEST-DET-1", determined_by="admin")
    with db.connect() as conn:
        actions = [r[0] for r in conn.execute("SELECT action FROM legal_audit_log ORDER BY id").fetchall()]
        assert actions == ["draft_document_seeded", "document_published", "document_accepted", "consent_withdrawn",
                           "counsel_review_recorded", "country_eligibility_recorded"]
    for sql in ("UPDATE legal_audit_log SET action = 'x'", "DELETE FROM legal_audit_log"):
        with pytest.raises(sqlite3.DatabaseError, match="append-only"):
            with db.connect() as conn:
                conn.execute(sql)


# --------------------------------------------------------------------------- country eligibility

def test_country_eligibility_defaults_to_undetermined_and_needs_a_reference(tmp_path):
    db = _db(tmp_path)
    assert legal.eligibility(db, "ng")["status"] == "UNDETERMINED"
    assert legal.eligibility(db, "not-a-country")["status"] == "UNDETERMINED"
    assert legal.eligibility(db, None)["valid_country_code"] is False
    with pytest.raises(legal.LegalError) as err:
        legal.record_eligibility(db, "NG", "PERMITTED", determination_ref="", determined_by="admin")
    assert err.value.code == "DETERMINATION_REF_REQUIRED"
    with pytest.raises(sqlite3.IntegrityError):
        with db.connect() as conn:
            conn.execute("INSERT INTO legal_country_eligibility (country_code, status) VALUES ('US', 'PROHIBITED')")
    result = legal.record_eligibility(db, "NG", "PERMITTED", determination_ref="TEST-DET-2", determined_by="admin")
    assert (result["status"], result["determination_ref"], result["determined_by"]) == (
        "PERMITTED", "TEST-DET-2", "admin")


# --------------------------------------------------------------------------- HTTP

@pytest.fixture
def api(tmp_path, monkeypatch):
    db = _db(tmp_path)
    monkeypatch.setattr(legal_api, "_db", lambda: db)
    monkeypatch.setattr(admin_legal, "_db", lambda: db)
    app = FastAPI()
    app.include_router(legal_api.router, prefix="/api")
    app.include_router(admin_legal.router, prefix="/api")
    state = {"user": "user-1", "admin": True}
    app.dependency_overrides[get_current_active_user] = lambda: {"id": state["user"], "status": "active"}
    app.dependency_overrides[require_admin] = lambda: dict(ADMIN)
    return SimpleNamespace(client=TestClient(app), db=db, app=app)


def test_http_publish_accept_status_withdraw(api):
    c = api.client
    res = c.post("/api/admin/legal/documents", json={
        "category": "TERMS_OF_SERVICE", "title": "Test terms", "body": PLACEHOLDER.format(n=1)})
    assert res.status_code == 201, res.text
    terms = res.json()
    assert terms["review_status"] == "DRAFT_PENDING_COUNSEL" and terms["is_draft"] is True
    # The publish request cannot smuggle an approval in.
    res = c.post("/api/admin/legal/documents", json={
        "category": "PRIVACY_POLICY", "title": "Test privacy", "body": PLACEHOLDER.format(n=1),
        "review_status": "COUNSEL_APPROVED", "reviewer_name": "Nobody"})
    assert res.status_code == 201 and res.json()["review_status"] == "DRAFT_PENDING_COUNSEL"

    public = c.get("/api/legal/documents?purpose=registration").json()
    assert {d["category"] for d in public["documents"]} == {
        "TERMS_OF_SERVICE", "PRIVACY_POLICY", "TRADING_RISK_DISCLOSURE"}
    assert set(public["unpublished"]) == {"DATA_PROCESSING_NOTICE", "REGISTRATION_CONSENT"}
    assert all(d["is_draft"] for d in public["documents"])

    status = c.get("/api/legal/status").json()
    assert status["reconsent_required"] is True and len(status["pending"]) == 3

    items = [{"doc_id": d["doc_id"], "content_sha256": d["content_sha256"]} for d in public["documents"]]
    res = c.post("/api/legal/accept", json={"documents": items})
    assert res.status_code == 200, res.text
    assert c.get("/api/legal/status").json()["reconsent_required"] is False

    bad = c.post("/api/legal/accept", json={"documents": [{"doc_id": terms["doc_id"], "content_sha256": "f" * 64}]})
    assert bad.status_code == 409 and bad.json()["detail"]["error_code"] == "CONTENT_HASH_MISMATCH"

    res = c.post("/api/legal/withdraw", json={"category": "TERMS_OF_SERVICE"})
    assert res.status_code == 200 and res.json()["effects"][0]["code"] == "SERVICE_TERMINATION_REQUESTED"
    assert c.post("/api/legal/withdraw", json={"category": "TERMS_OF_SERVICE"}).status_code == 409

    res = c.post(f"/api/admin/legal/documents/{terms['doc_id']}/review",
                 json={"reviewer_name": "Test Reviewer", "reviewer_reference": "TEST-REF-9"})
    assert res.status_code == 200 and res.json()["is_draft"] is False
    assert c.post(f"/api/admin/legal/documents/{terms['doc_id']}/review",
                  json={"reviewer_name": "Test Reviewer", "reviewer_reference": "TEST-REF-9"}).status_code == 409
    assert c.post("/api/admin/legal/documents/legal_missing/review",
                  json={"reviewer_name": "Test Reviewer", "reviewer_reference": "R-1"}).status_code == 404
    assert c.get(f"/api/legal/documents/{terms['doc_id']}").json()["review_status"] == "COUNSEL_APPROVED"

    res = c.post("/api/admin/legal/country-eligibility", json={"country_code": "NG", "status": "PROHIBITED"})
    assert res.status_code == 400
    res = c.post("/api/admin/legal/country-eligibility",
                 json={"country_code": "NG", "status": "PROHIBITED", "determination_ref": "TEST-DET-3"})
    assert res.status_code == 200 and res.json()["status"] == "PROHIBITED"
    assert c.get("/api/legal/eligibility/NG").json()["status"] == "PROHIBITED"
    assert c.get("/api/legal/eligibility/FR").json()["status"] == "UNDETERMINED"


def test_admin_legal_endpoints_require_an_admin(api):
    api.app.dependency_overrides.pop(require_admin)
    res = api.client.post("/api/admin/legal/documents", json={
        "category": "TERMS_OF_SERVICE", "title": "Test terms", "body": PLACEHOLDER.format(n=1)})
    assert res.status_code in (401, 403)
    res = api.client.post("/api/admin/legal/documents/x/review",
                          json={"reviewer_name": "Test Reviewer", "reviewer_reference": "R"})
    assert res.status_code in (401, 403)
    assert legal.current_document(api.db, "TERMS_OF_SERVICE") is None


def test_legal_status_requires_login(api):
    api.app.dependency_overrides.pop(get_current_active_user)
    assert api.client.get("/api/legal/status").status_code == 401
    assert api.client.get("/api/legal/documents").status_code == 200  # public


# --------------------------------------------------------------------------- registration

@pytest.fixture
def reg(tmp_path, monkeypatch):
    db_path = tmp_path / "reg.db"
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{db_path.as_posix()}")
    migrate(str(db_path))
    db = DB(path=str(db_path))
    monkeypatch.setattr(auth, "DB", lambda: db)
    monkeypatch.setattr(security, "pwd_context", CryptContext(schemes=["bcrypt"], bcrypt__rounds=4))
    monkeypatch.setattr(security, "_DUMMY_PASSWORD_HASH", None)
    monkeypatch.setattr(auth, "_deliver_code", lambda email, purpose, code: None)
    app = FastAPI()
    app.include_router(auth.router, prefix="/api/v1/auth")
    return SimpleNamespace(client=TestClient(app), db=db)


def test_registration_records_the_accepted_document_versions(reg):
    risk = legal.current_document(reg.db, "TRADING_RISK_DISCLOSURE")
    res = reg.client.post("/api/v1/auth/register", json={
        "email": "new.user@example.com", "password": "a long enough test password",
        "risk_disclaimer_accepted_at": T0.isoformat(),
        "legal_acceptances": [
            {"doc_id": risk["doc_id"], "content_sha256": risk["content_sha256"]},
            {"doc_id": "legal_does_not_exist", "content_sha256": "a" * 64},
        ]})
    assert res.status_code == 200, res.text
    uid = res.json()["id"]
    rows = legal.user_acceptances(reg.db, uid)
    assert [(r["doc_id"], r["source"], r["content_sha256"]) for r in rows] == [
        (risk["doc_id"], "registration", risk["content_sha256"])]
    assert legal.has_current_acceptance(reg.db, uid, "TRADING_RISK_DISCLOSURE") is True


def test_registration_without_acceptances_records_nothing_and_still_succeeds(reg):
    res = reg.client.post("/api/v1/auth/register", json={
        "email": "plain.user@example.com", "password": "a long enough test password"})
    assert res.status_code == 200, res.text
    assert legal.user_acceptances(reg.db, res.json()["id"]) == []
