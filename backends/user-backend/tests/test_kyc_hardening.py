"""KYC hardening: authenticated + owner-bound document endpoints, signed upload
URLs, path containment, content validation, encryption at rest, mandatory
production secrets, and manual (admin) review instead of self-certification.

Run: cd backends/user-backend && python -m pytest tests/test_kyc_hardening.py
"""
from __future__ import annotations

import base64
import os
import sys
import time
from pathlib import Path
from types import SimpleNamespace

import pytest
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from fastapi import FastAPI
from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "backends" / "shared"))
sys.path.insert(0, str(ROOT / "backends" / "user-backend"))

from app.api import admin, kyc  # noqa: E402
from app.core import kyc_encryption, kyc_storage  # noqa: E402
from shared_lib.core.policy.kyc_policy import KYCAction, evaluate_kyc_gate  # noqa: E402
from shared_lib.persistence.db import DB, utc_now_iso  # noqa: E402
from shared_lib.persistence.migrations import migrate  # noqa: E402

USER_A = "11111111-1111-4111-8111-111111111111"
USER_B = "22222222-2222-4222-8222-222222222222"
ADMIN = {"id": "admin-test", "email": "reviewer@example.com", "role": "admin"}

PNG = b"\x89PNG\r\n\x1a\n" + b"\x00" * 64
JPG = b"\xff\xd8\xff\xe0" + b"\x00" * 64
PDF = b"%PDF-1.4\n" + b"\x00" * 64

PERSONAL_INFO = {
    "full_legal_name": "Ada Example",
    "date_of_birth": "1990-01-01",
    "nationality": "NG",
    "country_of_residence": "NG",
    "address_line1": "1 Example Street",
    "address_city": "Lagos",
    "address_postal_code": "100001",
}


def _test_profile(monkeypatch):
    """An explicit non-production profile (the settings default is production)."""
    monkeypatch.setenv("APP_ENV", "TEST")
    monkeypatch.setenv("ENVIRONMENT_NAME", "test")
    monkeypatch.setenv("DATABASE_ROLE", "test")
    monkeypatch.delenv("KYC_ENCRYPTION_KEY", raising=False)
    monkeypatch.delenv("KYC_URL_SECRET", raising=False)


def _production_profile(monkeypatch):
    monkeypatch.setenv("APP_ENV", "PRODUCTION")


@pytest.fixture
def storage(tmp_path, monkeypatch):
    """Storage only (no database, no app)."""
    _test_profile(monkeypatch)
    monkeypatch.setattr(kyc_storage, "KYC_UPLOAD_DIR", tmp_path / "kyc_uploads")
    return SimpleNamespace(tmp_path=tmp_path)


@pytest.fixture
def env(tmp_path, monkeypatch):
    _test_profile(monkeypatch)
    db_path = tmp_path / "kyc.db"
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{db_path.as_posix()}")
    migrate(str(db_path))
    db = DB(path=str(db_path))
    monkeypatch.setattr(kyc_storage, "KYC_UPLOAD_DIR", tmp_path / "kyc_uploads")

    now = utc_now_iso()
    with db.connect() as conn:
        for user_id, email in ((USER_A, "a@example.com"), (USER_B, "b@example.com")):
            conn.execute(
                "INSERT INTO users (id, email, hashed_password, status, created_at, updated_at) "
                "VALUES (?, ?, ?, 'active', ?, ?)",
                (user_id, email, "not-a-real-hash", now, now),
            )

    state = {"user_id": USER_A}
    app = FastAPI()
    app.include_router(kyc.router)
    app.include_router(admin.router, prefix="/api")
    app.dependency_overrides[kyc.get_current_user_id] = lambda: state["user_id"]
    app.dependency_overrides[admin.require_admin] = lambda: dict(ADMIN)
    return SimpleNamespace(client=TestClient(app), db=db, state=state, tmp_path=tmp_path)


# ---------------------------------------------------------------------------
# Flow helpers
# ---------------------------------------------------------------------------

def _start_case(client):
    response = client.post("/kyc/start")
    assert response.status_code == 200, response.text
    return response.json()["case_id"]


def _request_upload(client, doc_type="passport", side="front"):
    response = client.post("/kyc/documents/upload-url", json={"doc_type": doc_type, "side": side})
    assert response.status_code == 200, response.text
    return response.json()


def _put(client, upload_url, content, content_type):
    return client.put(upload_url, content=content, headers={"Content-Type": content_type})


def _upload_document(client, doc_type="passport", side="front", content=PNG, content_type="image/png"):
    info = _request_upload(client, doc_type, side)
    response = _put(client, info["upload_url"], content, content_type)
    assert response.status_code == 200, response.text
    response = client.post("/kyc/documents/confirm", json={
        "doc_id": info["doc_id"],
        "file_ref": info["file_ref"],
        "side": side,
        "file_size_bytes": len(content),
        "content_type": content_type,
    })
    assert response.status_code == 200, response.text
    return info


def _upload_selfie(client):
    response = client.post("/kyc/face/start", json={"provider": "internal"})
    assert response.status_code == 200, response.text
    session = response.json()
    response = _put(client, session["upload_url"], JPG, "image/jpeg")
    assert response.status_code == 200, response.text
    response = client.post("/kyc/face/complete", json={"selfie_file_ref": session["selfie_upload_ref"]})
    assert response.status_code == 200, response.text
    return session


def _complete_and_submit(client):
    case_id = _start_case(client)
    response = client.post("/kyc/personal-info", json=PERSONAL_INFO)
    assert response.status_code == 200, response.text
    document = _upload_document(client)
    selfie = _upload_selfie(client)
    response = client.post("/kyc/submit")
    assert response.status_code == 200, response.text
    return SimpleNamespace(case_id=case_id, document=document, selfie=selfie, submit=response.json())


def _case_status(db, user_id=USER_A):
    with db.connect() as conn:
        row = conn.execute("SELECT status FROM kyc_cases WHERE user_id = ?", (user_id,)).fetchone()
    return row["status"] if row else None


# ---------------------------------------------------------------------------
# 1. File endpoints: one authenticated, owner-bound handler per path
# ---------------------------------------------------------------------------

def test_exactly_one_handler_per_file_path():
    """The unauthenticated handlers used to shadow authenticated duplicates."""
    seen = {}
    for route in kyc.router.routes:
        for method in getattr(route, "methods", set()) or set():
            seen.setdefault((method, route.path), []).append(route)
    upload = seen.get(("PUT", "/kyc/documents/upload/{file_ref:path}"), [])
    download = seen.get(("GET", "/kyc/documents/download/{file_ref:path}"), [])
    assert len(upload) == 1
    assert len(download) == 1
    duplicates = {key: len(routes) for key, routes in seen.items() if len(routes) > 1}
    assert duplicates == {}


def test_file_endpoints_require_login(tmp_path, monkeypatch):
    _test_profile(monkeypatch)
    monkeypatch.setattr(kyc_storage, "KYC_UPLOAD_DIR", tmp_path / "kyc_uploads")
    app = FastAPI()
    app.include_router(kyc.router)  # real auth dependency, no override
    client = TestClient(app)

    file_ref = f"documents/{USER_A}/passport_20260101_000000_abcdef_front"
    info = kyc_storage.build_upload_url(USER_A, file_ref)

    # Even a perfectly valid signed URL is useless without a login.
    assert client.put(info["upload_url"], content=PNG, headers={"Content-Type": "image/png"}).status_code == 401
    assert client.get(f"/kyc/documents/download/{file_ref}").status_code == 401
    assert not kyc_storage.file_exists(file_ref)


def test_upload_url_is_bound_to_the_user(env):
    _start_case(env.client)
    info = _request_upload(env.client)

    # Another logged-in user cannot use the URL.
    env.state["user_id"] = USER_B
    _start_case(env.client)
    assert _put(env.client, info["upload_url"], PNG, "image/png").status_code == 403

    env.state["user_id"] = USER_A
    base, signature = info["upload_url"].rsplit("sig=", 1)
    tampered = base + "sig=" + ("0" * len(signature))
    assert _put(env.client, tampered, PNG, "image/png").status_code == 403
    # The old 16-hex-character signature length is not accepted.
    assert _put(env.client, base + "sig=" + signature[:16], PNG, "image/png").status_code == 403
    assert not kyc_storage.file_exists(info["file_ref"])

    assert _put(env.client, info["upload_url"], PNG, "image/png").status_code == 200
    assert kyc_storage.file_exists(info["file_ref"])


def test_signature_is_full_length_and_bound_to_user_method_path_and_expiry(storage):
    file_ref = kyc_storage.generate_file_ref(USER_A, "passport", "front")
    info = kyc_storage.build_upload_url(USER_A, file_ref)
    signature = info["upload_url"].rsplit("sig=", 1)[1]
    expires = info["expires_at"]

    assert len(signature) == 64  # full HMAC-SHA256, hex
    assert kyc_storage.verify_upload_signature(USER_A, file_ref, expires, signature)
    assert not kyc_storage.verify_upload_signature(USER_B, file_ref, expires, signature)
    assert not kyc_storage.verify_upload_signature(USER_A, file_ref + "x", expires, signature)
    assert not kyc_storage.verify_upload_signature(USER_A, file_ref, expires + 1, signature)
    assert signature != kyc_storage._sign_url("GET", USER_A, file_ref, expires)

    past = int(time.time()) - 10
    expired_signature = kyc_storage._sign_url("PUT", USER_A, file_ref, past)
    assert not kyc_storage.verify_upload_signature(USER_A, file_ref, past, expired_signature)


def test_user_can_only_download_their_own_documents(env):
    _start_case(env.client)
    info = _upload_document(env.client)

    response = env.client.get(f"/kyc/documents/download/{info['file_ref']}")
    assert response.status_code == 200
    assert response.content == PNG
    assert response.headers["content-type"] == "image/png"

    env.state["user_id"] = USER_B
    assert env.client.get(f"/kyc/documents/download/{info['file_ref']}").status_code == 404


# ---------------------------------------------------------------------------
# 1c/1d. Path containment and content validation
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("bad_ref", [
    "../../etc/passwd",
    "/etc/passwd",
    "documents/../../outside",
    f"documents/{USER_A}/../../../outside",
    f"documents/{USER_A}/name.with.dots",
    f"documents/{USER_A}/nul\x00byte",
    f"documents\\{USER_A}\\backslash",
    f"documents/{USER_A}/nested/extra",
    f"other/{USER_A}/file",
    "",
])
def test_storage_rejects_refs_it_could_not_have_generated(storage, bad_ref):
    outside = storage.tmp_path / "outside.png"
    outside.write_bytes(PNG)

    assert not kyc_storage.is_valid_file_ref(bad_ref)
    assert kyc_storage.get_file_path(bad_ref) is None
    assert kyc_storage.read_stored_file(bad_ref) is None
    assert kyc_storage.delete_file(bad_ref) is False
    ok, _message = kyc_storage.save_uploaded_file(bad_ref, PNG)
    assert ok is False
    assert outside.read_bytes() == PNG


@pytest.mark.parametrize("suffix", ["\n", "\r\n", " ", "\n\n"])
def test_file_ref_with_trailing_whitespace_is_rejected(storage, suffix):
    good = kyc_storage.generate_file_ref(USER_A, "passport", "front")
    assert kyc_storage.is_valid_file_ref(good)
    # "$" also matches just before a final newline; the whole string must match.
    assert not kyc_storage.is_valid_file_ref(good + suffix)
    assert kyc_storage.file_ref_owner(good + suffix) is None
    assert not kyc_storage.file_ref_belongs_to(good + suffix, USER_A)
    assert kyc_storage.save_uploaded_file(good + suffix, PNG)[0] is False
    with pytest.raises(kyc_storage.InvalidFileRef):
        kyc_storage.generate_file_ref(USER_A + suffix, "passport", "front")
    with pytest.raises(kyc_storage.InvalidFileRef):
        kyc_storage.generate_selfie_ref(USER_A + suffix)
    with pytest.raises(kyc_storage.InvalidFileRef):
        kyc_storage.generate_file_ref(USER_A, "passport" + suffix, "front")


def test_confirm_does_not_trust_a_client_supplied_file_ref(env):
    _start_case(env.client)
    info = _request_upload(env.client)
    outside = env.tmp_path / "outside.png"
    outside.write_bytes(PNG)

    # A real file of ANOTHER user must not be attachable either.
    env.state["user_id"] = USER_B
    _start_case(env.client)
    other = _upload_document(env.client)
    env.state["user_id"] = USER_A

    for forged in ("../../outside", str(outside), other["file_ref"],
                   f"documents/{USER_A}/national_id_20260101_000000_abc_front"):
        response = env.client.post("/kyc/documents/confirm", json={
            "doc_id": info["doc_id"], "file_ref": forged, "side": "front",
            "file_size_bytes": 1, "content_type": "image/png",
        })
        assert response.status_code == 400, forged

    # The genuine reference is refused until the file was really uploaded.
    response = env.client.post("/kyc/documents/confirm", json={
        "doc_id": info["doc_id"], "file_ref": info["file_ref"], "side": "front",
        "file_size_bytes": 1, "content_type": "image/png",
    })
    assert response.status_code == 400

    with env.db.connect() as conn:
        row = conn.execute("SELECT front_file_ref FROM kyc_documents WHERE id = ?", (info["doc_id"],)).fetchone()
    assert row["front_file_ref"] is None
    assert outside.exists()


def test_delete_never_follows_a_poisoned_database_reference(env):
    _start_case(env.client)
    info = _upload_document(env.client)
    outside = env.tmp_path / "outside.png"
    outside.write_bytes(PNG)
    with env.db.connect() as conn:  # a legacy row holding a client-supplied path
        conn.execute("UPDATE kyc_documents SET back_file_ref = ? WHERE id = ?", ("../outside", info["doc_id"]))

    response = env.client.delete(f"/kyc/documents/{info['doc_id']}")
    assert response.status_code == 200, response.text
    assert outside.exists()
    assert not kyc_storage.file_exists(info["file_ref"])


def test_upload_validates_magic_bytes_declared_type_and_size(env):
    _start_case(env.client)
    info = _request_upload(env.client)
    url = info["upload_url"]

    assert _put(env.client, url, b"<html><script>alert(1)</script></html>", "image/png").status_code == 400
    assert _put(env.client, url, b"MZ\x90\x00 not an image", "image/jpeg").status_code == 400
    # Real JPEG bytes declared as PNG: content and declared type disagree.
    assert _put(env.client, url, JPG, "image/png").status_code == 400
    assert _put(env.client, url, b"", "image/png").status_code == 400
    too_large = b"\xff\xd8\xff" + b"\x00" * kyc_storage.MAX_FILE_SIZE
    assert _put(env.client, url, too_large, "image/jpeg").status_code == 413
    assert not kyc_storage.file_exists(info["file_ref"])

    for content, content_type in ((PNG, "image/png"), (JPG, "image/jpeg"), (PDF, "application/pdf")):
        assert _put(env.client, url, content, content_type).status_code == 200
        stored_content, stored_type = kyc_storage.read_stored_file(info["file_ref"])
        assert stored_content == content
        assert stored_type == content_type


def test_confirm_records_server_side_size_and_type(env):
    _start_case(env.client)
    info = _request_upload(env.client)
    assert _put(env.client, info["upload_url"], PNG, "image/png").status_code == 200
    response = env.client.post("/kyc/documents/confirm", json={
        "doc_id": info["doc_id"], "file_ref": info["file_ref"], "side": "front",
        "file_size_bytes": 999_999_999, "content_type": "text/html",
    })
    assert response.status_code == 200, response.text
    with env.db.connect() as conn:
        row = conn.execute(
            "SELECT file_size_bytes, file_content_type FROM kyc_documents WHERE id = ?", (info["doc_id"],)
        ).fetchone()
    assert row["file_size_bytes"] == len(PNG)
    assert row["file_content_type"] == "image/png"


# ---------------------------------------------------------------------------
# 2. Encryption at rest
# ---------------------------------------------------------------------------

def test_documents_are_encrypted_on_disk(env):
    _start_case(env.client)
    info = _upload_document(env.client)

    path = kyc_storage.get_file_path(info["file_ref"])
    raw = path.read_bytes()
    assert raw != PNG
    assert raw.startswith(kyc_encryption.FERNET_TOKEN_PREFIX)
    assert b"PNG" not in raw
    assert kyc_storage.read_stored_file(info["file_ref"])[0] == PNG


def test_legacy_plaintext_files_remain_readable(storage):
    file_ref = f"documents/{USER_A}/passport_20240101_000000_abcd1234_front"
    kyc_storage.ensure_upload_dir()
    legacy_path = kyc_storage.KYC_UPLOAD_DIR / f"{file_ref}.jpeg"
    legacy_path.parent.mkdir(parents=True, exist_ok=True)
    legacy_path.write_bytes(JPG)  # written by the old code: no encryption

    content, content_type = kyc_storage.read_stored_file(file_ref)
    assert content == JPG
    assert content_type == "image/jpeg"


# ---------------------------------------------------------------------------
# 1b/3. Secrets: mandatory in production, backward compatible derivation
# ---------------------------------------------------------------------------

def _legacy_pii_ciphertext(secret: str, value: str) -> str:
    """Ciphertext exactly as the pre-hardening code produced it."""
    kdf = PBKDF2HMAC(algorithm=hashes.SHA256(), length=32, salt=b"cosmicforge_kyc_salt", iterations=100000)
    key = base64.urlsafe_b64encode(kdf.derive(secret.encode()))
    return base64.urlsafe_b64encode(Fernet(key).encrypt(value.encode())).decode()


def test_existing_pii_ciphertext_still_decrypts(monkeypatch):
    _test_profile(monkeypatch)
    legacy_default = _legacy_pii_ciphertext("cosmicforge-kyc-dev-secret-key-2024", "Ada Example")
    assert kyc_encryption.decrypt_pii(legacy_default) == "Ada Example"

    real_key = "k" * 48
    monkeypatch.setenv("KYC_ENCRYPTION_KEY", real_key)
    # Written earlier with the same real key: same derivation, still readable.
    assert kyc_encryption.decrypt_pii(_legacy_pii_ciphertext(real_key, "Grace")) == "Grace"
    # Written while the server still ran on the built-in default: readable after the key is set.
    assert kyc_encryption.decrypt_pii(legacy_default) == "Ada Example"
    # New data is written with the real key only.
    fresh = kyc_encryption.encrypt_pii("Linus")
    assert kyc_encryption.decrypt_pii(fresh) == "Linus"
    monkeypatch.delenv("KYC_ENCRYPTION_KEY")
    assert kyc_encryption.decrypt_pii(fresh) is None


@pytest.mark.parametrize("value", [None, "", "cosmicforge-kyc-dev-secret-key-2024",
                                   "your-kyc-encryption-key-32-chars-min"])
def test_encryption_key_is_mandatory_in_production(monkeypatch, value):
    _production_profile(monkeypatch)
    if value is None:
        monkeypatch.delenv("KYC_ENCRYPTION_KEY", raising=False)
    else:
        monkeypatch.setenv("KYC_ENCRYPTION_KEY", value)

    with pytest.raises(kyc_encryption.KYCConfigError):
        kyc_encryption.encrypt_pii("secret")
    with pytest.raises(kyc_encryption.KYCConfigError):
        kyc_encryption.encrypt_file_bytes(PNG)
    with pytest.raises(kyc_encryption.KYCConfigError):
        kyc_encryption.assert_kyc_encryption_configured()


@pytest.mark.parametrize("value", [None, "", "kyc-url-signing-secret-dev"])
def test_url_secret_is_mandatory_in_production(monkeypatch, value):
    _production_profile(monkeypatch)
    monkeypatch.setenv("KYC_ENCRYPTION_KEY", "k" * 48)
    monkeypatch.setenv("SECRET_KEY", "s" * 48)  # must NOT be used as a fallback in production
    if value is None:
        monkeypatch.delenv("KYC_URL_SECRET", raising=False)
    else:
        monkeypatch.setenv("KYC_URL_SECRET", value)

    with pytest.raises(kyc_encryption.KYCConfigError):
        kyc_storage.assert_kyc_storage_configured()
    with pytest.raises(kyc_encryption.KYCConfigError):
        kyc_storage.build_upload_url(USER_A, kyc_storage.generate_file_ref(USER_A, "passport", "front"))


def test_production_with_real_secrets_works(monkeypatch, tmp_path):
    _production_profile(monkeypatch)
    monkeypatch.setenv("KYC_ENCRYPTION_KEY", "k" * 48)
    monkeypatch.setenv("KYC_URL_SECRET", "u" * 48)
    monkeypatch.setattr(kyc_storage, "KYC_UPLOAD_DIR", tmp_path / "kyc_uploads")

    kyc_encryption.assert_kyc_encryption_configured()
    kyc_storage.assert_kyc_storage_configured()
    assert kyc_encryption.decrypt_pii(kyc_encryption.encrypt_pii("Ada")) == "Ada"
    info = kyc_storage.generate_upload_url(USER_A, "passport", "front")
    signature = info["upload_url"].rsplit("sig=", 1)[1]
    assert kyc_storage.verify_upload_signature(USER_A, info["file_ref"], info["expires_at"], signature)


def test_kyc_api_fails_closed_when_production_secrets_are_missing(env, monkeypatch):
    _production_profile(monkeypatch)
    monkeypatch.delenv("KYC_ENCRYPTION_KEY", raising=False)
    monkeypatch.delenv("KYC_URL_SECRET", raising=False)

    assert env.client.get("/kyc/status").status_code == 503
    assert env.client.post("/kyc/start").status_code == 503
    assert _case_status(env.db) is None


# ---------------------------------------------------------------------------
# 4. No self-certification
# ---------------------------------------------------------------------------

def test_face_verification_ignores_client_passed_flag(env):
    _start_case(env.client)
    response = env.client.post("/kyc/face/start", json={"provider": "internal"})
    assert response.status_code == 200, response.text
    session = response.json()

    # The old client: nothing uploaded, "passed": true.
    response = env.client.post("/kyc/face/complete", json={
        "selfie_file_ref": session["selfie_upload_ref"], "passed": True,
    })
    assert response.status_code == 400
    with env.db.connect() as conn:
        check = conn.execute("SELECT status FROM kyc_selfie_checks WHERE user_id = ?", (USER_A,)).fetchone()
        case = conn.execute("SELECT completed_steps FROM kyc_cases WHERE user_id = ?", (USER_A,)).fetchone()
    assert check["status"] == "pending"
    assert "face_verification" not in case["completed_steps"]

    # A selfie must be an image, uploaded to the reference the server issued.
    assert _put(env.client, session["upload_url"], PDF, "application/pdf").status_code == 400
    forged = kyc_storage.build_upload_url(USER_A, kyc_storage.generate_selfie_ref(USER_A))
    assert _put(env.client, forged["upload_url"], JPG, "image/jpeg").status_code == 403

    assert _put(env.client, session["upload_url"], JPG, "image/jpeg").status_code == 200
    response = env.client.post("/kyc/face/complete", json={
        "selfie_file_ref": session["selfie_upload_ref"], "passed": False,
    })
    assert response.status_code == 200, response.text
    # Never "passed": only "submitted, pending manual review".
    assert response.json()["status"] == "pending_review"
    with env.db.connect() as conn:
        check = conn.execute("SELECT status, selfie_file_ref FROM kyc_selfie_checks WHERE user_id = ?", (USER_A,)).fetchone()
    assert check["status"] == "pending_review"
    assert check["selfie_file_ref"] == session["selfie_upload_ref"]


def test_submit_requires_real_evidence(env):
    _start_case(env.client)
    assert env.client.post("/kyc/personal-info", json=PERSONAL_INFO).status_code == 200
    _upload_document(env.client)

    # Legacy state: the fake client-side face check marked the selfie "passed"
    # without any file. That must not be submittable.
    now = utc_now_iso()
    with env.db.connect() as conn:
        case_id = conn.execute("SELECT id FROM kyc_cases WHERE user_id = ?", (USER_A,)).fetchone()["id"]
        conn.execute(
            "INSERT INTO kyc_selfie_checks (id, user_id, kyc_case_id, provider, status, created_at, updated_at) "
            "VALUES ('legacy-check', ?, ?, 'internal', 'passed', ?, ?)",
            (USER_A, case_id, now, now),
        )
    assert env.client.post("/kyc/submit").status_code == 400
    assert _case_status(env.db) == "in_progress"

    # ... and the status page tells the user which step to redo.
    checklist = env.client.get("/kyc/checklist").json()
    steps = {step["step"]: step for step in checklist["checklist"]}
    assert checklist["can_submit"] is False
    assert steps["face_verification"]["is_complete"] is False
    assert steps["id_document"]["is_complete"] is True
    assert steps["personal_info"]["is_complete"] is True


def test_submission_waits_for_manual_review_and_is_never_auto_approved(env):
    flow = _complete_and_submit(env.client)

    assert flow.submit["status"] == "submitted"
    assert _case_status(env.db) == "submitted"
    with env.db.connect() as conn:
        reviews = conn.execute("SELECT COUNT(*) AS n FROM kyc_reviews").fetchone()["n"]
    assert reviews == 0

    # The only gate on real-funds trading stays closed.
    gate = evaluate_kyc_gate(USER_A, KYCAction.START_LIVE_TRADING.value)
    assert gate.allowed is False

    checklist = env.client.get("/kyc/checklist").json()
    assert checklist["case_status"] == "submitted"
    assert all(step["is_complete"] for step in checklist["checklist"])

    # Evidence is frozen while under review.
    assert env.client.post("/kyc/submit").status_code == 409
    assert env.client.post("/kyc/documents/upload-url", json={"doc_type": "passport"}).status_code == 409
    assert env.client.delete(f"/kyc/documents/{flow.document['doc_id']}").status_code == 409
    assert env.client.post("/kyc/face/start", json={}).status_code == 409
    assert env.client.post("/kyc/personal-info", json=PERSONAL_INFO).status_code == 409
    assert _put(env.client, flow.document["upload_url"], JPG, "image/jpeg").status_code == 409
    assert kyc_storage.read_stored_file(flow.document["file_ref"])[0] == PNG


def test_upload_held_open_across_submit_cannot_replace_the_reviewed_file(env, monkeypatch):
    """The client controls how long the request body takes to arrive, so the
    "case is still editable" check must be repeated after the body was read."""
    _start_case(env.client)
    assert env.client.post("/kyc/personal-info", json=PERSONAL_INFO).status_code == 200
    document = _upload_document(env.client)
    selfie = _upload_selfie(env.client)
    real_read = kyc._read_limited_body

    async def body_arrives_after_the_submit(request, limit):
        # The upload passed its first check; while its body is still in
        # flight, the user submits the case for review.
        with env.db.connect() as conn:
            conn.execute(
                "UPDATE kyc_cases SET status = 'submitted', submitted_at = ? WHERE user_id = ?",
                (utc_now_iso(), USER_A))
        return await real_read(request, limit)

    monkeypatch.setattr(kyc, "_read_limited_body", body_arrives_after_the_submit)

    response = _put(env.client, document["upload_url"], JPG, "image/jpeg")
    assert response.status_code == 409
    assert _case_status(env.db) == "submitted"
    # The evidence under review is byte-for-byte what was submitted.
    assert kyc_storage.read_stored_file(document["file_ref"])[0] == PNG
    assert kyc_storage.stored_file_extension(document["file_ref"]) == "png"

    # Same for the selfie.
    with env.db.connect() as conn:
        conn.execute("UPDATE kyc_cases SET status = 'in_progress' WHERE user_id = ?", (USER_A,))
    response = _put(env.client, selfie["upload_url"], PNG, "image/png")
    assert response.status_code == 409
    assert kyc_storage.read_stored_file(selfie["selfie_upload_ref"])[0] == JPG


def test_selfie_reference_replaced_during_upload_is_refused(env, monkeypatch):
    _start_case(env.client)
    session = env.client.post("/kyc/face/start", json={"provider": "internal"}).json()
    real_read = kyc._read_limited_body

    async def reference_is_rotated_meanwhile(request, limit):
        with env.db.connect() as conn:
            conn.execute("UPDATE kyc_selfie_checks SET selfie_file_ref = ? WHERE user_id = ?",
                         (kyc_storage.generate_selfie_ref(USER_A), USER_A))
        return await real_read(request, limit)

    monkeypatch.setattr(kyc, "_read_limited_body", reference_is_rotated_meanwhile)
    assert _put(env.client, session["upload_url"], JPG, "image/jpeg").status_code == 403
    assert not kyc_storage.file_exists(session["selfie_upload_ref"])


def test_outstanding_unconfirmed_upload_references_are_capped(env):
    _start_case(env.client)
    issued = [_request_upload(env.client) for _ in range(kyc.MAX_UNCONFIRMED_UPLOADS)]
    assert len({info["file_ref"] for info in issued}) == kyc.MAX_UNCONFIRMED_UPLOADS

    refused = env.client.post("/kyc/documents/upload-url", json={"doc_type": "passport", "side": "front"})
    assert refused.status_code == 429
    # A refused request records nothing.
    with env.db.connect() as conn:
        count = conn.execute(
            "SELECT COUNT(*) AS n FROM kyc_audit_log WHERE event_type = 'kyc_id_upload_initiated'").fetchone()["n"]
    assert count == kyc.MAX_UNCONFIRMED_UPLOADS

    # Another user has their own allowance.
    env.state["user_id"] = USER_B
    _start_case(env.client)
    _request_upload(env.client)
    env.state["user_id"] = USER_A

    # Uploading a file does not free a slot (it is still unconfirmed)...
    info = issued[0]
    assert _put(env.client, info["upload_url"], PNG, "image/png").status_code == 200
    assert env.client.post(
        "/kyc/documents/upload-url", json={"doc_type": "passport", "side": "front"}).status_code == 429
    # ...confirming it does.
    response = env.client.post("/kyc/documents/confirm", json={
        "doc_id": info["doc_id"], "file_ref": info["file_ref"], "side": "front",
        "file_size_bytes": len(PNG), "content_type": "image/png",
    })
    assert response.status_code == 200, response.text
    _request_upload(env.client)
    assert env.client.post(
        "/kyc/documents/upload-url", json={"doc_type": "passport", "side": "front"}).status_code == 429


def test_issued_references_stop_counting_once_their_url_has_expired(env):
    _start_case(env.client)
    for _ in range(kyc.MAX_UNCONFIRMED_UPLOADS):
        _request_upload(env.client)
    assert env.client.post("/kyc/documents/upload-url", json={"doc_type": "passport"}).status_code == 429

    # Nothing was uploaded and the signed URLs are past their lifetime.
    with env.db.connect() as conn:
        conn.execute("UPDATE kyc_audit_log SET created_at = '2020-01-01T00:00:00Z' "
                     "WHERE event_type = 'kyc_id_upload_initiated'")
    _request_upload(env.client)


def test_unconfirmed_files_on_disk_are_capped_however_the_urls_were_obtained(env):
    _start_case(env.client)
    # Validly signed URLs that the issue-time cap never saw.
    urls = [
        kyc_storage.build_upload_url(USER_A, kyc_storage.generate_file_ref(USER_A, "passport", "front"))
        for _ in range(kyc.MAX_UNCONFIRMED_UPLOADS + 1)
    ]
    for info in urls[:kyc.MAX_UNCONFIRMED_UPLOADS]:
        assert _put(env.client, info["upload_url"], PNG, "image/png").status_code == 200

    extra = urls[-1]
    assert _put(env.client, extra["upload_url"], PNG, "image/png").status_code == 429
    assert not kyc_storage.file_exists(extra["file_ref"])
    assert len(kyc_storage.list_user_document_files(USER_A)) == kyc.MAX_UNCONFIRMED_UPLOADS
    # Replacing a file that is already stored needs no new slot.
    assert _put(env.client, urls[0]["upload_url"], JPG, "image/jpeg").status_code == 200
    assert kyc_storage.read_stored_file(urls[0]["file_ref"])[0] == JPG
    # And no further references are issued while the files sit unconfirmed.
    assert env.client.post("/kyc/documents/upload-url", json={"doc_type": "passport"}).status_code == 429


def test_stale_unconfirmed_uploads_are_removed_when_a_new_url_is_requested(env):
    _start_case(env.client)
    confirmed = _upload_document(env.client)                      # attached to the document row
    abandoned = _request_upload(env.client, "national_id", "front")
    recent = _request_upload(env.client, "national_id", "back")
    for info in (abandoned, recent):
        assert _put(env.client, info["upload_url"], PNG, "image/png").status_code == 200

    old = time.time() - kyc.UNCONFIRMED_UPLOAD_MAX_AGE_SECONDS - 60
    for info in (confirmed, abandoned):
        os.utime(kyc_storage.get_file_path(info["file_ref"]), (old, old))

    # Another user's request never touches these files.
    env.state["user_id"] = USER_B
    _start_case(env.client)
    _request_upload(env.client)
    assert kyc_storage.file_exists(abandoned["file_ref"])

    env.state["user_id"] = USER_A
    _request_upload(env.client, "drivers_license", "front")
    assert not kyc_storage.file_exists(abandoned["file_ref"])   # unconfirmed and older than 24 h: gone
    assert kyc_storage.file_exists(recent["file_ref"])          # unconfirmed but recent: kept
    assert kyc_storage.file_exists(confirmed["file_ref"])       # confirmed: kept however old
    assert kyc_storage.read_stored_file(confirmed["file_ref"])[0] == PNG


def test_user_role_cannot_review_their_own_case(env):
    flow = _complete_and_submit(env.client)
    env.client.app.dependency_overrides[kyc.get_current_active_user] = lambda: {
        "id": USER_A, "email": "a@example.com", "role": "user", "status": "active",
    }
    response = env.client.post(f"/kyc/review?case_id={flow.case_id}", json={"decision": "approved"})
    assert response.status_code == 403
    assert _case_status(env.db) == "submitted"


# ---------------------------------------------------------------------------
# 5. Admin review queue works on kyc_cases
# ---------------------------------------------------------------------------

def test_admin_review_endpoints_require_admin_login(tmp_path, monkeypatch):
    _test_profile(monkeypatch)
    app = FastAPI()
    app.include_router(admin.router, prefix="/api")  # real admin dependency
    client = TestClient(app)

    assert client.get("/api/admin/compliance/kyc-pending").status_code == 401
    assert client.get("/api/admin/compliance/kyc/some-case").status_code == 401
    assert client.get("/api/admin/compliance/kyc/some-case/selfie").status_code == 401
    assert client.get("/api/admin/compliance/kyc/some-case/documents/doc/front").status_code == 401
    assert client.post("/api/admin/compliance/kyc/some-case/approve").status_code == 401
    assert client.post("/api/admin/compliance/kyc/some-case/reject", json={"reason": "x"}).status_code == 401


def test_admin_can_list_view_and_approve_a_submitted_case(env):
    flow = _complete_and_submit(env.client)

    queue = env.client.get("/api/admin/compliance/kyc-pending")
    assert queue.status_code == 200, queue.text
    body = queue.json()
    assert body["count"] == 1
    submission = body["submissions"][0]
    # The shape the admin frontend reads.
    assert submission["id"] == flow.case_id
    assert submission["user_id"] == USER_A
    assert submission["email"] == "a@example.com"
    assert submission["full_name"] == "Ada Example"
    assert submission["status"] == "submitted"
    assert submission["submitted_at"]
    assert submission["document_type"] == "passport"

    detail = env.client.get(f"/api/admin/compliance/kyc/{flow.case_id}")
    assert detail.status_code == 200, detail.text
    detail = detail.json()
    assert detail["profile"]["full_legal_name"] == "Ada Example"
    assert detail["can_approve"] is True
    document = detail["documents"][0]
    assert document["has_front"] is True

    front = env.client.get(document["front_url"])
    assert front.status_code == 200
    assert front.content == PNG
    selfie = env.client.get(detail["selfie"]["url"])
    assert selfie.status_code == 200
    assert selfie.content == JPG

    response = env.client.post(f"/api/admin/compliance/kyc/{flow.case_id}/approve")
    assert response.status_code == 200, response.text
    assert _case_status(env.db) == "approved"
    assert evaluate_kyc_gate(USER_A, KYCAction.START_LIVE_TRADING.value).allowed is True

    with env.db.connect() as conn:
        review = conn.execute("SELECT reviewer_id, reviewer_type, decision FROM kyc_reviews").fetchone()
        audit = conn.execute(
            "SELECT user_id, details FROM auth_audit_log WHERE event_type = 'kyc_approved'"
        ).fetchone()
        kyc_events = {r["event_type"] for r in conn.execute("SELECT event_type FROM kyc_audit_log").fetchall()}
    assert (review["reviewer_id"], review["reviewer_type"], review["decision"]) == (ADMIN["id"], "admin", "approved")
    assert audit["user_id"] == USER_A
    assert ADMIN["id"] in audit["details"]
    assert {"kyc_approved", "kyc_case_viewed", "kyc_document_viewed", "kyc_selfie_viewed"} <= kyc_events

    # Decided cases leave the queue and cannot be decided twice.
    assert env.client.get("/api/admin/compliance/kyc-pending").json()["count"] == 0
    assert env.client.post(f"/api/admin/compliance/kyc/{flow.case_id}/approve").status_code == 409
    assert env.client.post(f"/api/admin/compliance/kyc/{flow.case_id}/reject", json={"reason": "late"}).status_code == 409
    assert _case_status(env.db) == "approved"


def test_admin_reject_requires_a_reason_and_is_audited(env):
    flow = _complete_and_submit(env.client)
    url = f"/api/admin/compliance/kyc/{flow.case_id}/reject"

    assert env.client.post(url).status_code == 400
    assert env.client.post(url, json={"reason": "   "}).status_code == 400
    assert _case_status(env.db) == "submitted"

    # JSON body, as the admin frontend sends it.
    response = env.client.post(url, json={"reason": "Document is unreadable"})
    assert response.status_code == 200, response.text
    assert _case_status(env.db) == "rejected"
    assert evaluate_kyc_gate(USER_A, KYCAction.START_LIVE_TRADING.value).allowed is False

    with env.db.connect() as conn:
        case = conn.execute("SELECT rejection_reason FROM kyc_cases WHERE id = ?", (flow.case_id,)).fetchone()
        audit = conn.execute("SELECT details FROM auth_audit_log WHERE event_type = 'kyc_rejected'").fetchone()
    assert case["rejection_reason"] == "Document is unreadable"
    assert "Document is unreadable" in audit["details"]

    checklist = env.client.get("/kyc/checklist").json()
    assert checklist["case_status"] == "rejected"
    assert checklist["rejection_reason"] == "Document is unreadable"


def test_admin_reject_also_accepts_the_legacy_query_parameter(env):
    flow = _complete_and_submit(env.client)
    response = env.client.post(
        f"/api/admin/compliance/kyc/{flow.case_id}/reject", params={"reason": "Selfie does not match"}
    )
    assert response.status_code == 200, response.text
    assert _case_status(env.db) == "rejected"


def test_admin_can_request_resubmission_and_user_can_resubmit(env):
    flow = _complete_and_submit(env.client)
    response = env.client.post(
        f"/api/admin/compliance/kyc/{flow.case_id}/request-resubmission", json={"reason": "Upload the back side"}
    )
    assert response.status_code == 200, response.text
    assert _case_status(env.db) == "needs_resubmission"

    # Editable again, and resubmission goes back to the queue -- not to approved.
    assert env.client.post("/kyc/submit").status_code == 200
    assert _case_status(env.db) == "submitted"
    assert env.client.get("/api/admin/compliance/kyc-pending").json()["count"] == 1


def test_admin_cannot_approve_a_case_without_evidence(env):
    case_id = _start_case(env.client)
    with env.db.connect() as conn:  # e.g. a legacy row submitted by the old self-certifying flow
        conn.execute("UPDATE kyc_cases SET status = 'submitted' WHERE id = ?", (case_id,))

    response = env.client.post(f"/api/admin/compliance/kyc/{case_id}/approve")
    assert response.status_code == 409
    assert _case_status(env.db) == "submitted"
    assert env.client.post(f"/api/admin/compliance/kyc/{case_id}/reject", json={"reason": "No documents"}).status_code == 200


def test_admin_cannot_decide_a_case_that_was_never_submitted(env):
    case_id = _start_case(env.client)
    assert env.client.post(f"/api/admin/compliance/kyc/{case_id}/approve").status_code == 409
    assert env.client.post("/api/admin/compliance/kyc/does-not-exist/approve").status_code == 404
    assert _case_status(env.db) == "in_progress"
