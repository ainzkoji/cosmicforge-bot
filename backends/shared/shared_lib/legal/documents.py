"""Versioned legal documents and the user's binding acceptances (Section H Step 8.2).

* :func:`publish` adds a new immutable version (the database refuses to change
  a published one). Every new document starts as ``DRAFT_PENDING_COUNSEL``;
  :func:`record_counsel_review` -- reachable only from the admin API -- is the
  only way to mark it ``COUNSEL_APPROVED`` and needs the reviewer's name and a
  reference to the review.
* :func:`accept` binds the acceptance to the exact ``content_sha256`` the user
  was shown; a hash that does not match the stored document, or a document
  that is no longer the current version, is refused.
* :func:`required_reconsent` lists what the user must (re)accept: documents
  never accepted, and documents with a *material* amendment published after
  the version the user accepted. Non-material amendments do not ask again.
* :func:`withdraw` withdraws consent for a category and reports its effects;
  afterwards :func:`has_current_acceptance` is False for that category, which
  is what a gate (e.g. automated execution) checks.

The IP address and user agent are stored only as keyed hashes.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Iterator, List, Optional, Sequence

from shared_lib.legal.schema import CATEGORIES, ELIGIBILITY_STATUSES

DRAFT = "DRAFT_PENDING_COUNSEL"
APPROVED = "COUNSEL_APPROVED"
DEFAULT_LOCALE = "en"

#: Documents required before an action, by purpose. A category with no
#: published document is not required (there is nothing to accept) and is
#: reported as ``unpublished`` instead.
REQUIRED_FOR: Dict[str, tuple] = {
    "registration": ("TERMS_OF_SERVICE", "PRIVACY_POLICY", "TRADING_RISK_DISCLOSURE",
                     "DATA_PROCESSING_NOTICE", "REGISTRATION_CONSENT"),
    "subscription": ("SUBSCRIPTION_TERMS",),
    "account_connection": ("ACCOUNT_CONNECTION_AUTHORIZATION",),
    "automated_execution": ("AUTOMATED_EXECUTION_CONSENT", "TRADING_RISK_DISCLOSURE"),
}

#: What withdrawing consent for a category means. Reported to the caller; the
#: platform-side consequence is that ``has_current_acceptance`` turns False.
WITHDRAWAL_EFFECTS: Dict[str, List[Dict[str, str]]] = {
    "AUTOMATED_EXECUTION_CONSENT": [
        {"code": "AUTOMATED_EXECUTION_NOT_PERMITTED",
         "description": "Consent to automated execution is withdrawn. Gates that check "
                        "has_current_acceptance(AUTOMATED_EXECUTION_CONSENT) refuse new automated orders. "
                        "Open positions are not closed automatically."},
    ],
    "ACCOUNT_CONNECTION_AUTHORIZATION": [
        {"code": "ACCOUNT_CONNECTION_NOT_AUTHORIZED",
         "description": "Authorization to use connected exchange/broker accounts is withdrawn. Connected "
                        "accounts must not be used and should be disconnected."},
    ],
    "TRADING_RISK_DISCLOSURE": [
        {"code": "RISK_ACKNOWLEDGEMENT_WITHDRAWN",
         "description": "The risk disclosure acknowledgement is withdrawn. Live and automated trading "
                        "require it to be accepted again."},
    ],
    "TERMS_OF_SERVICE": [
        {"code": "SERVICE_TERMINATION_REQUESTED",
         "description": "Continued use of the service requires accepting the Terms of Service. The "
                        "account is flagged for closure review by an operator."},
    ],
    "SUBSCRIPTION_TERMS": [
        {"code": "SUBSCRIPTION_TERMS_WITHDRAWN",
         "description": "New purchases require accepting the subscription terms again. An existing "
                        "subscription is not cancelled by this; cancel it from the subscription page."},
    ],
    "PRIVACY_POLICY": [
        {"code": "PRIVACY_OBJECTION_RECORDED",
         "description": "Recorded for operator follow-up. Data needed to run an active account cannot be "
                        "processed without it; an operator will contact you about restriction or closure."},
    ],
    "DATA_PROCESSING_NOTICE": [
        {"code": "PRIVACY_OBJECTION_RECORDED",
         "description": "Recorded for operator follow-up about restriction or closure."},
    ],
    "REGISTRATION_CONSENT": [
        {"code": "SERVICE_TERMINATION_REQUESTED",
         "description": "The account is flagged for closure review by an operator."},
    ],
    "COUNTRY_ELIGIBILITY": [
        {"code": "ELIGIBILITY_CONFIRMATION_WITHDRAWN",
         "description": "Services that depend on the country confirmation are not available until it "
                        "is given again."},
    ],
}

_LOCALE_RE = re.compile(r"^[a-z]{2,3}(-[a-z0-9]{2,8})?$")
_COUNTRY_RE = re.compile(r"^[A-Z]{2}$")


class LegalError(ValueError):
    def __init__(self, code: str, message: str, **details: Any):
        super().__init__(message)
        self.code = code
        self.details = details


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _now_iso(now: Optional[datetime] = None) -> str:
    return (now or datetime.now(timezone.utc)).isoformat()


@contextmanager
def _connection(conn_or_db) -> Iterator[Any]:
    if hasattr(conn_or_db, "connect"):
        with conn_or_db.connect() as conn:
            yield conn
    else:
        yield conn_or_db


def _fetch_dict(conn, sql: str, params: tuple = ()) -> Optional[Dict[str, Any]]:
    cur = conn.execute(sql, params)
    row = cur.fetchone()
    if row is None:
        return None
    return {desc[0]: row[idx] for idx, desc in enumerate(cur.description)}


def _fetch_all(conn, sql: str, params: tuple = ()) -> List[Dict[str, Any]]:
    cur = conn.execute(sql, params)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def content_sha256(title: str, body: str) -> str:
    """Hash of exactly what the user is shown (title and body as stored)."""
    return hashlib.sha256(f"{title}\n\n{body}".encode("utf-8")).hexdigest()


def _evidence_key() -> bytes:
    # Set LEGAL_EVIDENCE_HASH_KEY in production so the hashes cannot be
    # recomputed from a guessed IP address by someone without the key.
    return (os.environ.get("LEGAL_EVIDENCE_HASH_KEY") or "cosmicforge-legal-evidence-v1").encode("utf-8")


def evidence_hash(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    return hmac.new(_evidence_key(), str(value).encode("utf-8"), hashlib.sha256).hexdigest()


def normalize_locale(locale: Optional[str]) -> str:
    text = str(locale or DEFAULT_LOCALE).strip().lower().replace("_", "-")
    return text if _LOCALE_RE.match(text) else DEFAULT_LOCALE


def _check_category(category: str) -> str:
    category = str(category or "").strip().upper()
    if category not in CATEGORIES:
        raise LegalError("UNKNOWN_CATEGORY", f"unknown legal document category: {category!r}")
    return category


def audit(conn, action: str, *, actor_type: str, actor_id: Optional[str] = None, user_id: Optional[str] = None,
          doc_id: Optional[str] = None, category: Optional[str] = None, details: Optional[Dict[str, Any]] = None,
          now: Optional[str] = None) -> None:
    conn.execute(
        "INSERT INTO legal_audit_log (occurred_at, actor_type, actor_id, action, user_id, doc_id, category, details_json)"
        " VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (now or _now_iso(), actor_type, actor_id, action, user_id, doc_id, category,
         json.dumps(details, sort_keys=True, default=str) if details else None),
    )


def public_view(doc: Dict[str, Any]) -> Dict[str, Any]:
    """What anyone may see of a document. The reviewer's identity stays admin-only."""
    return {
        "doc_id": doc["doc_id"],
        "category": doc["category"],
        "version": doc["version"],
        "locale": doc["locale"],
        "title": doc["title"],
        "body": doc["body"],
        "content_sha256": doc["content_sha256"],
        "published_at": doc["published_at"],
        "material_change": bool(doc["material_change"]),
        "change_summary": doc.get("change_summary"),
        "review_status": doc["review_status"],
        "is_draft": doc["review_status"] != APPROVED,
        "counsel_reviewed_at": doc.get("reviewed_at") if doc["review_status"] == APPROVED else None,
    }


# ---------------------------------------------------------------------------
# documents
# ---------------------------------------------------------------------------

def get_document(conn_or_db, doc_id: str) -> Optional[Dict[str, Any]]:
    with _connection(conn_or_db) as conn:
        return _fetch_dict(conn, "SELECT * FROM legal_documents WHERE doc_id = ?", (str(doc_id),))


def _current_in_locale(conn, category: str, locale: str) -> Optional[Dict[str, Any]]:
    return _fetch_dict(
        conn,
        "SELECT * FROM legal_documents WHERE category = ? AND locale = ? ORDER BY version DESC LIMIT 1",
        (category, locale),
    )


def current_document(conn_or_db, category: str, locale: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """Newest version of ``category`` in ``locale``, falling back to the default locale."""
    category = _check_category(category)
    locale = normalize_locale(locale)
    with _connection(conn_or_db) as conn:
        doc = _current_in_locale(conn, category, locale)
        if doc is None and locale != DEFAULT_LOCALE:
            doc = _current_in_locale(conn, category, DEFAULT_LOCALE)
        return doc


def current_documents(conn_or_db, locale: Optional[str] = None,
                      categories: Optional[Iterable[str]] = None) -> List[Dict[str, Any]]:
    wanted = [_check_category(c) for c in (categories or CATEGORIES)]
    with _connection(conn_or_db) as conn:
        out = []
        for category in wanted:
            doc = current_document(conn, category, locale)
            if doc is not None:
                out.append(doc)
        return out


def list_versions(conn_or_db, category: Optional[str] = None) -> List[Dict[str, Any]]:
    with _connection(conn_or_db) as conn:
        if category:
            return _fetch_all(conn, "SELECT * FROM legal_documents WHERE category = ? ORDER BY locale, version",
                              (_check_category(category),))
        return _fetch_all(conn, "SELECT * FROM legal_documents ORDER BY category, locale, version")


def publish(
    conn_or_db,
    *,
    category: str,
    title: str,
    body: str,
    locale: Optional[str] = None,
    material_change: bool = True,
    change_summary: Optional[str] = None,
    published_by: Optional[str] = None,
    actor_type: str = "operator",
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    """Publish a new version. Never changes an existing one.

    The new document is always ``DRAFT_PENDING_COUNSEL``: publishing cannot
    claim counsel approval. The first version of a category is always treated
    as material (nobody has accepted anything yet).
    """
    category = _check_category(category)
    locale = normalize_locale(locale)
    title = str(title or "").strip()
    body = str(body or "")
    if not title or not body.strip():
        raise LegalError("EMPTY_DOCUMENT", "a legal document needs a title and a body")
    digest = content_sha256(title, body)
    now_iso = _now_iso(now)
    with _connection(conn_or_db) as conn:
        current = _current_in_locale(conn, category, locale)
        if current is not None and current["content_sha256"] == digest:
            raise LegalError("DUPLICATE_CONTENT", "this content is already the current version",
                             doc_id=current["doc_id"])
        version = (int(current["version"]) + 1) if current else 1
        doc_id = f"legal_{category.lower()}_{locale}_v{version}_{uuid.uuid4().hex[:8]}"
        material = 1 if (material_change or current is None) else 0
        conn.execute(
            """INSERT INTO legal_documents (doc_id, category, version, locale, title, body, content_sha256,
                   published_at, published_by, material_change, change_summary, supersedes_doc_id, review_status)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (doc_id, category, version, locale, title, body, digest, now_iso, published_by, material,
             (change_summary or "").strip() or None, current["doc_id"] if current else None, DRAFT),
        )
        audit(conn, "document_published", actor_type=actor_type, actor_id=published_by, doc_id=doc_id,
              category=category, details={"version": version, "locale": locale, "material_change": bool(material),
                                          "content_sha256": digest,
                                          "supersedes": current["doc_id"] if current else None}, now=now_iso)
        return _fetch_dict(conn, "SELECT * FROM legal_documents WHERE doc_id = ?", (doc_id,))


def record_counsel_review(
    conn_or_db,
    doc_id: str,
    *,
    reviewer_name: str,
    reviewer_reference: str,
    recorded_by: str,
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    """Record that counsel approved this exact version. Admin API only.

    Needs the reviewer's name and a reference to the review (engagement
    letter, ticket, opinion number). Final once recorded.
    """
    reviewer_name = str(reviewer_name or "").strip()
    reviewer_reference = str(reviewer_reference or "").strip()
    if len(reviewer_name) < 2 or len(reviewer_reference) < 2:
        raise LegalError("REVIEWER_REQUIRED", "counsel approval needs the reviewer's name and a review reference")
    if not str(recorded_by or "").strip():
        raise LegalError("RECORDER_REQUIRED", "the recording administrator must be identified")
    now_iso = _now_iso(now)
    with _connection(conn_or_db) as conn:
        doc = _fetch_dict(conn, "SELECT * FROM legal_documents WHERE doc_id = ?", (str(doc_id),))
        if doc is None:
            raise LegalError("DOCUMENT_NOT_FOUND", "legal document not found")
        if doc["review_status"] == APPROVED:
            raise LegalError("ALREADY_APPROVED", "counsel approval is already recorded for this version")
        conn.execute(
            """UPDATE legal_documents SET review_status = ?, reviewer_name = ?, reviewer_reference = ?,
                   reviewed_at = ?, review_recorded_by = ? WHERE doc_id = ?""",
            (APPROVED, reviewer_name, reviewer_reference, now_iso, str(recorded_by), doc["doc_id"]),
        )
        audit(conn, "counsel_review_recorded", actor_type="operator", actor_id=str(recorded_by),
              doc_id=doc["doc_id"], category=doc["category"],
              details={"reviewer_name": reviewer_name, "reviewer_reference": reviewer_reference,
                       "content_sha256": doc["content_sha256"]}, now=now_iso)
        return _fetch_dict(conn, "SELECT * FROM legal_documents WHERE doc_id = ?", (doc["doc_id"],))


# ---------------------------------------------------------------------------
# acceptances
# ---------------------------------------------------------------------------

def _active_acceptance(conn, user_id: str, category: str) -> Optional[Dict[str, Any]]:
    return _fetch_dict(
        conn,
        """SELECT a.*, d.published_at AS doc_published_at
             FROM legal_acceptances a JOIN legal_documents d ON d.doc_id = a.doc_id
            WHERE a.user_id = ? AND a.category = ? AND a.withdrawn_at IS NULL
            ORDER BY d.published_at DESC, a.accepted_at DESC LIMIT 1""",
        (str(user_id), category),
    )


def _material_since(conn, acceptance: Dict[str, Any], current: Dict[str, Any]) -> bool:
    """A material amendment was published after the version the user accepted."""
    if acceptance["doc_id"] == current["doc_id"]:
        return False
    if acceptance["locale"] == current["locale"]:
        row = conn.execute(
            "SELECT 1 FROM legal_documents WHERE category = ? AND locale = ? AND version > ? AND version <= ? "
            "AND material_change = 1 LIMIT 1",
            (current["category"], current["locale"], int(acceptance["version"]), int(current["version"])),
        ).fetchone()
    else:
        row = conn.execute(
            "SELECT 1 FROM legal_documents WHERE category = ? AND locale = ? AND published_at > ? "
            "AND material_change = 1 LIMIT 1",
            (current["category"], current["locale"], acceptance["doc_published_at"]),
        ).fetchone()
    return row is not None


def acceptance_status(conn_or_db, user_id: str, category: str, locale: Optional[str] = None) -> Dict[str, Any]:
    """``{category, state, current_doc_id, accepted_doc_id, ...}`` for one category.

    state: ``CURRENT`` (accepted, nothing material since), ``NOT_ACCEPTED``,
    ``MATERIAL_CHANGE`` (re-consent needed), ``WITHDRAWN`` or
    ``NOT_PUBLISHED`` (no document exists to accept).
    """
    category = _check_category(category)
    with _connection(conn_or_db) as conn:
        current = current_document(conn, category, locale)
        active = _active_acceptance(conn, user_id, category)
        out: Dict[str, Any] = {
            "category": category,
            "current_doc_id": current["doc_id"] if current else None,
            "current_version": current["version"] if current else None,
            "accepted_doc_id": active["doc_id"] if active else None,
            "accepted_version": active["version"] if active else None,
            "accepted_at": active["accepted_at"] if active else None,
        }
        if current is None:
            out["state"] = "NOT_PUBLISHED"
        elif active is None:
            withdrawn = conn.execute(
                "SELECT 1 FROM legal_acceptances WHERE user_id = ? AND category = ? AND withdrawn_at IS NOT NULL "
                "LIMIT 1", (str(user_id), category)).fetchone()
            out["state"] = "WITHDRAWN" if withdrawn else "NOT_ACCEPTED"
        elif _material_since(conn, active, current):
            out["state"] = "MATERIAL_CHANGE"
        else:
            out["state"] = "CURRENT"
        return out


def has_current_acceptance(conn_or_db, user_id: str, category: str, locale: Optional[str] = None) -> bool:
    """True only when a published document exists and the user's acceptance of
    it (or of an earlier version with no material amendment since) stands.

    False after a withdrawal. This is what consent-dependent gates check.
    """
    return acceptance_status(conn_or_db, user_id, category, locale)["state"] == "CURRENT"


def required_reconsent(conn_or_db, user_id: str, *, locale: Optional[str] = None,
                       purposes: Sequence[str] = ("registration",)) -> Dict[str, Any]:
    """What the user must accept now.

    Covers every category required for ``purposes`` and every category the
    user has an active acceptance for (a material amendment asks again).
    Withdrawn categories outside ``purposes`` are not re-prompted: the user
    chose to withdraw them.
    """
    with _connection(conn_or_db) as conn:
        wanted: List[str] = []
        for purpose in purposes:
            for category in REQUIRED_FOR.get(purpose, ()):
                if category not in wanted:
                    wanted.append(category)
        for row in conn.execute(
            "SELECT DISTINCT category FROM legal_acceptances WHERE user_id = ? AND withdrawn_at IS NULL",
            (str(user_id),),
        ).fetchall():
            if row[0] not in wanted:
                wanted.append(row[0])
        pending, unpublished, current = [], [], []
        for category in wanted:
            status = acceptance_status(conn, user_id, category, locale)
            if status["state"] == "NOT_PUBLISHED":
                unpublished.append(category)
            elif status["state"] == "CURRENT":
                current.append(category)
            else:
                doc = current_document(conn, category, locale)
                pending.append({"reason": status["state"], "accepted_version": status["accepted_version"],
                                "document": public_view(doc)})
        return {"pending": pending, "current": current, "unpublished": unpublished,
                "reconsent_required": bool(pending)}


def accept(
    conn_or_db,
    user_id: str,
    items: Sequence[Dict[str, Any]],
    *,
    ip: Optional[str] = None,
    user_agent: Optional[str] = None,
    source: str = "web",
    now: Optional[datetime] = None,
) -> List[Dict[str, Any]]:
    """Accept documents, each identified by ``doc_id`` and the ``content_sha256``
    the user was shown. All-or-nothing: one invalid item refuses the request.
    Accepting a document already accepted (and not withdrawn) is a no-op.
    """
    if not items:
        raise LegalError("NOTHING_TO_ACCEPT", "no documents were named")
    now_iso = _now_iso(now)
    with _connection(conn_or_db) as conn:
        docs = []
        for item in items:
            doc_id = str((item or {}).get("doc_id") or "")
            shown_hash = str((item or {}).get("content_sha256") or "").strip().lower()
            doc = _fetch_dict(conn, "SELECT * FROM legal_documents WHERE doc_id = ?", (doc_id,))
            if doc is None:
                raise LegalError("UNKNOWN_DOCUMENT", "unknown legal document", doc_id=doc_id)
            if not hmac.compare_digest(shown_hash.encode("utf-8"), str(doc["content_sha256"]).encode("utf-8")):
                raise LegalError("CONTENT_HASH_MISMATCH",
                                 "the document text shown does not match the published version", doc_id=doc_id)
            latest = _current_in_locale(conn, doc["category"], doc["locale"])
            if latest is None or latest["doc_id"] != doc["doc_id"]:
                raise LegalError("SUPERSEDED", "this version has been superseded; reload and accept the current one",
                                 doc_id=doc_id, current_doc_id=latest["doc_id"] if latest else None)
            docs.append(doc)

        results = []
        ip_hash, ua_hash = evidence_hash(ip), evidence_hash(user_agent)
        for doc in docs:
            existing = _fetch_dict(
                conn,
                "SELECT * FROM legal_acceptances WHERE user_id = ? AND doc_id = ? AND withdrawn_at IS NULL",
                (str(user_id), doc["doc_id"]),
            )
            if existing:
                results.append(existing)
                continue
            acceptance_id = f"lacc_{uuid.uuid4().hex}"
            conn.execute(
                """INSERT INTO legal_acceptances (acceptance_id, user_id, doc_id, category, version, locale,
                       content_sha256, review_status_at_acceptance, accepted_at, ip_hash, user_agent_hash, source)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (acceptance_id, str(user_id), doc["doc_id"], doc["category"], doc["version"], doc["locale"],
                 doc["content_sha256"], doc["review_status"], now_iso, ip_hash, ua_hash, str(source)[:40]),
            )
            audit(conn, "document_accepted", actor_type="user", actor_id=str(user_id), user_id=str(user_id),
                  doc_id=doc["doc_id"], category=doc["category"],
                  details={"version": doc["version"], "content_sha256": doc["content_sha256"],
                           "review_status": doc["review_status"], "source": source}, now=now_iso)
            results.append(_fetch_dict(conn, "SELECT * FROM legal_acceptances WHERE acceptance_id = ?",
                                       (acceptance_id,)))
        return results


def record_registration_acceptances(conn, user_id: str, items: Optional[Sequence[Dict[str, Any]]], *,
                                    ip: Optional[str] = None, user_agent: Optional[str] = None,
                                    now: Optional[datetime] = None) -> Dict[str, Any]:
    """Registration: record the acceptances the sign-up form submitted.

    Unlike :func:`accept` it never fails the registration: an item that does
    not match the current published text is skipped (and reported), and the
    user is asked for it again through ``required_reconsent`` after sign-in.
    Nothing is recorded that the client did not explicitly submit.
    """
    accepted, skipped = [], []
    for item in items or ():
        try:
            accepted.extend(accept(conn, user_id, [item], ip=ip, user_agent=user_agent,
                                   source="registration", now=now))
        except LegalError as exc:
            skipped.append({"doc_id": (item or {}).get("doc_id"), "error_code": exc.code})
    return {"accepted": [a["doc_id"] for a in accepted], "skipped": skipped}


def withdraw(conn_or_db, user_id: str, category: str, *, reason: Optional[str] = None,
             now: Optional[datetime] = None) -> Dict[str, Any]:
    """Withdraw the user's consent for ``category`` and report the effects."""
    category = _check_category(category)
    now_iso = _now_iso(now)
    with _connection(conn_or_db) as conn:
        cur = conn.execute(
            "UPDATE legal_acceptances SET withdrawn_at = ?, withdrawal_reason = ? "
            "WHERE user_id = ? AND category = ? AND withdrawn_at IS NULL",
            (now_iso, (reason or "").strip()[:500] or None, str(user_id), category),
        )
        if cur.rowcount == 0:
            raise LegalError("NOT_ACCEPTED", "there is no active acceptance to withdraw for this category")
        effects = WITHDRAWAL_EFFECTS.get(category, [])
        audit(conn, "consent_withdrawn", actor_type="user", actor_id=str(user_id), user_id=str(user_id),
              category=category, details={"withdrawn": cur.rowcount, "effects": [e["code"] for e in effects]},
              now=now_iso)
        return {
            "category": category,
            "withdrawn": cur.rowcount,
            "withdrawn_at": now_iso,
            "effects": effects,
            "has_current_acceptance": has_current_acceptance(conn, user_id, category),
        }


def user_acceptances(conn_or_db, user_id: str) -> List[Dict[str, Any]]:
    with _connection(conn_or_db) as conn:
        return _fetch_all(
            conn,
            "SELECT acceptance_id, doc_id, category, version, locale, content_sha256, review_status_at_acceptance, "
            "accepted_at, source, withdrawn_at FROM legal_acceptances WHERE user_id = ? ORDER BY accepted_at",
            (str(user_id),),
        )


# ---------------------------------------------------------------------------
# country eligibility
# ---------------------------------------------------------------------------

def normalize_country(country: Optional[str]) -> Optional[str]:
    text = str(country or "").strip().upper()
    return text if _COUNTRY_RE.match(text) else None


def eligibility(conn_or_db, country: Optional[str]) -> Dict[str, Any]:
    """The recorded determination for ``country``; ``UNDETERMINED`` by default.

    UNDETERMINED means nobody has made a (legal) determination yet. It is not
    a permission; callers decide what to do with it.
    """
    code = normalize_country(country)
    if code is None:
        return {"country_code": None, "status": "UNDETERMINED", "determination_ref": None,
                "determined_by": None, "determined_at": None, "valid_country_code": False}
    with _connection(conn_or_db) as conn:
        row = _fetch_dict(conn, "SELECT * FROM legal_country_eligibility WHERE country_code = ?", (code,))
    if row is None:
        return {"country_code": code, "status": "UNDETERMINED", "determination_ref": None,
                "determined_by": None, "determined_at": None, "valid_country_code": True}
    return {**row, "valid_country_code": True}


def record_eligibility(conn_or_db, country: str, status: str, *, determination_ref: Optional[str],
                       determined_by: str, notes: Optional[str] = None,
                       now: Optional[datetime] = None) -> Dict[str, Any]:
    code = normalize_country(country)
    if code is None:
        raise LegalError("INVALID_COUNTRY", "country must be an ISO 3166-1 alpha-2 code")
    status = str(status or "").strip().upper()
    if status not in ELIGIBILITY_STATUSES:
        raise LegalError("INVALID_STATUS", f"status must be one of {', '.join(ELIGIBILITY_STATUSES)}")
    ref = str(determination_ref or "").strip() or None
    if status != "UNDETERMINED" and not ref:
        raise LegalError("DETERMINATION_REF_REQUIRED",
                         "a determination other than UNDETERMINED needs a reference to the legal determination")
    now_iso = _now_iso(now)
    with _connection(conn_or_db) as conn:
        before = _fetch_dict(conn, "SELECT * FROM legal_country_eligibility WHERE country_code = ?", (code,))
        conn.execute(
            """INSERT INTO legal_country_eligibility (country_code, status, determination_ref, determined_by,
                   determined_at, notes) VALUES (?, ?, ?, ?, ?, ?)
               ON CONFLICT(country_code) DO UPDATE SET status = excluded.status,
                   determination_ref = excluded.determination_ref, determined_by = excluded.determined_by,
                   determined_at = excluded.determined_at, notes = excluded.notes""",
            (code, status, ref, str(determined_by), now_iso, (notes or "").strip()[:1000] or None),
        )
        audit(conn, "country_eligibility_recorded", actor_type="operator", actor_id=str(determined_by),
              category="COUNTRY_ELIGIBILITY",
              details={"country_code": code, "status": status, "determination_ref": ref,
                       "previous_status": before["status"] if before else "UNDETERMINED"}, now=now_iso)
    return eligibility(conn_or_db, code)


def list_eligibility(conn_or_db) -> List[Dict[str, Any]]:
    with _connection(conn_or_db) as conn:
        return _fetch_all(conn, "SELECT * FROM legal_country_eligibility ORDER BY country_code")


__all__ = [
    "DRAFT", "APPROVED", "REQUIRED_FOR", "WITHDRAWAL_EFFECTS", "LegalError",
    "content_sha256", "evidence_hash", "public_view", "get_document", "current_document", "current_documents",
    "list_versions", "publish", "record_counsel_review", "acceptance_status", "has_current_acceptance",
    "required_reconsent", "accept", "record_registration_acceptances", "withdraw", "user_acceptances",
    "eligibility", "record_eligibility", "list_eligibility",
]
