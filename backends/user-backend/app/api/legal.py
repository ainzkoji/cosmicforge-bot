"""Legal documents and consent: /api/legal (Section H Step 8.2).

* ``GET  /api/legal/documents``          public: current published versions
* ``GET  /api/legal/documents/{doc_id}`` public: one version
* ``GET  /api/legal/eligibility/{cc}``   public: recorded country determination
* ``GET  /api/legal/status``             auth: pending (re)consent, own acceptances
* ``POST /api/legal/accept``             auth: accept exact document versions
* ``POST /api/legal/withdraw``           auth: withdraw a category, effects reported

A document is shown with ``is_draft = true`` until counsel approval has been
recorded through the admin API; nothing here can mark a document approved.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel, Field

from app.api.auth import get_current_active_user
from shared_lib.legal import documents as legal
from shared_lib.persistence.db import DB

router = APIRouter(prefix="/legal", tags=["Legal"])


def _db() -> DB:
    return DB()


def _error(exc: legal.LegalError) -> HTTPException:
    status = {"UNKNOWN_DOCUMENT": 404, "DOCUMENT_NOT_FOUND": 404, "SUPERSEDED": 409,
              "CONTENT_HASH_MISMATCH": 409, "NOT_ACCEPTED": 409}.get(exc.code, 400)
    return HTTPException(status_code=status, detail={"error_code": exc.code, "message": str(exc), **exc.details})


class AcceptItem(BaseModel):
    doc_id: str = Field(min_length=1, max_length=200)
    content_sha256: str = Field(min_length=64, max_length=64)


class AcceptRequest(BaseModel):
    documents: List[AcceptItem] = Field(min_length=1, max_length=20)


class WithdrawRequest(BaseModel):
    category: str = Field(min_length=3, max_length=64)
    reason: Optional[str] = Field(None, max_length=500)


def _client_ip(request: Request) -> Optional[str]:
    # Stored only as a keyed hash (evidence that the acceptance came from a
    # session at this address); never used for authorization.
    return request.client.host if request.client else None


@router.get("/documents")
def get_current_documents(locale: Optional[str] = Query(None, max_length=16),
                          category: Optional[List[str]] = Query(None),
                          purpose: Optional[str] = Query(None, max_length=40)) -> Dict[str, Any]:
    """Current published versions. ``purpose=registration`` limits the list to
    what sign-up asks for and reports which of those are not published yet."""
    categories = category
    required: List[str] = []
    if purpose:
        if purpose not in legal.REQUIRED_FOR:
            raise HTTPException(400, detail={"error_code": "UNKNOWN_PURPOSE", "message": "unknown purpose"})
        required = list(legal.REQUIRED_FOR[purpose])
        categories = required
    try:
        docs = legal.current_documents(_db(), locale, categories)
    except legal.LegalError as exc:
        raise _error(exc)
    published = {d["category"] for d in docs}
    return {
        "documents": [legal.public_view(d) for d in docs],
        "purpose": purpose,
        "unpublished": [c for c in required if c not in published],
    }


@router.get("/documents/{doc_id}")
def get_document_version(doc_id: str) -> Dict[str, Any]:
    doc = legal.get_document(_db(), doc_id)
    if doc is None:
        raise HTTPException(404, detail={"error_code": "DOCUMENT_NOT_FOUND", "message": "legal document not found"})
    return legal.public_view(doc)


@router.get("/eligibility/{country_code}")
def get_country_eligibility(country_code: str) -> Dict[str, Any]:
    result = legal.eligibility(_db(), country_code)
    return {k: result.get(k) for k in ("country_code", "status", "determined_at", "valid_country_code")}


@router.get("/status")
def get_legal_status(locale: Optional[str] = Query(None, max_length=16),
                     purpose: List[str] = Query(["registration"]),
                     current_user: dict = Depends(get_current_active_user)) -> Dict[str, Any]:
    purposes = [p for p in purpose if p in legal.REQUIRED_FOR] or ["registration"]
    db = _db()
    status = legal.required_reconsent(db, current_user["id"], locale=locale, purposes=purposes)
    return {**status, "purposes": purposes, "acceptances": legal.user_acceptances(db, current_user["id"])}


@router.post("/accept")
def accept_documents(body: AcceptRequest, request: Request,
                     current_user: dict = Depends(get_current_active_user)) -> Dict[str, Any]:
    try:
        rows = legal.accept(_db(), current_user["id"], [item.model_dump() for item in body.documents],
                            ip=_client_ip(request), user_agent=request.headers.get("user-agent"), source="web")
    except legal.LegalError as exc:
        raise _error(exc)
    return {"accepted": [{k: r[k] for k in ("doc_id", "category", "version", "content_sha256", "accepted_at")}
                         for r in rows]}


@router.post("/withdraw")
def withdraw_consent(body: WithdrawRequest, current_user: dict = Depends(get_current_active_user)) -> Dict[str, Any]:
    try:
        return legal.withdraw(_db(), current_user["id"], body.category, reason=body.reason)
    except legal.LegalError as exc:
        raise _error(exc)


__all__ = ["router"]
