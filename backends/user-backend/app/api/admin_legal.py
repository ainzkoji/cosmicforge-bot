"""Admin legal operations: /api/admin/legal (Section H Step 8.2).

Administrators only (``require_admin``). Publishing creates a new immutable
DRAFT version; counsel approval is a separate, explicit step that records the
reviewer's name and a reference to the review. Country determinations need a
reference to the determination. Every call lands in ``legal_audit_log``.
"""
from __future__ import annotations

from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from app.core.deps import require_admin
from shared_lib.legal import documents as legal
from shared_lib.persistence.db import DB

router = APIRouter(prefix="/admin/legal", tags=["Admin Legal"])


def _db() -> DB:
    return DB()


def _error(exc: legal.LegalError) -> HTTPException:
    status = {"DOCUMENT_NOT_FOUND": 404, "ALREADY_APPROVED": 409, "DUPLICATE_CONTENT": 409}.get(exc.code, 400)
    return HTTPException(status_code=status, detail={"error_code": exc.code, "message": str(exc), **exc.details})


class PublishRequest(BaseModel):
    category: str = Field(min_length=3, max_length=64)
    locale: str = Field("en", min_length=2, max_length=16)
    title: str = Field(min_length=3, max_length=300)
    body: str = Field(min_length=20, max_length=500_000)
    material_change: bool = True
    change_summary: Optional[str] = Field(None, max_length=2000)


class ReviewRequest(BaseModel):
    reviewer_name: str = Field(min_length=2, max_length=200)
    reviewer_reference: str = Field(min_length=2, max_length=300)


class EligibilityRequest(BaseModel):
    country_code: str = Field(min_length=2, max_length=2)
    status: str = Field(pattern="^(UNDETERMINED|PERMITTED|RESTRICTED|PROHIBITED)$")
    determination_ref: Optional[str] = Field(None, max_length=300)
    notes: Optional[str] = Field(None, max_length=1000)


def _admin_view(doc: Dict[str, Any]) -> Dict[str, Any]:
    return {**legal.public_view(doc), "reviewer_name": doc.get("reviewer_name"),
            "reviewer_reference": doc.get("reviewer_reference"), "review_recorded_by": doc.get("review_recorded_by"),
            "published_by": doc.get("published_by"), "supersedes_doc_id": doc.get("supersedes_doc_id")}


@router.get("/documents")
def list_documents(category: Optional[str] = Query(None), admin: dict = Depends(require_admin)) -> Dict[str, Any]:
    try:
        return {"documents": [_admin_view(d) for d in legal.list_versions(_db(), category)]}
    except legal.LegalError as exc:
        raise _error(exc)


@router.post("/documents", status_code=201)
def publish_document(body: PublishRequest, admin: dict = Depends(require_admin)) -> Dict[str, Any]:
    """Publish a new version (always DRAFT_PENDING_COUNSEL; it supersedes the current one)."""
    try:
        doc = legal.publish(_db(), category=body.category, locale=body.locale, title=body.title, body=body.body,
                            material_change=body.material_change, change_summary=body.change_summary,
                            published_by=str(admin["id"]))
    except legal.LegalError as exc:
        raise _error(exc)
    return _admin_view(doc)


@router.post("/documents/{doc_id}/review")
def record_counsel_review(doc_id: str, body: ReviewRequest, admin: dict = Depends(require_admin)) -> Dict[str, Any]:
    """Record that counsel approved this exact version (name + review reference required)."""
    try:
        doc = legal.record_counsel_review(_db(), doc_id, reviewer_name=body.reviewer_name,
                                          reviewer_reference=body.reviewer_reference, recorded_by=str(admin["id"]))
    except legal.LegalError as exc:
        raise _error(exc)
    return _admin_view(doc)


@router.get("/country-eligibility")
def list_country_eligibility(admin: dict = Depends(require_admin)) -> Dict[str, Any]:
    return {"default_status": "UNDETERMINED", "determinations": legal.list_eligibility(_db())}


@router.post("/country-eligibility")
def record_country_eligibility(body: EligibilityRequest, admin: dict = Depends(require_admin)) -> Dict[str, Any]:
    try:
        return legal.record_eligibility(_db(), body.country_code, body.status,
                                        determination_ref=body.determination_ref, determined_by=str(admin["id"]),
                                        notes=body.notes)
    except legal.LegalError as exc:
        raise _error(exc)


__all__ = ["router"]
