"""Support desk API (Section H Step 8.5).

User (``get_current_active_user``)::

    GET  /api/support/categories
    POST /api/support/tickets                    {category, subject, message}
    GET  /api/support/tickets                    own tickets only
    GET  /api/support/tickets/{ticket_id}        404 for another user's ticket
    POST /api/support/tickets/{ticket_id}/replies {message}

Admin (``require_admin``)::

    GET  /api/admin/support/tickets?status=&assigned_to=&unassigned=
    GET  /api/admin/support/tickets/{ticket_id}
    GET  /api/admin/support/assignees
    POST /api/admin/support/tickets/{ticket_id}/assign   {assignee_id | null}
    POST /api/admin/support/tickets/{ticket_id}/replies  {message}
    POST /api/admin/support/tickets/{ticket_id}/close    {reason}

A message that looks like an API key, password or other secret is refused with 422 ``SECRET_DETECTED`` and is
never stored. Attachments are not accepted.
"""
from __future__ import annotations

from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from app.api.auth import get_current_active_user
from app.core import support_service as support
from app.core.deps import require_admin
from shared_lib.persistence.db import DB
from shared_lib.persistence.support_schema import TICKET_CATEGORIES

router = APIRouter(prefix="/support", tags=["Support"])
admin_router = APIRouter(prefix="/admin/support", tags=["Admin Support"])


def _db() -> DB:
    return DB()


def _error(exc: support.SupportError) -> HTTPException:
    return HTTPException(status_code=exc.status, detail={"error_code": exc.code, "message": exc.message, **exc.details})


class TicketCreate(BaseModel):
    category: str = Field(min_length=2, max_length=40)
    subject: str = Field(min_length=1, max_length=support.MAX_SUBJECT)
    message: str = Field(min_length=1, max_length=support.MAX_MESSAGE)


class Reply(BaseModel):
    message: str = Field(min_length=1, max_length=support.MAX_MESSAGE)


class Assign(BaseModel):
    assignee_id: Optional[str] = Field(None, max_length=100)


class Close(BaseModel):
    reason: str = Field(min_length=1, max_length=1000)


@router.get("/categories")
def categories(current_user: dict = Depends(get_current_active_user)) -> Dict[str, Any]:
    return {"categories": list(TICKET_CATEGORIES)}


@router.post("/tickets", status_code=201)
def create_ticket(body: TicketCreate, current_user: dict = Depends(get_current_active_user)) -> Dict[str, Any]:
    try:
        return support.create_ticket(_db(), str(current_user["id"]), category=body.category, subject=body.subject,
                                     message=body.message)
    except support.SupportError as exc:
        raise _error(exc)


@router.get("/tickets")
def list_tickets(current_user: dict = Depends(get_current_active_user)) -> Dict[str, Any]:
    return {"tickets": support.list_user_tickets(_db(), str(current_user["id"]))}


@router.get("/tickets/{ticket_id}")
def get_ticket(ticket_id: str, current_user: dict = Depends(get_current_active_user)) -> Dict[str, Any]:
    try:
        return support.get_user_ticket(_db(), str(current_user["id"]), ticket_id)
    except support.SupportError as exc:
        raise _error(exc)


@router.post("/tickets/{ticket_id}/replies")
def reply(ticket_id: str, body: Reply, current_user: dict = Depends(get_current_active_user)) -> Dict[str, Any]:
    try:
        return support.user_reply(_db(), str(current_user["id"]), ticket_id, body.message)
    except support.SupportError as exc:
        raise _error(exc)


# ── admin ───────────────────────────────────────────────────────────────────────────────────────────────
@admin_router.get("/tickets")
def admin_list(status: Optional[str] = Query(None, max_length=32), assigned_to: Optional[str] = Query(None),
               unassigned: bool = Query(False), admin: dict = Depends(require_admin)) -> Dict[str, Any]:
    try:
        return {"tickets": support.admin_list(_db(), status=status, assigned_to=assigned_to, unassigned=unassigned)}
    except support.SupportError as exc:
        raise _error(exc)


@admin_router.get("/assignees")
def admin_assignees(admin: dict = Depends(require_admin)) -> Dict[str, Any]:
    return {"assignees": support.assignable_admins(_db())}


@admin_router.get("/tickets/{ticket_id}")
def admin_get(ticket_id: str, admin: dict = Depends(require_admin)) -> Dict[str, Any]:
    try:
        return support.admin_get(_db(), ticket_id)
    except support.SupportError as exc:
        raise _error(exc)


@admin_router.post("/tickets/{ticket_id}/assign")
def admin_assign(ticket_id: str, body: Assign, admin: dict = Depends(require_admin)) -> Dict[str, Any]:
    try:
        return support.admin_assign(_db(), ticket_id, admin_id=str(admin["id"]), assignee_id=body.assignee_id)
    except support.SupportError as exc:
        raise _error(exc)


@admin_router.post("/tickets/{ticket_id}/replies")
def admin_reply(ticket_id: str, body: Reply, admin: dict = Depends(require_admin)) -> Dict[str, Any]:
    try:
        return support.admin_reply(_db(), ticket_id, admin_id=str(admin["id"]), message=body.message)
    except support.SupportError as exc:
        raise _error(exc)


@admin_router.post("/tickets/{ticket_id}/close")
def admin_close(ticket_id: str, body: Close, admin: dict = Depends(require_admin)) -> Dict[str, Any]:
    try:
        return support.admin_close(_db(), ticket_id, admin_id=str(admin["id"]), reason=body.reason)
    except support.SupportError as exc:
        raise _error(exc)
