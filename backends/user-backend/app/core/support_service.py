"""Support desk (Section H Step 8.5).

Users open tickets and talk to support; administrators list, assign, reply and close. Rules enforced here:

* **Tenant isolation** -- every user-side read and write is ``WHERE user_id = <caller>``. Another user's ticket is
  reported as not found (never "forbidden"), so ticket ids cannot be probed.
* **No secrets** -- a subject or message that looks like an API key, secret, password, private key or token is
  refused before it is stored (``SecretInMessage``), with a message telling the user to revoke the key if it was
  shared anywhere. Administrators' replies are checked the same way: support never asks for or sends a secret.
* **History** -- messages and events are append-only in the database.
* **User view** -- a user sees the conversation and the status; never which administrator is assigned or the
  administrator ids of replies (shown as "Support").
"""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from shared_lib.operations.secrets_scan import find_secrets
from shared_lib.persistence.support_schema import TICKET_CATEGORIES, TICKET_STATUSES

MAX_SUBJECT = 200
MAX_MESSAGE = 10_000
MAX_OPEN_TICKETS_PER_USER = 10
SECRET_REFUSAL_MESSAGE = (
    "Your message looks like it contains an API key, secret, password or token. Never share secrets with "
    "support -- we will never ask for them. Remove it and send your message again. If you pasted a real key "
    "anywhere, revoke it at your exchange now and create a new one.")


class SupportError(Exception):
    def __init__(self, code: str, message: str, status: int = 400, **details: Any) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.status = status
        self.details = details


class SecretInMessage(SupportError):
    def __init__(self, kinds: List[str]) -> None:
        super().__init__("SECRET_DETECTED", SECRET_REFUSAL_MESSAGE, 422, kinds=kinds)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _check_text(*texts: str) -> None:
    kinds = sorted({k for t in texts for k in find_secrets(t)})
    if kinds:
        raise SecretInMessage(kinds)


def _event(conn, ticket_id: str, event: str, actor_kind: str, actor_id: str, detail: Optional[dict] = None,
           at: Optional[str] = None) -> None:
    conn.execute("INSERT INTO support_ticket_events (ticket_id, event, actor_kind, actor_id, detail, at) "
                 "VALUES (?, ?, ?, ?, ?, ?)", (ticket_id, event, actor_kind, actor_id,
                                               json.dumps(detail or {}, sort_keys=True), at or _now()))


def _ticket(row) -> Dict[str, Any]:
    return dict(row)


def _messages(conn, ticket_id: str) -> List[Dict[str, Any]]:
    return [dict(r) for r in conn.execute(
        "SELECT message_id, author_kind, author_id, body, created_at FROM support_ticket_messages "
        "WHERE ticket_id = ? ORDER BY created_at, message_id", (ticket_id,)).fetchall()]


def user_view(ticket: Dict[str, Any], messages: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
    out = {k: ticket[k] for k in ("ticket_id", "category", "subject", "status", "created_at", "updated_at",
                                  "closed_at")}
    out["assigned"] = bool(ticket.get("assigned_admin_id"))
    if messages is not None:
        out["messages"] = [{"message_id": m["message_id"], "from": "YOU" if m["author_kind"] == "USER" else "SUPPORT",
                            "body": m["body"], "created_at": m["created_at"]} for m in messages]
    return out


def _validate_new(category: str, subject: str, message: str) -> tuple:
    category = (category or "").strip().upper()
    subject, message = (subject or "").strip(), (message or "").strip()
    if category not in TICKET_CATEGORIES:
        raise SupportError("INVALID_CATEGORY", f"category must be one of {', '.join(TICKET_CATEGORIES)}")
    if not subject or len(subject) > MAX_SUBJECT:
        raise SupportError("INVALID_SUBJECT", f"subject must be 1-{MAX_SUBJECT} characters")
    if not message or len(message) > MAX_MESSAGE:
        raise SupportError("INVALID_MESSAGE", f"message must be 1-{MAX_MESSAGE} characters")
    _check_text(subject, message)
    return category, subject, message


# ── user side ───────────────────────────────────────────────────────────────────────────────────────────
def create_ticket(db, user_id: str, *, category: str, subject: str, message: str) -> Dict[str, Any]:
    category, subject, message = _validate_new(category, subject, message)
    ticket_id, now = uuid.uuid4().hex, _now()
    with db.connect() as conn:
        open_count = conn.execute("SELECT COUNT(*) FROM support_tickets WHERE user_id = ? AND status != 'CLOSED'",
                                  (user_id,)).fetchone()[0]
        if open_count >= MAX_OPEN_TICKETS_PER_USER:
            raise SupportError("TOO_MANY_OPEN_TICKETS",
                               f"You already have {open_count} open tickets; reply to one of them instead.", 429)
        conn.execute("INSERT INTO support_tickets (ticket_id, user_id, category, subject, status, created_at, "
                     "updated_at) VALUES (?, ?, ?, ?, 'OPEN', ?, ?)", (ticket_id, user_id, category, subject, now, now))
        conn.execute("INSERT INTO support_ticket_messages (message_id, ticket_id, author_kind, author_id, body, "
                     "created_at) VALUES (?, ?, 'USER', ?, ?, ?)", (uuid.uuid4().hex, ticket_id, user_id, message, now))
        _event(conn, ticket_id, "OPENED", "USER", user_id, {"category": category}, now)
    return get_user_ticket(db, user_id, ticket_id)


def list_user_tickets(db, user_id: str) -> List[Dict[str, Any]]:
    with db.connect() as conn:
        rows = conn.execute("SELECT * FROM support_tickets WHERE user_id = ? ORDER BY updated_at DESC",
                            (user_id,)).fetchall()
    return [user_view(_ticket(r)) for r in rows]


def _owned(conn, user_id: str, ticket_id: str) -> Dict[str, Any]:
    row = conn.execute("SELECT * FROM support_tickets WHERE ticket_id = ? AND user_id = ?",
                       (ticket_id, user_id)).fetchone()
    if row is None:
        raise SupportError("TICKET_NOT_FOUND", "ticket not found", 404)
    return _ticket(row)


def get_user_ticket(db, user_id: str, ticket_id: str) -> Dict[str, Any]:
    with db.connect() as conn:
        ticket = _owned(conn, user_id, ticket_id)
        return user_view(ticket, _messages(conn, ticket_id))


def user_reply(db, user_id: str, ticket_id: str, message: str) -> Dict[str, Any]:
    message = (message or "").strip()
    if not message or len(message) > MAX_MESSAGE:
        raise SupportError("INVALID_MESSAGE", f"message must be 1-{MAX_MESSAGE} characters")
    _check_text(message)
    now = _now()
    with db.connect() as conn:
        ticket = _owned(conn, user_id, ticket_id)
        if ticket["status"] == "CLOSED":
            raise SupportError("TICKET_CLOSED", "This ticket is closed. Please open a new ticket.", 409)
        conn.execute("INSERT INTO support_ticket_messages (message_id, ticket_id, author_kind, author_id, body, "
                     "created_at) VALUES (?, ?, 'USER', ?, ?, ?)", (uuid.uuid4().hex, ticket_id, user_id, message, now))
        new_status = "OPEN" if ticket["status"] == "WAITING_ON_CUSTOMER" else ticket["status"]
        conn.execute("UPDATE support_tickets SET status = ?, updated_at = ? WHERE ticket_id = ?",
                     (new_status, now, ticket_id))
        _event(conn, ticket_id, "USER_REPLIED", "USER", user_id, {"status": new_status}, now)
    return get_user_ticket(db, user_id, ticket_id)


# ── admin side ──────────────────────────────────────────────────────────────────────────────────────────
def admin_list(db, *, status: Optional[str] = None, assigned_to: Optional[str] = None,
               unassigned: bool = False, limit: int = 200) -> List[Dict[str, Any]]:
    sql = ("SELECT t.*, u.email AS user_email, (SELECT COUNT(*) FROM support_ticket_messages m "
           "WHERE m.ticket_id = t.ticket_id) AS message_count FROM support_tickets t "
           "LEFT JOIN users u ON u.id = t.user_id WHERE 1=1")
    params: List[Any] = []
    if status:
        if status not in TICKET_STATUSES:
            raise SupportError("INVALID_STATUS", "unknown status")
        sql += " AND t.status = ?"
        params.append(status)
    if assigned_to:
        sql += " AND t.assigned_admin_id = ?"
        params.append(assigned_to)
    if unassigned:
        sql += " AND t.assigned_admin_id IS NULL"
    sql += " ORDER BY CASE t.status WHEN 'OPEN' THEN 0 WHEN 'IN_PROGRESS' THEN 1 WHEN 'WAITING_ON_CUSTOMER' THEN 2 " \
           "ELSE 3 END, t.updated_at DESC LIMIT ?"
    params.append(int(limit))
    with db.connect() as conn:
        return [dict(r) for r in conn.execute(sql, params).fetchall()]


def _any(conn, ticket_id: str) -> Dict[str, Any]:
    row = conn.execute("SELECT * FROM support_tickets WHERE ticket_id = ?", (ticket_id,)).fetchone()
    if row is None:
        raise SupportError("TICKET_NOT_FOUND", "ticket not found", 404)
    return _ticket(row)


def admin_get(db, ticket_id: str) -> Dict[str, Any]:
    with db.connect() as conn:
        ticket = _any(conn, ticket_id)
        user = conn.execute("SELECT email FROM users WHERE id = ?", (ticket["user_id"],)).fetchone()
        events = [dict(r) for r in conn.execute(
            "SELECT id, event, actor_kind, actor_id, detail, at FROM support_ticket_events WHERE ticket_id = ? "
            "ORDER BY id", (ticket_id,)).fetchall()]
        messages = _messages(conn, ticket_id)
    return {**ticket, "user_email": user["email"] if user else None, "messages": messages, "events": events}


def assignable_admins(db) -> List[Dict[str, Any]]:
    with db.connect() as conn:
        cols = {r[1] for r in conn.execute("PRAGMA table_info(admins)").fetchall()}
        name = "COALESCE(full_name, email)" if "full_name" in cols else "email"
        return [dict(r) for r in conn.execute(
            f"SELECT id, email, {name} AS display_name FROM admins WHERE is_active = 1 ORDER BY email").fetchall()]


def admin_assign(db, ticket_id: str, *, admin_id: str, assignee_id: Optional[str]) -> Dict[str, Any]:
    now = _now()
    with db.connect() as conn:
        ticket = _any(conn, ticket_id)
        if ticket["status"] == "CLOSED":
            raise SupportError("TICKET_CLOSED", "ticket is closed", 409)
        if assignee_id is not None:
            row = conn.execute("SELECT is_active FROM admins WHERE id = ?", (assignee_id,)).fetchone()
            if row is None or row["is_active"] != 1:
                raise SupportError("ASSIGNEE_NOT_AN_ACTIVE_ADMIN", "assignee must be an active administrator")
        status = "IN_PROGRESS" if assignee_id and ticket["status"] == "OPEN" else ticket["status"]
        conn.execute("UPDATE support_tickets SET assigned_admin_id = ?, status = ?, updated_at = ? WHERE ticket_id = ?",
                     (assignee_id, status, now, ticket_id))
        _event(conn, ticket_id, "ASSIGNED" if assignee_id else "UNASSIGNED", "ADMIN", admin_id,
               {"assignee_id": assignee_id}, now)
    return admin_get(db, ticket_id)


def admin_reply(db, ticket_id: str, *, admin_id: str, message: str) -> Dict[str, Any]:
    message = (message or "").strip()
    if not message or len(message) > MAX_MESSAGE:
        raise SupportError("INVALID_MESSAGE", f"message must be 1-{MAX_MESSAGE} characters")
    _check_text(message)
    now = _now()
    with db.connect() as conn:
        ticket = _any(conn, ticket_id)
        if ticket["status"] == "CLOSED":
            raise SupportError("TICKET_CLOSED", "ticket is closed", 409)
        conn.execute("INSERT INTO support_ticket_messages (message_id, ticket_id, author_kind, author_id, body, "
                     "created_at) VALUES (?, ?, 'ADMIN', ?, ?, ?)", (uuid.uuid4().hex, ticket_id, admin_id, message, now))
        conn.execute("UPDATE support_tickets SET status = 'WAITING_ON_CUSTOMER', updated_at = ?, "
                     "assigned_admin_id = COALESCE(assigned_admin_id, ?) WHERE ticket_id = ?", (now, admin_id, ticket_id))
        _event(conn, ticket_id, "ADMIN_REPLIED", "ADMIN", admin_id, None, now)
    return admin_get(db, ticket_id)


def admin_close(db, ticket_id: str, *, admin_id: str, reason: str) -> Dict[str, Any]:
    reason = (reason or "").strip()
    if not reason:
        raise SupportError("CLOSE_REASON_REQUIRED", "a reason is required to close a ticket")
    _check_text(reason)
    now = _now()
    with db.connect() as conn:
        ticket = _any(conn, ticket_id)
        if ticket["status"] == "CLOSED":
            raise SupportError("TICKET_CLOSED", "ticket is already closed", 409)
        conn.execute("UPDATE support_tickets SET status = 'CLOSED', closed_at = ?, closed_by = ?, close_reason = ?, "
                     "updated_at = ? WHERE ticket_id = ?", (now, admin_id, reason[:1000], now, ticket_id))
        _event(conn, ticket_id, "CLOSED", "ADMIN", admin_id, {"reason": reason[:1000]}, now)
    return admin_get(db, ticket_id)
