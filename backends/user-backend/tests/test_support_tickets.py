"""Section H Step 8.5: support desk -- ticket intake, tenant isolation, admin workflow, secret refusal."""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "backends" / "shared"))
sys.path.insert(0, str(ROOT / "backends" / "user-backend"))

from app.api import support as support_api  # noqa: E402
from app.api.auth import get_current_active_user  # noqa: E402
from app.core import support_service as support  # noqa: E402
from app.core.deps import require_admin  # noqa: E402
from shared_lib.persistence.db import DB  # noqa: E402
from shared_lib.persistence.migrations import migrate  # noqa: E402

NOW = "2026-10-17T12:00:00+00:00"


def _db(tmp_path: Path) -> DB:
    path = tmp_path / "support.db"
    migrate(str(path))
    db = DB(path=str(path))
    with db.connect() as conn:
        for uid in ("user-1", "user-2"):
            conn.execute("INSERT INTO users (id, email, hashed_password, status, created_at, updated_at) "
                         "VALUES (?, ?, 'x', 'active', ?, ?)", (uid, f"{uid}@example.test", NOW, NOW))
        cols = {r[1] for r in conn.execute("PRAGMA table_info(admins)").fetchall()}
        base = {"id": None, "email": None, "hashed_password": "x", "is_active": 1, "created_at": NOW,
                "updated_at": NOW, "role": "admin"}
        for aid, active in (("admin-1", 1), ("admin-2", 1), ("admin-off", 0)):
            row = {**base, "id": aid, "email": f"{aid}@example.test", "is_active": active}
            row = {k: v for k, v in row.items() if k in cols}
            conn.execute(f"INSERT INTO admins ({', '.join(row)}) VALUES ({', '.join('?' * len(row))})",
                         tuple(row.values()))
    return db


@pytest.fixture
def api(tmp_path, monkeypatch):
    db = _db(tmp_path)
    monkeypatch.setattr(support_api, "_db", lambda: db)
    app = FastAPI()
    app.include_router(support_api.router, prefix="/api")
    app.include_router(support_api.admin_router, prefix="/api")
    state = {"user": "user-1", "admin": "admin-1"}
    app.dependency_overrides[get_current_active_user] = lambda: {"id": state["user"], "status": "active"}
    app.dependency_overrides[require_admin] = lambda: {"id": state["admin"], "email": "a@example.test"}
    return SimpleNamespace(client=TestClient(app), db=db, state=state, app=app)


def _open(api, **kw):
    body = {"category": "BROKER_CONNECTION", "subject": "Cannot connect my exchange",
            "message": "The connection test fails with an error message about permissions."}
    body.update(kw)
    return api.client.post("/api/support/tickets", json=body)


def test_user_creates_and_reads_own_ticket(api):
    r = _open(api)
    assert r.status_code == 201, r.text
    t = r.json()
    assert t["status"] == "OPEN" and t["category"] == "BROKER_CONNECTION" and t["assigned"] is False
    assert [m["from"] for m in t["messages"]] == ["YOU"]
    listed = api.client.get("/api/support/tickets").json()["tickets"]
    assert [x["ticket_id"] for x in listed] == [t["ticket_id"]]
    assert "user_id" not in t and "assigned_admin_id" not in t


def test_tenant_isolation(api):
    t = _open(api).json()
    api.state["user"] = "user-2"
    assert api.client.get("/api/support/tickets").json()["tickets"] == []
    r = api.client.get(f"/api/support/tickets/{t['ticket_id']}")
    assert r.status_code == 404 and r.json()["detail"]["error_code"] == "TICKET_NOT_FOUND"
    r = api.client.post(f"/api/support/tickets/{t['ticket_id']}/replies", json={"message": "let me in"})
    assert r.status_code == 404
    with api.db.connect() as conn:
        assert conn.execute("SELECT COUNT(*) FROM support_ticket_messages").fetchone()[0] == 1


@pytest.mark.parametrize("message", [
    "my api key: AbCdEfGhIjKlMnOpQrStUvWxYz0123456789AbCdEfGhIjKlMnOpQrStUvWx",
    "secret=Zx81kLmQ09vbNmasdq",
    "password is hunter2hunter2",
    "-----BEGIN RSA PRIVATE KEY----- MIIEpAIBAAKCAQEA",
    "here you go vmPUZE6mv9SD5VNHk4HlWFsOr6aKE2zvsw0MuIgwCIPy6utIco14y7Ju91duEh8A",
    "AKIAABCDEFGHIJKLMNOP",
])
def test_messages_with_secrets_are_refused_and_not_stored(api, message):
    r = _open(api, message=message)
    assert r.status_code == 422
    detail = r.json()["detail"]
    assert detail["error_code"] == "SECRET_DETECTED" and "revoke" in detail["message"]
    assert message not in r.text
    with api.db.connect() as conn:
        assert conn.execute("SELECT COUNT(*) FROM support_tickets").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM support_ticket_messages").fetchone()[0] == 0


def test_ordinary_messages_are_accepted(api):
    for msg in ("I forgot my password and the reset email never arrived.",
                "Order 1234567890 on BTCUSDT was not filled; tx 0x" + "ab12" * 16,
                "My api key permissions: trading enabled, withdrawals disabled."):
        assert _open(api, message=msg).status_code == 201, msg


def test_secret_in_reply_and_subject_refused(api):
    t = _open(api).json()
    r = api.client.post(f"/api/support/tickets/{t['ticket_id']}/replies",
                        json={"message": "api_secret=Q2xvdWRTZWNyZXQxMjM0NTY3"})
    assert r.status_code == 422
    assert _open(api, subject="token: ghp_abcdefghijklmnopqrstuvwxyz0123").status_code == 422
    r = api.client.post(f"/api/admin/support/tickets/{t['ticket_id']}/replies",
                        json={"message": "please send your api key=XyZ1234567890abc"})
    assert r.status_code == 422


def test_admin_workflow_assign_reply_close(api):
    t = _open(api).json()
    tid = t["ticket_id"]
    listed = api.client.get("/api/admin/support/tickets").json()["tickets"]
    assert listed[0]["ticket_id"] == tid and listed[0]["user_email"] == "user-1@example.test"
    assignees = api.client.get("/api/admin/support/assignees").json()["assignees"]
    assert {a["id"] for a in assignees} == {"admin-1", "admin-2"}
    assert api.client.post(f"/api/admin/support/tickets/{tid}/assign",
                           json={"assignee_id": "admin-off"}).status_code == 400
    a = api.client.post(f"/api/admin/support/tickets/{tid}/assign", json={"assignee_id": "admin-2"}).json()
    assert a["assigned_admin_id"] == "admin-2" and a["status"] == "IN_PROGRESS"
    r = api.client.post(f"/api/admin/support/tickets/{tid}/replies",
                        json={"message": "Please check that trading permission is enabled on the key."})
    assert r.json()["status"] == "WAITING_ON_CUSTOMER"
    user_view = api.client.get(f"/api/support/tickets/{tid}").json()
    assert [m["from"] for m in user_view["messages"]] == ["YOU", "SUPPORT"]
    assert "admin-1" not in str(user_view) and "admin-2" not in str(user_view) and user_view["assigned"] is True
    r = api.client.post(f"/api/support/tickets/{tid}/replies", json={"message": "Enabled it, still failing."})
    assert r.json()["status"] == "OPEN"
    assert api.client.post(f"/api/admin/support/tickets/{tid}/close", json={"reason": " "}).status_code in (400, 422)
    closed = api.client.post(f"/api/admin/support/tickets/{tid}/close", json={"reason": "Resolved by user"}).json()
    assert closed["status"] == "CLOSED" and closed["closed_by"] == "admin-1"
    assert [e["event"] for e in closed["events"]] == ["OPENED", "ASSIGNED", "ADMIN_REPLIED", "USER_REPLIED", "CLOSED"]
    r = api.client.post(f"/api/support/tickets/{tid}/replies", json={"message": "one more thing"})
    assert r.status_code == 409 and r.json()["detail"]["error_code"] == "TICKET_CLOSED"
    assert len(api.client.get("/api/admin/support/tickets?status=CLOSED").json()["tickets"]) == 1


def test_messages_and_events_are_append_only(api):
    t = _open(api).json()
    for sql in ("UPDATE support_ticket_messages SET body='x'", "DELETE FROM support_ticket_messages",
                "UPDATE support_ticket_events SET actor_id='x'", "DELETE FROM support_ticket_events"):
        with pytest.raises(sqlite3.DatabaseError):
            with api.db.connect() as conn:
                conn.execute(sql)
    assert api.client.get(f"/api/support/tickets/{t['ticket_id']}").json()["messages"][0]["body"].startswith("The ")


def test_invalid_category_and_open_ticket_limit(api):
    assert _open(api, category="NONSENSE").status_code == 400
    for _ in range(support.MAX_OPEN_TICKETS_PER_USER):
        assert _open(api).status_code == 201
    r = _open(api)
    assert r.status_code == 429 and r.json()["detail"]["error_code"] == "TOO_MANY_OPEN_TICKETS"


def test_admin_endpoints_require_admin(api):
    api.app.dependency_overrides.pop(require_admin)
    assert api.client.get("/api/admin/support/tickets").status_code == 401


def test_migration_is_idempotent(tmp_path):
    path = tmp_path / "again.db"
    migrate(str(path))
    migrate(str(path))
    with sqlite3.connect(str(path)) as conn:
        names = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert {"support_tickets", "support_ticket_messages", "support_ticket_events"} <= names
