"""Admin governance proxy: /api/admin/governance/* -> bot-backend /api/v1/admin/governance/*.

* every route of the bot-backend router is proxied, and no other;
* admin only (no token, an ordinary user token and an end-user ``role=admin``
  token are all rejected, and nothing is sent upstream);
* the upstream call carries the emergency service token naming the operator;
* request bodies and upstream answers pass through unchanged -- a governance
  refusal (409 with its ``reason``) reaches the operator as the engine gave it;
* every write, and every read that did not succeed, is audited;
* a path parameter that is not a plain identifier never reaches the engine.
"""
from __future__ import annotations

import ast
import asyncio
import json
import re
import sys
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "backends" / "shared"))
sys.path.insert(0, str(ROOT / "backends" / "user-backend"))

from app.api import admin_emergency, admin_governance, proxy_utils  # noqa: E402
from app.core.deps import require_admin  # noqa: E402
from app.core.security import create_access_token, decode_admin_token, decode_token  # noqa: E402

ADMIN = {"id": "admin-test", "email": "ops@example.test", "role": "admin", "is_active": 1}
BASE = "/api/admin/governance"
UPSTREAM = "http://bot.test/api/v1/admin/governance"
BOT_ROUTER_SOURCE = ROOT / "backends" / "bot-backend" / "app" / "api" / "family_governance.py"

REASON = {"reason": "reviewed with the research lead"}
SWITCH = {"enabled": True, "reason": "incident 7"}
#: (method, path below the prefix, body, audit action) -- one row per route of the bot-backend router.
ROUTES = [
    ("GET", "/families", None, "families"),
    ("GET", "/families/DAILY_TREND", None, "family"),
    ("GET", "/history", None, "history"),
    ("GET", "/cycles", None, "cycles"),
    ("GET", "/shadow/divergences", None, "shadow_divergences"),
    ("POST", "/families/register", None, "families_register"),
    ("POST", "/families/DAILY_TREND/evidence",
     {"key": "section9_shadow_verified", "reference": "docs://shadow-week-1", "value": True}, "evidence"),
    ("POST", "/families/DAILY_TREND/promotion-requests", {"to_stage": "M1", **REASON}, "promotion_request"),
    ("POST", "/promotion-requests/fpr_0123456789abcdef01234567/approve", REASON, "promotion_approve"),
    ("POST", "/promotion-requests/fpr_0123456789abcdef01234567/reject", REASON, "promotion_reject"),
    ("POST", "/families/DAILY_TREND/revoke", {"to_stage": "M0", **REASON}, "promotion_revoke"),
    ("POST", "/families/DAILY_TREND/revoke-certification", REASON, "certification_revoke"),
    ("POST", "/families/DAILY_TREND/kill-switch", SWITCH, "family_kill_switch"),
    ("POST", "/families/DAILY_TREND/disable", SWITCH, "family_disable"),
    ("POST", "/accounts/brk_9f2c41aa07b1/kill-switch", SWITCH, "account_kill_switch"),
]
WRITES = [route for route in ROUTES if route[0] == "POST"]


class FakeUpstream:
    """Stands in for the pooled httpx client; records every outbound call."""

    def __init__(self, status_code=200, body=None, raises=None):
        self.status_code = status_code
        self.body = {} if body is None else body
        self.raises = raises
        self.calls = []
        self.audit = []

    async def request(self, **kwargs):
        self.calls.append(kwargs)
        if self.raises is not None:
            raise self.raises
        return SimpleNamespace(
            status_code=self.status_code,
            content=json.dumps(self.body).encode(),
            headers={"content-type": "application/json", "content-length": "1"},
        )


def _app(monkeypatch, upstream: FakeUpstream, *, admin: bool = True, capture_audit: bool = True) -> FastAPI:
    monkeypatch.setattr(proxy_utils, "_proxy_client", upstream)
    monkeypatch.setattr(proxy_utils, "BOT_BACKEND_BASE_URL", "http://bot.test")
    if capture_audit:
        # Durable audit rows are captured here instead of being written to a database.
        monkeypatch.setattr(admin_governance, "_write_audit",
                            lambda admin, action, details: upstream.audit.append((admin["id"], action, details)))
    app = FastAPI()
    app.include_router(admin_governance.router, prefix="/api")
    if admin:
        app.dependency_overrides[require_admin] = lambda: dict(ADMIN)
    return app


def _client(monkeypatch, upstream: FakeUpstream, **options) -> TestClient:
    return TestClient(_app(monkeypatch, upstream, **options))


def _send(client: TestClient, method: str, path: str, body=None, **kwargs):
    if body is not None:
        kwargs["json"] = body
    return client.request(method, f"{BASE}{path}", **kwargs)


def _served(app: FastAPI, method: str, path: str, body: bytes = b"{}"):
    """One request as an ASGI server hands it to the application: ``path`` is the request path after the
    server percent-decoded it ONCE. Returns (status, JSON body). No HTTP client is involved, so nothing
    normalises ``..`` or re-encodes the path on the way in."""
    sent = []

    async def receive():
        return {"type": "http.request", "body": body, "more_body": False}

    async def send(message):
        sent.append(message)

    scope = {"type": "http", "asgi": {"version": "3.0"}, "http_version": "1.1", "method": method, "scheme": "http",
             "path": path, "raw_path": path.encode("utf-8"), "query_string": b"", "root_path": "",
             "headers": [(b"content-type", b"application/json"), (b"content-length", str(len(body)).encode())],
             "client": ("testclient", 50000), "server": ("testserver", 80)}
    asyncio.run(app(scope, receive, send))
    payload = b"".join(message.get("body", b"") for message in sent if message["type"] == "http.response.body")
    return sent[0]["status"], json.loads(payload)


def _proxy_routes():
    return {(sorted(route.methods - {"HEAD"})[0], route.path[len("/admin/governance"):])
            for route in admin_governance.router.routes}


# ------------------------------------------------------------------------------------------ the route table
def test_the_route_table_of_this_file_is_the_proxy_router():
    def template(path):
        path = re.sub(r"^/families/DAILY_TREND", "/families/{family_id}", path)
        path = re.sub(r"^/promotion-requests/[^/]+", "/promotion-requests/{request_id}", path)
        return re.sub(r"^/accounts/[^/]+", "/accounts/{account_id}", path)

    assert {(method, template(path)) for method, path, _body, _action in ROUTES} == _proxy_routes()
    assert len(ROUTES) == len(admin_governance.router.routes) == 15


def test_every_bot_backend_governance_route_is_proxied_and_no_other():
    """Read from the bot-backend router's source (the two services cannot be imported into one process)."""
    if not BOT_ROUTER_SOURCE.is_file():
        pytest.skip("the bot-backend sources are not part of this checkout")
    source = BOT_ROUTER_SOURCE.read_text(encoding="utf-8")
    assert f'prefix="{admin_governance.UPSTREAM_PREFIX}"' in source
    upstream = {(method.upper(), path) for method, path in re.findall(r'@router\.(get|post|put|patch|delete)\("([^"]+)"', source)}
    assert upstream and upstream == _proxy_routes()


def test_the_router_is_registered_next_to_the_emergency_proxy():
    tree = ast.parse((ROOT / "backends" / "user-backend" / "app" / "main.py").read_text(encoding="utf-8"))
    imported = {alias.asname for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)
                and node.module == "app.api.admin_governance" for alias in node.names if alias.name == "router"}
    included = {call.args[0].id: {kw.arg: getattr(kw.value, "value", None) for kw in call.keywords}
                for call in ast.walk(tree) if isinstance(call, ast.Call) and isinstance(call.func, ast.Attribute)
                and call.func.attr == "include_router" and call.args and isinstance(call.args[0], ast.Name)}
    assert imported == {"admin_governance_router"}
    assert included["admin_governance_router"]["prefix"] == included["admin_emergency_router"]["prefix"] == "/api"


# ------------------------------------------------------------------------------------------- authorization
@pytest.mark.parametrize("method,path,body,_action", ROUTES)
def test_every_route_requires_an_admin_and_nothing_is_forwarded(monkeypatch, method, path, body, _action):
    upstream = FakeUpstream()
    client = _client(monkeypatch, upstream, admin=False)
    tokens = [create_access_token("normal-user", role="user"),
              # what an end-user account whose users.role is "admin" holds: not an admin-portal token
              create_access_token("end-user-admin", role="admin")]

    assert _send(client, method, path, body).status_code == 401
    for token in tokens:
        assert _send(client, method, path, body, headers={"Authorization": f"Bearer {token}"}).status_code == 401
    assert upstream.calls == []
    assert upstream.audit == []


def test_every_route_declares_the_admin_dependency():
    for route in admin_governance.router.routes:
        assert require_admin in {dependency.call for dependency in route.dependant.dependencies}, route.path


def test_the_upstream_call_carries_the_operator_service_token(monkeypatch):
    registry = {"families": [{"family_id": "DAILY_TREND", "governance_stage": "M0", "executable": False}],
                "executable": [], "log": {"ok": True, "records": 6, "head_hash": "ab" * 32}}
    upstream = FakeUpstream(200, registry)
    client = _client(monkeypatch, upstream)

    response = client.get(f"{BASE}/families", headers={"Authorization": "Bearer admin-portal-token"})

    assert response.status_code == 200
    assert response.json() == registry
    call = upstream.calls[0]
    assert (call["method"], call["url"], call["json"], call["params"]) == ("GET", f"{UPSTREAM}/families", None, None)
    assert call["timeout"] == admin_governance.READ_TIMEOUT_SECONDS
    scheme, _, token = call["headers"]["Authorization"].partition(" ")
    assert scheme == "Bearer"
    # The admin-portal token is not forwarded: a service token naming the verified operator is minted.
    assert token != "admin-portal-token" and decode_admin_token(token) is None
    claims = decode_token(token)
    assert claims is not None
    assert claims["type"] == "access" and claims["role"] == "admin" and claims["sub"] == ADMIN["id"]
    # What the bot-backend's require_admin_emergency demands: this ``act`` claim, issued for <= 120 s.
    assert claims["act"] == admin_emergency.EMERGENCY_ACTOR_CLAIM == "admin-emergency"
    assert 0 < claims["exp"] - claims["iat"] <= 120
    # A successful read is routine: log line only, no audit row.
    assert upstream.audit == []


@pytest.mark.parametrize("method,path,body,_action", ROUTES)
def test_every_route_forwards_to_its_own_upstream_path_with_a_fresh_token(monkeypatch, method, path, body, _action):
    upstream = FakeUpstream(200, {"event": {"event_id": "fev_1"}})
    client = _client(monkeypatch, upstream)

    first = _send(client, method, path, body)
    second = _send(client, method, path, body)

    assert first.status_code == second.status_code == 200
    assert first.json() == {"event": {"event_id": "fev_1"}}
    assert [(call["method"], call["url"], call["json"]) for call in upstream.calls] == [(method, f"{UPSTREAM}{path}", body)] * 2
    expected = admin_governance.READ_TIMEOUT_SECONDS if method == "GET" else admin_governance.WRITE_TIMEOUT_SECONDS
    assert all(call["timeout"] == expected for call in upstream.calls)
    claims = [decode_token(call["headers"]["Authorization"].partition(" ")[2]) for call in upstream.calls]
    assert all(c["sub"] == ADMIN["id"] and c["act"] == "admin-emergency" and 0 < c["exp"] - c["iat"] <= 120 for c in claims)
    # minted per call, never reused
    assert claims[0]["jti"] != claims[1]["jti"]


def test_the_operator_in_the_token_is_the_signed_in_admin_never_a_name_from_the_request(monkeypatch):
    upstream = FakeUpstream(200, {"event": {}})
    client = _client(monkeypatch, upstream)
    forged = {"reason": "looks fine", "operator_id": "someone-else", "reviewer": "someone-else", "sub": "someone-else"}

    client.post(f"{BASE}/promotion-requests/fpr_abc/approve", json=forged,
                headers={"Authorization": "Bearer admin-portal-token", "X-Admin-Id": "someone-else"})

    call = upstream.calls[0]
    assert decode_token(call["headers"]["Authorization"].partition(" ")[2])["sub"] == ADMIN["id"]
    assert set(call["headers"]) == {"Authorization"}


# ------------------------------------------------------------------------------- bodies and answers unchanged
def test_request_bodies_pass_through_unchanged(monkeypatch):
    upstream = FakeUpstream(200, {"event": {}})
    client = _client(monkeypatch, upstream)
    bodies = {
        "/families/DAILY_TREND/evidence": {"key": "certification_run_id", "reference": "run://2026-10-01", "value": {"run": 7}},
        "/families/DAILY_TREND/promotion-requests": {"to_stage": "M1", "reason": "start the shadow stages",
                                                    "evidence": {"v2_benchmark_tag": "v2.9.1"}},
        # a field the engine does not know is not this proxy's to drop or to interpret
        "/promotion-requests/fpr_abc/approve": {"reason": "forced", "prerequisites": {"certification_status": "CERTIFIED"}},
        "/families/DAILY_TREND/revoke": {"reason": "back to the start"},
        "/families/DAILY_TREND/kill-switch": {"enabled": False, "reason": "resolved"},
        "/accounts/acct-1/kill-switch": {"enabled": True, "reason": "customer asked"},
    }
    for path, body in bodies.items():
        assert client.post(f"{BASE}{path}", json=body).status_code == 200

    assert [(call["url"], call["json"]) for call in upstream.calls] == [(f"{UPSTREAM}{path}", body) for path, body in bodies.items()]


def test_registration_needs_no_body_and_forwards_one_when_it_is_sent(monkeypatch):
    upstream = FakeUpstream(200, {"events": []})
    client = _client(monkeypatch, upstream)

    assert client.post(f"{BASE}/families/register").status_code == 200
    assert client.post(f"{BASE}/families/register", json={"reason": "first registration"}).status_code == 200

    assert [call["json"] for call in upstream.calls] == [None, {"reason": "first registration"}]
    assert [details["request"] for _id, _action, details in upstream.audit] == [{}, {"reason": "first registration"}]


def test_limit_is_forwarded_as_a_query_parameter_and_nothing_else_is(monkeypatch):
    upstream = FakeUpstream(200, {"cycles": []})
    client = _client(monkeypatch, upstream)

    assert client.get(f"{BASE}/cycles?limit=7&family_id=X&offset=3").status_code == 200
    assert client.get(f"{BASE}/shadow/divergences?limit=250").status_code == 200
    assert client.get(f"{BASE}/shadow/divergences").status_code == 200
    assert client.get(f"{BASE}/cycles?limit=many").status_code == 422

    assert [(call["url"], call["params"]) for call in upstream.calls] == [
        (f"{UPSTREAM}/cycles", {"limit": 7}),
        (f"{UPSTREAM}/shadow/divergences", {"limit": 250}),
        (f"{UPSTREAM}/shadow/divergences", None),
    ]


@pytest.mark.parametrize("path,body,status_code,answer", [
    ("/promotion-requests/fpr_abc/approve", REASON, 409,
     {"detail": "PREREQUISITES_NOT_MET: EVIDENCE_MISSING:section9_shadow_verified", "reason": "PREREQUISITES_NOT_MET"}),
    ("/families/DAILY_TREND/promotion-requests", {"to_stage": "M6", **REASON}, 409,
     {"detail": "STAGE_SKIPPED: M0 -> M6: the next stage is M1", "reason": "STAGE_SKIPPED"}),
    ("/families/DAILY_TREND/promotion-requests", {"to_stage": "M7", **REASON}, 409,
     {"detail": "STAGE_NOT_OPERABLE_HERE: M7 is beyond M6; live promotion is a separate decision",
      "reason": "STAGE_NOT_OPERABLE_HERE"}),
    ("/promotion-requests/fpr_abc/approve", REASON, 409,
     {"detail": "PREREQUISITES_NOT_MET: RESEARCH_CERTIFICATION_NOT_CERTIFIED, ADMISSION_NOT_ELIGIBLE",
      "reason": "PREREQUISITES_NOT_MET"}),
    ("/families/NOPE/kill-switch", SWITCH, 404, {"detail": "FAMILY_NOT_REGISTERED: NOPE", "reason": "FAMILY_NOT_REGISTERED"}),
    ("/promotion-requests/unknown/reject", REASON, 404, {"detail": "REQUEST_NOT_FOUND: unknown", "reason": "REQUEST_NOT_FOUND"}),
    ("/families/DAILY_TREND/kill-switch", {"enabled": True, "reason": "x"}, 422,
     {"detail": [{"type": "string_too_short", "loc": ["body", "reason"], "msg": "String should have at least 3 characters"}]}),
    ("/accounts/GLOBAL/kill-switch", SWITCH, 400,
     {"detail": "the platform kill switch is on the emergency router", "reason": "SCOPE_INVALID"}),
    ("/accounts/acct-1/kill-switch", SWITCH, 503,
     {"detail": "the change could not be persisted", "reason": "KILL_SWITCH_NOT_PERSISTED"}),
    ("/families/DAILY_TREND/disable", SWITCH, 403,
     {"detail": "Emergency controls require the admin emergency service credential"}),
])
def test_refusals_reach_the_operator_unchanged(monkeypatch, path, body, status_code, answer):
    upstream = FakeUpstream(status_code, answer)
    client = _client(monkeypatch, upstream)

    response = client.post(f"{BASE}{path}", json=body)

    assert response.status_code == status_code
    assert response.json() == answer
    assert upstream.calls[0]["json"] == body
    (_admin_id, _action, details), = upstream.audit
    assert details["outcome_status"] == status_code and details["outcome_reason"] == answer.get("reason")


def test_an_unknown_family_read_passes_through_and_is_audited(monkeypatch):
    upstream = FakeUpstream(404, {"detail": "NOPE", "reason": "FAMILY_UNKNOWN"})
    client = _client(monkeypatch, upstream)

    response = client.get(f"{BASE}/families/NOPE")

    assert (response.status_code, response.json()) == (404, {"detail": "NOPE", "reason": "FAMILY_UNKNOWN"})
    assert [(action, details["outcome_status"], details["outcome_reason"], details["request"])
            for _id, action, details in upstream.audit] == [("family", 404, "FAMILY_UNKNOWN", {"family_id": "NOPE"})]


# ------------------------------------------------------------------------------------------------- audit
@pytest.mark.parametrize("method,path,body,action", WRITES)
def test_every_write_is_audited_with_operator_action_and_outcome(monkeypatch, method, path, body, action):
    upstream = FakeUpstream(200, {"event": {"event_id": "fev_1"}})
    client = _client(monkeypatch, upstream)
    _send(client, method, path, body)
    upstream.status_code, upstream.body = 409, {"detail": "FAMILY_DISABLED: DAILY_TREND", "reason": "FAMILY_DISABLED"}
    _send(client, method, path, body)
    upstream.raises = httpx.ReadTimeout("slow")
    _send(client, method, path, body)

    assert [(admin_id, recorded, details["outcome_status"], details["outcome_reason"])
            for admin_id, recorded, details in upstream.audit] == [
        (ADMIN["id"], action, 200, None),
        (ADMIN["id"], action, 409, "FAMILY_DISABLED"),
        (ADMIN["id"], action, 504, "UPSTREAM_TIMEOUT"),
    ]
    assert all(details["admin_id"] == ADMIN["id"] and details["action"] == action for _i, _a, details in upstream.audit)


def test_the_audit_keeps_who_what_and_why_but_never_the_whole_body(monkeypatch):
    upstream = FakeUpstream(200, {"event": {}})
    client = _client(monkeypatch, upstream)
    client.post(f"{BASE}/families/DAILY_TREND/kill-switch", json={"enabled": True, "reason": "incident 7"})
    client.post(f"{BASE}/families/DAILY_TREND/promotion-requests",
                json={"to_stage": "M1", "reason": "r" * 500, "evidence": {"v2_benchmark_tag": "v" * 5000}})
    client.post(f"{BASE}/families/DAILY_TREND/evidence",
                json={"key": "certification_run_id", "reference": "f" * 900, "value": {"big": ["x"] * 100}, "reason": "week 1"})
    client.post(f"{BASE}/promotion-requests/fpr_abc/approve", json={"reason": "reviewed", "to_stage": ["M9"], "enabled": {"a": 1}})
    client.post(f"{BASE}/accounts/brk_9f2c41aa07b1/kill-switch", json={"enabled": False, "reason": "resolved"})

    requests = [details["request"] for _id, _action, details in upstream.audit]
    assert requests == [
        {"family_id": "DAILY_TREND", "enabled": True, "reason": "incident 7"},
        {"family_id": "DAILY_TREND", "to_stage": "M1", "reason": "r" * 200},
        {"family_id": "DAILY_TREND", "key": "certification_run_id", "reference": "f" * 200, "reason": "week 1"},
        {"request_id": "fpr_abc", "to_stage": "<list>", "enabled": "<dict>", "reason": "reviewed"},
        {"account_id": "brk_9f2c41aa07b1", "enabled": False, "reason": "resolved"},
    ]


def test_reads_are_audited_only_when_they_do_not_succeed(monkeypatch):
    upstream = FakeUpstream(200, {"integrity": {"ok": True}, "events": []})
    client = _client(monkeypatch, upstream)
    for path in ("/families", "/families/DAILY_TREND", "/history", "/cycles", "/shadow/divergences"):
        assert client.get(f"{BASE}{path}").status_code == 200
    assert upstream.audit == []

    upstream.status_code, upstream.body = 500, {"detail": "Internal Server Error"}
    client.get(f"{BASE}/history")
    upstream.raises = httpx.ConnectError("refused")
    client.get(f"{BASE}/families")

    assert [(action, details["outcome_status"], details["outcome_reason"]) for _id, action, details in upstream.audit] == [
        ("history", 500, None), ("families", 502, "UPSTREAM_UNREACHABLE")]


def test_the_durable_row_is_an_admin_governance_event_naming_the_operator(monkeypatch):
    upstream = FakeUpstream(200, {"event": {}})
    client = _client(monkeypatch, upstream, capture_audit=False)       # the real _write_audit, down to the row writer
    rows = []
    monkeypatch.setattr(admin_emergency, "_write_audit_row", lambda event, admin, details: rows.append((event, admin, details)))

    client.post(f"{BASE}/families/DAILY_TREND/revoke", json={"to_stage": "M0", "reason": "back to the start"})

    (event, admin, details), = rows
    assert event == "admin_governance_promotion_revoke"
    assert (admin["id"], admin["email"]) == (ADMIN["id"], ADMIN["email"])
    assert details == {"admin_id": ADMIN["id"], "action": "promotion_revoke", "outcome_status": 200, "outcome_reason": None,
                       "request": {"family_id": "DAILY_TREND", "to_stage": "M0", "reason": "back to the start"}}


def test_a_failing_audit_store_never_changes_the_answer(monkeypatch):
    upstream = FakeUpstream(409, {"detail": "STAGE_SKIPPED: M0 -> M3", "reason": "STAGE_SKIPPED"})
    client = _client(monkeypatch, upstream)

    def broken(admin, action, details):
        raise RuntimeError("database is locked")

    monkeypatch.setattr(admin_governance, "_write_audit", broken)
    response = client.post(f"{BASE}/families/DAILY_TREND/promotion-requests", json={"to_stage": "M3", **REASON})
    assert response.status_code == 409 and response.json()["reason"] == "STAGE_SKIPPED"


def test_the_emergency_audit_is_not_touched_by_governance_calls(monkeypatch):
    upstream = FakeUpstream(200, {"event": {}})
    client = _client(monkeypatch, upstream)
    emergency_rows = []
    monkeypatch.setattr(admin_emergency, "_write_audit", lambda *row: emergency_rows.append(row))

    client.post(f"{BASE}/families/DAILY_TREND/disable", json=SWITCH)

    assert emergency_rows == [] and len(upstream.audit) == 1


# ---------------------------------------------------------------------------------------- transport failures
@pytest.mark.parametrize("error", [
    httpx.ConnectError("connection refused"),
    httpx.ConnectTimeout("connect timed out"),      # a timeout, but the request never left
    httpx.PoolTimeout("no connection available"),
])
def test_request_that_was_never_sent_is_reported_as_nothing_done(monkeypatch, error):
    upstream = FakeUpstream(raises=error)
    client = _client(monkeypatch, upstream)

    response = client.post(f"{BASE}/promotion-requests/fpr_abc/approve", json=REASON)

    assert response.status_code == 502
    payload = response.json()
    assert payload["reason"] == "UPSTREAM_UNREACHABLE"
    assert "unreachable" in payload["detail"] and "nothing was done" in payload["detail"]
    assert "UNKNOWN" not in payload["detail"] and "event" not in payload


@pytest.mark.parametrize("error,reason", [
    (httpx.ReadTimeout("slow"), "UPSTREAM_TIMEOUT"),
    (httpx.WriteTimeout("slow write"), "UPSTREAM_TIMEOUT"),
    (httpx.ReadError("connection reset by peer"), "UPSTREAM_OUTCOME_UNKNOWN"),
    (httpx.RemoteProtocolError("server disconnected without sending a response"), "UPSTREAM_OUTCOME_UNKNOWN"),
])
def test_failure_after_sending_is_an_unknown_outcome_never_a_success(monkeypatch, error, reason):
    upstream = FakeUpstream(raises=error)
    client = _client(monkeypatch, upstream)

    response = client.post(f"{BASE}/promotion-requests/fpr_abc/approve", json=REASON)

    assert response.status_code == 504
    payload = response.json()
    assert payload["reason"] == reason
    assert "UNKNOWN" in payload["detail"] and "Reload the family registry and its history" in payload["detail"]
    assert "nothing was done" not in payload["detail"] and "event" not in payload


def test_upstream_401_is_a_502_and_does_not_log_the_admin_out(monkeypatch):
    upstream = FakeUpstream(401, {"detail": "Invalid token"})
    client = _client(monkeypatch, upstream)

    for response in (client.get(f"{BASE}/families"), client.post(f"{BASE}/families/DAILY_TREND/disable", json=SWITCH)):
        assert response.status_code == 502
        assert response.json()["reason"] == "UPSTREAM_AUTH_REJECTED"


# ----------------------------------------------------------------------------------------------- identifiers
HOSTILE = [
    "..", ".", "a.b", ".hidden", "a..b",
    "..%2f..%2femergency%2fflatten", "..%2F..%2Femergency%2Fkill-switch", "a%2fb", "%2e%2e", "a%00b", "%",
    "a b", " a", "a ", "a\tb", "a\nb", "DAILY_TREND\n",
    "a?b", "a#b", "a;b", "a&b", "a=b", "a:b", "a@b", "a\\b", "a|b", "a*b", "{family_id}", "a,b",
    "-a", "_a", "ｆａｍｉｌｙ", "fam​ily", "é", "A" * 129,
]


def test_only_plain_identifiers_are_identifiers():
    for value in ("DAILY_TREND", "RESIDUAL_MOMENTUM_PORTFOLIO_TOP1", "TREND_PULLBACK_V2", "fpr_0123456789abcdef01234567",
                  "brk_9f2c41aa07b1", "acct-1", "550e8400-e29b-41d4-a716-446655440000", "a", "9", "A" * 128):
        assert admin_governance.is_identifier(value), value
    for value in HOSTILE + ["", "a/b", "/", "a/", "../x", None, 7, b"abc", ["a"]]:
        assert not admin_governance.is_identifier(value), repr(value)


@pytest.mark.parametrize("method,template", [
    ("GET", "/families/{}"),
    ("POST", "/families/{}/evidence"),
    ("POST", "/families/{}/promotion-requests"),
    ("POST", "/promotion-requests/{}/approve"),
    ("POST", "/promotion-requests/{}/reject"),
    ("POST", "/families/{}/revoke"),
    ("POST", "/families/{}/revoke-certification"),
    ("POST", "/families/{}/kill-switch"),
    ("POST", "/families/{}/disable"),
    ("POST", "/accounts/{}/kill-switch"),
])
def test_an_identifier_that_is_not_plain_never_reaches_the_engine(monkeypatch, method, template):
    """The path as an ASGI server delivers it (decoded once), so ``%2f`` here is what a client sent as
    ``%252f`` and ``..`` is what it sent as ``..`` or ``%2e%2e``."""
    upstream = FakeUpstream(200, {"event": {}})
    app = _app(monkeypatch, upstream)

    for value in HOSTILE:
        status, payload = _served(app, method, f"{BASE}{template.format(value)}", json.dumps(SWITCH).encode())
        assert (status, payload["reason"]) == (400, "IDENTIFIER_INVALID"), repr(value)
        assert "NOT sent" in payload["detail"]

    assert upstream.calls == []
    # each refusal is on record, with what was attempted (bounded)
    assert len(upstream.audit) == len(HOSTILE)
    assert all(details["outcome_status"] == 400 and details["outcome_reason"] == "IDENTIFIER_INVALID"
               for _id, _action, details in upstream.audit)
    attempted = [next(v for k, v in details["request"].items() if k.endswith("_id")) for _i, _a, details in upstream.audit]
    assert attempted == [value[:200] for value in HOSTILE]


@pytest.mark.parametrize("method,path", [
    # a slash in the identifier is a different path: no governance route has that shape
    ("POST", "/families/DAILY_TREND/../../emergency/flatten/kill-switch"),
    ("POST", "/families/../emergency/kill-switch"),
    ("POST", "/families/a/b/kill-switch"),
    ("POST", "/accounts/../families/DAILY_TREND/kill-switch"),
    ("POST", "/promotion-requests/../../emergency/flatten/approve"),
    ("GET", "/families/../history"),
    ("GET", "/families/DAILY_TREND/../../emergency/status"),
    ("GET", "/../emergency/status"),
])
def test_a_slash_in_an_identifier_matches_no_route(monkeypatch, method, path):
    upstream = FakeUpstream(200, {"event": {}})
    app = _app(monkeypatch, upstream)

    status, _payload = _served(app, method, f"{BASE}{path}", json.dumps(SWITCH).encode())

    assert status in (404, 405)
    assert upstream.calls == []


@pytest.mark.parametrize("encoded", [
    "..%2f..%2femergency%2fflatten", "..%252f..%252femergency%252fflatten", "..%25252f..%25252femergency%25252fflatten",
    "%2e%2e", "%252e%252e", "..", "a%2fb", "a%252fb", "DAILY_TREND%2f..%2f..%2femergency%2fkill-switch",
    "DAILY_TREND%252f..%252f..%252femergency%252fkill-switch", "a%20b", "a%3fb", "a%23b", "a%00b",
])
def test_no_encoding_of_a_traversal_reaches_another_upstream_path_through_an_http_client(monkeypatch, encoded):
    """Whatever the client and the test transport make of the encoding, the engine is never called with
    anything but the governance path of a plain identifier."""
    upstream = FakeUpstream(200, {"event": {}})
    client = _client(monkeypatch, upstream)

    responses = [client.get(f"{BASE}/families/{encoded}"),
                 client.post(f"{BASE}/families/{encoded}/kill-switch", json=SWITCH),
                 client.post(f"{BASE}/promotion-requests/{encoded}/approve", json=REASON),
                 client.post(f"{BASE}/accounts/{encoded}/kill-switch", json=SWITCH)]

    assert all(response.status_code in (400, 404, 405) for response in responses)
    assert upstream.calls == []


def test_a_plain_identifier_is_placed_in_exactly_one_path_segment(monkeypatch):
    upstream = FakeUpstream(200, {"event": {}})
    client = _client(monkeypatch, upstream)

    client.post(f"{BASE}/families/register/kill-switch", json=SWITCH)       # a family that happens to be called "register"
    client.get(f"{BASE}/families/history")
    client.post(f"{BASE}/accounts/GLOBAL/kill-switch", json=SWITCH)         # refused by the engine (400), not rewritten here

    assert [call["url"] for call in upstream.calls] == [
        f"{UPSTREAM}/families/register/kill-switch", f"{UPSTREAM}/families/history", f"{UPSTREAM}/accounts/GLOBAL/kill-switch"]
    for call in upstream.calls:
        assert re.fullmatch(re.escape(UPSTREAM) + r"(/[A-Za-z0-9_-]+)+", call["url"])
