"""MT pairing hardening: CSPRNG pairing codes, connector device binding on
claim, one-time claimed codes for /pair, and bridge URL validation.

Run: cd backends/user-backend && python -m pytest tests/test_mt_pairing_hardening.py
"""
from __future__ import annotations

import importlib.util
import re
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "backends" / "shared"))
sys.path.insert(0, str(ROOT / "backends" / "user-backend"))

from app.api import mt_pairing  # noqa: E402
from app.core import mt_pairing_service  # noqa: E402
from shared_lib.persistence.db import DB  # noqa: E402

USER = "mt-pairing-test-user"
DEVICE_SECRET = "0123456789abcdef0123456789abcdef"  # what the connector generates: token_hex(16)
OTHER_SECRET = "fedcba9876543210fedcba9876543210"
BRIDGE_URL = "https://test-tunnel.trycloudflare.com"
BRIDGE_TOKEN = "bridge-token-0123456789abcdef-0123456789"

PUBLIC_IP = "93.184.216.34"


def _public_resolver(host, port):
    return [PUBLIC_IP]


@pytest.fixture
def db(tmp_path, monkeypatch):
    """A throw-away database built by the repository's migration runner -- never data/bot.db."""
    monkeypatch.setenv("APP_ENV", "TEST")
    monkeypatch.setenv("ENVIRONMENT_NAME", "test")
    monkeypatch.setenv("DATABASE_ROLE", "test")
    path = tmp_path / "mt_pairing.db"
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{path.as_posix()}")
    runner = Path(__file__).resolve().parents[1] / "migrations" / "run_migration.py"
    spec = importlib.util.spec_from_file_location("run_migration", runner)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.run_migrations()
    return DB(path=str(path))


@pytest.fixture
def client(db):
    app = FastAPI()
    app.include_router(mt_pairing.router)
    return TestClient(app)


def _complete(pairing_code, bridge_url=BRIDGE_URL, platform="mt5"):
    return mt_pairing_service.complete_pairing(
        pairing_code=pairing_code,
        bridge_url=bridge_url,
        bridge_token=BRIDGE_TOKEN,
        tls_mode="strict",
        mt_platform=platform,
        account_login="99999",
        server="MetaQuotes-Demo",
    )


def _status(db, session_id):
    with db.connect() as conn:
        row = conn.execute(
            "SELECT status, bridge_url FROM mt_pairing_sessions WHERE id = ?", (session_id,)
        ).fetchone()
    return row["status"], row["bridge_url"]


def _pair_payload(pairing_code, bridge_url=BRIDGE_URL):
    return {
        "pairing_code": pairing_code,
        "bridge_url": bridge_url,
        "bridge_token": BRIDGE_TOKEN,
        "tls_mode": "strict",
        "account": {"login": "99999", "server": "MetaQuotes-Demo", "platform": "mt5"},
    }


# ---------------------------------------------------------------------------
# Pairing code generation
# ---------------------------------------------------------------------------

def test_pairing_code_comes_from_the_csprng(monkeypatch):
    assert not hasattr(mt_pairing_service, "random")

    codes = {mt_pairing_service.generate_pairing_code() for _ in range(200)}
    assert len(codes) == 200
    for code in codes:
        assert re.fullmatch(r"[A-HJ-NP-Z2-9]{4}-[A-HJ-NP-Z2-9]{4}", code)

    # Every character is drawn through ``secrets``.
    monkeypatch.setattr(mt_pairing_service.secrets, "choice", lambda chars: "Z")
    assert mt_pairing_service.generate_pairing_code() == "ZZZZ-ZZZZ"


# ---------------------------------------------------------------------------
# /connector/claim: device_secret is verified
# ---------------------------------------------------------------------------

def test_claim_columns_are_added_idempotently_even_when_two_requests_race():
    def table(columns):
        conn = sqlite3.connect(":memory:")
        conn.execute(f"CREATE TABLE mt_pairing_sessions ({', '.join(c + ' TEXT' for c in columns)})")
        return conn

    def columns(conn):
        return {row[1] for row in conn.execute("PRAGMA table_info(mt_pairing_sessions)").fetchall()}

    conn = table(["id"])
    mt_pairing_service._ensure_claim_columns(conn)
    mt_pairing_service._ensure_claim_columns(conn)  # second call: nothing left to add
    assert {"device_secret_hash", "connector_claimed_at"} <= columns(conn)

    class StaleView:
        """Sees the table as it was before another request added the columns:
        its own ALTERs then fail with "duplicate column name"."""

        def __init__(self, real):
            self.real = real
            self.alters = 0

        def execute(self, sql, *args):
            if sql.startswith("PRAGMA"):
                return sqlite3.connect(":memory:").execute("SELECT 0, 'id'")
            self.alters += 1
            return self.real.execute(sql, *args)

    stale = StaleView(conn)
    mt_pairing_service._ensure_claim_columns(stale)  # must not raise
    assert stale.alters == 2

    class Failing:
        def execute(self, sql, *args):
            if sql.startswith("PRAGMA"):
                return sqlite3.connect(":memory:").execute("SELECT 0, 'id'")
            raise sqlite3.OperationalError("database is locked")

    # Only the "already there" error is swallowed.
    with pytest.raises(sqlite3.OperationalError):
        mt_pairing_service._ensure_claim_columns(Failing())

    # No table at all: left to the caller's query to report.
    mt_pairing_service._ensure_claim_columns(sqlite3.connect(":memory:"))


def test_claim_binds_the_session_to_the_first_device_secret(db):
    session = mt_pairing_service.create_pairing_session(USER, "mt5", "demo")

    first = mt_pairing_service.claim_pairing_session(session["session_id"], DEVICE_SECRET)
    assert first["pairing_code"] == session["pairing_code"]

    # The same connector may ask again (retry) ...
    again = mt_pairing_service.claim_pairing_session(session["session_id"], DEVICE_SECRET)
    assert again["pairing_code"] == session["pairing_code"]

    # ... anyone else who learned the session id may not.
    with pytest.raises(ValueError):
        mt_pairing_service.claim_pairing_session(session["session_id"], OTHER_SECRET)

    with db.connect() as conn:
        row = conn.execute(
            "SELECT device_secret_hash FROM mt_pairing_sessions WHERE id = ?", (session["session_id"],)
        ).fetchone()
    assert row["device_secret_hash"]
    assert DEVICE_SECRET not in row["device_secret_hash"]  # only a hash is stored


@pytest.mark.parametrize("secret", ["", "short", "x" * 300, None])
def test_claim_rejects_missing_or_weak_device_secrets(db, secret):
    session = mt_pairing_service.create_pairing_session(USER, "mt5", "demo")
    with pytest.raises(ValueError):
        mt_pairing_service.claim_pairing_session(session["session_id"], secret)
    # A rejected claim must not bind the session.
    assert mt_pairing_service.claim_pairing_session(session["session_id"], DEVICE_SECRET)["pairing_code"]


def test_claim_of_expired_session_is_refused_and_recorded(db):
    session = mt_pairing_service.create_pairing_session(USER, "mt5", "demo")
    past = (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat()
    with db.connect() as conn:
        conn.execute("UPDATE mt_pairing_sessions SET expires_at = ? WHERE id = ?", (past, session["session_id"]))

    with pytest.raises(ValueError):
        mt_pairing_service.claim_pairing_session(session["session_id"], DEVICE_SECRET)
    assert _status(db, session["session_id"])[0] == "expired"


def test_claim_endpoint_enforces_the_device_secret(client):
    session = mt_pairing_service.create_pairing_session(USER, "mt5", "demo")
    url = "/api/v1/mt/connector/claim"

    assert client.post(url, json={"session_id": session["session_id"]}).status_code == 422
    assert client.post(url, json={"session_id": session["session_id"], "device_secret": "short"}).status_code == 422
    assert client.post(url, json={"session_id": "unknown", "device_secret": DEVICE_SECRET}).status_code == 400

    ok = client.post(url, json={"session_id": session["session_id"], "device_secret": DEVICE_SECRET})
    assert ok.status_code == 200, ok.text
    assert ok.json()["pairing_code"] == session["pairing_code"]

    stolen = client.post(url, json={"session_id": session["session_id"], "device_secret": OTHER_SECRET})
    assert stolen.status_code == 400
    assert "pairing_code" not in stolen.json()


# ---------------------------------------------------------------------------
# /pair: claimed, unexpired, single-use code + validated URL
# ---------------------------------------------------------------------------

def test_pairing_code_is_useless_until_released_through_a_claim(db):
    session = mt_pairing_service.create_pairing_session(USER, "mt5", "demo")

    with pytest.raises(ValueError):
        _complete(session["pairing_code"])
    assert _status(db, session["session_id"]) == ("pending", None)

    mt_pairing_service.claim_pairing_session(session["session_id"], DEVICE_SECRET)
    assert _complete(session["pairing_code"]) == session["session_id"]
    assert _status(db, session["session_id"]) == ("paired", BRIDGE_URL)


def test_pairing_code_is_single_use(db):
    session = mt_pairing_service.create_pairing_session(USER, "mt5", "demo")
    mt_pairing_service.claim_pairing_session(session["session_id"], DEVICE_SECRET)
    _complete(session["pairing_code"])

    with pytest.raises(ValueError):
        _complete(session["pairing_code"], bridge_url="https://attacker.example.com")
    assert _status(db, session["session_id"]) == ("paired", BRIDGE_URL)
    # A paired session cannot be re-claimed either.
    with pytest.raises(ValueError):
        mt_pairing_service.claim_pairing_session(session["session_id"], DEVICE_SECRET)


def test_expired_pairing_code_is_refused(db):
    session = mt_pairing_service.create_pairing_session(USER, "mt5", "demo")
    mt_pairing_service.claim_pairing_session(session["session_id"], DEVICE_SECRET)
    past = (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat()
    with db.connect() as conn:
        conn.execute("UPDATE mt_pairing_sessions SET expires_at = ? WHERE id = ?", (past, session["session_id"]))

    with pytest.raises(ValueError):
        _complete(session["pairing_code"])
    assert _status(db, session["session_id"]) == ("expired", None)


def test_pair_endpoint_requires_a_valid_claimed_code_and_url(client, db):
    session = mt_pairing_service.create_pairing_session(USER, "mt5", "demo")
    url = "/api/v1/mt/pair"

    # Guessing a code, or using one that was never released to a connector.
    assert client.post(url, json=_pair_payload("AAAA-BBBB")).status_code == 400
    assert client.post(url, json=_pair_payload(session["pairing_code"])).status_code == 400
    assert _status(db, session["session_id"]) == ("pending", None)

    mt_pairing_service.claim_pairing_session(session["session_id"], DEVICE_SECRET)

    # A valid code does not make a hostile URL acceptable.
    for hostile in ("http://169.254.169.254/latest/meta-data/", "https://169.254.169.254/",
                    "https://metadata.google.internal/", "ftp://example.com/", "https://user:pw@example.com/"):
        assert client.post(url, json=_pair_payload(session["pairing_code"], hostile)).status_code == 400, hostile
    assert _status(db, session["session_id"]) == ("pending", None)

    ok = client.post(url, json=_pair_payload(session["pairing_code"]))
    assert ok.status_code == 200, ok.text
    assert ok.json()["ok"] is True
    assert _status(db, session["session_id"]) == ("paired", BRIDGE_URL)

    assert client.post(url, json=_pair_payload(session["pairing_code"])).status_code == 400


# ---------------------------------------------------------------------------
# Bridge URL validation
# ---------------------------------------------------------------------------

def test_bridge_url_accepts_public_https_in_production():
    validate = mt_pairing_service.validate_bridge_url
    assert validate(BRIDGE_URL, production=True, resolver=_public_resolver) == BRIDGE_URL
    assert validate("https://bridge.example.com:8443/base", production=True, resolver=_public_resolver)


@pytest.mark.parametrize("url", [
    "http://bridge.example.com",              # plain HTTP
    "https://localhost:8443",
    "https://127.0.0.1:8443",
    "https://[::1]:8443",
    "https://10.0.0.5",
    "https://172.16.4.4",
    "https://192.168.1.10",
    "https://0.0.0.0",
    "https://169.254.169.254/latest/meta-data/",   # cloud metadata (link-local)
    "https://[::ffff:169.254.169.254]/",
    "https://[fe80::1]/",
    "https://100.100.100.200/",
    "https://metadata.google.internal/",
    "https://service.internal/",
    "https://intranet/",
    "https://user:password@bridge.example.com/",
    "https://bridge.example.com/#fragment",
    "ftp://bridge.example.com/",
    "file:///etc/passwd",
    "https://",
    "https://bridge.example.com/ path",
    "",
])
def test_bridge_url_rejected_in_production(url):
    with pytest.raises(ValueError):
        mt_pairing_service.validate_bridge_url(url, production=True, resolver=_public_resolver)


@pytest.mark.parametrize("resolved", [["10.0.0.5"], ["127.0.0.1"], ["169.254.169.254"],
                                      [PUBLIC_IP, "192.168.0.1"], ["::1"], []])
def test_bridge_url_rejected_when_hostname_resolves_to_internal_address(resolved):
    with pytest.raises(ValueError):
        mt_pairing_service.validate_bridge_url(
            "https://rebind.example.com", production=True, resolver=lambda host, port: resolved
        )


def test_bridge_url_rejected_when_hostname_does_not_resolve():
    def failing_resolver(host, port):
        raise OSError("no such host")

    with pytest.raises(ValueError):
        mt_pairing_service.validate_bridge_url(
            "https://nxdomain.example.com", production=True, resolver=failing_resolver
        )


def test_bridge_url_outside_production_allows_local_http_but_never_metadata():
    def no_dns(host, port):
        raise AssertionError("hostnames are not resolved outside production")

    validate = mt_pairing_service.validate_bridge_url
    assert validate("http://localhost:8443", production=False, resolver=no_dns)
    assert validate("https://127.0.0.1:8443", production=False, resolver=no_dns)
    assert validate(BRIDGE_URL, production=False, resolver=no_dns)
    for url in ("http://169.254.169.254/", "https://[fe80::1]/", "https://metadata.google.internal/",
                "ftp://localhost/", "https://user:pw@localhost/"):
        with pytest.raises(ValueError):
            validate(url, production=False, resolver=no_dns)
