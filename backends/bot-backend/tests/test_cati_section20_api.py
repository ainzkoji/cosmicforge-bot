"""CATI Section 20: backend API surface (existing /api/v1/brokers family + the admin /api/v1/capabilities/cati
family), tenancy, redaction, dry-run transfer plans, throttled sync, operator-only research routes."""
from __future__ import annotations

import json
import time
from decimal import Decimal

import pytest

from test_phase2b_permissions_transfers import (  # noqa: F401  (db / api are fixtures)
    SAFE_EVIDENCE, FakeAdapter, _account, _service, _token, api, db,
)


def _admin_token(user="ops-admin"):
    import jwt

    from app.core.config import settings
    from app.core.security import AUDIENCE, ISSUER

    now = int(time.time())
    return jwt.encode({"sub": user, "type": "access", "role": "admin", "iss": ISSUER, "aud": AUDIENCE, "iat": now,
                       "exp": now + 600}, settings.SECRET_KEY, algorithm=settings.ALGORITHM)


H = lambda user: {"Authorization": f"Bearer {_token(user)}"}  # noqa: E731
SECRET_MARKERS = ("-key-1234", "api_secret", "\"sec\"", "encrypted_blob", "passphrase", "Authorization")


@pytest.fixture
def no_venue_calls(monkeypatch, tmp_path):
    """No test touches a real venue: discovery refresh is stubbed; research data dir is an empty temp dir."""
    import app.activation.account_status as ast
    from app.market_data import research_status as rs

    calls = []
    monkeypatch.setattr(ast, "refresh_if_stale", lambda *a, **k: None)
    monkeypatch.setattr(ast, "sync_discovery", lambda db, auth, **k: calls.append(auth.account_id) or
                        {"status": "SYNCED", "venue": "bybit_linear", "discovered": 0})
    monkeypatch.setattr(ast, "_refresh_attempts", {})
    monkeypatch.setenv("CATI_RESEARCH_DATA_DIR", str(tmp_path / "no-research-data"))
    rs.reset_cache()
    yield calls
    rs.reset_cache()


def _seed_catalog(db):
    from app.exchange.instruments import DiscoveredInstrument, InstrumentCatalog

    def ins(sym, ac="CRYPTO", canon=None, pt="PERPETUAL", tradable=True, src="VENUE_METADATA", base=None, quote="USDT"):
        return DiscoveredInstrument("bybit_linear", sym, ac, pt, canon or f"{sym[:-4]}/USDT:PERP", base or sym[:-4],
                                    quote, "USDT", "PERPETUAL", "Trading", tradable, 0.1, 0.001, 0.001, 100.0, 5.0, 10.0,
                                    classification_source=src)
    InstrumentCatalog(db).upsert("bybit_linear", "DEMO", [
        ins("BTCUSDT"), ins("ETHUSDT"),
        ins("EURUSDT", ac="FX", canon="EUR/USD:PERP", pt="FX_PERPETUAL", base="EUR"),
        ins("NEWCOINUSDT"),
    ], int(time.time() * 1000))


# ---------------------------------------------------------------- 20.2 / 20.12 capabilities + tenancy
def test_account_capabilities_are_owner_scoped_and_secret_free(db, api, no_venue_calls):
    client, _ad = api
    _account(db, "acc_a", "alice", perms=SAFE_EVIDENCE)
    _seed_catalog(db)
    r = client.get("/api/v1/brokers/acc_a/market-status", headers=H("alice"))
    assert r.status_code == 200
    body = r.json()
    assert body["broker_account_id"] == "acc_a" and body["environment"] == "DEMO" and body["observed_at_ms"]
    assert body["withdrawal_permission_required"] is False
    assert body["permission_health"]["WITHDRAW"]["use"] == "NEVER_REQUIRED"
    assert body["permission_health"]["INTERNAL_TRANSFER"]["state"] in ("VERIFIED", "MISSING", "UNVERIFIED")
    assert {"CRYPTO", "FX"} <= set(body["markets"]) and body["markets"]["CRYPTO"]["execution"]["reason_class"]
    assert body["topology"]["class"] in ("UNIFIED", "SEGMENTED", "UNKNOWN", "UNSUPPORTED")
    text = json.dumps(body)
    assert not any(m in text for m in SECRET_MARKERS)
    # cross-user: 404 (existence not revealed), for every account-scoped route
    for method, path in (("get", "/api/v1/brokers/acc_a/market-status"),
                         ("post", "/api/v1/brokers/acc_a/market-discovery/sync"),
                         ("post", "/api/v1/brokers/acc_a/capital-transfer-plan")):
        kw = {"json": {"asset": "USDT", "amount": "1", "source_wallet": "FUND", "destination_wallet": "CONTRACT"}} \
            if path.endswith("plan") else {}
        assert getattr(client, method)(path, headers=H("mallory"), **kw).status_code == 404, path
    assert client.get("/api/v1/brokers/acc_a/market-status").status_code == 401


# ---------------------------------------------------------------- 20.3 sync reuse + throttle + lineage
def test_instrument_sync_reuses_discovery_and_is_throttled(db, api, no_venue_calls):
    client, _ad = api
    _account(db, "acc_a", "alice", perms=SAFE_EVIDENCE)
    first = client.post("/api/v1/brokers/acc_a/market-discovery/sync", headers=H("alice")).json()
    again = client.post("/api/v1/brokers/acc_a/market-discovery/sync", headers=H("alice")).json()
    assert first["status"] == "SYNCED" and again["status"] == "THROTTLED" and again["reason"] == "SYNC_COOLDOWN"
    assert no_venue_calls == ["acc_a"]  # one venue call; the repeat never reaches the venue
    lin = first["lineage"]
    assert lin["user_id"] == "alice" and lin["broker_account_id"] == "acc_a" and lin["request_id"].startswith("isync")


# ---------------------------------------------------------------- 20.4 instruments + four separate states
def test_instruments_separate_market_research_certification_execution(db, api, no_venue_calls):
    client, _ad = api
    _account(db, "acc_a", "alice", perms=SAFE_EVIDENCE)
    _seed_catalog(db)
    rows = client.get("/api/v1/brokers/acc_a/market-status?instruments_family=CRYPTO", headers=H("alice")).json()["instruments"]
    new = next(r for r in rows if r["venue_symbol"] == "NEWCOINUSDT")
    assert new["readiness"]["market_available"]["state"] is True
    assert new["readiness"]["research"] == {"state": "RESEARCH_ONLY", "reason": "INSTRUMENT_RESEARCH_ONLY"}
    assert new["readiness"]["certification"]["reason"] == "CERTIFICATION_NOT_READY"
    assert new["readiness"]["execution_authorized"]["state"] is False  # discovery never makes it trade-ready
    fx = client.get("/api/v1/brokers/acc_a/market-status?instruments_family=FX", headers=H("alice")).json()["instruments"]
    assert fx and fx[0]["readiness"]["research"]["reason"] in ("DATASET_ACQUIRING", "INSTRUMENT_RESEARCH_ONLY")
    only_ready = client.get("/api/v1/brokers/acc_a/market-status?instruments_family=CRYPTO&execution_authorized=true",
                            headers=H("alice")).json()["instruments"]
    assert only_ready == []
    ro = client.get("/api/v1/brokers/acc_a/market-status?instruments_family=CRYPTO&research_state=RESEARCH_ONLY",
                    headers=H("alice")).json()["instruments"]
    assert {r["venue_symbol"] for r in ro} == {"BTCUSDT", "ETHUSDT", "NEWCOINUSDT"}  # not in a bybit research universe


# ---------------------------------------------------------------- 20.5 topology
def test_topology_view_unified_segmented_unknown():
    from app.activation.account_status import topology_view

    uni = topology_view("bybit", "UNIFIED", internal_transfer={"state": "BLOCKED", "reason": "PERMISSION_EVIDENCE_REQUIRED"},
                        observed_at_ms=5)
    assert uni["class"] == "UNIFIED" and "POLICY constraints" in uni["logical_allocation"] and uni["observed_at_ms"] == 5
    assert all(not r["available"] and r["reason_code"] == "PERMISSION_EVIDENCE_REQUIRED" for r in uni["routes"])
    assert all(r["fee"] is None for r in uni["routes"])  # route fees are never assumed
    seg = topology_view("bybit", "CLASSIC", internal_transfer={"state": "ACTIVE"}, observed_at_ms=5)
    assert seg["class"] == "SEGMENTED" and "logical_allocation" not in seg and all(r["available"] for r in seg["routes"])
    unk = topology_view("bybit", None, internal_transfer={"state": "BLOCKED"}, observed_at_ms=None)
    assert unk["class"] == "UNKNOWN" and unk["reason_code"] == "ACCOUNT_TOPOLOGY_UNKNOWN" and unk["routes"] == []


# ---------------------------------------------------------------- 20.6 dry-run plan: side-effect free
def test_transfer_plan_is_side_effect_free_and_explains_itself(db, api):
    client, ad = api
    _account(db, "acc_a", "alice", perms=SAFE_EVIDENCE)
    body = {"asset": "USDT", "amount": "100", "source_wallet": "FUND", "destination_wallet": "CONTRACT"}
    r = client.post("/api/v1/brokers/acc_a/capital-transfer-plan", json=body, headers=H("alice"))
    assert r.status_code == 200
    p = r.json()
    assert p["dry_run"] is True and p["side_effects"] == "NONE" and p["physical_transfer_required"] is True
    assert p["policy_decision"]["eligible"] is True and p["route"]["route_code"]
    assert p["route_facts"]["fee"]["state"] == "UNAVAILABLE" and "TRANSFER_COST_UNAVAILABLE" in p["reason_codes"]
    assert p["approval"]["required"] is True and p["valid_until_ms"] > p["observed_at_ms"]
    assert p["withdrawal"] == "NEVER_USED"
    assert ad.submits == []  # the broker was never asked to move anything
    with db.connect() as c:
        assert c.execute("SELECT COUNT(*) FROM broker_transfer_requests").fetchone()[0] == 0
        assert c.execute("SELECT COUNT(*) FROM broker_transfer_events").fetchone()[0] == 0
    same = client.post("/api/v1/brokers/acc_a/capital-transfer-plan", json=body, headers=H("alice")).json()
    assert same["plan_id"] == p["plan_id"]  # deterministic identity
    too_big = client.post("/api/v1/brokers/acc_a/capital-transfer-plan", headers=H("alice"),
                          json={**body, "amount": "999999"}).json()
    assert too_big["policy_decision"]["reason_code"] == "INSUFFICIENT_TRANSFERABLE_BALANCE"
    bad = client.post("/api/v1/brokers/acc_a/capital-transfer-plan", headers=H("alice"), json={"asset": "USDT"})
    assert bad.status_code == 422 and bad.json()["detail"]["reason_code"] == "PLAN_INPUT_REQUIRED"


def test_automated_plan_needs_policy_and_approval(db, api):
    client, _ad = api
    _account(db, "acc_a", "alice", perms=SAFE_EVIDENCE)
    body = {"asset": "USDT", "amount": "100", "source_wallet": "FUND", "destination_wallet": "CONTRACT", "automated": True}
    p = client.post("/api/v1/brokers/acc_a/capital-transfer-plan", json=body, headers=H("alice")).json()
    assert p["policy_decision"]["eligible"] is False and p["policy_decision"]["reason_code"] == "AUTOMATION_NOT_AUTHORIZED"


# ---------------------------------------------------------------- 20.7 / 20.8 execute + history
def test_execute_is_idempotent_and_ambiguous_never_retried(db, api):
    from app.transfers.models import TransferStatus as S

    client, ad = api
    _account(db, "acc_a", "alice", perms=SAFE_EVIDENCE)
    body = {"asset": "USDT", "amount": "10", "source_wallet": "FUND", "destination_wallet": "CONTRACT",
            "idempotency_key": "idem-sec20-0001"}

    def timeout(**k):
        raise TimeoutError("read timed out")
    ad.submit_behavior = timeout
    first = client.post("/api/v1/brokers/acc_a/internal-transfers", json=body, headers=H("alice")).json()
    again = client.post("/api/v1/brokers/acc_a/internal-transfers", json=body, headers=H("alice")).json()
    assert first["status"] == S.UNKNOWN.value and again["id"] == first["id"] and len(ad.submits) == 1
    hist = client.get("/api/v1/brokers/acc_a/internal-transfers", headers=H("alice")).json()["transfers"]
    assert [t["status"] for t in hist] == ["UNKNOWN"] and hist[0]["failure_reason"] == "SUBMIT_OUTCOME_UNKNOWN"
    ev = client.get(f"/api/v1/brokers/acc_a/internal-transfers/{first['id']}", headers=H("alice")).json()["events"]
    assert [e["to_status"] for e in ev][-1] == "UNKNOWN"
    assert not any(m in json.dumps(hist) + json.dumps(ev) for m in SECRET_MARKERS)


# ---------------------------------------------------------------- 20.9-20.11 operator-only research routes
def test_research_routes_are_admin_only_and_safe(db, api, no_venue_calls):
    client, _ad = api
    admin = {"Authorization": f"Bearer {_admin_token()}"}
    for path in ("/api/v1/capabilities/cati/multi-asset", "/api/v1/capabilities/cati/datasets"):
        assert client.get(path).status_code == 401
        assert client.get(path, headers=H("alice")).status_code == 403
        assert client.get(path, headers=admin).status_code == 200
    plan = {"dataset": "FX_REFERENCE", "provider": "dukascopy", "instruments": ["EURUSD"], "timeframe": "1m",
            "start": "2025-01-06", "end": "2025-01-10"}
    assert client.post("/api/v1/capabilities/cati/datasets/backfill-plan", json=plan, headers=H("alice")).status_code == 403
    ok = client.post("/api/v1/capabilities/cati/datasets/backfill-plan", json=plan, headers=admin)
    assert ok.status_code == 200 and ok.json()["execution"] == "SUPERVISED_OPERATOR_JOB"
    assert ok.json()["holdout_access"] == "NONE" and ok.json()["job_id"].startswith("bfj")
    for bad, code in ((dict(plan, provider="somewhere"), "PROVIDER_NOT_ALLOWED"),
                      (dict(plan, instruments=["EURUSD", "NOTAPAIR"]), "INSTRUMENT_OUTSIDE_FROZEN_UNIVERSE"),
                      (dict(plan, dataset="ANY_URL"), "DATASET_NOT_ALLOWED"),
                      (dict(plan, start="2023-01-02"), "RANGE_OUTSIDE_FROZEN_WINDOW"),
                      (dict(plan, start="2010-01-01"), "RANGE_OUT_OF_BOUNDS")):
        r = client.post("/api/v1/capabilities/cati/datasets/backfill-plan", json=bad, headers=admin)
        assert r.status_code == 422 and r.json()["detail"]["reason_code"] == code


def test_manifests_and_status_keep_states_separate(db, api, no_venue_calls):
    client, _ad = api
    admin = {"Authorization": f"Bearer {_admin_token()}"}
    ds = client.get("/api/v1/capabilities/cati/datasets", headers=admin).json()
    by = {d["dataset"]: d for d in ds["datasets"]}
    assert set(by) == {"CRYPTO_BROAD", "CRYPTO_DEEP", "FX_REFERENCE"}
    assert all(d["status"] == "FROZEN" and d["holdout"] == "CLOSED" and d["execution_authorized"] is False
               for d in by.values())
    assert by["CRYPTO_BROAD"]["universe_hash"].startswith("a5b5d1ee")
    assert by["FX_REFERENCE"]["acquisition"]["state"] == "UNAVAILABLE"  # the temp data dir holds no FX database
    text = json.dumps(ds)
    assert "no-research-data" not in text and ":\\\\" not in text  # no filesystem internals
    st = client.get("/api/v1/capabilities/cati/multi-asset", headers=admin).json()
    for fam in ("CRYPTO", "FX"):
        f = st["families"][fam]
        assert set(f) >= {"market_available", "data_readiness", "manifest", "certification", "holdout", "governance",
                          "execution_authority", "external_venue_validation"}
        assert f["holdout"]["state"] == "CLOSED" and f["execution_authority"] == {
            **f["execution_authority"], "demo": False, "production": False}
        assert f["external_venue_validation"]["bybit_linear"] == "EXTERNAL_VALIDATION_REQUIRED"
        assert "ready" not in f  # never collapsed into one boolean
