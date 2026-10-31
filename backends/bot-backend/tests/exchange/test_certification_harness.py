"""Scope refusal and concrete external-reduction wiring; no real venue orders."""
import dataclasses
import time
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from app.exchange.platform_adapter import contract as C
from app.exchange.platform_adapter.certification import CertificationRegister, CertificationRefused, OperatorPermit
from app.exchange.platform_adapter.certification_harness import CertificationHarness
from app.trading_intelligence.execution.entry_permit import certification_order_permit, transport_entry_permitted
from shared_lib.persistence.cati_platform_schema import ensure_cati_platform_schema
from shared_lib.persistence.db import DB


@pytest.fixture
def db(tmp_path):
    database = DB(str(tmp_path / "certification.db"))
    ensure_cati_platform_schema(database)
    return database


def permit():
    now = int(time.time() * 1000)
    return OperatorPermit("test-operator", "test-owner-authorization", C.BYBIT, C.DEMO, "dedicated-account",
                          "one-symbol-test", "BTCUSDT", 100, now - 1000, now + 60_000, allow_orders=True)


def auth():
    return SimpleNamespace(account_id="dedicated-account", broker_type=C.BYBIT, environment="demo", user_id="owner",
                           api_secret="synthetic", api_key="synthetic", base_url="https://api-demo.bybit.com")


def test_invalid_permit_is_refused_before_client_construction(db, monkeypatch):
    from app.exchange.platform_adapter import certification_harness as H
    build = Mock(side_effect=AssertionError("invalid permit built a client"))
    monkeypatch.setattr(H, "build_client_from_auth", build)
    for invalid in (None, dataclasses.replace(permit(), account_ref="other-account"),
                    dataclasses.replace(permit(), environment=C.LIVE),
                    dataclasses.replace(permit(), issued_at_ms=1, expires_at_ms=2)):
        with pytest.raises(CertificationRefused):
            CertificationHarness(db, auth(), invalid, source_commit="synthetic")
    build.assert_not_called()


def test_harness_connects_fault_transport_and_refuses_orders_without_runtime_lease(db, monkeypatch):
    from app.exchange.platform_adapter import certification_harness as H
    from app.trading_intelligence.integration import production_runtime
    captured = []
    def build(resolved, *, http):
        captured.append(http)
        return auth()
    monkeypatch.setattr(H, "build_client_from_auth", build)
    monkeypatch.setattr(production_runtime, "owner_current", lambda db: False)
    harness = CertificationHarness(db, auth(), permit(), source_commit="synthetic")
    assert harness.runner.faults is harness.faults and captured[0].transport is harness.faults
    with pytest.raises(CertificationRefused, match="CANONICAL_RUNTIME_LEASE_REQUIRED"):
        harness.run()
    assert harness._http == []


def test_raw_certification_permit_requires_pending_action_and_exact_scope(db, monkeypatch):
    from app.trading_intelligence.integration import residual_prospective
    p = permit()
    register = CertificationRegister(db)
    run = register.start_run(p, adapter_version="synthetic", source_commit="synthetic", started_at=p.issued_at_ms + 1)
    request = C.OrderRequest(p.symbol, C.BUY, C.MARKET, "cert-synthetic", quantity=0.001)
    client = SimpleNamespace(_production_db=db, _broker_account_id=p.account_ref, broker_environment="demo")
    payload = {"symbol": p.symbol, "side": "Buy", "qty": "0.001", "orderLinkId": request.client_order_id, "orderType": "Market"}
    monkeypatch.setattr(residual_prospective, "owner_current", lambda db: True)
    with certification_order_permit(db, run, p, request):
        assert not transport_entry_permitted(client, p.symbol, "Buy", payload=payload)
        register.record_action(run_id=run, phase="INTENT", action="PLACE_ORDER", symbol=p.symbol,
                               client_order_id=request.client_order_id, outcome=None,
                               document={"side": C.BUY, "quantity": 0.001}, recorded_at=p.issued_at_ms + 2)
        assert transport_entry_permitted(client, p.symbol, "Buy", payload=payload)
        assert not transport_entry_permitted(client, p.symbol, "Buy", payload={**payload, "qty": "0.002"})
        assert not transport_entry_permitted(client, "ETHUSDT", "Buy", payload=payload)
        register.record_action(run_id=run, phase="OUTCOME", action="PLACE_ORDER", symbol=p.symbol,
                               client_order_id=request.client_order_id, outcome="ACCEPTED", document={}, recorded_at=p.issued_at_ms + 3)
        assert not transport_entry_permitted(client, p.symbol, "Buy", payload=payload)
    assert not transport_entry_permitted(client, p.symbol, "Buy", payload=payload)


def test_external_reduction_is_bounded_to_the_runs_own_position_and_never_repeated(db):
    from app.exchange.platform_adapter.fake import FakeVenue
    fake = FakeVenue(C.BYBIT, wallet_balance=10_000)
    fake.add_instrument("BTCUSDT", price=50_000, quantity_step=0.0001)
    fake.external_trade("BTCUSDT", C.BUY, 0.0015)
    harness = CertificationHarness.__new__(CertificationHarness)
    harness.permit, harness._external_done = permit(), False
    harness._adapter = lambda: fake
    harness.runner = SimpleNamespace(_flat_at_start=True, _run_id="synthetic", _unit=0.0005,
                                     _position_quantity=0.0015, _next_id=lambda: "cert-external",
                                     _mutate=lambda action, cid, document, send: send())
    harness._reduce_externally("BTCUSDT", 0.0005)
    assert fake.positions("BTCUSDT")[0].quantity == pytest.approx(0.001)
    with pytest.raises(CertificationRefused):
        harness._reduce_externally("BTCUSDT", 0.0005)


# ------------------------------------------------------------------ the whole harness over a mocked transport
#: how each venue's adapter moves a stop in PROTECTION_REPLACE (Section K1 D4 / pipeline.protection)
REPLACEMENT = {C.BINANCE: "CREATE_THEN_CANCEL", C.BYBIT: "AMEND_IN_PLACE", C.BINGX: "REPLACEMENT_NOT_POSSIBLE_SAFELY"}


class _ProductionOrderGate:
    """The configuration a real DEMO run has: production profile, demo order submission on, live off. Every
    raw entry then needs the certification permit (entry_permit), exactly as at the venue boundary."""
    production, DEMO_ORDER_SUBMISSION_ENABLED, LIVE_ORDER_SUBMISSION_ENABLED = True, True, False

    def __init__(self, base):
        self._base = base

    def __getattr__(self, name):
        return getattr(self._base, name)


@pytest.mark.parametrize("venue", [C.BINANCE, C.BYBIT, C.BINGX])
def test_the_full_harness_runs_end_to_end_over_a_mocked_transport(db, venue, monkeypatch):
    """Section 28: the whole catalogue through the REAL raw client, adapter, HTTP budget, fault injector, entry
    permit (production order gate on) and canonical runtime lease, against a simulated venue behind a mocked
    transport. No network. A rehearsal proves the harness and decides nothing: it records neither CERTIFIED nor
    FAILED. Found by this test: BingX entries were refused by the entry permit (it did not read the
    ``clientOrderId`` key BingX sends), the outside reduction was counted twice, a fresh adapter looked a stop up
    by a client id the venue does not store, and fills of a position stop were asked for by an id that has none."""
    import json
    from _certification_rehearsal import rehearse
    from app.core import config
    from app.exchange.platform_adapter import certification as CF

    monkeypatch.setattr(config, "settings", _ProductionOrderGate(config.settings))
    result, evidence, fake, wire = rehearse(venue, db)
    failing = {c["check_id"]: (c["status"], c["reason"], c["evidence"][:400])
               for c in evidence["register"]["checks"] if c["status"] != "PASS"}
    expected = {"USER_STREAM_SPEC": ("NOT_SUPPORTED", "NO_USER_STREAM_IN_THIS_ENVIRONMENT", "{}")} \
        if venue == C.BINGX else {}                       # the VST private stream is not documented: refused
    assert failing == expected
    assert (result.outcome, result.recorded_status, result.cleanup_complete) == ("PASSED", None, True)
    checks = {c["check_id"]: json.loads(c["evidence"]) for c in evidence["register"]["checks"]}
    assert checks["PROTECTION_REPLACE"]["method"] == REPLACEMENT[venue]
    assert checks["PROTECTION_REPLACE"]["unprotected_between"] is False
    assert ("STOP_REPLACEMENT" in result.operations) == (venue != C.BINGX)
    assert {"ENTRY", "PROTECTIVE_STOP", "CANCEL", "CLOSE"} <= set(result.operations)
    assert fake.positions("BTCUSDT") == () and fake.open_orders("BTCUSDT") == ()       # flat, nothing resting
    assert evidence["mocked_transport"] is True and evidence["register"]["actions"]
    assert all(str(a["client_order_id"] or "cert").startswith("cert") for a in evidence["register"]["actions"])
    # the register this rehearsal wrote to answers NOT_CERTIFIED: a rehearsal is never evidence about a venue
    assert CF.certification_status(db, venue, C.DEMO, int(time.time() * 1000)).status == CF.NOT_CERTIFIED
    assert wire.calls and all(not path.startswith("http") for _, path, _ in wire.calls)


def test_a_mocked_transport_serves_only_a_rehearsal_account_and_a_rehearsal_account_only_a_mocked_one(db, monkeypatch):
    from app.exchange.platform_adapter import certification_harness as H
    from _certification_rehearsal import rehearsal_permit, synthetic_auth
    monkeypatch.setattr(H, "build_client_from_auth", Mock(side_effect=AssertionError("a client was built")))
    real_auth = SimpleNamespace(**{**vars(synthetic_auth(C.BYBIT, "dedicated-account"))})
    with pytest.raises(CertificationRefused, match="MOCKED_TRANSPORT_AND_REHEARSAL_ACCOUNT_GO_TOGETHER"):
        CertificationHarness(db, real_auth, dataclasses.replace(rehearsal_permit(C.BYBIT, "dedicated-account")),
                             source_commit="x", transport_factory=lambda: None)
    rehearsal_auth = synthetic_auth(C.BYBIT, "rehearsal-bybit-account")
    with pytest.raises(CertificationRefused, match="MOCKED_TRANSPORT_AND_REHEARSAL_ACCOUNT_GO_TOGETHER"):
        CertificationHarness(db, rehearsal_auth, rehearsal_permit(C.BYBIT, "rehearsal-bybit-account"), source_commit="x")
    live = SimpleNamespace(**{**vars(rehearsal_auth), "base_url": "https://api.bybit.com"})
    with pytest.raises(CertificationRefused, match="ACCOUNT_IS_NOT_ON_THE_DEMO_HOST"):
        CertificationHarness(db, live, rehearsal_permit(C.BYBIT, "rehearsal-bybit-account"), source_commit="x",
                             transport_factory=lambda: None)


def test_a_refusal_of_the_platforms_own_budget_is_waited_out_and_a_venue_refusal_never_is():
    from app.exchange.platform_adapter.certification_harness import MAX_LOCAL_BUDGET_WAIT_S, _PacedAdapter
    naps, calls = [], []

    class Inner:
        venue, environment, adapter_version = C.BYBIT, C.DEMO, "x"

        def balance(self):
            calls.append("balance")
            if len(calls) < 3:
                raise C.AdapterError(C.RATE_LIMITED, "LOCAL_HTTP_BUDGET_REFUSED", venue=C.BYBIT, retry_after_s=1.5)
            return "ok"

        def place_order(self, request):
            raise C.AdapterError(C.RATE_LIMITED, "HTTP_BUDGET_VENUE_REFUSAL", venue=C.BYBIT, http_status=429,
                                 retry_after_s=2.0)

        def server_time(self):
            raise C.AdapterError(C.RATE_LIMITED, "LOCAL_HTTP_BUDGET_REFUSED", venue=C.BYBIT, retry_after_s=1e9)

    paced = _PacedAdapter(Inner(), naps.append)
    assert isinstance(paced, C.VenueAdapter) and paced.venue == C.BYBIT
    assert paced.balance() == "ok" and naps == [1.5, 1.5] and calls == ["balance"] * 3
    with pytest.raises(C.AdapterError, match="HTTP_BUDGET_VENUE_REFUSAL"):
        paced.place_order(None)                                   # the venue said wait: surfaced, never retried
    assert naps == [1.5, 1.5]
    with pytest.raises(C.AdapterError):
        paced.server_time()                                       # never waits longer than the bound in total
    assert sum(naps[2:]) == MAX_LOCAL_BUDGET_WAIT_S


@pytest.mark.parametrize("venue", [C.BINANCE, C.BYBIT, C.BINGX])
def test_preflight_names_what_a_real_demo_run_needs_without_reading_any_value(venue):
    from app.exchange.platform_adapter.certification_harness import VENUE_REQUIREMENTS, preflight
    secret_like = "x" * 44
    env = {"BROKER_SECRET_KEY": secret_like, "DATABASE_URL": "sqlite:///scratch.db", "DEMO_ORDER_SUBMISSION_ENABLED": "true"}
    ok = preflight(venue, symbol="BTCUSDT", max_notional=200.0, authorization_ref="owner-2026-11-01",
                   operator_ref="operator-1", allow_orders=True, env=env)
    assert ok["ready"] and ok["problems"] == [] and secret_like not in repr(ok)
    assert ok["requirements"]["environment"] == C.DEMO and ok["requirements"]["rest_host"].startswith("https://")
    bad = preflight(venue, symbol="btc-usdt", max_notional=0, authorization_ref="", operator_ref="",
                    environment="LIVE", allow_orders=True, leverage=5, env={})
    assert set(bad["problems"]) == {
        "ENVIRONMENT_MUST_BE_DEMO", "OWNER_AUTHORIZATION_REFERENCE_REQUIRED", "OPERATOR_REFERENCE_REQUIRED",
        "SYMBOL_NOT_CANONICAL", "MAX_NOTIONAL_REQUIRED", "LEVERAGE_OTHER_THAN_1_NEEDS_A_SEPARATE_OWNER_DECISION",
        "SETTING_NOT_PRESENT:BROKER_SECRET_KEY", "SETTING_NOT_PRESENT:DATABASE_URL",
        "DEMO_ORDER_SUBMISSION_ENABLED_IS_NOT_SET"} and not bad["ready"]
    assert set(VENUE_REQUIREMENTS) == {C.BINANCE, C.BYBIT, C.BINGX}
