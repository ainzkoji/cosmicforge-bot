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
