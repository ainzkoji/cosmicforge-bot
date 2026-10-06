"""Section 18: only approved, capital-ready, hard-risk-valid TradePlans reach an EXISTING adapter, on current
venue metadata, with broker-native idempotency.

Existing suites already prove (not duplicated here): submit-unknown -> RESOLUTION_PENDING -> broker
reconciliation (order found / fill found / no position), partial fills, no duplicate submission of the same
plan, restart recovery of pending ownership, protection placement at the broker-filled quantity
(test_section20_execution_boundary); pending / UNKNOWN / confirmed transfer capital gating and the hard daily
cap after capital is ready (test_capital_readiness_boundary); zero plans for non-approved candidates
(test_section17_18_shadow_flow)."""
import time

import pytest
from _exec import Harness

from app.exchange import catalog_refresh
from app.trading_intelligence.execution.adapter import UnvalidatedExecutionAdapter, adapter_supports
from app.trading_intelligence.execution.binance_adapter import executor_adapter_for
from app.trading_intelligence.execution.boundary import BoundaryStatus as B


@pytest.fixture
def h(tmp_path):
    catalog_refresh._reset_for_tests()
    yield Harness(tmp_path)
    catalog_refresh._reset_for_tests()


def test_approved_capital_ready_plan_reaches_the_existing_adapter(h):
    res = h.run()
    assert res.status == B.EXECUTED and len(h.seen["orders"]) == 1
    assert h.reservation_status() == "CONSUMED"


@pytest.mark.parametrize("scope,code", [(("someone_else", None), "EXECUTION_ACCOUNT_SCOPE_MISMATCH"),
                                        (None, "EXECUTION_ACCOUNT_SCOPE_UNKNOWN")])
def test_plan_of_another_tenant_or_account_never_executes(h, scope, code):
    if scope is not None:
        scope = ("someone_else", h.plan.broker_account_id + "_other")
    res = h.run(h.boundary(account_scope=scope))
    assert res.status == B.WRONG_ACCOUNT and res.reason_codes == (code,)
    assert h.seen["orders"] == [] and h.reservation_status() == "RESERVED"  # the plan's own reservation is untouched


def test_missing_preflight_fails_closed(h):
    res = h.run(h.boundary(preflight=None))
    assert res.status == B.PREFLIGHT_BLOCKED and res.reason_codes == ("SUBMISSION_PREFLIGHT_NOT_CONFIGURED",)
    assert h.seen["orders"] == []


def test_stale_metadata_is_refreshed_before_submission(h):
    h.seed_catalog(now=h.now - 3 * 3_600_000)  # last seen 3h ago
    refreshed = []

    def refresh():
        refreshed.append(1)
        h.seed_catalog(now=h.now)  # the Section 7 sync writes current metadata

    res = h.run(h.boundary(preflight=h.preflight(seed=False, refresh=refresh)))
    assert refreshed == [1] and res.status == B.EXECUTED
    assert catalog_refresh.pending("binance_usdm", "DEMO")["trigger"] == "PRE_SUBMIT_METADATA_STALE"


def test_refresh_stamped_after_the_evaluation_started_is_fresh(h):
    # Production's refresh stamps the wall clock, later than the ``now`` the
    # evaluation began with. The record just fetched is not a stale one.
    h.seed_catalog(now=h.now - 3 * 3_600_000)
    res = h.run(h.boundary(preflight=h.preflight(seed=False, refresh=lambda: h.seed_catalog(now=h.now + 1_500))))
    assert res.status == B.EXECUTED, res.reason_codes


def test_metadata_stamped_far_in_the_future_is_not_trusted(h):
    h.seed_catalog(now=h.now + 3_600_000)  # a clock fault, not a refresh
    res = h.run(h.boundary(preflight=h.preflight(seed=False)))
    assert res.status == B.PREFLIGHT_BLOCKED and res.reason_codes == ("CAPABILITY_STALE",)


def test_failed_refresh_blocks_the_order(h):
    h.seed_catalog(now=h.now - 3 * 3_600_000)

    def refresh():
        raise RuntimeError("venue down")

    res = h.run(h.boundary(preflight=h.preflight(seed=False, refresh=refresh)))
    assert res.status == B.PREFLIGHT_BLOCKED and res.reason_codes == ("CAPABILITY_STALE",)
    assert h.seen["orders"] == [] and h.reservation_status() == "RELEASED"


def test_unknown_and_delisted_instruments_block(h):
    res = h.run(h.boundary(preflight=h.preflight(seed=False)))
    assert res.status == B.PREFLIGHT_BLOCKED and res.reason_codes == ("INSTRUMENT_UNKNOWN",)


def test_delisted_instrument_blocks(tmp_path):
    h = Harness(tmp_path)
    h.seed_catalog(now=h.now - 1_000)
    h.seed_catalog(now=h.now, instruments=[h.instrument("ETHUSDT")])  # BTCUSDT disappeared from discovery
    res = h.run(h.boundary(preflight=h.preflight(seed=False)))
    assert res.status == B.PREFLIGHT_BLOCKED and res.reason_codes == ("INSTRUMENT_DELISTED",)
    assert h.seen["orders"] == []


def test_capability_regression_after_planning_blocks(h):
    h.seed_catalog(now=h.now - 1_000)
    h.seed_catalog(now=h.now, instruments=[h.instrument("BTCUSDT", min_qty=0.01), h.instrument("ETHUSDT")])
    assert h.plan.decision_time < h.now
    res = h.run(h.boundary(preflight=h.preflight(seed=False)))
    assert res.status == B.PREFLIGHT_BLOCKED and "CAPABILITY_CHANGED_SINCE_PLAN" in res.reason_codes
    assert h.seen["orders"] == []


def test_api_non_tradable_product_names_its_reason(h):
    h.seed_catalog(instruments=[h.instrument("BTCUSDT", api_tradable=False, venue_metadata={"apiStateOpen": "false"}),
                                h.instrument("ETHUSDT")])
    res = h.run(h.boundary(preflight=h.preflight(seed=False)))
    assert res.status == B.PREFLIGHT_BLOCKED
    assert {"INSTRUMENT_NOT_API_TRADABLE", "VENUE_API_NOT_SUPPORTED"} <= set(res.reason_codes)


def test_fx_or_tradfi_product_is_never_forced_through_crypto_logic(h):
    # the catalog says this venue symbol is an FX perpetual classified only from symbol shape
    fx = h.instrument("BTCUSDT", asset_class="FX", canonical_symbol="BTC/USDT:PERP", classification_source="SYMBOL_SHAPE")
    h.seed_catalog(instruments=[fx, h.instrument("ETHUSDT")])
    res = h.run(h.boundary(preflight=h.preflight(seed=False)))
    assert res.status == B.PREFLIGHT_BLOCKED
    assert {"PRODUCT_TYPE_MISMATCH", "CLASSIFICATION_NOT_VENUE_EVIDENCED"} <= set(res.reason_codes)
    assert h.seen["orders"] == []


def test_min_notional_violation_blocks_after_hard_risk_before_the_broker(tmp_path):
    h = Harness(tmp_path, fixed=0.5)  # 0.5 margin x 5 leverage = 2.5 notional < 5 venue minimum
    res = h.run()
    assert res.status == B.PREFLIGHT_BLOCKED and "MIN_NOTIONAL_NOT_MET" in res.reason_codes
    assert res.risk_decision is not None and res.risk_decision.approved  # hard risk ran first and approved
    assert h.seen["orders"] == [] and h.reservation_status() == "RELEASED"


def test_quantity_is_rerounded_and_material_rounding_blocks(h):
    pre = h.preflight()
    ok = pre.quantity(h.plan, 6.0004, 100.0, h.now)
    assert ok.ok and ok.quantity == pytest.approx(6.0)  # rounded DOWN to the step, never up
    h.seed_catalog(instruments=[h.instrument("BTCUSDT", qty_step=1.0, min_qty=1.0), h.instrument("ETHUSDT")])
    coarse = h.preflight(seed=False).quantity(h.plan, 1.5, 100.0, h.now)
    assert not coarse.ok and "QUANTITY_ROUNDING_MATERIAL" in coarse.reason_codes
    big = h.preflight(seed=False).quantity(h.plan, 50_000.0, 100.0, h.now)
    assert "QUANTITY_ABOVE_MAXIMUM" in big.reason_codes


def test_client_order_id_is_deterministic_from_plan_lineage(h, monkeypatch):
    ex = h.executor
    ident = f"{h.plan.trade_plan_id}|{h.plan.trade_plan_hash}"
    a = ex._build_entry_idempotency("BTCUSDT", "BUY", 600.0, 95.0, 110.0, intent_identity=ident)
    monkeypatch.setattr(time, "time", lambda: 9_999_999_999.0)  # any later cycle / restart
    b = ex._build_entry_idempotency("BTCUSDT", "BUY", 600.0, 95.0, 110.0, intent_identity=ident)
    other = ex._build_entry_idempotency("BTCUSDT", "BUY", 600.0, 95.0, 110.0, intent_identity=ident + "x")
    assert a == b and a[1] != other[1]
    assert len(a[1]) <= 36  # fits Bybit orderLinkId (36) and BingX clientOrderID (40)


def test_existing_adapter_selection_per_broker(h):
    binance = executor_adapter_for("binance", h.executor)
    assert binance.venue == "BINANCE_USDM" and adapter_supports(binance, "DEMO")
    for broker, venue in (("bybit", "BYBIT_LINEAR"), ("bingx", "BINGX_SWAP")):
        a = executor_adapter_for(broker, h.executor)
        # the SAME executor wrapper (one broker execution contract) -- no second adapter -- but UNVALIDATED
        assert type(a) is type(binance) and a.venue == venue and not adapter_supports(a, "DEMO")
    for broker in ("mt5", "oanda", "ibkr", None):
        a = executor_adapter_for(broker, h.executor)
        assert isinstance(a, UnvalidatedExecutionAdapter)  # CFD / MT5 / TradFi never reach crypto order logic
    assert type(binance).execution_support_status == "CONTRACT_VALIDATED"  # the class default is untouched


def test_unvalidated_venue_adapter_never_reaches_hard_risk_or_the_broker(h):
    from unittest.mock import MagicMock

    orch = MagicMock()
    a = executor_adapter_for("bybit", h.executor)
    b = h.boundary(adapter=a)
    b.orchestrator = orch
    res = h.run(b)
    assert res.status in (B.VENUE_MISMATCH, B.ADAPTER_UNVALIDATED)
    orch.process_trade_plan.assert_not_called()
    assert h.seen["orders"] == []
