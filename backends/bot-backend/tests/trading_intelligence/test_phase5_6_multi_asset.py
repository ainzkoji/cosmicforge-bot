"""Phase 5/6: FX market context, currency exposure graph, capital allocation
planner, opportunity gate, multi-broker account selection, certification
scopes, transfer simulation and the combined portfolio simulation."""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import pytest

from shared_lib.broker.wallets import BrokerTopology, BrokerWallet, WalletPurpose, topology_for

from app.trading_intelligence.capital.planner import (
    ACCOUNT_RECONCILIATION_REQUIRED, INSUFFICIENT_SAFE_CAPITAL, LOGICAL_REALLOCATION, NO_ACTION_SHARED_COLLATERAL,
    PHYSICAL_INTERNAL_TRANSFER_REQUIRED, TRANSFER_UNSUPPORTED, AccountCapitalState, CapitalSettings, is_fundable,
    plan_capital,
)
from app.trading_intelligence.contracts.instrument import from_fx_pair
from app.trading_intelligence.fx.context import build_fx_context
from app.trading_intelligence.integration.opportunity_gate import REQUIREMENTS, admit_to_trade_plan
from app.trading_intelligence.portfolio.currency_exposure import (
    ConcentrationLimits, ExposureItem, build_exposure, check_concentration,
)
from app.trading_intelligence.research.certification import scopes
from app.trading_intelligence.research.certification.portfolio_sim import SimSignal, simulate_portfolio
from app.trading_intelligence.research.certification.transfer_sim import (
    DependentEntry, TransferModel, resolve_dependent_entry, run_scenarios, simulate_transfer,
)
from app.trading_intelligence.venue.account_selection import AccountCandidate, select_account

M = 60_000
TUE_14 = int(datetime(2026, 1, 6, 14, tzinfo=timezone.utc).timestamp() * 1000)
SAT = int(datetime(2026, 1, 10, 12, tzinfo=timezone.utc).timestamp() * 1000)
D = Decimal


# ── 5B FXMarketContext ─────────────────────────────────────────────────────

def test_fx_context_is_causal_deterministic_and_never_zero_fills():
    key = from_fx_pair(venue="bybit_linear", venue_symbol="EURUSDT", base="EUR", quote="USD", contract_type="PERPETUAL")
    ref = {"open_time": TUE_14 - M, "mid_close": 1.1, "bid_close": 1.0999, "ask_close": 1.1001, "timeframe": "1m"}
    vq = {"bid": 1.1004, "ask": 1.1006, "mark": 1.1005, "time": TUE_14}
    a = build_fx_context(instrument_key=key, as_of_ms=TUE_14, reference=ref, venue_quote=vq, funding_rate=0.0001)
    b = build_fx_context(instrument_key=key, as_of_ms=TUE_14, reference=ref, venue_quote=vq, funding_rate=0.0001)
    assert a.context_hash == b.context_hash and a.tradable and a.session == "LONDON_NY_OVERLAP"
    assert a.usd_exposure_long == -1 and a.currency_exposure_long == {"EUR": 1, "USD": -1}
    assert a.divergence_bps == pytest.approx(4.545, abs=0.01)
    assert a.unavailable == {"calendar": "NO_VALIDATED_CALENDAR_SOURCE", "rate_differential": "NO_RATE_SOURCE"}
    empty = build_fx_context(instrument_key=key, as_of_ms=TUE_14, reference=None)
    assert empty.reference_price is None and empty.divergence_bps is None and empty.funding_rate is None
    assert {"reference", "venue_price", "divergence", "spread", "funding"} <= set(empty.unavailable)
    assert not empty.tradable
    future = build_fx_context(instrument_key=key, as_of_ms=TUE_14, reference=ref, venue_quote={**vq, "time": TUE_14 + 1})
    assert future.unavailable["venue_quote"] == "VENUE_QUOTE_AFTER_DECISION_TIME"
    closed = build_fx_context(instrument_key=key, as_of_ms=SAT, reference={**ref, "open_time": SAT - M}, venue_quote=vq)
    assert not closed.market_open and not closed.tradable


# ── 5C/5D currency exposure ────────────────────────────────────────────────

def test_cross_pairs_reveal_hidden_currency_concentration():
    eur = [ExposureItem(p, "FX", "EUR", q, "LONG", 10_000) for p, q in (("EUR/USD", "USD"), ("EUR/GBP", "GBP"),
                                                                        ("EUR/JPY", "JPY"))]
    exp = build_exposure(eur)
    assert exp.by_code["EUR"] == 30_000 and exp.share("EUR") == pytest.approx(1.0)
    usd = build_exposure([ExposureItem(p, "FX", "USD", q, "LONG", 10_000) for p, q in
                          (("USD/JPY", "JPY"), ("USD/CHF", "CHF"), ("USD/CAD", "CAD"))])
    assert usd.by_code["USD"] == 30_000
    ok, reasons = check_concentration(build_exposure(eur[:2]), build_exposure(eur[2:]),
                                      ConcentrationLimits(max_single_currency_share=0.6))
    assert not ok and "CURRENCY_CONCENTRATION:EUR" in reasons
    hedge = build_exposure([ExposureItem("EUR/USD", "FX", "EUR", "USD", "SHORT", 10_000)])
    assert check_concentration(build_exposure(eur), hedge, ConcentrationLimits(max_single_currency_share=0.6))[0]


def test_crypto_and_fx_share_usd_risk_and_unknown_notional_fails_closed():
    btc = ExposureItem("BTC/USDT:PERP", "CRYPTO", "BTC", "USDT", "LONG", 20_000)
    eur = ExposureItem("EUR/USD", "FX", "EUR", "USD", "LONG", 20_000)
    e = build_exposure([btc, eur])
    assert e.by_code == {"BTC": 20_000, "EUR": 20_000, "USD": -20_000}  # USDT settlement leg is not FX risk
    assert e.by_asset_class == {"CRYPTO": 20_000, "FX": 20_000}
    full = build_exposure([btc, eur], crypto_quote_legs=True)
    assert full.by_code["USD"] == -40_000 and full.by_code_raw["USDT"] == -20_000
    ok, reasons = check_concentration(e, build_exposure([ExposureItem("X", "FX", "EUR", "USD", "LONG", None)]))
    assert not ok and reasons == ("CURRENCY_EXPOSURE_NOTIONAL_UNKNOWN",)
    capped = ConcentrationLimits(max_single_currency_share=1.0, max_asset_class_share={"CRYPTO": 0.5})
    assert "ASSET_CLASS_CONCENTRATION:CRYPTO" in check_concentration(e, build_exposure([btc]), capped)[1]


# ── 5E/5F capital allocation planner ───────────────────────────────────────

AUTO = CapitalSettings(mode="AUTOMATED_INTERNAL_REALLOCATION", auto_rebalance_enabled=True, authorized=True)


def _state(topo, free, **kw):
    return AccountCapitalState("acc", "USDT", topo, {k: (None if v is None else D(str(v))) for k, v in free.items()},
                               transfer_capability_usable=kw.pop("usable", True), **kw)


def test_unified_account_needs_no_transfer():
    topo = topology_for("bybit", "UNIFIED")
    plan = plan_capital(state=_state(topo, {"UNIFIED": 1000, "FUND": 5000}), product="FX_PERPETUAL",
                        required=D("200"), settings=AUTO, plan_key="p1")
    assert plan.outcome == NO_ACTION_SHARED_COLLATERAL and plan.transfer is None and is_fundable(plan)


def test_dedicated_wallet_logical_allocation_then_physical_transfer():
    topo = topology_for("binance")
    enough = plan_capital(state=_state(topo, {"UMFUTURE": 1000, "FUNDING": 0, "MAIN": 0}), product="CRYPTO_PERPETUAL",
                          required=D("300"), settings=AUTO, plan_key="p")
    assert enough.outcome == LOGICAL_REALLOCATION
    short = plan_capital(state=_state(topo, {"UMFUTURE": 100, "FUNDING": 5000, "MAIN": None}),
                         product="CRYPTO_PERPETUAL", required=D("300"), settings=AUTO, plan_key="p")
    assert short.outcome == PHYSICAL_INTERNAL_TRANSFER_REQUIRED
    t = short.transfer
    assert (t.source_wallet, t.destination_wallet, t.amount, t.auto_submit) == ("FUNDING", "UMFUTURE", D("210.00000000"), True)
    assert not is_fundable(short) and not is_fundable(short, "SUBMITTED") and not is_fundable(short, "UNKNOWN")
    assert is_fundable(short, "COMPLETED")
    again = plan_capital(state=_state(topo, {"UMFUTURE": 100, "FUNDING": 5000, "MAIN": None}),
                         product="CRYPTO_PERPETUAL", required=D("300"), settings=AUTO, plan_key="p")
    assert again.transfer.idempotency_key == t.idempotency_key  # deterministic: a retry is the same transfer
    manual = plan_capital(state=_state(topo, {"UMFUTURE": 100, "FUNDING": 5000}), product="CRYPTO_PERPETUAL",
                          required=D("300"), settings=CapitalSettings(), plan_key="p")
    assert manual.transfer.auto_submit is False and "USER_ACTION_REQUIRED" in manual.reason_codes


@pytest.mark.parametrize("free,kw,settings,outcome,reason", [
    ({"UMFUTURE": 100, "FUNDING": 5000}, {"unresolved_transfers": 1}, AUTO, ACCOUNT_RECONCILIATION_REQUIRED, "TRANSFER_UNRESOLVED"),
    ({"UMFUTURE": 100, "FUNDING": 5000}, {"usable": False, "transfer_block_reason": "WITHDRAW_PERMISSION_PRESENT"}, AUTO,
     TRANSFER_UNSUPPORTED, "WITHDRAW_PERMISSION_PRESENT"),
    ({"UMFUTURE": 100, "FUNDING": 150}, {}, AUTO, INSUFFICIENT_SAFE_CAPITAL, "NO_WALLET_WITH_SAFE_SPARE_CAPITAL"),
    ({"UMFUTURE": 100, "FUNDING": None, "MAIN": None}, {}, AUTO, INSUFFICIENT_SAFE_CAPITAL, "NO_WALLET_WITH_SAFE_SPARE_CAPITAL"),
    ({"UMFUTURE": None}, {}, AUTO, INSUFFICIENT_SAFE_CAPITAL, "TRADING_WALLET_BALANCE_UNKNOWN"),
    ({"UMFUTURE": 100, "FUNDING": 5000}, {}, CapitalSettings(mode="AUTOMATED_INTERNAL_REALLOCATION",
                                                             auto_rebalance_enabled=True, authorized=True,
                                                             min_funding_balance=D("4900")),
     INSUFFICIENT_SAFE_CAPITAL, "NO_WALLET_WITH_SAFE_SPARE_CAPITAL"),
])
def test_planner_fail_closed_paths(free, kw, settings, outcome, reason):
    plan = plan_capital(state=_state(topology_for("binance"), free, **kw), product="CRYPTO_PERPETUAL",
                        required=D("300"), settings=settings, plan_key="k")
    assert plan.outcome == outcome and reason in plan.reason_codes and not is_fundable(plan)


def test_product_without_a_wallet_is_unsupported():
    plan = plan_capital(state=_state(topology_for("bingx"), {"PFUTURES": 1000}), product="FX_PERPETUAL",
                        required=D("10"), settings=AUTO, plan_key="k")
    assert plan.outcome == TRANSFER_UNSUPPORTED and "NO_WALLET_COLLATERALISES_PRODUCT" in plan.reason_codes


# ── 5H gate ────────────────────────────────────────────────────────────────

def test_opportunity_gate_requires_every_input():
    everything = {k: True for k, _ in REQUIREMENTS}
    assert admit_to_trade_plan(everything).admitted
    r = admit_to_trade_plan({**everything, "internal_transfer_confirmed": None, "permissions_valid": False})
    assert not r.admitted and r.reason_codes == ("INTERNAL_TRANSFER_NOT_CONFIRMED", "PERMISSIONS_INVALID")
    assert len(admit_to_trade_plan({}).reason_codes) == len(REQUIREMENTS)


# ── 5I account selection ───────────────────────────────────────────────────

def _cand(acc, broker, **kw):
    base = dict(user_id="alice", broker_account_id=acc, broker=broker, execution_permitted=True, instrument_active=True,
                credential_valid=True, permissions_valid=True, gross_expectancy_bps=20.0, fee_bps=5.0, spread_bps=2.0,
                slippage_bps=1.0, funding_bps=1.0, capital_outcome="NO_ACTION_SHARED_COLLATERAL")
    base.update(kw)
    return AccountCandidate(**base)


def test_account_selection_after_cost_same_user_only():
    sel = select_account("alice", [
        _cand("a_bin", "binance", fee_bps=4.0),
        _cand("a_byb", "bybit", fee_bps=5.5, capital_outcome="PHYSICAL_INTERNAL_TRANSFER_REQUIRED"),
        _cand("a_bgx", "bingx", execution_permitted=False),
        _cand("b_bin", "binance", user_id="bob", fee_bps=0.0),
        _cand("a_unk", "binance", funding_bps=None),
        _cand("a_poor", "binance", capital_outcome="INSUFFICIENT_SAFE_CAPITAL"),
    ])
    assert sel.selected == "a_bin"
    assert sel.excluded["b_bin"] == ("DIFFERENT_USER",) and "COSTS_UNKNOWN" in sel.excluded["a_unk"]
    assert "EXECUTION_NOT_PERMITTED" in sel.excluded["a_bgx"] and sel.excluded["a_poor"] == ("CAPITAL:INSUFFICIENT_SAFE_CAPITAL",)
    assert sel.scores["a_byb"] < sel.scores["a_bin"]


# ── 6A scopes ──────────────────────────────────────────────────────────────

def test_certification_scopes_are_separate_and_fail_closed():
    assert set(scopes.SCOPES) >= {"CRYPTO/BINANCE/PERPETUAL", "CRYPTO/BYBIT/PERPETUAL", "CRYPTO/BINGX/PERPETUAL",
                                  "FX/REFERENCE_DATA/CATI_INTELLIGENCE", "FX/BYBIT/FX_PERPETUAL"}
    ns = {scopes.holdout_namespace(s, "same-dataset") for s in scopes.SCOPES}
    assert len(ns) == len(scopes.SCOPES)  # no two scopes can share a holdout identity
    status, reasons = scopes.scope_readiness("FX/BINGX/TRADFI", freeze=None, data_days=None, min_days=730,
                                             api_execution_supported=False)
    assert status == scopes.BLOCKED_CAPABILITY and "API_EXECUTION_NOT_SUPPORTED" in reasons
    full = scopes.ScopeFreeze("FX/REFERENCE_DATA/CATI_INTELLIGENCE", {k: "x" for k in scopes.REQUIRED_FREEZE_INPUTS})
    assert scopes.scope_readiness("FX/REFERENCE_DATA/CATI_INTELLIGENCE", freeze=full, data_days=800, min_days=730,
                                  api_execution_supported=None) == (scopes.READY, ())
    partial = scopes.ScopeFreeze("CRYPTO/BYBIT/PERPETUAL", {"dataset_manifest_hash": "h"})
    st, rs = scopes.scope_readiness("CRYPTO/BYBIT/PERPETUAL", freeze=partial, data_days=800, min_days=730,
                                    api_execution_supported=True)
    assert st == scopes.BLOCKED_FREEZE and "universe_hash" in rs[0]


def test_existing_section22_policy_is_unchanged():
    from app.trading_intelligence.research.certification.policy import canonical_certification_policy

    p = canonical_certification_policy()
    assert p.min_forward_demo_days.value >= 30 and p.min_forward_demo_executed_count.value >= 30


# ── 6E transfer simulation ─────────────────────────────────────────────────

def test_transfer_simulation_scenarios():
    insufficient = simulate_transfer(seed=1, transfer_key="t", submitted_at_ms=0, amount=100, transferable_at_submit=50)
    assert insufficient.final_status == "FAILED"
    counts = run_scenarios(seed=3, n=2000, model=TransferModel(p_fail=0.05, p_unknown=0.1))
    assert set(counts) >= {"COMPLETED", "FAILED", "COMPLETED_VIA_UNKNOWN"} and counts == run_scenarios(
        seed=3, n=2000, model=TransferModel(p_fail=0.05, p_unknown=0.1))
    t = simulate_transfer(seed=9, transfer_key="ok", submitted_at_ms=1_000, amount=100, transferable_at_submit=500,
                          model=TransferModel(p_fail=0, p_unknown=0))
    assert t.status_at(1_000) == "CONFIRMATION_PENDING" and not t.funds_available_at(t.confirmed_at_ms - 1)
    assert t.funds_available_at(t.confirmed_at_ms)
    entry = DependentEntry("e", decision_ms=1_000, expires_ms=1_000 + 60_000, required=100)
    status, at = resolve_dependent_entry(entry, t)
    assert status == "EXECUTED" and at == t.confirmed_at_ms  # capital counted only once confirmed
    assert resolve_dependent_entry(entry, t, risk_still_accepts_at={at: False})[0] == "ABANDONED_RISK_CHANGED_WHILE_PENDING"
    slow = simulate_transfer(seed=9, transfer_key="slow", submitted_at_ms=1_000, amount=100, transferable_at_submit=500,
                             model=TransferModel(p_fail=0, p_unknown=1.0, p_unknown_resolves_completed=1.0))
    assert slow.status_at(1_001) == "UNKNOWN"
    assert resolve_dependent_entry(DependentEntry("e", 1_000, 1_000 + 30_000, 100), slow)[0] == "ABANDONED_TRANSFER_TOO_SLOW"
    failed = simulate_transfer(seed=9, transfer_key="f", submitted_at_ms=0, amount=100, transferable_at_submit=500,
                               model=TransferModel(p_fail=1.0))
    assert resolve_dependent_entry(entry, failed)[0] == "ABANDONED_TRANSFER_FAILED"


# ── 6D combined portfolio simulation ───────────────────────────────────────

def test_combined_crypto_fx_portfolio_simulation():
    sigs = [
        SimSignal("s1", 0, "BTC/USDT:PERP", "CRYPTO", "CRYPTO_PERPETUAL", "BTC", "USDT", "LONG", 5_000, 500),
        SimSignal("s2", 0, "EUR/USD", "FX", "FX_PERPETUAL", "EUR", "USD", "LONG", 5_000, 300),
        SimSignal("s3", M, "EUR/GBP", "FX", "FX_PERPETUAL", "EUR", "GBP", "LONG", 5_000, 300),
        SimSignal("s4", 2 * M, "ETH/USDT:PERP", "CRYPTO", "CRYPTO_PERPETUAL", "ETH", "USDT", "LONG", 5_000, 5_000),
    ]
    uta = simulate_portfolio(sigs, topology=topology_for("bybit", "UNIFIED"), free_by_wallet={"UNIFIED": 2_000, "FUND": 0},
                             limits=ConcentrationLimits(max_single_currency_share=0.6))
    assert uta.executed == ["s1", "s2"] and uta.rejected["s3"] == "CURRENCY_CONCENTRATION:EUR"
    assert uta.rejected["s4"].startswith("INSUFFICIENT_SAFE_CAPITAL")
    assert uta.transfers == {}  # unified collateral: never a physical move
    binance = simulate_portfolio(sigs[:1], topology=topology_for("binance"),
                                 free_by_wallet={"UMFUTURE": 100, "FUNDING": 5_000, "MAIN": 0},
                                 model=TransferModel(p_fail=0, p_unknown=0))
    assert binance.executed == ["s1"] and binance.transfers == {"s1": "COMPLETED"}
    assert binance.final_free["FUNDING"] < 5_000


# ── 5G FX-perpetual economics overlay ──────────────────────────────────────

def test_fx_perp_overlay_after_cost_ranking():
    from app.trading_intelligence.economics.fx_perp import after_cost_expectancy_R, fx_perp_overlay

    key = from_fx_pair(venue="bybit_linear", venue_symbol="EURUSDT", base="EUR", quote="USD", contract_type="PERPETUAL")
    ref = {"open_time": TUE_14 - M, "mid_close": 1.1, "bid_close": 1.0999, "ask_close": 1.1001, "timeframe": "1m"}
    ctx = build_fx_context(instrument_key=key, as_of_ms=TUE_14, reference=ref,
                           venue_quote={"bid": 1.1010, "ask": 1.1012, "time": TUE_14})
    o = fx_perp_overlay(fx_context=ctx, entry=1.1, risk=0.0022, spread_R=0.05)
    assert o.tradable and o.divergence_R == pytest.approx(10.0 / 1e4 * 1.1 / 0.0022, rel=1e-3)
    assert after_cost_expectancy_R(1.0, 0.1, o) == pytest.approx(1.0 - 0.1 - o.total_extra_R)
    no_ref = build_fx_context(instrument_key=key, as_of_ms=TUE_14, reference=None)
    fb = fx_perp_overlay(fx_context=no_ref, entry=1.1, risk=0.0022, spread_R=0.05)
    assert fb.divergence_R > 0 and fb.reasons[0].startswith("DIVERGENCE_FALLBACK_USED")  # never zero
    closed = build_fx_context(instrument_key=key, as_of_ms=SAT, reference=ref)
    assert after_cost_expectancy_R(1.0, 0.1, fx_perp_overlay(fx_context=closed, entry=1.1, risk=0.0022,
                                                              spread_R=0.05)) is None
