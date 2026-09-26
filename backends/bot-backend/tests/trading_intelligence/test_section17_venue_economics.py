"""Section 17 -- broker/venue economic adaptation: the full 17.23 matrix."""
from __future__ import annotations

import ast
import dataclasses
import json
from pathlib import Path

import pytest
from _helpers import candidate_for, full_chain, market_state_and_regime, flat_rows, instrument
from _plan import binance_like_raw, library, venue_evaluated
from _venue import (
    FAKE_SECRETS, FIXTURE_REGISTRY, FX_CLOSED_MS, FX_OPEN_MS, FUT_EXPIRY_MS, binance_case, binance_key, binance_raw,
    binance_request, fut_case, fut_raw, fut_request, fx_case, fx_key, fx_raw, fx_request,
)

from app.trading_intelligence.contracts.economics import AdmissionStatus, CostScope, EconomicsReasonCode
from app.trading_intelligence.contracts.setup import SetupCandidate
from app.trading_intelligence.contracts.system_health import BrokerHealthContext
from app.trading_intelligence.contracts.venue_economics import VenueReasonCode as R
from app.trading_intelligence.economics.engine import evaluate_economic_opportunity
from app.trading_intelligence.forecast.engine import build_outcome_forecast
from app.trading_intelligence.venue.adapter import (
    UnsupportedVenueAdapter, VenueEconomicRequest, VenueRawSnapshot, normalize_environment,
)
from app.trading_intelligence.venue.binance import BinanceUsdmEconomicAdapter, collect_binance_raw
from app.trading_intelligence.venue.contract_suite import run_venue_contract_suite
from app.trading_intelligence.venue.cost_model import build_venue_cost_estimate, rollover_count
from app.trading_intelligence.venue.policy import VenueCostPolicy
from app.trading_intelligence.venue.reference_adapters import DatedFuturesEconomicAdapter, ForexEconomicAdapter
from app.trading_intelligence.venue.registry import ADAPTER_STATUS_REGISTRY, resolve_adapter

BACKEND = Path(__file__).resolve().parents[2]
NOT_VIABLE = EconomicsReasonCode.COST_NOT_VIABLE.value


def cand(key, t, *, side="LONG", entry=100.0, risk_frac=0.01):
    sign = 1 if side == "LONG" else -1
    return SetupCandidate.build(
        market_state_id="m", snapshot_id="s", data_hash="h", instrument_key=key, timeframe="15m", decision_time=t,
        setup_family="TREND_PULLBACK_V2", setup_version="2.0.0", setup_policy_hash="p", side=side,
        trigger_reference=entry, structural_invalidation=entry * (1 - sign * risk_frac),
        target_reference=entry * (1 + sign * 2 * risk_frac))


def btc(side="LONG", raw=None, **req):
    raw = raw or binance_raw()
    a = BinanceUsdmEconomicAdapter()
    obs = a.observe(binance_request(raw=raw, **req), raw)
    mid = (obs.spread_observation.best_bid + obs.spread_observation.best_ask) / 2
    return obs, build_venue_cost_estimate(cand(binance_key(), obs.decision_time, side=side, entry=mid), obs)


# ============================== GENERAL ==============================
def test_deterministic_adapter_output_and_cost_estimate():
    o1, c1 = btc()
    o2, c2 = btc()
    assert o1.observation_hash == o2.observation_hash and o1.observation_id == o2.observation_id
    assert c1 == c2 and c1.cost_estimate_id == c2.cost_estimate_id


def test_no_credentials_or_authorization_headers_in_payloads():
    obs, cost = btc()
    blob = json.dumps([dataclasses.asdict(obs), dataclasses.asdict(cost)], default=str).lower()
    for word in ("api_key", "apikey", "secret", "signature", "authorization", "x-mbx-apikey"):
        assert word not in blob
    for s in FAKE_SECRETS:
        assert s.lower() not in blob


class _SecretClient:
    """Existing-client stand-in: touching a credential attribute fails the test."""

    def __init__(self, symbol="BTCUSDT"):
        self._rec = binance_raw(symbol).payloads

    @property
    def api_key(self):
        raise AssertionError("CATI must never read the client's api key")

    @property
    def api_secret(self):
        raise AssertionError("CATI must never read the client's secret")

    def exchange_info_cached(self):
        return {"serverTime": self._rec["exchange_info_as_of"], "symbols": [self._rec["exchange_info_symbol"]]}

    def book_ticker(self, s):
        return dict(self._rec["book_ticker"], signature="LEAK?", apiKey=FAKE_SECRETS[1])

    def mark_price(self, s):
        return self._rec["premium_index"]

    def depth(self, s, limit=20):
        return self._rec["depth"]

    def funding_info(self):
        return self._rec["funding_info"]


def test_collector_uses_public_methods_only_and_whitelists_fields():
    raw = collect_binance_raw(_SecretClient(), "BTCUSDT", clock=lambda: 1)
    blob = json.dumps(raw.payloads).lower()
    assert "signature" not in blob and FAKE_SECRETS[1].lower() not in blob and "apikey" not in blob
    assert set(raw.payloads) >= {"exchange_info_symbol", "book_ticker", "premium_index", "depth", "funding_info"}


def test_collector_failure_is_explicit_never_fabricated():
    class Broken(_SecretClient):
        def book_ticker(self, s):
            raise RuntimeError("down")

    raw = collect_binance_raw(Broken(), "BTCUSDT", clock=lambda: 1)
    assert "book_ticker" not in raw.payloads and R.COLLECTION_ERROR.value in raw.reason_codes


def test_stale_book_is_never_treated_as_live():
    raw = binance_raw()
    stale_t = BinanceUsdmEconomicAdapter().causal_decision_time(raw, 0) + 60_000
    obs, cost = btc(raw=raw, decision_time=stale_t)
    assert obs.spread_observation.source != "LIVE_TOP_OF_BOOK" and obs.spread_observation.stale
    assert {R.STALE_BOOK.value, R.SPREAD_FALLBACK_USED.value} <= set(obs.reason_codes)


def test_unknown_fee_tier_uses_conservative_fallback_never_zero_never_cheapest():
    obs, cost = btc()
    fee = obs.fee_observation
    assert fee.source == "CONSERVATIVE_CONFIGURED_FALLBACK" and R.FEE_TIER_UNKNOWN.value in obs.reason_codes
    assert fee.taker_fee_rate == 0.0005 and fee.maker_fee_rate == 0.0002  # VIP0: the highest regular tier
    assert cost.fee_R > 0 and cost.uncertainty_breakdown.fee_uncertainty_R > 0


def test_zero_cost_refusal():
    raw = binance_raw(commission={"makerCommissionRate": "0", "takerCommissionRate": "0"})
    obs, cost = btc(raw=raw)
    assert R.ZERO_FEE_REFUSED.value in obs.reason_codes and cost.fee_R > 0  # refused -> conservative fallback
    zero = dataclasses.replace(obs.fee_observation, source="BROKER_METADATA", taker_fee_rate=0.0, maker_fee_rate=0.0)
    c2 = build_venue_cost_estimate(cand(binance_key(), obs.decision_time), dataclasses.replace(obs, fee_observation=zero))
    assert NOT_VIABLE in c2.reason_codes and R.ZERO_FEE_REFUSED.value in c2.reason_codes


def test_account_isolation_fee_tiers_never_shared():
    raw_a = binance_raw(commission={"makerCommissionRate": "0.0001", "takerCommissionRate": "0.0002"})
    oa, ca = btc(raw=raw_a, account="acct-A")
    ob, cb = btc(account="acct-B")
    assert ca.broker_account_id == "acct-A" and cb.broker_account_id == "acct-B"
    assert ca.fee_R < cb.fee_R and ca.cost_estimate_id != cb.cost_estimate_id
    assert ca.cost_scope == cb.cost_scope == CostScope.ACCOUNT.value
    assert ca.run_id == "run1" and ca.cycle_id == "cyc1" and ca.bot_instance_id == "botA"


def test_reference_scope_estimate_carries_no_tenant_identity():
    obs, _ = btc(account=None, user=None, bot=None)
    c = build_venue_cost_estimate(cand(binance_key(), obs.decision_time), obs)
    assert c.cost_scope == CostScope.VENUE.value and c.user_id is None and c.broker_account_id is None


def test_environment_identity_comes_from_account_not_env():
    assert normalize_environment("demo") == "DEMO" and normalize_environment("live") == "REAL"
    assert normalize_environment("testnet") == "TESTNET" and normalize_environment(None) == "UNKNOWN"
    assert normalize_environment("practice") == "DEMO" and normalize_environment("???") == "UNKNOWN"
    for env, status in (("DEMO", "DEMO_VALIDATED"), ("REAL", "SHADOW_VALIDATED"), ("TESTNET", "SHADOW_VALIDATED")):
        obs, _ = btc(env=env)
        assert obs.environment == env and obs.adapter_status == status
    obs, cost = btc(env="mystery")
    assert R.ENVIRONMENT_UNKNOWN.value in obs.reason_codes and NOT_VIABLE in cost.reason_codes


# ============================== FEES ==============================
def test_maker_and_taker_fees():
    raw = binance_raw(commission={"makerCommissionRate": "0.0001", "takerCommissionRate": "0.0003"})
    obs, cost = btc(raw=raw)
    assert obs.fee_observation.source == "OBSERVED_ACCOUNT_TIER"
    assert cost.uncertainty_breakdown.fee_uncertainty_R == 0.0
    mid = (obs.spread_observation.best_bid + obs.spread_observation.best_ask) / 2
    c = cand(binance_key(), obs.decision_time, entry=mid)
    assert cost.fee_R == pytest.approx(2 * 0.0003 * mid / c.initial_structural_risk)  # taker both sides (conservative)
    maker = build_venue_cost_estimate(c, obs, policy=VenueCostPolicy(assume_taker_entry=False, assume_taker_exit=False))
    assert maker.fee_R == pytest.approx(2 * 0.0001 * mid / c.initial_structural_risk)


def test_per_contract_commission_futures():
    a = DatedFuturesEconomicAdapter(status_registry=FIXTURE_REGISTRY)
    obs = a.observe(fut_request(), fut_raw())
    c = cand(fut_request().instrument_key, FX_OPEN_MS, entry=5025.0, risk_frac=0.004)
    cost = build_venue_cost_estimate(c, obs)
    per = 0.85 + 1.28 + 0.10
    # one contract: 2 sides * per-contract charges / (risk points * multiplier)
    assert cost.fee_R == pytest.approx(2 * per / (c.initial_structural_risk * 50))
    assert cost.native_costs["quantity"] == 1.0


def test_fallback_fee_futures_when_commission_unknown():
    a = DatedFuturesEconomicAdapter(status_registry=FIXTURE_REGISTRY)
    obs = a.observe(fut_request(), fut_raw(commission=None))
    assert obs.fee_observation.source == "CONSERVATIVE_CONFIGURED_FALLBACK"
    cost = build_venue_cost_estimate(cand(fut_request().instrument_key, FX_OPEN_MS, entry=5025.0), obs)
    assert cost.fee_R > 0 and cost.uncertainty_breakdown.fee_uncertainty_R > 0


def test_fee_side_independence():
    _, lc = btc("LONG")
    _, sc = btc("SHORT")
    assert lc.fee_R == sc.fee_R and lc.spread_R == sc.spread_R


# ============================== SPREAD ==============================
def test_live_spread_from_top_of_book():
    obs, cost = btc()
    sp = obs.spread_observation
    assert sp.source == "LIVE_TOP_OF_BOOK" and sp.spread_absolute == pytest.approx(sp.best_ask - sp.best_bid)
    assert any(l.component == "SPREAD" and l.source == "LIVE_TOP_OF_BOOK" for l in cost.component_lineage)


def test_historical_spread_distribution_fallback_and_unavailable():
    raw = binance_raw()
    payloads = {k: v for k, v in raw.payloads.items() if k != "book_ticker"}
    hist = dataclasses.replace(raw, payloads=dict(payloads, spread_history_bps=[1.0, 2.0, 3.0, 4.0, 5.0]))
    a = BinanceUsdmEconomicAdapter()
    obs = a.observe(binance_request(raw=hist), hist)
    assert obs.spread_observation.source == "RECENT_VENUE_DISTRIBUTION" and obs.spread_observation.spread_bps == 4.0
    bucket = dataclasses.replace(raw, payloads=dict(payloads, liquidity_bucket="LOW"))
    assert a.observe(binance_request(raw=bucket), bucket).spread_observation.source == "LIQUIDITY_BUCKET_HISTORICAL"
    plain = dataclasses.replace(raw, payloads=payloads)
    assert a.observe(binance_request(raw=plain), plain).spread_observation.source == "CONSERVATIVE_VENUE_FALLBACK"
    no_fb = BinanceUsdmEconomicAdapter(VenueCostPolicy(fallback_spread_bps={}))
    obs_u = no_fb.observe(binance_request(raw=plain), plain)
    assert obs_u.spread_observation.source == "UNAVAILABLE"
    assert NOT_VIABLE in build_venue_cost_estimate(cand(binance_key(), obs_u.decision_time, entry=84000), obs_u).reason_codes


def test_fallback_spread_costs_more_uncertainty_than_live():
    _, live = btc()
    raw = binance_raw()
    plain = dataclasses.replace(raw, payloads={k: v for k, v in raw.payloads.items() if k != "book_ticker"})
    obs = BinanceUsdmEconomicAdapter().observe(binance_request(raw=plain), plain)
    c = build_venue_cost_estimate(cand(binance_key(), obs.decision_time, entry=84000.0), obs)
    assert c.uncertainty_breakdown.spread_uncertainty_R > live.uncertainty_breakdown.spread_uncertainty_R


# ============================== SLIPPAGE ==============================
def test_depth_walk_slippage_is_size_aware_and_non_improving():
    obs, _ = btc()
    c = cand(binance_key(), obs.decision_time, entry=84000.0)
    costs = [build_venue_cost_estimate(c, obs, reference_notional=n).slippage_R for n in (1e3, 1e5, 1e6, 5e6, 5e7)]
    assert all(b >= a for a, b in zip(costs, costs[1:])) and costs[-1] > costs[0]
    big = build_venue_cost_estimate(c, obs, reference_notional=5e7)
    assert R.DEPTH_INSUFFICIENT_FOR_SIZE.value in big.reason_codes and R.LARGE_ORDER_VS_DEPTH.value in big.reason_codes
    curve = build_venue_cost_estimate(c, obs).marginal_cost_curve
    assert [n for n, _ in curve] == sorted(n for n, _ in curve)
    assert all(b[1] >= a[1] for a, b in zip(curve, curve[1:]))


def test_historical_slippage_fallback_hierarchy_and_missing_depth_uncertainty():
    _, with_depth = btc()
    raw = binance_raw(with_depth=False)
    obs = BinanceUsdmEconomicAdapter().observe(binance_request(raw=raw), raw)
    assert obs.slippage_observation.source == "VENUE_ASSET_CLASS_DEFAULT" and R.DEPTH_UNAVAILABLE.value in obs.reason_codes
    c = cand(binance_key(), obs.decision_time, entry=84000.0)
    fb = build_venue_cost_estimate(c, obs)
    assert fb.slippage_R > 0  # never zero because depth is missing
    assert fb.uncertainty_breakdown.slippage_uncertainty_R > with_depth.uncertainty_breakdown.slippage_uncertainty_R
    hist = dataclasses.replace(raw, payloads=dict(raw.payloads, slippage_history_bps=1.5))
    obs_h = BinanceUsdmEconomicAdapter().observe(binance_request(raw=hist), hist)
    assert obs_h.slippage_observation.source == "HISTORICAL_INSTRUMENT" and obs_h.slippage_observation.fallback_level == 1
    cons = BinanceUsdmEconomicAdapter(VenueCostPolicy(venue_class_slippage_bps={}))
    assert cons.observe(binance_request(raw=raw), raw).slippage_observation.source == "CONSERVATIVE_CONFIGURED_FALLBACK"


# ============================== CRYPTO FUNDING ==============================
def _funding_obs(rate, next_offset_ms, raw_t=None):
    base = binance_raw()
    prem = dict(base.payloads["premium_index"], lastFundingRate=str(rate))
    a = BinanceUsdmEconomicAdapter()
    t = a.causal_decision_time(base, base.captured_at)
    prem["nextFundingTime"] = t + next_offset_ms
    raw = binance_raw(premium=prem)
    return a.observe(binance_request(raw=raw), raw)


def test_positive_funding_long_pays_short_not_credited_by_default():
    obs = _funding_obs(0.0005, 60_000)  # stamp one minute after decision: always crossed
    lc = build_venue_cost_estimate(cand(binance_key(), obs.decision_time, entry=84000.0), obs)
    sc = build_venue_cost_estimate(cand(binance_key(), obs.decision_time, side="SHORT", entry=84000.0), obs)
    assert lc.funding_R > 0 and sc.funding_R == 0.0
    assert R.FUNDING_INCOME_NOT_CREDITED.value in sc.reason_codes and sc.native_costs["uncredited_funding_income_R"] > 0


def test_credited_funding_income_never_improves_conservative_edge():
    ev = venue_evaluated(side="SHORT", raw_kwargs={"funding": 0.003, "next_funding_in_ms": 60_000})
    obs = ev.venue_observation
    policy = VenueCostPolicy(credit_funding_income=True)
    credited = build_venue_cost_estimate(ev.candidate, obs, forecast=ev.forecast, policy=policy)
    default = ev.cost_estimate
    opp_c = evaluate_economic_opportunity(ev.candidate, ev.market_state, ev.forecast, credited)
    opp_d = evaluate_economic_opportunity(ev.candidate, ev.market_state, ev.forecast, default)
    assert credited.funding_R < 0 and default.funding_R == 0.0  # income exists, credited only under the policy
    assert opp_c.ev_net_r > opp_d.ev_net_r
    assert opp_c.conservative_edge_r <= opp_d.conservative_edge_r + 1e-12


def test_time_to_funding_and_multi_period_hold():
    far = _funding_obs(0.0005, 10 * 3_600_000)  # next stamp after the whole max hold
    c = cand(binance_key(), far.decision_time, entry=84000.0)
    assert build_venue_cost_estimate(c, far).funding_R == 0.0
    near = _funding_obs(0.0005, 60_000)
    one = build_venue_cost_estimate(c, near, policy=VenueCostPolicy(default_holding_bars=4))
    many = build_venue_cost_estimate(c, near, policy=VenueCostPolicy(default_holding_bars=40, max_holding_bars=48))
    assert one.native_costs["funding_stamps_expected"] == 1
    assert many.native_costs["funding_stamps_expected"] == 2 and many.funding_R == pytest.approx(2 * one.funding_R)


def test_predicted_funding_unavailable_is_not_fabricated():
    obs, cost = btc()
    f = obs.funding_observation
    assert f.predicted_funding_rate is None and R.PREDICTED_FUNDING_UNAVAILABLE.value in obs.reason_codes
    assert any(l.component == "FUNDING" and l.source == "CURRENT_RATE_SCHEDULE" for l in cost.component_lineage)


def test_missing_funding_charges_conservative_fallback_not_zero():
    raw = binance_raw()
    no_prem = dataclasses.replace(raw, payloads={k: v for k, v in raw.payloads.items() if k != "premium_index"})
    obs = BinanceUsdmEconomicAdapter().observe(binance_request(raw=no_prem), no_prem)
    for side in ("LONG", "SHORT"):
        c = build_venue_cost_estimate(cand(binance_key(), obs.decision_time, side=side, entry=84000.0), obs)
        assert c.funding_R > 0 and R.FUNDING_FALLBACK_USED.value in c.reason_codes


def test_mark_index_available_on_binance():
    obs, _ = btc()
    assert obs.funding_observation.mark_price and obs.funding_observation.index_price
    assert dict(obs.feature_availability)["mark_index"] is True


# ============================== FOREX ==============================
def _fx(commission=None, financing="default", t=FX_OPEN_MS, symbol="EURUSD", registry=FIXTURE_REGISTRY, **kw):
    a = ForexEconomicAdapter(status_registry=registry)
    raw = fx_raw(symbol, t=t, commission=commission, financing=financing)
    return a.observe(fx_request(symbol, t=t), raw)


def test_forex_spread_only_account_is_not_zero_cost():
    obs = _fx()
    c = build_venue_cost_estimate(cand(fx_key(), FX_OPEN_MS, entry=1.10004, risk_frac=0.002), obs)
    assert obs.fee_observation.fee_model == "SPREAD_ONLY" and c.fee_R == 0.0 and c.spread_R > 0
    assert NOT_VIABLE not in c.reason_codes


def test_forex_commission_account():
    obs = _fx(commission={"model": "SPREAD_PLUS_COMMISSION", "per_lot": 3.5, "lot_units": 100000, "currency": "USD"})
    c = build_venue_cost_estimate(cand(fx_key(), FX_OPEN_MS, entry=1.1, risk_frac=0.002), obs)
    q = c.native_costs["quantity"]
    assert c.fee_R == pytest.approx(2 * 3.5 * q / 100000 / c.native_costs["risk_ccy"])


def test_forex_commission_in_other_currency_needs_conversion():
    comm = {"model": "SPREAD_PLUS_COMMISSION", "per_lot": 3.5, "lot_units": 100000, "currency": "USD"}
    obs = _fx(symbol="USDJPY", commission=comm)
    key = fx_key("USDJPY")
    c = build_venue_cost_estimate(cand(key, FX_OPEN_MS, entry=150.0, risk_frac=0.002), obs)
    assert NOT_VIABLE in c.reason_codes and R.CURRENCY_CONVERSION_UNAVAILABLE.value in c.reason_codes
    ok = _fx(symbol="USDJPY", commission=dict(comm, to_quote_rate=150.0))
    assert NOT_VIABLE not in build_venue_cost_estimate(cand(key, FX_OPEN_MS, entry=150.0, risk_frac=0.002), ok).reason_codes


def test_forex_long_and_short_swap_and_rollover():
    obs = _fx()
    long_c = cand(fx_key(), FX_OPEN_MS, entry=1.1, risk_frac=0.002)
    short_c = cand(fx_key(), FX_OPEN_MS, side="SHORT", entry=1.1, risk_frac=0.002)
    policy = VenueCostPolicy(default_holding_bars=48)  # 12h from Wed 10:00 NY crosses Wed 17:00 (triple)
    lc = build_venue_cost_estimate(long_c, obs, policy=policy)
    sc = build_venue_cost_estimate(short_c, obs, policy=policy)
    assert lc.native_costs["fx_rollovers_expected"] == 3  # Wednesday triple swap
    assert lc.carry_R > 0 and lc.funding_R == 0.0  # native swap in carry, never a fake funding rate
    assert sc.carry_R == 0.0 and R.FINANCING_INCOME_NOT_CREDITED.value in sc.reason_codes


def test_rollover_counting():
    assert rollover_count(FX_OPEN_MS, 3_600_000) == 0
    assert rollover_count(FX_OPEN_MS, 8 * 3_600_000, triple_weekday=2) == 3
    assert rollover_count(FX_OPEN_MS, 8 * 3_600_000) == 1


def test_forex_no_crypto_funding_assumption_and_missing_swap_fails_closed():
    obs = _fx(financing=None)
    assert obs.funding_observation.applicable is False and obs.funding_observation.source == "NOT_APPLICABLE"
    c = cand(fx_key(), FX_OPEN_MS, entry=1.1, risk_frac=0.002)
    long_hold = build_venue_cost_estimate(c, obs, policy=VenueCostPolicy(default_holding_bars=48))
    assert R.FINANCING_REQUIRED_UNAVAILABLE.value in long_hold.reason_codes and NOT_VIABLE in long_hold.reason_codes
    certified = build_venue_cost_estimate(c, obs, policy=VenueCostPolicy(default_holding_bars=48,
                                                                         fx_financing_fallback_annual_rate=-0.05))
    assert certified.carry_R > 0 and R.FINANCING_FALLBACK_USED.value in certified.reason_codes


def test_forex_non_24_7_session():
    closed = _fx(t=FX_CLOSED_MS)
    assert closed.session_state.status == "CLOSED" and not closed.tradable
    c = build_venue_cost_estimate(cand(fx_key(), FX_CLOSED_MS, entry=1.1, risk_frac=0.002), closed)
    assert NOT_VIABLE in c.reason_codes and R.MARKET_CLOSED.value in c.reason_codes
    assert _fx().session_state.status == "OPEN"


def test_forex_fee_semantics_unknown_fails_closed():
    a = ForexEconomicAdapter(status_registry=FIXTURE_REGISTRY)
    raw = fx_raw()
    no_comm = dataclasses.replace(raw, payloads={k: v for k, v in raw.payloads.items() if k != "commission"})
    obs = a.observe(fx_request(), no_comm)
    assert NOT_VIABLE in build_venue_cost_estimate(cand(fx_key(), FX_OPEN_MS, entry=1.1), obs).reason_codes


# ============================== FUTURES ==============================
def _fut(**kw):
    a = DatedFuturesEconomicAdapter(status_registry=FIXTURE_REGISTRY)
    return a.observe(fut_request(t=kw.pop("t", FX_OPEN_MS)), fut_raw(**kw))


def test_futures_metadata_multiplier_expiry_tick_settlement():
    obs = _fut()
    m = obs.instrument_metadata
    assert (m.contract_multiplier, m.tick_size, m.tick_value, m.settlement_currency, m.expiry_ms) == (
        50.0, 0.25, 12.5, "USD", FUT_EXPIRY_MS)
    assert m.quote_currency == "USD" and "USDT" not in m.canonical_symbol


def test_futures_carry_basis_side_aware_and_availability():
    obs = _fut()
    c_long = cand(fut_request().instrument_key, FX_OPEN_MS, entry=5025.0, risk_frac=0.004)
    c_short = cand(fut_request().instrument_key, FX_OPEN_MS, side="SHORT", entry=5025.0, risk_frac=0.004)
    lc, sc = build_venue_cost_estimate(c_long, obs), build_venue_cost_estimate(c_short, obs)
    assert obs.carry_observation.basis_absolute == pytest.approx(25.0)
    assert lc.carry_R > 0 and sc.carry_R == 0.0 and R.CARRY_BENEFIT_NOT_CREDITED.value in sc.reason_codes
    no_spot = _fut(spot=None)
    nc = build_venue_cost_estimate(c_long, no_spot)
    assert no_spot.carry_observation.source == "UNAVAILABLE" and nc.carry_R == 0.0
    assert nc.uncertainty_breakdown.carry_uncertainty_R > 0 and R.CARRY_UNAVAILABLE.value in nc.reason_codes


def test_futures_expiry_inside_hold_fails_closed_and_session():
    near = _fut(expiry=FX_OPEN_MS + 3_600_000)
    c = cand(fut_request().instrument_key, FX_OPEN_MS, entry=5025.0, risk_frac=0.004)
    assert R.EXPIRY_WITHIN_HOLD.value in build_venue_cost_estimate(c, near).reason_codes
    assert NOT_VIABLE in build_venue_cost_estimate(c, _fut(session="CLOSED")).reason_codes
    unknown = _fut(session=None)
    assert unknown.session_state.status == "UNKNOWN" and NOT_VIABLE in build_venue_cost_estimate(c, unknown).reason_codes
    thin = build_venue_cost_estimate(c, _fut(session="THIN"))
    normal = build_venue_cost_estimate(c, _fut())
    assert thin.uncertainty_breakdown.spread_uncertainty_R > normal.uncertainty_breakdown.spread_uncertainty_R


# ============================== BROKER CONTRACT ==============================
@pytest.mark.parametrize("make", [lambda: binance_case("BTCUSDT"), lambda: binance_case("ETHUSDT"), fx_case, fut_case],
                         ids=["binance-demo-btc", "binance-demo-eth", "forex", "dated-futures"])
def test_venue_contract_suite_passes(make):
    report = run_venue_contract_suite(make())
    assert report.passed, [(c.name, c.detail) for c in report.failures]
    assert len(report.checks) >= 18


def test_contract_suite_catches_a_broken_adapter():
    class ZeroFeeAdapter(BinanceUsdmEconomicAdapter):
        def estimate_fees(self, request, raw):
            return dataclasses.replace(super().estimate_fees(request, raw), source="VENUE_DEFAULT",
                                       taker_fee_rate=0.0, maker_fee_rate=0.0, reason_codes=())

    case = dataclasses.replace(binance_case(), adapter=ZeroFeeAdapter())
    report = run_venue_contract_suite(case)
    assert not report.passed and "fee_handling" in {c.name for c in report.failures}


def test_runtime_statuses_are_declared_not_inferred():
    assert ADAPTER_STATUS_REGISTRY[("binance_usdm", "DEMO")] == "DEMO_VALIDATED"
    assert not any(k[0] in ("forex_reference", "dated_futures_reference") for k in ADAPTER_STATUS_REGISTRY)
    assert all(v != "PRODUCTION_VALIDATED" for v in ADAPTER_STATUS_REGISTRY.values())


def test_unvalidated_adapter_fails_closed():
    obs = ForexEconomicAdapter().observe(fx_request(), fx_raw())  # runtime registry: UNVALIDATED
    assert obs.adapter_status == "UNVALIDATED" and R.ADAPTER_UNVALIDATED.value in obs.reason_codes
    assert NOT_VIABLE in build_venue_cost_estimate(cand(fx_key(), FX_OPEN_MS, entry=1.1), obs).reason_codes


@pytest.mark.parametrize("broker", [ "oanda", "ibkr", "mt5", None])
def test_unsupported_venue_fails_closed(broker):
    adapter, collector = resolve_adapter(broker)
    assert isinstance(adapter, UnsupportedVenueAdapter) and collector is None
    req = VenueEconomicRequest(instrument_key=binance_key(), environment="DEMO", decision_time=1, user_id="u",
                               broker_account_id="a")
    obs = adapter.observe(req, VenueRawSnapshot("BTCUSDT", {}, 1))
    cost = build_venue_cost_estimate(cand(binance_key(), 1), obs)
    assert R.UNSUPPORTED_VENUE.value in obs.reason_codes and NOT_VIABLE in cost.reason_codes


def test_broker_health_unavailable_or_degraded():
    down = BrokerHealthContext("acct-1", "BINANCE", "DEMO", "UNAVAILABLE", 0, "test")
    obs, cost = btc(health=down)
    assert NOT_VIABLE in cost.reason_codes
    deg = BrokerHealthContext("acct-1", "BINANCE", "DEMO", "DEGRADED", 0, "test")
    _, dc = btc(health=deg)
    _, ok = btc()
    assert dc.uncertainty_breakdown.adapter_uncertainty_R > ok.uncertainty_breakdown.adapter_uncertainty_R


def test_non_causal_observation_is_refused():
    raw = binance_raw()
    obs, cost = btc(raw=raw, decision_time=raw.captured_at - 10_000)
    assert R.NON_CAUSAL_OBSERVATION.value in obs.reason_codes and NOT_VIABLE in cost.reason_codes


def test_instrument_mapping_mismatch_fails_closed():
    raw = binance_raw()
    obs = BinanceUsdmEconomicAdapter().observe(binance_request(raw=raw), raw)
    other = cand(from_eth := binance_key("ETHUSDT"), obs.decision_time, entry=84000.0)
    assert NOT_VIABLE in build_venue_cost_estimate(other, obs).reason_codes
    assert from_eth.canonical_symbol != obs.instrument_key.canonical_symbol


# ============================== SECTION 13 INTEGRATION ==============================
def test_venue_cost_changes_ev_net_and_costs_subtracted_exactly_once():
    ev = venue_evaluated()
    opp = ev.opportunity
    assert opp.cost_estimate_id == ev.cost_estimate.cost_estimate_id
    assert opp.cost_r == pytest.approx(ev.cost_estimate.total_cost_R)
    assert opp.ev_net_r == pytest.approx(opp.ev_gross_r - ev.cost_estimate.total_cost_R)
    assert opp.cost_r == pytest.approx(opp.fee_R + opp.spread_R + opp.slippage_R + opp.funding_R + opp.carry_R)


def test_worse_costs_cannot_improve_conservative_edge():
    cheap = venue_evaluated(raw_kwargs={"commission": {"makerCommissionRate": "0.0001", "takerCommissionRate": "0.0002"}})
    dear = venue_evaluated()
    no_depth = venue_evaluated(raw_kwargs={"depth": False})
    assert dear.cost_estimate.total_cost_R > cheap.cost_estimate.total_cost_R
    assert dear.opportunity.ev_net_r < cheap.opportunity.ev_net_r
    assert dear.opportunity.conservative_edge_r < cheap.opportunity.conservative_edge_r
    assert no_depth.opportunity.conservative_edge_r <= dear.opportunity.conservative_edge_r


def test_same_candidate_differs_across_two_accounts():
    a = venue_evaluated(account="acctA", raw_kwargs={"commission": {"makerCommissionRate": "0.0001",
                                                                     "takerCommissionRate": "0.0002"}})
    b = venue_evaluated(account="acctB")
    assert a.candidate.setup_candidate_id == b.candidate.setup_candidate_id
    assert a.opportunity.economic_opportunity_id != b.opportunity.economic_opportunity_id
    assert a.opportunity.ev_net_r != b.opportunity.ev_net_r
    assert (a.opportunity.broker_account_id, b.opportunity.broker_account_id) == ("acctA", "acctB")


def test_untrustworthy_venue_evidence_becomes_insufficient_evidence():
    ev = venue_evaluated(env="mystery")
    assert NOT_VIABLE in ev.cost_estimate.reason_codes
    assert ev.opportunity.admission_status == AdmissionStatus.INSUFFICIENT_EVIDENCE.value
    assert "COST_UNBOUNDED" in ev.opportunity.reason_codes and ev.veto.outcome != "APPROVE_FOR_RANKING"


def test_no_duplicate_admission_engine():
    """Section 17 computes costs only: no EV/edge/admission/veto symbols."""
    forbidden = ("ev_gross", "ev_net", "conservative_edge", "admission_status", "AdmissionGate", "evaluate_veto",
                 "evaluate_economic_opportunity", "place_order", "submit")
    for path in (BACKEND / "app" / "trading_intelligence" / "venue").glob("*.py"):
        src = path.read_text(encoding="utf-8")
        tree = ast.parse(src)
        names = {n.id for n in ast.walk(tree) if isinstance(n, ast.Name)} | {
            n.attr for n in ast.walk(tree) if isinstance(n, ast.Attribute)} | {
            a.name for n in ast.walk(tree) if isinstance(n, ast.ImportFrom) for a in n.names}
        assert not (names & set(forbidden)), (path.name, names & set(forbidden))


def test_reference_path_unchanged_without_venue_context():
    chain = full_chain()
    assert chain["cost"].cost_scope == "REFERENCE_RESEARCH" and chain["cost"].venue_observation_id is None
