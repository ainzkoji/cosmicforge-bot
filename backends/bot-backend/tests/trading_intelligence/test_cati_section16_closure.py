from dataclasses import replace
from types import SimpleNamespace
import pytest
from app.exchange.instruments import DiscoveredInstrument
from app.trading_intelligence.venue.perpetual import BybitEconomicAdapter, BingXEconomicAdapter, collect_perpetual_raw
from app.trading_intelligence.venue.adapter import VenueEconomicRequest, VenueRawSnapshot
from app.trading_intelligence.venue.policy import MultiAssetVenueCostPolicy, VenueCostPolicy
from app.trading_intelligence.venue.cost_model import build_venue_cost_estimate
from app.trading_intelligence.contracts.setup import SetupCandidate
from app.trading_intelligence.economics.transfer import TransferEconomics, attach_multi_asset_economics
from app.trading_intelligence.capital.planner import NO_ACTION_SHARED_COLLATERAL, PHYSICAL_INTERNAL_TRANSFER_REQUIRED

T = 1_750_000_000_000


def case(venue="bybit", fee=.0006):
    symbol = "BTCUSDT" if venue == "bybit" else "BTC-USDT"
    info = DiscoveredInstrument(venue=venue, venue_symbol=symbol, asset_class="CRYPTO", product_type="PERPETUAL",
        canonical_symbol="BTC/USDT:PERP", base_currency="BTC", quote_currency="USDT", settlement_asset="USDT",
        contract_type="PERPETUAL", status="Trading", api_tradable=True, tick_size=.1, qty_step=.001,
        min_qty=.001, max_qty=1000, min_notional=5, max_leverage=10, funding_interval_minutes=480)
    key = info.to_instrument_key()
    cls = BybitEconomicAdapter if venue == "bybit" else BingXEconomicAdapter
    # Fixture-only trust: production registry is deliberately unchanged.
    a = cls(status_registry={(cls.adapter_id, "DEMO"): "SHADOW_VALIDATED"})
    payload = {"instrument": info, "metadata_as_of": T, "funding_as_of": T,
        "book": {"bids": [[100-i*.1, 100] for i in range(5)],
                 "asks": [[100.1+i*.1, 100] for i in range(5)], "time": T},
        "funding": {"fundingRate": .0001, "nextFundingTime": T+3_600_000, "markPrice": 100},
        "account_fee": {"maker": .0002, "taker": fee, "as_of": T, "user_id": "u", "broker_account_id": "a",
                        "environment": "DEMO", "venue_symbol": symbol}}
    raw = VenueRawSnapshot(symbol, payload, T)
    req = VenueEconomicRequest(key, "DEMO", T, user_id="u", broker_account_id="a")
    c = SetupCandidate.build(market_state_id="m", snapshot_id="s", data_hash="h", instrument_key=key,
        timeframe="15m", decision_time=T, setup_family="TREND_PULLBACK_V2", setup_version="2", setup_policy_hash="p",
        side="LONG", trigger_reference=100, structural_invalidation=99, target_reference=103)
    c = replace(c, valid_until=T+60_000)
    return a, req, raw, c


@pytest.mark.parametrize("venue", ["bybit", "bingx"])
def test_adapter_costs_and_rounding(venue):
    a, req, raw, c = case(venue)
    obs = a.observe(req, raw)
    cost = build_venue_cost_estimate(c, obs, policy=a.policy)
    assert obs.source_quality == "VALID" and cost.source_quality == "VALID"
    assert cost.fee_R > 0 and cost.spread_R > 0 and cost.slippage_R > 0 and cost.funding_R > 0
    assert cost.native_costs["quantity"] == 10
    assert obs.instrument_metadata.settlement_currency == "USDT"
    assert a.observe(req, raw).observation_hash == obs.observation_hash


@pytest.mark.parametrize("part,reason", [("account_fee", "FEE_UNAVAILABLE"), ("book", "SPREAD_UNAVAILABLE"),
    ("funding", "FUNDING_UNAVAILABLE"), ("instrument", "INSTRUMENT_METADATA_UNAVAILABLE")])
def test_missing_components_block(part, reason):
    a, req, raw, c = case()
    payload = dict(raw.payloads); payload.pop(part)
    obs = a.observe(req, replace(raw, payloads=payload))
    cost = build_venue_cost_estimate(c, obs, policy=a.policy)
    assert cost.source_quality == "INVALID" and reason in obs.reason_codes
    annotated = attach_multi_asset_economics(cost, c, obs)
    evidence = annotated.native_costs["multi_asset_economics"]
    assert evidence["availability"] == "UNAVAILABLE_WITH_REASON"
    if part == "book":
        assert evidence["spread_R"] is None and evidence["slippage_R"] is None


@pytest.mark.parametrize("field", ["user_id", "broker_account_id", "environment", "venue_symbol"])
def test_fee_scope_isolation(field):
    a, req, raw, _ = case()
    payload = {**raw.payloads, "account_fee": {**raw.payloads["account_fee"], field: "other"}}
    assert a.observe(req, replace(raw, payloads=payload)).fee_observation.source == "UNAVAILABLE"


@pytest.mark.parametrize("part,stamp", [("metadata_as_of", T-86_400_001), ("funding_as_of", T-120_001)])
def test_stale_components_block(part, stamp):
    a, req, raw, c = case()
    obs = a.observe(req, replace(raw, payloads={**raw.payloads, part: stamp}))
    assert build_venue_cost_estimate(c, obs, policy=a.policy).source_quality == "INVALID"


def test_stale_fee_and_reference_spread_never_replace_executable_book():
    a, req, raw, c = case()
    p = dict(raw.payloads); p.pop("book")
    p["reference_spread_bps"] = .01
    p["account_fee"] = {**p["account_fee"], "as_of": T-300_001}
    obs = a.observe(req, replace(raw, payloads=p))
    assert obs.spread_observation.spread_bps is None
    assert obs.fee_observation.maker_fee_rate is None


def test_venue_fee_difference_and_maker_policy():
    costs=[]
    for venue, fee in (("bybit", .0006), ("bingx", .0008)):
        a, req, raw, c=case(venue, fee)
        costs.append(build_venue_cost_estimate(c,a.observe(req,raw),policy=a.policy).fee_R)
    assert costs[0] != costs[1]
    a,req,raw,c=case()
    a.policy=replace(a.policy, assume_taker_entry=False, assume_taker_exit=False)
    assert build_venue_cost_estimate(c,a.observe(req,raw),policy=a.policy).fee_R < costs[0]


@pytest.mark.parametrize("route,latency,fee,reason", [
    (NO_ACTION_SHARED_COLLATERAL,None,None,None),
    (PHYSICAL_INTERNAL_TRANSFER_REQUIRED,1000,1,None),
    (PHYSICAL_INTERNAL_TRANSFER_REQUIRED,60_000,1,"TRANSFER_ARRIVES_AFTER_OPPORTUNITY_EXPIRY"),
    (PHYSICAL_INTERNAL_TRANSFER_REQUIRED,None,1,"TRANSFER_LATENCY_UNAVAILABLE"),
    (PHYSICAL_INTERNAL_TRANSFER_REQUIRED,1000,None,"TRANSFER_COST_UNAVAILABLE"),
])
def test_transfer_route_fee_and_expiry(route,latency,fee,reason):
    a,req,raw,c=case(); obs=a.observe(req,raw)
    cost=build_venue_cost_estimate(c,obs,policy=a.policy)
    transfer=TransferEconomics(route,"u","a",T,T+60_000,"fixture",fee,latency,"USDT")
    result=attach_multi_asset_economics(cost,c,obs,transfer)
    if reason:
        assert reason in result.reason_codes and result.source_quality == "INVALID"
    else:
        assert result.source_quality == "VALID"
        expected=0 if route == NO_ACTION_SHARED_COLLATERAL else fee/cost.native_costs["risk_ccy"]
        assert result.total_cost_R == cost.total_cost_R+expected


def test_new_policy_does_not_mutate_default_and_registry_stays_unvalidated():
    from app.trading_intelligence.venue.registry import resolve_adapter
    before=VenueCostPolicy().policy_hash
    for venue in ("bybit","bingx"):
        a,_=resolve_adapter(venue)
        assert a.status_for("DEMO") == "UNVALIDATED"
        assert a.policy.policy_hash != before
    assert VenueCostPolicy().policy_hash == before


def test_cached_collector_never_shares_account_fees():
    a,req,raw,c=case()
    class Client:
        calls=0
        def get_instrument(self,s): return raw.payloads["instrument"]
        def get_orderbook(self,s,limit): return raw.payloads["book"]
        def get_funding(self,s): return raw.payloads["funding"]
        def get_trading_fee_rates(self,s):
            self.calls+=1
            return {"maker":.0002,"taker":.0006,"secret":"DO_NOT_COPY"}
    client=Client()
    x=collect_perpetual_raw(client,raw.venue_symbol,user_id="u",broker_account_id="a",environment="DEMO")
    y=collect_perpetual_raw(client,raw.venue_symbol,user_id="u",broker_account_id="a",environment="DEMO")
    z=collect_perpetual_raw(client,raw.venue_symbol,user_id="v",broker_account_id="b",environment="DEMO")
    assert x is y and x is not z and client.calls == 2
    assert "DO_NOT_COPY" not in str(x.payloads)


# -- frozen identities, mapping order, settlement constraints, collector behaviour ------------------------------
#: Frozen at baseline edc31030 and re-proven after this change. A different value means Section 22 moved.
RESEARCH_DEFAULT_V1_HASH = "55631f31a30ad4a3cf09cf55e6af698bea5d60cce862a303d35c1bcdd842dfe3"
DEFAULT_VENUE_COST_POLICY_HASH = "234bac9012b2c1686ea000e7c67e14141fe49155b370a5ba8964bfe8853d6291"


def test_frozen_section22_and_default_cost_policy_hashes_unchanged():
    from app.trading_intelligence.research.certification.policy import research_default_v1
    from app.trading_intelligence.venue.policy import default_venue_cost_policy
    assert research_default_v1().policy_hash == RESEARCH_DEFAULT_V1_HASH
    assert default_venue_cost_policy().policy_hash == DEFAULT_VENUE_COST_POLICY_HASH
    assert MultiAssetVenueCostPolicy().policy_hash not in (RESEARCH_DEFAULT_V1_HASH, DEFAULT_VENUE_COST_POLICY_HASH)


def test_economics_only_after_concrete_venue_mapping():
    a, req, raw, c = case("bybit")
    _, _, bingx_raw, bingx_c = case("bingx")
    obs = a.observe(req, raw)
    # a Bybit observation cannot price a candidate mapped to BingX (or vice versa)
    assert "INSTRUMENT_MAPPING_MISMATCH" in build_venue_cost_estimate(bingx_c, obs, policy=a.policy).reason_codes
    # nor can a raw snapshot for another venue symbol be observed under this request
    assert "INSTRUMENT_MAPPING_MISMATCH" in a.observe(req, bingx_raw).reason_codes


def test_expired_opportunity_blocks():
    a, req, raw, c = case()
    cost = build_venue_cost_estimate(replace(c, valid_until=T), a.observe(req, raw), policy=a.policy)
    assert cost.source_quality == "INVALID" and "OPPORTUNITY_EXPIRED" in cost.reason_codes


def test_minimum_notional_costs_the_executable_quantity():
    a, req, raw, c = case()
    cost = build_venue_cost_estimate(c, a.observe(req, raw), policy=a.policy, reference_notional=1.0)
    assert "BELOW_MIN_NOTIONAL_AT_REFERENCE" in cost.reason_codes
    assert cost.native_costs["quantity"] * 100 >= 5  # bumped to the venue minimum, never an impossible 0.01


def test_frozen_policy_cannot_price_new_venues():
    from app.trading_intelligence.economics.canonical import canonical_economics
    a, req, raw, c = case()
    with pytest.raises(ValueError, match="MULTI_ASSET_VERSIONED_POLICY_REQUIRED"):
        canonical_economics(c, None, None, a.observe(req, raw), venue_policy=VenueCostPolicy())
    with pytest.raises(ValueError, match="MULTI_ASSET_VERSIONED_POLICY_REQUIRED"):
        BybitEconomicAdapter(policy=VenueCostPolicy())


def test_bingx_funding_interval_from_venue_premium_index_only():
    a, req, raw, c = case("bingx")
    info = replace(raw.payloads["instrument"], funding_interval_minutes=None)
    no_interval = {**raw.payloads, "instrument": info}
    assert "FUNDING_UNAVAILABLE" in a.observe(req, replace(raw, payloads=no_interval)).reason_codes
    published = {**no_interval, "funding": {**raw.payloads["funding"], "fundingIntervalHours": 8}}
    f = a.observe(req, replace(raw, payloads=published)).funding_observation
    assert f.source == "CURRENT_RATE_SCHEDULE" and f.funding_interval_ms == 8 * 3_600_000


def test_failed_reads_retry_after_backoff_not_full_ttl(monkeypatch):
    from app.trading_intelligence.venue import perpetual
    _, _, raw, _ = case()
    clock = [T]
    monkeypatch.setattr(perpetual.time, "time", lambda: clock[0] / 1000)

    class Client:
        instrument_calls = 0
        def get_instrument(self, s):
            self.instrument_calls += 1
            if self.instrument_calls == 1:
                raise RuntimeError("transient")
            return raw.payloads["instrument"]
        def get_orderbook(self, s, limit): return raw.payloads["book"]
        def get_funding(self, s): return raw.payloads["funding"]
        def get_trading_fee_rates(self, s): return {"maker": .0002, "taker": .0006}

    client = Client()
    first = collect_perpetual_raw(client, raw.venue_symbol, user_id="u", broker_account_id="a", environment="DEMO")
    assert first.payloads["instrument"] is None
    clock[0] += perpetual.FAILED_READ_BACKOFF_MS + 1
    second = collect_perpetual_raw(client, raw.venue_symbol, user_id="u", broker_account_id="a", environment="DEMO")
    assert second.payloads["instrument"] is raw.payloads["instrument"] and client.instrument_calls == 2
