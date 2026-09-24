"""Reusable venue economic contract suite (Section 17.18).

Every CATI economic adapter -- Binance today, a real Forex/futures broker
later -- must pass the SAME checks before its status may rise above
UNVALIDATED. The suite is pure: it runs an adapter over one recorded
``VenueRawSnapshot`` and derived mutations of it (stale book, missing depth,
missing metadata, future-stamped data) and reports each check with detail.
It never touches a network or a broker.
"""
from __future__ import annotations

import dataclasses
import json
from dataclasses import dataclass, field, replace
from typing import Any, Mapping, Optional, Tuple

from app.trading_intelligence.contracts.economics import EconomicsReasonCode
from app.trading_intelligence.contracts.setup import SetupCandidate
from app.trading_intelligence.contracts.venue_economics import (
    FeeModel, FeeSource, SlippageSource, SpreadSource, VenueReasonCode,
)
from app.trading_intelligence.venue.adapter import BaseVenueEconomicAdapter, VenueEconomicRequest, VenueRawSnapshot
from app.trading_intelligence.venue.cost_model import build_venue_cost_estimate

R = VenueReasonCode
_SECRET_KEY_NAMES = ("api_key", "apikey", "secret", "signature", "authorization", "x-mbx-apikey", "password", "token")


@dataclass(frozen=True)
class ContractCase:
    adapter: BaseVenueEconomicAdapter
    raw: VenueRawSnapshot
    request: VenueEconomicRequest
    #: canonical_symbol, venue_symbol, tick_size, step_size, minimum_quantity,
    #: minimum_notional, contract_multiplier, funding_applicable,
    #: financing_applicable, carry_applicable, mark_index, order_types (subset)
    expected: Mapping[str, Any]
    #: raw payload key holding instrument metadata (dropped by the missing-capability check)
    metadata_payload_key: str = "instrument"
    #: values that must never appear anywhere in the evidence (e.g. a client's secrets)
    forbidden_values: Tuple[str, ...] = ()


@dataclass(frozen=True)
class ContractCheck:
    name: str
    passed: bool
    detail: str = ""


@dataclass(frozen=True)
class ContractReport:
    adapter_id: str
    checks: Tuple[ContractCheck, ...] = field(default_factory=tuple)

    @property
    def passed(self) -> bool:
        return bool(self.checks) and all(c.passed for c in self.checks)

    @property
    def failures(self) -> Tuple[ContractCheck, ...]:
        return tuple(c for c in self.checks if not c.passed)


def _candidate(case: ContractCase, side: str) -> SetupCandidate:
    obs = case.adapter.observe(case.request, case.raw)
    sp = obs.spread_observation
    mid = ((sp.best_bid + sp.best_ask) / 2.0) if sp.best_bid and sp.best_ask else float(case.expected["reference_price"])
    sign = 1.0 if side == "LONG" else -1.0
    return SetupCandidate.build(
        market_state_id="contract_suite", snapshot_id="contract_suite", data_hash="contract_suite",
        instrument_key=case.request.instrument_key, timeframe="15m", decision_time=case.request.decision_time,
        setup_family="CONTRACT_SUITE", setup_version="1.0.0", setup_policy_hash="contract_suite", side=side,
        trigger_reference=mid, structural_invalidation=mid * (1 - sign * 0.01), target_reference=mid * (1 + sign * 0.02),
    )


def _serialize(obj: Any) -> str:
    return json.dumps(dataclasses.asdict(obj), default=str, sort_keys=True).lower()


def run_venue_contract_suite(case: ContractCase) -> ContractReport:
    a, raw, req, exp = case.adapter, case.raw, case.request, case.expected
    checks = []

    def check(name: str, ok: bool, detail: str = "") -> None:
        checks.append(ContractCheck(name, bool(ok), detail))

    obs = a.observe(req, raw)
    meta = obs.instrument_metadata
    long_c, short_c = _candidate(case, "LONG"), _candidate(case, "SHORT")
    cost_long = build_venue_cost_estimate(long_c, obs, policy=a.policy)
    cost_short = build_venue_cost_estimate(short_c, obs, policy=a.policy)

    # instrument mapping, both directions
    check("instrument_mapping_canonical_to_venue", meta is not None and meta.venue_symbol == exp["venue_symbol"],
          f"meta={meta and meta.venue_symbol}")
    check("instrument_mapping_venue_to_canonical",
          meta is not None and meta.canonical_symbol == exp["canonical_symbol"] == req.instrument_key.canonical_symbol,
          f"meta={meta and meta.canonical_symbol}")
    for fld in ("tick_size", "step_size", "minimum_quantity", "minimum_notional", "contract_multiplier"):
        check(f"precision_{fld}", meta is not None and getattr(meta, fld) == exp.get(fld),
              f"{getattr(meta, fld, None)} != {exp.get(fld)}")

    # environment identity comes from the request (canonical account), never inferred
    other_env = "REAL" if obs.environment != "REAL" else "DEMO"
    obs_other = a.observe(replace(req, environment=other_env), raw)
    check("environment_identity", obs.environment == exp["environment"] and obs_other.environment == other_env
          and bool(obs.broker) and bool(obs.venue), f"{obs.environment}/{obs_other.environment}")

    # fees are never implicitly zero
    fee = obs.fee_observation
    zero_ok = fee.fee_model == FeeModel.SPREAD_ONLY.value and cost_long.spread_R > 0
    check("fee_handling", fee.source != FeeSource.UNAVAILABLE.value and (cost_long.fee_R > 0 or zero_ok)
          and any(l.component == "FEE" for l in cost_long.component_lineage), f"{fee.source} fee_R={cost_long.fee_R}")

    # spread: live when fresh, never "live" when stale
    if exp.get("has_book", True):
        check("spread_live", obs.spread_observation.source == SpreadSource.LIVE_TOP_OF_BOOK.value,
              obs.spread_observation.source)
        stale_req = replace(req, decision_time=req.decision_time + a.policy.max_book_age_ms + 60_000)
        stale = a.observe(stale_req, raw)
        check("spread_stale_not_live", stale.spread_observation.source != SpreadSource.LIVE_TOP_OF_BOOK.value
              and R.STALE_BOOK.value in stale.reason_codes, stale.spread_observation.source)

    # slippage: missing depth falls back, never to zero, and costs more certainty
    no_depth_raw = replace(raw, payloads={k: v for k, v in raw.payloads.items() if k != "depth"})
    no_depth = a.observe(req, no_depth_raw)
    cost_nd = build_venue_cost_estimate(long_c, no_depth, policy=a.policy)
    check("slippage_fallback", no_depth.slippage_observation.source != SlippageSource.DEPTH_WALK.value
          and cost_nd.slippage_R > 0
          and cost_nd.uncertainty_breakdown.slippage_uncertainty_R >= cost_long.uncertainty_breakdown.slippage_uncertainty_R,
          f"{no_depth.slippage_observation.source} slip_R={cost_nd.slippage_R}")

    # holding-cost capability matches the asset class
    check("funding_capability", obs.funding_observation.applicable == bool(exp["funding_applicable"]),
          obs.funding_observation.source)
    check("financing_capability", obs.financing_observation.applicable == bool(exp["financing_applicable"]),
          obs.financing_observation.source)
    check("carry_capability", obs.carry_observation.applicable == bool(exp["carry_applicable"]),
          obs.carry_observation.source)
    if exp.get("mark_index"):
        f = obs.funding_observation
        check("mark_index", f.mark_price is not None and f.index_price is not None, f"{f.mark_price}/{f.index_price}")

    # execution capabilities
    caps = obs.execution_capabilities
    wanted = set(exp.get("order_types", ()))
    check("supported_order_types", caps is not None and wanted <= set(caps.supported_order_types)
          and caps.supports_market == ("MARKET" in caps.supported_order_types)
          and caps.supports_limit == ("LIMIT" in caps.supported_order_types),
          f"{caps and caps.supported_order_types}")

    # side-aware holding cost, side-independent execution cost
    check("side_independent_execution_cost", cost_long.fee_R == cost_short.fee_R and cost_long.spread_R == cost_short.spread_R,
          f"{cost_long.fee_R}/{cost_short.fee_R}")
    rate = obs.funding_observation.current_funding_rate
    if obs.funding_observation.applicable and rate:
        payer_long = rate > 0
        check("side_aware_funding", (cost_long.funding_R > cost_short.funding_R) == payer_long
              or cost_long.funding_R == cost_short.funding_R == 0.0,
              f"long={cost_long.funding_R} short={cost_short.funding_R}")

    # missing capability fails closed, never "free"
    no_meta = replace(raw, payloads={k: v for k, v in raw.payloads.items() if k != case.metadata_payload_key})
    obs_nm = a.observe(req, no_meta)
    cost_nm = build_venue_cost_estimate(long_c, obs_nm, policy=a.policy)
    check("missing_capability_fails_closed", R.INSTRUMENT_METADATA_UNAVAILABLE.value in obs_nm.reason_codes
          and EconomicsReasonCode.COST_NOT_VIABLE.value in cost_nm.reason_codes, str(obs_nm.reason_codes))

    # no secret leakage anywhere in the evidence
    blob = _serialize(obs) + _serialize(cost_long)
    leaked = [v for v in case.forbidden_values if v and v.lower() in blob]
    named = [k for k in _SECRET_KEY_NAMES if f'"{k}"' in blob]
    check("no_secret_leakage", not leaked and not named, f"leaked={leaked} named={named}")

    # causality: evidence stamped after the decision is refused
    early = replace(req, decision_time=obs.observed_at - 1) if obs.observed_at > req.decision_time - 10**9 else req
    obs_early = a.observe(early, raw)
    ts = a.payload_timestamps(raw)
    if ts and max(ts) > early.decision_time:
        check("causal_timestamps", R.NON_CAUSAL_OBSERVATION.value in obs_early.reason_codes, str(obs_early.reason_codes))

    # determinism
    again = a.observe(req, raw)
    cost_again = build_venue_cost_estimate(long_c, again, policy=a.policy)
    check("deterministic_observation", again.observation_hash == obs.observation_hash)
    check("deterministic_cost_estimate", cost_again == cost_long)

    return ContractReport(a.adapter_id, tuple(checks))


__all__ = ["ContractCase", "ContractCheck", "ContractReport", "run_venue_contract_suite"]
